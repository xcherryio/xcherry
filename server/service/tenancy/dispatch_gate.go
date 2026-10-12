// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package tenancy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/superdurable/dex/config"
	"github.com/superdurable/dex/gen/dexpb"
	"github.com/superdurable/dex/service/common/log"
	"github.com/superdurable/dex/service/common/log/tag"
	"golang.org/x/sync/semaphore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	dispatchSlotHeartbeatInterval = 5 * time.Second
	wakeRedialInterval            = 2 * time.Second
	wakeNotificationInterval      = 10 * time.Second
	activeNotificationInterval    = time.Minute
	wakeRequestTimeout            = 5 * time.Second
	wakeKindWake                  = "wake"
	wakeKindActive                = "active"
)

// DispatchTenant identifies the tenant of one dispatch and the claims its worker target is checked with.
type DispatchTenant struct {
	ID     string
	Claims map[string]string
}

// DispatchGate admits a tenant's dispatches to Workers: it rechecks the worker target, bounds concurrent
// dispatches per tenant, and holds a dispatch to an unavailable Worker while the wake hook starts it.
// In single-tenant mode it admits every dispatch unchanged.
type DispatchGate struct {
	policy          *WorkerTargetPolicy
	maxDispatches   int64
	wakeURL         string
	wakeBudget      time.Duration
	internalToken   *InternalTokenFile
	httpClient      *http.Client
	logger          log.Logger
	isMultiTenant   bool
	isFairness      bool
	mu              sync.Mutex
	slotsByTenant   map[string]*tenantDispatchSlots
	lastWakeByKey   map[wakeNotificationKey]time.Time
	notificationsWG sync.WaitGroup
}

type tenantDispatchSlots struct {
	semaphore *semaphore.Weighted
	users     int
}

type wakeNotificationKey struct {
	target string
	kind   string
}

// NewDispatchGate builds the gate; internalToken may be nil when no wake URL is configured.
func NewDispatchGate(
	cfg *config.TenancyConfig,
	internalToken *InternalTokenFile,
	logger log.Logger,
) (*DispatchGate, error) {
	if cfg == nil || logger == nil {
		panic("dispatch gate requires a tenancy config and a logger")
	}
	policy, err := NewWorkerTargetPolicy(cfg)
	if err != nil {
		return nil, err
	}
	if cfg.Wake.URL != "" && internalToken == nil {
		panic("a wake URL requires the internal token")
	}
	return &DispatchGate{
		policy:        policy,
		maxDispatches: int64(cfg.Limits.EffectiveMaxConcurrentDispatchesPerTenant()),
		wakeURL:       cfg.Wake.URL,
		wakeBudget:    cfg.Wake.EffectiveBudget(),
		internalToken: internalToken,
		httpClient:    &http.Client{Timeout: wakeRequestTimeout},
		logger:        logger,
		isMultiTenant: cfg.IsMultiTenant(),
		isFairness:    cfg.IsMultiTenant() && cfg.Fairness,
		slotsByTenant: map[string]*tenantDispatchSlots{},
		lastWakeByKey: map[wakeNotificationKey]time.Time{},
	}, nil
}

// FairnessKey returns the task queue fairness key of a tenant's workflows, or empty when fairness is off.
func (g *DispatchGate) FairnessKey(tenantID string) string {
	if !g.isFairness {
		return ""
	}
	return tenantID
}

// Policy returns the worker target policy, nil in single-tenant mode.
func (g *DispatchGate) Policy() *WorkerTargetPolicy {
	return g.policy
}

// Admit checks the worker target and waits for one of the tenant's dispatch slots, calling heartbeat while it
// waits. Waiting counts no attempt. The returned release must be called once the dispatch ends.
func (g *DispatchGate) Admit(
	ctx context.Context,
	tenant DispatchTenant,
	target *dexpb.WorkerTarget,
	heartbeat func(),
) (func(), error) {
	if !g.isMultiTenant || tenant.ID == "" {
		return func() {}, nil
	}
	if err := g.policy.Check(tenant.Claims, target); err != nil {
		return nil, err
	}
	slots := g.retainSlots(tenant.ID)
	for {
		waitCtx, cancel := context.WithTimeout(ctx, dispatchSlotHeartbeatInterval)
		err := slots.semaphore.Acquire(waitCtx, 1)
		cancel()
		if err == nil {
			return func() {
				slots.semaphore.Release(1)
				g.releaseSlots(tenant.ID)
			}, nil
		}
		if ctx.Err() != nil {
			g.releaseSlots(tenant.ID)
			return nil, ctx.Err()
		}
		heartbeat()
	}
}

// retainSlots keeps one entry per tenant with active or waiting dispatches, so the map stays bounded.
func (g *DispatchGate) retainSlots(tenantID string) *tenantDispatchSlots {
	g.mu.Lock()
	defer g.mu.Unlock()
	slots, ok := g.slotsByTenant[tenantID]
	if !ok {
		slots = &tenantDispatchSlots{semaphore: semaphore.NewWeighted(g.maxDispatches)}
		g.slotsByTenant[tenantID] = slots
	}
	slots.users++
	return slots
}

func (g *DispatchGate) releaseSlots(tenantID string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	slots := g.slotsByTenant[tenantID]
	slots.users--
	if slots.users == 0 {
		delete(g.slotsByTenant, tenantID)
	}
}

// NewWakeHold starts the wake budget of one dispatch.
func (g *DispatchGate) NewWakeHold(tenant DispatchTenant, target *dexpb.WorkerTarget, heartbeat func()) *WakeHold {
	return &WakeHold{gate: g, tenant: tenant, target: target.GetAddress(), heartbeat: heartbeat}
}

// WakeHold holds one dispatch to an unavailable Worker within the wake budget.
type WakeHold struct {
	gate      *DispatchGate
	tenant    DispatchTenant
	target    string
	heartbeat func()
	startedAt time.Time
}

// ShouldRedial reports whether a dispatch that failed with err should be retried after waiting 2 seconds.
// It notifies the wake hook and holds only while the budget lasts; the wait heartbeats and counts no attempt.
func (h *WakeHold) ShouldRedial(ctx context.Context, err error) bool {
	if h.gate.wakeURL == "" || h.tenant.ID == "" || !isWorkerUnavailable(err) {
		return false
	}
	if h.startedAt.IsZero() {
		h.startedAt = time.Now()
	}
	if time.Since(h.startedAt)+wakeRedialInterval > h.gate.wakeBudget {
		return false
	}
	h.gate.notify(h.tenant.ID, h.target, wakeKindWake, wakeNotificationInterval)
	h.heartbeat()
	timer := time.NewTimer(wakeRedialInterval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// RecordSuccess tells the wake hook that the tenant's Worker served a dispatch.
func (h *WakeHold) RecordSuccess() {
	if h.gate.wakeURL == "" || h.tenant.ID == "" {
		return
	}
	h.gate.notify(h.tenant.ID, h.target, wakeKindActive, activeNotificationInterval)
}

// notify posts one notification asynchronously, at most once per target and kind per interval.
func (g *DispatchGate) notify(tenantID string, target string, kind string, interval time.Duration) {
	key := wakeNotificationKey{target: target, kind: kind}
	now := time.Now()
	g.mu.Lock()
	if now.Sub(g.lastWakeByKey[key]) < interval {
		g.mu.Unlock()
		return
	}
	for staleKey, sentAt := range g.lastWakeByKey {
		if now.Sub(sentAt) >= activeNotificationInterval {
			delete(g.lastWakeByKey, staleKey)
		}
	}
	g.lastWakeByKey[key] = now
	g.mu.Unlock()
	g.notificationsWG.Add(1)
	go func() {
		defer g.notificationsWG.Done()
		if err := g.post(tenantID, target, kind); err != nil {
			g.logger.Warn("worker wake notification failed", tag.Address(target), tag.Error(err))
		}
	}()
}

func (g *DispatchGate) post(tenantID string, target string, kind string) error {
	body, err := json.Marshal(map[string]string{"tenant": tenantID, "target": target, "kind": kind})
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), wakeRequestTimeout)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, g.wakeURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+g.internalToken.Token())
	response, err := g.httpClient.Do(request)
	if err != nil {
		return err
	}
	if closeErr := response.Body.Close(); closeErr != nil {
		return closeErr
	}
	if response.StatusCode/100 != 2 {
		return fmt.Errorf("wake URL answered HTTP %d", response.StatusCode)
	}
	return nil
}

// Close waits for notifications in flight.
func (g *DispatchGate) Close() {
	g.notificationsWG.Wait()
}

func isWorkerUnavailable(err error) bool {
	var noAddress interface{ IsNoWorkerAddress() bool }
	if errors.As(err, &noAddress) && noAddress.IsNoWorkerAddress() {
		return true
	}
	return status.Code(err) == codes.Unavailable
}
