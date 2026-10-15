// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package wake

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
	"github.com/superdurable/dex/service/common/log"
	"github.com/superdurable/dex/service/common/log/tag"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const redialInterval = 2 * time.Second

type notificationKey struct {
	target string
	kind   string
}

// Notifier sends Worker wake and activity messages.
type Notifier struct {
	cfg           *config.WakeConfig
	token         interface{ Token() string }
	logger        log.Logger
	httpClient    *http.Client
	mu            sync.Mutex
	lastByKey     map[notificationKey]time.Time
	notifications sync.WaitGroup
}

// NewNotifier creates a Worker wake notifier with an optional bearer token.
func NewNotifier(cfg *config.WakeConfig, token interface{ Token() string }, logger log.Logger) *Notifier {
	if cfg == nil || logger == nil {
		panic("wake config and logger must not be nil")
	}
	return &Notifier{cfg: cfg, token: token, logger: logger, httpClient: &http.Client{Timeout: 5 * time.Second}, lastByKey: map[notificationKey]time.Time{}}
}

// NewHold starts the wake budget of one dispatch.
func (n *Notifier) NewHold(target string, heartbeat func()) *Hold {
	return &Hold{notifier: n, target: target, heartbeat: heartbeat}
}

// NotifyWorkerUnavailable invokes the configured wake hook for an unavailable pull queue.
func (n *Notifier) NotifyWorkerUnavailable(queue string) {
	if n.cfg.URL != "" {
		n.notify(queue, "wake", 10*time.Second)
	}
}

// NotifyWorkerActive reports a successful pull dispatch to the wake hook.
func (n *Notifier) NotifyWorkerActive(queue string) {
	if n.cfg.URL != "" {
		n.notify(queue, "active", time.Minute)
	}
}

// Hold waits for an unavailable Worker within the configured budget.
type Hold struct {
	notifier  *Notifier
	target    string
	heartbeat func()
	startedAt time.Time
}

// ShouldRedial reports whether the dispatch should retry after a wait.
func (h *Hold) ShouldRedial(ctx context.Context, dispatchErr error) bool {
	if h.notifier.cfg.URL == "" || !isWorkerUnavailable(dispatchErr) {
		return false
	}
	if h.startedAt.IsZero() {
		h.startedAt = time.Now()
	}
	if time.Since(h.startedAt)+redialInterval > h.notifier.cfg.EffectiveBudget() {
		return false
	}
	h.notifier.notify(h.target, "wake", 10*time.Second)
	h.heartbeat()
	timer := time.NewTimer(redialInterval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// RecordSuccess reports that the Worker served a dispatch.
func (h *Hold) RecordSuccess() {
	if h.notifier.cfg.URL != "" {
		h.notifier.notify(h.target, "active", time.Minute)
	}
}

func (n *Notifier) notify(target string, kind string, interval time.Duration) {
	key := notificationKey{target: target, kind: kind}
	now := time.Now()
	n.mu.Lock()
	if now.Sub(n.lastByKey[key]) < interval {
		n.mu.Unlock()
		return
	}
	for oldKey, sentAt := range n.lastByKey {
		if now.Sub(sentAt) >= time.Minute {
			delete(n.lastByKey, oldKey)
		}
	}
	n.lastByKey[key] = now
	n.mu.Unlock()
	n.notifications.Add(1)
	go n.sendNotification(target, kind)
}

func (n *Notifier) sendNotification(target string, kind string) {
	defer n.notifications.Done()
	if err := n.post(target, kind); err != nil {
		n.logger.Warn("worker wake notification failed", tag.Address(target), tag.Error(err))
	}
}

func (n *Notifier) post(target string, kind string) error {
	body, err := json.Marshal(map[string]string{"target": target, "kind": kind})
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, n.cfg.URL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	if n.token != nil {
		if token := n.token.Token(); token != "" {
			request.Header.Set("Authorization", "Bearer "+token)
		}
	}
	response, err := n.httpClient.Do(request)
	if err != nil {
		return err
	}
	if err := response.Body.Close(); err != nil {
		return err
	}
	if response.StatusCode/100 != 2 {
		return fmt.Errorf("wake URL answered HTTP %d", response.StatusCode)
	}
	return nil
}

// Close waits for in-flight notifications.
func (n *Notifier) Close() { n.notifications.Wait() }

func isWorkerUnavailable(dispatchErr error) bool {
	var noAddress interface{ IsNoWorkerAddress() bool }
	if errors.As(dispatchErr, &noAddress) && noAddress.IsNoWorkerAddress() {
		return true
	}
	return status.Code(dispatchErr) == codes.Unavailable
}
