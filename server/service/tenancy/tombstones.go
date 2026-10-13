// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package tenancy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/superdurable/dex/config"
	"github.com/superdurable/dex/service/common/log"
	"github.com/superdurable/dex/service/common/log/tag"
)

const (
	maxTombstoneUpdateAttempts = 10
	tombstoneReadTimeout       = 10 * time.Second
)

// ErrTombstoneRecordConflict is returned by a TenancyRecordStore whose conditional write lost to another writer.
var ErrTombstoneRecordConflict = errors.New("tombstone record changed concurrently")

// TenancyRecordStore reads and conditionally writes the one tombstone object of a namespace.
type TenancyRecordStore interface {
	ReadTenancyRecord(ctx context.Context) ([]byte, string, error)
	WriteTenancyRecord(ctx context.Context, data []byte, version string) error
}

// TenantTombstone is the persisted state of one tombstoned tenant.
type TenantTombstone struct {
	TombstonedAt time.Time  `json:"tombstonedAt"`
	FirstEmptyAt *time.Time `json:"firstEmptyAt,omitempty"`
	PurgeDoneAt  *time.Time `json:"purgeDoneAt,omitempty"`
}

// TombstoneRecord maps tenant IDs to their tombstones.
type TombstoneRecord struct {
	Tenants map[string]*TenantTombstone `json:"tenants"`
}

// TombstoneRegistry caches the tombstoned tenants, rereading the record every refresh interval, and applies
// conditional updates. Every replica reads one object whatever the number of tenants.
type TombstoneRegistry struct {
	store       TenancyRecordStore
	retention   time.Duration
	isConflict  func(error) bool
	logger      log.Logger
	stop        chan struct{}
	stopped     sync.WaitGroup
	mu          sync.RWMutex
	tombstoned  map[string]struct{}
	isCloseDone bool
}

// NewTombstoneRegistry loads the record once and starts the refresh loop; isConflict recognizes lost writes.
func NewTombstoneRegistry(
	cfg *config.TenancyConfig,
	store TenancyRecordStore,
	isConflict func(error) bool,
	logger log.Logger,
) (*TombstoneRegistry, error) {
	if cfg == nil || store == nil || isConflict == nil || logger == nil {
		panic("tombstone registry dependencies must not be nil")
	}
	registry := &TombstoneRegistry{
		store:      store,
		retention:  cfg.EffectiveTombstoneRetention(),
		isConflict: isConflict,
		logger:     logger,
		stop:       make(chan struct{}),
		tombstoned: map[string]struct{}{},
	}
	ctx, cancel := context.WithTimeout(context.Background(), tombstoneReadTimeout)
	defer cancel()
	if err := registry.refresh(ctx); err != nil {
		return nil, fmt.Errorf("load tenant tombstones: %w", err)
	}
	registry.stopped.Add(1)
	go registry.refreshLoop(cfg.EffectiveTombstoneRefreshInterval())
	return registry, nil
}

// IsTombstoned reports whether the tenant's credentials must be refused.
func (r *TombstoneRegistry) IsTombstoned(tenantID string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, isTombstoned := r.tombstoned[tenantID]
	return isTombstoned
}

// Read returns the current record from the store.
func (r *TombstoneRegistry) Read(ctx context.Context) (*TombstoneRecord, error) {
	record, _, err := r.read(ctx)
	return record, err
}

// Update applies mutate to the current record and writes it conditionally, retrying when another writer won.
// Tombstones whose retention passed are removed in the same write.
func (r *TombstoneRegistry) Update(
	ctx context.Context,
	mutate func(*TombstoneRecord, time.Time) error,
) (*TombstoneRecord, error) {
	for attempt := 0; attempt < maxTombstoneUpdateAttempts; attempt++ {
		record, version, err := r.read(ctx)
		if err != nil {
			return nil, err
		}
		now := time.Now().UTC()
		if err := mutate(record, now); err != nil {
			return nil, err
		}
		r.removeExpired(record, now)
		data, err := json.Marshal(record)
		if err != nil {
			return nil, err
		}
		err = r.store.WriteTenancyRecord(ctx, data, version)
		if r.isConflict(err) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("write tenant tombstones: %w", err)
		}
		r.apply(record)
		return record, nil
	}
	return nil, fmt.Errorf("write tenant tombstones: %w", ErrTombstoneRecordConflict)
}

// TombstoneExpiry returns when a tombstone is removed, or false while its purge is not done.
func (r *TombstoneRegistry) TombstoneExpiry(tombstone *TenantTombstone) (time.Time, bool) {
	if tombstone.PurgeDoneAt == nil {
		return time.Time{}, false
	}
	return tombstone.PurgeDoneAt.Add(r.retention), true
}

func (r *TombstoneRegistry) Close() {
	r.mu.Lock()
	if r.isCloseDone {
		r.mu.Unlock()
		return
	}
	r.isCloseDone = true
	r.mu.Unlock()
	close(r.stop)
	r.stopped.Wait()
}

func (r *TombstoneRegistry) refreshLoop(interval time.Duration) {
	defer r.stopped.Done()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-r.stop:
			return
		case <-ticker.C:
		}
		ctx, cancel := context.WithTimeout(context.Background(), tombstoneReadTimeout)
		if err := r.refresh(ctx); err != nil {
			r.logger.Error("refresh tenant tombstones; keeping the previous set", tag.Error(err))
		}
		cancel()
	}
}

// refresh rereads the record, and removes expired tombstones when there are any.
func (r *TombstoneRegistry) refresh(ctx context.Context) error {
	record, _, err := r.read(ctx)
	if err != nil {
		return err
	}
	if r.hasExpired(record, time.Now().UTC()) {
		_, err = r.Update(ctx, func(*TombstoneRecord, time.Time) error { return nil })
		return err
	}
	r.apply(record)
	return nil
}

func (r *TombstoneRegistry) read(ctx context.Context) (*TombstoneRecord, string, error) {
	data, version, err := r.store.ReadTenancyRecord(ctx)
	if err != nil {
		return nil, "", fmt.Errorf("read tenant tombstones: %w", err)
	}
	record := &TombstoneRecord{Tenants: map[string]*TenantTombstone{}}
	if len(data) == 0 {
		return record, version, nil
	}
	if err := json.Unmarshal(data, record); err != nil {
		return nil, "", fmt.Errorf("decode tenant tombstones: %w", err)
	}
	if record.Tenants == nil {
		record.Tenants = map[string]*TenantTombstone{}
	}
	return record, version, nil
}

func (r *TombstoneRegistry) apply(record *TombstoneRecord) {
	tombstoned := make(map[string]struct{}, len(record.Tenants))
	for tenantID := range record.Tenants {
		tombstoned[tenantID] = struct{}{}
	}
	r.mu.Lock()
	r.tombstoned = tombstoned
	r.mu.Unlock()
}

func (r *TombstoneRegistry) hasExpired(record *TombstoneRecord, now time.Time) bool {
	for _, tombstone := range record.Tenants {
		if expiry, isDone := r.TombstoneExpiry(tombstone); isDone && !now.Before(expiry) {
			return true
		}
	}
	return false
}

func (r *TombstoneRegistry) removeExpired(record *TombstoneRecord, now time.Time) {
	for tenantID, tombstone := range record.Tenants {
		if expiry, isDone := r.TombstoneExpiry(tombstone); isDone && !now.Before(expiry) {
			delete(record.Tenants, tenantID)
		}
	}
}
