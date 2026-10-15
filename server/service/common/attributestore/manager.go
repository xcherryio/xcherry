// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package attributestore

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/superdurable/dex/config"
	"github.com/superdurable/dex/gen/dexpb"
	"github.com/superdurable/dex/service/common/log"
	"github.com/superdurable/dex/service/common/log/tag"
	"golang.org/x/sync/errgroup"
)

type Manager struct {
	cfg     *config.AttributeStoreConfig
	logger  log.Logger
	entries map[string]store
	cancel  context.CancelFunc
	wg      sync.WaitGroup
}

type store interface {
	writeBatch(context.Context, string, []*dexpb.AttributeSyncItem) error
	close() error
}

type schemaRefreshingStore interface {
	refreshSchema(context.Context) error
}

type secretRedactedError struct {
	source error
	secret string
}

func NewManager(
	ctx context.Context,
	cfg *config.AttributeStoreConfig,
	logger log.Logger,
) (*Manager, error) {
	if cfg == nil || logger == nil {
		panic("Attribute Store Manager requires config and logger")
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	managerCtx, cancel := context.WithCancel(ctx)
	manager := &Manager{
		cfg:     cfg,
		logger:  logger,
		entries: make(map[string]store, len(cfg.Stores)),
		cancel:  cancel,
	}
	if err := manager.openEntries(managerCtx); err != nil {
		cancel()
		if closeErr := manager.closeStores(); closeErr != nil {
			return nil, errors.Join(err, closeErr)
		}
		return nil, err
	}
	manager.startRefreshers(managerCtx)
	return manager, nil
}

func (m *Manager) openEntries(ctx context.Context) error {
	group, groupCtx := errgroup.WithContext(ctx)
	type namedStore struct {
		name  string
		store store
	}
	opened := make(chan namedStore, len(m.cfg.Stores))
	for name, entryCfg := range m.cfg.Stores {
		name := name
		entryCfg := entryCfg
		group.Go(func() error {
			entry, err := openStore(groupCtx, entryCfg, m.logger.WithTags(tag.AttributeStore(name)))
			if err != nil {
				return fmt.Errorf("initialize Attribute Store %q: %w", name, err)
			}
			opened <- namedStore{name: name, store: entry}
			return nil
		})
	}
	err := group.Wait()
	close(opened)
	for entry := range opened {
		m.entries[entry.name] = entry.store
	}
	return err
}

func openStore(ctx context.Context, cfg config.AttributeStoreConfigEntry, logger log.Logger) (store, error) {
	var (
		entry store
		err   error
	)
	if cfg.Type == config.AttributeStoreTypeMongoDB {
		entry, err = openMongoStore(ctx, cfg, logger)
	} else {
		entry, err = openSQLStore(ctx, cfg, logger)
	}
	if err != nil {
		return nil, redactStorageSecret(err, cfg.DSN)
	}
	return entry, nil
}

func redactStorageSecret(err error, secret string) error {
	if err == nil || secret == "" || !strings.Contains(err.Error(), secret) {
		return err
	}
	return &secretRedactedError{source: err, secret: secret}
}

func (e *secretRedactedError) Error() string {
	return strings.ReplaceAll(e.source.Error(), e.secret, "[REDACTED]")
}

func (e *secretRedactedError) Unwrap() error {
	return e.source
}

func (m *Manager) startRefreshers(ctx context.Context) {
	for name, entry := range m.entries {
		refresher, refreshesSchema := entry.(schemaRefreshingStore)
		if !refreshesSchema {
			continue
		}
		m.wg.Add(1)
		go func() {
			defer m.wg.Done()
			m.refreshLoop(ctx, name, refresher)
		}()
	}
}

func (m *Manager) refreshLoop(ctx context.Context, name string, entry schemaRefreshingStore) {
	random := rand.New(rand.NewSource(time.Now().UnixNano()))
	for {
		timer := time.NewTimer(jitterInterval(m.cfg.EffectiveSchemaSyncInterval(), random.Float64()))
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return
		case <-timer.C:
		}
		if err := entry.refreshSchema(ctx); err != nil {
			m.logger.Error("refresh Attribute Store schema", tag.AttributeStore(name), tag.Error(err))
		}
	}
}

func jitterInterval(interval time.Duration, sample float64) time.Duration {
	return time.Duration(float64(interval) * (0.9 + sample*0.2))
}

func (m *Manager) HasStore(name string) bool {
	_, found := m.entries[name]
	return found
}

func (m *Manager) WriteBatch(ctx context.Context, input *dexpb.SyncAttributeBatchActivityInput) error {
	if input == nil || input.GetFlowId() == "" || input.GetConfigName() == "" {
		return fmt.Errorf("Attribute Store batch requires FlowID and config name")
	}
	entry, found := m.entries[input.GetConfigName()]
	if !found {
		return fmt.Errorf("Attribute Store %q is unavailable", input.GetConfigName())
	}
	return entry.writeBatch(ctx, input.GetFlowId(), input.GetItems())
}

func (m *Manager) Close() error {
	m.cancel()
	m.wg.Wait()
	return m.closeStores()
}

func (m *Manager) closeStores() error {
	var errs []error
	for name, entry := range m.entries {
		if err := entry.close(); err != nil {
			errs = append(errs, fmt.Errorf("close Attribute Store %q: %w", name, err))
		}
	}
	return errors.Join(errs...)
}
