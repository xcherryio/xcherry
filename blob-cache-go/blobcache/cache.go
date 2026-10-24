// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

// This file is the cache's orchestration layer. Ristretto holds only entry
// metadata while fileStore owns payload bytes, and lifecycle locks coordinate
// reads, writes, purges, and shutdown. Put reserves the complete logical file
// cost before committing, allowing eviction callbacks to remove disk victims
// before new bytes appear. For example, TinyLFU rejection or failed victim
// cleanup leaves no final candidate file, while an admitted entry becomes
// readable only after its atomic disk commit succeeds.

package blobcache

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"

	"github.com/dgraph-io/ristretto/v2"
)

// Cache stores immutable blob payloads on disk under a Ristretto policy.
type Cache struct {
	cfg    *Config
	store  fileStore
	policy *ristretto.Cache[string, *diskEntry]
	logger Logger

	lifecycleMu sync.RWMutex
	commitMu    sync.Mutex
	callbackMu  sync.Mutex
	pendingMu   sync.Mutex
	pendingCond *sync.Cond
	pendingKeys map[string]struct{}

	callbackErr    error
	cleanupBacklog map[string]struct{}
	purgeRequired  bool
	closing        atomic.Bool
	closed         atomic.Bool
}

// New constructs and reconciles a disk blob cache.
// It returns a ready cache, or an error when initialization cannot establish storage invariants.
func New(cfg *Config) (*Cache, error) {
	if cfg == nil {
		panic("blobcache.New requires Config")
	}
	if err := validateConfig(cfg); err != nil {
		return nil, err
	}
	store, err := newLocalFileStore(cfg.Dir)
	if err != nil {
		return nil, err
	}
	return newCache(cfg, store)
}

func newCache(cfg *Config, store fileStore) (*Cache, error) {
	if err := store.prepare(); err != nil {
		return nil, err
	}
	cache := &Cache{
		cfg:            cfg,
		store:          store,
		logger:         defaultLogger(cfg.Logger),
		cleanupBacklog: make(map[string]struct{}),
		pendingKeys:    make(map[string]struct{}),
	}
	cache.pendingCond = sync.NewCond(&cache.pendingMu)
	if err := cache.initializePolicy(); err != nil {
		return nil, err
	}
	if err := cache.recover(); err != nil {
		cache.closePolicyPreservingFiles()
		return nil, err
	}
	return cache, nil
}

func (cache *Cache) initializePolicy() error {
	policy, err := ristretto.NewCache(&ristretto.Config[string, *diskEntry]{
		NumCounters:        cache.cfg.FrequencyCounters,
		MaxCost:            cache.cfg.MaxBytes,
		BufferItems:        defaultBufferItems,
		IgnoreInternalCost: true,
		OnEvict:            cache.handlePolicyEviction,
		OnReject:           cache.handlePolicyRejection,
	})
	if err != nil {
		return fmt.Errorf("create Ristretto policy: %w", err)
	}
	cache.policy = policy
	return nil
}

func (cache *Cache) handlePolicyEviction(item *ristretto.Item[*diskEntry]) {
	// One Ristretto admission can report the same victim twice, because its
	// eviction sample is refilled without excluding keys it already holds. The
	// repeated report carries no entry: the first report already removed it.
	if item.Value == nil {
		return
	}
	cache.handlePolicyRemoval(item.Value)
}

func (cache *Cache) handlePolicyRejection(item *ristretto.Item[*diskEntry]) {
	cache.handlePolicyRemoval(item.Value)
}

func (cache *Cache) handlePolicyRemoval(entry *diskEntry) {
	if cache.closing.Load() {
		return
	}
	if !entry.beginEviction() {
		return
	}
	if err := cache.store.remove(entry.path); err != nil {
		cache.recordCallbackError(entry.path, err)
	}
}

// Get returns a validated payload and records its policy access.
// A hit returns a new payload slice and true. A normal miss returns nil, false, nil. Validation, lifecycle, or I/O failures return an error.
func (cache *Cache) Get(blobID string) ([]byte, bool, error) {
	if err := validateBlobID(blobID); err != nil {
		return nil, false, err
	}

	cache.lifecycleMu.RLock()
	defer cache.lifecycleMu.RUnlock()
	if cache.closed.Load() {
		return nil, false, ErrClosed
	}

	entry, found := cache.policy.Get(blobID)
	if !found {
		return nil, false, nil
	}
	if !entry.acquireRead() {
		return nil, false, nil
	}
	payload, err := cache.store.read(entry)
	entry.releaseRead()
	if err == nil {
		return payload, true, nil
	}
	if !errors.Is(err, os.ErrNotExist) && !errors.Is(err, ErrCorrupt) {
		return nil, false, err
	}

	invalidateErr := cache.invalidateEntry(entry)
	if invalidateErr != nil {
		return nil, false, errors.Join(err, invalidateErr)
	}
	return nil, false, nil
}

// Put attempts to admit and atomically commit an immutable blob.
// It returns true when the blob is stored or identical content already exists. Oversized or policy-rejected blobs return false, nil. Validation, mismatch, storage, or reconciliation failures return an error.
func (cache *Cache) Put(blobID string, payload []byte) (bool, error) {
	if err := validateBlobID(blobID); err != nil {
		return false, err
	}

	cache.lifecycleMu.RLock()
	defer cache.lifecycleMu.RUnlock()
	if cache.closed.Load() {
		return false, ErrClosed
	}

	metadata, err := calculateMetadata(blobID, payload, cache.store.pathFor(blobID))
	if err != nil {
		return false, err
	}
	if metadata.size > cache.cfg.MaxBytes {
		return false, nil
	}
	cache.reservePut(blobID)
	defer cache.releasePut(blobID)

	entry, completed, accepted, err := cache.admitPut(metadata, payload)
	if completed {
		return accepted, err
	}

	tempPath, stageResult, stageErr := cache.store.stage(metadata, payload)
	cache.commitMu.Lock()
	defer cache.commitMu.Unlock()
	if stageErr != nil {
		if stageResult.orphanPath != "" {
			cache.addCleanupPath(stageResult.orphanPath)
		}
		cache.removeCandidate(entry)
		return false, stageErr
	}
	admitted, found := cache.policy.Get(blobID)
	if !found || admitted != entry {
		entry.beginEviction()
		if err := cache.store.remove(tempPath); err != nil {
			cache.addCleanupPath(tempPath)
			return false, err
		}
		return false, nil
	}
	commitResult, err := cache.store.publish(metadata, tempPath)
	if err != nil {
		if commitResult.orphanPath != "" {
			cache.addCleanupPath(commitResult.orphanPath)
		}
		cache.removeCandidate(entry)
		return false, err
	}
	if !entry.markReady() {
		panic("admitted blob cache entry left pending state")
	}
	return true, nil
}

func (cache *Cache) admitPut(metadata fileMetadata, payload []byte) (*diskEntry, bool, bool, error) {
	cache.commitMu.Lock()
	defer cache.commitMu.Unlock()
	if err := cache.retryCleanup(); err != nil {
		return nil, true, false, err
	}

	reused, found, err := cache.reuseExisting(metadata, payload)
	if err != nil {
		return nil, true, false, err
	}
	if found {
		return nil, true, reused, nil
	}

	entry := newPendingEntry(metadata)
	cache.resetCallbackError()
	if !cache.policy.Set(metadata.blobID, entry, metadata.size) {
		entry.beginEviction()
		return nil, true, false, nil
	}
	cache.policy.Wait()

	callbackErr := cache.takeCallbackError()
	if callbackErr != nil {
		cache.removeCandidate(entry)
		return nil, true, false, fmt.Errorf("%w: %v", ErrReconciliation, callbackErr)
	}
	admitted, found := cache.policy.Get(metadata.blobID)
	if !found || admitted != entry {
		entry.beginEviction()
		return nil, true, false, nil
	}
	return entry, false, false, nil
}

func (cache *Cache) reservePut(blobID string) {
	cache.pendingMu.Lock()
	defer cache.pendingMu.Unlock()
	for {
		if _, found := cache.pendingKeys[blobID]; !found {
			cache.pendingKeys[blobID] = struct{}{}
			return
		}
		cache.pendingCond.Wait()
	}
}

func (cache *Cache) releasePut(blobID string) {
	cache.pendingMu.Lock()
	delete(cache.pendingKeys, blobID)
	cache.pendingCond.Broadcast()
	cache.pendingMu.Unlock()
}

// Delete removes one cached blob without deleting source data.
// It returns nil when the blob is removed or absent, and an error for invalid IDs, closed caches, or storage failures.
func (cache *Cache) Delete(blobID string) error {
	if err := validateBlobID(blobID); err != nil {
		return err
	}

	cache.lifecycleMu.RLock()
	defer cache.lifecycleMu.RUnlock()
	if cache.closed.Load() {
		return ErrClosed
	}

	cache.commitMu.Lock()
	defer cache.commitMu.Unlock()
	if err := cache.retryCleanup(); err != nil {
		return err
	}

	entry, found := cache.policy.Get(blobID)
	if !found {
		path := cache.store.pathFor(blobID)
		if err := cache.store.remove(path); err != nil {
			cache.addCleanupPath(path)
			return err
		}
		return nil
	}

	entry.beginEviction()
	if err := cache.store.remove(entry.path); err != nil {
		entry.restoreReady()
		return err
	}
	cache.policy.Del(blobID)
	cache.policy.Wait()
	return nil
}

// DeleteAll purges the cache while leaving it open and reusable.
// It returns nil after a complete purge, or an error when the cache is closed or storage cleanup fails.
func (cache *Cache) DeleteAll() error {
	cache.lifecycleMu.Lock()
	defer cache.lifecycleMu.Unlock()
	if cache.closed.Load() {
		return ErrClosed
	}

	cache.commitMu.Lock()
	defer cache.commitMu.Unlock()

	cache.resetCallbackError()
	cache.policy.Clear()
	callbackErr := cache.takeCallbackError()
	if err := cache.store.purge(); err != nil {
		cache.callbackMu.Lock()
		cache.purgeRequired = true
		cache.callbackMu.Unlock()
		return errors.Join(callbackErr, err)
	}

	cache.callbackMu.Lock()
	cache.cleanupBacklog = make(map[string]struct{})
	cache.purgeRequired = false
	cache.callbackMu.Unlock()
	return nil
}

// Close releases in-memory resources and preserves committed files.
// It returns nil when closed, including repeated calls. Cleanup failures are returned after in-memory shutdown completes.
func (cache *Cache) Close() error {
	cache.lifecycleMu.Lock()
	defer cache.lifecycleMu.Unlock()
	if cache.closed.Load() {
		return nil
	}

	cache.commitMu.Lock()
	defer cache.commitMu.Unlock()
	cleanupErr := cache.retryCleanup()
	cache.closePolicyPreservingFiles()
	return cleanupErr
}

func (cache *Cache) recover() error {
	cache.commitMu.Lock()
	defer cache.commitMu.Unlock()

	if err := cache.store.purgeTemp(); err != nil {
		return fmt.Errorf("%w: %v", ErrReconciliation, err)
	}
	scanned, err := cache.store.scan()
	if err != nil {
		return fmt.Errorf("%w: %v", ErrReconciliation, err)
	}
	for _, invalidPath := range scanned.invalidPaths {
		if err := cache.store.remove(invalidPath); err != nil {
			return fmt.Errorf("%w: %v", ErrReconciliation, err)
		}
	}
	for _, metadata := range scanned.entries {
		if err := cache.recoverEntry(metadata); err != nil {
			return err
		}
	}
	return nil
}

func (cache *Cache) recoverEntry(metadata fileMetadata) error {
	if metadata.size > cache.cfg.MaxBytes {
		if err := cache.store.remove(metadata.path); err != nil {
			return fmt.Errorf("%w: %v", ErrReconciliation, err)
		}
		return nil
	}

	entry := newReadyEntry(metadata)
	cache.resetCallbackError()
	if !cache.policy.Set(metadata.blobID, entry, metadata.size) {
		if err := cache.store.remove(metadata.path); err != nil {
			return fmt.Errorf("%w: %v", ErrReconciliation, err)
		}
		return nil
	}
	cache.policy.Wait()
	if callbackErr := cache.takeCallbackError(); callbackErr != nil {
		return fmt.Errorf("%w: %v", ErrReconciliation, callbackErr)
	}

	admitted, found := cache.policy.Get(metadata.blobID)
	if found && admitted == entry {
		return nil
	}
	if err := cache.store.remove(metadata.path); err != nil {
		return fmt.Errorf("%w: %v", ErrReconciliation, err)
	}
	return nil
}

func (cache *Cache) reuseExisting(
	metadata fileMetadata,
	payload []byte,
) (bool, bool, error) {
	entry, found := cache.policy.Get(metadata.blobID)
	if !found {
		return false, false, nil
	}
	if !entry.acquireRead() {
		return false, false, nil
	}
	existingPayload, err := cache.store.read(entry)
	entry.releaseRead()
	if err == nil {
		if entry.size != metadata.size ||
			entry.checksum != metadata.checksum ||
			!bytes.Equal(existingPayload, payload) {
			return false, true, fmt.Errorf("%w: blob %q", ErrContentMismatch, metadata.blobID)
		}
		return true, true, nil
	}
	if !errors.Is(err, os.ErrNotExist) && !errors.Is(err, ErrCorrupt) {
		return false, true, err
	}

	entry.beginEviction()
	removeErr := cache.store.remove(entry.path)
	cache.policy.Del(metadata.blobID)
	cache.policy.Wait()
	if removeErr != nil {
		cache.addCleanupPath(entry.path)
		return false, true, removeErr
	}
	return false, false, nil
}

func (cache *Cache) invalidateEntry(entry *diskEntry) error {
	cache.commitMu.Lock()
	defer cache.commitMu.Unlock()

	current, found := cache.policy.Get(entry.blobID)
	if !found || current != entry {
		return nil
	}
	entry.beginEviction()
	removeErr := cache.store.remove(entry.path)
	cache.policy.Del(entry.blobID)
	cache.policy.Wait()
	if removeErr != nil {
		cache.addCleanupPath(entry.path)
		return removeErr
	}
	return nil
}

func (cache *Cache) removeCandidate(entry *diskEntry) {
	current, found := cache.policy.Get(entry.blobID)
	if found && current == entry {
		cache.policy.Del(entry.blobID)
		cache.policy.Wait()
	}
	entry.beginEviction()
}

func (cache *Cache) closePolicyPreservingFiles() {
	cache.closing.Store(true)
	cache.policy.Wait()
	cache.policy.Close()
	cache.closed.Store(true)
}

func (cache *Cache) resetCallbackError() {
	cache.callbackMu.Lock()
	cache.callbackErr = nil
	cache.callbackMu.Unlock()
}

func (cache *Cache) recordCallbackError(path string, err error) {
	cache.callbackMu.Lock()
	cache.callbackErr = errors.Join(cache.callbackErr, err)
	cache.cleanupBacklog[path] = struct{}{}
	cache.callbackMu.Unlock()
}

func (cache *Cache) takeCallbackError() error {
	cache.callbackMu.Lock()
	defer cache.callbackMu.Unlock()

	err := cache.callbackErr
	cache.callbackErr = nil
	return err
}

func (cache *Cache) addCleanupPath(path string) {
	cache.callbackMu.Lock()
	cache.cleanupBacklog[path] = struct{}{}
	cache.callbackMu.Unlock()
}

func (cache *Cache) retryCleanup() error {
	cache.callbackMu.Lock()
	defer cache.callbackMu.Unlock()

	if cache.purgeRequired {
		if err := cache.store.purge(); err != nil {
			return fmt.Errorf("%w: %v", ErrReconciliation, err)
		}
		cache.purgeRequired = false
		cache.cleanupBacklog = make(map[string]struct{})
		return nil
	}

	var cleanupErr error
	for path := range cache.cleanupBacklog {
		if err := cache.store.remove(path); err != nil {
			cleanupErr = errors.Join(cleanupErr, err)
			continue
		}
		delete(cache.cleanupBacklog, path)
	}
	if cleanupErr != nil {
		return fmt.Errorf("%w: %v", ErrReconciliation, cleanupErr)
	}
	return nil
}

// Logger returns the configured logger.
func (cache *Cache) Logger() Logger {
	return cache.logger
}
