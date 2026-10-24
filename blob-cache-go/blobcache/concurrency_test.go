// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package blobcache

import (
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestConcurrentGetPutAndDeleteAll(t *testing.T) {
	cache, rootDir := newTestCache(t, 4096)
	require.True(t, putForTest(t, cache, "shared", []byte("initial")))

	var waitGroup sync.WaitGroup
	errorChannel := make(chan error, 128)
	for workerIndex := 0; workerIndex < 8; workerIndex++ {
		waitGroup.Add(1)
		go runCacheWorker(cache, workerIndex, &waitGroup, errorChannel)
	}
	for purgeIndex := 0; purgeIndex < 5; purgeIndex++ {
		waitGroup.Add(1)
		go runCachePurge(cache, &waitGroup, errorChannel)
	}
	waitGroup.Wait()
	close(errorChannel)
	for err := range errorChannel {
		require.NoError(t, err)
	}
	require.LessOrEqual(t, cacheOwnedBytes(t, rootDir), cache.cfg.MaxBytes)
}

func TestConcurrentSameIDPutsCommitOneFile(t *testing.T) {
	cache, rootDir := newTestCache(t, 1<<20)
	var waitGroup sync.WaitGroup
	errorChannel := make(chan error, 32)

	for workerIndex := 0; workerIndex < 32; workerIndex++ {
		waitGroup.Add(1)
		go runSameIDPut(cache, &waitGroup, errorChannel)
	}
	waitGroup.Wait()
	close(errorChannel)
	for err := range errorChannel {
		require.NoError(t, err)
	}
	require.Len(t, regularFiles(t, filepath.Join(rootDir, "blobs")), 1)
}

func TestConcurrentDifferentIDPutsCommitFiles(t *testing.T) {
	cache, rootDir := newTestCache(t, 1<<20)
	const workerCount = 16
	start := make(chan struct{})
	errorChannel := make(chan error, workerCount)
	var waitGroup sync.WaitGroup
	for workerIndex := 0; workerIndex < workerCount; workerIndex++ {
		waitGroup.Add(1)
		go runDistinctIDPut(cache, workerIndex, start, &waitGroup, errorChannel)
	}
	close(start)
	waitGroup.Wait()
	close(errorChannel)
	for err := range errorChannel {
		require.NoError(t, err)
	}
	for workerIndex := 0; workerIndex < workerCount; workerIndex++ {
		blobID := "distinct-" + strconv.Itoa(workerIndex)
		payload, found, err := cache.Get(blobID)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, []byte("payload"), payload)
	}
	require.Len(t, regularFiles(t, filepath.Join(rootDir, "blobs")), workerCount)
}

func BenchmarkCachePut4KiB(b *testing.B) {
	benchmarkCachePut4KiB(b, false)
}

func BenchmarkCachePut4KiBParallel(b *testing.B) {
	benchmarkCachePut4KiB(b, true)
}

func benchmarkCachePut4KiB(b *testing.B, isParallel bool) {
	cache, err := New(&Config{Dir: b.TempDir(), MaxBytes: 1 << 40, FrequencyCounters: 100_000})
	require.NoError(b, err)
	b.Cleanup(func() {
		require.NoError(b, cache.Close())
	})
	payload := make([]byte, 4096)
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	if isParallel {
		var nextID atomic.Uint64
		b.RunParallel(func(worker *testing.PB) {
			for worker.Next() {
				blobID := "benchmark-" + strconv.FormatUint(nextID.Add(1), 10)
				cached, putErr := cache.Put(blobID, payload)
				if putErr != nil || !cached {
					b.Fatalf("put %s: cached=%t, error=%v", blobID, cached, putErr)
				}
			}
		})
		return
	}
	for index := 0; index < b.N; index++ {
		blobID := "benchmark-" + strconv.Itoa(index)
		cached, putErr := cache.Put(blobID, payload)
		if putErr != nil || !cached {
			b.Fatalf("put %s: cached=%t, error=%v", blobID, cached, putErr)
		}
	}
}

func TestDeleteWaitsForActiveReader(t *testing.T) {
	rootDir := t.TempDir()
	cfg := &Config{Dir: rootDir, MaxBytes: 1 << 20, FrequencyCounters: 10_000}
	baseStore, err := newLocalFileStore(rootDir)
	require.NoError(t, err)
	blockingStore := &blockingReadStore{
		fileStore:    baseStore,
		readStarted:  make(chan struct{}),
		releaseRead:  make(chan struct{}),
		removeCalled: make(chan struct{}, 1),
	}
	cache, err := newCache(cfg, blockingStore)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, cache.Close())
	})
	require.True(t, putForTest(t, cache, "leased", []byte("payload")))

	getDone := make(chan error, 1)
	go runCacheGet(cache, "leased", getDone)
	<-blockingStore.readStarted

	deleteDone := make(chan error, 1)
	go runCacheDelete(cache, "leased", deleteDone)

	probe := evictionProbe{cache: cache, blobID: "leased"}
	require.Eventually(t, probe.evicted, testEventuallyTimeout, testEventuallyInterval)

	select {
	case <-blockingStore.removeCalled:
		t.Fatal("file removal started before the active reader finished")
	default:
	}
	close(blockingStore.releaseRead)
	require.NoError(t, <-getDone)
	require.NoError(t, <-deleteDone)
	require.Error(t, receiveRemoveSignal(blockingStore.removeCalled))
}

func TestDeleteFailureKeepsEntryReadable(t *testing.T) {
	rootDir := t.TempDir()
	cfg := &Config{Dir: rootDir, MaxBytes: 1 << 20, FrequencyCounters: 10_000}
	baseStore, err := newLocalFileStore(rootDir)
	require.NoError(t, err)
	failingStore := &removeFailureStore{fileStore: baseStore}
	cache, err := newCache(cfg, failingStore)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, cache.Close())
	})
	require.True(t, putForTest(t, cache, "kept", []byte("payload")))

	failingStore.setFailNextRemove()
	require.Error(t, cache.Delete("kept"))
	payload, found, err := cache.Get("kept")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, []byte("payload"), payload)
}

func TestEvictionFailureAbortsCommitAndRetriesCleanup(t *testing.T) {
	rootDir := t.TempDir()
	const payloadSize = 32
	maxBytes := int64(fixedHeaderSize + len("first1") + payloadSize)
	cfg := &Config{Dir: rootDir, MaxBytes: maxBytes, FrequencyCounters: 10_000}
	baseStore, err := newLocalFileStore(rootDir)
	require.NoError(t, err)
	failingStore := &removeFailureStore{fileStore: baseStore}
	cache, err := newCache(cfg, failingStore)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, cache.Close())
	})

	require.True(t, putForTest(t, cache, "first1", make([]byte, payloadSize)))
	failingStore.setFailNextRemove()
	cached, err := cache.Put("second", make([]byte, payloadSize))
	require.ErrorIs(t, err, ErrReconciliation)
	require.False(t, cached)
	require.LessOrEqual(t, cacheOwnedBytes(t, rootDir), maxBytes)

	cached, err = cache.Put("third3", make([]byte, payloadSize))
	require.NoError(t, err)
	require.True(t, cached)
	require.LessOrEqual(t, cacheOwnedBytes(t, rootDir), maxBytes)
}

func TestDeleteAllFailureIsRetriedBeforePut(t *testing.T) {
	rootDir := t.TempDir()
	cfg := &Config{Dir: rootDir, MaxBytes: 1 << 20, FrequencyCounters: 10_000}
	baseStore, err := newLocalFileStore(rootDir)
	require.NoError(t, err)
	failingStore := &purgeFailureStore{fileStore: baseStore}
	cache, err := newCache(cfg, failingStore)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, cache.Close())
	})

	require.True(t, putForTest(t, cache, "before", []byte("payload")))
	failingStore.setFailNextPurge()
	require.Error(t, cache.DeleteAll())

	cached, err := cache.Put("after", []byte("fresh"))
	require.NoError(t, err)
	require.True(t, cached)
	_, found, err := cache.Get("before")
	require.NoError(t, err)
	require.False(t, found)
}

func runCacheWorker(
	cache *Cache,
	worker int,
	waitGroup *sync.WaitGroup,
	errorChannel chan<- error,
) {
	defer waitGroup.Done()
	for iteration := 0; iteration < 50; iteration++ {
		blobID := "worker-" + string(rune('a'+worker)) + "-" + string(rune('A'+iteration))
		cached, err := cache.Put(blobID, []byte{byte(iteration)})
		if err != nil {
			errorChannel <- err
			return
		}
		if cached {
			_, _, err = cache.Get(blobID)
			if err != nil {
				errorChannel <- err
				return
			}
		}
	}
}

func runCachePurge(cache *Cache, waitGroup *sync.WaitGroup, errorChannel chan<- error) {
	defer waitGroup.Done()
	if err := cache.DeleteAll(); err != nil {
		errorChannel <- err
	}
}

func runSameIDPut(cache *Cache, waitGroup *sync.WaitGroup, errorChannel chan<- error) {
	defer waitGroup.Done()
	cached, err := cache.Put("same-id", []byte("same-payload"))
	if err != nil {
		errorChannel <- err
		return
	}
	if !cached {
		errorChannel <- errors.New("same-ID put was not cached")
	}
}

func runDistinctIDPut(
	cache *Cache,
	workerIndex int,
	start <-chan struct{},
	waitGroup *sync.WaitGroup,
	errorChannel chan<- error,
) {
	defer waitGroup.Done()
	<-start
	blobID := "distinct-" + strconv.Itoa(workerIndex)
	cached, err := cache.Put(blobID, []byte("payload"))
	if err != nil {
		errorChannel <- err
		return
	}
	if !cached {
		errorChannel <- fmt.Errorf("different-ID put %q was not cached", blobID)
	}
}

func runCacheGet(cache *Cache, blobID string, done chan<- error) {
	_, _, err := cache.Get(blobID)
	done <- err
}

func runCacheDelete(cache *Cache, blobID string, done chan<- error) {
	done <- cache.Delete(blobID)
}

type evictionProbe struct {
	cache  *Cache
	blobID string
}

func (probe evictionProbe) evicted() bool {
	entry, found := probe.cache.policy.Get(probe.blobID)
	if !found {
		return false
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()
	return entry.state == entryEvicted
}

const (
	testEventuallyTimeout  = 2 * time.Second
	testEventuallyInterval = 10 * time.Millisecond
)

type blockingReadStore struct {
	fileStore
	readStarted  chan struct{}
	releaseRead  chan struct{}
	removeCalled chan struct{}
	startOnce    sync.Once
}

func (store *blockingReadStore) read(entry *diskEntry) ([]byte, error) {
	store.startOnce.Do(func() {
		close(store.readStarted)
	})
	<-store.releaseRead
	return store.fileStore.read(entry)
}

func (store *blockingReadStore) remove(path string) error {
	select {
	case store.removeCalled <- struct{}{}:
	default:
	}
	return store.fileStore.remove(path)
}

type removeFailureStore struct {
	fileStore
	mu             sync.Mutex
	failNextRemove bool
}

func (store *removeFailureStore) setFailNextRemove() {
	store.mu.Lock()
	store.failNextRemove = true
	store.mu.Unlock()
}

func (store *removeFailureStore) remove(path string) error {
	store.mu.Lock()
	shouldFail := store.failNextRemove
	store.failNextRemove = false
	store.mu.Unlock()
	if shouldFail {
		return errors.New("injected remove failure")
	}
	return store.fileStore.remove(path)
}

type purgeFailureStore struct {
	fileStore
	mu            sync.Mutex
	failNextPurge bool
}

func (store *purgeFailureStore) setFailNextPurge() {
	store.mu.Lock()
	store.failNextPurge = true
	store.mu.Unlock()
}

func (store *purgeFailureStore) purge() error {
	store.mu.Lock()
	shouldFail := store.failNextPurge
	store.failNextPurge = false
	store.mu.Unlock()
	if shouldFail {
		return errors.New("injected purge failure")
	}
	return store.fileStore.purge()
}

func receiveRemoveSignal(removeCalled <-chan struct{}) error {
	select {
	case <-removeCalled:
		return errors.New("remove called")
	default:
		return nil
	}
}
