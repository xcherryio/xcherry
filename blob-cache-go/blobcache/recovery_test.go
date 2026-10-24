// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package blobcache

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCacheWarmRestart(t *testing.T) {
	rootDir := t.TempDir()
	cfg := &Config{Dir: rootDir, MaxBytes: 1 << 20, FrequencyCounters: 10_000}

	firstCache, err := New(cfg)
	require.NoError(t, err)
	require.True(t, putForTest(t, firstCache, "warm-id", []byte("persisted")))
	require.NoError(t, firstCache.Close())
	require.NotZero(t, cacheOwnedBytes(t, rootDir))

	secondCache, err := New(cfg)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, secondCache.Close())
	})
	payload, found, err := secondCache.Get("warm-id")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, []byte("persisted"), payload)
}

func TestCacheRecoveryRemovesTempAndCorruptFiles(t *testing.T) {
	rootDir := t.TempDir()
	cfg := &Config{Dir: rootDir, MaxBytes: 1 << 20, FrequencyCounters: 10_000}

	firstCache, err := New(cfg)
	require.NoError(t, err)
	require.True(t, putForTest(t, firstCache, "corrupt-id", []byte("payload")))
	entry, found := firstCache.policy.Get("corrupt-id")
	require.True(t, found)
	require.NoError(t, firstCache.Close())

	file, err := os.OpenFile(entry.path, os.O_WRONLY, 0)
	require.NoError(t, err)
	_, err = file.WriteAt([]byte{0xff}, fixedHeaderSize)
	require.NoError(t, err)
	require.NoError(t, file.Close())

	tempPath := filepath.Join(rootDir, "tmp", "interrupted.tmp")
	require.NoError(t, os.WriteFile(tempPath, []byte("partial"), 0o600))

	secondCache, err := New(cfg)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, secondCache.Close())
	})
	_, found, err = secondCache.Get("corrupt-id")
	require.NoError(t, err)
	require.False(t, found)
	require.NoFileExists(t, entry.path)
	require.NoFileExists(t, tempPath)
}

func TestCacheRejectsDamagedFileAndAcceptsRefetch(t *testing.T) {
	for _, damage := range []struct {
		name  string
		apply func(*testing.T, string, string)
	}{
		{
			name: "truncated",
			apply: func(t *testing.T, path string, blobID string) {
				require.NoError(t, os.Truncate(path, int64(fixedHeaderSize+len(blobID)+2)))
			},
		},
		{
			name: "corrupted",
			apply: func(t *testing.T, path string, blobID string) {
				file, err := os.OpenFile(path, os.O_WRONLY, 0)
				require.NoError(t, err)
				_, err = file.WriteAt([]byte{0xff}, int64(fixedHeaderSize+len(blobID)))
				require.NoError(t, err)
				require.NoError(t, file.Close())
			},
		},
	} {
		t.Run(damage.name, func(t *testing.T) {
			cache, rootDir := newTestCache(t, 1<<20)
			blobID := "refetched-id"
			payload := []byte("original-payload")
			require.True(t, putForTest(t, cache, blobID, payload))
			path := cache.store.pathFor(blobID)
			damage.apply(t, path, blobID)

			cachedPayload, found, err := cache.Get(blobID)
			require.NoError(t, err)
			require.False(t, found)
			require.Nil(t, cachedPayload)
			require.NoFileExists(t, path)

			require.True(t, putForTest(t, cache, blobID, payload))
			cachedPayload, found, err = cache.Get(blobID)
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, payload, cachedPayload)
			require.LessOrEqual(t, cacheOwnedBytes(t, rootDir), cache.cfg.MaxBytes)
		})
	}
}

func TestCacheRecoveryRemovesNonZeroReservedHeader(t *testing.T) {
	rootDir := t.TempDir()
	cfg := &Config{Dir: rootDir, MaxBytes: 1 << 20, FrequencyCounters: 10_000}

	firstCache, err := New(cfg)
	require.NoError(t, err)
	require.True(t, putForTest(t, firstCache, "reserved-id", []byte("payload")))
	entry, found := firstCache.policy.Get("reserved-id")
	require.True(t, found)
	require.NoError(t, firstCache.Close())

	file, err := os.OpenFile(entry.path, os.O_WRONLY, 0)
	require.NoError(t, err)
	_, err = file.WriteAt([]byte{1}, 5)
	require.NoError(t, err)
	require.NoError(t, file.Close())

	secondCache, err := New(cfg)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, secondCache.Close())
	})
	_, found, err = secondCache.Get("reserved-id")
	require.NoError(t, err)
	require.False(t, found)
	require.NoFileExists(t, entry.path)
}

func TestCacheRecoveryAppliesReducedBudget(t *testing.T) {
	rootDir := t.TempDir()
	largeConfig := &Config{Dir: rootDir, MaxBytes: 1 << 20, FrequencyCounters: 10_000}

	firstCache, err := New(largeConfig)
	require.NoError(t, err)
	for index := 0; index < 8; index++ {
		blobID := "restart-" + string(rune('a'+index))
		require.True(t, putForTest(t, firstCache, blobID, make([]byte, 64)))
	}
	require.NoError(t, firstCache.Close())

	smallLimit := int64(fixedHeaderSize + len("restart-a") + 64)
	secondCache, err := New(&Config{
		Dir:               rootDir,
		MaxBytes:          smallLimit,
		FrequencyCounters: 10_000,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, secondCache.Close())
	})
	require.LessOrEqual(t, cacheOwnedBytes(t, rootDir), smallLimit)
	require.LessOrEqual(t, len(regularFiles(t, filepath.Join(rootDir, "blobs"))), 1)
}

func TestDeleteAllThenCloseLeavesEmptyCache(t *testing.T) {
	rootDir := t.TempDir()
	cache, err := New(&Config{Dir: rootDir, MaxBytes: 1 << 20, FrequencyCounters: 10_000})
	require.NoError(t, err)
	require.True(t, putForTest(t, cache, "ephemeral", []byte("payload")))

	require.NoError(t, cache.DeleteAll())
	require.NoError(t, cache.Close())
	require.Zero(t, cacheOwnedBytes(t, rootDir))
}
