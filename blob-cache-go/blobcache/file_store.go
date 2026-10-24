// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

// This file implements local persistence inside one exclusively owned cache
// directory. Blob IDs map to deterministic sharded paths, commits write and
// close temporary files before atomic rename, and reads validate metadata before
// returning payloads. Startup scans committed files, streams checksum
// verification without loading the whole cache, and reports malformed paths
// for removal. For example, a crash before rename leaves only a temporary file
// that startup purges, while a crash after rename leaves an entry that recovery
// admits only if its length and checksum remain valid.

package blobcache

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// fileStore isolates filesystem persistence and enables failure testing.
type fileStore interface {
	// prepare creates the owned directory layout before recovery.
	prepare() error
	// purgeTemp removes writes interrupted before their atomic rename.
	purgeTemp() error
	// scan finds committed entries and corrupted files during recovery.
	scan() (scanResult, error)
	// pathFor maps a blob ID to its deterministic sharded path.
	pathFor(blobID string) string
	// stage writes a complete temporary entry without publishing it.
	stage(metadata fileMetadata, payload []byte) (string, commitResult, error)
	// publish atomically places a staged entry at its final path.
	publish(metadata fileMetadata, tempPath string) (commitResult, error)
	// read validates the stored entry before returning its payload.
	read(entry *diskEntry) ([]byte, error)
	// remove deletes one entry while treating an absent file as removed.
	remove(path string) error
	// purge resets all cache-owned storage while preserving usability.
	purge() error
}

type localFileStoreImpl struct {
	rootDir  string
	tempDir  string
	blobsDir string
}

func newLocalFileStore(rootDir string) (fileStore, error) {
	absoluteRoot, err := filepath.Abs(rootDir)
	if err != nil {
		return nil, fmt.Errorf("resolve blob cache root: %w", err)
	}
	return &localFileStoreImpl{
		rootDir:  filepath.Clean(absoluteRoot),
		tempDir:  filepath.Join(absoluteRoot, "tmp"),
		blobsDir: filepath.Join(absoluteRoot, "blobs"),
	}, nil
}

func (store *localFileStoreImpl) prepare() error {
	if err := os.MkdirAll(store.rootDir, 0o700); err != nil {
		return fmt.Errorf("create blob cache root: %w", err)
	}
	if err := os.MkdirAll(store.tempDir, 0o700); err != nil {
		return fmt.Errorf("create blob cache temp directory: %w", err)
	}
	if err := os.MkdirAll(store.blobsDir, 0o700); err != nil {
		return fmt.Errorf("create blob cache data directory: %w", err)
	}
	return nil
}

func (store *localFileStoreImpl) purgeTemp() error {
	if err := os.RemoveAll(store.tempDir); err != nil {
		return fmt.Errorf("purge blob cache temp directory: %w", err)
	}
	if err := os.MkdirAll(store.tempDir, 0o700); err != nil {
		return fmt.Errorf("recreate blob cache temp directory: %w", err)
	}
	return nil
}

func (store *localFileStoreImpl) scan() (scanResult, error) {
	collector := &scanCollector{store: store}
	err := filepath.WalkDir(store.blobsDir, collector.visit)
	if err != nil {
		return scanResult{}, fmt.Errorf("scan blob cache: %w", err)
	}

	sort.Slice(collector.result.entries, func(leftIndex, rightIndex int) bool {
		left := collector.result.entries[leftIndex]
		right := collector.result.entries[rightIndex]
		if left.modTime.Equal(right.modTime) {
			return left.path < right.path
		}
		return left.modTime.After(right.modTime)
	})
	sort.Strings(collector.result.invalidPaths)
	return collector.result, nil
}

func (collector *scanCollector) visit(path string, entry os.DirEntry, walkErr error) error {
	if walkErr != nil {
		return walkErr
	}
	if entry.IsDir() || !entry.Type().IsRegular() || filepath.Ext(entry.Name()) != ".blob" {
		return nil
	}

	metadata, err := collector.store.inspect(path)
	if err == nil {
		collector.result.entries = append(collector.result.entries, metadata)
		return nil
	}
	if errors.Is(err, ErrCorrupt) {
		collector.result.invalidPaths = append(collector.result.invalidPaths, path)
		return nil
	}
	return err
}

func (store *localFileStoreImpl) inspect(path string) (fileMetadata, error) {
	file, err := os.Open(path)
	if err != nil {
		return fileMetadata{}, fmt.Errorf("open cache entry %s: %w", store.relativePath(path), err)
	}

	metadata, inspectErr := store.inspectOpenFile(file, path)
	closeErr := file.Close()
	return metadata, errors.Join(inspectErr, closeErr)
}

func (store *localFileStoreImpl) inspectOpenFile(file *os.File, path string) (fileMetadata, error) {
	fileInfo, err := file.Stat()
	if err != nil {
		return fileMetadata{}, fmt.Errorf("stat cache entry %s: %w", store.relativePath(path), err)
	}
	if !fileInfo.Mode().IsRegular() {
		return fileMetadata{}, fmt.Errorf("%w: non-regular file %s", ErrCorrupt, store.relativePath(path))
	}

	header, blobID, err := store.readPrefix(file, fileInfo.Size(), path)
	if err != nil {
		return fileMetadata{}, err
	}

	checksum := crc32.New(crcTable)
	writeChecksumPrefix(checksum, blobID)
	copied, err := io.CopyN(checksum, file, int64(header.payloadLength))
	if err != nil {
		return fileMetadata{}, fmt.Errorf("%w: read payload %s: %v", ErrCorrupt, store.relativePath(path), err)
	}
	if copied != int64(header.payloadLength) || checksum.Sum32() != header.checksum {
		return fileMetadata{}, fmt.Errorf("%w: checksum mismatch %s", ErrCorrupt, store.relativePath(path))
	}

	return fileMetadata{
		blobID:   blobID,
		path:     path,
		size:     fileInfo.Size(),
		checksum: header.checksum,
		modTime:  fileInfo.ModTime(),
	}, nil
}

func (store *localFileStoreImpl) pathFor(blobID string) string {
	sum := sha256.Sum256([]byte(blobID))
	encoded := hex.EncodeToString(sum[:])
	return filepath.Join(store.blobsDir, encoded[0:2], encoded[2:4], encoded+".blob")
}

func (store *localFileStoreImpl) stage(metadata fileMetadata, payload []byte) (string, commitResult, error) {
	tempFile, err := os.CreateTemp(store.tempDir, "blob-*.tmp")
	if err != nil {
		return "", commitResult{}, fmt.Errorf("create blob cache temp file: %w", err)
	}
	tempPath := tempFile.Name()

	header := encodeHeader(fileHeader{
		blobIDLength:  uint32(len(metadata.blobID)),
		payloadLength: uint64(len(payload)),
		checksum:      metadata.checksum,
	})
	if err := writeAll(tempFile, header); err != nil {
		result, failure := store.failCommit(tempFile, tempPath, fmt.Errorf("write blob cache header: %w", err))
		return "", result, failure
	}
	if err := writeAll(tempFile, []byte(metadata.blobID)); err != nil {
		result, failure := store.failCommit(tempFile, tempPath, fmt.Errorf("write blob cache ID: %w", err))
		return "", result, failure
	}
	if err := writeAll(tempFile, payload); err != nil {
		result, failure := store.failCommit(tempFile, tempPath, fmt.Errorf("write blob cache payload: %w", err))
		return "", result, failure
	}
	if err := tempFile.Close(); err != nil {
		result, failure := store.failCommit(nil, tempPath, fmt.Errorf("close blob cache temp file: %w", err))
		return "", result, failure
	}
	return tempPath, commitResult{}, nil
}

func (store *localFileStoreImpl) publish(metadata fileMetadata, tempPath string) (commitResult, error) {
	if err := os.MkdirAll(filepath.Dir(metadata.path), 0o700); err != nil {
		return store.failCommit(nil, tempPath, fmt.Errorf("create blob cache shard: %w", err))
	}
	if _, err := os.Lstat(metadata.path); err == nil {
		return store.failCommit(nil, tempPath, fmt.Errorf("%w: final path already exists", ErrContentMismatch))
	} else if !errors.Is(err, os.ErrNotExist) {
		return store.failCommit(nil, tempPath, fmt.Errorf("inspect final cache path: %w", err))
	}
	if err := os.Rename(tempPath, metadata.path); err != nil {
		return store.failCommit(nil, tempPath, fmt.Errorf("commit blob cache file: %w", err))
	}
	return commitResult{}, nil
}

func (store *localFileStoreImpl) read(entry *diskEntry) ([]byte, error) {
	file, err := os.Open(entry.path)
	if err != nil {
		return nil, fmt.Errorf("open cache entry %s: %w", store.relativePath(entry.path), err)
	}

	payload, readErr := store.readOpenFile(file, entry)
	closeErr := file.Close()
	return payload, errors.Join(readErr, closeErr)
}

func (store *localFileStoreImpl) readOpenFile(file *os.File, entry *diskEntry) ([]byte, error) {
	fileInfo, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat cache entry %s: %w", store.relativePath(entry.path), err)
	}
	header, blobID, err := store.readPrefix(file, fileInfo.Size(), entry.path)
	if err != nil {
		return nil, err
	}
	if blobID != entry.blobID ||
		header.checksum != entry.checksum || fileInfo.Size() != entry.size {
		return nil, fmt.Errorf("%w: metadata mismatch %s", ErrCorrupt, store.relativePath(entry.path))
	}
	if header.payloadLength > uint64(math.MaxInt) {
		return nil, fmt.Errorf("%w: payload too large %s", ErrCorrupt, store.relativePath(entry.path))
	}

	payload := make([]byte, int(header.payloadLength))
	if _, err := io.ReadFull(file, payload); err != nil {
		return nil, fmt.Errorf("%w: read payload %s: %v", ErrCorrupt, store.relativePath(entry.path), err)
	}
	if calculateChecksum(blobID, payload) != header.checksum {
		return nil, fmt.Errorf("%w: checksum mismatch %s", ErrCorrupt, store.relativePath(entry.path))
	}
	return payload, nil
}

func (store *localFileStoreImpl) remove(path string) error {
	err := os.Remove(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("remove cache entry %s: %w", store.relativePath(path), err)
	}
	return nil
}

func (store *localFileStoreImpl) purge() error {
	tempErr := os.RemoveAll(store.tempDir)
	blobsErr := os.RemoveAll(store.blobsDir)
	prepareErr := store.prepare()
	if err := errors.Join(tempErr, blobsErr, prepareErr); err != nil {
		return fmt.Errorf("purge blob cache: %w", err)
	}
	return nil
}

func (store *localFileStoreImpl) readPrefix(file *os.File, fileSize int64, path string) (fileHeader, string, error) {
	if fileSize < fixedHeaderSize {
		return fileHeader{}, "", fmt.Errorf("%w: truncated header %s", ErrCorrupt, store.relativePath(path))
	}

	encodedHeader := make([]byte, fixedHeaderSize)
	if _, err := io.ReadFull(file, encodedHeader); err != nil {
		return fileHeader{}, "", fmt.Errorf("%w: read header %s: %v", ErrCorrupt, store.relativePath(path), err)
	}
	header, err := decodeHeader(encodedHeader)
	if err != nil {
		return fileHeader{}, "", fmt.Errorf("%w: %s", err, store.relativePath(path))
	}
	if header.blobIDLength == 0 || header.blobIDLength > maxBlobIDBytes {
		return fileHeader{}, "", fmt.Errorf("%w: invalid blob ID length %s", ErrCorrupt, store.relativePath(path))
	}
	if header.payloadLength > math.MaxInt64 {
		return fileHeader{}, "", fmt.Errorf("%w: payload length overflow %s", ErrCorrupt, store.relativePath(path))
	}

	expectedSize := int64(fixedHeaderSize) + int64(header.blobIDLength)
	if int64(header.payloadLength) > math.MaxInt64-expectedSize {
		return fileHeader{}, "", fmt.Errorf("%w: file length overflow %s", ErrCorrupt, store.relativePath(path))
	}
	expectedSize += int64(header.payloadLength)
	if expectedSize != fileSize {
		return fileHeader{}, "", fmt.Errorf("%w: file length mismatch %s", ErrCorrupt, store.relativePath(path))
	}

	blobIDBytes := make([]byte, int(header.blobIDLength))
	if _, err := io.ReadFull(file, blobIDBytes); err != nil {
		return fileHeader{}, "", fmt.Errorf("%w: read blob ID %s: %v", ErrCorrupt, store.relativePath(path), err)
	}
	blobID := string(blobIDBytes)
	if store.pathFor(blobID) != filepath.Clean(path) {
		return fileHeader{}, "", fmt.Errorf("%w: blob ID path mismatch %s", ErrCorrupt, store.relativePath(path))
	}
	return header, blobID, nil
}

func (store *localFileStoreImpl) failCommit(
	tempFile *os.File,
	tempPath string,
	commitErr error,
) (commitResult, error) {
	var closeErr error
	if tempFile != nil {
		closeErr = tempFile.Close()
	}
	removeErr := os.Remove(tempPath)
	if errors.Is(removeErr, os.ErrNotExist) {
		removeErr = nil
	}
	if removeErr != nil {
		return commitResult{orphanPath: tempPath}, errors.Join(commitErr, closeErr, removeErr)
	}
	return commitResult{}, errors.Join(commitErr, closeErr)
}

func (store *localFileStoreImpl) relativePath(path string) string {
	relative, err := filepath.Rel(store.rootDir, path)
	if err != nil || strings.HasPrefix(relative, "..") {
		return filepath.Base(path)
	}
	return relative
}

type fileMetadata struct {
	blobID   string
	path     string
	size     int64
	checksum uint32
	modTime  time.Time
}

type scanResult struct {
	entries      []fileMetadata
	invalidPaths []string
}

type scanCollector struct {
	store  *localFileStoreImpl
	result scanResult
}

type commitResult struct {
	orphanPath string
}

func writeAll(writer io.Writer, value []byte) error {
	for len(value) > 0 {
		written, err := writer.Write(value)
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
		value = value[written:]
	}
	return nil
}
