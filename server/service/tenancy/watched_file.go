// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package tenancy

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/superdurable/dex/service/common/log"
	"github.com/superdurable/dex/service/common/log/tag"
)

// watchedFileCheckInterval bounds how often a request may stat a watched file.
const watchedFileCheckInterval = time.Second

// watchedFile holds the parsed content of a file and reparses it after its modification time or size changes.
// A missing or unreadable file is parsed as empty, so revocation fails closed; malformed content keeps the previous one.
type watchedFile[T any] struct {
	path   string
	parse  func([]byte) (T, error)
	logger log.Logger

	mu        sync.Mutex
	content   T
	modTime   time.Time
	size      int64
	checkedAt time.Time
}

func newWatchedFile[T any](path string, parse func([]byte) (T, error), logger log.Logger) (*watchedFile[T], error) {
	file := &watchedFile[T]{path: path, parse: parse, logger: logger}
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("stat %s: %w", path, err)
	}
	if err := file.load(info); err != nil {
		return nil, err
	}
	file.checkedAt = time.Now()
	return file, nil
}

// Content returns the current parsed content, rereading the file at most once per check interval.
func (f *watchedFile[T]) Content() T {
	f.mu.Lock()
	defer f.mu.Unlock()
	now := time.Now()
	if now.Sub(f.checkedAt) < watchedFileCheckInterval {
		return f.content
	}
	f.checkedAt = now
	info, err := os.Stat(f.path)
	if err != nil {
		f.logger.Error("stat watched tenancy file; treating it as empty", tag.Path(f.path), tag.Error(err))
		f.setEmpty()
		return f.content
	}
	if info.ModTime().Equal(f.modTime) && info.Size() == f.size {
		return f.content
	}
	if err := f.load(info); err != nil {
		f.logger.Error("reload watched tenancy file; keeping its previous content", tag.Path(f.path), tag.Error(err))
	}
	return f.content
}

func (f *watchedFile[T]) setEmpty() {
	content, err := f.parse(nil)
	if err != nil {
		panic(fmt.Sprintf("parse empty tenancy file: %v", err))
	}
	f.content = content
	f.modTime = time.Time{}
	f.size = -1
}

func (f *watchedFile[T]) load(info os.FileInfo) error {
	data, err := os.ReadFile(f.path)
	if err != nil {
		return fmt.Errorf("read %s: %w", f.path, err)
	}
	content, err := f.parse(data)
	if err != nil {
		return fmt.Errorf("parse %s: %w", f.path, err)
	}
	f.content = content
	f.modTime = info.ModTime()
	f.size = info.Size()
	return nil
}
