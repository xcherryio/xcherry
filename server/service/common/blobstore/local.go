// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package blobstore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const localListPageSize = 1000

func writeLocalObject(ctx context.Context, root string, key string, data []byte) (returnErr error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	path, err := localObjectPath(root, key)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("create blob directory: %w", err)
	}
	temporary, err := os.CreateTemp(filepath.Dir(path), ".dex-blob-*")
	if err != nil {
		return fmt.Errorf("create temporary blob: %w", err)
	}
	temporaryPath := temporary.Name()
	defer func() {
		if removeErr := os.Remove(temporaryPath); removeErr != nil && !os.IsNotExist(removeErr) {
			returnErr = errors.Join(returnErr, fmt.Errorf("remove temporary blob: %w", removeErr))
		}
	}()
	if err := temporary.Chmod(0o600); err != nil {
		return errors.Join(fmt.Errorf("secure temporary blob: %w", err), temporary.Close())
	}
	if _, err := temporary.Write(data); err != nil {
		return errors.Join(fmt.Errorf("write temporary blob: %w", err), temporary.Close())
	}
	if err := temporary.Sync(); err != nil {
		return errors.Join(fmt.Errorf("sync temporary blob: %w", err), temporary.Close())
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close temporary blob: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return fmt.Errorf("publish blob: %w", err)
	}
	return nil
}

func readLocalObject(ctx context.Context, root string, key string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	path, err := localObjectPath(root, key)
	if err != nil {
		return nil, err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return data, ctx.Err()
}

func deleteLocalObject(ctx context.Context, root string, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	path, err := localObjectPath(root, key)
	if err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("delete local blob: %w", err)
	}
	return nil
}

func deleteLocalWorkflowObjects(ctx context.Context, root string, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	path, err := localObjectPath(root, key)
	if err != nil {
		return err
	}
	if err := os.RemoveAll(path); err != nil {
		return fmt.Errorf("delete local workflow blobs: %w", err)
	}
	return nil
}

func listLocalWorkflowPaths(
	ctx context.Context,
	root string,
	namespace string,
	continuationToken *string,
) (*ListObjectPathsOutput, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	namespacePath, err := localObjectPath(root, namespace)
	if err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(namespacePath)
	if os.IsNotExist(err) {
		return &ListObjectPathsOutput{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("list local workflow blobs: %w", err)
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			names = append(names, entry.Name())
		}
	}
	sort.Strings(names)
	start := 0
	if continuationToken != nil {
		start = sort.SearchStrings(names, *continuationToken)
		for start < len(names) && names[start] <= *continuationToken {
			start++
		}
	}
	end := start + localListPageSize
	if end > len(names) {
		end = len(names)
	}
	output := &ListObjectPathsOutput{WorkflowPaths: names[start:end]}
	if end < len(names) && end > start {
		output.ContinuationToken = &names[end-1]
	}
	return output, nil
}

func countLocalObjects(ctx context.Context, root string, prefix string) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	path, err := localObjectPath(root, prefix)
	if err != nil {
		return 0, err
	}
	var count int64
	err = filepath.WalkDir(path, func(_ string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if !entry.IsDir() {
			count++
		}
		return nil
	})
	if os.IsNotExist(err) {
		return 0, nil
	}
	return count, err
}

func localObjectPath(root string, key string) (string, error) {
	absoluteRoot, err := filepath.Abs(root)
	if err != nil {
		return "", fmt.Errorf("resolve local blob root: %w", err)
	}
	cleanKey := filepath.Clean(filepath.FromSlash(key))
	if cleanKey == "." || filepath.IsAbs(cleanKey) {
		return "", fmt.Errorf("invalid local blob key %q", key)
	}
	path := filepath.Join(absoluteRoot, cleanKey)
	relative, err := filepath.Rel(absoluteRoot, path)
	if err != nil {
		return "", fmt.Errorf("resolve local blob key: %w", err)
	}
	if relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("local blob key escapes root")
	}
	return path, nil
}
