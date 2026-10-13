// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package blobstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

// tenancyRecordPath holds the tombstones of a multi-tenant namespace. Tenant segments never start with "_".
const tenancyRecordPath = "_tenancy/tombstones"

// ErrTenancyRecordConflict reports a conditional write that lost to a concurrent writer; reread and retry.
var ErrTenancyRecordConflict = errors.New("tenancy record changed concurrently")

func getS3ObjectWithVersion(
	ctx context.Context,
	client *s3.Client,
	bucket string,
	key string,
) (data []byte, version string, returnErr error) {
	result, err := client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if IsObjectNotFound(err) {
		return nil, "", nil
	}
	if err != nil {
		return nil, "", err
	}
	defer func() {
		if closeErr := result.Body.Close(); closeErr != nil && returnErr == nil {
			returnErr = closeErr
		}
	}()
	data, err = io.ReadAll(result.Body)
	return data, aws.ToString(result.ETag), err
}

// putS3ObjectIfVersion writes only when the object still has version, or does not exist when version is empty.
func putS3ObjectIfVersion(
	ctx context.Context,
	client *s3.Client,
	bucket string,
	key string,
	data []byte,
	version string,
) error {
	input := &s3.PutObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(data),
		ContentType: aws.String("application/json"),
	}
	if version == "" {
		input.IfNoneMatch = aws.String("*")
	} else {
		input.IfMatch = aws.String(version)
	}
	_, err := client.PutObject(ctx, input)
	var apiError smithy.APIError
	if errors.As(err, &apiError) &&
		(apiError.ErrorCode() == "PreconditionFailed" || apiError.ErrorCode() == "ConditionalRequestConflict") {
		return fmt.Errorf("%w: %v", ErrTenancyRecordConflict, err)
	}
	return err
}

// deleteS3ObjectsUnderPrefix deletes at most limit objects under prefix and returns how many it deleted.
func deleteS3ObjectsUnderPrefix(ctx context.Context, client *s3.Client, bucket string, prefix string, limit int) (int, error) {
	listed, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket:  aws.String(bucket),
		Prefix:  aws.String(prefix),
		MaxKeys: aws.Int32(int32(limit)),
	})
	if err != nil || len(listed.Contents) == 0 {
		return 0, err
	}
	objects := make([]types.ObjectIdentifier, 0, len(listed.Contents))
	for _, object := range listed.Contents {
		objects = append(objects, types.ObjectIdentifier{Key: object.Key})
	}
	output, err := client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
		Bucket: aws.String(bucket),
		Delete: &types.Delete{Objects: objects, Quiet: aws.Bool(true)},
	})
	if err != nil {
		return 0, err
	}
	if len(output.Errors) > 0 {
		return 0, fmt.Errorf("delete tenant objects: %s", aws.ToString(output.Errors[0].Message))
	}
	return len(objects), nil
}

// readLocalObjectWithVersion returns the content and its SHA-256 as version, or no version when absent.
func readLocalObjectWithVersion(ctx context.Context, root string, key string) ([]byte, string, error) {
	data, err := readLocalObject(ctx, root, key)
	if errors.Is(err, os.ErrNotExist) {
		return nil, "", nil
	}
	if err != nil {
		return nil, "", err
	}
	return data, localObjectVersion(data), nil
}

// writeLocalObjectIfVersion compares and writes under an exclusive file lock shared by every local process.
func writeLocalObjectIfVersion(
	ctx context.Context,
	root string,
	key string,
	data []byte,
	version string,
) (returnErr error) {
	path, err := localObjectPath(root, key)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("create tenancy record directory: %w", err)
	}
	lock, err := os.OpenFile(path+".lock", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return fmt.Errorf("open tenancy record lock: %w", err)
	}
	defer func() {
		if closeErr := lock.Close(); closeErr != nil && returnErr == nil {
			returnErr = closeErr
		}
	}()
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); err != nil {
		return fmt.Errorf("lock tenancy record: %w", err)
	}
	_, currentVersion, err := readLocalObjectWithVersion(ctx, root, key)
	if err != nil {
		return err
	}
	if currentVersion != version {
		return ErrTenancyRecordConflict
	}
	return writeLocalObject(ctx, root, key, data)
}

// deleteLocalObjectsUnderDirectory deletes at most limit files under the directory key.
func deleteLocalObjectsUnderDirectory(ctx context.Context, root string, key string, limit int) (int, error) {
	directory, err := localObjectPath(root, key)
	if err != nil {
		return 0, err
	}
	deleted := 0
	walkErr := filepath.WalkDir(directory, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return filepath.SkipDir
			}
			return err
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		if entry.IsDir() {
			return nil
		}
		if deleted >= limit {
			return filepath.SkipAll
		}
		if err := os.Remove(path); err != nil {
			return err
		}
		deleted++
		return nil
	})
	if walkErr != nil {
		return deleted, walkErr
	}
	if deleted < limit {
		if err := os.RemoveAll(directory); err != nil {
			return deleted, err
		}
	}
	return deleted, nil
}

func localObjectVersion(data []byte) string {
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}
