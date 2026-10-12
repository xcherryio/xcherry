// Legacy Materials in this file remain under their original licenses.
// See LEGACY_NOTICES.md.

// Modifications Copyright (c) 2026 Super Durable, Inc.
//
// Modifications after the Legacy Cutoff are licensed under the
// Sustainable Use License 1.0.
// Legacy Materials remain under their original licenses.
// See LICENSE and LEGACY_NOTICES.md.

package blobstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"io"
	"io/fs"
	"math/big"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/superdurable/dex/blob-cache-go/blobcache"
	"github.com/superdurable/dex/config"
	"github.com/superdurable/dex/service/common/log"
	"github.com/superdurable/dex/service/common/log/tag"
	"go.temporal.io/sdk/client"
)

var errStoreNotFound = errors.New("store not found")

const base36Alphabet = "0123456789abcdefghijklmnopqrstuvwxyz"

type blobStoreImpl struct {
	s3Client                    *s3.Client
	pathPrefix                  string // the Temporal namespace or Cadence domain + "/"
	isMultiTenant               bool
	activeStorage               config.BlobStoreConfigEntry
	supportedStore              map[string]config.BlobStoreConfigEntry // storeId as key
	logger                      log.Logger
	writeObjectErrorCounter     client.MetricsCounter
	readObjectErrorCounter      client.MetricsCounter
	writeObjectSuccessHistogram client.MetricsTimer
	readObjectSuccessHistogram  client.MetricsTimer
	objectIDLength              int
	blobCache                   *blobcache.Cache
	blobCacheHitCounter         client.MetricsCounter
	blobCacheMissCounter        client.MetricsCounter
	blobCacheWriteCounter       client.MetricsCounter
	blobCacheReadFillCounter    client.MetricsCounter
	blobCacheBypassCounter      client.MetricsCounter
	blobCacheErrorCounter       client.MetricsCounter
}

// NewBlobStore builds the store; in multi-tenant mode a tenant's objects are placed under its tenant segment.
func NewBlobStore(
	s3Client *s3.Client,
	temporalOrCadenceNamespace string,
	isMultiTenant bool,
	storeConfig *config.BlobStoreConfig,
	logger log.Logger,
	metrics client.MetricsHandler,
) (BlobStore, error) {
	if storeConfig == nil {
		panic("NewBlobStore requires BlobStoreConfig")
	}
	if err := storeConfig.Validate(); err != nil {
		return nil, err
	}
	if !storeConfig.EffectiveEnabled() {
		return nil, nil
	}

	var activeStorage *config.BlobStoreConfigEntry
	supportedStores := map[string]config.BlobStoreConfigEntry{}
	hasS3Storage := false
	for _, storage := range storeConfig.SupportedStorages {
		if storage.Status == config.StorageStatusActive {
			if activeStorage != nil {
				return nil, errors.New("cannot have more than one active storage configured")
			}
			activeStorage = &storage
		}
		supportedStores[storage.StorageId] = storage
		switch storage.StorageType {
		case config.StorageTypeS3:
			hasS3Storage = true
		case config.StorageTypeLocal:
			if storage.LocalDirectory == "" {
				return nil, errors.New("local storage requires a directory")
			}
		default:
			return nil, fmt.Errorf("unsupported blob storage type %q", storage.StorageType)
		}
	}
	if activeStorage == nil {
		return nil, errors.New("no active storage found")
	}

	metricsHandler := metrics.WithTags(map[string]string{"prefix": temporalOrCadenceNamespace})
	writeObjectErrorCounter := metricsHandler.Counter("write_object_error")
	readObjectErrorCounter := metricsHandler.Counter("read_object_error")
	writeObjectSuccessHistogram := metricsHandler.Timer("write_object_success")
	readObjectSuccessHistogram := metricsHandler.Timer("read_object_success")
	var attributeCache *blobcache.Cache
	if storeConfig.BlobCache.Directory != "" && hasS3Storage {
		var err error
		attributeCache, err = blobcache.New(&blobcache.Config{
			Dir:      storeConfig.BlobCache.Directory,
			MaxBytes: storeConfig.BlobCache.EffectiveMaxBytes(),
		})
		if err != nil {
			return nil, fmt.Errorf("initialize Attribute blob cache: %w", err)
		}
	}

	return &blobStoreImpl{
		s3Client:                    s3Client,
		pathPrefix:                  temporalOrCadenceNamespace + "/",
		isMultiTenant:               isMultiTenant,
		activeStorage:               *activeStorage,
		supportedStore:              supportedStores,
		logger:                      logger,
		writeObjectErrorCounter:     writeObjectErrorCounter,
		readObjectErrorCounter:      readObjectErrorCounter,
		writeObjectSuccessHistogram: writeObjectSuccessHistogram,
		readObjectSuccessHistogram:  readObjectSuccessHistogram,
		objectIDLength:              storeConfig.EffectiveObjectIDLength(),
		blobCache:                   attributeCache,
		blobCacheHitCounter:         metricsHandler.Counter("blob_cache_hit"),
		blobCacheMissCounter:        metricsHandler.Counter("blob_cache_miss"),
		blobCacheWriteCounter:       metricsHandler.Counter("blob_cache_write_through"),
		blobCacheReadFillCounter:    metricsHandler.Counter("blob_cache_read_fill"),
		blobCacheBypassCounter:      metricsHandler.Counter("blob_cache_bypass"),
		blobCacheErrorCounter:       metricsHandler.Counter("blob_cache_error"),
	}, nil
}

func (b *blobStoreImpl) WriteObject(
	ctx context.Context,
	flowID string,
	invocationID string,
	data []byte,
) (storeID, locator string, err error) {
	storeID = b.activeStorage.StorageId
	objectID, err := deterministicBlobObjectID(invocationID, data, b.objectIDLength)
	if err != nil {
		return "", "", err
	}
	writeDate, err := formatBlobReferenceDate(time.Now())
	if err != nil {
		return "", "", err
	}
	locator = writeDate + "/" + objectID
	path, err := valueObjectPath(b.isMultiTenant, flowID, locator)
	if err != nil {
		return "", "", err
	}

	err = b.writeObject(ctx, b.activeStorage, path, data)
	if err != nil {
		b.writeObjectErrorCounter.Inc(1)
		var re s3.ResponseError
		if errors.As(err, &re) {
			b.logger.Error("PutObject S3 API error",
				tag.RequestID(re.ServiceRequestID()),
				tag.HostID(re.ServiceHostID()),
				tag.Bucket(b.activeStorage.S3Bucket),
				tag.WorkflowID(flowID),
				tag.Error(err))
			err = fmt.Errorf("failed to write object (requestId=%s, hostId=%s): %w",
				re.ServiceRequestID(), re.ServiceHostID(), err)
		} else {
			b.logger.Error("PutObject error",
				tag.Bucket(b.activeStorage.S3Bucket),
				tag.WorkflowID(flowID),
				tag.Error(err))
			err = fmt.Errorf("failed to write object: %w", err)
		}
		return
	}
	if b.activeStorage.StorageType == config.StorageTypeS3 {
		if err = b.cacheAttributeObject(storeID, path, data, b.blobCacheWriteCounter); err != nil {
			b.writeObjectErrorCounter.Inc(1)
			b.logger.Error("Attribute blob cache write failed",
				tag.Path(path),
				tag.StoreID(storeID),
				tag.Error(err))
			err = fmt.Errorf("failed to cache written object: %w", err)
			return
		}
	}
	b.writeObjectSuccessHistogram.Record(time.Duration(len(data)))
	return
}

func (b *blobStoreImpl) WriteStepEventInput(
	ctx context.Context,
	runStarted time.Time,
	flowID string,
	runID string,
	stepExecutionID string,
	method string,
	data []byte,
) error {
	path := stepEventInputPath(b.isMultiTenant, runStarted, flowID, runID, stepExecutionID, method)
	if err := b.writeObject(ctx, b.activeStorage, path, data); err != nil {
		b.writeObjectErrorCounter.Inc(1)
		return fmt.Errorf("write step event input: %w", err)
	}
	b.writeObjectSuccessHistogram.Record(time.Duration(len(data)))
	b.logger.Info("Step event input snapshot written",
		tag.StoreID(b.activeStorage.StorageId),
		tag.Path(b.pathPrefix+path),
		tag.WorkflowID(flowID),
		tag.WorkflowRunID(runID),
		tag.StepExecutionID(stepExecutionID),
		tag.StepEventInputMethod(method))
	return nil
}

func (b *blobStoreImpl) ReadStepEventInput(
	ctx context.Context,
	runStarted time.Time,
	flowID string,
	runID string,
	stepExecutionID string,
	method string,
) ([]byte, bool, error) {
	path := stepEventInputPath(b.isMultiTenant, runStarted, flowID, runID, stepExecutionID, method)
	storeIDs := make([]string, 0, len(b.supportedStore))
	for storeID := range b.supportedStore {
		if storeID != b.activeStorage.StorageId {
			storeIDs = append(storeIDs, storeID)
		}
	}
	sort.Strings(storeIDs)
	storeIDs = append([]string{b.activeStorage.StorageId}, storeIDs...)
	for _, storeID := range storeIDs {
		storage := b.supportedStore[storeID]
		data, err := b.readObject(ctx, storage, path)
		if err == nil {
			b.readObjectSuccessHistogram.Record(time.Duration(len(data)))
			return data, true, nil
		}
		if IsObjectNotFound(err) {
			continue
		}
		b.readObjectErrorCounter.Inc(1)
		return nil, false, fmt.Errorf("read step event input: %w", err)
	}
	return nil, false, nil
}

func (b *blobStoreImpl) writeObject(
	ctx context.Context,
	storage config.BlobStoreConfigEntry,
	path string,
	data []byte,
) error {
	switch storage.StorageType {
	case config.StorageTypeS3:
		if b.s3Client == nil {
			return errors.New("S3 client is not configured")
		}
		return putObject(ctx, b.s3Client, storage.S3Bucket, b.objectKey(storage, path), data)
	case config.StorageTypeLocal:
		return writeLocalObject(ctx, storage.LocalDirectory, b.objectKey(storage, path), data)
	default:
		return fmt.Errorf("unsupported blob storage type %q", storage.StorageType)
	}
}

func deterministicBlobObjectID(invocationID string, data []byte, objectIDLength int) (string, error) {
	hasher := sha256.New()
	components := [][]byte{
		[]byte("dex-blob-v2"),
		[]byte(invocationID),
		data,
	}
	for _, component := range components {
		if err := writeHashComponent(hasher, component); err != nil {
			return "", err
		}
	}
	return fixedWidthBase36(hasher.Sum(nil), objectIDLength), nil
}

func fixedWidthBase36(digest []byte, length int) string {
	value := new(big.Int).SetBytes(digest)
	base := big.NewInt(int64(len(base36Alphabet)))
	remainder := new(big.Int)
	encoded := make([]byte, length)
	for index := length - 1; index >= 0; index-- {
		value.DivMod(value, base, remainder)
		encoded[index] = base36Alphabet[remainder.Int64()]
	}
	return string(encoded)
}

func writeHashComponent(hasher hash.Hash, component []byte) error {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(component)))
	if err := writeHashBytes(hasher, length[:]); err != nil {
		return err
	}
	return writeHashBytes(hasher, component)
}

func writeHashBytes(hasher hash.Hash, data []byte) error {
	written, err := hasher.Write(data)
	if err != nil {
		return err
	}
	if written != len(data) {
		return io.ErrShortWrite
	}
	return nil
}

func (b *blobStoreImpl) ReadObject(ctx context.Context, storeID, flowID, locator string) ([]byte, error) {
	path, err := valueObjectPath(b.isMultiTenant, flowID, locator)
	if err != nil {
		return nil, err
	}
	storeConfig, ok := b.supportedStore[storeID]
	if !ok {
		b.readObjectErrorCounter.Inc(1)
		return nil, fmt.Errorf("%w for %s", errStoreNotFound, storeID)
	}
	if storeConfig.StorageType == config.StorageTypeS3 && b.blobCache != nil {
		data, found, err := b.readCachedAttributeObject(storeID, path)
		if err != nil {
			b.readObjectErrorCounter.Inc(1)
			b.logger.Error("Attribute blob cache read failed",
				tag.Path(path),
				tag.StoreID(storeID),
				tag.Error(err))
			return nil, fmt.Errorf("failed to read cached object: %w", err)
		}
		if found {
			b.readObjectSuccessHistogram.Record(time.Duration(len(data)))
			return data, nil
		}
	}
	data, err := b.readObject(ctx, storeConfig, path)
	if err != nil {
		b.readObjectErrorCounter.Inc(1)
		var re s3.ResponseError
		if errors.As(err, &re) {
			b.logger.Error("GetObject S3 API error",
				tag.RequestID(re.ServiceRequestID()),
				tag.HostID(re.ServiceHostID()),
				tag.Bucket(storeConfig.S3Bucket),
				tag.Path(path),
				tag.StoreID(storeID),
				tag.Error(err))
			return nil, fmt.Errorf("failed to read object (requestId=%s, hostId=%s): %w",
				re.ServiceRequestID(), re.ServiceHostID(), err)
		}
		b.logger.Error("GetObject error",
			tag.Bucket(storeConfig.S3Bucket),
			tag.Path(path),
			tag.StoreID(storeID),
			tag.Error(err))
		return nil, fmt.Errorf("failed to read object: %w", err)
	}
	if storeConfig.StorageType == config.StorageTypeS3 {
		if err := b.cacheAttributeObject(storeID, path, data, b.blobCacheReadFillCounter); err != nil {
			b.readObjectErrorCounter.Inc(1)
			b.logger.Error("Attribute blob cache fill failed",
				tag.Path(path),
				tag.StoreID(storeID),
				tag.Error(err))
			return nil, fmt.Errorf("failed to fill object cache: %w", err)
		}
	}
	b.readObjectSuccessHistogram.Record(time.Duration(len(data)))
	return data, nil
}

func (b *blobStoreImpl) readCachedAttributeObject(storeID, path string) ([]byte, bool, error) {
	data, found, err := b.blobCache.Get(b.attributeCacheKey(storeID, path))
	if err != nil {
		b.recordBlobCacheError()
		return nil, false, err
	}
	if !found {
		b.blobCacheMissCounter.Inc(1)
		return nil, false, nil
	}
	b.blobCacheHitCounter.Inc(1)
	return data, true, nil
}

func (b *blobStoreImpl) cacheAttributeObject(
	storeID string,
	path string,
	data []byte,
	successCounter client.MetricsCounter,
) error {
	if b.blobCache == nil {
		return nil
	}
	cached, err := b.blobCache.Put(b.attributeCacheKey(storeID, path), data)
	if err != nil {
		b.recordBlobCacheError()
		return err
	}
	if !cached {
		b.blobCacheBypassCounter.Inc(1)
		return nil
	}
	successCounter.Inc(1)
	return nil
}

func (b *blobStoreImpl) attributeCacheKey(storeID, path string) string {
	return fmt.Sprintf(
		"%d:%s%d:%s%d:%s",
		len(b.pathPrefix),
		b.pathPrefix,
		len(storeID),
		storeID,
		len(path),
		path,
	)
}

func (b *blobStoreImpl) recordBlobCacheError() {
	b.blobCacheErrorCounter.Inc(1)
}

func (b *blobStoreImpl) readObject(
	ctx context.Context,
	storage config.BlobStoreConfigEntry,
	path string,
) ([]byte, error) {
	switch storage.StorageType {
	case config.StorageTypeS3:
		if b.s3Client == nil {
			return nil, errors.New("S3 client is not configured")
		}
		return getObject(ctx, b.s3Client, storage.S3Bucket, b.objectKey(storage, path))
	case config.StorageTypeLocal:
		return readLocalObject(ctx, storage.LocalDirectory, b.objectKey(storage, path))
	default:
		return nil, fmt.Errorf("unsupported blob storage type %q", storage.StorageType)
	}
}

func (b *blobStoreImpl) Close() error {
	if b.blobCache == nil {
		return nil
	}
	return b.blobCache.Close()
}

// IsObjectNotFound reports whether a backend object is absent.
func IsObjectNotFound(err error) bool {
	if errors.Is(err, fs.ErrNotExist) {
		return true
	}
	var apiError smithy.APIError
	if !errors.As(err, &apiError) {
		return false
	}
	return apiError.ErrorCode() == "NoSuchKey" || apiError.ErrorCode() == "NotFound"
}

// IsObjectUnavailable reports whether persisted data cannot exist in this server configuration.
func IsObjectUnavailable(err error) bool {
	return errors.Is(err, errStoreNotFound) || IsObjectNotFound(err)
}

func putObject(ctx context.Context, client *s3.Client, bucketName string, key string, content []byte) error {
	_, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(bucketName),
		Key:         aws.String(key),
		Body:        bytes.NewReader(content),
		ContentType: aws.String("application/octet-stream"),
	})
	return err
}

func getObject(ctx context.Context, client *s3.Client, bucketName, key string) ([]byte, error) {
	result, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}

	data, readErr := io.ReadAll(result.Body)
	_ = result.Body.Close()
	if readErr != nil {
		return nil, readErr
	}
	return data, nil
}

func (b *blobStoreImpl) CountWorkflowObjectsForTesting(ctx context.Context, flowID string) (int64, error) {
	// Create the prefix to match objects for this Flow for today.
	yymmdd := formatBlobStorageDate(time.Now())
	prefix := b.objectKey(b.activeStorage, workflowDirectory(b.isMultiTenant, yymmdd, flowID)+"/")
	if b.activeStorage.StorageType == config.StorageTypeLocal {
		return countLocalObjects(ctx, b.activeStorage.LocalDirectory, prefix)
	}
	if b.s3Client == nil {
		return 0, errors.New("S3 client is not configured")
	}

	// List objects with the prefix (limited to 1000 objects as documented)
	result, err := b.s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(b.activeStorage.S3Bucket),
		Prefix: aws.String(prefix),
	})
	if err != nil {
		return 0, err
	}

	return int64(len(result.Contents)), nil
}

func (b *blobStoreImpl) DeleteWorkflowObjects(ctx context.Context, storeId, workflowPath string) error {
	storeConfig, ok := b.supportedStore[storeId]
	if !ok {
		return fmt.Errorf("%w for %s", errStoreNotFound, storeId)
	}
	if storeConfig.StorageType == config.StorageTypeLocal {
		if err := deleteLocalWorkflowObjects(
			ctx,
			storeConfig.LocalDirectory,
			b.objectKey(storeConfig, workflowPath),
		); err != nil {
			return err
		}
		b.logger.Info("DeleteWorkflowObjects completed",
			tag.StoreID(storeId),
			tag.WorkflowPath(workflowPath))
		return nil
	}
	if b.s3Client == nil {
		return errors.New("S3 client is not configured")
	}

	// Construct the prefix for all objects of this workflow
	prefix := b.objectKey(storeConfig, workflowPath+"/")

	// Paginate through all objects and delete them in batches
	var continuationToken *string
	var totalDeleted int
	for {
		listInput := &s3.ListObjectsV2Input{
			Bucket: aws.String(storeConfig.S3Bucket),
			Prefix: aws.String(prefix),
		}

		if continuationToken != nil {
			listInput.ContinuationToken = continuationToken
		}

		listResult, err := b.s3Client.ListObjectsV2(ctx, listInput)
		if err != nil {
			var re s3.ResponseError
			if errors.As(err, &re) {
				b.logger.Error("ListObjectsV2 S3 API error",
					tag.RequestID(re.ServiceRequestID()),
					tag.HostID(re.ServiceHostID()),
					tag.Bucket(storeConfig.S3Bucket),
					tag.WorkflowPath(workflowPath),
					tag.Error(err))
				return fmt.Errorf("failed to list objects for deletion (requestId=%s, hostId=%s): %w",
					re.ServiceRequestID(), re.ServiceHostID(), err)
			}
			return fmt.Errorf("failed to list objects for deletion: %w", err)
		}

		// If no objects found, we're done
		if len(listResult.Contents) == 0 {
			break
		}

		// Prepare objects for batch deletion
		var objectsToDelete []types.ObjectIdentifier
		for _, obj := range listResult.Contents {
			if obj.Key != nil {
				objectsToDelete = append(objectsToDelete, types.ObjectIdentifier{
					Key: obj.Key,
				})
			}
		}

		// Delete objects in batch
		if len(objectsToDelete) > 0 {
			deleteResult, err := b.s3Client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
				Bucket: aws.String(storeConfig.S3Bucket),
				Delete: &types.Delete{
					Objects: objectsToDelete,
					Quiet:   aws.Bool(true), // Don't return successful deletions
				},
			})
			if err != nil {
				// Log S3-specific request identifiers for debugging with AWS Support
				var re s3.ResponseError
				if errors.As(err, &re) {
					b.logger.Error("DeleteObjects S3 API error",
						tag.RequestID(re.ServiceRequestID()),
						tag.HostID(re.ServiceHostID()),
						tag.Bucket(storeConfig.S3Bucket),
						tag.WorkflowPath(workflowPath),
						tag.Error(err))
					return fmt.Errorf("failed to delete objects (requestId=%s, hostId=%s): %w",
						re.ServiceRequestID(), re.ServiceHostID(), err)
				}
				return fmt.Errorf("failed to delete objects: %w", err)
			}

			// Check for any delete errors
			if len(deleteResult.Errors) > 0 {
				var errorMsgs []string
				for _, delErr := range deleteResult.Errors {
					if delErr.Key != nil && delErr.Code != nil && delErr.Message != nil {
						errorMsgs = append(errorMsgs, fmt.Sprintf("key=%s, code=%s, message=%s",
							*delErr.Key, *delErr.Code, *delErr.Message))
					}
				}
				resultMetadata := fmt.Sprintf("%+v", deleteResult.ResultMetadata)
				b.logger.Error("DeleteObjects failed",
					tag.Bucket(storeConfig.S3Bucket),
					tag.WorkflowPath(workflowPath),
					tag.ResultMetadata(resultMetadata))
				return fmt.Errorf("some objects failed to delete (resultMetadata=%s): %s", resultMetadata, strings.Join(errorMsgs, "; "))
			}
			for _, deletedObject := range objectsToDelete {
				b.logger.Info("Blob object deleted",
					tag.StoreID(storeId),
					tag.Path(aws.ToString(deletedObject.Key)))
			}

			totalDeleted += len(objectsToDelete)
		}

		// Check if there are more objects to process
		if listResult.IsTruncated == nil || !*listResult.IsTruncated {
			break
		}
		continuationToken = listResult.NextContinuationToken
	}

	b.logger.Info("DeleteWorkflowObjects completed",
		tag.StoreID(storeId),
		tag.Bucket(storeConfig.S3Bucket),
		tag.WorkflowPath(workflowPath),
		tag.TotalDeleted(totalDeleted))

	return nil
}

func (b *blobStoreImpl) ListWorkflowPaths(ctx context.Context, input ListObjectPathsInput) (*ListObjectPathsOutput, error) {
	storeConfig, ok := b.supportedStore[input.StoreId]
	if !ok {
		return nil, fmt.Errorf("%w for %s", errStoreNotFound, input.StoreId)
	}
	relativeDirectory := ""
	if input.TenantID != "" {
		relativeDirectory = input.TenantID + "/"
	}
	var names []string
	var continuationToken *string
	if storeConfig.StorageType == config.StorageTypeLocal {
		output, err := listLocalWorkflowPaths(
			ctx,
			storeConfig.LocalDirectory,
			strings.TrimSuffix(b.objectKey(storeConfig, relativeDirectory), "/"),
			input.ContinuationToken,
		)
		if err != nil {
			return nil, err
		}
		names, continuationToken = output.WorkflowPaths, output.ContinuationToken
	} else {
		if b.s3Client == nil {
			return nil, errors.New("S3 client is not configured")
		}
		listPrefix := b.objectKey(storeConfig, relativeDirectory)
		result, err := b.s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(storeConfig.S3Bucket),
			Prefix:            aws.String(listPrefix),
			Delimiter:         aws.String("/"),
			ContinuationToken: input.ContinuationToken,
		})
		if err != nil {
			return nil, err
		}
		for _, commonPrefix := range result.CommonPrefixes {
			if commonPrefix.Prefix != nil && strings.HasPrefix(*commonPrefix.Prefix, listPrefix) {
				names = append(names, strings.TrimSuffix(strings.TrimPrefix(*commonPrefix.Prefix, listPrefix), "/"))
			}
		}
		continuationToken = result.NextContinuationToken
	}
	output := &ListObjectPathsOutput{ContinuationToken: continuationToken}
	for _, name := range names {
		// Workflow directories always contain "$"; a root directory without one is a tenant segment.
		if input.TenantID == "" && !strings.Contains(name, "$") {
			output.TenantSegments = append(output.TenantSegments, name)
			continue
		}
		output.WorkflowPaths = append(output.WorkflowPaths, relativeDirectory+name)
	}
	return output, nil
}

// objectKey returns the storage key of a namespace-relative path: <s3KeyPrefix><namespace>/<path> for S3.
func (b *blobStoreImpl) objectKey(storage config.BlobStoreConfigEntry, path string) string {
	if storage.StorageType == config.StorageTypeS3 {
		return storage.S3KeyPrefix + b.pathPrefix + path
	}
	return b.pathPrefix + path
}

func (b *blobStoreImpl) ReadTenancyRecord(ctx context.Context) ([]byte, string, error) {
	key := b.objectKey(b.activeStorage, tenancyRecordPath)
	if b.activeStorage.StorageType == config.StorageTypeLocal {
		return readLocalObjectWithVersion(ctx, b.activeStorage.LocalDirectory, key)
	}
	if b.s3Client == nil {
		return nil, "", errors.New("S3 client is not configured")
	}
	return getS3ObjectWithVersion(ctx, b.s3Client, b.activeStorage.S3Bucket, key)
}

func (b *blobStoreImpl) WriteTenancyRecord(ctx context.Context, data []byte, version string) error {
	key := b.objectKey(b.activeStorage, tenancyRecordPath)
	if b.activeStorage.StorageType == config.StorageTypeLocal {
		return writeLocalObjectIfVersion(ctx, b.activeStorage.LocalDirectory, key, data, version)
	}
	if b.s3Client == nil {
		return errors.New("S3 client is not configured")
	}
	return putS3ObjectIfVersion(ctx, b.s3Client, b.activeStorage.S3Bucket, key, data, version)
}

func (b *blobStoreImpl) DeleteTenantObjects(ctx context.Context, tenantID string, limit int) (int, error) {
	if !tenantSegmentPattern.MatchString(tenantID) {
		return 0, fmt.Errorf("%w: %q", ErrInvalidTenantSegment, tenantID)
	}
	storeIDs := make([]string, 0, len(b.supportedStore))
	for storeID := range b.supportedStore {
		storeIDs = append(storeIDs, storeID)
	}
	sort.Strings(storeIDs)
	deleted := 0
	for _, storeID := range storeIDs {
		if deleted >= limit {
			break
		}
		storage := b.supportedStore[storeID]
		prefix := b.objectKey(storage, tenantID+"/")
		var count int
		var err error
		if storage.StorageType == config.StorageTypeLocal {
			count, err = deleteLocalObjectsUnderDirectory(ctx, storage.LocalDirectory, prefix, limit-deleted)
		} else if b.s3Client != nil {
			count, err = deleteS3ObjectsUnderPrefix(ctx, b.s3Client, storage.S3Bucket, prefix, limit-deleted)
		}
		deleted += count
		if err != nil {
			return deleted, fmt.Errorf("delete tenant objects in %s: %w", storeID, err)
		}
	}
	return deleted, nil
}
