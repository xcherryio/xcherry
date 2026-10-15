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
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/superdurable/dex/config"
	"github.com/superdurable/dex/gen/dexpb"
	"github.com/superdurable/dex/service/common/log/loggerimpl"
	"github.com/superdurable/dex/service/common/ptr"
	"go.temporal.io/sdk/client"
)

const (
	testBucket    = "dex-test-bucket"
	testRegion    = "us-east-1"
	testEndpoint  = "http://localhost:9000"
	testAccessKey = "minioadmin"
	testSecretKey = "minioadmin"
	testNamespace = "default"
	testStorageId = "test-storage-id"
)

func createTestBlobStore(t *testing.T) BlobStore {
	return createTestBlobStoreWithCache(t, config.BlobCacheConfig{})
}

func createTestBlobStoreWithCache(t *testing.T, cacheConfig config.BlobCacheConfig) BlobStore {
	// Create S3 client for MinIO
	cfg, err := awsconfig.LoadDefaultConfig(context.Background(),
		awsconfig.WithRegion(testRegion),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(testAccessKey, testSecretKey, "")),
		awsconfig.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(
			func(service, region string, options ...interface{}) (aws.Endpoint, error) {
				return aws.Endpoint{
					URL: testEndpoint,
				}, nil
			})),
	)
	assert.NoError(t, err)

	s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})

	// Create bucket if it doesn't exist
	_, err = s3Client.HeadBucket(context.Background(), &s3.HeadBucketInput{
		Bucket: aws.String(testBucket),
	})
	if err != nil {
		// Bucket doesn't exist, create it
		_, err = s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
			Bucket: aws.String(testBucket),
		})
		assert.NoError(t, err)
	}

	// Create test configuration
	storeConfig := config.BlobStoreConfig{
		Enabled:          ptr.Any(true),
		ThresholdInBytes: 100,
		BlobCache:        cacheConfig,
		SupportedStorages: []config.BlobStoreConfigEntry{
			{
				Status:      config.StorageStatusActive,
				StorageId:   testStorageId,
				StorageType: config.StorageTypeS3,
				S3Endpoint:  testEndpoint,
				S3Bucket:    testBucket,
				S3Region:    testRegion,
				S3AccessKey: testAccessKey,
				S3SecretKey: testSecretKey,
			},
		},
	}

	logger, err := loggerimpl.NewDevelopment()
	assert.NoError(t, err)
	blobStore, err := NewBlobStore(s3Client, testNamespace, &storeConfig, logger, client.MetricsNopHandler)
	assert.NoError(t, err)
	assert.NotNil(t, blobStore)
	t.Cleanup(func() { assert.NoError(t, blobStore.Close()) })

	return blobStore
}

func TestBlobStoreIntegration(t *testing.T) {
	// Skip if not running integration tests or MinIO is not available
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	blobStore := createTestBlobStore(t)
	ctx := context.Background()

	// Test data
	workflowId1 := "test-workflow-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	workflowId2 := "test-workflow-" + strconv.FormatInt(time.Now().UnixNano()+1, 10)
	testData := "test data content"

	t.Run("WriteAndCountObjects", func(t *testing.T) {
		// Initial count should be 0
		count, err := blobStore.CountWorkflowObjectsForTesting(ctx, workflowId1)
		assert.NoError(t, err)
		assert.Equal(t, int64(0), count)

		// Write first object
		storeId1, path1, err := blobStore.WriteObject(
			ctx,
			workflowId1,
			"write-1",
			[]byte(testData),
		)
		assert.NoError(t, err)
		assert.Equal(t, testStorageId, storeId1)
		assert.NotEmpty(t, path1)

		// Count should be 1
		count, err = blobStore.CountWorkflowObjectsForTesting(ctx, workflowId1)
		assert.NoError(t, err)
		assert.Equal(t, int64(1), count)

		// Write second object for same workflow
		storeId2, path2, err := blobStore.WriteObject(
			ctx,
			workflowId1,
			"write-2",
			[]byte(testData+"2"),
		)
		assert.NoError(t, err)
		assert.Equal(t, testStorageId, storeId2)
		assert.NotEmpty(t, path2)
		assert.NotEqual(t, path1, path2)

		// Count should be 2
		count, err = blobStore.CountWorkflowObjectsForTesting(ctx, workflowId1)
		assert.NoError(t, err)
		assert.Equal(t, int64(2), count)

		// Write object for different workflow
		storeId3, path3, err := blobStore.WriteObject(
			ctx,
			workflowId2,
			"write-3",
			[]byte(testData+"3"),
		)
		assert.NoError(t, err)
		assert.Equal(t, testStorageId, storeId3)
		assert.NotEmpty(t, path3)

		// Count for first workflow should still be 2
		count, err = blobStore.CountWorkflowObjectsForTesting(ctx, workflowId1)
		assert.NoError(t, err)
		assert.Equal(t, int64(2), count)

		// Count for second workflow should be 1
		count, err = blobStore.CountWorkflowObjectsForTesting(ctx, workflowId2)
		assert.NoError(t, err)
		assert.Equal(t, int64(1), count)
	})

	t.Run("ReadObjects", func(t *testing.T) {
		// Write an object
		storeId, path, err := blobStore.WriteObject(
			ctx,
			workflowId1,
			"read",
			[]byte(testData),
		)
		assert.NoError(t, err)

		// Read it back
		retrievedData, err := blobStore.ReadObject(ctx, storeId, workflowId1, path)
		assert.NoError(t, err)
		assert.Equal(t, []byte(testData), retrievedData)
	})

	t.Run("ListWorkflowPaths", func(t *testing.T) {
		// Write objects for multiple workflows
		_, _, err := blobStore.WriteObject(
			ctx,
			workflowId1,
			"list-1",
			[]byte(testData),
		)
		assert.NoError(t, err)
		_, _, err = blobStore.WriteObject(
			ctx,
			workflowId2,
			"list-2",
			[]byte(testData),
		)
		assert.NoError(t, err)

		// List workflow paths
		input := ListObjectPathsInput{
			StoreId: testStorageId,
		}
		output, err := blobStore.ListWorkflowPaths(ctx, input)
		assert.NoError(t, err)
		assert.NotNil(t, output)

		// Should contain both workflow paths
		assert.True(t, len(output.WorkflowPaths) >= 2)

		// Verify workflow paths contain expected patterns
		todayPrefix := formatBlobStorageDate(time.Now())
		expectedPath1 := fmt.Sprintf("%s$%s", todayPrefix, encodeFlowIDPathPart(workflowId1))
		expectedPath2 := fmt.Sprintf("%s$%s", todayPrefix, encodeFlowIDPathPart(workflowId2))

		foundPath1 := false
		foundPath2 := false
		for _, path := range output.WorkflowPaths {
			if path == expectedPath1 {
				foundPath1 = true
			}
			if path == expectedPath2 {
				foundPath2 = true
			}
		}
		assert.True(t, foundPath1, "Expected path for workflowId1 not found")
		assert.True(t, foundPath2, "Expected path for workflowId2 not found")
	})

	t.Run("DeleteWorkflowObjects", func(t *testing.T) {
		// Create a new workflow ID for this test
		deleteTestWorkflowId := "delete-test-workflow-" + strconv.FormatInt(time.Now().UnixNano(), 10)

		// Write multiple objects for the workflow
		_, _, err := blobStore.WriteObject(
			ctx,
			deleteTestWorkflowId,
			"delete-1",
			[]byte("test data 1"),
		)
		assert.NoError(t, err)
		_, _, err = blobStore.WriteObject(
			ctx,
			deleteTestWorkflowId,
			"delete-2",
			[]byte("test data 2"),
		)
		assert.NoError(t, err)
		_, _, err = blobStore.WriteObject(
			ctx,
			deleteTestWorkflowId,
			"delete-3",
			[]byte("test data 3"),
		)
		assert.NoError(t, err)

		// Verify objects exist
		count, err := blobStore.CountWorkflowObjectsForTesting(ctx, deleteTestWorkflowId)
		assert.NoError(t, err)
		assert.Equal(t, int64(3), count)

		// Delete all objects for the workflow
		todayPrefix := formatBlobStorageDate(time.Now())
		workflowPath := fmt.Sprintf("%s$%s", todayPrefix, encodeFlowIDPathPart(deleteTestWorkflowId))
		err = blobStore.DeleteWorkflowObjects(ctx, testStorageId, workflowPath)
		assert.NoError(t, err)

		// Verify objects are deleted
		count, err = blobStore.CountWorkflowObjectsForTesting(ctx, deleteTestWorkflowId)
		assert.NoError(t, err)
		assert.Equal(t, int64(0), count)
	})

	t.Run("DeleteWorkflowObjectsMultiple", func(t *testing.T) {
		// Create a workflow with more objects to test pagination handling
		multiDeleteWorkflowId := "multi-delete-workflow-" + strconv.FormatInt(time.Now().UnixNano(), 10)

		// Create 10 objects for this workflow
		numObjects := 10
		for i := 0; i < numObjects; i++ {
			data := fmt.Sprintf("test data %d", i)
			_, _, err := blobStore.WriteObject(
				ctx,
				multiDeleteWorkflowId,
				"multi-delete-"+strconv.Itoa(i),
				[]byte(data),
			)
			assert.NoError(t, err)
		}

		// Verify all objects exist
		count, err := blobStore.CountWorkflowObjectsForTesting(ctx, multiDeleteWorkflowId)
		assert.NoError(t, err)
		assert.Equal(t, int64(numObjects), count)

		// Delete all objects for the workflow
		todayPrefix := formatBlobStorageDate(time.Now())
		workflowPath := fmt.Sprintf("%s$%s", todayPrefix, encodeFlowIDPathPart(multiDeleteWorkflowId))
		err = blobStore.DeleteWorkflowObjects(ctx, testStorageId, workflowPath)
		assert.NoError(t, err)

		// Verify all objects are deleted
		count, err = blobStore.CountWorkflowObjectsForTesting(ctx, multiDeleteWorkflowId)
		assert.NoError(t, err)
		assert.Equal(t, int64(0), count)
	})

	t.Run("DeleteWorkflowObjectsNonExistent", func(t *testing.T) {
		// Try to delete objects for a workflow that doesn't exist
		todayPrefix := formatBlobStorageDate(time.Now())
		workflowPath := fmt.Sprintf("%s$%s", todayPrefix, encodeFlowIDPathPart("non-existent-workflow"))
		err := blobStore.DeleteWorkflowObjects(ctx, testStorageId, workflowPath)
		assert.NoError(t, err) // Should succeed even if no objects to delete
	})

	t.Run("ErrorHandling", func(t *testing.T) {
		// Test with invalid store ID
		input := ListObjectPathsInput{
			StoreId: "invalid-store-id",
		}
		_, err := blobStore.ListWorkflowPaths(ctx, input)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "store not found")

		// Test reading with invalid store ID
		_, err = blobStore.ReadObject(
			ctx, "invalid-store-id", "flow", mustBlobReferenceDate(t, time.Now())+"/0000000000",
		)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "store not found")

		// Test delete with invalid store ID
		err = blobStore.DeleteWorkflowObjects(ctx, "invalid-store-id", "some-workflow-path")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "store not found")

		// Test reading a non-existent key from a valid store triggers the new error wrapping
		_, err = blobStore.ReadObject(
			ctx, testStorageId, "flow", mustBlobReferenceDate(t, time.Now())+"/0000000000",
		)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to read object")
	})

	t.Run("DeterministicObjectIdentity", func(t *testing.T) {
		workflowID := "deterministic-workflow-" + strconv.FormatInt(time.Now().UnixNano(), 10)
		payload := []byte("payload")

		_, firstPath, err := blobStore.WriteObject(ctx, workflowID, "request-1", payload)
		assert.NoError(t, err)
		_, retryPath, err := blobStore.WriteObject(ctx, workflowID, "request-1", payload)
		assert.NoError(t, err)
		assert.Equal(t, firstPath, retryPath)

		count, err := blobStore.CountWorkflowObjectsForTesting(ctx, workflowID)
		assert.NoError(t, err)
		assert.Equal(t, int64(1), count)

		distinctInputs := []struct {
			invocationId string
			data         []byte
		}{
			{invocationId: "request-1", data: []byte("other-payload")},
			{invocationId: "request-2", data: payload},
		}
		for _, input := range distinctInputs {
			_, path, writeErr := blobStore.WriteObject(ctx, workflowID, input.invocationId, input.data)
			assert.NoError(t, writeErr)
			assert.NotEqual(t, firstPath, path)
		}

		count, err = blobStore.CountWorkflowObjectsForTesting(ctx, workflowID)
		assert.NoError(t, err)
		assert.Equal(t, int64(3), count)
	})

	t.Run("ValueRoundTrip", func(t *testing.T) {
		workflowID := "round-trip-workflow-" + strconv.FormatInt(time.Now().UnixNano(), 10)
		stringValue := &dexpb.Value{
			Kind: &dexpb.Value_StringValue{StringValue: "large string value"},
		}
		objectPayload := []byte{0x00, 0x01, 0xfe, 0xff}
		objectValue := &dexpb.Value{
			Kind: &dexpb.Value_ObjValue{
				ObjValue: &dexpb.EncodedObject{Encoding: "binary", Payload: objectPayload},
			},
		}

		err := OffloadLargeValue(ctx, stringValue, workflowID, "round-trip", 1, blobStore, true)
		assert.NoError(t, err)
		err = OffloadLargeValue(ctx, objectValue, workflowID, "round-trip", 1, blobStore, true)
		assert.NoError(t, err)
		err = HydrateValue(ctx, workflowID, stringValue, blobStore)
		assert.NoError(t, err)
		err = HydrateValue(ctx, workflowID, objectValue, blobStore)
		assert.NoError(t, err)

		assert.Equal(t, "large string value", stringValue.GetStringValue())
		assert.Equal(t, "binary", objectValue.GetObjValue().GetEncoding())
		assert.Equal(t, objectPayload, objectValue.GetObjValue().GetPayload())
	})
}

func mustBlobReferenceDate(t *testing.T, timestamp time.Time) string {
	t.Helper()
	referenceDate, err := formatBlobReferenceDate(timestamp)
	require.NoError(t, err)
	return referenceDate
}
