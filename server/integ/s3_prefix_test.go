// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package integ

import (
	"context"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/superdurable/dex/service"
	"github.com/superdurable/dex/service/bootstrap"
	"github.com/superdurable/dex/service/common/blobstore"
)

func TestS3KeyPrefixLayoutAndCleanupTemporal(t *testing.T) {
	if !*temporalIntegTest || *dexServerAddress != "" {
		t.Skip()
	}
	keyPrefix := "prefix-" + strings.ReplaceAll(uuid.NewString(), "-", "")[:12] + "/"
	runtime := startDexService(t, DexServiceTestConfig{
		BackendType:     service.BackendTypeTemporal,
		S3TestThreshold: 10,
		S3KeyPrefix:     keyPrefix,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	configForS3 := createTestConfig(t, DexServiceTestConfig{
		BackendType:     service.BackendTypeTemporal,
		S3TestThreshold: 10,
		S3KeyPrefix:     keyPrefix,
	})
	s3Client, err := bootstrap.CreateS3Client(ctx, &configForS3)
	require.NoError(t, err)

	flowID := "deleted-flow-" + uuid.NewString()
	_, locator, err := globalBlobStore.WriteObject(ctx, flowID, "value", []byte(strings.Repeat("v", 64)))
	require.NoError(t, err)
	objectPath, err := blobstore.ValueObjectPath(flowID, locator)
	require.NoError(t, err)
	expectedKey := keyPrefix + testNamespace + "/" + objectPath
	require.Regexp(t, "^"+regexp.QuoteMeta(keyPrefix+testNamespace+"/")+`[0-9]{6}\$`+regexp.QuoteMeta(flowID)+`/`, expectedKey)

	listedObjects, err := s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String("dex-test-bucket"),
		Prefix: aws.String(keyPrefix + testNamespace + "/"),
	})
	require.NoError(t, err)
	require.Len(t, listedObjects.Contents, 1)
	require.Equal(t, expectedKey, aws.ToString(listedObjects.Contents[0].Key))

	workflowPath := strings.SplitN(objectPath, "/", 2)[0]
	listedPaths, err := globalBlobStore.ListWorkflowPaths(ctx, blobstore.ListObjectPathsInput{StoreId: "s3-store-id"})
	require.NoError(t, err)
	require.Contains(t, listedPaths.WorkflowPaths, workflowPath)

	cleanupFlowID := "cleanup-" + uuid.NewString()
	require.NoError(t, runtime.UnifiedClient.StartBlobStoreCleanupWorkflow(
		ctx, service.TaskQueue, cleanupFlowID, "", "s3-store-id",
	))
	_, err = runtime.UnifiedClient.GetWorkflowResult(ctx, nil, cleanupFlowID, "")
	require.NoError(t, err)

	listedObjects, err = s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String("dex-test-bucket"),
		Prefix: aws.String(keyPrefix + testNamespace + "/"),
	})
	require.NoError(t, err)
	require.Empty(t, listedObjects.Contents)
	objectCount, err := globalBlobStore.CountWorkflowObjectsForTesting(ctx, flowID)
	require.NoError(t, err)
	require.Zero(t, objectCount)
}
