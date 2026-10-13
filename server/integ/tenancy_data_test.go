// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package integ

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/superdurable/dex/config"
	"github.com/superdurable/dex/gen/dexpb"
	"github.com/superdurable/dex/service"
	"github.com/superdurable/dex/service/bootstrap"
	"github.com/superdurable/dex/service/tenancy"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const tenantBlobTestBucket = "dex-test-bucket"

// TestTenantBlobLayoutAndCleanupTemporal covers tenant object keys and cleanup by stored Flow ID on MinIO.
func TestTenantBlobLayoutAndCleanupTemporal(t *testing.T) {
	if !*temporalIntegTest || *dexServerAddress != "" {
		t.Skip()
	}
	keyPrefix := "prefix-" + strings.ReplaceAll(uuid.NewString(), "-", "")[:12] + "/"
	confirmationDelay := 5 * time.Second
	pair := startTenantPair(t, func(testConfig *DexServiceTestConfig) {
		testConfig.S3TestThreshold = 10
		testConfig.S3KeyPrefix = keyPrefix
		testConfig.TenantCleanupConfirmationDelay = confirmationDelay
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	s3Client := newTenantTestS3Client(t, ctx)
	listKeys := func(prefix string) []string {
		output, err := s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket: aws.String(tenantBlobTestBucket), Prefix: aws.String(prefix),
		})
		require.NoError(t, err)
		keys := []string{}
		for _, object := range output.Contents {
			keys = append(keys, aws.ToString(object.Key))
		}
		return keys
	}
	namespacePrefix := keyPrefix + testNamespace + "/"

	sharedFlowID := "blobs-" + uuid.NewString()
	largeValue := strings.Repeat("v", 64)
	for _, tenant := range []struct {
		client dexpb.FlowServiceClient
		target *dexpb.WorkerTarget
	}{{pair.alpha, pair.alphaTarget}, {pair.beta, pair.betaTarget}} {
		startTenantProbeFlow(t, ctx, tenant.client, tenant.target, sharedFlowID, tenantProbeFlowType, stringValue(largeValue))
	}

	t.Run("each tenant's objects lie under its own prefix with the application Flow ID", func(t *testing.T) {
		for _, tenantID := range []string{"alpha", "beta"} {
			keys := listKeys(namespacePrefix + tenantID + "/")
			require.NotEmpty(t, keys, tenantID)
			for _, key := range keys {
				require.Contains(t, key, "$"+sharedFlowID, key)
				require.NotContains(t, key, "T%3A", "keys carry the application Flow ID, not the stored ID")
			}
		}
	})

	ghostFlowID := "ghost-" + uuid.NewString()
	for _, tenantID := range []string{"alpha", "beta"} {
		_, _, err := globalBlobStore.WriteObject(ctx, tenancy.StoredFlowID(tenantID, ghostFlowID), "ghost", []byte(largeValue))
		require.NoError(t, err)
	}
	invalidSegmentKey := namespacePrefix + "Not-A-Tenant/261012$x/object"
	_, err := s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(tenantBlobTestBucket), Key: aws.String(invalidSegmentKey), Body: bytes.NewReader([]byte("x")),
	})
	require.NoError(t, err)

	t.Run("cleanup keeps running Flows, deletes deleted ones after two NotFounds, and skips invalid tenants", func(t *testing.T) {
		cleanupStarted := time.Now()
		cleanupWorkflowID := "test-cleanup-" + uuid.NewString()
		require.NoError(t, pair.runtime.UnifiedClient.StartBlobStoreCleanupWorkflow(
			ctx, service.TaskQueue, cleanupWorkflowID, "", "s3-store-id",
		))
		_, err := pair.runtime.UnifiedClient.GetWorkflowResult(ctx, nil, cleanupWorkflowID, "")
		require.NoError(t, err)
		require.GreaterOrEqual(t, time.Since(cleanupStarted), confirmationDelay,
			"tenant paths are deleted only after the confirmation delay")

		for _, tenantID := range []string{"alpha", "beta"} {
			keys := listKeys(namespacePrefix + tenantID + "/")
			hasRunningFlowObject, hasGhostObject := false, false
			for _, key := range keys {
				hasRunningFlowObject = hasRunningFlowObject || strings.Contains(key, "$"+sharedFlowID)
				hasGhostObject = hasGhostObject || strings.Contains(key, "$"+ghostFlowID)
			}
			require.True(t, hasRunningFlowObject, "the running Flow of %s keeps its blobs", tenantID)
			require.False(t, hasGhostObject, "the deleted Flow of %s loses its blobs", tenantID)
		}
		require.Equal(t, []string{invalidSegmentKey}, listKeys(invalidSegmentKey))
	})
}

func newTenantTestS3Client(t *testing.T, ctx context.Context) *s3.Client {
	t.Helper()
	cfg := createTestConfig(t, DexServiceTestConfig{BackendType: service.BackendTypeTemporal, S3TestThreshold: 10})
	s3Client, err := bootstrap.CreateS3Client(ctx, &cfg)
	require.NoError(t, err)
	return s3Client
}

func TestTenantStreamBudgetsTemporal(t *testing.T) {
	if !*temporalIntegTest || *dexServerAddress != "" {
		t.Skip()
	}
	for _, backend := range []config.StreamStoreBackend{config.StreamStoreBackendMemory, config.StreamStoreBackendRedis} {
		t.Run(string(backend), func(t *testing.T) {
			if backend == config.StreamStoreBackendRedis {
				deleteTenantStreamKeys(t, "alpha", "beta")
			}
			pair := startTenantPair(t, func(testConfig *DexServiceTestConfig) {
				testConfig.StreamStore = config.StreamStoreConfig{Backend: backend}
				if backend == config.StreamStoreBackendRedis {
					testConfig.StreamStore.RedisURL = streamTestRedisURL
				}
				testConfig.Tenancy.Limits.MaxStreamCapacityBytes = 4096
				testConfig.Tenancy.Limits.MaxStreamBytesPerTenant = 1 << 20
			})
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			flowType, streamName := "StreamBudgetFlow", "events-"+uuid.NewString()
			write := func(client dexpb.FlowServiceClient, flowID string, size int) error {
				_, err := client.WriteStream(ctx, &dexpb.WriteStreamRequest{
					FlowId: flowID, FlowType: flowType, StreamName: streamName, Source: "test",
					StreamCapacityBytes: 1 << 20, Value: stringValue(strings.Repeat("s", size)),
				})
				return err
			}

			flowID := "stream-" + uuid.NewString()
			require.NoError(t, write(pair.alpha, flowID, 1000))
			require.NoError(t, write(pair.alpha, flowID, 1000))
			err := write(pair.alpha, flowID, 3000)
			require.Equal(t, codes.ResourceExhausted, status.Code(err),
				"the requested 1 MiB capacity is capped at 4 KiB: %v", err)
			require.NoError(t, write(pair.beta, flowID, 3000),
				"equal Flow type and Stream name of another tenant has its own budget")

			if backend == config.StreamStoreBackendRedis {
				options, parseErr := redis.ParseURL(streamTestRedisURL)
				require.NoError(t, parseErr)
				redisClient := redis.NewClient(options)
				defer func() { require.NoError(t, redisClient.Close()) }()
				for _, tenantID := range []string{"alpha", "beta"} {
					keys, scanErr := redisClient.Keys(ctx, "dex:stream:v1:t:"+tenantID+":*").Result()
					require.NoError(t, scanErr)
					require.NotEmpty(t, keys, "tenant %s Stream keys share the t:<tenant>: prefix", tenantID)
				}
			}
		})
	}
}

func TestTenantStreamBytesPerTenantTemporal(t *testing.T) {
	if !*temporalIntegTest || *dexServerAddress != "" {
		t.Skip()
	}
	for _, backend := range []config.StreamStoreBackend{config.StreamStoreBackendMemory, config.StreamStoreBackendRedis} {
		t.Run(string(backend), func(t *testing.T) {
			if backend == config.StreamStoreBackendRedis {
				deleteTenantStreamKeys(t, "alpha", "beta")
			}
			pair := startTenantPair(t, func(testConfig *DexServiceTestConfig) {
				testConfig.StreamStore = config.StreamStoreConfig{Backend: backend}
				if backend == config.StreamStoreBackendRedis {
					testConfig.StreamStore.RedisURL = streamTestRedisURL
				}
				testConfig.Tenancy.Limits.MaxStreamCapacityBytes = 1 << 20
				testConfig.Tenancy.Limits.MaxStreamBytesPerTenant = 6000
			})
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			write := func(client dexpb.FlowServiceClient, streamName string) error {
				_, err := client.WriteStream(ctx, &dexpb.WriteStreamRequest{
					FlowId: "bytes-" + uuid.NewString(), FlowType: "StreamBytesFlow", StreamName: streamName,
					Source: "test", StreamCapacityBytes: 1 << 20, Value: stringValue(strings.Repeat("b", 2000)),
				})
				return err
			}
			streamPrefix := uuid.NewString()
			require.NoError(t, write(pair.alpha, streamPrefix+"-first"))
			require.NoError(t, write(pair.alpha, streamPrefix+"-second"))
			err := write(pair.alpha, streamPrefix+"-third")
			require.Equal(t, codes.ResourceExhausted, status.Code(err),
				"one tenant's Streams together stay within its budget: %v", err)
			require.NoError(t, write(pair.beta, streamPrefix+"-third"), "another tenant is unaffected")
		})
	}
}

// deleteTenantStreamKeys removes earlier runs' Stream keys of the fixed test tenants from the test Redis database.
func deleteTenantStreamKeys(t *testing.T, tenantIDs ...string) {
	t.Helper()
	options, err := redis.ParseURL(streamTestRedisURL)
	require.NoError(t, err)
	redisClient := redis.NewClient(options)
	defer func() { require.NoError(t, redisClient.Close()) }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, tenantID := range tenantIDs {
		keys, err := redisClient.Keys(ctx, "dex:stream:v1:t:"+tenantID+":*").Result()
		require.NoError(t, err)
		if len(keys) > 0 {
			require.NoError(t, redisClient.Del(ctx, keys...).Err())
		}
	}
}
