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
	"github.com/superdurable/dex/service/tenancy"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestTenantTombstoneAndPurgeTemporal(t *testing.T) {
	if !*temporalIntegTest || !*testSearchIntegTest || *dexServerAddress != "" {
		t.Skip()
	}
	keyPrefix := "purge-" + strings.ReplaceAll(uuid.NewString(), "-", "")[:12] + "/"
	deleteTenantStreamKeys(t, "alpha", "beta")
	pair := startTenantPair(t, func(testConfig *DexServiceTestConfig) {
		testConfig.S3TestThreshold = 10
		testConfig.S3KeyPrefix = keyPrefix
		testConfig.StreamStore = config.StreamStoreConfig{Backend: config.StreamStoreBackendRedis, RedisURL: streamTestRedisURL}
		testConfig.Tenancy.TombstoneRefreshInterval = time.Second
		testConfig.Tenancy.TombstoneRetention = 3 * time.Second
		testConfig.Tenancy.PurgeConfirmationDelay = 2 * time.Second
		testConfig.Tenancy.Limits.PurgeExecutionsPerSecond = 10
	})
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	operator := dexpb.NewTenantAdminServiceClient(newDexClientConnectionWithCredential(
		t, pair.runtime.PublicAddress, pair.environment.OperatorToken, config.DefaultGrpcMaxMessageBytes,
	))
	tenantAdmin := dexpb.NewTenantAdminServiceClient(newDexClientConnectionWithCredential(
		t, pair.runtime.PublicAddress, pair.environment.TenantJWT(t, "alpha"), config.DefaultGrpcMaxMessageBytes,
	))

	const openFlows = 12
	alphaFlowIDs := []string{}
	for i := 0; i < openFlows+1; i++ {
		flowID := "purge-" + uuid.NewString()
		alphaFlowIDs = append(alphaFlowIDs, flowID)
		startTenantProbeFlow(t, ctx, pair.alpha, pair.alphaTarget, flowID, tenantProbeFlowType, stringValue(strings.Repeat("p", 64)))
	}
	completedFlowID := alphaFlowIDs[openFlows]
	_, err := pair.alpha.PublishToChannel(ctx, &dexpb.PublishToChannelRequest{
		FlowId: completedFlowID, Messages: []*dexpb.ChannelMessage{{ChannelName: tenantProbeChannel}},
	})
	require.NoError(t, err)
	_, err = pair.alpha.WaitForFlow(ctx, &dexpb.WaitForFlowRequest{FlowId: completedFlowID, WaitTimeSeconds: 30})
	require.NoError(t, err)
	_, err = pair.alpha.WriteStream(ctx, &dexpb.WriteStreamRequest{
		FlowId: alphaFlowIDs[0], FlowType: tenantProbeFlowType, StreamName: "events", Source: "test",
		StreamCapacityBytes: 64 * 1024, Value: stringValue("alpha"),
	})
	require.NoError(t, err)
	betaFlowID := "survivor-" + uuid.NewString()
	startTenantProbeFlow(t, ctx, pair.beta, pair.betaTarget, betaFlowID, tenantProbeFlowType, stringValue(strings.Repeat("q", 64)))

	t.Run("a tenant credential is refused on TenantAdminService", func(t *testing.T) {
		_, err := tenantAdmin.DescribeTenant(ctx, &dexpb.DescribeTenantRequest{TenantId: "alpha"})
		require.Equal(t, codes.PermissionDenied, status.Code(err), "error: %v", err)
	})

	t.Run("a purge requires a tombstone", func(t *testing.T) {
		_, err := operator.PurgeTenant(ctx, &dexpb.PurgeTenantRequest{TenantId: "alpha"})
		require.Equal(t, codes.FailedPrecondition, status.Code(err), "error: %v", err)
		_, err = operator.TombstoneTenant(ctx, &dexpb.TombstoneTenantRequest{TenantId: "Not-A-Tenant"})
		require.Equal(t, codes.InvalidArgument, status.Code(err), "error: %v", err)
	})

	t.Run("a tombstoned tenant's valid credential is refused and another tenant is unaffected", func(t *testing.T) {
		response, err := operator.TombstoneTenant(ctx, &dexpb.TombstoneTenantRequest{TenantId: "alpha"})
		require.NoError(t, err)
		require.True(t, response.GetState().GetIsTombstoned())
		_, err = operator.TombstoneTenant(ctx, &dexpb.TombstoneTenantRequest{TenantId: "alpha"})
		require.NoError(t, err, "tombstoning is idempotent")
		require.Eventually(t, func() bool {
			_, err := pair.alpha.GetServerInfo(ctx, &emptypb.Empty{})
			return status.Code(err) == codes.PermissionDenied
		}, 10*time.Second, 100*time.Millisecond)
		_, err = pair.beta.GetFlowSummary(ctx, &dexpb.GetFlowSummaryRequest{FlowId: betaFlowID})
		require.NoError(t, err)
	})

	t.Run("purge removes executions, blobs and Stream keys at its rate and reports done after two empty passes", func(t *testing.T) {
		firstCallStarted := time.Now()
		first, err := operator.PurgeTenant(ctx, &dexpb.PurgeTenantRequest{TenantId: "alpha"})
		require.NoError(t, err)
		processed := first.GetTerminatedExecutions() + first.GetDeletedExecutions()
		require.GreaterOrEqual(t, processed, int32(openFlows), "the first batch handles every open execution")
		require.GreaterOrEqual(t, time.Since(firstCallStarted), time.Duration(processed-1)*100*time.Millisecond,
			"executions are paced at purgeExecutionsPerSecond")
		require.False(t, first.GetDone())

		var final *dexpb.PurgeTenantResponse
		emptyPasses := 0
		require.Eventually(t, func() bool {
			response, err := operator.PurgeTenant(ctx, &dexpb.PurgeTenantRequest{TenantId: "alpha"})
			require.NoError(t, err)
			if response.GetTerminatedExecutions()+response.GetDeletedExecutions()+response.GetDeletedBlobObjects() == 0 {
				emptyPasses++
			}
			final = response
			return response.GetDone()
		}, 2*time.Minute, 500*time.Millisecond)
		require.GreaterOrEqual(t, emptyPasses, 2, "done needs two empty passes")
		require.NotNil(t, final.GetState().GetPurgeDoneTime())
		again, err := operator.PurgeTenant(ctx, &dexpb.PurgeTenantRequest{TenantId: "alpha"})
		require.NoError(t, err)
		require.True(t, again.GetDone(), "a finished purge stays done")

		for _, flowID := range alphaFlowIDs {
			_, err := pair.runtime.UnifiedClient.DescribeWorkflowExecution(ctx, tenancy.StoredFlowID("alpha", flowID), "", nil)
			require.True(t, pair.runtime.UnifiedClient.IsNotFoundError(err), "execution %s remains: %v", flowID, err)
		}
		s3Client := newTenantTestS3Client(t, ctx)
		listed, err := s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket: aws.String(tenantBlobTestBucket), Prefix: aws.String(keyPrefix + testNamespace + "/alpha/"),
		})
		require.NoError(t, err)
		require.Empty(t, listed.Contents, "the tenant's blob prefix is empty")
		options, err := redis.ParseURL(streamTestRedisURL)
		require.NoError(t, err)
		redisClient := redis.NewClient(options)
		defer func() { require.NoError(t, redisClient.Close()) }()
		keys, err := redisClient.Keys(ctx, "dex:stream:v1:t:alpha:*").Result()
		require.NoError(t, err)
		require.Empty(t, keys, "the tenant's Stream keys are deleted")

		summary, err := pair.beta.GetFlowSummary(ctx, &dexpb.GetFlowSummaryRequest{FlowId: betaFlowID})
		require.NoError(t, err)
		require.Equal(t, dexpb.FlowStatus_FLOW_STATUS_RUNNING, summary.GetFlowStatus(), "another tenant's Flow survives")
	})

	t.Run("the tenant ID is accepted again only after the tombstone retention", func(t *testing.T) {
		described, err := operator.DescribeTenant(ctx, &dexpb.DescribeTenantRequest{TenantId: "alpha"})
		require.NoError(t, err)
		expiry := described.GetState().GetTombstoneExpiryTime().AsTime()
		require.Eventually(t, func() bool {
			_, err := pair.alpha.GetServerInfo(ctx, &emptypb.Empty{})
			if err == nil {
				require.False(t, time.Now().Before(expiry), "accepted before the tombstone expired")
			}
			return err == nil
		}, 20*time.Second, 200*time.Millisecond)
		described, err = operator.DescribeTenant(ctx, &dexpb.DescribeTenantRequest{TenantId: "alpha"})
		require.NoError(t, err)
		require.False(t, described.GetState().GetIsTombstoned())
	})
}
