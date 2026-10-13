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
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/superdurable/dex/config"
	"github.com/superdurable/dex/gen/dexpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestTenantRateLimitsTemporal(t *testing.T) {
	if !*temporalIntegTest || *dexServerAddress != "" {
		t.Skip()
	}
	slowBucket := func(burst int) config.TenantRateLimitConfig {
		return config.TenantRateLimitConfig{Rate: 0.01, Burst: burst}
	}
	pair := startTenantPair(t, func(testConfig *DexServiceTestConfig) {
		testConfig.Tenancy.Limits.StartFlowsPerSecond = slowBucket(2)
		testConfig.Tenancy.Limits.WritesPerSecond = slowBucket(2)
		testConfig.Tenancy.Limits.SearchesPerSecond = slowBucket(1)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	start := func(client dexpb.FlowServiceClient, target *dexpb.WorkerTarget) (string, error) {
		flowID := "limited-" + uuid.NewString()
		_, err := client.StartFlow(ctx, &dexpb.StartFlowRequest{
			RequestId: newRequestID(), FlowId: flowID, FlowType: tenantProbeFlowType, FlowTimeoutSeconds: 60,
			StartStepType: tenantProbeStep, FlowStartOptions: withWorkerTarget(nil, target),
		})
		return flowID, err
	}
	requireExhausted := func(t *testing.T, err error) {
		t.Helper()
		require.Equal(t, codes.ResourceExhausted, status.Code(err), "error: %v", err)
	}

	t.Run("starts", func(t *testing.T) {
		for i := 0; i < 2; i++ {
			_, err := start(pair.alpha, pair.alphaTarget)
			require.NoError(t, err)
		}
		_, err := start(pair.alpha, pair.alphaTarget)
		requireExhausted(t, err)
		_, err = start(pair.beta, pair.betaTarget)
		require.NoError(t, err, "another tenant still starts Flows")
	})

	t.Run("writes", func(t *testing.T) {
		flowID, err := start(pair.beta, pair.betaTarget)
		require.NoError(t, err)
		publish := func() error {
			_, err := pair.beta.PublishToChannel(ctx, &dexpb.PublishToChannelRequest{
				FlowId: flowID, Messages: []*dexpb.ChannelMessage{{ChannelName: "unused"}},
			})
			return err
		}
		require.NoError(t, publish())
		_, err = pair.beta.SetAttributes(ctx, &dexpb.SetAttributesRequest{
			FlowId: flowID, RequestId: uuid.NewString(),
			Attributes: []*dexpb.AttributeWrite{{Key: "key", Value: stringValue("value")}},
		})
		require.NoError(t, err)
		requireExhausted(t, publish())
		_, err = pair.alpha.SetAttributes(ctx, &dexpb.SetAttributesRequest{
			FlowId: "missing-" + uuid.NewString(), RequestId: uuid.NewString(),
			Attributes: []*dexpb.AttributeWrite{{Key: "key", Value: stringValue("value")}},
		})
		require.Equal(t, codes.NotFound, status.Code(err), "another tenant's writes are not limited: %v", err)
	})

	t.Run("searches", func(t *testing.T) {
		_, err := pair.alpha.SearchFlows(ctx, &dexpb.SearchFlowsRequest{PageSize: 1})
		require.NoError(t, err)
		_, err = pair.alpha.SearchFlows(ctx, &dexpb.SearchFlowsRequest{PageSize: 1})
		requireExhausted(t, err)
		_, err = pair.beta.SearchFlows(ctx, &dexpb.SearchFlowsRequest{PageSize: 1})
		require.NoError(t, err, "another tenant still searches")
	})

	t.Run("reads are not limited", func(t *testing.T) {
		for i := 0; i < 5; i++ {
			_, err := pair.alpha.GetServerInfo(ctx, nil)
			require.NoError(t, err)
		}
	})
}
