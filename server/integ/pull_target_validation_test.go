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
	"github.com/superdurable/dex/gen/dexpb"
	"github.com/superdurable/dex/service"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestPullWorkerTargetValidationTemporal(t *testing.T) {
	if !*temporalIntegTest || *dexServerAddress != "" {
		t.Skip()
	}
	testPullWorkerTargetValidation(t, service.BackendTypeTemporal)
}

func TestPullWorkerTargetValidationCadence(t *testing.T) {
	if !*cadenceIntegTest || *dexServerAddress != "" {
		t.Skip()
	}
	testPullWorkerTargetValidation(t, service.BackendTypeCadence)
}

func testPullWorkerTargetValidation(t *testing.T, backend service.BackendType) {
	t.Helper()
	runtime := startInProcessDexService(t, DexServiceTestConfig{BackendType: backend})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for _, target := range []*dexpb.WorkerTarget{
		{Address: "127.0.0.1:9000", IsPullMode: true},
		{IsPullMode: true, IsHeadlessAddress: true},
	} {
		_, err := runtime.FlowClient.StartFlow(ctx, &dexpb.StartFlowRequest{
			RequestId:        newRequestID(),
			FlowId:           "invalid-pull-target-" + uuid.NewString(),
			FlowType:         "PullTargetValidationFlow",
			FlowStartOptions: withWorkerTarget(nil, target),
		})
		require.Equal(t, codes.InvalidArgument, status.Code(err))
	}
	flowID := "pull-target-update-" + uuid.NewString()
	_, err := runtime.FlowClient.StartFlow(ctx, &dexpb.StartFlowRequest{
		RequestId: newRequestID(), FlowId: flowID, FlowType: "PullTargetValidationFlow",
		FlowTimeoutSeconds: 120, FlowStartOptions: withWorkerTarget(nil, &dexpb.WorkerTarget{IsPullMode: true}),
	})
	require.NoError(t, err)
	_, err = runtime.FlowClient.UpdateFlowConfig(ctx, &dexpb.UpdateFlowConfigRequest{
		FlowId: flowID, FlowConfig: &dexpb.FlowConfig{
			WorkerTarget: &dexpb.WorkerTarget{Address: "127.0.0.1:9000", IsPullMode: true},
		},
	})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	_, err = runtime.FlowClient.StopFlow(ctx, &dexpb.StopFlowRequest{FlowId: flowID})
	require.NoError(t, err)
}
