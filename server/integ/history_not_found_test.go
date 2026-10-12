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
	"github.com/superdurable/dex/integ/workflow/basic"
	"github.com/superdurable/dex/service"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestGetHistoryEventsNotFoundTemporal(t *testing.T) {
	if !*temporalIntegTest {
		t.Skip()
	}
	testGetHistoryEventsNotFound(t, service.BackendTypeTemporal)
}

func TestGetHistoryEventsNotFoundCadence(t *testing.T) {
	if !*cadenceIntegTest {
		t.Skip()
	}
	testGetHistoryEventsNotFound(t, service.BackendTypeCadence)
}

func testGetHistoryEventsNotFound(t *testing.T, backendType service.BackendType) {
	workerTarget := startWorker(t, basic.NewHandler())
	runtime := startDexService(t, DexServiceTestConfig{BackendType: backendType})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	flowID := basic.FlowType + "-" + uuid.NewString()
	_, err := runtime.FlowClient.StartFlow(ctx, &dexpb.StartFlowRequest{
		RequestId:          newRequestID(),
		FlowId:             flowID,
		FlowType:           basic.FlowType,
		FlowTimeoutSeconds: 100,
		StartStepType:      basic.Step1,
		FlowStartOptions:   withWorkerTarget(nil, workerTarget),
	})
	require.NoError(t, err)

	for name, request := range map[string]*dexpb.GetHistoryEventsRequest{
		"unknown Flow":          {FlowId: "missing-" + uuid.NewString(), RunId: uuid.NewString()},
		"unknown run of a Flow": {FlowId: flowID, RunId: uuid.NewString()},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := runtime.FlowClient.GetHistoryEvents(ctx, request)
			require.Equal(t, codes.NotFound, status.Code(err), "error: %v", err)
			require.Equal(
				t,
				dexpb.ErrorSubStatus_ERROR_SUB_STATUS_FLOW_NOT_EXISTS,
				grpcServiceErrorResponse(t, err).GetSubStatus(),
			)
		})
	}
}
