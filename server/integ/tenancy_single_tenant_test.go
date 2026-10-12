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
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/superdurable/dex/gen/dexpb"
	"github.com/superdurable/dex/service"
	uclient "github.com/superdurable/dex/service/client"
	"github.com/superdurable/dex/service/common/ptr"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestSingleTenantFlowIDsWithTenantPrefixTemporal shows single-tenant mode never interprets a "T:" Flow ID.
// Servers before reserved ":" in Flow IDs could start such Flows; this test creates one through the engine.
func TestSingleTenantFlowIDsWithTenantPrefixTemporal(t *testing.T) {
	if !*temporalIntegTest || *dexServerAddress != "" {
		t.Skip()
	}
	worker := &tenantProbeHandler{}
	workerTarget := startWorker(t, worker)
	runtime := startDexService(t, DexServiceTestConfig{BackendType: service.BackendTypeTemporal})
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	_, err := runtime.FlowClient.StartFlow(ctx, &dexpb.StartFlowRequest{
		RequestId: newRequestID(), FlowId: "T:order-1", FlowType: tenantProbeFlowType,
		StartStepType: tenantProbeStep, FlowStartOptions: withWorkerTarget(nil, workerTarget),
	})
	require.Equal(t, codes.InvalidArgument, status.Code(err), "StartFlow still refuses \":\" in Flow IDs: %v", err)

	flowID := "T:order-" + uuid.NewString()
	runID, err := runtime.UnifiedClient.StartInterpreterWorkflow(ctx, uclient.StartWorkflowOptions{
		ID:               flowID,
		TaskQueue:        service.TaskQueue,
		SearchAttributes: map[string]interface{}{service.SearchAttributeDexWorkflowType: tenantProbeFlowType},
		Memo:             map[string]interface{}{service.ReqId: &dexpb.EncodedObject{Payload: []byte(newRequestID())}},
		IdReusePolicy:    ptr.Any(dexpb.IdReusePolicy_ID_REUSE_POLICY_DISALLOW_REUSE),
	}, &dexpb.InterpreterWorkflowInput{
		FlowType:      tenantProbeFlowType,
		StartStepType: tenantProbeStep,
		Config: &dexpb.FlowConfig{
			WorkerTarget:   workerTarget,
			StepDurability: ptr.Any(dexpb.StepDurability_STEP_DURABILITY_SYNC),
		},
	})
	require.NoError(t, err)

	summary, err := runtime.FlowClient.GetFlowSummary(ctx, &dexpb.GetFlowSummaryRequest{FlowId: flowID})
	require.NoError(t, err)
	require.Equal(t, flowID, summary.GetFlowExecutionId().GetFlowId())

	if *testSearchIntegTest {
		require.Eventually(t, func() bool {
			response, searchErr := runtime.FlowClient.SearchFlows(ctx, &dexpb.SearchFlowsRequest{
				Query: `WorkflowId = "` + flowID + `"`, PageSize: 10,
			})
			require.NoError(t, searchErr)
			return len(response.GetFlowRuns()) == 1 && response.GetFlowRuns()[0].GetFlowId() == flowID
		}, 30*time.Second, 200*time.Millisecond)
	}

	_, err = runtime.FlowClient.PublishToChannel(ctx, &dexpb.PublishToChannelRequest{
		FlowId: flowID, Messages: []*dexpb.ChannelMessage{{ChannelName: tenantProbeChannel}},
	})
	require.NoError(t, err)
	result, err := runtime.FlowClient.WaitForFlow(ctx, &dexpb.WaitForFlowRequest{
		FlowId: flowID, NeedsResults: true, WaitTimeSeconds: 30,
	})
	require.NoError(t, err)
	require.Equal(t, dexpb.FlowStatus_FLOW_STATUS_COMPLETED, result.GetFlowStatus())
	require.Equal(t, flowID, result.GetResults()[0].GetCompletedStepOutput().GetStringValue())
	require.True(t, slices.Contains(worker.observed(), flowID), "the Worker sees the Flow ID unchanged")

	history, err := runtime.FlowClient.GetHistoryEvents(ctx, &dexpb.GetHistoryEventsRequest{FlowId: flowID, RunId: runID})
	require.NoError(t, err)
	require.Equal(t, flowID, history.GetEvents()[0].GetFlowStartedOrContinued().GetFlowExecutionId().GetFlowId())
}
