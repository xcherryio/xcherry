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
	"github.com/superdurable/dex/service"
	"github.com/superdurable/dex/service/common/ptr"
)

func TestPullRoutingTemporal(t *testing.T) {
	if !*temporalIntegTest || *dexServerAddress != "" {
		t.Skip()
	}
	testPullRouting(t, service.BackendTypeTemporal)
}

func TestPullRoutingCadence(t *testing.T) {
	if !*cadenceIntegTest || *dexServerAddress != "" {
		t.Skip()
	}
	testPullRouting(t, service.BackendTypeCadence)
}

func TestPullRPCRoutingTemporal(t *testing.T) {
	if !*temporalIntegTest || *dexServerAddress != "" {
		t.Skip()
	}
	runtime := startInProcessDexService(t, DexServiceTestConfig{BackendType: service.BackendTypeTemporal, Matching: &config.MatchingConfig{}})
	workerID := "routing-rpc-" + uuid.NewString()
	worker := startRawPullWorker(t, runtime.PublicAddress, dispatchProbeFlowType, workerID, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	flowID := "pull-rpc-routing-" + uuid.NewString()
	started, err := runtime.FlowClient.StartFlow(ctx, &dexpb.StartFlowRequest{RequestId: newRequestID(), FlowId: flowID, FlowType: dispatchProbeFlowType, FlowTimeoutSeconds: 120, FlowStartOptions: withWorkerTarget(nil, &dexpb.WorkerTarget{IsPullMode: true})})
	require.NoError(t, err)
	_, err = runtime.FlowClient.InvokeRPC(ctx, &dexpb.InvokeRPCRequest{RequestId: newRequestID(), FlowId: flowID, RpcName: "getProbe", IsTransactional: true, TimeoutSeconds: 10})
	require.NoError(t, err)
	require.Equal(t, "rpc", waitForPullTask(t, worker, flowID).method)
	history, err := runtime.FlowClient.GetHistoryEvents(ctx, &dexpb.GetHistoryEventsRequest{FlowId: flowID, RunId: started.GetRunId(), EstimatePageSize: 100})
	require.NoError(t, err)
	var rpcEvent *dexpb.RpcExecutionCompletedEvent
	for _, event := range history.GetEvents() {
		if event.GetRpcExecutionCompleted() != nil {
			rpcEvent = event.GetRpcExecutionCompleted()
		}
	}
	require.NotNil(t, rpcEvent)
	require.Equal(t, workerID, rpcEvent.GetServedByAppWorkerId())
	_, err = runtime.FlowClient.StopFlow(ctx, &dexpb.StopFlowRequest{FlowId: flowID})
	require.NoError(t, err)
	stopped, err := runtime.FlowClient.WaitForFlow(ctx, &dexpb.WaitForFlowRequest{FlowId: flowID, WaitTimeSeconds: 10})
	require.NoError(t, err)
	require.NotEqual(t, dexpb.FlowStatus_FLOW_STATUS_RUNNING, stopped.GetFlowStatus())
	history, err = runtime.FlowClient.GetHistoryEvents(ctx, &dexpb.GetHistoryEventsRequest{FlowId: flowID, RunId: started.GetRunId(), EstimatePageSize: 100})
	require.NoError(t, err)
	var closed *dexpb.FlowClosedHistoryEvent
	for _, event := range history.GetEvents() {
		if event.GetFlowClosed() != nil {
			closed = event.GetFlowClosed()
		}
	}
	require.NotNil(t, closed)
	require.Equal(t, workerID, closed.GetStickyAppWorkerId())
}

func testPullRouting(t *testing.T, backend service.BackendType) {
	for _, durability := range []dexpb.StepDurability{dexpb.StepDurability_STEP_DURABILITY_SYNC, dexpb.StepDurability_STEP_DURABILITY_ASYNC} {
		t.Run(durability.String(), func(t *testing.T) {
			runtime := startInProcessDexService(t, DexServiceTestConfig{BackendType: backend, Matching: &config.MatchingConfig{DispatchWait: 5 * time.Second, StickyWait: 100 * time.Millisecond}})
			firstWorkerID := "routing-first-" + uuid.NewString()
			firstWorker := startUnknownRejectingPullWorker(t, runtime.PublicAddress, dispatchProbeFlowType, firstWorkerID, rawPullWorkerConfig{shouldStartSecondStep: true})
			ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
			defer cancel()
			flowID := "pull-routing-" + uuid.NewString()
			started, err := runtime.FlowClient.StartFlow(ctx, &dexpb.StartFlowRequest{
				RequestId: newRequestID(), FlowId: flowID, FlowType: dispatchProbeFlowType, StartStepType: dispatchProbeStep, FlowTimeoutSeconds: 120,
				FlowStartOptions: &dexpb.FlowStartOptions{FlowConfigOverride: &dexpb.FlowConfig{WorkerTarget: &dexpb.WorkerTarget{IsPullMode: true}, StepDurability: ptr.Any(durability)}},
			})
			require.NoError(t, err)
			require.Equal(t, "wait_for", waitForPullTask(t, firstWorker, flowID).method)
			requirePullActiveRouting(t, ctx, runtime.FlowClient, flowID, firstWorkerID)
			_, err = runtime.FlowClient.PublishToChannel(ctx, &dexpb.PublishToChannelRequest{FlowId: flowID, Messages: []*dexpb.ChannelMessage{{ChannelName: dispatchProbeChannel}}})
			require.NoError(t, err)
			require.Equal(t, "execute", waitForPullTask(t, firstWorker, flowID).method)
			require.Equal(t, "wait_for", waitForPullTask(t, firstWorker, flowID).method)
			probe := &pullRoutingStateProbe{test: t, ctx: ctx, client: runtime.FlowClient, flowID: flowID, workerID: firstWorkerID, source: dexpb.PullRoutingSource_PULL_ROUTING_SOURCE_STICKY_QUEUE}
			require.Eventually(t, probe.hasExpectedRouting, 10*time.Second, 50*time.Millisecond)
			firstWorker.stop()
			secondWorkerID := "routing-second-" + uuid.NewString()
			secondWorker := startRawPullWorker(t, runtime.PublicAddress, dispatchProbeFlowType, secondWorkerID, nil)
			_, err = runtime.FlowClient.PublishToChannel(ctx, &dexpb.PublishToChannelRequest{FlowId: flowID, Messages: []*dexpb.ChannelMessage{{ChannelName: dispatchProbeChannel}}})
			require.NoError(t, err)
			require.Equal(t, "execute", waitForPullTask(t, secondWorker, flowID).method)
			completed, err := runtime.FlowClient.WaitForFlow(ctx, &dexpb.WaitForFlowRequest{FlowId: flowID, WaitTimeSeconds: 25})
			require.NoError(t, err)
			require.Equal(t, dexpb.FlowStatus_FLOW_STATUS_COMPLETED, completed.GetFlowStatus(), completed.GetErrorMessage())
			history, err := runtime.FlowClient.GetHistoryEvents(ctx, &dexpb.GetHistoryEventsRequest{FlowId: flowID, RunId: started.GetRunId(), EstimatePageSize: 200})
			require.NoError(t, err)
			var routing []*dexpb.PullRouting
			for _, event := range history.GetEvents() {
				if event.GetStepWaitForCompleted() != nil {
					routing = append(routing, event.GetStepWaitForCompleted().GetContext().GetPullRouting())
				}
				if event.GetStepExecuteCompleted() != nil {
					routing = append(routing, event.GetStepExecuteCompleted().GetContext().GetPullRouting())
				}
			}
			require.Len(t, routing, 4)
			for index, expectedSource := range []dexpb.PullRoutingSource{dexpb.PullRoutingSource_PULL_ROUTING_SOURCE_FLOW_TYPE_QUEUE, dexpb.PullRoutingSource_PULL_ROUTING_SOURCE_STICKY_QUEUE, dexpb.PullRoutingSource_PULL_ROUTING_SOURCE_STICKY_QUEUE, dexpb.PullRoutingSource_PULL_ROUTING_SOURCE_FLOW_TYPE_QUEUE} {
				require.NotNil(t, routing[index])
				require.Equal(t, expectedSource, routing[index].GetSource())
				expectedWorkerID := firstWorkerID
				if index == 3 {
					expectedWorkerID = secondWorkerID
				}
				require.Equal(t, expectedWorkerID, routing[index].GetServedByAppWorkerId())
				if index > 0 {
					require.Equal(t, firstWorkerID, routing[index].GetStickyAppWorkerId())
				}
			}
		})
	}
}
