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
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/superdurable/dex/gen/dexpb"
	"github.com/superdurable/dex/service"
	uclient "github.com/superdurable/dex/service/client"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	slowEngineTaskFlowType              = "slow_engine_task"
	slowEngineTaskIdleStepType          = "idle"
	slowEngineTaskIdleChannelName       = "never-published"
	slowEngineTaskAttributeKey          = "progress"
	slowEngineTaskStartAttributeValue   = "committed-at-start"
	slowEngineTaskHeldRPCAttributeValue = "written-by-held-rpc"
	holdEngineTaskRPCName               = "holdEngineTask"
	readFlowRPCName                     = "readFlow"
	getProgressAttributeRPCName         = "getProgressAttribute"
)

// slowEngineTaskHandler keeps a transactional RPC handler busy, which keeps the Flow's engine task open.
type slowEngineTaskHandler struct {
	holdDuration    time.Duration
	holdStarted     chan struct{}
	holdStartedOnce sync.Once
}

func newSlowEngineTaskHandler(holdDuration time.Duration) *slowEngineTaskHandler {
	return &slowEngineTaskHandler{
		holdDuration: holdDuration,
		holdStarted:  make(chan struct{}),
	}
}

func (h *slowEngineTaskHandler) InvokeWaitForMethod(
	_ context.Context,
	_ *dexpb.InvokeWaitForMethodRequest,
) (*dexpb.InvokeWaitForMethodResponse, error) {
	return &dexpb.InvokeWaitForMethodResponse{
		WaitingCondition: &dexpb.WaitingCondition{
			WaitingConditionType: dexpb.WaitingConditionType_WAITING_CONDITION_TYPE_ALL_COMPLETED,
			ChannelConditions: []*dexpb.ChannelCondition{
				{ChannelName: slowEngineTaskIdleChannelName},
			},
		},
	}, nil
}

func (h *slowEngineTaskHandler) InvokeWorkerRPC(
	ctx context.Context,
	request *dexpb.InvokeWorkerRPCRequest,
) (*dexpb.InvokeWorkerRPCResponse, error) {
	switch request.GetRpcName() {
	case holdEngineTaskRPCName:
		h.holdStartedOnce.Do(func() { close(h.holdStarted) })
		holdTimer := time.NewTimer(h.holdDuration)
		defer holdTimer.Stop()
		select {
		case <-holdTimer.C:
		case <-ctx.Done():
			return nil, status.FromContextError(ctx.Err()).Err()
		}
		return &dexpb.InvokeWorkerRPCResponse{
			Output: stringValue(request.GetRpcName()),
			UpsertAttributes: []*dexpb.AttributeWrite{{
				Key:   slowEngineTaskAttributeKey,
				Value: stringValue(slowEngineTaskHeldRPCAttributeValue),
			}},
		}, nil
	case getProgressAttributeRPCName:
		// Returning no effects keeps this RPC a pure read of the loaded snapshot.
		for _, attribute := range request.GetAttributes() {
			if attribute.GetKey() == slowEngineTaskAttributeKey {
				return &dexpb.InvokeWorkerRPCResponse{Output: attribute.GetValue()}, nil
			}
		}
		return nil, status.Errorf(codes.NotFound, "Attribute %q was not loaded", slowEngineTaskAttributeKey)
	}
	return &dexpb.InvokeWorkerRPCResponse{Output: stringValue(request.GetRpcName())}, nil
}

func TestReadBehindSlowEngineTaskTemporal(t *testing.T) {
	if !*temporalIntegTest {
		t.Skip()
	}
	t.Run("reads-complete-within-caller-deadline", testReadsCompleteWithinCallerDeadline)
	t.Run("deadline-less-reads-report-request-timeout", testDeadlineLessReadsReportRequestTimeout)
	t.Run("caller-deadline-and-cancellation-end-reads-at-once", testCallerDeadlineAndCancellationEndReadsAtOnce)
}

func testReadsCompleteWithinCallerDeadline(t *testing.T) {
	handler := newSlowEngineTaskHandler(6 * time.Second)
	runtime, flowID := startSlowEngineTaskFlow(t, handler, DexServiceTestConfig{})
	holdResult := holdEngineTask(t, runtime, flowID, handler, 10)

	readCtx, cancelRead := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancelRead()
	readStarted := time.Now()
	var rpcResponse *dexpb.InvokeRPCResponse
	var reads errgroup.Group
	reads.Go(func() error {
		var err error
		rpcResponse, err = runtime.FlowClient.InvokeRPC(readCtx, newReadFlowRPCRequest(flowID))
		return err
	})
	reads.Go(func() error {
		_, err := runtime.FlowClient.GetFlowState(readCtx, &dexpb.GetFlowStateRequest{FlowId: flowID})
		return err
	})
	require.NoError(t, reads.Wait())
	if isServerZDBinaryMode() {
		// server-zd reads a committed snapshot, so reads finish before the transactional RPC.
		require.Less(t, time.Since(readStarted), handler.holdDuration)
	} else {
		// The first read attempt gets half of the 8-second deadline, so success after it proves a retry.
		require.Greater(t, time.Since(readStarted), 4*time.Second)
	}
	require.Equal(t, readFlowRPCName, rpcResponse.GetOutput().GetStringValue())
	require.NoError(t, <-holdResult)
	stopSlowEngineTaskFlow(t, runtime, flowID)
}

func testDeadlineLessReadsReportRequestTimeout(t *testing.T) {
	handler := newSlowEngineTaskHandler(14 * time.Second)
	runtime, flowID := startSlowEngineTaskFlow(t, handler, DexServiceTestConfig{MaxWaitSeconds: 20})
	holdResult := holdEngineTask(t, runtime, flowID, handler, 18)

	readStarted := time.Now()
	var rpcResponse *dexpb.InvokeRPCResponse
	var attributesResponse *dexpb.GetAttributesResponse
	var rpcErr, attributesErr error
	var reads sync.WaitGroup
	reads.Add(2)
	go func() {
		defer reads.Done()
		rpcResponse, rpcErr = runtime.FlowClient.InvokeRPC(
			context.Background(),
			newGetProgressAttributeRPCRequest(flowID),
		)
	}()
	go func() {
		defer reads.Done()
		attributesResponse, attributesErr = runtime.FlowClient.GetAttributes(context.Background(), &dexpb.GetAttributesRequest{
			FlowId:  flowID,
			AllKeys: true,
		})
	}()
	reads.Wait()
	if isServerZDBinaryMode() {
		// server-zd reads a committed snapshot instead of waiting for the transactional RPC.
		require.NoError(t, rpcErr)
		require.NoError(t, attributesErr)
		requireStringValue(t, slowEngineTaskStartAttributeValue, rpcResponse.GetOutput())
		requireSlowEngineTaskAttribute(t, slowEngineTaskStartAttributeValue, attributesResponse)
	} else {
		requireReadRequestTimeout(t, rpcErr)
		requireReadRequestTimeout(t, attributesErr)
	}
	require.Less(t, time.Since(readStarted), 14*time.Second)
	require.NoError(t, <-holdResult)
	if isServerZDBinaryMode() {
		// The held RPC's write must commit, or the start-value assertions prove nothing.
		requireSlowEngineTaskAttribute(t, slowEngineTaskHeldRPCAttributeValue, getSlowEngineTaskAttributes(t, runtime, flowID))
	}
	stopSlowEngineTaskFlow(t, runtime, flowID)
}

func testCallerDeadlineAndCancellationEndReadsAtOnce(t *testing.T) {
	requireTemporalOnly(t)
	handler := newSlowEngineTaskHandler(8 * time.Second)
	runtime, flowID := startSlowEngineTaskFlow(t, handler, DexServiceTestConfig{})
	holdResult := holdEngineTask(t, runtime, flowID, handler, 10)

	deadlineCtx, cancelDeadlineRead := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelDeadlineRead()
	readStarted := time.Now()
	var dump dexpb.DebugDumpResponse
	err := runtime.UnifiedClient.QueryWorkflow(deadlineCtx, &dump, flowID, "", service.DebugDumpQueryType)
	require.ErrorIs(t, err, uclient.ErrQueryRequestTimeout)
	require.Less(t, time.Since(readStarted), 3500*time.Millisecond)

	cancelCtx, cancelRead := context.WithCancel(context.Background())
	defer cancelRead()
	cancelTimer := time.AfterFunc(time.Second, cancelRead)
	defer cancelTimer.Stop()
	readStarted = time.Now()
	err = runtime.UnifiedClient.QueryWorkflow(cancelCtx, &dump, flowID, "", service.DebugDumpQueryType)
	require.ErrorIs(t, err, context.Canceled)
	require.NotErrorIs(t, err, uclient.ErrQueryRequestTimeout)
	require.Less(t, time.Since(readStarted), 1500*time.Millisecond)
	require.NoError(t, <-holdResult)
	stopSlowEngineTaskFlow(t, runtime, flowID)
}

func newReadFlowRPCRequest(flowID string) *dexpb.InvokeRPCRequest {
	return &dexpb.InvokeRPCRequest{
		RequestId:      newRequestID(),
		FlowId:         flowID,
		RpcName:        readFlowRPCName,
		TimeoutSeconds: 5,
	}
}

func newGetProgressAttributeRPCRequest(flowID string) *dexpb.InvokeRPCRequest {
	return &dexpb.InvokeRPCRequest{
		RequestId:      newRequestID(),
		FlowId:         flowID,
		RpcName:        getProgressAttributeRPCName,
		TimeoutSeconds: 5,
	}
}

func getSlowEngineTaskAttributes(t *testing.T, runtime *integRuntime, flowID string) *dexpb.GetAttributesResponse {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	response, err := runtime.FlowClient.GetAttributes(ctx, &dexpb.GetAttributesRequest{
		FlowId:  flowID,
		AllKeys: true,
	})
	require.NoError(t, err)
	return response
}

func requireSlowEngineTaskAttribute(t *testing.T, expectedValue string, response *dexpb.GetAttributesResponse) {
	t.Helper()
	attributes := response.GetAttributes()
	require.Len(t, attributes, 1, "attributes: %v", attributes)
	require.Equal(t, slowEngineTaskAttributeKey, attributes[0].GetKey())
	requireStringValue(t, expectedValue, attributes[0].GetValue())
}

func requireStringValue(t *testing.T, expected string, actual *dexpb.Value) {
	t.Helper()
	require.True(t, proto.Equal(stringValue(expected), actual), "expected %q, got %v", expected, actual)
}

func requireReadRequestTimeout(t *testing.T, err error) {
	t.Helper()
	require.Equal(t, codes.DeadlineExceeded, status.Code(err), "read error: %v", err)
	require.Equal(
		t,
		dexpb.ErrorSubStatus_ERROR_SUB_STATUS_REQUEST_TIMEOUT,
		grpcServiceErrorResponse(t, err).GetSubStatus(),
	)
}

func startSlowEngineTaskFlow(
	t *testing.T,
	handler *slowEngineTaskHandler,
	testConfig DexServiceTestConfig,
) (*integRuntime, string) {
	t.Helper()
	workerTarget := startWorker(t, handler)
	testConfig.BackendType = service.BackendTypeTemporal
	runtime := startDexService(t, testConfig)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	flowID := slowEngineTaskFlowType + "-" + uuid.NewString()
	_, err := runtime.FlowClient.StartFlow(ctx, &dexpb.StartFlowRequest{
		RequestId:          newRequestID(),
		FlowId:             flowID,
		FlowType:           slowEngineTaskFlowType,
		FlowTimeoutSeconds: 60,
		StartStepType:      slowEngineTaskIdleStepType,
		FlowStartOptions: withWorkerTarget(&dexpb.FlowStartOptions{
			Attributes: []*dexpb.AttributeWrite{{
				Key:   slowEngineTaskAttributeKey,
				Value: stringValue(slowEngineTaskStartAttributeValue),
			}},
		}, workerTarget),
	})
	require.NoError(t, err)
	return runtime, flowID
}

// holdEngineTask starts a transactional RPC and returns once its handler keeps the engine task open.
func holdEngineTask(
	t *testing.T,
	runtime *integRuntime,
	flowID string,
	handler *slowEngineTaskHandler,
	timeoutSeconds int32,
) <-chan error {
	t.Helper()
	holdResult := make(chan error, 1)
	go func() {
		holdCtx, cancelHold := context.WithTimeout(
			context.Background(),
			time.Duration(timeoutSeconds+5)*time.Second,
		)
		defer cancelHold()
		_, err := runtime.FlowClient.InvokeRPC(holdCtx, &dexpb.InvokeRPCRequest{
			RequestId:       newRequestID(),
			FlowId:          flowID,
			RpcName:         holdEngineTaskRPCName,
			TimeoutSeconds:  timeoutSeconds,
			IsTransactional: true,
		})
		holdResult <- err
	}()
	select {
	case <-handler.holdStarted:
	case err := <-holdResult:
		require.FailNow(t, "transactional RPC returned before its handler started", "error: %v", err)
	case <-time.After(10 * time.Second):
		require.FailNow(t, "transactional RPC handler did not start")
	}
	return holdResult
}

func stopSlowEngineTaskFlow(t *testing.T, runtime *integRuntime, flowID string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := runtime.FlowClient.StopFlow(ctx, &dexpb.StopFlowRequest{
		FlowId:   flowID,
		StopType: dexpb.StopType_STOP_TYPE_TERMINATE,
	})
	require.NoError(t, err)
}
