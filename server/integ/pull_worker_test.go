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
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/superdurable/dex/config"
	"github.com/superdurable/dex/gen/dexpb"
	"github.com/superdurable/dex/integ/workflow/common"
	"github.com/superdurable/dex/service"
	uclient "github.com/superdurable/dex/service/client"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type pushRPCErrorWorker struct{}

func (pushRPCErrorWorker) InvokeWorkerRPC(
	context.Context, *dexpb.InvokeWorkerRPCRequest,
) (*dexpb.InvokeWorkerRPCResponse, error) {
	workerStatus, err := status.New(codes.InvalidArgument, "invalid Worker input").WithDetails(&dexpb.WorkerErrorResponse{
		Detail: "invalid Worker input", ErrorType: "InputError", StackTrace: "worker stack",
	})
	if err != nil {
		return nil, err
	}
	return nil, workerStatus.Err()
}

type pullTaskObservation struct {
	queue   string
	method  string
	flowID  string
	attempt int32
}

type rawPullWorker struct {
	events              chan pullTaskObservation
	started             chan pullTaskObservation
	errors              chan error
	cancellations       []context.CancelFunc
	queueCancels        map[string]context.CancelFunc
	rpcError            *dexpb.WorkerErrorResponse
	disconnectOnWaitFor bool
	spawnSubFlow        bool
	spawnParallel       bool
	blockParallel       bool
	releaseParallel     chan struct{}
}

func startRawPullWorker(t *testing.T, address string, flowType string, workerID string, rpcError *dexpb.WorkerErrorResponse) *rawPullWorker {
	return newRawPullWorker(t, address, flowType, workerID, rpcError, false, false, false, false)
}

func startDisconnectingPullWorker(t *testing.T, address string, flowType string, workerID string) *rawPullWorker {
	return newRawPullWorker(t, address, flowType, workerID, nil, true, false, false, false)
}

func startSubFlowPullWorker(t *testing.T, address string, flowType string, workerID string) *rawPullWorker {
	return newRawPullWorker(t, address, flowType, workerID, nil, false, true, false, false)
}

func startParallelPullWorker(t *testing.T, address string, flowType string, workerID string, blockParallel bool) *rawPullWorker {
	return newRawPullWorker(t, address, flowType, workerID, nil, false, false, true, blockParallel)
}

func newRawPullWorker(t *testing.T, address string, flowType string, workerID string, rpcError *dexpb.WorkerErrorResponse, disconnectOnWaitFor bool, spawnSubFlow bool, spawnParallel bool, blockParallel bool) *rawPullWorker {
	t.Helper()
	connection, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, connection.Close()) })
	worker := &rawPullWorker{
		events: make(chan pullTaskObservation, 16), started: make(chan pullTaskObservation, 16),
		errors: make(chan error, 4), queueCancels: map[string]context.CancelFunc{}, releaseParallel: make(chan struct{}),
		rpcError: rpcError, disconnectOnWaitFor: disconnectOnWaitFor, spawnSubFlow: spawnSubFlow,
		spawnParallel: spawnParallel, blockParallel: blockParallel,
	}
	for _, queue := range []string{flowType, workerID} {
		ctx, cancel := context.WithCancel(context.Background())
		worker.cancellations = append(worker.cancellations, cancel)
		worker.queueCancels[queue] = cancel
		t.Cleanup(cancel)
		stream, err := dexpb.NewWorkerTaskServiceClient(connection).PollWorkerTask(ctx)
		require.NoError(t, err)
		require.NoError(t, stream.Send(&dexpb.PollWorkerTaskRequest{Frame: &dexpb.PollWorkerTaskRequest_Register{
			Register: &dexpb.PollWorkerTaskRegister{Queue: queue, AppWorkerId: workerID, FlowTypes: []string{flowType}},
		}}))
		registered, err := stream.Recv()
		require.NoError(t, err)
		require.NotNil(t, registered.GetRegistered())
		go worker.serve(queue, stream)
	}
	return worker
}

func (w *rawPullWorker) serve(queue string, stream dexpb.WorkerTaskService_PollWorkerTaskClient) {
	for {
		frame, err := stream.Recv()
		if err != nil {
			return
		}
		if frame.GetPing() != nil {
			if err := stream.Send(&dexpb.PollWorkerTaskRequest{Frame: &dexpb.PollWorkerTaskRequest_Pong{Pong: &dexpb.PollWorkerTaskPong{}}}); err != nil {
				w.errors <- err
				return
			}
			continue
		}
		task := frame.GetTask()
		if task == nil {
			continue
		}
		method := ""
		observation := pullTaskObservation{queue: queue}
		var output *dexpb.PollWorkerTaskFrame
		switch request := task.GetRequest().(type) {
		case *dexpb.PollWorkerTask_WaitFor:
			method = "wait_for"
			if w.disconnectOnWaitFor {
				w.events <- pullTaskObservation{queue: queue, method: method, flowID: request.WaitFor.GetContext().GetFlowId(), attempt: request.WaitFor.GetContext().GetAttempt()}
				w.stop()
				return
			}
			waitingCondition := &dexpb.WaitingCondition{
				WaitingConditionType: dexpb.WaitingConditionType_WAITING_CONDITION_TYPE_ALL_COMPLETED,
				ChannelConditions:    []*dexpb.ChannelCondition{{ChannelName: dispatchProbeChannel}},
			}
			if w.spawnSubFlow {
				waitingCondition.ChannelConditions = nil
				waitingCondition.SubFlowConditions = []*dexpb.SubFlowCondition{{
					SubFlowType: request.WaitFor.GetFlowType(), StartStepType: "ChildStep",
					StepOptions: &dexpb.StepOptions{SkipWaitFor: true},
				}}
			}
			output = &dexpb.PollWorkerTaskFrame{TaskId: task.GetTaskId(), Output: &dexpb.PollWorkerTaskFrame_WaitFor{
				WaitFor: &dexpb.InvokeWaitForMethodOutput{Output: &dexpb.InvokeWaitForMethodOutput_Result{
					Result: &dexpb.InvokeWaitForMethodResponse{WaitingCondition: waitingCondition},
				}},
			}}
			observation = pullTaskObservation{queue: queue, method: method, flowID: request.WaitFor.GetContext().GetFlowId(), attempt: request.WaitFor.GetContext().GetAttempt()}
		case *dexpb.PollWorkerTask_Execute:
			method = "execute"
			decision := &dexpb.StepDecision{
				CloseDecision: common.GracefulCompleteDecision(stringValue(request.Execute.GetContext().GetFlowId())),
			}
			if w.spawnParallel {
				decision.CloseDecision = &dexpb.CloseDecision{
					CloseDecisionType: dexpb.CloseDecisionType_CLOSE_DECISION_TYPE_DEAD_END,
				}
				if request.Execute.GetStepType() == dispatchProbeStep {
					decision.CloseDecision = nil
					decision.NextSteps = []*dexpb.StepMovement{
						{StepType: "ParallelFirst", StepOptions: &dexpb.StepOptions{SkipWaitFor: true}},
						{StepType: "ParallelSecond", StepOptions: &dexpb.StepOptions{SkipWaitFor: true}},
					}
				}
			}
			output = &dexpb.PollWorkerTaskFrame{TaskId: task.GetTaskId(), Output: &dexpb.PollWorkerTaskFrame_Execute{
				Execute: &dexpb.InvokeExecuteMethodOutput{Output: &dexpb.InvokeExecuteMethodOutput_Result{
					Result: &dexpb.InvokeExecuteMethodResponse{StepDecision: decision},
				}},
			}}
			observation = pullTaskObservation{queue: queue, method: method, flowID: request.Execute.GetContext().GetFlowId(), attempt: request.Execute.GetContext().GetAttempt()}
			if w.blockParallel && request.Execute.GetStepType() != dispatchProbeStep {
				w.started <- observation
				<-w.releaseParallel
			}
		case *dexpb.PollWorkerTask_Rpc:
			method = "rpc"
			if w.rpcError != nil {
				output = &dexpb.PollWorkerTaskFrame{TaskId: task.GetTaskId(), Output: &dexpb.PollWorkerTaskFrame_WorkerError{WorkerError: w.rpcError}}
			} else {
				output = &dexpb.PollWorkerTaskFrame{TaskId: task.GetTaskId(), Output: &dexpb.PollWorkerTaskFrame_Rpc{
					Rpc: &dexpb.InvokeWorkerRPCResponse{Output: stringValue("pull-rpc-ok")},
				}}
			}
			observation = pullTaskObservation{queue: queue, method: method, flowID: request.Rpc.GetContext().GetFlowId(), attempt: request.Rpc.GetContext().GetAttempt()}
		default:
			w.errors <- fmt.Errorf("unexpected pull task request %T", task.GetRequest())
			return
		}
		if err := stream.Send(&dexpb.PollWorkerTaskRequest{Frame: &dexpb.PollWorkerTaskRequest_TaskFrame{TaskFrame: output}}); err != nil {
			w.errors <- err
			return
		}
		w.events <- observation
	}
}

func (w *rawPullWorker) stop() {
	for _, cancel := range w.cancellations {
		cancel()
	}
}

func (w *rawPullWorker) stopQueue(queue string) {
	w.queueCancels[queue]()
}

func TestPullWorkerDispatchTemporal(t *testing.T) {
	if !*temporalIntegTest || *dexServerAddress != "" {
		t.Skip()
	}
	testPullWorkerDispatch(t, service.BackendTypeTemporal)
}

func TestPullWorkerDispatchCadence(t *testing.T) {
	if !*cadenceIntegTest || *dexServerAddress != "" {
		t.Skip()
	}
	testPullWorkerDispatch(t, service.BackendTypeCadence)
}

func TestPullWorkerUnavailableTemporal(t *testing.T) {
	if !*temporalIntegTest || *dexServerAddress != "" {
		t.Skip()
	}
	testPullWorkerUnavailable(t, service.BackendTypeTemporal)
}

func TestPullWorkerStickyFallbackTemporal(t *testing.T) {
	if !*temporalIntegTest || *dexServerAddress != "" {
		t.Skip()
	}
	testPullWorkerStickyFallback(t, service.BackendTypeTemporal)
}

func TestPullWorkerStickyFallbackCadence(t *testing.T) {
	if !*cadenceIntegTest || *dexServerAddress != "" {
		t.Skip()
	}
	testPullWorkerStickyFallback(t, service.BackendTypeCadence)
}

func TestPullWorkerAppearsDuringRetriesTemporal(t *testing.T) {
	if !*temporalIntegTest || *dexServerAddress != "" {
		t.Skip()
	}
	testPullWorkerAppearsDuringRetries(t, service.BackendTypeTemporal)
}

func TestPullWorkerAppearsDuringRetriesCadence(t *testing.T) {
	if !*cadenceIntegTest || *dexServerAddress != "" {
		t.Skip()
	}
	testPullWorkerAppearsDuringRetries(t, service.BackendTypeCadence)
}

func TestPullWorkerDisconnectTemporal(t *testing.T) {
	if !*temporalIntegTest || *dexServerAddress != "" {
		t.Skip()
	}
	testPullWorkerDisconnect(t, service.BackendTypeTemporal)
}

func TestPullWorkerDisconnectCadence(t *testing.T) {
	if !*cadenceIntegTest || *dexServerAddress != "" {
		t.Skip()
	}
	testPullWorkerDisconnect(t, service.BackendTypeCadence)
}

func TestPullWorkerAsyncLocalTemporal(t *testing.T) {
	if !*temporalIntegTest || *dexServerAddress != "" {
		t.Skip()
	}
	testPullWorkerAsyncLocal(t, service.BackendTypeTemporal)
}

func TestPullWorkerAsyncLocalCadence(t *testing.T) {
	if !*cadenceIntegTest || *dexServerAddress != "" {
		t.Skip()
	}
	testPullWorkerAsyncLocal(t, service.BackendTypeCadence)
}

func TestPullSubFlowStartsWithoutStickyQueueTemporal(t *testing.T) {
	if !*temporalIntegTest || *dexServerAddress != "" {
		t.Skip()
	}
	testPullSubFlowStartsWithoutStickyQueue(t, service.BackendTypeTemporal)
}

func TestPullSubFlowStartsWithoutStickyQueueCadence(t *testing.T) {
	if !*cadenceIntegTest || *dexServerAddress != "" {
		t.Skip()
	}
	testPullSubFlowStartsWithoutStickyQueue(t, service.BackendTypeCadence)
}

func TestPullParallelLastWriteTemporal(t *testing.T) {
	if !*temporalIntegTest || *dexServerAddress != "" {
		t.Skip()
	}
	testPullParallelLastWrite(t, service.BackendTypeTemporal)
}

func TestPullParallelLastWriteCadence(t *testing.T) {
	if !*cadenceIntegTest || *dexServerAddress != "" {
		t.Skip()
	}
	testPullParallelLastWrite(t, service.BackendTypeCadence)
}

func testPullParallelLastWrite(t *testing.T, backend service.BackendType) {
	t.Helper()
	runtime := startInProcessDexService(t, DexServiceTestConfig{
		BackendType: backend, Matching: &config.MatchingConfig{
			DispatchWait: 3 * time.Second, StickyWait: 100 * time.Millisecond,
		},
	})
	firstWorkerID := "pull-parallel-first-" + uuid.NewString()
	firstWorker := startParallelPullWorker(t, runtime.PublicAddress, dispatchProbeFlowType, firstWorkerID, true)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	flowID := "pull-parallel-" + uuid.NewString()
	require.NoError(t, startDispatchProbe(ctx, runtime.FlowClient, flowID, dispatchProbeStep,
		&dexpb.WorkerTarget{IsPullMode: true}, nil))
	require.Equal(t, dispatchProbeFlowType, waitForPullTask(t, firstWorker, flowID).queue)
	waitForDexTaskQueue(t, ctx, runtime.UnifiedClient, flowID, firstWorkerID)
	firstWorker.stopQueue(dispatchProbeFlowType)
	secondWorkerID := "pull-parallel-second-" + uuid.NewString()
	secondWorker := startParallelPullWorker(t, runtime.PublicAddress, dispatchProbeFlowType, secondWorkerID, false)
	_, err := runtime.FlowClient.PublishToChannel(ctx, &dexpb.PublishToChannelRequest{
		FlowId: flowID, Messages: []*dexpb.ChannelMessage{{ChannelName: dispatchProbeChannel}},
	})
	require.NoError(t, err)
	require.Equal(t, firstWorkerID, waitForPullTask(t, firstWorker, flowID).queue)
	select {
	case started := <-firstWorker.started:
		require.Equal(t, flowID, started.flowID)
	case <-ctx.Done():
		t.Fatal("first parallel Step was not dispatched")
	}
	secondTask := waitForPullTask(t, secondWorker, flowID)
	require.Equal(t, "execute", secondTask.method)
	require.Equal(t, dispatchProbeFlowType, secondTask.queue)
	waitForDexTaskQueue(t, ctx, runtime.UnifiedClient, flowID, secondWorkerID)
	close(firstWorker.releaseParallel)
	require.Equal(t, firstWorkerID, waitForPullTask(t, firstWorker, flowID).queue)
	waitForDexTaskQueue(t, ctx, runtime.UnifiedClient, flowID, firstWorkerID)
	_, err = runtime.FlowClient.StopFlow(ctx, &dexpb.StopFlowRequest{FlowId: flowID})
	require.NoError(t, err)
}

func testPullSubFlowStartsWithoutStickyQueue(t *testing.T, backend service.BackendType) {
	t.Helper()
	runtime := startInProcessDexService(t, DexServiceTestConfig{BackendType: backend, Matching: &config.MatchingConfig{}})
	workerID := "pull-subflow-worker-" + uuid.NewString()
	worker := startSubFlowPullWorker(t, runtime.PublicAddress, dispatchProbeFlowType, workerID)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	flowID := "pull-subflow-" + uuid.NewString()
	require.NoError(t, startDispatchProbe(ctx, runtime.FlowClient, flowID, dispatchProbeStep,
		&dexpb.WorkerTarget{IsPullMode: true}, nil))
	require.Equal(t, dispatchProbeFlowType, waitForPullTask(t, worker, flowID).queue)
	waitForDexTaskQueue(t, ctx, runtime.UnifiedClient, flowID, workerID)
	childFlowID := "SubFlow:" + flowID + "-" + dispatchProbeStep + "-1-0"
	childTask := waitForPullTask(t, worker, childFlowID)
	require.Equal(t, "execute", childTask.method)
	require.Equal(t, dispatchProbeFlowType, childTask.queue)
	result, err := runtime.FlowClient.WaitForFlow(ctx, &dexpb.WaitForFlowRequest{FlowId: flowID, WaitTimeSeconds: 25})
	require.NoError(t, err)
	require.Equal(t, dexpb.FlowStatus_FLOW_STATUS_COMPLETED, result.GetFlowStatus(), result.GetErrorMessage())
	require.Equal(t, workerID, waitForPullTask(t, worker, flowID).queue)
}

func testPullWorkerAsyncLocal(t *testing.T, backend service.BackendType) {
	t.Helper()
	stepOptions := &dexpb.StepOptions{
		WaitForDurabilityOverride: dexpb.StepDurability_STEP_DURABILITY_ASYNC,
		ExecuteDurabilityOverride: dexpb.StepDurability_STEP_DURABILITY_ASYNC,
		WaitForRetryPolicy:        &dexpb.RetryPolicy{MaximumAttempts: 5, InitialIntervalSeconds: 1},
	}
	for _, isWorkerIdle := range []bool{true, false} {
		t.Run(fmt.Sprintf("worker_idle_%t", isWorkerIdle), func(t *testing.T) {
			recorder := &wakeRecorder{}
			wakeServer := httptest.NewServer(recorder)
			t.Cleanup(wakeServer.Close)
			runtime := startInProcessDexService(t, DexServiceTestConfig{
				BackendType: backend,
				Matching: &config.MatchingConfig{
					DispatchWait: 3 * time.Second, StickyWait: time.Second,
					LocalDispatchWait: 200 * time.Millisecond, LocalStickyWait: 100 * time.Millisecond,
					LocalExecutionReserve: 50 * time.Millisecond,
				},
				Wake: &config.WakeConfig{URL: wakeServer.URL, Budget: 5 * time.Second},
			})
			var worker *rawPullWorker
			if isWorkerIdle {
				worker = startRawPullWorker(t, runtime.PublicAddress, dispatchProbeFlowType, "pull-async-idle-"+uuid.NewString(), nil)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			flowID := "pull-async-" + uuid.NewString()
			require.NoError(t, startDispatchProbe(ctx, runtime.FlowClient, flowID, dispatchProbeStep,
				&dexpb.WorkerTarget{IsPullMode: true}, stepOptions))
			if !isWorkerIdle {
				require.Eventually(t, func() bool { return recorder.count("wake") > 0 }, 10*time.Second, 20*time.Millisecond)
				worker = startRawPullWorker(t, runtime.PublicAddress, dispatchProbeFlowType, "pull-async-late-"+uuid.NewString(), nil)
			}
			observation := waitForPullTask(t, worker, flowID)
			require.Equal(t, "wait_for", observation.method)
			if isWorkerIdle {
				require.Equal(t, int32(1), observation.attempt)
			} else {
				require.Greater(t, observation.attempt, int32(1))
			}
			_, err := runtime.FlowClient.PublishToChannel(ctx, &dexpb.PublishToChannelRequest{
				FlowId: flowID, Messages: []*dexpb.ChannelMessage{{ChannelName: dispatchProbeChannel}},
			})
			require.NoError(t, err)
			result, err := runtime.FlowClient.WaitForFlow(ctx, &dexpb.WaitForFlowRequest{FlowId: flowID, WaitTimeSeconds: 25})
			require.NoError(t, err)
			require.Equal(t, dexpb.FlowStatus_FLOW_STATUS_COMPLETED, result.GetFlowStatus())
		})
	}
}

func TestPullWorkerQueueSurvivesContinueAsNewTemporal(t *testing.T) {
	if !*temporalIntegTest || *dexServerAddress != "" {
		t.Skip()
	}
	testPullWorkerQueueSurvivesContinueAsNew(t, service.BackendTypeTemporal)
}

func TestPullWorkerQueueSurvivesContinueAsNewCadence(t *testing.T) {
	if !*cadenceIntegTest || *dexServerAddress != "" {
		t.Skip()
	}
	testPullWorkerQueueSurvivesContinueAsNew(t, service.BackendTypeCadence)
}

func testPullWorkerQueueSurvivesContinueAsNew(t *testing.T, backend service.BackendType) {
	t.Helper()
	runtime := startInProcessDexService(t, DexServiceTestConfig{BackendType: backend, Matching: &config.MatchingConfig{}})
	workerID := "pull-continue-worker-" + uuid.NewString()
	worker := startRawPullWorker(t, runtime.PublicAddress, dispatchProbeFlowType, workerID, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	flowID := "pull-continue-" + uuid.NewString()
	startStep := ""
	if backend == service.BackendTypeCadence {
		startStep = dispatchProbeStep
	}
	startResponse, err := runtime.FlowClient.StartFlow(ctx, &dexpb.StartFlowRequest{
		RequestId: newRequestID(), FlowId: flowID, FlowType: dispatchProbeFlowType,
		FlowTimeoutSeconds: 120, StartStepType: startStep,
		FlowStartOptions: withWorkerTarget(nil, &dexpb.WorkerTarget{IsPullMode: true}),
	})
	require.NoError(t, err)
	if backend == service.BackendTypeTemporal {
		_, err = runtime.FlowClient.InvokeRPC(ctx, &dexpb.InvokeRPCRequest{
			RequestId: newRequestID(), FlowId: flowID, RpcName: "getProbe", IsTransactional: true, TimeoutSeconds: 15,
		})
		require.NoError(t, err)
	}
	require.Equal(t, dispatchProbeFlowType, waitForPullTask(t, worker, flowID).queue)
	waitForDexTaskQueue(t, ctx, runtime.UnifiedClient, flowID, workerID)
	_, err = runtime.FlowClient.TriggerContinueAsNew(ctx, &dexpb.TriggerContinueAsNewRequest{
		FlowId: flowID, RunId: startResponse.GetRunId(),
	})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		currentRunID, queryErr := currentFlowRunID(ctx, runtime, flowID)
		return queryErr == nil && currentRunID != startResponse.GetRunId()
	}, 10*time.Second, 50*time.Millisecond)
	waitForDexTaskQueue(t, ctx, runtime.UnifiedClient, flowID, workerID)
	_, err = runtime.FlowClient.InvokeRPC(ctx, &dexpb.InvokeRPCRequest{
		RequestId: newRequestID(), FlowId: flowID, RpcName: "getProbe", TimeoutSeconds: 15,
	})
	require.NoError(t, err)
	require.Equal(t, workerID, waitForPullTask(t, worker, flowID).queue)
	_, err = runtime.FlowClient.StopFlow(ctx, &dexpb.StopFlowRequest{FlowId: flowID})
	require.NoError(t, err)
}

func testPullWorkerDisconnect(t *testing.T, backend service.BackendType) {
	t.Helper()
	runtime := startInProcessDexService(t, DexServiceTestConfig{
		BackendType: backend,
		Matching:    &config.MatchingConfig{DispatchWait: time.Second, StickyWait: 100 * time.Millisecond},
	})
	firstWorker := startDisconnectingPullWorker(t, runtime.PublicAddress, dispatchProbeFlowType, "pull-disconnect-"+uuid.NewString())
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	flowID := "pull-disconnect-" + uuid.NewString()
	require.NoError(t, startDispatchProbe(ctx, runtime.FlowClient, flowID, dispatchProbeStep,
		&dexpb.WorkerTarget{IsPullMode: true},
		&dexpb.StepOptions{WaitForRetryPolicy: &dexpb.RetryPolicy{MaximumAttempts: 5, InitialIntervalSeconds: 1}},
	))
	require.Equal(t, "wait_for", waitForPullTask(t, firstWorker, flowID).method)
	secondWorker := startRawPullWorker(t, runtime.PublicAddress, dispatchProbeFlowType, "pull-replacement-"+uuid.NewString(), nil)
	require.Equal(t, "wait_for", waitForPullTask(t, secondWorker, flowID).method)
	_, err := runtime.FlowClient.PublishToChannel(ctx, &dexpb.PublishToChannelRequest{
		FlowId: flowID, Messages: []*dexpb.ChannelMessage{{ChannelName: dispatchProbeChannel}},
	})
	require.NoError(t, err)
	result, err := runtime.FlowClient.WaitForFlow(ctx, &dexpb.WaitForFlowRequest{FlowId: flowID, WaitTimeSeconds: 25})
	require.NoError(t, err)
	require.Equal(t, dexpb.FlowStatus_FLOW_STATUS_COMPLETED, result.GetFlowStatus())
}

func testPullWorkerAppearsDuringRetries(t *testing.T, backend service.BackendType) {
	t.Helper()
	recorder := &wakeRecorder{}
	wakeServer := httptest.NewServer(recorder)
	t.Cleanup(wakeServer.Close)
	runtime := startInProcessDexService(t, DexServiceTestConfig{
		BackendType: backend,
		Matching:    &config.MatchingConfig{DispatchWait: 100 * time.Millisecond, StickyWait: 50 * time.Millisecond},
		Wake:        &config.WakeConfig{URL: wakeServer.URL, Budget: 5 * time.Second},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	flowID := "pull-retry-" + uuid.NewString()
	require.NoError(t, startDispatchProbe(ctx, runtime.FlowClient, flowID, dispatchProbeStep,
		&dexpb.WorkerTarget{IsPullMode: true},
		&dexpb.StepOptions{WaitForRetryPolicy: &dexpb.RetryPolicy{MaximumAttempts: 5, InitialIntervalSeconds: 1}},
	))
	require.Eventually(t, func() bool { return recorder.count("wake") > 0 }, 10*time.Second, 50*time.Millisecond)
	worker := startRawPullWorker(t, runtime.PublicAddress, dispatchProbeFlowType, "pull-retry-worker-"+uuid.NewString(), nil)
	require.Equal(t, "wait_for", waitForPullTask(t, worker, flowID).method)
	_, err := runtime.FlowClient.PublishToChannel(ctx, &dexpb.PublishToChannelRequest{
		FlowId: flowID, Messages: []*dexpb.ChannelMessage{{ChannelName: dispatchProbeChannel}},
	})
	require.NoError(t, err)
	result, err := runtime.FlowClient.WaitForFlow(ctx, &dexpb.WaitForFlowRequest{FlowId: flowID, WaitTimeSeconds: 25})
	require.NoError(t, err)
	require.Equal(t, dexpb.FlowStatus_FLOW_STATUS_COMPLETED, result.GetFlowStatus())
}

func testPullWorkerStickyFallback(t *testing.T, backend service.BackendType) {
	t.Helper()
	runtime := startInProcessDexService(t, DexServiceTestConfig{
		BackendType: backend,
		Matching:    &config.MatchingConfig{DispatchWait: time.Second, StickyWait: 100 * time.Millisecond},
	})
	workerID := "pull-sticky-old-" + uuid.NewString()
	firstWorker := startRawPullWorker(t, runtime.PublicAddress, dispatchProbeFlowType, workerID, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	flowID := "pull-sticky-fallback-" + uuid.NewString()
	require.NoError(t, startDispatchProbe(ctx, runtime.FlowClient, flowID, dispatchProbeStep,
		&dexpb.WorkerTarget{IsPullMode: true}, nil))
	require.Equal(t, dispatchProbeFlowType, waitForPullTask(t, firstWorker, flowID).queue)
	waitForDexTaskQueue(t, ctx, runtime.UnifiedClient, flowID, workerID)
	firstWorker.stop()
	secondWorker := startRawPullWorker(t, runtime.PublicAddress, dispatchProbeFlowType, "pull-sticky-new-"+uuid.NewString(), nil)
	_, err := runtime.FlowClient.PublishToChannel(ctx, &dexpb.PublishToChannelRequest{
		FlowId: flowID, Messages: []*dexpb.ChannelMessage{{ChannelName: dispatchProbeChannel}},
	})
	require.NoError(t, err)
	result, err := runtime.FlowClient.WaitForFlow(ctx, &dexpb.WaitForFlowRequest{FlowId: flowID, WaitTimeSeconds: 25})
	require.NoError(t, err)
	require.Equal(t, dexpb.FlowStatus_FLOW_STATUS_COMPLETED, result.GetFlowStatus())
	require.Equal(t, dispatchProbeFlowType, waitForPullTask(t, secondWorker, flowID).queue)
}

func TestPullWorkerUnavailableCadence(t *testing.T) {
	if !*cadenceIntegTest || *dexServerAddress != "" {
		t.Skip()
	}
	testPullWorkerUnavailable(t, service.BackendTypeCadence)
}

func TestPullWorkerErrorMatchesPushTemporal(t *testing.T) {
	if !*temporalIntegTest || *dexServerAddress != "" {
		t.Skip()
	}
	testPullWorkerErrorMatchesPush(t, service.BackendTypeTemporal)
}

func TestPullWorkerErrorMatchesPushCadence(t *testing.T) {
	if !*cadenceIntegTest || *dexServerAddress != "" {
		t.Skip()
	}
	testPullWorkerErrorMatchesPush(t, service.BackendTypeCadence)
}

func TestPullRPCDispatchRespectsCallerDeadlineTemporal(t *testing.T) {
	if !*temporalIntegTest || *dexServerAddress != "" {
		t.Skip()
	}
	testPullRPCDispatchRespectsCallerDeadline(t, service.BackendTypeTemporal)
}

func TestPullRPCDispatchRespectsCallerDeadlineCadence(t *testing.T) {
	if !*cadenceIntegTest || *dexServerAddress != "" {
		t.Skip()
	}
	testPullRPCDispatchRespectsCallerDeadline(t, service.BackendTypeCadence)
}

func testPullRPCDispatchRespectsCallerDeadline(t *testing.T, backend service.BackendType) {
	t.Helper()
	runtime := startInProcessDexService(t, DexServiceTestConfig{
		BackendType: backend,
		Matching:    &config.MatchingConfig{DispatchWait: 5 * time.Second, StickyWait: time.Second},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	flowID := "pull-rpc-deadline-" + uuid.NewString()
	_, err := runtime.FlowClient.StartFlow(ctx, &dexpb.StartFlowRequest{
		RequestId: newRequestID(), FlowId: flowID, FlowType: dispatchProbeFlowType,
		FlowTimeoutSeconds: 120, FlowStartOptions: withWorkerTarget(nil, &dexpb.WorkerTarget{IsPullMode: true}),
	})
	require.NoError(t, err)
	waitForDexTaskQueue(t, ctx, runtime.UnifiedClient, flowID, "")
	for _, isTransactional := range []bool{false, true} {
		callCtx, cancelCall := context.WithTimeout(ctx, 400*time.Millisecond)
		startedAt := time.Now()
		_, err = runtime.FlowClient.InvokeRPC(callCtx, &dexpb.InvokeRPCRequest{
			RequestId: newRequestID(), FlowId: flowID, RpcName: "getProbe", IsTransactional: isTransactional,
			TimeoutSeconds: 10,
		})
		cancelCall()
		require.Error(t, err)
		require.Less(t, time.Since(startedAt), 2*time.Second)
	}
	_, err = runtime.FlowClient.StopFlow(ctx, &dexpb.StopFlowRequest{FlowId: flowID})
	require.NoError(t, err)
}

func testPullWorkerErrorMatchesPush(t *testing.T, backend service.BackendType) {
	t.Helper()
	runtime := startInProcessDexService(t, DexServiceTestConfig{BackendType: backend, Matching: &config.MatchingConfig{}})
	pushTarget := startWorker(t, pushRPCErrorWorker{})
	startRawPullWorker(t, runtime.PublicAddress, dispatchProbeFlowType, "pull-error-"+uuid.NewString(),
		&dexpb.WorkerErrorResponse{Detail: "invalid Worker input", ErrorType: "InputError", StackTrace: "worker stack", GrpcStatus: int32(codes.InvalidArgument)})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var observedErrors []*dexpb.ServiceErrorResponse
	var observedStatusCodes []codes.Code
	for _, target := range []*dexpb.WorkerTarget{pushTarget, &dexpb.WorkerTarget{IsPullMode: true}} {
		flowID := "pull-error-compare-" + uuid.NewString()
		_, err := runtime.FlowClient.StartFlow(ctx, &dexpb.StartFlowRequest{
			RequestId: newRequestID(), FlowId: flowID, FlowType: dispatchProbeFlowType,
			FlowTimeoutSeconds: 120, FlowStartOptions: withWorkerTarget(nil, target),
		})
		require.NoError(t, err)
		_, err = runtime.FlowClient.InvokeRPC(ctx, &dexpb.InvokeRPCRequest{
			RequestId: newRequestID(), FlowId: flowID, RpcName: "getProbe", TimeoutSeconds: 15,
		})
		require.Error(t, err)
		observedStatusCodes = append(observedStatusCodes, status.Code(err))
		observedErrors = append(observedErrors, grpcServiceErrorResponse(t, err))
		_, err = runtime.FlowClient.StopFlow(ctx, &dexpb.StopFlowRequest{FlowId: flowID})
		require.NoError(t, err)
	}
	require.Equal(t, observedStatusCodes[0], observedStatusCodes[1])
	require.Equal(t, observedErrors[0].GetSubStatus(), observedErrors[1].GetSubStatus())
	require.Equal(t, observedErrors[0].GetOriginalWorkerErrorStatus(), observedErrors[1].GetOriginalWorkerErrorStatus())
	require.Equal(t, int32(codes.InvalidArgument), observedErrors[1].GetOriginalWorkerErrorStatus())
	require.Equal(t, observedErrors[0].GetOriginalWorkerErrorDetail(), observedErrors[1].GetOriginalWorkerErrorDetail())
	require.Equal(t, observedErrors[0].GetOriginalWorkerErrorType(), observedErrors[1].GetOriginalWorkerErrorType())
}

func testPullWorkerUnavailable(t *testing.T, backend service.BackendType) {
	t.Helper()
	runtime := startInProcessDexService(t, DexServiceTestConfig{
		BackendType: backend,
		Matching:    &config.MatchingConfig{DispatchWait: 100 * time.Millisecond, StickyWait: 50 * time.Millisecond},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	flowID := "pull-unavailable-" + uuid.NewString()
	require.NoError(t, startDispatchProbe(ctx, runtime.FlowClient, flowID, dispatchProbeStep,
		&dexpb.WorkerTarget{IsPullMode: true},
		&dexpb.StepOptions{WaitForRetryPolicy: &dexpb.RetryPolicy{MaximumAttempts: 2, InitialIntervalSeconds: 1}},
	))
	result, err := runtime.FlowClient.WaitForFlow(ctx, &dexpb.WaitForFlowRequest{FlowId: flowID, WaitTimeSeconds: 25})
	require.NoError(t, err)
	require.Equal(t, dexpb.FlowStatus_FLOW_STATUS_FAILED, result.GetFlowStatus())
	require.True(t, strings.Contains(result.GetErrorMessage(), "no Worker available for queue "+dispatchProbeFlowType), result.GetErrorMessage())
}

func testPullWorkerDispatch(t *testing.T, backend service.BackendType) {
	t.Helper()
	runtime := startInProcessDexService(t, DexServiceTestConfig{BackendType: backend, Matching: &config.MatchingConfig{}})
	workerID := "pull-worker-" + uuid.NewString()
	worker := startRawPullWorker(t, runtime.PublicAddress, dispatchProbeFlowType, workerID, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	flowID := "pull-dispatch-" + uuid.NewString()
	require.NoError(t, startDispatchProbe(ctx, runtime.FlowClient, flowID, dispatchProbeStep,
		&dexpb.WorkerTarget{IsPullMode: true}, nil))
	first := waitForPullTask(t, worker, flowID)
	require.Equal(t, pullTaskObservation{queue: dispatchProbeFlowType, method: "wait_for", flowID: flowID, attempt: 1}, first)
	waitForDexTaskQueue(t, ctx, runtime.UnifiedClient, flowID, workerID)
	_, err := runtime.FlowClient.PublishToChannel(ctx, &dexpb.PublishToChannelRequest{
		FlowId: flowID, Messages: []*dexpb.ChannelMessage{{ChannelName: dispatchProbeChannel}},
	})
	require.NoError(t, err)
	result, err := runtime.FlowClient.WaitForFlow(ctx, &dexpb.WaitForFlowRequest{FlowId: flowID, WaitTimeSeconds: 30})
	require.NoError(t, err)
	require.Equal(t, dexpb.FlowStatus_FLOW_STATUS_COMPLETED, result.GetFlowStatus())
	second := waitForPullTask(t, worker, flowID)
	require.Equal(t, pullTaskObservation{queue: workerID, method: "execute", flowID: flowID, attempt: 1}, second)

	rpcFlowID := "pull-rpc-" + uuid.NewString()
	_, err = runtime.FlowClient.StartFlow(ctx, &dexpb.StartFlowRequest{
		RequestId: newRequestID(), FlowId: rpcFlowID, FlowType: dispatchProbeFlowType,
		FlowTimeoutSeconds: 120, FlowStartOptions: withWorkerTarget(nil, &dexpb.WorkerTarget{IsPullMode: true}),
	})
	require.NoError(t, err)
	for rpcIndex, transactionMode := range []bool{false, true, false} {
		_, err = runtime.FlowClient.InvokeRPC(ctx, &dexpb.InvokeRPCRequest{
			RequestId: newRequestID(), FlowId: rpcFlowID, RpcName: "getProbe", IsTransactional: transactionMode,
			TimeoutSeconds: 15,
		})
		require.NoError(t, err)
		observed := waitForPullTask(t, worker, rpcFlowID)
		require.Equal(t, "rpc", observed.method)
		expectedQueue := dispatchProbeFlowType
		if rpcIndex == 2 && backend == service.BackendTypeTemporal {
			expectedQueue = workerID
		}
		require.Equal(t, expectedQueue, observed.queue)
	}
	_, err = runtime.FlowClient.StopFlow(ctx, &dexpb.StopFlowRequest{FlowId: rpcFlowID})
	require.NoError(t, err)
}

func waitForPullTask(t *testing.T, worker *rawPullWorker, flowID string) pullTaskObservation {
	t.Helper()
	for {
		select {
		case event := <-worker.events:
			if event.flowID == flowID {
				return event
			}
		case err := <-worker.errors:
			require.NoError(t, err)
		case <-time.After(30 * time.Second):
			t.Fatal("timed out waiting for pull task")
		}
	}
}

func waitForDexTaskQueue(
	t *testing.T, ctx context.Context, client uclient.UnifiedClient, flowID string, expectedQueue string,
) {
	t.Helper()
	require.Eventually(t, func() bool {
		var preparation dexpb.PrepareRpcQueryResponse
		err := client.QueryWorkflow(ctx, &preparation, flowID, "", service.PrepareRpcQueryType, &dexpb.PrepareRpcQueryRequest{})
		return err == nil && preparation.GetDexTaskQueue() == expectedQueue
	}, 10*time.Second, 50*time.Millisecond)
}
