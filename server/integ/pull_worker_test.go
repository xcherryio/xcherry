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
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/superdurable/dex/config"
	"github.com/superdurable/dex/gen/dexpb"
	"github.com/superdurable/dex/integ/workflow/common"
	"github.com/superdurable/dex/service"
	"github.com/superdurable/dex/service/bootstrap"
	uclient "github.com/superdurable/dex/service/client"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type pushRPCErrorWorker struct{}

type pushWaitForErrorWorker struct {
	retryAfterSeconds int32
}

type pushSubFlowParentWorker struct{}

func (pushSubFlowParentWorker) InvokeWaitForMethod(
	_ context.Context, request *dexpb.InvokeWaitForMethodRequest,
) (*dexpb.InvokeWaitForMethodResponse, error) {
	if request.GetStepType() != dispatchProbeStep {
		return nil, status.Error(codes.FailedPrecondition, "child Step reached push Worker")
	}
	return &dexpb.InvokeWaitForMethodResponse{WaitingCondition: &dexpb.WaitingCondition{
		WaitingConditionType: dexpb.WaitingConditionType_WAITING_CONDITION_TYPE_ALL_COMPLETED,
		SubFlowConditions: []*dexpb.SubFlowCondition{{
			SubFlowType:   request.GetFlowType(),
			StartStepType: "ChildStep",
			StepOptions:   &dexpb.StepOptions{SkipWaitFor: true},
			Options: &dexpb.SubFlowOptions{FlowConfigOverride: &dexpb.FlowConfig{
				WorkerTarget: &dexpb.WorkerTarget{IsPullMode: true},
			}},
		}},
	}}, nil
}

func (pushSubFlowParentWorker) InvokeExecuteMethod(
	_ context.Context, request *dexpb.InvokeExecuteMethodRequest,
) (*dexpb.InvokeExecuteMethodResponse, error) {
	if request.GetStepType() != dispatchProbeStep {
		return nil, status.Error(codes.FailedPrecondition, "child Step reached push Worker")
	}
	return &dexpb.InvokeExecuteMethodResponse{StepDecision: &dexpb.StepDecision{
		CloseDecision: common.GracefulCompleteDecision(stringValue(request.GetContext().GetFlowId())),
	}}, nil
}

func (w pushWaitForErrorWorker) InvokeWaitForMethod(
	context.Context, *dexpb.InvokeWaitForMethodRequest,
) (*dexpb.InvokeWaitForMethodResponse, error) {
	workerStatus, err := status.New(codes.InvalidArgument, "invalid Worker input").WithDetails(&dexpb.WorkerErrorResponse{
		Detail: "invalid Worker input", ErrorType: "InputError", StackTrace: "worker stack",
		RetryAfterSeconds: w.retryAfterSeconds,
	})
	if err != nil {
		return nil, err
	}
	return nil, workerStatus.Err()
}

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

type pullExecuteBehavior int

const (
	pullExecuteDefault pullExecuteBehavior = iota
	pullExecuteSilentFirst
	pullExecuteCheckpointFirst
	pullExecutePeriodicHeartbeat
)

type rawPullWorker struct {
	events                  chan pullTaskObservation
	started                 chan pullTaskObservation
	canceled                chan string
	errors                  chan error
	cancellations           []context.CancelFunc
	queueCancels            map[string]context.CancelFunc
	rpcError                *dexpb.WorkerErrorResponse
	disconnectOnWaitFor     bool
	spawnSubFlow            bool
	hasInvalidSubFlowTarget bool
	spawnParallel           bool
	blockParallel           bool
	sendProgress            bool
	holdWaitFor             bool
	releaseParallel         chan struct{}
	rejectNextStickyTask    chan struct{}
	rejectedStickyTasks     chan pullTaskObservation
	executeBehavior         pullExecuteBehavior
	restoredHeartbeats      chan string
}

func startHeartbeatPullWorker(t *testing.T, address string, flowType string, behavior pullExecuteBehavior) *rawPullWorker {
	return newRawPullWorkerWithExecuteBehavior(t, address, flowType, "heartbeat-worker-"+uuid.NewString(), nil, false, false, false, false, false, false, false, behavior)
}

func startRawPullWorker(t *testing.T, address string, flowType string, workerID string, rpcError *dexpb.WorkerErrorResponse) *rawPullWorker {
	return newRawPullWorker(t, address, flowType, workerID, rpcError, false, false, false, false, false, false, false)
}

func startProgressPullWorker(t *testing.T, address string, flowType string, workerID string) *rawPullWorker {
	return newRawPullWorker(t, address, flowType, workerID, nil, false, false, false, false, true, false, false)
}

func startDisconnectingPullWorker(t *testing.T, address string, flowType string, workerID string) *rawPullWorker {
	return newRawPullWorker(t, address, flowType, workerID, nil, true, false, false, false, false, false, false)
}

func startSubFlowPullWorker(t *testing.T, address string, flowType string, workerID string) *rawPullWorker {
	return newRawPullWorker(t, address, flowType, workerID, nil, false, true, false, false, false, false, false)
}

func startInvalidSubFlowPullWorker(t *testing.T, address string, flowType string, workerID string) *rawPullWorker {
	return newRawPullWorker(t, address, flowType, workerID, nil, false, true, false, false, false, false, true)
}

func startParallelPullWorker(t *testing.T, address string, flowType string, workerID string, blockParallel bool) *rawPullWorker {
	return newRawPullWorker(t, address, flowType, workerID, nil, false, false, true, blockParallel, false, false, false)
}

func startCancelPullWorker(t *testing.T, address string, flowType string, workerID string) *rawPullWorker {
	return newRawPullWorker(t, address, flowType, workerID, nil, false, false, false, false, false, true, false)
}

func newRawPullWorker(t *testing.T, address string, flowType string, workerID string, rpcError *dexpb.WorkerErrorResponse, disconnectOnWaitFor bool, spawnSubFlow bool, spawnParallel bool, blockParallel bool, sendProgress bool, holdWaitFor bool, hasInvalidSubFlowTarget bool) *rawPullWorker {
	return newRawPullWorkerWithExecuteBehavior(t, address, flowType, workerID, rpcError, disconnectOnWaitFor, spawnSubFlow, spawnParallel, blockParallel, sendProgress, holdWaitFor, hasInvalidSubFlowTarget, pullExecuteDefault)
}

func newRawPullWorkerWithExecuteBehavior(t *testing.T, address string, flowType string, workerID string, rpcError *dexpb.WorkerErrorResponse, disconnectOnWaitFor bool, spawnSubFlow bool, spawnParallel bool, blockParallel bool, sendProgress bool, holdWaitFor bool, hasInvalidSubFlowTarget bool, executeBehavior pullExecuteBehavior) *rawPullWorker {
	t.Helper()
	connection, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, connection.Close()) })
	worker := &rawPullWorker{
		events: make(chan pullTaskObservation, 16), started: make(chan pullTaskObservation, 16), canceled: make(chan string, 4),
		errors: make(chan error, 4), queueCancels: map[string]context.CancelFunc{}, releaseParallel: make(chan struct{}),
		rejectNextStickyTask: make(chan struct{}, 1), rejectedStickyTasks: make(chan pullTaskObservation, 1),
		restoredHeartbeats: make(chan string, 2), executeBehavior: executeBehavior,
		rpcError: rpcError, disconnectOnWaitFor: disconnectOnWaitFor, spawnSubFlow: spawnSubFlow,
		spawnParallel: spawnParallel, blockParallel: blockParallel, sendProgress: sendProgress, holdWaitFor: holdWaitFor,
		hasInvalidSubFlowTarget: hasInvalidSubFlowTarget,
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
	heldTaskCancels := make(map[string]context.CancelFunc)
	defer func() {
		for _, cancel := range heldTaskCancels {
			cancel()
		}
	}()
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
		if cancellation := frame.GetCancelTask(); cancellation != nil {
			if cancel, exists := heldTaskCancels[cancellation.GetTaskId()]; exists {
				cancel()
				delete(heldTaskCancels, cancellation.GetTaskId())
			}
			w.canceled <- cancellation.GetTaskId()
			continue
		}
		task := frame.GetTask()
		if task == nil {
			continue
		}
		if queue != taskFlowType(task) {
			select {
			case <-w.rejectNextStickyTask:
				if err := stream.Send(&dexpb.PollWorkerTaskRequest{Frame: &dexpb.PollWorkerTaskRequest_Reject{Reject: &dexpb.PollWorkerTaskReject{TaskId: task.GetTaskId(), Reason: dexpb.PollWorkerTaskRejectReason_POLL_WORKER_TASK_REJECT_REASON_NO_CAPACITY}}}); err != nil {
					w.errors <- err
					return
				}
				w.rejectedStickyTasks <- pullTaskObservation{queue: queue}
				continue
			default:
			}
		}
		method := ""
		observation := pullTaskObservation{queue: queue}
		var output *dexpb.PollWorkerTaskFrame
		switch request := task.GetRequest().(type) {
		case *dexpb.PollWorkerTask_WaitFor:
			method = "wait_for"
			if w.rpcError != nil {
				output = &dexpb.PollWorkerTaskFrame{TaskId: task.GetTaskId(), Output: &dexpb.PollWorkerTaskFrame_WorkerError{WorkerError: w.rpcError}}
				observation = pullTaskObservation{queue: queue, method: method, flowID: request.WaitFor.GetContext().GetFlowId(), attempt: request.WaitFor.GetContext().GetAttempt()}
				break
			}
			if w.holdWaitFor {
				heldContext, cancel := context.WithCancel(context.Background())
				heldTaskCancels[task.GetTaskId()] = cancel
				go w.sendHeldWaitForHeartbeats(heldContext, stream, task.GetTaskId())
				w.events <- pullTaskObservation{queue: queue, method: method, flowID: request.WaitFor.GetContext().GetFlowId(), attempt: request.WaitFor.GetContext().GetAttempt()}
				continue
			}
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
				if w.hasInvalidSubFlowTarget {
					waitingCondition.SubFlowConditions[0].Options = &dexpb.SubFlowOptions{FlowConfigOverride: &dexpb.FlowConfig{
						WorkerTarget: &dexpb.WorkerTarget{Address: "127.0.0.1:9000", IsPullMode: true},
					}}
				}
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
			if observation.attempt == 1 && w.executeBehavior == pullExecuteSilentFirst {
				w.events <- observation
				continue
			}
			if observation.attempt == 1 && w.executeBehavior == pullExecuteCheckpointFirst {
				heartbeat := &dexpb.PollWorkerTaskFrame{TaskId: task.GetTaskId(), Output: &dexpb.PollWorkerTaskFrame_Execute{
					Execute: &dexpb.InvokeExecuteMethodOutput{Output: &dexpb.InvokeExecuteMethodOutput_Heartbeat{
						Heartbeat: &dexpb.StepMethodHeartbeat{Value: stringValue("pull-checkpoint")},
					}},
				}}
				if err := stream.Send(&dexpb.PollWorkerTaskRequest{Frame: &dexpb.PollWorkerTaskRequest_TaskFrame{TaskFrame: heartbeat}}); err != nil {
					w.errors <- err
					return
				}
				w.events <- observation
				continue
			}
			if observation.attempt > 1 && w.executeBehavior == pullExecuteCheckpointFirst {
				w.restoredHeartbeats <- request.Execute.GetContext().GetLastHeartbeatValue().GetStringValue()
			}
			if w.executeBehavior == pullExecutePeriodicHeartbeat {
				if err := w.sendPeriodicExecuteHeartbeats(stream, task.GetTaskId()); err != nil {
					w.errors <- err
					return
				}
			}
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
		if w.sendProgress && method != "rpc" {
			if err := w.sendProgressFrames(stream, task.GetTaskId(), method); err != nil {
				w.errors <- err
				return
			}
		}
		if err := stream.Send(&dexpb.PollWorkerTaskRequest{Frame: &dexpb.PollWorkerTaskRequest_TaskFrame{TaskFrame: output}}); err != nil {
			w.errors <- err
			return
		}
		w.events <- observation
	}
}

func (w *rawPullWorker) sendHeldWaitForHeartbeats(ctx context.Context, stream dexpb.WorkerTaskService_PollWorkerTaskClient, taskID string) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			frame := &dexpb.PollWorkerTaskFrame{TaskId: taskID, Output: &dexpb.PollWorkerTaskFrame_WaitFor{
				WaitFor: &dexpb.InvokeWaitForMethodOutput{Output: &dexpb.InvokeWaitForMethodOutput_Heartbeat{Heartbeat: &dexpb.StepMethodHeartbeat{}}},
			}}
			if err := stream.Send(&dexpb.PollWorkerTaskRequest{Frame: &dexpb.PollWorkerTaskRequest_TaskFrame{TaskFrame: frame}}); err != nil {
				w.errors <- err
				return
			}
		}
	}
}

func (w *rawPullWorker) sendPeriodicExecuteHeartbeats(stream dexpb.WorkerTaskService_PollWorkerTaskClient, taskID string) error {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for heartbeatCount := 0; heartbeatCount < 4; heartbeatCount++ {
		<-ticker.C
		frame := &dexpb.PollWorkerTaskFrame{TaskId: taskID, Output: &dexpb.PollWorkerTaskFrame_Execute{
			Execute: &dexpb.InvokeExecuteMethodOutput{Output: &dexpb.InvokeExecuteMethodOutput_Heartbeat{Heartbeat: &dexpb.StepMethodHeartbeat{}}},
		}}
		if err := stream.Send(&dexpb.PollWorkerTaskRequest{Frame: &dexpb.PollWorkerTaskRequest_TaskFrame{TaskFrame: frame}}); err != nil {
			return err
		}
	}
	return nil
}

func taskFlowType(task *dexpb.PollWorkerTask) string {
	switch request := task.GetRequest().(type) {
	case *dexpb.PollWorkerTask_WaitFor:
		return request.WaitFor.GetFlowType()
	case *dexpb.PollWorkerTask_Execute:
		return request.Execute.GetFlowType()
	case *dexpb.PollWorkerTask_Rpc:
		return request.Rpc.GetFlowType()
	default:
		return ""
	}
}

func (w *rawPullWorker) sendProgressFrames(
	stream dexpb.WorkerTaskService_PollWorkerTaskClient, taskID string, method string,
) error {
	var heartbeat *dexpb.PollWorkerTaskFrame
	var streamWrite *dexpb.PollWorkerTaskFrame
	progress := &dexpb.StepStreamWrite{
		StreamName: method + "-progress", StreamCapacityBytes: 1 << 20,
		Value: stringValue(method + "-progress"),
	}
	if method == "wait_for" {
		heartbeat = &dexpb.PollWorkerTaskFrame{TaskId: taskID, Output: &dexpb.PollWorkerTaskFrame_WaitFor{
			WaitFor: &dexpb.InvokeWaitForMethodOutput{Output: &dexpb.InvokeWaitForMethodOutput_Heartbeat{
				Heartbeat: &dexpb.StepMethodHeartbeat{Value: stringValue("wait-checkpoint")},
			}},
		}}
		streamWrite = &dexpb.PollWorkerTaskFrame{TaskId: taskID, Output: &dexpb.PollWorkerTaskFrame_WaitFor{
			WaitFor: &dexpb.InvokeWaitForMethodOutput{Output: &dexpb.InvokeWaitForMethodOutput_StreamWrite{StreamWrite: progress}},
		}}
	} else {
		heartbeat = &dexpb.PollWorkerTaskFrame{TaskId: taskID, Output: &dexpb.PollWorkerTaskFrame_Execute{
			Execute: &dexpb.InvokeExecuteMethodOutput{Output: &dexpb.InvokeExecuteMethodOutput_Heartbeat{
				Heartbeat: &dexpb.StepMethodHeartbeat{Value: stringValue("execute-checkpoint")},
			}},
		}}
		streamWrite = &dexpb.PollWorkerTaskFrame{TaskId: taskID, Output: &dexpb.PollWorkerTaskFrame_Execute{
			Execute: &dexpb.InvokeExecuteMethodOutput{Output: &dexpb.InvokeExecuteMethodOutput_StreamWrite{StreamWrite: progress}},
		}}
	}
	for _, frame := range []*dexpb.PollWorkerTaskFrame{heartbeat, streamWrite} {
		if err := stream.Send(&dexpb.PollWorkerTaskRequest{Frame: &dexpb.PollWorkerTaskRequest_TaskFrame{TaskFrame: frame}}); err != nil {
			return err
		}
	}
	return nil
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

func TestPullWorkerSplitRoleTemporal(t *testing.T) {
	if !*temporalIntegTest || *dexServerAddress != "" {
		t.Skip()
	}
	testPullWorkerSplitRole(t, service.BackendTypeTemporal)
}

func TestPullWorkerSplitRoleCadence(t *testing.T) {
	if !*cadenceIntegTest || *dexServerAddress != "" {
		t.Skip()
	}
	testPullWorkerSplitRole(t, service.BackendTypeCadence)
}

func TestPullWorkerAllInOneTemporal(t *testing.T) {
	if !*temporalIntegTest || *dexServerAddress != "" {
		t.Skip()
	}
	testPullWorkerAllInOne(t, service.BackendTypeTemporal)
}

func TestPullWorkerAllInOneCadence(t *testing.T) {
	if !*cadenceIntegTest || *dexServerAddress != "" {
		t.Skip()
	}
	testPullWorkerAllInOne(t, service.BackendTypeCadence)
}

func TestPullRPCWhenMatchingClosesTemporal(t *testing.T) {
	if !*temporalIntegTest || *dexServerAddress != "" {
		t.Skip()
	}
	testPullRPCWhenMatchingCloses(t, service.BackendTypeTemporal)
}

func TestPullRPCWhenMatchingClosesCadence(t *testing.T) {
	if !*cadenceIntegTest || *dexServerAddress != "" {
		t.Skip()
	}
	testPullRPCWhenMatchingCloses(t, service.BackendTypeCadence)
}

func testPullRPCWhenMatchingCloses(t *testing.T, backend service.BackendType) {
	t.Helper()
	runtime := startInProcessDexService(t, DexServiceTestConfig{BackendType: backend, Matching: &config.MatchingConfig{}})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	flowID := "matching-closed-rpc-" + uuid.NewString()
	_, err := runtime.FlowClient.StartFlow(ctx, &dexpb.StartFlowRequest{
		RequestId: newRequestID(), FlowId: flowID, FlowType: dispatchProbeFlowType,
		FlowTimeoutSeconds: 120, FlowStartOptions: withWorkerTarget(nil, &dexpb.WorkerTarget{IsPullMode: true}),
	})
	require.NoError(t, err)
	runtime.Matching.Close()
	startedAt := time.Now()
	_, err = runtime.FlowClient.InvokeRPC(ctx, &dexpb.InvokeRPCRequest{
		RequestId: newRequestID(), FlowId: flowID, RpcName: "getProbe", TimeoutSeconds: 5,
	})
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.Less(t, time.Since(startedAt), time.Second)
	_, err = runtime.FlowClient.StopFlow(ctx, &dexpb.StopFlowRequest{FlowId: flowID})
	require.NoError(t, err)
}

func testPullWorkerAllInOne(t *testing.T, backend service.BackendType) {
	t.Helper()
	cfg := createTestConfig(t, DexServiceTestConfig{BackendType: backend, Matching: &config.MatchingConfig{}})
	runtime := startPullRuntime(t, &cfg, bootstrap.Services{API: true, Interpreter: true, Matching: true})
	connection := newDexClientConnectionWithCredential(t, runtime.publicAddress, "", config.DefaultGrpcMaxMessageBytes)
	flowClient := dexpb.NewFlowServiceClient(connection)
	worker := startRawPullWorker(t, runtime.publicAddress, dispatchProbeFlowType, "all-in-one-worker-"+uuid.NewString(), nil)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	flowID := "pull-all-in-one-" + uuid.NewString()
	require.NoError(t, startDispatchProbe(ctx, flowClient, flowID, dispatchProbeStep, &dexpb.WorkerTarget{IsPullMode: true}, nil))
	require.Equal(t, "wait_for", waitForPullTask(t, worker, flowID).method)
	_, err := flowClient.PublishToChannel(ctx, &dexpb.PublishToChannelRequest{FlowId: flowID, Messages: []*dexpb.ChannelMessage{{ChannelName: dispatchProbeChannel}}})
	require.NoError(t, err)
	result, err := flowClient.WaitForFlow(ctx, &dexpb.WaitForFlowRequest{FlowId: flowID, WaitTimeSeconds: 25})
	require.NoError(t, err)
	require.Equal(t, dexpb.FlowStatus_FLOW_STATUS_COMPLETED, result.GetFlowStatus(), result.GetErrorMessage())
	rpcFlowID := "pull-all-in-one-rpc-" + uuid.NewString()
	_, err = flowClient.StartFlow(ctx, &dexpb.StartFlowRequest{
		RequestId: newRequestID(), FlowId: rpcFlowID, FlowType: dispatchProbeFlowType,
		FlowTimeoutSeconds: 120, FlowStartOptions: withWorkerTarget(nil, &dexpb.WorkerTarget{IsPullMode: true}),
	})
	require.NoError(t, err)
	_, err = flowClient.InvokeRPC(ctx, &dexpb.InvokeRPCRequest{
		RequestId: newRequestID(), FlowId: rpcFlowID, RpcName: "getProbe", TimeoutSeconds: 10,
	})
	require.NoError(t, err)
	require.Equal(t, "rpc", waitForPullTask(t, worker, rpcFlowID).method)
	_, err = flowClient.StopFlow(ctx, &dexpb.StopFlowRequest{FlowId: rpcFlowID})
	require.NoError(t, err)
}

func TestPullWorkerCancellationTemporal(t *testing.T) {
	if !*temporalIntegTest || *dexServerAddress != "" {
		t.Skip()
	}
	testPullWorkerCancellation(t, service.BackendTypeTemporal)
}

func TestPullWorkerCancellationCadence(t *testing.T) {
	if !*cadenceIntegTest || *dexServerAddress != "" {
		t.Skip()
	}
	testPullWorkerCancellation(t, service.BackendTypeCadence)
}

func TestPullWorkerHeartbeatTimeoutTemporal(t *testing.T) {
	if !*temporalIntegTest || *dexServerAddress != "" {
		t.Skip()
	}
	testPullWorkerHeartbeatTimeout(t, service.BackendTypeTemporal)
}

func TestPullWorkerHeartbeatTimeoutCadence(t *testing.T) {
	if !*cadenceIntegTest || *dexServerAddress != "" {
		t.Skip()
	}
	testPullWorkerHeartbeatTimeout(t, service.BackendTypeCadence)
}

func testPullWorkerHeartbeatTimeout(t *testing.T, backend service.BackendType) {
	t.Helper()
	for _, testCase := range []struct {
		name     string
		behavior pullExecuteBehavior
	}{
		{name: "silent_assigned_worker", behavior: pullExecuteSilentFirst},
		{name: "checkpoint_restored_on_retry", behavior: pullExecuteCheckpointFirst},
		{name: "periodic_worker_heartbeat", behavior: pullExecutePeriodicHeartbeat},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			runtime := startInProcessDexService(t, DexServiceTestConfig{
				BackendType: backend,
				Matching:    &config.MatchingConfig{DispatchWait: 6 * time.Second, StickyWait: 100 * time.Millisecond},
			})
			worker := startHeartbeatPullWorker(t, runtime.PublicAddress, dispatchProbeFlowType, testCase.behavior)
			ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
			defer cancel()
			flowID := "pull-heartbeat-" + uuid.NewString()
			stepOptions := &dexpb.StepOptions{
				SkipWaitFor: true, HeartbeatTimeoutSeconds: 2,
				ExecuteRetryPolicy: &dexpb.RetryPolicy{InitialIntervalSeconds: 1, MaximumIntervalSeconds: 1, MaximumAttempts: 2},
			}
			require.NoError(t, startDispatchProbe(ctx, runtime.FlowClient, flowID, dispatchProbeStep,
				&dexpb.WorkerTarget{IsPullMode: true}, stepOptions))
			if testCase.behavior != pullExecutePeriodicHeartbeat {
				first := waitForPullTask(t, worker, flowID)
				require.Equal(t, "execute", first.method)
				require.Equal(t, int32(1), first.attempt)
				replacement := startHeartbeatPullWorker(t, runtime.PublicAddress, dispatchProbeFlowType, testCase.behavior)
				startedAt := time.Now()
				second := waitForPullTask(t, replacement, flowID)
				require.Equal(t, "execute", second.method)
				require.Equal(t, int32(2), second.attempt)
				require.GreaterOrEqual(t, time.Since(startedAt), 1500*time.Millisecond)
				require.Less(t, time.Since(startedAt), 8*time.Second)
				if testCase.behavior == pullExecuteCheckpointFirst {
					require.Equal(t, "pull-checkpoint", <-replacement.restoredHeartbeats)
				}
			}
			result, err := runtime.FlowClient.WaitForFlow(ctx, &dexpb.WaitForFlowRequest{FlowId: flowID, WaitTimeSeconds: 20})
			require.NoError(t, err)
			require.Equal(t, dexpb.FlowStatus_FLOW_STATUS_COMPLETED, result.GetFlowStatus(), result.GetErrorMessage())
			if testCase.behavior == pullExecutePeriodicHeartbeat {
				observed := waitForPullTask(t, worker, flowID)
				require.Equal(t, int32(1), observed.attempt)
			}
		})
	}
	t.Run("unassigned_wait", func(t *testing.T) {
		runtime := startInProcessDexService(t, DexServiceTestConfig{
			BackendType: backend,
			Matching:    &config.MatchingConfig{DispatchWait: 6 * time.Second, StickyWait: 100 * time.Millisecond},
		})
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		flowID := "pull-unassigned-" + uuid.NewString()
		require.NoError(t, startDispatchProbe(ctx, runtime.FlowClient, flowID, dispatchProbeStep,
			&dexpb.WorkerTarget{IsPullMode: true}, &dexpb.StepOptions{
				SkipWaitFor: true, HeartbeatTimeoutSeconds: 2,
				ExecuteRetryPolicy: &dexpb.RetryPolicy{MaximumAttempts: 1},
			}))
		select {
		case <-time.After(3500 * time.Millisecond):
		case <-ctx.Done():
			t.Fatal("timed out while waiting to register Worker")
		}
		worker := startHeartbeatPullWorker(t, runtime.PublicAddress, dispatchProbeFlowType, pullExecuteDefault)
		observed := waitForPullTask(t, worker, flowID)
		require.Equal(t, int32(1), observed.attempt)
		result, err := runtime.FlowClient.WaitForFlow(ctx, &dexpb.WaitForFlowRequest{FlowId: flowID, WaitTimeSeconds: 15})
		require.NoError(t, err)
		require.Equal(t, dexpb.FlowStatus_FLOW_STATUS_COMPLETED, result.GetFlowStatus(), result.GetErrorMessage())
	})
}

func testPullWorkerCancellation(t *testing.T, backend service.BackendType) {
	t.Helper()
	runtime := startInProcessDexService(t, DexServiceTestConfig{BackendType: backend, Matching: &config.MatchingConfig{}})
	worker := startCancelPullWorker(t, runtime.PublicAddress, dispatchProbeFlowType, "cancel-worker-"+uuid.NewString())
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	flowID := "pull-cancel-" + uuid.NewString()
	require.NoError(t, startDispatchProbe(ctx, runtime.FlowClient, flowID, dispatchProbeStep, &dexpb.WorkerTarget{IsPullMode: true}, nil))
	require.Equal(t, "wait_for", waitForPullTask(t, worker, flowID).method)
	_, err := runtime.FlowClient.StopFlow(ctx, &dexpb.StopFlowRequest{FlowId: flowID})
	require.NoError(t, err)
	select {
	case taskID := <-worker.canceled:
		require.NotEmpty(t, taskID)
	case <-ctx.Done():
		t.Fatal("pull Worker did not receive CancelTask")
	}
}

func testPullWorkerSplitRole(t *testing.T, backend service.BackendType) {
	t.Helper()
	internalTokenFile := filepath.Join(t.TempDir(), "internal-token")
	require.NoError(t, os.WriteFile(internalTokenFile, []byte(uuid.NewString()+"\n"), 0o600))
	firstConfig := createTestConfig(t, DexServiceTestConfig{BackendType: backend, Matching: &config.MatchingConfig{}})
	firstConfig.Internal.TokenFile = internalTokenFile
	firstConfig.Membership = &config.MembershipConfig{Listen: "127.0.0.1:0", BroadcastAddress: "127.0.0.1", RefreshInterval: 50 * time.Millisecond}
	first := startPullRuntime(t, &firstConfig, bootstrap.Services{API: true, Interpreter: true})
	require.Eventually(t, first.hasMembershipAddress, 10*time.Second, 50*time.Millisecond)
	secondConfig := createTestConfig(t, DexServiceTestConfig{BackendType: backend, Matching: &config.MatchingConfig{}})
	secondConfig.Internal.TokenFile = internalTokenFile
	secondConfig.Membership = &config.MembershipConfig{
		Listen: "127.0.0.1:0", BroadcastAddress: "127.0.0.1", RefreshInterval: 50 * time.Millisecond,
		Seeds: config.MembershipSeedsConfig{Static: []string{first.runtime.MembershipAddress()}},
	}
	startPullRuntime(t, &secondConfig, bootstrap.Services{Matching: true})
	connection := newDexClientConnectionWithCredential(t, first.publicAddress, "", config.DefaultGrpcMaxMessageBytes)
	registration := &pullRegistrationAttempt{t: t, connection: connection, queue: "routing-probe-" + uuid.NewString()}
	require.Eventually(t, registration.tryRegister, 10*time.Second, 50*time.Millisecond)
	flowClient := dexpb.NewFlowServiceClient(connection)
	worker := startRawPullWorker(t, first.publicAddress, dispatchProbeFlowType, "split-worker-"+uuid.NewString(), nil)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	flowID := "pull-split-" + uuid.NewString()
	require.NoError(t, startDispatchProbe(ctx, flowClient, flowID, dispatchProbeStep, &dexpb.WorkerTarget{IsPullMode: true}, nil))
	require.Equal(t, dispatchProbeFlowType, waitForPullTask(t, worker, flowID).queue)
	_, err := flowClient.PublishToChannel(ctx, &dexpb.PublishToChannelRequest{FlowId: flowID, Messages: []*dexpb.ChannelMessage{{ChannelName: dispatchProbeChannel}}})
	require.NoError(t, err)
	result, err := flowClient.WaitForFlow(ctx, &dexpb.WaitForFlowRequest{FlowId: flowID, WaitTimeSeconds: 25})
	require.NoError(t, err)
	require.Equal(t, dexpb.FlowStatus_FLOW_STATUS_COMPLETED, result.GetFlowStatus(), result.GetErrorMessage())
	rpcFlowID := "pull-split-rpc-" + uuid.NewString()
	_, err = flowClient.StartFlow(ctx, &dexpb.StartFlowRequest{
		RequestId: newRequestID(), FlowId: rpcFlowID, FlowType: dispatchProbeFlowType,
		FlowTimeoutSeconds: 120, FlowStartOptions: withWorkerTarget(nil, &dexpb.WorkerTarget{IsPullMode: true}),
	})
	require.NoError(t, err)
	_, err = flowClient.InvokeRPC(ctx, &dexpb.InvokeRPCRequest{
		RequestId: newRequestID(), FlowId: rpcFlowID, RpcName: "getProbe", TimeoutSeconds: 10,
	})
	require.NoError(t, err)
	require.Equal(t, "rpc", waitForPullTask(t, worker, rpcFlowID).method)
	_, err = flowClient.StopFlow(ctx, &dexpb.StopFlowRequest{FlowId: rpcFlowID})
	require.NoError(t, err)
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

func TestPullWorkerStickyCapacityRecoveryTemporal(t *testing.T) {
	if !*temporalIntegTest || *dexServerAddress != "" {
		t.Skip()
	}
	testPullWorkerStickyCapacityRecovery(t, service.BackendTypeTemporal)
}

func TestPullWorkerStickyCapacityRecoveryCadence(t *testing.T) {
	if !*cadenceIntegTest || *dexServerAddress != "" {
		t.Skip()
	}
	testPullWorkerStickyCapacityRecovery(t, service.BackendTypeCadence)
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

func TestPushParentRoutesSubFlowToPullWorkerTemporal(t *testing.T) {
	if !*temporalIntegTest || *dexServerAddress != "" {
		t.Skip()
	}
	testPushParentRoutesSubFlowToPullWorker(t, service.BackendTypeTemporal)
}

func TestPushParentRoutesSubFlowToPullWorkerCadence(t *testing.T) {
	if !*cadenceIntegTest || *dexServerAddress != "" {
		t.Skip()
	}
	testPushParentRoutesSubFlowToPullWorker(t, service.BackendTypeCadence)
}

func testPushParentRoutesSubFlowToPullWorker(t *testing.T, backend service.BackendType) {
	t.Helper()
	runtime := startInProcessDexService(t, DexServiceTestConfig{BackendType: backend, Matching: &config.MatchingConfig{}})
	pushTarget := startWorker(t, pushSubFlowParentWorker{})
	pullWorkerID := "pull-subflow-override-" + uuid.NewString()
	pullWorker := startRawPullWorker(t, runtime.PublicAddress, dispatchProbeFlowType, pullWorkerID, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	flowID := "push-subflow-parent-" + uuid.NewString()
	require.NoError(t, startDispatchProbe(ctx, runtime.FlowClient, flowID, dispatchProbeStep, pushTarget, nil))
	childFlowID := "SubFlow:" + flowID + "-" + dispatchProbeStep + "-1-0"
	childTask := waitForPullTask(t, pullWorker, childFlowID)
	require.Equal(t, pullTaskObservation{queue: dispatchProbeFlowType, method: "execute", flowID: childFlowID, attempt: 1}, childTask)
	result, err := runtime.FlowClient.WaitForFlow(ctx, &dexpb.WaitForFlowRequest{FlowId: flowID, WaitTimeSeconds: 25})
	require.NoError(t, err)
	require.Equal(t, dexpb.FlowStatus_FLOW_STATUS_COMPLETED, result.GetFlowStatus(), result.GetErrorMessage())
}

func TestPullSubFlowRejectsInvalidTargetTemporal(t *testing.T) {
	if !*temporalIntegTest || *dexServerAddress != "" {
		t.Skip()
	}
	testPullSubFlowRejectsInvalidTarget(t, service.BackendTypeTemporal)
}

func TestPullSubFlowRejectsInvalidTargetCadence(t *testing.T) {
	if !*cadenceIntegTest || *dexServerAddress != "" {
		t.Skip()
	}
	testPullSubFlowRejectsInvalidTarget(t, service.BackendTypeCadence)
}

func testPullSubFlowRejectsInvalidTarget(t *testing.T, backend service.BackendType) {
	t.Helper()
	cfg := createTestConfig(t, DexServiceTestConfig{BackendType: backend, Matching: &config.MatchingConfig{}})
	runtime := startPullRuntime(t, &cfg, bootstrap.Services{API: true, Interpreter: true, Matching: true})
	connection := newDexClientConnectionWithCredential(t, runtime.publicAddress, "", config.DefaultGrpcMaxMessageBytes)
	flowClient := dexpb.NewFlowServiceClient(connection)
	startInvalidSubFlowPullWorker(t, runtime.publicAddress, dispatchProbeFlowType, "invalid-subflow-"+uuid.NewString())
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	flowID := "invalid-subflow-" + uuid.NewString()
	require.NoError(t, startDispatchProbe(ctx, flowClient, flowID, dispatchProbeStep, &dexpb.WorkerTarget{IsPullMode: true},
		&dexpb.StepOptions{WaitForRetryPolicy: &dexpb.RetryPolicy{MaximumAttempts: 1}}))
	startedAt := time.Now()
	result, err := flowClient.WaitForFlow(ctx, &dexpb.WaitForFlowRequest{FlowId: flowID, WaitTimeSeconds: 25})
	require.NoError(t, err)
	require.Equal(t, dexpb.FlowStatus_FLOW_STATUS_FAILED, result.GetFlowStatus())
	require.Equal(t, dexpb.FlowErrorType_FLOW_ERROR_TYPE_INVALID_USER_FLOW_CODE, result.GetErrorType())
	require.Equal(t, "SubFlow start rejected: pull worker_target cannot specify address", result.GetErrorMessage())
	require.Less(t, time.Since(startedAt), 10*time.Second)
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
		WaitForRetryPolicy:        &dexpb.RetryPolicy{MaximumAttempts: 3, InitialIntervalSeconds: 1},
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
				},
				Wake: &config.WakeConfig{URL: wakeServer.URL, Budget: 5 * time.Second},
			})
			var worker *rawPullWorker
			startedAt := time.Now()
			if isWorkerIdle {
				worker = startRawPullWorker(t, runtime.PublicAddress, dispatchProbeFlowType, "pull-async-idle-"+uuid.NewString(), nil)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			flowID := "pull-async-" + uuid.NewString()
			require.NoError(t, startDispatchProbe(ctx, runtime.FlowClient, flowID, dispatchProbeStep,
				&dexpb.WorkerTarget{IsPullMode: true}, stepOptions))
			if !isWorkerIdle {
				require.Eventually(t, func() bool {
					return recorder.count("wake") > 0 && time.Since(startedAt) >= 700*time.Millisecond
				}, 10*time.Second, 20*time.Millisecond)
				worker = startRawPullWorker(t, runtime.PublicAddress, dispatchProbeFlowType, "pull-async-late-"+uuid.NewString(), nil)
			}
			observation := waitForPullTask(t, worker, flowID)
			require.Equal(t, "wait_for", observation.method)
			if isWorkerIdle {
				require.Equal(t, int32(1), observation.attempt)
				require.Less(t, time.Since(startedAt), 7*time.Second)
			} else {
				require.Equal(t, int32(2), observation.attempt)
				require.GreaterOrEqual(t, time.Since(startedAt), 700*time.Millisecond)
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
	t.Run("single_attempt_policy", func(t *testing.T) {
		runtime := startInProcessDexService(t, DexServiceTestConfig{BackendType: backend, Matching: &config.MatchingConfig{}})
		worker := startRawPullWorker(t, runtime.PublicAddress, dispatchProbeFlowType, "pull-async-single-"+uuid.NewString(), nil)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		flowID := "pull-async-single-" + uuid.NewString()
		require.NoError(t, startDispatchProbe(ctx, runtime.FlowClient, flowID, dispatchProbeStep,
			&dexpb.WorkerTarget{IsPullMode: true}, &dexpb.StepOptions{
				WaitForDurabilityOverride: dexpb.StepDurability_STEP_DURABILITY_ASYNC,
				WaitForRetryPolicy:        &dexpb.RetryPolicy{MaximumAttempts: 1},
			}))
		require.Equal(t, int32(1), waitForPullTask(t, worker, flowID).attempt)
		_, err := runtime.FlowClient.PublishToChannel(ctx, &dexpb.PublishToChannelRequest{
			FlowId: flowID, Messages: []*dexpb.ChannelMessage{{ChannelName: dispatchProbeChannel}},
		})
		require.NoError(t, err)
		result, err := runtime.FlowClient.WaitForFlow(ctx, &dexpb.WaitForFlowRequest{FlowId: flowID, WaitTimeSeconds: 25})
		require.NoError(t, err)
		require.Equal(t, dexpb.FlowStatus_FLOW_STATUS_COMPLETED, result.GetFlowStatus(), result.GetErrorMessage())
	})
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
		Matching:    &config.MatchingConfig{DispatchWait: 2 * time.Second, StickyWait: 100 * time.Millisecond},
	})
	firstWorker := startDisconnectingPullWorker(t, runtime.PublicAddress, dispatchProbeFlowType, "pull-disconnect-"+uuid.NewString())
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	flowID := "pull-disconnect-" + uuid.NewString()
	require.NoError(t, startDispatchProbe(ctx, runtime.FlowClient, flowID, dispatchProbeStep,
		&dexpb.WorkerTarget{IsPullMode: true},
		&dexpb.StepOptions{WaitForRetryPolicy: &dexpb.RetryPolicy{MaximumAttempts: 1}},
	))
	firstTask := waitForPullTask(t, firstWorker, flowID)
	require.Equal(t, "wait_for", firstTask.method)
	require.Equal(t, int32(1), firstTask.attempt)
	secondWorker := startRawPullWorker(t, runtime.PublicAddress, dispatchProbeFlowType, "pull-replacement-"+uuid.NewString(), nil)
	secondTask := waitForPullTask(t, secondWorker, flowID)
	require.Equal(t, "wait_for", secondTask.method)
	require.Equal(t, int32(1), secondTask.attempt)
	_, err := runtime.FlowClient.PublishToChannel(ctx, &dexpb.PublishToChannelRequest{
		FlowId: flowID, Messages: []*dexpb.ChannelMessage{{ChannelName: dispatchProbeChannel}},
	})
	require.NoError(t, err)
	result, err := runtime.FlowClient.WaitForFlow(ctx, &dexpb.WaitForFlowRequest{FlowId: flowID, WaitTimeSeconds: 25})
	require.NoError(t, err)
	require.Equal(t, dexpb.FlowStatus_FLOW_STATUS_COMPLETED, result.GetFlowStatus())
}

func testPullWorkerStickyCapacityRecovery(t *testing.T, backend service.BackendType) {
	t.Helper()
	stickyWait := 5 * time.Second
	runtime := startInProcessDexService(t, DexServiceTestConfig{
		BackendType: backend,
		Matching:    &config.MatchingConfig{DispatchWait: 7 * time.Second, StickyWait: stickyWait},
	})
	workerID := "pull-capacity-" + uuid.NewString()
	worker := startRawPullWorker(t, runtime.PublicAddress, dispatchProbeFlowType, workerID, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	flowID := "pull-capacity-" + uuid.NewString()
	require.NoError(t, startDispatchProbe(ctx, runtime.FlowClient, flowID, dispatchProbeStep,
		&dexpb.WorkerTarget{IsPullMode: true}, nil))
	require.Equal(t, dispatchProbeFlowType, waitForPullTask(t, worker, flowID).queue)
	waitForDexTaskQueue(t, ctx, runtime.UnifiedClient, flowID, workerID)
	worker.rejectNextStickyTask <- struct{}{}
	_, err := runtime.FlowClient.PublishToChannel(ctx, &dexpb.PublishToChannelRequest{
		FlowId: flowID, Messages: []*dexpb.ChannelMessage{{ChannelName: dispatchProbeChannel}},
	})
	require.NoError(t, err)
	select {
	case rejected := <-worker.rejectedStickyTasks:
		require.Equal(t, workerID, rejected.queue)
	case <-ctx.Done():
		t.Fatal("sticky Worker did not reject the task")
	}
	rejectedAt := time.Now()
	served := waitForPullTask(t, worker, flowID)
	require.Equal(t, workerID, served.queue)
	require.Equal(t, "execute", served.method)
	require.Equal(t, int32(1), served.attempt)
	require.Less(t, time.Since(rejectedAt), stickyWait/2)
	result, err := runtime.FlowClient.WaitForFlow(ctx, &dexpb.WaitForFlowRequest{FlowId: flowID, WaitTimeSeconds: 25})
	require.NoError(t, err)
	require.Equal(t, dexpb.FlowStatus_FLOW_STATUS_COMPLETED, result.GetFlowStatus(), result.GetErrorMessage())
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

func TestPullStepErrorMatchesPushTemporal(t *testing.T) {
	if !*temporalIntegTest || *dexServerAddress != "" {
		t.Skip()
	}
	testPullStepErrorMatchesPush(t, service.BackendTypeTemporal)
}

func TestPullStepErrorMatchesPushCadence(t *testing.T) {
	if !*cadenceIntegTest || *dexServerAddress != "" {
		t.Skip()
	}
	testPullStepErrorMatchesPush(t, service.BackendTypeCadence)
}

func testPullStepErrorMatchesPush(t *testing.T, backend service.BackendType) {
	t.Helper()
	retryAfterSeconds := int32(0)
	if backend == service.BackendTypeTemporal {
		retryAfterSeconds = 1
	}
	runtime := startInProcessDexService(t, DexServiceTestConfig{BackendType: backend, Matching: &config.MatchingConfig{}})
	pushTarget := startWorker(t, pushWaitForErrorWorker{retryAfterSeconds: retryAfterSeconds})
	startRawPullWorker(t, runtime.PublicAddress, dispatchProbeFlowType, "step-error-"+uuid.NewString(),
		&dexpb.WorkerErrorResponse{Detail: "invalid Worker input", ErrorType: "InputError", StackTrace: "worker stack",
			GrpcStatus: int32(codes.InvalidArgument), RetryAfterSeconds: retryAfterSeconds})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var results []*dexpb.FlowResult
	for _, target := range []*dexpb.WorkerTarget{pushTarget, {IsPullMode: true}} {
		flowID := "step-error-" + uuid.NewString()
		startedAt := time.Now()
		require.NoError(t, startDispatchProbe(ctx, runtime.FlowClient, flowID, dispatchProbeStep, target,
			&dexpb.StepOptions{WaitForRetryPolicy: &dexpb.RetryPolicy{MaximumAttempts: 2, InitialIntervalSeconds: 1}}))
		result, err := runtime.FlowClient.WaitForFlow(ctx, &dexpb.WaitForFlowRequest{FlowId: flowID, WaitTimeSeconds: 25})
		require.NoError(t, err)
		require.GreaterOrEqual(t, time.Since(startedAt), time.Second)
		results = append(results, result)
	}
	require.Equal(t, dexpb.FlowStatus_FLOW_STATUS_FAILED, results[0].GetFlowStatus())
	require.Equal(t, results[0].GetErrorType(), results[1].GetErrorType())
	require.Equal(t, dexpb.FlowErrorType_FLOW_ERROR_TYPE_WORKER_API_FAIL, results[1].GetErrorType())
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

func TestPullTransactionalRPCShortBudgetTemporal(t *testing.T) {
	if !*temporalIntegTest || *dexServerAddress != "" {
		t.Skip()
	}
	runtime := startInProcessDexService(t, DexServiceTestConfig{BackendType: service.BackendTypeTemporal, Matching: &config.MatchingConfig{}})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	flowID := "pull-short-rpc-" + uuid.NewString()
	_, err := runtime.FlowClient.StartFlow(ctx, &dexpb.StartFlowRequest{
		RequestId: newRequestID(), FlowId: flowID, FlowType: dispatchProbeFlowType,
		FlowTimeoutSeconds: 120, FlowStartOptions: withWorkerTarget(nil, &dexpb.WorkerTarget{IsPullMode: true}),
	})
	require.NoError(t, err)
	worker := startRawPullWorker(t, runtime.PublicAddress, dispatchProbeFlowType, "short-rpc-worker-"+uuid.NewString(), nil)
	startedAt := time.Now()
	response, err := runtime.FlowClient.InvokeRPC(ctx, &dexpb.InvokeRPCRequest{
		RequestId: newRequestID(), FlowId: flowID, RpcName: "getProbe", IsTransactional: true, TimeoutSeconds: 3,
	})
	require.NoError(t, err)
	require.NotNil(t, response)
	require.Less(t, time.Since(startedAt), 3*time.Second)
	require.Equal(t, "rpc", waitForPullTask(t, worker, flowID).method)
	_, err = runtime.FlowClient.StopFlow(ctx, &dexpb.StopFlowRequest{FlowId: flowID})
	require.NoError(t, err)
}

func TestPullTransactionalRPCTimeoutReleasesLockTemporal(t *testing.T) {
	if !*temporalIntegTest || *dexServerAddress != "" {
		t.Skip()
	}
	runtime := startInProcessDexService(t, DexServiceTestConfig{BackendType: service.BackendTypeTemporal, Matching: &config.MatchingConfig{}})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	flowID := "pull-rpc-lock-" + uuid.NewString()
	_, err := runtime.FlowClient.StartFlow(ctx, &dexpb.StartFlowRequest{
		RequestId: newRequestID(), FlowId: flowID, FlowType: dispatchProbeFlowType,
		FlowTimeoutSeconds: 120, FlowStartOptions: withWorkerTarget(nil, &dexpb.WorkerTarget{IsPullMode: true}),
	})
	require.NoError(t, err)
	startedAt := time.Now()
	_, err = runtime.FlowClient.InvokeRPC(ctx, &dexpb.InvokeRPCRequest{
		RequestId: newRequestID(), FlowId: flowID, RpcName: "getProbe", IsTransactional: true, TimeoutSeconds: 3,
	})
	require.Error(t, err)
	require.Less(t, time.Since(startedAt), 5*time.Second)
	worker := startRawPullWorker(t, runtime.PublicAddress, dispatchProbeFlowType, "rpc-lock-worker-"+uuid.NewString(), nil)
	startedAt = time.Now()
	_, err = runtime.FlowClient.InvokeRPC(ctx, &dexpb.InvokeRPCRequest{
		RequestId: newRequestID(), FlowId: flowID, RpcName: "getProbe", IsTransactional: true, TimeoutSeconds: 3,
	})
	require.NoError(t, err)
	require.Less(t, time.Since(startedAt), 3*time.Second)
	require.Equal(t, "rpc", waitForPullTask(t, worker, flowID).method)
	_, err = runtime.FlowClient.StopFlow(ctx, &dexpb.StopFlowRequest{FlowId: flowID})
	require.NoError(t, err)
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
	require.Equal(t, observedErrors[0].GetOriginalWorkerErrorStackTrace(), observedErrors[1].GetOriginalWorkerErrorStackTrace())
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
	startedAt := time.Now()
	result, err := runtime.FlowClient.WaitForFlow(ctx, &dexpb.WaitForFlowRequest{FlowId: flowID, WaitTimeSeconds: 25})
	require.NoError(t, err)
	require.Equal(t, dexpb.FlowStatus_FLOW_STATUS_FAILED, result.GetFlowStatus())
	require.True(t, strings.Contains(result.GetErrorMessage(), "no Worker available for queue "+dispatchProbeFlowType), result.GetErrorMessage())
	require.GreaterOrEqual(t, time.Since(startedAt), time.Second)
}

func testPullWorkerDispatch(t *testing.T, backend service.BackendType) {
	t.Helper()
	runtime := startInProcessDexService(t, DexServiceTestConfig{
		BackendType: backend, Matching: &config.MatchingConfig{},
		StreamStore: config.StreamStoreConfig{Backend: config.StreamStoreBackendMemory},
	})
	workerID := "pull-worker-" + uuid.NewString()
	worker := startProgressPullWorker(t, runtime.PublicAddress, dispatchProbeFlowType, workerID)
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
	for _, method := range []string{"wait_for", "execute"} {
		messages, listErr := runtime.FlowClient.ListStreamMessages(ctx, &dexpb.ListStreamMessagesRequest{
			FlowId: flowID, FlowType: dispatchProbeFlowType, StreamName: method + "-progress", PageSize: 10,
		})
		require.NoError(t, listErr)
		require.Len(t, messages.GetMessages(), 1)
		require.Equal(t, method+"-progress", messages.GetMessages()[0].GetValue().GetStringValue())
	}

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
