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
	"errors"
	"fmt"
	"io"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/superdurable/dex/config"
	"github.com/superdurable/dex/gen/dexpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

type pullWorkerAdapter struct {
	workerID    string
	pushAddress string
	mu          sync.Mutex
	registered  map[string]bool
}

type pullWorkerFlowClient struct {
	dexpb.FlowServiceClient
	test          *testing.T
	serverAddress string
}

type pullAdapterStream struct {
	stream        dexpb.WorkerTaskService_PollWorkerTaskClient
	pullClient    dexpb.WorkerTaskServiceClient
	worker        dexpb.WorkerServiceClient
	adapter       *pullWorkerAdapter
	test          *testing.T
	serverAddress string
	queue         string
	flowType      string
	streamCancel  context.CancelFunc
	mu            sync.Mutex
}

var pullWorkerAdapters sync.Map
var pullFlowTypeOwners sync.Map
var pullAdapterRegistrationTimeout = 5 * time.Second

func registerPullWorkerAdapter(t *testing.T, pushAddress string) *dexpb.WorkerTarget {
	t.Helper()
	target := &dexpb.WorkerTarget{IsPullMode: true}
	adapter := &pullWorkerAdapter{
		workerID: "integ-" + uuid.NewString(), pushAddress: pushAddress,
		registered: make(map[string]bool),
	}
	pullWorkerAdapters.Store(target, adapter)
	t.Cleanup(func() { pullWorkerAdapters.Delete(target) })
	return target
}

func configurePullWorkerClient(t *testing.T, runtime *integRuntime) *integRuntime {
	t.Helper()
	if *integWorkerMode == "pull" {
		runtime.FlowClient = &pullWorkerFlowClient{
			FlowServiceClient: runtime.FlowClient, test: t, serverAddress: runtime.PublicAddress,
		}
	}
	return runtime
}

func requirePushWorkers(t *testing.T, reason string) {
	t.Helper()
	if *integWorkerMode == "pull" {
		t.Skipf("requires push Workers: %s", reason)
	}
}

func (client *pullWorkerFlowClient) StartFlow(
	ctx context.Context, request *dexpb.StartFlowRequest, options ...grpc.CallOption,
) (*dexpb.StartFlowResponse, error) {
	if target := request.GetFlowStartOptions().GetFlowConfigOverride().GetWorkerTarget(); target != nil {
		if target.GetIsPullMode() {
			loaded, ok := pullWorkerAdapters.Load(target)
			if !ok {
				return nil, fmt.Errorf("pull Worker target for Flow type %s was not registered by startWorker", request.GetFlowType())
			}
			adapter := loaded.(*pullWorkerAdapter)
			if err := adapter.registerFlowType(client.test, client.serverAddress, request.GetFlowType()); err != nil {
				return nil, err
			}
		}
	}
	return client.FlowServiceClient.StartFlow(ctx, request, options...)
}

func (client *pullWorkerFlowClient) UpdateFlowConfig(
	ctx context.Context, request *dexpb.UpdateFlowConfigRequest, options ...grpc.CallOption,
) (*emptypb.Empty, error) {
	target := request.GetFlowConfig().GetWorkerTarget()
	if target != nil && target.GetIsPullMode() {
		loaded, ok := pullWorkerAdapters.Load(target)
		if !ok {
			return nil, fmt.Errorf("pull Worker target for Flow %s was not registered by startWorker", request.GetFlowId())
		}
		summary, err := client.FlowServiceClient.GetFlowSummary(ctx, &dexpb.GetFlowSummaryRequest{FlowId: request.GetFlowId()})
		if err != nil {
			return nil, err
		}
		if err := loaded.(*pullWorkerAdapter).registerFlowType(client.test, client.serverAddress, summary.GetFlowType()); err != nil {
			return nil, err
		}
	}
	return client.FlowServiceClient.UpdateFlowConfig(ctx, request, options...)
}

func (adapter *pullWorkerAdapter) registerFlowType(t *testing.T, serverAddress string, flowType string) error {
	adapter.mu.Lock()
	defer adapter.mu.Unlock()
	registrationKey := serverAddress + "/" + flowType
	if adapter.registered[registrationKey] {
		return nil
	}
	registeredOwner, ownerExists := pullFlowTypeOwners.LoadOrStore(registrationKey, adapter)
	if ownerExists && registeredOwner != adapter {
		return fmt.Errorf("pull Flow type %s on %s is already registered by another test Worker", flowType, serverAddress)
	}
	shouldReleaseOwner := !ownerExists
	defer func() {
		if shouldReleaseOwner {
			pullFlowTypeOwners.CompareAndDelete(registrationKey, adapter)
		}
	}()
	pushConnection, err := newDexClientConnection(adapter.pushAddress, config.DefaultGrpcMaxMessageBytes)
	if err != nil {
		return err
	}
	pullConnection, err := newDexClientConnection(serverAddress, config.DefaultGrpcMaxMessageBytes)
	if err != nil {
		return errors.Join(err, pushConnection.Close())
	}
	worker := dexpb.NewWorkerServiceClient(pushConnection)
	pullClient := dexpb.NewWorkerTaskServiceClient(pullConnection)
	var streamCancels []context.CancelFunc
	registrationSucceeded := false
	defer func() {
		if !registrationSucceeded {
			for _, cancel := range streamCancels {
				cancel()
			}
			if closeErr := pullConnection.Close(); closeErr != nil {
				t.Logf("close failed pull registration: %v", closeErr)
			}
			if closeErr := pushConnection.Close(); closeErr != nil {
				t.Logf("close failed push registration: %v", closeErr)
			}
		}
	}()
	for _, queue := range []string{flowType, adapter.workerID} {
		for range 4 {
			streamCtx, cancel := context.WithCancel(context.Background())
			streamCancels = append(streamCancels, cancel)
			stream, streamCancel, streamErr := openRegisteredPullStream(streamCtx, pullClient, queue, adapter.workerID, flowType)
			if streamErr != nil {
				return streamErr
			}
			go (&pullAdapterStream{
				stream: stream, pullClient: pullClient, worker: worker, adapter: adapter,
				test: t, serverAddress: serverAddress, queue: queue, flowType: flowType, streamCancel: streamCancel,
			}).serve(streamCtx)
		}
	}
	adapter.registered[registrationKey] = true
	registrationSucceeded = true
	shouldReleaseOwner = false
	t.Cleanup(func() {
		for _, cancel := range streamCancels {
			cancel()
		}
		require.NoError(t, pullConnection.Close())
		require.NoError(t, pushConnection.Close())
		pullFlowTypeOwners.CompareAndDelete(registrationKey, adapter)
	})
	return nil
}

func openRegisteredPullStream(
	ctx context.Context, client dexpb.WorkerTaskServiceClient, queue string, workerID string, flowType string,
) (dexpb.WorkerTaskService_PollWorkerTaskClient, context.CancelFunc, error) {
	streamCtx, cancel := context.WithCancel(ctx)
	registrationSucceeded := false
	defer func() {
		if !registrationSucceeded {
			cancel()
		}
	}()
	stream, err := client.PollWorkerTask(streamCtx)
	if err != nil {
		return nil, nil, err
	}
	registration := &dexpb.PollWorkerTaskRegister{
		Queue: queue, AppWorkerId: workerID, FlowTypes: []string{flowType}, SdkName: "integ-pull-adapter",
	}
	if err := stream.Send(&dexpb.PollWorkerTaskRequest{Frame: &dexpb.PollWorkerTaskRequest_Register{Register: registration}}); err != nil {
		return nil, nil, err
	}
	type registrationResult struct {
		response *dexpb.PollWorkerTaskResponse
		err      error
	}
	acknowledgmentResult := make(chan registrationResult, 1)
	go func() {
		response, receiveErr := stream.Recv()
		acknowledgmentResult <- registrationResult{response: response, err: receiveErr}
	}()
	var acknowledgment *dexpb.PollWorkerTaskResponse
	select {
	case result := <-acknowledgmentResult:
		if result.err != nil {
			return nil, nil, result.err
		}
		acknowledgment = result.response
	case <-time.After(pullAdapterRegistrationTimeout):
		return nil, nil, fmt.Errorf("pull Worker registration on %s timed out", queue)
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	}
	if acknowledgment.GetRegistered() == nil {
		return nil, nil, fmt.Errorf("pull Worker registration on %s was not acknowledged", queue)
	}
	registrationSucceeded = true
	return stream, cancel, nil
}

func (adapter *pullAdapterStream) serve(ctx context.Context) {
	for ctx.Err() == nil {
		adapter.serveCurrent(ctx)
		adapter.streamCancel()
		if ctx.Err() != nil {
			return
		}
		for ctx.Err() == nil {
			stream, streamCancel, err := openRegisteredPullStream(ctx, adapter.pullClient, adapter.queue, adapter.adapter.workerID, adapter.flowType)
			if err == nil {
				adapter.mu.Lock()
				adapter.stream = stream
				adapter.mu.Unlock()
				adapter.streamCancel = streamCancel
				break
			}
			log.Printf("reconnect pull Worker queue %s: %v", adapter.queue, err)
			retryTimer := time.NewTimer(100 * time.Millisecond)
			select {
			case <-ctx.Done():
				retryTimer.Stop()
				return
			case <-retryTimer.C:
			}
		}
	}
}

func (adapter *pullAdapterStream) serveCurrent(ctx context.Context) {
	var taskCancel context.CancelFunc
	var taskDone chan struct{}
	taskID := ""
	for {
		response, err := adapter.stream.Recv()
		if err != nil {
			if taskCancel != nil {
				taskCancel()
			}
			return
		}
		if response.GetPing() != nil {
			if err := adapter.send(&dexpb.PollWorkerTaskRequest{Frame: &dexpb.PollWorkerTaskRequest_Pong{Pong: &dexpb.PollWorkerTaskPong{}}}); err != nil {
				return
			}
		}
		if cancellation := response.GetCancelTask(); cancellation != nil && taskCancel != nil && cancellation.GetTaskId() == taskID {
			taskCancel()
			taskCancel = nil
		}
		if task := response.GetTask(); task != nil {
			taskID = task.GetTaskId()
			currentTaskDone := make(chan struct{})
			taskDone = currentTaskDone
			taskCtx := ctx
			if deadline := task.GetDeadline(); deadline != nil {
				taskCtx, taskCancel = context.WithDeadline(ctx, deadline.AsTime())
			} else {
				taskCtx, taskCancel = context.WithCancel(ctx)
			}
			currentTaskCancel := taskCancel
			go func() {
				adapter.runTask(taskCtx, task, currentTaskCancel)
				close(currentTaskDone)
			}()
		}
		if response.GetReconnect() != nil {
			if taskDone != nil {
				select {
				case <-taskDone:
				case <-ctx.Done():
				}
			}
			return
		}
	}
}

func (adapter *pullAdapterStream) runTask(ctx context.Context, task *dexpb.PollWorkerTask, cancel context.CancelFunc) {
	defer cancel()
	var err error
	switch request := task.GetRequest().(type) {
	case *dexpb.PollWorkerTask_WaitFor:
		err = adapter.relayWaitFor(ctx, task.GetTaskId(), request.WaitFor)
	case *dexpb.PollWorkerTask_Execute:
		err = adapter.relayExecute(ctx, task.GetTaskId(), request.Execute)
	case *dexpb.PollWorkerTask_Rpc:
		var result *dexpb.InvokeWorkerRPCResponse
		result, err = adapter.worker.InvokeWorkerRPC(ctx, request.Rpc)
		if err == nil {
			err = adapter.sendTaskFrame(&dexpb.PollWorkerTaskFrame{TaskId: task.GetTaskId(), Output: &dexpb.PollWorkerTaskFrame_Rpc{Rpc: result}})
		}
	default:
		err = fmt.Errorf("unsupported pull task %T", request)
	}
	if err != nil && ctx.Err() == nil {
		workerError := &dexpb.WorkerErrorResponse{Detail: err.Error(), GrpcStatus: int32(status.Code(err))}
		if grpcStatus, ok := status.FromError(err); ok {
			workerError.Detail = grpcStatus.Message()
			for _, detail := range grpcStatus.Details() {
				if reported, ok := detail.(*dexpb.WorkerErrorResponse); ok {
					workerError = reported
					workerError.GrpcStatus = int32(grpcStatus.Code())
					break
				}
			}
		} else {
			workerError.GrpcStatus = int32(codes.Unknown)
		}
		if sendErr := adapter.sendTaskFrame(&dexpb.PollWorkerTaskFrame{TaskId: task.GetTaskId(), Output: &dexpb.PollWorkerTaskFrame_WorkerError{WorkerError: workerError}}); sendErr != nil {
			log.Printf("send pull Worker error for task %s: %v", task.GetTaskId(), sendErr)
		}
	}
}

func (adapter *pullAdapterStream) relayWaitFor(ctx context.Context, taskID string, request *dexpb.InvokeWaitForMethodRequest) error {
	outputStream, err := adapter.worker.InvokeWaitForMethod(ctx, request)
	if err != nil {
		return err
	}
	for {
		output, err := outputStream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		for _, condition := range output.GetResult().GetWaitingCondition().GetSubFlowConditions() {
			if err := adapter.adapter.registerFlowType(adapter.test, adapter.serverAddress, condition.GetSubFlowType()); err != nil {
				return err
			}
		}
		if err := adapter.sendTaskFrame(&dexpb.PollWorkerTaskFrame{TaskId: taskID, Output: &dexpb.PollWorkerTaskFrame_WaitFor{WaitFor: output}}); err != nil {
			return err
		}
	}
}

func (adapter *pullAdapterStream) relayExecute(ctx context.Context, taskID string, request *dexpb.InvokeExecuteMethodRequest) error {
	outputStream, err := adapter.worker.InvokeExecuteMethod(ctx, request)
	if err != nil {
		return err
	}
	for {
		output, err := outputStream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := adapter.sendTaskFrame(&dexpb.PollWorkerTaskFrame{TaskId: taskID, Output: &dexpb.PollWorkerTaskFrame_Execute{Execute: output}}); err != nil {
			return err
		}
	}
}

func (adapter *pullAdapterStream) sendTaskFrame(frame *dexpb.PollWorkerTaskFrame) error {
	return adapter.send(&dexpb.PollWorkerTaskRequest{Frame: &dexpb.PollWorkerTaskRequest_TaskFrame{TaskFrame: frame}})
}

func (adapter *pullAdapterStream) send(request *dexpb.PollWorkerTaskRequest) error {
	adapter.mu.Lock()
	defer adapter.mu.Unlock()
	return adapter.stream.Send(request)
}
