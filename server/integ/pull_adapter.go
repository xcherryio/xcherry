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

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/superdurable/dex/config"
	"github.com/superdurable/dex/gen/dexpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
	stream dexpb.WorkerTaskService_PollWorkerTaskClient
	worker dexpb.WorkerServiceClient
	mu     sync.Mutex
}

var pullWorkerAdapters sync.Map

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

func (client *pullWorkerFlowClient) StartFlow(
	ctx context.Context, request *dexpb.StartFlowRequest, options ...grpc.CallOption,
) (*dexpb.StartFlowResponse, error) {
	if target := request.GetFlowStartOptions().GetFlowConfigOverride().GetWorkerTarget(); target != nil {
		if loaded, ok := pullWorkerAdapters.Load(target); ok {
			adapter := loaded.(*pullWorkerAdapter)
			if err := adapter.registerFlowType(client.test, client.serverAddress, request.GetFlowType()); err != nil {
				return nil, err
			}
		}
	}
	return client.FlowServiceClient.StartFlow(ctx, request, options...)
}

func (adapter *pullWorkerAdapter) registerFlowType(t *testing.T, serverAddress string, flowType string) error {
	adapter.mu.Lock()
	defer adapter.mu.Unlock()
	registrationKey := serverAddress + "/" + flowType
	if adapter.registered[registrationKey] {
		return nil
	}
	pushConnection, err := newDexClientConnection(adapter.pushAddress, config.DefaultGrpcMaxMessageBytes)
	if err != nil {
		return err
	}
	pullConnection, err := newDexClientConnection(serverAddress, config.DefaultGrpcMaxMessageBytes)
	if err != nil {
		return errors.Join(err, pushConnection.Close())
	}
	t.Cleanup(func() {
		require.NoError(t, pullConnection.Close())
		require.NoError(t, pushConnection.Close())
	})
	worker := dexpb.NewWorkerServiceClient(pushConnection)
	for _, queue := range []string{flowType, adapter.workerID} {
		for range 4 {
			streamCtx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			stream, streamErr := dexpb.NewWorkerTaskServiceClient(pullConnection).PollWorkerTask(streamCtx)
			if streamErr != nil {
				return streamErr
			}
			registration := &dexpb.PollWorkerTaskRegister{
				Queue: queue, AppWorkerId: adapter.workerID, FlowTypes: []string{flowType},
				SdkName: "integ-pull-adapter",
			}
			if sendErr := stream.Send(&dexpb.PollWorkerTaskRequest{Frame: &dexpb.PollWorkerTaskRequest_Register{Register: registration}}); sendErr != nil {
				return sendErr
			}
			acknowledgment, receiveErr := stream.Recv()
			if receiveErr != nil {
				return receiveErr
			}
			if acknowledgment.GetRegistered() == nil {
				return fmt.Errorf("pull Worker registration on %s was not acknowledged", queue)
			}
			go (&pullAdapterStream{stream: stream, worker: worker}).serve(streamCtx)
		}
	}
	adapter.registered[registrationKey] = true
	return nil
}

func (adapter *pullAdapterStream) serve(ctx context.Context) {
	var taskCancel context.CancelFunc
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
			taskCtx := ctx
			if deadline := task.GetDeadline(); deadline != nil {
				taskCtx, taskCancel = context.WithDeadline(ctx, deadline.AsTime())
			} else {
				taskCtx, taskCancel = context.WithCancel(ctx)
			}
			go adapter.runTask(taskCtx, task, taskCancel)
		}
		if response.GetReconnect() != nil {
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
