// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package dex

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"regexp"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/superdurable/dex/sdk-go/gen/dexpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type pullTestServer struct {
	dexpb.UnimplementedWorkerTaskServiceServer
	registered                   chan *pullTestStream
	mu                           sync.Mutex
	supportsUnknownNameRejection bool
	reconnectFirst               bool
	firstReconnectQueue          chan string
	streams                      []*pullTestStream
}

type pullTestStream struct {
	stream       dexpb.WorkerTaskService_PollWorkerTaskServer
	registration *dexpb.PollWorkerTaskRegister
	frames       chan *dexpb.PollWorkerTaskRequest
	abort        chan struct{}
	sendMu       sync.Mutex
}

func (server *pullTestServer) PollWorkerTask(stream dexpb.WorkerTaskService_PollWorkerTaskServer) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	registered := &pullTestStream{stream: stream, registration: first.GetRegister(), frames: make(chan *dexpb.PollWorkerTaskRequest, 16), abort: make(chan struct{})}
	server.mu.Lock()
	server.streams = append(server.streams, registered)
	shouldReconnect := server.reconnectFirst
	server.reconnectFirst = false
	server.mu.Unlock()
	if shouldReconnect {
		server.firstReconnectQueue <- registered.registration.Queue
		return stream.Send(&dexpb.PollWorkerTaskResponse{Frame: &dexpb.PollWorkerTaskResponse_Reconnect{Reconnect: &dexpb.PollWorkerTaskReconnect{Reason: "owner moved"}}})
	}
	if err := stream.Send(&dexpb.PollWorkerTaskResponse{Frame: &dexpb.PollWorkerTaskResponse_Registered{Registered: &dexpb.PollWorkerTaskRegistered{SupportsUnknownNameRejection: server.supportsUnknownNameRejection}}}); err != nil {
		return err
	}
	select {
	case server.registered <- registered:
	case <-stream.Context().Done():
		return stream.Context().Err()
	}
	received := make(chan *dexpb.PollWorkerTaskRequest)
	receiveError := make(chan error, 1)
	go func() {
		for {
			frame, receiveErr := stream.Recv()
			if receiveErr != nil {
				receiveError <- receiveErr
				return
			}
			select {
			case received <- frame:
			case <-stream.Context().Done():
				return
			}
		}
	}()
	for {
		select {
		case frame := <-received:
			select {
			case registered.frames <- frame:
			case <-stream.Context().Done():
				return stream.Context().Err()
			}
		case err := <-receiveError:
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		case <-registered.abort:
			return status.Error(codes.Unavailable, "test stream failure")
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}
}

func (stream *pullTestStream) send(frame *dexpb.PollWorkerTaskResponse) error {
	stream.sendMu.Lock()
	defer stream.sendMu.Unlock()
	return stream.stream.Send(frame)
}

func startPullTestServer(t *testing.T) (string, *pullTestServer) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	grpcServer := grpc.NewServer()
	service := &pullTestServer{registered: make(chan *pullTestStream, 32), firstReconnectQueue: make(chan string, 1)}
	dexpb.RegisterWorkerTaskServiceServer(grpcServer, service)
	dexpb.RegisterFlowServiceServer(grpcServer, &workerSyncFlowService{})
	serveResult := make(chan error, 1)
	go servePullTestServer(grpcServer, listener, serveResult)
	t.Cleanup(func() {
		grpcServer.Stop()
		requireServeStopped(t, <-serveResult)
	})
	return listener.Addr().String(), service
}

func servePullTestServer(grpcServer *grpc.Server, listener net.Listener, serveResult chan<- error) {
	serveResult <- grpcServer.Serve(listener)
}

func awaitPullRegistration(t *testing.T, server *pullTestServer) *pullTestStream {
	t.Helper()
	select {
	case stream := <-server.registered:
		return stream
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Worker registration")
		return nil
	}
}

func awaitPullFrame(t *testing.T, stream *pullTestStream) *dexpb.PollWorkerTaskRequest {
	t.Helper()
	select {
	case frame := <-stream.frames:
		return frame
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for Worker task frame; stream context: %v", stream.stream.Context().Err())
		return nil
	}
}

func awaitPullTaskCompletion(t *testing.T, worker *Worker) {
	t.Helper()
	require.Eventually(t, func() bool {
		worker.pull.mu.Lock()
		defer worker.pull.mu.Unlock()
		return worker.pull.busyTasks == 0
	}, 5*time.Second, time.Millisecond)
}

func TestWorkerPullTaskFramesAndReconnect(t *testing.T) {
	address, server := startPullTestServer(t)
	worker, err := newWorkerForTest(t, []Flow{workerFlow}, WorkerOptions{FlowServiceAddress: address, Pull: &PullWorkerOptions{MaxConcurrentTasks: 1, AppWorkerID: "worker-test-12345678"}})
	require.NoError(t, err)
	require.Equal(t, &WorkerTarget{Pull: true}, worker.WorkerTarget())
	started := make(chan error, 1)
	go func() { started <- worker.Start() }()
	first := awaitPullRegistration(t, server)
	second := awaitPullRegistration(t, server)
	queues := map[string]*pullTestStream{first.registration.Queue: first, second.registration.Queue: second}
	require.Contains(t, queues, "worker-test-12345678")
	flowType := GetFinalFlowType(workerFlow)
	require.Contains(t, queues, flowType)
	for _, stream := range queues {
		require.Equal(t, "worker-test-12345678", stream.registration.AppWorkerId)
		require.Equal(t, []string{flowType}, stream.registration.FlowTypes)
		require.Equal(t, "go", stream.registration.SdkName)
		require.NotEmpty(t, stream.registration.SdkVersion)
	}
	queueStream := queues[flowType]
	require.NoError(t, queueStream.send(&dexpb.PollWorkerTaskResponse{Frame: &dexpb.PollWorkerTaskResponse_Ping{Ping: &dexpb.PollWorkerTaskPing{}}}))
	require.NotNil(t, awaitPullFrame(t, queueStream).GetPong())
	input := workerTestInput{OrderID: "order-1", Mode: "progress"}
	require.NoError(t, queueStream.send(&dexpb.PollWorkerTaskResponse{Frame: &dexpb.PollWorkerTaskResponse_Task{Task: &dexpb.PollWorkerTask{TaskId: "wait-1", Request: &dexpb.PollWorkerTask_WaitFor{WaitFor: &dexpb.InvokeWaitForMethodRequest{Context: workerStepContext(), FlowType: flowType, StepType: GetFinalStepType(workerTestWait), StepInput: mustEncodeWorkerTestValue(t, input)}}}}}))
	var sawHeartbeat, sawStreamWrite, sawResult bool
	for !sawResult {
		var received *dexpb.PollWorkerTaskRequest
		select {
		case received = <-queueStream.frames:
		case <-time.After(5 * time.Second):
			worker.pull.mu.Lock()
			for active := range worker.pull.streams {
				active.mu.Lock()
				t.Logf("stream queue=%s busy=%t draining=%t task=%s", active.queue, active.isBusy, active.isDraining, active.taskID)
				active.mu.Unlock()
			}
			worker.pull.mu.Unlock()
			t.Fatal("timed out waiting for WaitFor frame")
		}
		frame := received.GetTaskFrame()
		require.NotNil(t, frame)
		require.Equal(t, "wait-1", frame.TaskId)
		output := frame.GetWaitFor()
		require.NotNil(t, output)
		if output.GetHeartbeat() != nil {
			sawHeartbeat = true
		}
		if output.GetStreamWrite() != nil {
			sawStreamWrite = true
		}
		if output.GetResult() != nil {
			sawResult = true
		}
	}
	require.True(t, sawHeartbeat)
	require.True(t, sawStreamWrite)
	awaitPullTaskCompletion(t, worker)
	queueStream = awaitPullIdleQueueStream(t, worker, server, flowType)
	executeRequest := workerExecuteRequest(t, workerTestInput{OrderID: "order-1", Mode: "execute-progress"})
	require.NoError(t, queueStream.send(&dexpb.PollWorkerTaskResponse{Frame: &dexpb.PollWorkerTaskResponse_Task{Task: &dexpb.PollWorkerTask{TaskId: "execute-1", Request: &dexpb.PollWorkerTask_Execute{Execute: executeRequest}}}}))
	for {
		frame := awaitPullFrame(t, queueStream).GetTaskFrame()
		require.NotNil(t, frame)
		if frame.GetExecute().GetResult() != nil {
			break
		}
	}
	awaitPullTaskCompletion(t, worker)
	queueStream = awaitPullIdleQueueStream(t, worker, server, flowType)
	require.NoError(t, queueStream.send(&dexpb.PollWorkerTaskResponse{Frame: &dexpb.PollWorkerTaskResponse_Task{Task: &dexpb.PollWorkerTask{TaskId: "rpc-1", Request: &dexpb.PollWorkerTask_Rpc{Rpc: workerRPCRequest(t, workerTestInput{OrderID: "order-1"})}}}}))
	require.NotNil(t, awaitPullFrame(t, queueStream).GetTaskFrame().GetRpc())
	awaitPullTaskCompletion(t, worker)
	queueStream = awaitPullIdleQueueStream(t, worker, server, flowType)
	for _, mode := range []string{"progress-error", "progress-panic"} {
		require.NoError(t, queueStream.send(&dexpb.PollWorkerTaskResponse{Frame: &dexpb.PollWorkerTaskResponse_Task{Task: &dexpb.PollWorkerTask{TaskId: mode, Request: &dexpb.PollWorkerTask_WaitFor{WaitFor: &dexpb.InvokeWaitForMethodRequest{Context: workerStepContext(), FlowType: flowType, StepType: GetFinalStepType(workerTestWait), StepInput: mustEncodeWorkerTestValue(t, workerTestInput{OrderID: "order-1", Mode: mode})}}}}}))
		for {
			frame := awaitPullFrame(t, queueStream).GetTaskFrame()
			if frame.GetWorkerError() != nil {
				require.NotEmpty(t, frame.GetWorkerError().Detail)
				break
			}
		}
		awaitPullTaskCompletion(t, worker)
		queueStream = awaitPullIdleQueueStream(t, worker, server, flowType)
	}
	require.NoError(t, queueStream.send(&dexpb.PollWorkerTaskResponse{Frame: &dexpb.PollWorkerTaskResponse_Task{Task: &dexpb.PollWorkerTask{TaskId: "invalid-request", Request: &dexpb.PollWorkerTask_WaitFor{WaitFor: &dexpb.InvokeWaitForMethodRequest{FlowType: flowType, StepType: GetFinalStepType(workerTestWait), StepInput: mustEncodeWorkerTestValue(t, input)}}}}}))
	invalidFrame := awaitPullFrame(t, queueStream).GetTaskFrame()
	require.NotNil(t, invalidFrame)
	require.Equal(t, int32(codes.InvalidArgument), invalidFrame.GetWorkerError().GetGrpcStatus())
	awaitPullTaskCompletion(t, worker)
	queueStream = awaitPullIdleQueueStream(t, worker, server, flowType)
	require.NoError(t, queueStream.send(&dexpb.PollWorkerTaskResponse{Frame: &dexpb.PollWorkerTaskResponse_Reconnect{Reconnect: &dexpb.PollWorkerTaskReconnect{Reason: "test"}}}))
	var reconnected *pullTestStream
	for reconnected == nil {
		candidate := awaitPullRegistration(t, server)
		if candidate.registration.Queue == flowType && candidate != queueStream {
			reconnected = candidate
		}
	}
	require.Equal(t, flowType, reconnected.registration.Queue)
	require.Equal(t, "worker-test-12345678", reconnected.registration.AppWorkerId)
	stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, worker.Stop(stopCtx))
	require.NoError(t, <-started)
}

func TestWorkerPullLegacyRegistrationReturnsUnknownTaskErrors(t *testing.T) {
	address, server := startPullTestServer(t)
	worker, err := newWorkerForTest(t, []Flow{workerFlow}, WorkerOptions{FlowServiceAddress: address, Pull: &PullWorkerOptions{MaxConcurrentTasks: 1, AppWorkerID: "legacy-registration-worker"}})
	require.NoError(t, err)
	started := make(chan error, 1)
	go func() { started <- worker.Start() }()
	flowType := GetFinalFlowType(workerFlow)
	var queueStream *pullTestStream
	for queueStream == nil {
		candidate := awaitPullRegistration(t, server)
		if candidate.registration.Queue == flowType {
			queueStream = candidate
		}
	}
	for _, task := range []*dexpb.PollWorkerTask{
		{TaskId: "unknown-step", Request: &dexpb.PollWorkerTask_WaitFor{WaitFor: &dexpb.InvokeWaitForMethodRequest{Context: workerStepContext(), FlowType: flowType, StepType: "MissingStep", StepInput: mustEncodeWorkerTestValue(t, workerTestInput{})}}},
		{TaskId: "unknown-rpc", Request: &dexpb.PollWorkerTask_Rpc{Rpc: &dexpb.InvokeWorkerRPCRequest{Context: workerRPCContext(), FlowType: flowType, RpcName: "MissingRPC", Input: mustEncodeWorkerTestValue(t, workerTestInput{})}}},
	} {
		require.NoError(t, queueStream.send(&dexpb.PollWorkerTaskResponse{Frame: &dexpb.PollWorkerTaskResponse_Task{Task: task}}))
		frame := awaitPullFrame(t, queueStream)
		require.Nil(t, frame.GetReject())
		require.Equal(t, task.GetTaskId(), frame.GetTaskFrame().GetTaskId())
		require.Equal(t, int32(codes.NotFound), frame.GetTaskFrame().GetWorkerError().GetGrpcStatus())
		require.Contains(t, frame.GetTaskFrame().GetWorkerError().GetDetail(), "not registered")
		awaitPullTaskCompletion(t, worker)
		queueStream = awaitPullIdleQueueStream(t, worker, server, flowType)
	}
	require.NoError(t, queueStream.send(&dexpb.PollWorkerTaskResponse{Frame: &dexpb.PollWorkerTaskResponse_Ping{Ping: &dexpb.PollWorkerTaskPing{}}}))
	require.NotNil(t, awaitPullFrame(t, queueStream).GetPong())
	stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, worker.Stop(stopCtx))
	require.NoError(t, <-started)
}

func TestWorkerPullGeneratedIdentityAndTargetValidation(t *testing.T) {
	t.Setenv("DEX_WORKER_INSTANCE", "Pod.Name_42")
	appWorkerID, err := generateAppWorkerID()
	require.NoError(t, err)
	require.Regexp(t, regexp.MustCompile(`^pod-name-42-[a-z2-7]{8}$`), appWorkerID)
	require.NoError(t, validateWorkerTarget(WorkerTarget{Pull: true}))
	require.Error(t, validateWorkerTarget(WorkerTarget{Pull: true, Address: "localhost:8803"}))
	require.Error(t, validateWorkerTarget(WorkerTarget{Pull: true, Headless: true}))
	require.Error(t, validateAppWorkerID("bad/id"))
	_, err = newWorkerForTest(t, []Flow{workerFlow}, WorkerOptions{WorkerTarget: WorkerTarget{Pull: true}})
	require.ErrorContains(t, err, "WorkerOptions.Pull")
	_, err = newWorkerForTest(t, []Flow{workerFlow}, WorkerOptions{Pull: &PullWorkerOptions{StopDrainTimeout: -time.Second}})
	require.ErrorContains(t, err, "StopDrainTimeout")
	_, _, startOptions, err := mapStartFlowOptions(StartFlowOptions{ConfigOverride: &FlowConfig{WorkerTarget: &WorkerTarget{Pull: true}}})
	require.NoError(t, err)
	require.True(t, startOptions.FlowConfigOverride.WorkerTarget.IsPullMode)
	registry, err := NewRegistry([]Flow{workerFlow})
	require.NoError(t, err)
	registered, found := registry.lookupFlow(GetFinalFlowType(workerFlow))
	require.True(t, found)
	subFlowOptions, err := mapSubFlowOptions(registered, SubFlowOptions{ConfigOverride: &FlowConfig{WorkerTarget: &WorkerTarget{Pull: true}}})
	require.NoError(t, err)
	require.True(t, subFlowOptions.FlowConfigOverride.WorkerTarget.IsPullMode)
	_, err = mapFlowConfig(&FlowConfig{WorkerTarget: &WorkerTarget{Pull: true, Address: "worker:8803"}})
	require.Error(t, err)
}

func TestWorkerPullCancelCapacityAndStopDrain(t *testing.T) {
	address, server := startPullTestServer(t)
	flow := &workerBlockingFlow{entered: make(chan struct{}), release: make(chan struct{})}
	worker, err := newWorkerForTest(t, []Flow{flow}, WorkerOptions{FlowServiceAddress: address, Pull: &PullWorkerOptions{MaxConcurrentTasks: 1, AppWorkerID: "blocking-worker-12345678"}})
	require.NoError(t, err)
	started := make(chan error, 1)
	go func() { started <- worker.Start() }()
	var queueStream, stickyStream *pullTestStream
	for queueStream == nil || stickyStream == nil {
		candidate := awaitPullRegistration(t, server)
		if candidate.registration.Queue == GetFinalFlowType(flow) {
			queueStream = candidate
		} else if candidate.registration.Queue == "blocking-worker-12345678" {
			stickyStream = candidate
		}
	}
	input := mustEncodeWorkerTestValue(t, struct{}{})
	makeTask := func(taskID string) *dexpb.PollWorkerTaskResponse {
		return &dexpb.PollWorkerTaskResponse{Frame: &dexpb.PollWorkerTaskResponse_Task{Task: &dexpb.PollWorkerTask{
			TaskId: taskID,
			Request: &dexpb.PollWorkerTask_Rpc{Rpc: &dexpb.InvokeWorkerRPCRequest{
				Context: workerRPCContext(), FlowType: GetFinalFlowType(flow), RpcName: "Block", Input: input,
			}},
		}}}
	}
	require.NoError(t, queueStream.send(makeTask("blocking-1")))
	select {
	case <-flow.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("RPC did not start")
	}
	require.NoError(t, stickyStream.send(makeTask("overflow")))
	rejected := awaitPullFrame(t, stickyStream).GetReject()
	require.NotNil(t, rejected)
	require.Equal(t, "overflow", rejected.TaskId)
	require.Equal(t, dexpb.PollWorkerTaskRejectReason_POLL_WORKER_TASK_REJECT_REASON_NO_CAPACITY, rejected.Reason)
	require.NoError(t, stickyStream.send(&dexpb.PollWorkerTaskResponse{Frame: &dexpb.PollWorkerTaskResponse_Ping{Ping: &dexpb.PollWorkerTaskPing{}}}))
	require.NotNil(t, awaitPullFrame(t, stickyStream).GetPong())
	require.NoError(t, queueStream.send(&dexpb.PollWorkerTaskResponse{Frame: &dexpb.PollWorkerTaskResponse_CancelTask{CancelTask: &dexpb.PollWorkerTaskCancelTask{TaskId: "blocking-1"}}}))
	frame := awaitPullFrame(t, queueStream).GetTaskFrame()
	require.NotNil(t, frame.GetWorkerError())
	require.Equal(t, "blocking-1", frame.TaskId)
	awaitPullTaskCompletion(t, worker)
	queueStream = awaitPullIdleQueueStream(t, worker, server, GetFinalFlowType(flow))
	require.NoError(t, queueStream.send(makeTask("blocking-2")))
	require.Eventually(t, func() bool {
		worker.pull.mu.Lock()
		defer worker.pull.mu.Unlock()
		return worker.pull.busyTasks == 1
	}, 5*time.Second, time.Millisecond)
	stopResult := make(chan error, 1)
	stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() { stopResult <- worker.Stop(stopCtx) }()
	require.Eventually(t, func() bool {
		worker.pull.mu.Lock()
		defer worker.pull.mu.Unlock()
		return worker.pull.isStopping && worker.pull.busyTasks == 1
	}, 5*time.Second, time.Millisecond)
	select {
	case err := <-stopResult:
		t.Fatalf("Stop returned before task completed: %v", err)
	default:
	}
	close(flow.release)
	require.NotNil(t, awaitPullFrame(t, queueStream).GetTaskFrame().GetRpc())
	require.NoError(t, <-stopResult)
	require.NoError(t, <-started)
}

func TestWorkerPullForcedStopWaitsForHandler(t *testing.T) {
	address, server := startPullTestServer(t)
	flow := &workerBlockingFlow{entered: make(chan struct{}), release: make(chan struct{})}
	worker, err := newWorkerForTest(t, []Flow{flow}, WorkerOptions{FlowServiceAddress: address, Pull: &PullWorkerOptions{MaxConcurrentTasks: 1, AppWorkerID: "force-stop-worker-12345678"}})
	require.NoError(t, err)
	started := make(chan error, 1)
	go func() { started <- worker.Start() }()
	var queueStream *pullTestStream
	for queueStream == nil {
		candidate := awaitPullRegistration(t, server)
		if candidate.registration.Queue == GetFinalFlowType(flow) {
			queueStream = candidate
		}
	}
	request := &dexpb.InvokeWorkerRPCRequest{Context: workerRPCContext(), FlowType: GetFinalFlowType(flow), RpcName: "Block", Input: mustEncodeWorkerTestValue(t, struct{}{})}
	require.NoError(t, queueStream.send(&dexpb.PollWorkerTaskResponse{Frame: &dexpb.PollWorkerTaskResponse_Task{Task: &dexpb.PollWorkerTask{TaskId: "forced", Request: &dexpb.PollWorkerTask_Rpc{Rpc: request}}}}))
	select {
	case <-flow.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("RPC did not start")
	}
	stopCtx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, worker.Stop(stopCtx), context.Canceled)
	worker.pull.mu.Lock()
	require.Equal(t, 0, worker.pull.busyTasks)
	worker.pull.mu.Unlock()
	require.NoError(t, <-started)
}

func TestWorkerPullReconnectBeforeRegistered(t *testing.T) {
	address, server := startPullTestServer(t)
	server.mu.Lock()
	server.reconnectFirst = true
	server.mu.Unlock()
	worker, err := newWorkerForTest(t, []Flow{workerFlow}, WorkerOptions{FlowServiceAddress: address, Pull: &PullWorkerOptions{MaxConcurrentTasks: 1, AppWorkerID: "first-reconnect-12345678"}})
	require.NoError(t, err)
	started := make(chan error, 1)
	begin := time.Now()
	go func() { started <- worker.Start() }()
	var reconnectQueue string
	select {
	case reconnectQueue = <-server.firstReconnectQueue:
	case <-time.After(5 * time.Second):
		t.Fatal("server did not request reconnect")
	}
	var registered *pullTestStream
	for registered == nil {
		candidate := awaitPullRegistration(t, server)
		if candidate.registration.Queue == reconnectQueue {
			registered = candidate
		}
	}
	require.GreaterOrEqual(t, time.Since(begin), 90*time.Millisecond)
	stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, worker.Stop(stopCtx))
	require.NoError(t, <-started)
}

func TestWorkerPullTaskDeadlineCancelsHandler(t *testing.T) {
	address, server := startPullTestServer(t)
	flow := &workerBlockingFlow{entered: make(chan struct{}), release: make(chan struct{})}
	worker, err := newWorkerForTest(t, []Flow{flow}, WorkerOptions{FlowServiceAddress: address, Pull: &PullWorkerOptions{MaxConcurrentTasks: 1, AppWorkerID: "deadline-worker-12345678"}})
	require.NoError(t, err)
	started := make(chan error, 1)
	go func() { started <- worker.Start() }()
	var queueStream *pullTestStream
	for queueStream == nil {
		candidate := awaitPullRegistration(t, server)
		if candidate.registration.Queue == GetFinalFlowType(flow) {
			queueStream = candidate
		}
	}
	request := &dexpb.InvokeWorkerRPCRequest{Context: workerRPCContext(), FlowType: GetFinalFlowType(flow), RpcName: "Block", Input: mustEncodeWorkerTestValue(t, struct{}{})}
	require.NoError(t, queueStream.send(&dexpb.PollWorkerTaskResponse{Frame: &dexpb.PollWorkerTaskResponse_Task{Task: &dexpb.PollWorkerTask{TaskId: "deadline", Deadline: timestamppb.New(time.Now().Add(-time.Second)), Request: &dexpb.PollWorkerTask_Rpc{Rpc: request}}}}))
	frame := awaitPullFrame(t, queueStream).GetTaskFrame()
	require.NotNil(t, frame.GetWorkerError())
	require.Contains(t, frame.GetWorkerError().Detail, "deadline exceeded")
	awaitPullTaskCompletion(t, worker)
	stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, worker.Stop(stopCtx))
	require.NoError(t, <-started)
}

func TestWorkerPullStreamFailureBacksOff(t *testing.T) {
	address, server := startPullTestServer(t)
	worker, err := newWorkerForTest(t, []Flow{workerFlow}, WorkerOptions{FlowServiceAddress: address, Pull: &PullWorkerOptions{MaxConcurrentTasks: 1, AppWorkerID: "backoff-worker-12345678"}})
	require.NoError(t, err)
	started := make(chan error, 1)
	go func() { started <- worker.Start() }()
	var queueStream *pullTestStream
	flowType := GetFinalFlowType(workerFlow)
	for queueStream == nil {
		candidate := awaitPullRegistration(t, server)
		if candidate.registration.Queue == flowType {
			queueStream = candidate
		}
	}
	failedAt := time.Now()
	close(queueStream.abort)
	var reconnected *pullTestStream
	for reconnected == nil {
		candidate := awaitPullRegistration(t, server)
		if candidate.registration.Queue == flowType && candidate != queueStream {
			reconnected = candidate
		}
	}
	require.GreaterOrEqual(t, time.Since(failedAt), 90*time.Millisecond)
	stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, worker.Stop(stopCtx))
	require.NoError(t, <-started)
}

func TestWorkerPullReplenishesIdleStreamAndDrainsReconnect(t *testing.T) {
	address, server := startPullTestServer(t)
	flow := &workerBlockingFlow{entered: make(chan struct{}), release: make(chan struct{})}
	worker, err := newWorkerForTest(t, []Flow{flow}, WorkerOptions{FlowServiceAddress: address, Pull: &PullWorkerOptions{MaxConcurrentTasks: 2, AppWorkerID: "reconnect-worker-12345678"}})
	require.NoError(t, err)
	started := make(chan error, 1)
	go func() { started <- worker.Start() }()
	flowType := GetFinalFlowType(flow)
	var busyStream *pullTestStream
	for busyStream == nil {
		candidate := awaitPullRegistration(t, server)
		if candidate.registration.Queue == flowType {
			busyStream = candidate
		}
	}
	request := &dexpb.InvokeWorkerRPCRequest{Context: workerRPCContext(), FlowType: flowType, RpcName: "Block", Input: mustEncodeWorkerTestValue(t, struct{}{})}
	require.NoError(t, busyStream.send(&dexpb.PollWorkerTaskResponse{Frame: &dexpb.PollWorkerTaskResponse_Task{Task: &dexpb.PollWorkerTask{TaskId: "drain-1", Request: &dexpb.PollWorkerTask_Rpc{Rpc: request}}}}))
	select {
	case <-flow.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("RPC did not start")
	}
	var replacement *pullTestStream
	for replacement == nil {
		candidate := awaitPullRegistration(t, server)
		if candidate.registration.Queue == flowType && candidate != busyStream {
			replacement = candidate
		}
	}
	require.NoError(t, busyStream.send(&dexpb.PollWorkerTaskResponse{Frame: &dexpb.PollWorkerTaskResponse_Reconnect{Reconnect: &dexpb.PollWorkerTaskReconnect{Reason: "owner changed"}}}))
	require.Eventually(t, func() bool {
		worker.pull.mu.Lock()
		defer worker.pull.mu.Unlock()
		for active := range worker.pull.streams {
			if active.queue != flowType {
				continue
			}
			active.mu.Lock()
			isDraining := active.isBusy && active.isDraining
			active.mu.Unlock()
			if isDraining {
				return true
			}
		}
		return false
	}, 5*time.Second, time.Millisecond)
	select {
	case frame := <-busyStream.frames:
		t.Fatalf("task finished before release: %v", frame)
	default:
	}
	close(flow.release)
	require.NotNil(t, awaitPullFrame(t, busyStream).GetTaskFrame().GetRpc())
	var reconnected *pullTestStream
	for reconnected == nil {
		candidate := awaitPullRegistration(t, server)
		if candidate.registration.Queue == flowType && candidate != replacement && candidate != busyStream {
			reconnected = candidate
		}
	}
	require.Equal(t, "reconnect-worker-12345678", reconnected.registration.AppWorkerId)
	stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, worker.Stop(stopCtx))
	require.NoError(t, <-started)
}

func TestWorkerPullShrinksSurplusIdleStreams(t *testing.T) {
	for _, idleStreams := range []int{1, 2} {
		t.Run(fmt.Sprint(idleStreams), func(t *testing.T) {
			address, server := startPullTestServer(t)
			flow := &workerBlockingFlow{entered: make(chan struct{}), release: make(chan struct{})}
			worker, err := newWorkerForTest(t, []Flow{flow}, WorkerOptions{FlowServiceAddress: address, Pull: &PullWorkerOptions{MaxConcurrentTasks: 3, IdleStreamsPerQueue: idleStreams, AppWorkerID: "shrink-worker"}})
			require.NoError(t, err)
			started := make(chan error, 1)
			go func() { started <- worker.Start() }()
			t.Cleanup(func() {
				require.NoError(t, worker.Stop(context.Background()))
				require.NoError(t, <-started)
			})
			flowType := GetFinalFlowType(flow)
			for taskNumber := 0; taskNumber < 3; taskNumber++ {
				var taskStream *pullTestStream
				for taskStream == nil {
					candidate := awaitPullRegistration(t, server)
					if candidate.registration.Queue == flowType {
						taskStream = candidate
					}
				}
				request := &dexpb.InvokeWorkerRPCRequest{Context: workerRPCContext(), FlowType: flowType, RpcName: "Block", Input: mustEncodeWorkerTestValue(t, struct{}{})}
				require.NoError(t, taskStream.send(&dexpb.PollWorkerTaskResponse{Frame: &dexpb.PollWorkerTaskResponse_Task{Task: &dexpb.PollWorkerTask{TaskId: fmt.Sprint(taskNumber), Request: &dexpb.PollWorkerTask_Rpc{Rpc: request}}}}))
			}
			require.Eventually(t, func() bool {
				worker.pull.mu.Lock()
				defer worker.pull.mu.Unlock()
				return worker.pull.busyTasks == 3 && len(worker.pull.streams) == 3+2*idleStreams && worker.pull.queueRegistrations[flowType].registeredStreams == 3+idleStreams
			}, 5*time.Second, time.Millisecond)
			require.True(t, worker.PullRegistrationState().IsReady)
			worker.pull.mu.Lock()
			for stream := range worker.pull.streams {
				stream.mu.Lock()
				require.False(t, stream.isBusy && stream.isDraining)
				stream.mu.Unlock()
			}
			worker.pull.mu.Unlock()
			close(flow.release)
			require.Eventually(t, func() bool {
				require.True(t, worker.PullRegistrationState().IsReady)
				worker.pull.mu.Lock()
				defer worker.pull.mu.Unlock()
				return worker.pull.busyTasks == 0 && len(worker.pull.streams) == 2*idleStreams && worker.pull.queueRegistrations[flowType].registeredStreams == idleStreams
			}, 5*time.Second, time.Millisecond)
			require.NoError(t, worker.WaitPullRegistered(context.Background()))
		})
	}
}

func awaitPullIdleQueueStream(t *testing.T, worker *Worker, server *pullTestServer, queue string) *pullTestStream {
	t.Helper()
	var idleStream *pullTestStream
	require.Eventually(t, func() bool {
		worker.pull.mu.Lock()
		defer worker.pull.mu.Unlock()
		if len(worker.pull.streams) != len(worker.pull.queues) {
			return false
		}
		server.mu.Lock()
		defer server.mu.Unlock()
		for _, candidate := range server.streams {
			if candidate.registration.Queue == queue && candidate.stream.Context().Err() == nil {
				idleStream = candidate
			}
		}
		return idleStream != nil
	}, 5*time.Second, time.Millisecond)
	return idleStream
}
