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
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/superdurable/dex/sdk-go/gen/dexpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type registrationTestServer struct {
	dexpb.UnimplementedWorkerTaskServiceServer
	streams chan *registrationTestStream
}

type registrationTestStream struct {
	queue    string
	commands chan registrationTestCommand
}

type registrationTestCommand struct {
	response *dexpb.PollWorkerTaskResponse
	err      error
}

func (server *registrationTestServer) PollWorkerTask(stream dexpb.WorkerTaskService_PollWorkerTaskServer) error {
	request, err := stream.Recv()
	if err != nil {
		return err
	}
	registration := &registrationTestStream{queue: request.GetRegister().GetQueue(), commands: make(chan registrationTestCommand, 8)}
	select {
	case server.streams <- registration:
	case <-stream.Context().Done():
		return stream.Context().Err()
	}
	for {
		select {
		case command := <-registration.commands:
			if command.err != nil {
				return command.err
			}
			if err := stream.Send(command.response); err != nil {
				return err
			}
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}
}

func TestWorkerWaitPullRegisteredAllQueuesAndConcurrentWaiters(t *testing.T) {
	worker, server, result := startRegistrationTestWorker(t, PullWorkerOptions{}, &workerSyncFlowService{})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	waiters := make(chan error, 8)
	for range 8 {
		go func() { waiters <- worker.WaitPullRegistered(ctx) }()
	}
	go func() { result <- worker.Start() }()
	first := awaitRegistrationTestStream(t, server, 5*time.Second)
	second := awaitRegistrationTestStream(t, server, 5*time.Second)
	first.register()
	require.Eventually(t, func() bool {
		return worker.PullRegistrationState().Queues[queueStateIndex(worker, first.queue)].HasRegisteredStream
	}, time.Second, time.Millisecond)
	require.False(t, worker.PullRegistrationState().IsReady)
	select {
	case err := <-waiters:
		t.Fatalf("readiness completed before every queue registered: %v", err)
	default:
	}
	second.register()
	for range 8 {
		require.NoError(t, <-waiters)
	}
	state := worker.PullRegistrationState()
	require.True(t, state.IsReady)
	require.Len(t, state.Queues, 2)
	state.Queues[0].Queue = "changed snapshot"
	require.NotEqual(t, state.Queues[0].Queue, worker.PullRegistrationState().Queues[0].Queue)
	require.NoError(t, worker.Stop(ctx))
	require.NoError(t, worker.WaitPullRegistered(ctx))
}

func TestWorkerWaitPullRegisteredPermanentRejections(t *testing.T) {
	for _, code := range []codes.Code{codes.ResourceExhausted, codes.InvalidArgument, codes.Unauthenticated, codes.PermissionDenied, codes.Unimplemented} {
		t.Run(code.String(), func(t *testing.T) {
			worker, server, result := startRegistrationTestWorker(t, PullWorkerOptions{}, &workerSyncFlowService{})
			go func() { result <- worker.Start() }()
			stream := awaitRegistrationTestStream(t, server, 5*time.Second)
			rejection := status.Error(code, "registration rejected")
			stream.reject(rejection)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			err := worker.WaitPullRegistered(ctx)
			var registrationError *PullRegistrationError
			require.ErrorAs(t, err, &registrationError)
			require.Equal(t, stream.queue, registrationError.Queue)
			require.ErrorIs(t, err, rejection)
			require.Equal(t, code, status.Code(err))
			require.Contains(t, err.Error(), stream.queue)
		})
	}
}

func TestWorkerWaitPullRegisteredStartupFailures(t *testing.T) {
	startupFailure := status.Error(codes.PermissionDenied, "startup rejected")
	for _, scenario := range []struct {
		name    string
		service *workerSyncFlowService
	}{
		{name: "server info", service: &workerSyncFlowService{serverInfoFailure: startupFailure}},
		{name: "protocol", service: &workerSyncFlowService{serverInfo: &dexpb.ServerInfo{MinimumSupportedProtocolVersion: 999, CurrentProtocolVersion: 999}}},
		{name: "attribute indexes", service: &workerSyncFlowService{attributeFailure: startupFailure}},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			worker, _, result := startRegistrationTestWorker(t, PullWorkerOptions{}, scenario.service)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			waitResult := make(chan error, 1)
			go func() { waitResult <- worker.WaitPullRegistered(ctx) }()
			go func() { result <- worker.Start() }()
			err := <-waitResult
			require.Error(t, err)
			require.NotErrorIs(t, err, context.DeadlineExceeded)
			require.ErrorIs(t, worker.WaitPullRegistered(ctx), err)
			if scenario.name != "protocol" {
				require.ErrorIs(t, err, startupFailure)
			}
		})
	}
}

func TestWorkerWaitPullRegisteredCancellationAndStop(t *testing.T) {
	worker, _, _ := startRegistrationTestWorker(t, PullWorkerOptions{}, &workerSyncFlowService{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, worker.WaitPullRegistered(ctx), context.Canceled)
	timeoutCtx, cancelTimeout := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancelTimeout()
	require.ErrorIs(t, worker.WaitPullRegistered(timeoutCtx), context.DeadlineExceeded)
	require.Error(t, worker.WaitPullRegistered(nil))
	require.NoError(t, worker.Stop(context.Background()))
	require.ErrorContains(t, worker.WaitPullRegistered(context.Background()), "stopped before registration")
	pushWorker, err := newWorkerForTest(t, []Flow{workerFlow}, WorkerOptions{})
	require.NoError(t, err)
	require.ErrorContains(t, pushWorker.WaitPullRegistered(context.Background()), "requires a pull Worker")
	require.Equal(t, PullRegistrationState{}, pushWorker.PullRegistrationState())
	require.NoError(t, pushWorker.Stop(context.Background()))
}

func TestWorkerPullRegistrationStateDoesNotFlap(t *testing.T) {
	worker, server, result := startRegistrationTestWorker(t, PullWorkerOptions{}, &workerSyncFlowService{})
	go func() { result <- worker.Start() }()
	first := awaitRegistrationTestStream(t, server, 5*time.Second)
	second := awaitRegistrationTestStream(t, server, 5*time.Second)
	first.register()
	second.register()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, worker.WaitPullRegistered(ctx))
	first.commands <- registrationTestCommand{response: &dexpb.PollWorkerTaskResponse{Frame: &dexpb.PollWorkerTaskResponse_Reconnect{Reconnect: &dexpb.PollWorkerTaskReconnect{}}}}
	replacement := awaitRegistrationTestStream(t, server, 5*time.Second)
	require.True(t, worker.PullRegistrationState().IsReady)
	replacement.register()
	replacement.reject(status.Error(codes.Unavailable, "temporary disconnect"))
	replacement = awaitRegistrationTestStream(t, server, 5*time.Second)
	require.True(t, worker.PullRegistrationState().IsReady)
	replacement.reject(status.Error(codes.PermissionDenied, "credential revoked"))
	require.Eventually(t, func() bool { return !worker.PullRegistrationState().IsReady }, 5*time.Second, time.Millisecond)
	require.NoError(t, worker.WaitPullRegistered(ctx))
	state := worker.PullRegistrationState().Queues[queueStateIndex(worker, replacement.queue)]
	require.False(t, state.HasRegisteredStream)
	require.Equal(t, codes.PermissionDenied, status.Code(state.LastPermanentError))
	select {
	case err := <-result:
		t.Fatalf("default retry behavior stopped Start: %v", err)
	default:
	}
	replacement = awaitRegistrationTestStream(t, server, 95*time.Second)
	replacement.register()
	require.Eventually(t, func() bool { return worker.PullRegistrationState().IsReady }, 5*time.Second, time.Millisecond)
	require.Nil(t, worker.PullRegistrationState().Queues[queueStateIndex(worker, replacement.queue)].LastPermanentError)
}

func TestWorkerPullStopOnPermanentStreamRejection(t *testing.T) {
	for _, isInitiallyReady := range []bool{false, true} {
		t.Run(map[bool]string{false: "before readiness", true: "after readiness"}[isInitiallyReady], func(t *testing.T) {
			worker, server, result := startRegistrationTestWorker(t, PullWorkerOptions{StopOnPermanentStreamRejection: true}, &workerSyncFlowService{})
			go func() { result <- worker.Start() }()
			first := awaitRegistrationTestStream(t, server, 5*time.Second)
			second := awaitRegistrationTestStream(t, server, 5*time.Second)
			if isInitiallyReady {
				first.register()
				second.register()
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				require.NoError(t, worker.WaitPullRegistered(ctx))
			}
			first.reject(status.Error(codes.ResourceExhausted, "stream cap"))
			select {
			case err := <-result:
				var rejection *PullRegistrationError
				require.ErrorAs(t, err, &rejection)
				require.Equal(t, first.queue, rejection.Queue)
				require.Equal(t, codes.ResourceExhausted, status.Code(rejection.Err))
			case <-time.After(5 * time.Second):
				t.Fatal("Start did not return after permanent rejection")
			}
			require.NoError(t, worker.Stop(context.Background()))
			require.NoError(t, worker.Stop(context.Background()))
			select {
			case <-worker.pull.done:
			default:
				t.Fatal("pull goroutines remain after Start returned")
			}
		})
	}
}

func TestWorkerPullRegistrationRejectionWithRemainingStream(t *testing.T) {
	for _, shouldStop := range []bool{false, true} {
		t.Run(map[bool]string{false: "default retries", true: "stop enabled"}[shouldStop], func(t *testing.T) {
			worker, server, result := startRegistrationTestWorker(t, PullWorkerOptions{
				IdleStreamsPerQueue: 2, StopOnPermanentStreamRejection: shouldStop,
			}, &workerSyncFlowService{})
			go func() { result <- worker.Start() }()
			streamsByQueue := make(map[string][]*registrationTestStream)
			for range 4 {
				stream := awaitRegistrationTestStream(t, server, 5*time.Second)
				streamsByQueue[stream.queue] = append(streamsByQueue[stream.queue], stream)
			}
			stickyStreams := streamsByQueue[worker.pull.appWorkerID]
			stickyStreams[0].register()
			require.Eventually(t, func() bool {
				return worker.PullRegistrationState().Queues[0].HasRegisteredStream
			}, 5*time.Second, time.Millisecond)
			stickyStreams[1].reject(status.Error(codes.ResourceExhausted, "surplus stream rejected"))
			require.Eventually(t, func() bool {
				return worker.PullRegistrationState().Queues[0].LastPermanentError != nil
			}, 5*time.Second, time.Millisecond)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			waitResult := make(chan error, 1)
			go func() { waitResult <- worker.WaitPullRegistered(ctx) }()
			worker.pull.mu.Lock()
			initialRegistrationError := worker.pull.initialRegistrationError
			worker.pull.mu.Unlock()
			require.Nil(t, initialRegistrationError)
			for _, stream := range streamsByQueue[GetFinalFlowType(workerFlow)] {
				stream.register()
			}
			require.NoError(t, <-waitResult)
			require.True(t, worker.PullRegistrationState().IsReady)
			require.True(t, worker.PullRegistrationState().Queues[0].HasRegisteredStream)
			require.Equal(t, codes.ResourceExhausted, status.Code(worker.PullRegistrationState().Queues[0].LastPermanentError))
			select {
			case err := <-result:
				t.Fatalf("surplus stream rejection stopped a registered queue: %v", err)
			default:
			}
			stickyStreams[0].reject(status.Error(codes.Unavailable, "last stream disconnected"))
			require.Eventually(t, func() bool { return !worker.PullRegistrationState().IsReady }, 5*time.Second, time.Millisecond)
			replacement := awaitRegistrationTestStream(t, server, 5*time.Second)
			require.Equal(t, worker.pull.appWorkerID, replacement.queue)
			replacement.reject(status.Error(codes.ResourceExhausted, "queue has no registered stream"))
			if shouldStop {
				select {
				case err := <-result:
					var rejection *PullRegistrationError
					require.ErrorAs(t, err, &rejection)
					require.Equal(t, replacement.queue, rejection.Queue)
					require.Equal(t, codes.ResourceExhausted, status.Code(rejection.Err))
				case <-ctx.Done():
					t.Fatal("Start did not return when the queue was permanently rejected without a remaining stream")
				}
				require.False(t, worker.PullRegistrationState().IsReady)
				require.NoError(t, worker.Stop(ctx))
			} else {
				require.Eventually(t, func() bool {
					return status.Convert(worker.PullRegistrationState().Queues[0].LastPermanentError).Message() == "queue has no registered stream"
				}, 5*time.Second, time.Millisecond)
				select {
				case err := <-result:
					t.Fatalf("default retry behavior stopped Start: %v", err)
				default:
				}
			}
		})
	}
}

func TestWorkerPullPermanentRejectionDrainsActiveHandler(t *testing.T) {
	flow := &workerBlockingFlow{entered: make(chan struct{}), release: make(chan struct{})}
	worker, server, result := startRegistrationTestWorker(t, PullWorkerOptions{
		StopOnPermanentStreamRejection: true, StopDrainTimeout: 20 * time.Millisecond,
	}, &workerSyncFlowService{}, flow)
	go func() { result <- worker.Start() }()
	var taskStream *registrationTestStream
	var controlStream *registrationTestStream
	for range 2 {
		stream := awaitRegistrationTestStream(t, server, 5*time.Second)
		stream.register()
		if stream.queue == GetFinalFlowType(flow) {
			taskStream = stream
		} else {
			controlStream = stream
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, worker.WaitPullRegistered(ctx))
	request := &dexpb.InvokeWorkerRPCRequest{Context: workerRPCContext(), FlowType: GetFinalFlowType(flow), RpcName: "Block", Input: mustEncodeWorkerTestValue(t, struct{}{})}
	taskStream.commands <- registrationTestCommand{response: &dexpb.PollWorkerTaskResponse{Frame: &dexpb.PollWorkerTaskResponse_Task{Task: &dexpb.PollWorkerTask{TaskId: "drain-active-handler", Request: &dexpb.PollWorkerTask_Rpc{Rpc: request}}}}}
	select {
	case <-flow.entered:
	case <-ctx.Done():
		t.Fatal("handler did not start")
	}
	controlStream.reject(status.Error(codes.ResourceExhausted, "stream cap"))
	select {
	case err := <-result:
		var rejection *PullRegistrationError
		require.ErrorAs(t, err, &rejection)
	case <-ctx.Done():
		t.Fatal("Start did not drain and cancel the active handler")
	}
	require.NoError(t, worker.Stop(ctx))
	worker.pull.mu.Lock()
	require.Zero(t, worker.pull.busyTasks)
	require.Empty(t, worker.pull.streams)
	worker.pull.mu.Unlock()
}

func startRegistrationTestWorker(t *testing.T, options PullWorkerOptions, flowService *workerSyncFlowService, flows ...Flow) (*Worker, *registrationTestServer, chan error) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	grpcServer := grpc.NewServer()
	server := &registrationTestServer{streams: make(chan *registrationTestStream, 32)}
	dexpb.RegisterWorkerTaskServiceServer(grpcServer, server)
	dexpb.RegisterFlowServiceServer(grpcServer, flowService)
	serveResult := make(chan error, 1)
	go servePullTestServer(grpcServer, listener, serveResult)
	if len(flows) == 0 {
		flows = []Flow{workerFlow}
	}
	worker, err := newWorkerForTest(t, flows, WorkerOptions{FlowServiceAddress: listener.Addr().String(), Pull: &options})
	require.NoError(t, err)
	result := make(chan error, 1)
	t.Cleanup(func() {
		require.NoError(t, worker.Stop(context.Background()))
		grpcServer.Stop()
		requireServeStopped(t, <-serveResult)
	})
	return worker, server, result
}

func awaitRegistrationTestStream(t *testing.T, server *registrationTestServer, timeout time.Duration) *registrationTestStream {
	t.Helper()
	select {
	case stream := <-server.streams:
		return stream
	case <-time.After(timeout):
		t.Fatal("timed out awaiting queue stream")
		return nil
	}
}

func queueStateIndex(worker *Worker, queue string) int {
	for index, state := range worker.PullRegistrationState().Queues {
		if state.Queue == queue {
			return index
		}
	}
	panic("queue missing")
}

func (stream *registrationTestStream) register() {
	stream.commands <- registrationTestCommand{response: &dexpb.PollWorkerTaskResponse{Frame: &dexpb.PollWorkerTaskResponse_Registered{Registered: &dexpb.PollWorkerTaskRegistered{}}}}
}

func (stream *registrationTestStream) reject(err error) {
	stream.commands <- registrationTestCommand{err: err}
}
