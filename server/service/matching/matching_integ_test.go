// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package matching

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/dgryski/go-farm"
	"github.com/stretchr/testify/require"
	"github.com/superdurable/dex/config"
	"github.com/superdurable/dex/gen/dexpb"
	"github.com/superdurable/dex/service/membership"
	"github.com/temporalio/ringpop-go/hashring"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type testMembership struct {
	mu      sync.Mutex
	owners  map[string]membership.HostInfo
	changes chan membership.Change
}

type testHashMember struct{ address string }

func (m testHashMember) GetAddress() string          { return m.address }
func (m testHashMember) Identity() string            { return m.address }
func (m testHashMember) Label(string) (string, bool) { return "", false }

func (m *testMembership) Lookup(_ membership.Role, queue string) (membership.HostInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if owner, exists := m.owners[queue]; exists {
		return owner, nil
	}
	return membership.HostInfo{IsSelf: true}, nil
}
func (m *testMembership) Subscribe() (<-chan membership.Change, func()) { return m.changes, func() {} }

type testServer struct {
	dexpb.UnimplementedWorkerTaskServiceServer
	engine     *Engine
	server     *grpc.Server
	listener   net.Listener
	connection *grpc.ClientConn
}

func (s *testServer) PollWorkerTask(stream dexpb.WorkerTaskService_PollWorkerTaskServer) error {
	return s.engine.ProxyPollWorkerTask(stream)
}

func startTestServer(t *testing.T, settings config.MatchingConfig, members *testMembership) *testServer {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	clients := func(ctx context.Context, address string) (dexpb.MatchingServiceClient, error) {
		conn, dialErr := grpc.DialContext(ctx, address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if dialErr != nil {
			return nil, dialErr
		}
		t.Cleanup(func() { require.NoError(t, conn.Close()) })
		return dexpb.NewMatchingServiceClient(conn), nil
	}
	engine, err := NewEngine(&settings, zap.NewNop(), members, clients)
	require.NoError(t, err)
	grpcServer := grpc.NewServer()
	fixture := &testServer{engine: engine, server: grpcServer, listener: listener}
	dexpb.RegisterMatchingServiceServer(grpcServer, engine)
	dexpb.RegisterWorkerTaskServiceServer(grpcServer, fixture)
	go func() {
		if serveErr := grpcServer.Serve(listener); serveErr != nil {
			t.Log(serveErr)
		}
	}()
	connection, err := grpc.DialContext(context.Background(), listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	fixture.connection = connection
	t.Cleanup(func() { require.NoError(t, connection.Close()); engine.Close(); grpcServer.Stop() })
	return fixture
}

func newTestMembership() *testMembership {
	return &testMembership{owners: make(map[string]membership.HostInfo), changes: make(chan membership.Change, 8)}
}

func registerWorker(t *testing.T, server *testServer, queue, workerID string) dexpb.WorkerTaskService_PollWorkerTaskClient {
	t.Helper()
	stream, err := dexpb.NewWorkerTaskServiceClient(server.connection).PollWorkerTask(testStreamContext(t))
	require.NoError(t, err)
	require.NoError(t, stream.Send(&dexpb.PollWorkerTaskRequest{Frame: &dexpb.PollWorkerTaskRequest_Register{Register: &dexpb.PollWorkerTaskRegister{Queue: queue, AppWorkerId: workerID, FlowTypes: []string{"flow"}}}}))
	registered, err := stream.Recv()
	require.NoError(t, err)
	require.NotNil(t, registered.GetRegistered())
	return stream
}

func testStreamContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func waitWorkerIdle(t *testing.T, engine *Engine, queueName, workerID string) {
	t.Helper()
	require.Eventually(t, func() bool {
		value, exists := engine.queues.Load(queueName)
		if !exists {
			return false
		}
		queue := value.(*queueState)
		queue.mu.Lock()
		defer queue.mu.Unlock()
		for _, worker := range queue.idle {
			if worker.registration.AppWorkerId == workerID {
				return true
			}
		}
		return false
	}, time.Second, time.Millisecond)
}

func dispatchRequest(queue, fallback string, sticky, total time.Duration) *dexpb.DispatchTaskRequest {
	return &dexpb.DispatchTaskRequest{Queue: queue, FallbackQueue: fallback, FlowId: "flow-1", TaskId: "task-1", StickyWait: durationpb.New(sticky), DispatchWait: durationpb.New(total), Deadline: timestamppb.New(time.Now().Add(5 * time.Second)), Request: &dexpb.DispatchTaskRequest_WaitFor{WaitFor: &dexpb.InvokeWaitForMethodRequest{}}, SupportsAssignmentState: true}
}

func receiveDispatch(t *testing.T, stream dexpb.MatchingService_DispatchTaskClient) []*dexpb.DispatchTaskResponse {
	t.Helper()
	var frames []*dexpb.DispatchTaskResponse
	for {
		frame, err := stream.Recv()
		if err == io.EOF {
			return frames
		}
		require.NoError(t, err)
		if frame.GetAssigned() != nil || frame.GetUnassigned() != nil {
			continue
		}
		frames = append(frames, frame)
	}
}

func requireDispatchError(t *testing.T, stream dexpb.MatchingService_DispatchTaskClient) {
	t.Helper()
	for {
		frame, err := stream.Recv()
		if err != nil {
			return
		}
		require.True(t, frame.GetAssigned() != nil || frame.GetUnassigned() != nil)
	}
}

func TestMatchingRelayAndReuseIntegration(t *testing.T) {
	server := startTestServer(t, config.MatchingConfig{}, newTestMembership())
	worker := registerWorker(t, server, "flow", "worker-1")
	for _, taskID := range []string{"first", "second"} {
		request := dispatchRequest("flow", "", 0, time.Second)
		request.TaskId = taskID
		dispatch, err := dexpb.NewMatchingServiceClient(server.connection).DispatchTask(testStreamContext(t), request)
		require.NoError(t, err)
		assigned, err := worker.Recv()
		require.NoError(t, err)
		require.Equal(t, taskID, assigned.GetTask().TaskId)
		require.NoError(t, worker.Send(&dexpb.PollWorkerTaskRequest{Frame: &dexpb.PollWorkerTaskRequest_TaskFrame{TaskFrame: &dexpb.PollWorkerTaskFrame{TaskId: taskID, Output: &dexpb.PollWorkerTaskFrame_WaitFor{WaitFor: &dexpb.InvokeWaitForMethodOutput{Output: &dexpb.InvokeWaitForMethodOutput_Heartbeat{}}}}}}))
		require.NoError(t, worker.Send(&dexpb.PollWorkerTaskRequest{Frame: &dexpb.PollWorkerTaskRequest_TaskFrame{TaskFrame: &dexpb.PollWorkerTaskFrame{TaskId: taskID, Output: &dexpb.PollWorkerTaskFrame_WaitFor{WaitFor: &dexpb.InvokeWaitForMethodOutput{Output: &dexpb.InvokeWaitForMethodOutput_Result{Result: &dexpb.InvokeWaitForMethodResponse{}}}}}}}))
		frames := receiveDispatch(t, dispatch)
		require.Len(t, frames, 2)
		require.Empty(t, frames[0].ServedByAppWorkerId)
		require.Equal(t, "worker-1", frames[1].ServedByAppWorkerId)
	}
}

func TestMatchingLegacyDispatchOmitsAssignmentStateIntegration(t *testing.T) {
	server := startTestServer(t, config.MatchingConfig{}, newTestMembership())
	worker := registerWorker(t, server, "flow", "legacy-worker")
	request := dispatchRequest("flow", "", 0, time.Second)
	request.SupportsAssignmentState = false
	dispatch, err := dexpb.NewMatchingServiceClient(server.connection).DispatchTask(testStreamContext(t), request)
	require.NoError(t, err)
	assignment, err := worker.Recv()
	require.NoError(t, err)
	require.NoError(t, worker.Send(&dexpb.PollWorkerTaskRequest{Frame: &dexpb.PollWorkerTaskRequest_TaskFrame{
		TaskFrame: &dexpb.PollWorkerTaskFrame{TaskId: assignment.GetTask().GetTaskId(), Output: &dexpb.PollWorkerTaskFrame_Rpc{Rpc: &dexpb.InvokeWorkerRPCResponse{}}},
	}}))
	first, err := dispatch.Recv()
	require.NoError(t, err)
	require.NotNil(t, first.GetRpc())
	_, err = dispatch.Recv()
	require.ErrorIs(t, err, io.EOF)
}

func TestMatchingLegacyDispatchCapacityRejectOmitsAssignmentStateIntegration(t *testing.T) {
	server := startTestServer(t, config.MatchingConfig{}, newTestMembership())
	worker := registerWorker(t, server, "flow", "legacy-rejecting-worker")
	request := dispatchRequest("flow", "", 0, 100*time.Millisecond)
	request.SupportsAssignmentState = false
	dispatch, err := dexpb.NewMatchingServiceClient(server.connection).DispatchTask(testStreamContext(t), request)
	require.NoError(t, err)
	assignment, err := worker.Recv()
	require.NoError(t, err)
	require.NoError(t, worker.Send(&dexpb.PollWorkerTaskRequest{Frame: &dexpb.PollWorkerTaskRequest_Reject{
		Reject: &dexpb.PollWorkerTaskReject{TaskId: assignment.GetTask().GetTaskId(), Reason: dexpb.PollWorkerTaskRejectReason_POLL_WORKER_TASK_REJECT_REASON_NO_CAPACITY},
	}}))
	first, err := dispatch.Recv()
	require.NoError(t, err)
	require.Equal(t, dexpb.DispatchTaskErrorReason_DISPATCH_TASK_ERROR_REASON_NO_WORKER_AVAILABLE, first.GetDispatchError().GetReason())
}

func TestMatchingLegacyDispatchDisconnectRedispatchOmitsAssignmentStateIntegration(t *testing.T) {
	server := startTestServer(t, config.MatchingConfig{}, newTestMembership())
	firstWorker := registerWorker(t, server, "flow", "legacy-disconnecting-worker")
	request := dispatchRequest("flow", "", 0, time.Second)
	request.SupportsAssignmentState = false
	dispatch, err := dexpb.NewMatchingServiceClient(server.connection).DispatchTask(testStreamContext(t), request)
	require.NoError(t, err)
	firstAssignment, err := firstWorker.Recv()
	require.NoError(t, err)
	require.NoError(t, firstWorker.CloseSend())
	replacementWorker := registerWorker(t, server, "flow", "legacy-replacement-worker")
	replacementAssignment, err := replacementWorker.Recv()
	require.NoError(t, err)
	require.Equal(t, firstAssignment.GetTask().GetTaskId(), replacementAssignment.GetTask().GetTaskId())
	require.NoError(t, replacementWorker.Send(&dexpb.PollWorkerTaskRequest{Frame: &dexpb.PollWorkerTaskRequest_TaskFrame{
		TaskFrame: &dexpb.PollWorkerTaskFrame{TaskId: replacementAssignment.GetTask().GetTaskId(), Output: &dexpb.PollWorkerTaskFrame_Rpc{Rpc: &dexpb.InvokeWorkerRPCResponse{}}},
	}}))
	first, err := dispatch.Recv()
	require.NoError(t, err)
	require.NotNil(t, first.GetRpc())
}

func TestMatchingFallbackAndUnavailableIntegration(t *testing.T) {
	server := startTestServer(t, config.MatchingConfig{}, newTestMembership())
	worker := registerWorker(t, server, "flow", "worker-2")
	request := dispatchRequest("stale-worker", "flow", 40*time.Millisecond, 250*time.Millisecond)
	started := time.Now()
	dispatch, err := dexpb.NewMatchingServiceClient(server.connection).DispatchTask(testStreamContext(t), request)
	require.NoError(t, err)
	assigned, err := worker.Recv()
	require.NoError(t, err)
	require.GreaterOrEqual(t, time.Since(started), 35*time.Millisecond)
	require.NoError(t, worker.Send(&dexpb.PollWorkerTaskRequest{Frame: &dexpb.PollWorkerTaskRequest_TaskFrame{TaskFrame: &dexpb.PollWorkerTaskFrame{TaskId: assigned.GetTask().TaskId, Output: &dexpb.PollWorkerTaskFrame_WorkerError{WorkerError: &dexpb.WorkerErrorResponse{}}}}}))
	frames := receiveDispatch(t, dispatch)
	require.Len(t, frames, 1)
	require.Equal(t, "worker-2", frames[0].ServedByAppWorkerId)
	unavailable, err := dexpb.NewMatchingServiceClient(server.connection).DispatchTask(testStreamContext(t), dispatchRequest("empty", "", 0, 60*time.Millisecond))
	require.NoError(t, err)
	frames = receiveDispatch(t, unavailable)
	require.Len(t, frames, 1)
	require.Equal(t, dexpb.DispatchTaskErrorReason_DISPATCH_TASK_ERROR_REASON_NO_WORKER_AVAILABLE, frames[0].GetDispatchError().Reason)
}

func TestMatchingAssignmentStateAfterCapacityRejectIntegration(t *testing.T) {
	server := startTestServer(t, config.MatchingConfig{}, newTestMembership())
	worker := registerWorker(t, server, "flow", "worker-rejecting")
	dispatch, err := dexpb.NewMatchingServiceClient(server.connection).DispatchTask(testStreamContext(t), dispatchRequest("flow", "", 0, 100*time.Millisecond))
	require.NoError(t, err)
	assignment, err := worker.Recv()
	require.NoError(t, err)
	assigned, err := dispatch.Recv()
	require.NoError(t, err)
	require.NotNil(t, assigned.GetAssigned())
	require.NoError(t, worker.Send(&dexpb.PollWorkerTaskRequest{Frame: &dexpb.PollWorkerTaskRequest_Reject{
		Reject: &dexpb.PollWorkerTaskReject{TaskId: assignment.GetTask().GetTaskId(), Reason: dexpb.PollWorkerTaskRejectReason_POLL_WORKER_TASK_REJECT_REASON_NO_CAPACITY},
	}}))
	unassigned, err := dispatch.Recv()
	require.NoError(t, err)
	require.NotNil(t, unassigned.GetUnassigned())
	final, err := dispatch.Recv()
	require.NoError(t, err)
	require.Equal(t, dexpb.DispatchTaskErrorReason_DISPATCH_TASK_ERROR_REASON_NO_WORKER_AVAILABLE, final.GetDispatchError().GetReason())
}

func TestMatchingRejectDisconnectAndCancelIntegration(t *testing.T) {
	server := startTestServer(t, config.MatchingConfig{}, newTestMembership())
	first := registerWorker(t, server, "flow", "worker-a")
	second := registerWorker(t, server, "flow", "worker-b")
	waitWorkerIdle(t, server.engine, "flow", "worker-a")
	waitWorkerIdle(t, server.engine, "flow", "worker-b")
	dispatch, err := dexpb.NewMatchingServiceClient(server.connection).DispatchTask(testStreamContext(t), dispatchRequest("flow", "", 0, time.Second))
	require.NoError(t, err)
	assigned, err := first.Recv()
	require.NoError(t, err)
	require.NoError(t, first.Send(&dexpb.PollWorkerTaskRequest{Frame: &dexpb.PollWorkerTaskRequest_Reject{Reject: &dexpb.PollWorkerTaskReject{TaskId: assigned.GetTask().TaskId, Reason: dexpb.PollWorkerTaskRejectReason_POLL_WORKER_TASK_REJECT_REASON_NO_CAPACITY}}}))
	assigned, err = second.Recv()
	require.NoError(t, err)
	require.NoError(t, second.CloseSend())
	assigned, err = first.Recv()
	require.NoError(t, err)
	require.Equal(t, "task-1", assigned.GetTask().TaskId)
	require.NoError(t, first.Send(&dexpb.PollWorkerTaskRequest{Frame: &dexpb.PollWorkerTaskRequest_TaskFrame{TaskFrame: &dexpb.PollWorkerTaskFrame{TaskId: assigned.GetTask().TaskId, Output: &dexpb.PollWorkerTaskFrame_Rpc{Rpc: &dexpb.InvokeWorkerRPCResponse{}}}}}))
	frames := receiveDispatch(t, dispatch)
	require.Len(t, frames, 1)
	require.Equal(t, "worker-a", frames[0].GetServedByAppWorkerId())
	ctx, cancel := context.WithCancel(testStreamContext(t))
	canceled, err := dexpb.NewMatchingServiceClient(server.connection).DispatchTask(ctx, dispatchRequest("flow", "", 0, time.Second))
	require.NoError(t, err)
	assigned, err = first.Recv()
	require.NoError(t, err)
	cancel()
	control, err := first.Recv()
	require.NoError(t, err)
	require.Equal(t, assigned.GetTask().TaskId, control.GetCancelTask().TaskId)
	requireDispatchError(t, canceled)
}

func TestMatchingDisconnectAfterOutputIntegration(t *testing.T) {
	server := startTestServer(t, config.MatchingConfig{}, newTestMembership())
	worker := registerWorker(t, server, "flow", "worker-progress")
	dispatch, err := dexpb.NewMatchingServiceClient(server.connection).DispatchTask(testStreamContext(t), dispatchRequest("flow", "", 0, time.Second))
	require.NoError(t, err)
	assigned, err := worker.Recv()
	require.NoError(t, err)
	require.NoError(t, worker.Send(&dexpb.PollWorkerTaskRequest{Frame: &dexpb.PollWorkerTaskRequest_TaskFrame{TaskFrame: &dexpb.PollWorkerTaskFrame{TaskId: assigned.GetTask().TaskId, Output: &dexpb.PollWorkerTaskFrame_WaitFor{WaitFor: &dexpb.InvokeWaitForMethodOutput{Output: &dexpb.InvokeWaitForMethodOutput_Heartbeat{}}}}}}))
	require.NoError(t, worker.CloseSend())
	frames := receiveDispatch(t, dispatch)
	require.Len(t, frames, 2)
	require.NotNil(t, frames[0].GetWaitFor())
	require.Equal(t, dexpb.DispatchTaskErrorReason_DISPATCH_TASK_ERROR_REASON_WORKER_DISCONNECTED, frames[1].GetDispatchError().GetReason())
}

func TestMatchingRejectAfterOutputDoesNotResumeMatchingIntegration(t *testing.T) {
	server := startTestServer(t, config.MatchingConfig{}, newTestMembership())
	worker := registerWorker(t, server, "flow", "worker-rejecting-after-output")
	dispatch, err := dexpb.NewMatchingServiceClient(server.connection).DispatchTask(testStreamContext(t), dispatchRequest("flow", "", 0, time.Second))
	require.NoError(t, err)
	assignment, err := worker.Recv()
	require.NoError(t, err)
	taskID := assignment.GetTask().GetTaskId()
	heartbeat := &dexpb.PollWorkerTaskFrame{TaskId: taskID, Output: &dexpb.PollWorkerTaskFrame_WaitFor{
		WaitFor: &dexpb.InvokeWaitForMethodOutput{Output: &dexpb.InvokeWaitForMethodOutput_Heartbeat{Heartbeat: &dexpb.StepMethodHeartbeat{}}},
	}}
	require.NoError(t, worker.Send(&dexpb.PollWorkerTaskRequest{Frame: &dexpb.PollWorkerTaskRequest_TaskFrame{TaskFrame: heartbeat}}))
	require.NoError(t, worker.Send(&dexpb.PollWorkerTaskRequest{Frame: &dexpb.PollWorkerTaskRequest_Reject{
		Reject: &dexpb.PollWorkerTaskReject{TaskId: taskID, Reason: dexpb.PollWorkerTaskRejectReason_POLL_WORKER_TASK_REJECT_REASON_NO_CAPACITY},
	}}))
	assignedState, err := dispatch.Recv()
	require.NoError(t, err)
	require.NotNil(t, assignedState.GetAssigned())
	progress, err := dispatch.Recv()
	require.NoError(t, err)
	require.NotNil(t, progress.GetWaitFor().GetHeartbeat())
	final, err := dispatch.Recv()
	require.NoError(t, err)
	require.Equal(t, dexpb.DispatchTaskErrorReason_DISPATCH_TASK_ERROR_REASON_WORKER_DISCONNECTED, final.GetDispatchError().GetReason())
}

func TestMatchingDisconnectBeforeOutputExhaustsWaitIntegration(t *testing.T) {
	server := startTestServer(t, config.MatchingConfig{}, newTestMembership())
	worker := registerWorker(t, server, "flow", "worker-disconnecting")
	dispatch, err := dexpb.NewMatchingServiceClient(server.connection).DispatchTask(testStreamContext(t), dispatchRequest("flow", "", 0, 500*time.Millisecond))
	require.NoError(t, err)
	assigned, err := worker.Recv()
	require.NoError(t, err)
	require.NotNil(t, assigned.GetTask())
	assignmentState, err := dispatch.Recv()
	require.NoError(t, err)
	require.NotNil(t, assignmentState.GetAssigned())
	require.NoError(t, worker.CloseSend())
	unassignedState, err := dispatch.Recv()
	require.NoError(t, err)
	require.NotNil(t, unassignedState.GetUnassigned())
	frames := receiveDispatch(t, dispatch)
	require.Len(t, frames, 1)
	require.Equal(t, dexpb.DispatchTaskErrorReason_DISPATCH_TASK_ERROR_REASON_NO_WORKER_AVAILABLE, frames[0].GetDispatchError().GetReason())
}

func TestMatchingDisconnectAfterWaitBudgetIntegration(t *testing.T) {
	server := startTestServer(t, config.MatchingConfig{}, newTestMembership())
	worker := registerWorker(t, server, "flow", "worker-late-disconnect")
	dispatchStarted := time.Now()
	request := dispatchRequest("flow", "", 0, 50*time.Millisecond)
	request.Request = &dexpb.DispatchTaskRequest_Execute{Execute: &dexpb.InvokeExecuteMethodRequest{}}
	dispatch, err := dexpb.NewMatchingServiceClient(server.connection).DispatchTask(testStreamContext(t), request)
	require.NoError(t, err)
	assigned, err := worker.Recv()
	require.NoError(t, err)
	require.NotNil(t, assigned.GetTask())
	require.Eventually(t, func() bool { return time.Since(dispatchStarted) >= 100*time.Millisecond }, time.Second, 5*time.Millisecond)
	require.NoError(t, worker.CloseSend())
	frames := receiveDispatch(t, dispatch)
	require.Len(t, frames, 1)
	require.Equal(t, dexpb.DispatchTaskErrorReason_DISPATCH_TASK_ERROR_REASON_WORKER_DISCONNECTED, frames[0].GetDispatchError().GetReason())
}

func TestMatchingCapacityRejectBackoffBoundsResends(t *testing.T) {
	server := startTestServer(t, config.MatchingConfig{}, newTestMembership())
	worker := registerWorker(t, server, "flow", "worker-at-capacity")
	dispatch, err := dexpb.NewMatchingServiceClient(server.connection).DispatchTask(testStreamContext(t), dispatchRequest("flow", "", 0, 1100*time.Millisecond))
	require.NoError(t, err)
	type rejectionResult struct {
		assignmentCount int
		err             error
	}
	results := make(chan rejectionResult, 1)
	go func() {
		assignmentCount := 0
		for {
			frame, receiveErr := worker.Recv()
			if receiveErr != nil {
				if receiveErr == io.EOF {
					receiveErr = nil
				}
				results <- rejectionResult{assignmentCount: assignmentCount, err: receiveErr}
				return
			}
			if task := frame.GetTask(); task != nil {
				assignmentCount++
				if sendErr := worker.Send(&dexpb.PollWorkerTaskRequest{Frame: &dexpb.PollWorkerTaskRequest_Reject{Reject: &dexpb.PollWorkerTaskReject{TaskId: task.GetTaskId(), Reason: dexpb.PollWorkerTaskRejectReason_POLL_WORKER_TASK_REJECT_REASON_NO_CAPACITY}}}); sendErr != nil {
					results <- rejectionResult{assignmentCount: assignmentCount, err: sendErr}
					return
				}
			}
		}
	}()
	frames := receiveDispatch(t, dispatch)
	require.Len(t, frames, 1)
	require.Equal(t, dexpb.DispatchTaskErrorReason_DISPATCH_TASK_ERROR_REASON_NO_WORKER_AVAILABLE, frames[0].GetDispatchError().GetReason())
	require.NoError(t, worker.CloseSend())
	result := <-results
	require.NoError(t, result.err)
	require.GreaterOrEqual(t, result.assignmentCount, 2)
	require.LessOrEqual(t, result.assignmentCount, 3)
}

func TestMatchingAssignedWorkerCancelRedispatchIntegration(t *testing.T) {
	server := startTestServer(t, config.MatchingConfig{}, newTestMembership())
	ctx, cancel := context.WithCancel(testStreamContext(t))
	worker, err := dexpb.NewWorkerTaskServiceClient(server.connection).PollWorkerTask(ctx)
	require.NoError(t, err)
	require.NoError(t, worker.Send(&dexpb.PollWorkerTaskRequest{Frame: &dexpb.PollWorkerTaskRequest_Register{Register: &dexpb.PollWorkerTaskRegister{Queue: "flow", AppWorkerId: "worker-cancel", FlowTypes: []string{"flow"}}}}))
	registered, err := worker.Recv()
	require.NoError(t, err)
	require.NotNil(t, registered.GetRegistered())
	waitWorkerIdle(t, server.engine, "flow", "worker-cancel")
	dispatch, err := dexpb.NewMatchingServiceClient(server.connection).DispatchTask(testStreamContext(t), dispatchRequest("flow", "", 0, time.Second))
	require.NoError(t, err)
	assigned, err := worker.Recv()
	require.NoError(t, err)
	replacement := registerWorker(t, server, "flow", "worker-replacement")
	cancel()
	reassigned, err := replacement.Recv()
	require.NoError(t, err)
	require.Equal(t, assigned.GetTask().GetTaskId(), reassigned.GetTask().GetTaskId())
	require.NoError(t, replacement.Send(&dexpb.PollWorkerTaskRequest{Frame: &dexpb.PollWorkerTaskRequest_TaskFrame{TaskFrame: &dexpb.PollWorkerTaskFrame{TaskId: reassigned.GetTask().GetTaskId(), Output: &dexpb.PollWorkerTaskFrame_Rpc{Rpc: &dexpb.InvokeWorkerRPCResponse{}}}}}))
	frames := receiveDispatch(t, dispatch)
	require.Len(t, frames, 1)
	require.Equal(t, "worker-replacement", frames[0].GetServedByAppWorkerId())
}

func TestMatchingForwardAndProxyIntegration(t *testing.T) {
	ownerMembership := newTestMembership()
	owner := startTestServer(t, config.MatchingConfig{}, ownerMembership)
	receiverMembership := newTestMembership()
	receiverMembership.owners["flow"] = membership.HostInfo{RPCAddress: owner.listener.Addr().String()}
	receiver := startTestServer(t, config.MatchingConfig{}, receiverMembership)
	worker := registerWorker(t, receiver, "flow", "worker-forwarded")
	dispatch, err := dexpb.NewMatchingServiceClient(receiver.connection).DispatchTask(testStreamContext(t), dispatchRequest("flow", "", 0, time.Second))
	require.NoError(t, err)
	assigned, err := worker.Recv()
	require.NoError(t, err)
	require.NoError(t, worker.Send(&dexpb.PollWorkerTaskRequest{Frame: &dexpb.PollWorkerTaskRequest_TaskFrame{TaskFrame: &dexpb.PollWorkerTaskFrame{TaskId: assigned.GetTask().TaskId, Output: &dexpb.PollWorkerTaskFrame_Rpc{Rpc: &dexpb.InvokeWorkerRPCResponse{}}}}}))
	frames := receiveDispatch(t, dispatch)
	require.Len(t, frames, 1)
	require.Equal(t, "worker-forwarded", frames[0].ServedByAppWorkerId)
}

func TestMatchingRegistrationLimitsIntegration(t *testing.T) {
	server := startTestServer(t, config.MatchingConfig{MaxStreams: 2, MaxConcurrentTasksCap: 1}, newTestMembership())
	registerWorker(t, server, "flow", "worker-1")
	stream, err := dexpb.NewWorkerTaskServiceClient(server.connection).PollWorkerTask(testStreamContext(t))
	require.NoError(t, err)
	require.NoError(t, stream.Send(&dexpb.PollWorkerTaskRequest{Frame: &dexpb.PollWorkerTaskRequest_Register{Register: &dexpb.PollWorkerTaskRegister{Queue: "flow", AppWorkerId: "worker-1", FlowTypes: []string{"flow"}}}}))
	_, err = stream.Recv()
	require.Error(t, err)
	invalid, err := dexpb.NewWorkerTaskServiceClient(server.connection).PollWorkerTask(testStreamContext(t))
	require.NoError(t, err)
	require.NoError(t, invalid.Send(&dexpb.PollWorkerTaskRequest{Frame: &dexpb.PollWorkerTaskRequest_Register{Register: &dexpb.PollWorkerTaskRegister{Queue: "flow", AppWorkerId: "bad/id", FlowTypes: []string{"flow"}}}}))
	_, err = invalid.Recv()
	require.Error(t, err)
	registerWorker(t, server, "flow", "worker-2")
	full, err := dexpb.NewWorkerTaskServiceClient(server.connection).PollWorkerTask(testStreamContext(t))
	require.NoError(t, err)
	require.NoError(t, full.Send(&dexpb.PollWorkerTaskRequest{Frame: &dexpb.PollWorkerTaskRequest_Register{Register: &dexpb.PollWorkerTaskRegister{Queue: "flow", AppWorkerId: "worker-3", FlowTypes: []string{"flow"}}}}))
	_, err = full.Recv()
	require.Error(t, err)
	wrongQueue, err := dexpb.NewWorkerTaskServiceClient(server.connection).PollWorkerTask(testStreamContext(t))
	require.NoError(t, err)
	require.NoError(t, wrongQueue.Send(&dexpb.PollWorkerTaskRequest{Frame: &dexpb.PollWorkerTaskRequest_Register{Register: &dexpb.PollWorkerTaskRegister{Queue: "other", AppWorkerId: "worker-4", FlowTypes: []string{"flow"}}}}))
	_, err = wrongQueue.Recv()
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestMatchingExecuteAndRPCIntegration(t *testing.T) {
	server := startTestServer(t, config.MatchingConfig{}, newTestMembership())
	worker := registerWorker(t, server, "flow", "worker-variants")
	cases := []struct {
		name       string
		setRequest func(*dexpb.DispatchTaskRequest)
		output     *dexpb.PollWorkerTaskFrame
	}{
		{name: "execute", setRequest: func(request *dexpb.DispatchTaskRequest) {
			request.Request = &dexpb.DispatchTaskRequest_Execute{Execute: &dexpb.InvokeExecuteMethodRequest{}}
		}, output: &dexpb.PollWorkerTaskFrame{Output: &dexpb.PollWorkerTaskFrame_Execute{Execute: &dexpb.InvokeExecuteMethodOutput{Output: &dexpb.InvokeExecuteMethodOutput_Result{Result: &dexpb.InvokeExecuteMethodResponse{}}}}}},
		{name: "rpc", setRequest: func(request *dexpb.DispatchTaskRequest) {
			request.Request = &dexpb.DispatchTaskRequest_Rpc{Rpc: &dexpb.InvokeWorkerRPCRequest{}}
		}, output: &dexpb.PollWorkerTaskFrame{Output: &dexpb.PollWorkerTaskFrame_Rpc{Rpc: &dexpb.InvokeWorkerRPCResponse{}}}},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			request := dispatchRequest("flow", "", 0, time.Second)
			request.TaskId = testCase.name
			testCase.setRequest(request)
			dispatch, err := dexpb.NewMatchingServiceClient(server.connection).DispatchTask(testStreamContext(t), request)
			require.NoError(t, err)
			assigned, err := worker.Recv()
			require.NoError(t, err)
			require.Equal(t, testCase.name, assigned.GetTask().TaskId)
			testCase.output.TaskId = testCase.name
			require.NoError(t, worker.Send(&dexpb.PollWorkerTaskRequest{Frame: &dexpb.PollWorkerTaskRequest_TaskFrame{TaskFrame: testCase.output}}))
			frames := receiveDispatch(t, dispatch)
			require.Len(t, frames, 1)
			require.Equal(t, "worker-variants", frames[0].ServedByAppWorkerId)
		})
	}
}

func TestMatchingRealMembershipThreeEngineIntegration(t *testing.T) {
	listeners := make([]net.Listener, 3)
	for index := range listeners {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		listeners[index] = listener
	}
	membershipSettings := &config.MembershipConfig{Listen: "127.0.0.1:0", BroadcastAddress: "127.0.0.1", RefreshInterval: 50 * time.Millisecond}
	monitors := make([]*membership.Monitor, 3)
	var err error
	monitors[0], err = membership.NewMonitor(membershipSettings, []membership.Role{membership.RoleMatching}, listeners[0].Addr().String(), nil, zap.NewNop())
	require.NoError(t, err)
	require.NoError(t, monitors[0].Start(context.Background()))
	t.Cleanup(monitors[0].Stop)
	seed := monitors[0].Members(membership.RoleMatching)[0].Address
	for index := 1; index < 3; index++ {
		settings := *membershipSettings
		settings.Seeds.Static = []string{seed}
		monitors[index], err = membership.NewMonitor(&settings, []membership.Role{membership.RoleMatching}, listeners[index].Addr().String(), nil, zap.NewNop())
		require.NoError(t, err)
		require.NoError(t, monitors[index].Start(context.Background()))
		t.Cleanup(monitors[index].Stop)
	}
	require.Eventually(t, func() bool {
		for _, monitor := range monitors {
			if len(monitor.Members(membership.RoleMatching)) != 3 {
				return false
			}
		}
		return true
	}, 10*time.Second, 20*time.Millisecond)
	servers := make([]*testServer, 3)
	for index := range servers {
		servers[index] = startTestServerForMonitor(t, listeners[index], monitors[index])
	}
	newMembershipListener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	newMembershipAddress := newMembershipListener.Addr().String()
	require.NoError(t, newMembershipListener.Close())
	futureRing := hashring.New(farm.Fingerprint32, 100)
	for _, member := range monitors[1].Members(membership.RoleMatching) {
		futureRing.AddMembers(testHashMember{address: member.Address})
	}
	futureRing.AddMembers(testHashMember{address: newMembershipAddress})
	queue := ""
	for index := 0; index < 1000; index++ {
		candidate := fmt.Sprintf("flow-%d", index)
		owner, lookupErr := monitors[1].Lookup(membership.RoleMatching, candidate)
		require.NoError(t, lookupErr)
		futureAddress, isFound := futureRing.Lookup(candidate)
		if owner.IsSelf && isFound && futureAddress == newMembershipAddress {
			queue = candidate
			break
		}
	}
	require.NotEmpty(t, queue)
	worker := registerWorkerForFlowTypes(t, servers[0], queue, "worker-ring", []string{queue})
	request := dispatchRequest(queue, "", 0, time.Second)
	dispatch, err := dexpb.NewMatchingServiceClient(servers[2].connection).DispatchTask(testStreamContext(t), request)
	require.NoError(t, err)
	assigned, err := worker.Recv()
	require.NoError(t, err)
	newListener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	settings := *membershipSettings
	settings.Listen = newMembershipAddress
	settings.Seeds.Static = []string{seed}
	newMonitor, err := membership.NewMonitor(&settings, []membership.Role{membership.RoleMatching}, newListener.Addr().String(), nil, zap.NewNop())
	require.NoError(t, err)
	require.NoError(t, newMonitor.Start(context.Background()))
	t.Cleanup(newMonitor.Stop)
	monitors = append(monitors, newMonitor)
	servers = append(servers, startTestServerForMonitor(t, newListener, newMonitor))
	require.Eventually(t, func() bool {
		return len(monitors[1].Members(membership.RoleMatching)) == 4
	}, 10*time.Second, 20*time.Millisecond)
	newOwner, err := monitors[1].Lookup(membership.RoleMatching, queue)
	require.NoError(t, err)
	require.False(t, newOwner.IsSelf, "adding members did not transfer queue ownership")
	require.Eventually(t, func() bool {
		for _, monitor := range monitors {
			owner, lookupErr := monitor.Lookup(membership.RoleMatching, queue)
			if lookupErr != nil || owner.RPCAddress != newOwner.RPCAddress {
				return false
			}
		}
		return true
	}, 10*time.Second, 20*time.Millisecond)
	oldQueue := servers[1].engine.getQueue(queue)
	require.Eventually(t, func() bool {
		oldQueue.mu.Lock()
		defer oldQueue.mu.Unlock()
		for registered := range oldQueue.streams {
			registered.mu.Lock()
			isClosing := registered.isClosing
			registered.mu.Unlock()
			if isClosing {
				return true
			}
		}
		return false
	}, 5*time.Second, 10*time.Millisecond)
	servers[1].engine.releaseQueue(queue, oldQueue)
	reconnectFrames := make(chan *dexpb.PollWorkerTaskResponse, 1)
	reconnectErrors := make(chan error, 1)
	go func() {
		frame, recvErr := worker.Recv()
		if recvErr != nil {
			reconnectErrors <- recvErr
		} else {
			reconnectFrames <- frame
		}
	}()
	select {
	case <-reconnectFrames:
		t.Fatal("reconnect arrived before current task finished")
	case recvErr := <-reconnectErrors:
		t.Fatal(recvErr)
	default:
	}
	require.NoError(t, worker.Send(&dexpb.PollWorkerTaskRequest{Frame: &dexpb.PollWorkerTaskRequest_TaskFrame{TaskFrame: &dexpb.PollWorkerTaskFrame{TaskId: assigned.GetTask().TaskId, Output: &dexpb.PollWorkerTaskFrame_Rpc{Rpc: &dexpb.InvokeWorkerRPCResponse{}}}}}))
	frames := receiveDispatch(t, dispatch)
	require.Len(t, frames, 1)
	require.Equal(t, "worker-ring", frames[0].ServedByAppWorkerId)
	select {
	case reconnect := <-reconnectFrames:
		require.NotNil(t, reconnect.GetReconnect())
	case recvErr := <-reconnectErrors:
		t.Fatal(recvErr)
	case <-time.After(5 * time.Second):
		t.Fatal("reconnect was not sent after task completion")
	}
	require.False(t, newOwner.RPCAddress == listeners[1].Addr().String())
	replacement := registerWorkerForFlowTypes(t, servers[0], queue, "worker-ring", []string{queue})
	replacementRequest := dispatchRequest(queue, "", 0, time.Second)
	replacementRequest.TaskId = "replacement"
	replacementDispatch, err := dexpb.NewMatchingServiceClient(servers[2].connection).DispatchTask(testStreamContext(t), replacementRequest)
	require.NoError(t, err)
	replacementAssignment, err := replacement.Recv()
	require.NoError(t, err)
	require.Equal(t, "replacement", replacementAssignment.GetTask().TaskId)
	require.NoError(t, replacement.Send(&dexpb.PollWorkerTaskRequest{Frame: &dexpb.PollWorkerTaskRequest_TaskFrame{TaskFrame: &dexpb.PollWorkerTaskFrame{TaskId: "replacement", Output: &dexpb.PollWorkerTaskFrame_Rpc{Rpc: &dexpb.InvokeWorkerRPCResponse{}}}}}))
	require.Len(t, receiveDispatch(t, replacementDispatch), 1)
}

func startTestServerForMonitor(t *testing.T, listener net.Listener, monitor *membership.Monitor) *testServer {
	t.Helper()
	clients := func(ctx context.Context, address string) (dexpb.MatchingServiceClient, error) {
		connection, err := grpc.DialContext(ctx, address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}
		t.Cleanup(func() { require.NoError(t, connection.Close()) })
		return dexpb.NewMatchingServiceClient(connection), nil
	}
	settings := config.MatchingConfig{}
	engine, err := NewEngine(&settings, zap.NewNop(), monitor, clients)
	require.NoError(t, err)
	grpcServer := grpc.NewServer()
	server := &testServer{engine: engine, server: grpcServer, listener: listener}
	dexpb.RegisterMatchingServiceServer(grpcServer, engine)
	dexpb.RegisterWorkerTaskServiceServer(grpcServer, server)
	go func() {
		if serveErr := grpcServer.Serve(listener); serveErr != nil {
			t.Log(serveErr)
		}
	}()
	connection, err := grpc.DialContext(context.Background(), listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	server.connection = connection
	t.Cleanup(func() { require.NoError(t, connection.Close()); engine.Close(); grpcServer.Stop() })
	return server
}

func registerWorkerForFlowTypes(t *testing.T, server *testServer, queue, workerID string, flowTypes []string) dexpb.WorkerTaskService_PollWorkerTaskClient {
	t.Helper()
	stream, err := dexpb.NewWorkerTaskServiceClient(server.connection).PollWorkerTask(testStreamContext(t))
	require.NoError(t, err)
	require.NoError(t, stream.Send(&dexpb.PollWorkerTaskRequest{Frame: &dexpb.PollWorkerTaskRequest_Register{Register: &dexpb.PollWorkerTaskRegister{Queue: queue, AppWorkerId: workerID, FlowTypes: flowTypes}}}))
	registered, err := stream.Recv()
	require.NoError(t, err)
	require.NotNil(t, registered.GetRegistered())
	return stream
}

func TestMatchingAffinityIntegration(t *testing.T) {
	server := startTestServer(t, config.MatchingConfig{}, newTestMembership())
	first := registerWorker(t, server, "flow", "worker-first")
	registerWorker(t, server, "flow", "worker-second")
	waitWorkerIdle(t, server.engine, "flow", "worker-first")
	for _, taskID := range []string{"affinity-1", "affinity-2"} {
		request := dispatchRequest("flow", "", 0, time.Second)
		request.TaskId = taskID
		dispatch, err := dexpb.NewMatchingServiceClient(server.connection).DispatchTask(testStreamContext(t), request)
		require.NoError(t, err)
		assigned, err := first.Recv()
		require.NoError(t, err)
		require.Equal(t, taskID, assigned.GetTask().TaskId)
		require.NoError(t, first.Send(&dexpb.PollWorkerTaskRequest{Frame: &dexpb.PollWorkerTaskRequest_TaskFrame{TaskFrame: &dexpb.PollWorkerTaskFrame{TaskId: taskID, Output: &dexpb.PollWorkerTaskFrame_Rpc{Rpc: &dexpb.InvokeWorkerRPCResponse{}}}}}))
		require.Len(t, receiveDispatch(t, dispatch), 1)
		waitWorkerIdle(t, server.engine, "flow", "worker-first")
	}
}

func TestMatchingReconnectAfterTaskIntegration(t *testing.T) {
	members := newTestMembership()
	server := startTestServer(t, config.MatchingConfig{}, members)
	worker := registerWorker(t, server, "flow", "worker-drain")
	dispatch, err := dexpb.NewMatchingServiceClient(server.connection).DispatchTask(testStreamContext(t), dispatchRequest("flow", "", 0, time.Second))
	require.NoError(t, err)
	assigned, err := worker.Recv()
	require.NoError(t, err)
	members.mu.Lock()
	members.owners["flow"] = membership.HostInfo{RPCAddress: "127.0.0.1:1"}
	members.mu.Unlock()
	members.changes <- membership.Change{Role: membership.RoleMatching}
	queueState := server.engine.getQueue("flow")
	require.Eventually(t, func() bool {
		queueState.mu.Lock()
		defer queueState.mu.Unlock()
		for registered := range queueState.streams {
			registered.mu.Lock()
			isClosing := registered.isClosing
			registered.mu.Unlock()
			if isClosing {
				return true
			}
		}
		return false
	}, time.Second, time.Millisecond)
	server.engine.releaseQueue("flow", queueState)
	control := make(chan *dexpb.PollWorkerTaskResponse, 1)
	errors := make(chan error, 1)
	go func() {
		response, recvErr := worker.Recv()
		if recvErr != nil {
			errors <- recvErr
		} else {
			control <- response
		}
	}()
	select {
	case <-control:
		t.Fatal("reconnect arrived before current task finished")
	case recvErr := <-errors:
		t.Fatal(recvErr)
	default:
	}
	require.NoError(t, worker.Send(&dexpb.PollWorkerTaskRequest{Frame: &dexpb.PollWorkerTaskRequest_TaskFrame{TaskFrame: &dexpb.PollWorkerTaskFrame{TaskId: assigned.GetTask().TaskId, Output: &dexpb.PollWorkerTaskFrame_Rpc{Rpc: &dexpb.InvokeWorkerRPCResponse{}}}}}))
	require.Len(t, receiveDispatch(t, dispatch), 1)
	select {
	case response := <-control:
		require.NotNil(t, response.GetReconnect())
	case recvErr := <-errors:
		t.Fatal(recvErr)
	case <-time.After(time.Second):
		t.Fatal("reconnect was not sent after task completion")
	}
}

func TestMatchingForwardingLoopIntegration(t *testing.T) {
	firstMembership := newTestMembership()
	first := startTestServer(t, config.MatchingConfig{}, firstMembership)
	secondMembership := newTestMembership()
	second := startTestServer(t, config.MatchingConfig{}, secondMembership)
	firstMembership.mu.Lock()
	firstMembership.owners["flow"] = membership.HostInfo{RPCAddress: second.listener.Addr().String()}
	firstMembership.mu.Unlock()
	secondMembership.mu.Lock()
	secondMembership.owners["flow"] = membership.HostInfo{RPCAddress: first.listener.Addr().String()}
	secondMembership.mu.Unlock()
	dispatch, err := dexpb.NewMatchingServiceClient(first.connection).DispatchTask(testStreamContext(t), dispatchRequest("flow", "", 0, time.Second))
	require.NoError(t, err)
	_, err = dispatch.Recv()
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	poll, err := dexpb.NewWorkerTaskServiceClient(first.connection).PollWorkerTask(testStreamContext(t))
	require.NoError(t, err)
	require.NoError(t, poll.Send(&dexpb.PollWorkerTaskRequest{Frame: &dexpb.PollWorkerTaskRequest_Register{Register: &dexpb.PollWorkerTaskRegister{Queue: "flow", AppWorkerId: "worker-loop", FlowTypes: []string{"flow"}}}}))
	_, err = poll.Recv()
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
}

func TestMatchingAbandonedTaskReleasesStreamIntegration(t *testing.T) {
	for _, frameCount := range []int{0, 64} {
		t.Run(fmt.Sprintf("late-frames-%d", frameCount), func(t *testing.T) {
			server := startTestServer(t, config.MatchingConfig{}, newTestMembership())
			worker := registerWorker(t, server, "flow", "worker-abandoned")
			ctx, cancel := context.WithCancel(testStreamContext(t))
			dispatch, err := dexpb.NewMatchingServiceClient(server.connection).DispatchTask(ctx, dispatchRequest("flow", "", 0, time.Second))
			require.NoError(t, err)
			assigned, err := worker.Recv()
			require.NoError(t, err)
			cancel()
			control, err := worker.Recv()
			require.NoError(t, err)
			require.Equal(t, assigned.GetTask().TaskId, control.GetCancelTask().TaskId)
			requireDispatchError(t, dispatch)
			for index := 0; index < frameCount; index++ {
				frame := &dexpb.PollWorkerTaskRequest{Frame: &dexpb.PollWorkerTaskRequest_TaskFrame{TaskFrame: &dexpb.PollWorkerTaskFrame{TaskId: assigned.GetTask().TaskId, Output: &dexpb.PollWorkerTaskFrame_WaitFor{WaitFor: &dexpb.InvokeWaitForMethodOutput{Output: &dexpb.InvokeWaitForMethodOutput_Heartbeat{}}}}}}
				if sendErr := worker.Send(frame); sendErr != nil {
					break
				}
			}
			require.Eventually(t, func() bool {
				server.engine.limitsMu.Lock()
				defer server.engine.limitsMu.Unlock()
				return server.engine.streamCount == 0
			}, 5*time.Second, 10*time.Millisecond)
			require.Eventually(t, func() bool {
				_, exists := server.engine.queues.Load("flow")
				return !exists
			}, time.Second, 10*time.Millisecond)
		})
	}
}

func TestMatchingCanceledTaskFinalFrameReusesStreamIntegration(t *testing.T) {
	server := startTestServer(t, config.MatchingConfig{}, newTestMembership())
	worker := registerWorker(t, server, "flow", "worker-reusable")
	ctx, cancel := context.WithCancel(testStreamContext(t))
	dispatch, err := dexpb.NewMatchingServiceClient(server.connection).DispatchTask(ctx, dispatchRequest("flow", "", 0, time.Second))
	require.NoError(t, err)
	assigned, err := worker.Recv()
	require.NoError(t, err)
	cancel()
	control, err := worker.Recv()
	require.NoError(t, err)
	require.Equal(t, assigned.GetTask().TaskId, control.GetCancelTask().TaskId)
	requireDispatchError(t, dispatch)
	require.NoError(t, worker.Send(&dexpb.PollWorkerTaskRequest{Frame: &dexpb.PollWorkerTaskRequest_TaskFrame{TaskFrame: &dexpb.PollWorkerTaskFrame{TaskId: assigned.GetTask().TaskId, Output: &dexpb.PollWorkerTaskFrame_Rpc{Rpc: &dexpb.InvokeWorkerRPCResponse{}}}}}))
	secondRequest := dispatchRequest("flow", "", 0, time.Second)
	secondRequest.TaskId = "second-task"
	second, err := dexpb.NewMatchingServiceClient(server.connection).DispatchTask(testStreamContext(t), secondRequest)
	require.NoError(t, err)
	secondAssignment, err := worker.Recv()
	require.NoError(t, err)
	require.Equal(t, "second-task", secondAssignment.GetTask().TaskId)
	require.NoError(t, worker.Send(&dexpb.PollWorkerTaskRequest{Frame: &dexpb.PollWorkerTaskRequest_TaskFrame{TaskFrame: &dexpb.PollWorkerTaskFrame{TaskId: "second-task", Output: &dexpb.PollWorkerTaskFrame_Rpc{Rpc: &dexpb.InvokeWorkerRPCResponse{}}}}}))
	require.Len(t, receiveDispatch(t, second), 1)
}

func TestMatchingFinalFrameBeforeEOFRaceIntegration(t *testing.T) {
	server := startTestServer(t, config.MatchingConfig{}, newTestMembership())
	for index := 0; index < 100; index++ {
		worker := registerWorker(t, server, "flow", fmt.Sprintf("worker-%d", index))
		request := dispatchRequest("flow", "", 0, time.Second)
		request.TaskId = fmt.Sprintf("task-%d", index)
		dispatch, err := dexpb.NewMatchingServiceClient(server.connection).DispatchTask(testStreamContext(t), request)
		require.NoError(t, err)
		assigned, err := worker.Recv()
		require.NoError(t, err)
		require.NoError(t, worker.Send(&dexpb.PollWorkerTaskRequest{Frame: &dexpb.PollWorkerTaskRequest_TaskFrame{TaskFrame: &dexpb.PollWorkerTaskFrame{TaskId: assigned.GetTask().TaskId, Output: &dexpb.PollWorkerTaskFrame_Rpc{Rpc: &dexpb.InvokeWorkerRPCResponse{}}}}}))
		require.NoError(t, worker.CloseSend())
		frames := receiveDispatch(t, dispatch)
		require.Len(t, frames, 1)
		require.Equal(t, fmt.Sprintf("worker-%d", index), frames[0].ServedByAppWorkerId)
		require.Eventually(t, func() bool {
			server.engine.limitsMu.Lock()
			defer server.engine.limitsMu.Unlock()
			return server.engine.streamCount == 0
		}, time.Second, time.Millisecond)
	}
}

func TestMatchingStaleAndDuplicateFramesIntegration(t *testing.T) {
	server := startTestServer(t, config.MatchingConfig{}, newTestMembership())
	worker := registerWorker(t, server, "flow", "worker-stale")
	for _, taskID := range []string{"first", "second"} {
		request := dispatchRequest("flow", "", 0, time.Second)
		request.TaskId = taskID
		dispatch, err := dexpb.NewMatchingServiceClient(server.connection).DispatchTask(testStreamContext(t), request)
		require.NoError(t, err)
		_, err = worker.Recv()
		require.NoError(t, err)
		require.NoError(t, worker.Send(&dexpb.PollWorkerTaskRequest{Frame: &dexpb.PollWorkerTaskRequest_TaskFrame{TaskFrame: &dexpb.PollWorkerTaskFrame{TaskId: "stale", Output: &dexpb.PollWorkerTaskFrame_Rpc{Rpc: &dexpb.InvokeWorkerRPCResponse{}}}}}))
		valid := &dexpb.PollWorkerTaskRequest{Frame: &dexpb.PollWorkerTaskRequest_TaskFrame{TaskFrame: &dexpb.PollWorkerTaskFrame{TaskId: taskID, Output: &dexpb.PollWorkerTaskFrame_Rpc{Rpc: &dexpb.InvokeWorkerRPCResponse{}}}}}
		require.NoError(t, worker.Send(valid))
		require.Len(t, receiveDispatch(t, dispatch), 1)
		require.NoError(t, worker.Send(valid))
	}
}

func TestMatchingCrossEngineStickyFallbackIntegration(t *testing.T) {
	for _, stickyWait := range []time.Duration{0, 30 * time.Millisecond} {
		t.Run(stickyWait.String(), func(t *testing.T) {
			entryMembers := newTestMembership()
			stickyMembers := newTestMembership()
			fallbackMembers := newTestMembership()
			entry := startTestServer(t, config.MatchingConfig{}, entryMembers)
			sticky := startTestServer(t, config.MatchingConfig{}, stickyMembers)
			fallback := startTestServer(t, config.MatchingConfig{}, fallbackMembers)
			entryMembers.mu.Lock()
			entryMembers.owners["sticky"] = membership.HostInfo{RPCAddress: sticky.listener.Addr().String()}
			entryMembers.owners["fallback"] = membership.HostInfo{RPCAddress: fallback.listener.Addr().String()}
			entryMembers.mu.Unlock()
			stickyMembers.mu.Lock()
			stickyMembers.owners["fallback"] = membership.HostInfo{RPCAddress: entry.listener.Addr().String()}
			stickyMembers.mu.Unlock()
			worker := registerWorkerForFlowTypes(t, fallback, "fallback", "worker-fallback", []string{"fallback"})
			dispatch, err := dexpb.NewMatchingServiceClient(entry.connection).DispatchTask(testStreamContext(t), dispatchRequest("sticky", "fallback", stickyWait, time.Second))
			require.NoError(t, err)
			assigned, err := worker.Recv()
			require.NoError(t, err)
			require.NoError(t, worker.Send(&dexpb.PollWorkerTaskRequest{Frame: &dexpb.PollWorkerTaskRequest_TaskFrame{TaskFrame: &dexpb.PollWorkerTaskFrame{TaskId: assigned.GetTask().TaskId, Output: &dexpb.PollWorkerTaskFrame_Rpc{Rpc: &dexpb.InvokeWorkerRPCResponse{}}}}}))
			frames := receiveDispatch(t, dispatch)
			require.Len(t, frames, 1)
			require.Equal(t, "worker-fallback", frames[0].ServedByAppWorkerId)
		})
	}
}

func TestMatchingLegacyForwarderOmitsAssignmentStateIntegration(t *testing.T) {
	entryMembers := newTestMembership()
	entry := startTestServer(t, config.MatchingConfig{}, entryMembers)
	owner := startTestServer(t, config.MatchingConfig{}, newTestMembership())
	entryMembers.mu.Lock()
	entryMembers.owners["flow"] = membership.HostInfo{RPCAddress: owner.listener.Addr().String()}
	entryMembers.mu.Unlock()
	worker := registerWorker(t, owner, "flow", "legacy-forwarded-worker")
	request := dispatchRequest("flow", "", 0, time.Second)
	request.SupportsAssignmentState = false
	dispatch, err := dexpb.NewMatchingServiceClient(entry.connection).DispatchTask(testStreamContext(t), request)
	require.NoError(t, err)
	assignment, err := worker.Recv()
	require.NoError(t, err)
	require.NoError(t, worker.Send(&dexpb.PollWorkerTaskRequest{Frame: &dexpb.PollWorkerTaskRequest_TaskFrame{
		TaskFrame: &dexpb.PollWorkerTaskFrame{TaskId: assignment.GetTask().GetTaskId(), Output: &dexpb.PollWorkerTaskFrame_Rpc{Rpc: &dexpb.InvokeWorkerRPCResponse{}}},
	}}))
	first, err := dispatch.Recv()
	require.NoError(t, err)
	require.NotNil(t, first.GetRpc())
}

func TestMatchingRemoteStickyDisconnectAfterWaitFallsBackIntegration(t *testing.T) {
	entryMembers := newTestMembership()
	entry := startTestServer(t, config.MatchingConfig{}, entryMembers)
	sticky := startTestServer(t, config.MatchingConfig{}, newTestMembership())
	fallback := startTestServer(t, config.MatchingConfig{}, newTestMembership())
	entryMembers.mu.Lock()
	entryMembers.owners["sticky"] = membership.HostInfo{RPCAddress: sticky.listener.Addr().String()}
	entryMembers.owners["fallback"] = membership.HostInfo{RPCAddress: fallback.listener.Addr().String()}
	entryMembers.mu.Unlock()
	stickyWorker := registerWorkerForFlowTypes(t, sticky, "sticky", "sticky", []string{"fallback"})
	fallbackWorker := registerWorkerForFlowTypes(t, fallback, "fallback", "worker-fallback", []string{"fallback"})
	started := time.Now()
	dispatch, err := dexpb.NewMatchingServiceClient(entry.connection).DispatchTask(testStreamContext(t), dispatchRequest("sticky", "fallback", 100*time.Millisecond, time.Second))
	require.NoError(t, err)
	assigned, err := stickyWorker.Recv()
	require.NoError(t, err)
	require.NotNil(t, assigned.GetTask())
	stickyState, err := dispatch.Recv()
	require.NoError(t, err)
	require.NotNil(t, stickyState.GetAssigned())
	require.Eventually(t, func() bool { return time.Since(started) >= 200*time.Millisecond }, time.Second, 5*time.Millisecond)
	require.NoError(t, stickyWorker.CloseSend())
	unassignedState, err := dispatch.Recv()
	require.NoError(t, err)
	require.NotNil(t, unassignedState.GetUnassigned())
	reassigned, err := fallbackWorker.Recv()
	require.NoError(t, err)
	require.Equal(t, assigned.GetTask().GetTaskId(), reassigned.GetTask().GetTaskId())
	fallbackState, err := dispatch.Recv()
	require.NoError(t, err)
	require.NotNil(t, fallbackState.GetAssigned())
	require.NoError(t, fallbackWorker.Send(&dexpb.PollWorkerTaskRequest{Frame: &dexpb.PollWorkerTaskRequest_TaskFrame{TaskFrame: &dexpb.PollWorkerTaskFrame{TaskId: reassigned.GetTask().GetTaskId(), Output: &dexpb.PollWorkerTaskFrame_Rpc{Rpc: &dexpb.InvokeWorkerRPCResponse{}}}}}))
	frames := receiveDispatch(t, dispatch)
	require.Len(t, frames, 1)
	require.Equal(t, "worker-fallback", frames[0].GetServedByAppWorkerId())
}

func TestMatchingRegistrationAfterCloseIntegration(t *testing.T) {
	server := startTestServer(t, config.MatchingConfig{}, newTestMembership())
	server.engine.Close()
	worker, err := dexpb.NewWorkerTaskServiceClient(server.connection).PollWorkerTask(testStreamContext(t))
	require.NoError(t, err)
	require.NoError(t, worker.Send(&dexpb.PollWorkerTaskRequest{Frame: &dexpb.PollWorkerTaskRequest_Register{Register: &dexpb.PollWorkerTaskRegister{Queue: "flow", AppWorkerId: "worker-closed", FlowTypes: []string{"flow"}}}}))
	_, err = worker.Recv()
	require.Equal(t, codes.Unavailable, status.Code(err))
}

func TestMatchingUntrustedNameLimitsIntegration(t *testing.T) {
	server := startTestServer(t, config.MatchingConfig{MaxQueueLength: 8, MaxFlowTypes: 1, MaxFlowTypeLength: 4}, newTestMembership())
	for _, registration := range []*dexpb.PollWorkerTaskRegister{
		{Queue: "queue-too-long", AppWorkerId: "worker-1", FlowTypes: []string{"flow"}},
		{Queue: "flow", AppWorkerId: "worker-2", FlowTypes: []string{"flow", "other"}},
		{Queue: "flow", AppWorkerId: "worker-3", FlowTypes: []string{"flow-too-long"}},
	} {
		worker, err := dexpb.NewWorkerTaskServiceClient(server.connection).PollWorkerTask(testStreamContext(t))
		require.NoError(t, err)
		require.NoError(t, worker.Send(&dexpb.PollWorkerTaskRequest{Frame: &dexpb.PollWorkerTaskRequest_Register{Register: registration}}))
		_, err = worker.Recv()
		require.Equal(t, codes.InvalidArgument, status.Code(err))
	}
	request := dispatchRequest("queue-too-long", "", 0, time.Second)
	dispatch, err := dexpb.NewMatchingServiceClient(server.connection).DispatchTask(testStreamContext(t), request)
	require.NoError(t, err)
	_, err = dispatch.Recv()
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestMatchingShortDispatchWaitUsesDefaultStickyIntegration(t *testing.T) {
	server := startTestServer(t, config.MatchingConfig{DispatchWait: 3 * time.Second}, newTestMembership())
	require.NotNil(t, server.engine)
}
