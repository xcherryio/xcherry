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

	"github.com/stretchr/testify/require"
	"github.com/superdurable/dex/config"
	"github.com/superdurable/dex/gen/dexpb"
	"github.com/superdurable/dex/service/membership"
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
	stream, err := dexpb.NewWorkerTaskServiceClient(server.connection).PollWorkerTask(context.Background())
	require.NoError(t, err)
	require.NoError(t, stream.Send(&dexpb.PollWorkerTaskRequest{Frame: &dexpb.PollWorkerTaskRequest_Register{Register: &dexpb.PollWorkerTaskRegister{Queue: queue, AppWorkerId: workerID, FlowTypes: []string{"flow"}}}}))
	registered, err := stream.Recv()
	require.NoError(t, err)
	require.NotNil(t, registered.GetRegistered())
	return stream
}

func dispatchRequest(queue, fallback string, sticky, total time.Duration) *dexpb.DispatchTaskRequest {
	return &dexpb.DispatchTaskRequest{Queue: queue, FallbackQueue: fallback, FlowId: "flow-1", TaskId: "task-1", StickyWait: durationpb.New(sticky), DispatchWait: durationpb.New(total), Deadline: timestamppb.New(time.Now().Add(5 * time.Second)), Request: &dexpb.DispatchTaskRequest_WaitFor{WaitFor: &dexpb.InvokeWaitForMethodRequest{}}}
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
		frames = append(frames, frame)
	}
}

func TestMatchingRelayAndReuseIntegration(t *testing.T) {
	server := startTestServer(t, config.MatchingConfig{}, newTestMembership())
	worker := registerWorker(t, server, "flow", "worker-1")
	for _, taskID := range []string{"first", "second"} {
		request := dispatchRequest("flow", "", 0, time.Second)
		request.TaskId = taskID
		dispatch, err := dexpb.NewMatchingServiceClient(server.connection).DispatchTask(context.Background(), request)
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

func TestMatchingFallbackAndUnavailableIntegration(t *testing.T) {
	server := startTestServer(t, config.MatchingConfig{}, newTestMembership())
	worker := registerWorker(t, server, "flow", "worker-2")
	request := dispatchRequest("stale-worker", "flow", 40*time.Millisecond, 250*time.Millisecond)
	started := time.Now()
	dispatch, err := dexpb.NewMatchingServiceClient(server.connection).DispatchTask(context.Background(), request)
	require.NoError(t, err)
	assigned, err := worker.Recv()
	require.NoError(t, err)
	require.GreaterOrEqual(t, time.Since(started), 35*time.Millisecond)
	require.NoError(t, worker.Send(&dexpb.PollWorkerTaskRequest{Frame: &dexpb.PollWorkerTaskRequest_TaskFrame{TaskFrame: &dexpb.PollWorkerTaskFrame{TaskId: assigned.GetTask().TaskId, Output: &dexpb.PollWorkerTaskFrame_WorkerError{WorkerError: &dexpb.WorkerErrorResponse{}}}}}))
	frames := receiveDispatch(t, dispatch)
	require.Len(t, frames, 1)
	require.Equal(t, "worker-2", frames[0].ServedByAppWorkerId)
	unavailable, err := dexpb.NewMatchingServiceClient(server.connection).DispatchTask(context.Background(), dispatchRequest("empty", "", 0, 60*time.Millisecond))
	require.NoError(t, err)
	frames = receiveDispatch(t, unavailable)
	require.Len(t, frames, 1)
	require.Equal(t, dexpb.DispatchTaskErrorReason_DISPATCH_TASK_ERROR_REASON_NO_WORKER_AVAILABLE, frames[0].GetDispatchError().Reason)
}

func TestMatchingRejectDisconnectAndCancelIntegration(t *testing.T) {
	server := startTestServer(t, config.MatchingConfig{}, newTestMembership())
	first := registerWorker(t, server, "flow", "worker-a")
	second := registerWorker(t, server, "flow", "worker-b")
	dispatch, err := dexpb.NewMatchingServiceClient(server.connection).DispatchTask(context.Background(), dispatchRequest("flow", "", 0, time.Second))
	require.NoError(t, err)
	assigned, err := first.Recv()
	require.NoError(t, err)
	require.NoError(t, first.Send(&dexpb.PollWorkerTaskRequest{Frame: &dexpb.PollWorkerTaskRequest_Reject{Reject: &dexpb.PollWorkerTaskReject{TaskId: assigned.GetTask().TaskId, Reason: dexpb.PollWorkerTaskRejectReason_POLL_WORKER_TASK_REJECT_REASON_NO_CAPACITY}}}))
	assigned, err = second.Recv()
	require.NoError(t, err)
	require.NoError(t, second.CloseSend())
	frames := receiveDispatch(t, dispatch)
	require.Len(t, frames, 1)
	require.Equal(t, dexpb.DispatchTaskErrorReason_DISPATCH_TASK_ERROR_REASON_WORKER_DISCONNECTED, frames[0].GetDispatchError().Reason)
	ctx, cancel := context.WithCancel(context.Background())
	canceled, err := dexpb.NewMatchingServiceClient(server.connection).DispatchTask(ctx, dispatchRequest("flow", "", 0, time.Second))
	require.NoError(t, err)
	assigned, err = first.Recv()
	require.NoError(t, err)
	cancel()
	control, err := first.Recv()
	require.NoError(t, err)
	require.Equal(t, assigned.GetTask().TaskId, control.GetCancelTask().TaskId)
	_, err = canceled.Recv()
	require.Error(t, err)
}

func TestMatchingForwardAndProxyIntegration(t *testing.T) {
	ownerMembership := newTestMembership()
	owner := startTestServer(t, config.MatchingConfig{}, ownerMembership)
	receiverMembership := newTestMembership()
	receiverMembership.owners["flow"] = membership.HostInfo{RPCAddress: owner.listener.Addr().String()}
	receiver := startTestServer(t, config.MatchingConfig{}, receiverMembership)
	worker := registerWorker(t, receiver, "flow", "worker-forwarded")
	dispatch, err := dexpb.NewMatchingServiceClient(receiver.connection).DispatchTask(context.Background(), dispatchRequest("flow", "", 0, time.Second))
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
	stream, err := dexpb.NewWorkerTaskServiceClient(server.connection).PollWorkerTask(context.Background())
	require.NoError(t, err)
	require.NoError(t, stream.Send(&dexpb.PollWorkerTaskRequest{Frame: &dexpb.PollWorkerTaskRequest_Register{Register: &dexpb.PollWorkerTaskRegister{Queue: "flow", AppWorkerId: "worker-1", FlowTypes: []string{"flow"}}}}))
	_, err = stream.Recv()
	require.Error(t, err)
	invalid, err := dexpb.NewWorkerTaskServiceClient(server.connection).PollWorkerTask(context.Background())
	require.NoError(t, err)
	require.NoError(t, invalid.Send(&dexpb.PollWorkerTaskRequest{Frame: &dexpb.PollWorkerTaskRequest_Register{Register: &dexpb.PollWorkerTaskRegister{Queue: "flow", AppWorkerId: "bad/id", FlowTypes: []string{"flow"}}}}))
	_, err = invalid.Recv()
	require.Error(t, err)
	registerWorker(t, server, "flow", "worker-2")
	full, err := dexpb.NewWorkerTaskServiceClient(server.connection).PollWorkerTask(context.Background())
	require.NoError(t, err)
	require.NoError(t, full.Send(&dexpb.PollWorkerTaskRequest{Frame: &dexpb.PollWorkerTaskRequest_Register{Register: &dexpb.PollWorkerTaskRegister{Queue: "flow", AppWorkerId: "worker-3", FlowTypes: []string{"flow"}}}}))
	_, err = full.Recv()
	require.Error(t, err)
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
			dispatch, err := dexpb.NewMatchingServiceClient(server.connection).DispatchTask(context.Background(), request)
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
	queue := ""
	for index := 0; index < 1000; index++ {
		candidate := fmt.Sprintf("flow-%d", index)
		owner, lookupErr := monitors[1].Lookup(membership.RoleMatching, candidate)
		require.NoError(t, lookupErr)
		if owner.IsSelf {
			queue = candidate
			break
		}
	}
	require.NotEmpty(t, queue)
	worker := registerWorkerForFlowTypes(t, servers[0], queue, "worker-ring", []string{queue})
	request := dispatchRequest(queue, "", 0, time.Second)
	dispatch, err := dexpb.NewMatchingServiceClient(servers[2].connection).DispatchTask(context.Background(), request)
	require.NoError(t, err)
	assigned, err := worker.Recv()
	require.NoError(t, err)
	require.NoError(t, worker.Send(&dexpb.PollWorkerTaskRequest{Frame: &dexpb.PollWorkerTaskRequest_TaskFrame{TaskFrame: &dexpb.PollWorkerTaskFrame{TaskId: assigned.GetTask().TaskId, Output: &dexpb.PollWorkerTaskFrame_Rpc{Rpc: &dexpb.InvokeWorkerRPCResponse{}}}}}))
	frames := receiveDispatch(t, dispatch)
	require.Len(t, frames, 1)
	require.Equal(t, "worker-ring", frames[0].ServedByAppWorkerId)
	servers[1].engine.Close()
	reconnect, err := worker.Recv()
	require.NoError(t, err)
	require.NotNil(t, reconnect.GetReconnect())
	monitors[1].Stop()
	require.Eventually(t, func() bool {
		return len(monitors[0].Members(membership.RoleMatching)) == 2 && len(monitors[2].Members(membership.RoleMatching)) == 2
	}, 10*time.Second, 20*time.Millisecond)
	replacement := registerWorkerForFlowTypes(t, servers[0], queue, "worker-ring", []string{queue})
	require.NotNil(t, replacement)
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
	stream, err := dexpb.NewWorkerTaskServiceClient(server.connection).PollWorkerTask(context.Background())
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
	for _, taskID := range []string{"affinity-1", "affinity-2"} {
		request := dispatchRequest("flow", "", 0, time.Second)
		request.TaskId = taskID
		dispatch, err := dexpb.NewMatchingServiceClient(server.connection).DispatchTask(context.Background(), request)
		require.NoError(t, err)
		assigned, err := first.Recv()
		require.NoError(t, err)
		require.Equal(t, taskID, assigned.GetTask().TaskId)
		require.NoError(t, first.Send(&dexpb.PollWorkerTaskRequest{Frame: &dexpb.PollWorkerTaskRequest_TaskFrame{TaskFrame: &dexpb.PollWorkerTaskFrame{TaskId: taskID, Output: &dexpb.PollWorkerTaskFrame_Rpc{Rpc: &dexpb.InvokeWorkerRPCResponse{}}}}}))
		require.Len(t, receiveDispatch(t, dispatch), 1)
	}
}

func TestMatchingReconnectAfterTaskIntegration(t *testing.T) {
	members := newTestMembership()
	server := startTestServer(t, config.MatchingConfig{}, members)
	worker := registerWorker(t, server, "flow", "worker-drain")
	dispatch, err := dexpb.NewMatchingServiceClient(server.connection).DispatchTask(context.Background(), dispatchRequest("flow", "", 0, time.Second))
	require.NoError(t, err)
	assigned, err := worker.Recv()
	require.NoError(t, err)
	members.mu.Lock()
	members.owners["flow"] = membership.HostInfo{RPCAddress: "127.0.0.1:1"}
	members.mu.Unlock()
	members.changes <- membership.Change{Role: membership.RoleMatching}
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
	case <-time.After(30 * time.Millisecond):
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
	dispatch, err := dexpb.NewMatchingServiceClient(first.connection).DispatchTask(context.Background(), dispatchRequest("flow", "", 0, time.Second))
	require.NoError(t, err)
	_, err = dispatch.Recv()
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	poll, err := dexpb.NewWorkerTaskServiceClient(first.connection).PollWorkerTask(context.Background())
	require.NoError(t, err)
	require.NoError(t, poll.Send(&dexpb.PollWorkerTaskRequest{Frame: &dexpb.PollWorkerTaskRequest_Register{Register: &dexpb.PollWorkerTaskRegister{Queue: "flow", AppWorkerId: "worker-loop", FlowTypes: []string{"flow"}}}}))
	_, err = poll.Recv()
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
}
