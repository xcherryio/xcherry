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
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/superdurable/dex/gen/dexpb"
	"github.com/superdurable/dex/integ/workflow/basic"
	"github.com/superdurable/dex/service"
	"google.golang.org/grpc"
)

type reconnectingPullTaskServer struct {
	dexpb.UnimplementedWorkerTaskServiceServer
	registrations   atomic.Int32
	isAcknowledging bool
}

func (server *reconnectingPullTaskServer) PollWorkerTask(stream dexpb.WorkerTaskService_PollWorkerTaskServer) error {
	request, err := stream.Recv()
	if err != nil {
		return err
	}
	if request.GetRegister() == nil {
		return fmt.Errorf("expected Worker registration")
	}
	registrationNumber := server.registrations.Add(1)
	if !server.isAcknowledging {
		<-stream.Context().Done()
		return nil
	}
	if err := stream.Send(&dexpb.PollWorkerTaskResponse{Frame: &dexpb.PollWorkerTaskResponse_Registered{Registered: &dexpb.PollWorkerTaskRegistered{}}}); err != nil {
		return err
	}
	if registrationNumber == 1 {
		return stream.Send(&dexpb.PollWorkerTaskResponse{Frame: &dexpb.PollWorkerTaskResponse_Reconnect{Reconnect: &dexpb.PollWorkerTaskReconnect{Reason: "test"}}})
	}
	<-stream.Context().Done()
	return nil
}

func TestPullAdapterReconnects(t *testing.T) {
	server, client := startReconnectTestServer(t, true)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, streamCancel, err := openRegisteredPullStream(ctx, client, "flow", "worker", "flow")
	require.NoError(t, err)
	adapter := &pullAdapterStream{
		stream: stream, streamCancel: streamCancel, pullClient: client,
		adapter: &pullWorkerAdapter{workerID: "worker"}, queue: "flow", flowType: "flow",
	}
	done := make(chan struct{})
	go func() {
		adapter.serve(ctx)
		close(done)
	}()
	require.Eventually(t, func() bool { return server.registrations.Load() >= 2 }, 5*time.Second, 10*time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("pull adapter did not stop after cancellation")
	}
}

func TestPullAdapterRegistrationTimeout(t *testing.T) {
	_, client := startReconnectTestServer(t, false)
	previousTimeout := pullAdapterRegistrationTimeout
	pullAdapterRegistrationTimeout = 50 * time.Millisecond
	defer func() { pullAdapterRegistrationTimeout = previousTimeout }()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, _, err := openRegisteredPullStream(ctx, client, "flow", "worker", "flow")
	require.ErrorContains(t, err, "registration on flow timed out")
}

func startReconnectTestServer(t *testing.T, isAcknowledging bool) (*reconnectingPullTaskServer, dexpb.WorkerTaskServiceClient) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	grpcServer := grpc.NewServer()
	server := &reconnectingPullTaskServer{isAcknowledging: isAcknowledging}
	dexpb.RegisterWorkerTaskServiceServer(grpcServer, server)
	serveErrors := make(chan error, 1)
	go func() { serveErrors <- grpcServer.Serve(listener) }()
	connection, err := newDexClientConnection(listener.Addr().String(), 1024*1024)
	require.NoError(t, err)
	t.Cleanup(func() {
		grpcServer.Stop()
		require.NoError(t, connection.Close())
		serveErr := <-serveErrors
		if serveErr != nil && !errors.Is(serveErr, grpc.ErrServerStopped) {
			t.Errorf("serve fake pull Worker service: %v", serveErr)
		}
	})
	return server, dexpb.NewWorkerTaskServiceClient(connection)
}

func usePullWorkersForTest(t *testing.T) {
	t.Helper()
	previousMode := *integWorkerMode
	*integWorkerMode = "pull"
	t.Cleanup(func() { *integWorkerMode = previousMode })
}

func TestPullAdapterBasicTemporal(t *testing.T) {
	if !*temporalIntegTest {
		t.Skip()
	}
	usePullWorkersForTest(t)
	doTestBasicFlow(t, service.BackendTypeTemporal, nil)
}

func TestPullAdapterUnregisteredSubFlowTemporal(t *testing.T) {
	if !*temporalIntegTest {
		t.Skip()
	}
	usePullWorkersForTest(t)
	doTestFlowTimeoutSubFlowReport(t, service.BackendTypeTemporal)
}

func TestPullAdapterRejectsAmbiguousWorkersTemporal(t *testing.T) {
	if !*temporalIntegTest {
		t.Skip()
	}
	usePullWorkersForTest(t)
	firstTarget := startWorker(t, basic.NewHandler())
	secondTarget := startWorker(t, basic.NewHandler())
	runtime := startDexService(t, DexServiceTestConfig{BackendType: service.BackendTypeTemporal})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	firstFlowID := "ambiguous-worker-" + newRequestID()
	require.NoError(t, startAmbiguousWorkerFlow(ctx, runtime.FlowClient, firstFlowID, firstTarget))
	require.ErrorContains(t, startAmbiguousWorkerFlow(ctx, runtime.FlowClient, "ambiguous-worker-"+newRequestID(), secondTarget), "already registered by another test Worker")
	require.ErrorContains(t, startAmbiguousWorkerFlow(ctx, runtime.FlowClient, "ambiguous-worker-"+newRequestID(), &dexpb.WorkerTarget{IsPullMode: true}), "was not registered by startWorker")
	_, err := runtime.FlowClient.UpdateFlowConfig(ctx, &dexpb.UpdateFlowConfigRequest{
		FlowId: firstFlowID, FlowConfig: &dexpb.FlowConfig{WorkerTarget: secondTarget},
	})
	require.ErrorContains(t, err, "already registered by another test Worker")
}

func startAmbiguousWorkerFlow(ctx context.Context, client dexpb.FlowServiceClient, flowID string, target *dexpb.WorkerTarget) error {
	_, err := client.StartFlow(ctx, &dexpb.StartFlowRequest{
		RequestId: newRequestID(), FlowId: flowID,
		FlowType: basic.FlowType, FlowTimeoutSeconds: 30, StartStepType: basic.Step1,
		FlowStartOptions: withWorkerTarget(nil, target),
	})
	return err
}

func TestPullWaitForAttributeSuccessTemporal(t *testing.T) {
	if !*temporalIntegTest {
		t.Skip()
	}
	usePullWorkersForTest(t)
	requireTemporalOnly(t)
	doTestWaitForAttributeSuccess(t)
}
