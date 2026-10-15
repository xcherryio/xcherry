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
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/superdurable/dex/config"
	"github.com/superdurable/dex/gen/dexpb"
	"github.com/superdurable/dex/integ/workflow/common"
	"github.com/superdurable/dex/service"
	"google.golang.org/grpc"
)

func TestGeneralWakeTemporal(t *testing.T) {
	if !*temporalIntegTest || *dexServerAddress != "" {
		t.Skip()
	}
	testGeneralWake(t, service.BackendTypeTemporal)
}

func TestGeneralWakeCadence(t *testing.T) {
	if !*cadenceIntegTest || *dexServerAddress != "" {
		t.Skip()
	}
	testGeneralWake(t, service.BackendTypeCadence)
}

func testGeneralWake(t *testing.T, backend service.BackendType) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	recorder := &wakeRecorder{}
	wakeServer := httptest.NewServer(recorder)
	t.Cleanup(wakeServer.Close)
	reservation, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	idleWorkerAddress := reservation.Addr().String()
	require.NoError(t, reservation.Close())
	testConfig := DexServiceTestConfig{
		BackendType: backend,
		Wake:        &config.WakeConfig{URL: wakeServer.URL, Budget: 60 * time.Second},
	}
	runtime := startInProcessDexService(t, testConfig)
	flowID := "wake-" + uuid.NewString()
	require.NoError(t, startDispatchProbe(ctx, runtime.FlowClient, flowID, dispatchProbeStep,
		&dexpb.WorkerTarget{Address: idleWorkerAddress}, nil))
	require.Eventually(t, func() bool { return recorder.count("wake") > 0 }, 20*time.Second, 100*time.Millisecond)
	recorder.mu.Lock()
	require.Equal(t, map[string]string{"target": idleWorkerAddress, "kind": "wake"}, recorder.notifications[0])
	require.Empty(t, recorder.credentials[0])
	recorder.mu.Unlock()
	worker := newDispatchProbeHandler()
	startStreamingWorkerOnAddress(t, worker, idleWorkerAddress)
	require.Eventually(t, func() bool { return len(worker.observedWaitFors()) == 1 }, 30*time.Second, 100*time.Millisecond)
	_, err = runtime.FlowClient.PublishToChannel(ctx, &dexpb.PublishToChannelRequest{
		FlowId: flowID, Messages: []*dexpb.ChannelMessage{{ChannelName: dispatchProbeChannel}},
	})
	require.NoError(t, err)
	result, err := runtime.FlowClient.WaitForFlow(ctx, &dexpb.WaitForFlowRequest{FlowId: flowID, WaitTimeSeconds: 30})
	require.NoError(t, err)
	require.Equal(t, dexpb.FlowStatus_FLOW_STATUS_COMPLETED, result.GetFlowStatus())
}

const (
	dispatchProbeFlowType = "DispatchProbeFlow"
	dispatchProbeStep     = "DispatchStep"
	dispatchProbeChannel  = "proceed"
)

type dispatchProbeHandler struct {
	dexpb.UnimplementedWorkerServiceServer
	mu       sync.Mutex
	waitFors []*dexpb.Context
}

func newDispatchProbeHandler() *dispatchProbeHandler { return &dispatchProbeHandler{} }

func (h *dispatchProbeHandler) InvokeWaitForMethod(request *dexpb.InvokeWaitForMethodRequest, stream dexpb.WorkerService_InvokeWaitForMethodServer) error {
	h.mu.Lock()
	h.waitFors = append(h.waitFors, request.GetContext())
	h.mu.Unlock()
	return stream.Send(&dexpb.InvokeWaitForMethodOutput{Output: &dexpb.InvokeWaitForMethodOutput_Result{Result: &dexpb.InvokeWaitForMethodResponse{WaitingCondition: &dexpb.WaitingCondition{
		WaitingConditionType: dexpb.WaitingConditionType_WAITING_CONDITION_TYPE_ALL_COMPLETED,
		ChannelConditions:    []*dexpb.ChannelCondition{{ChannelName: dispatchProbeChannel}},
	}}}})
}

func (h *dispatchProbeHandler) InvokeExecuteMethod(request *dexpb.InvokeExecuteMethodRequest, stream dexpb.WorkerService_InvokeExecuteMethodServer) error {
	return stream.Send(&dexpb.InvokeExecuteMethodOutput{Output: &dexpb.InvokeExecuteMethodOutput_Result{Result: &dexpb.InvokeExecuteMethodResponse{
		StepDecision: &dexpb.StepDecision{CloseDecision: common.GracefulCompleteDecision(stringValue(request.GetContext().GetFlowId()))},
	}}})
}

func (h *dispatchProbeHandler) observedWaitFors() []*dexpb.Context {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]*dexpb.Context(nil), h.waitFors...)
}

func startDispatchProbe(ctx context.Context, client dexpb.FlowServiceClient, flowID string, startStep string, workerTarget *dexpb.WorkerTarget, stepOptions *dexpb.StepOptions) error {
	_, err := client.StartFlow(ctx, &dexpb.StartFlowRequest{
		RequestId: newRequestID(), FlowId: flowID, FlowType: dispatchProbeFlowType,
		FlowTimeoutSeconds: 120, StartStepType: startStep, StepOptions: stepOptions,
		FlowStartOptions: withWorkerTarget(nil, workerTarget),
	})
	return err
}

type wakeRecorder struct {
	mu            sync.Mutex
	notifications []map[string]string
	credentials   []string
}

func (r *wakeRecorder) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	body, err := io.ReadAll(request.Body)
	if err != nil {
		writer.WriteHeader(http.StatusBadRequest)
		return
	}
	notification := map[string]string{}
	if err := json.Unmarshal(body, &notification); err != nil {
		writer.WriteHeader(http.StatusBadRequest)
		return
	}
	r.mu.Lock()
	r.notifications = append(r.notifications, notification)
	r.credentials = append(r.credentials, request.Header.Get("Authorization"))
	r.mu.Unlock()
	writer.WriteHeader(http.StatusNoContent)
}

func (r *wakeRecorder) count(kind string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	count := 0
	for _, notification := range r.notifications {
		if notification["kind"] == kind {
			count++
		}
	}
	return count
}

func startStreamingWorkerOnAddress(t *testing.T, handler dexpb.WorkerServiceServer, address string) {
	t.Helper()
	listener, err := net.Listen("tcp", address)
	require.NoError(t, err)
	server := grpc.NewServer()
	dexpb.RegisterWorkerServiceServer(server, handler)
	go serveStreamingWorker(t, server, listener)
	t.Cleanup(server.GracefulStop)
}

func serveStreamingWorker(t *testing.T, server *grpc.Server, listener net.Listener) {
	if err := server.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
		t.Errorf("serve Worker: %v", err)
	}
}
