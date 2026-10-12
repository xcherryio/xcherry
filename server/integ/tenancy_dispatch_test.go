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
	"github.com/superdurable/dex/service/tenancy"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	dispatchProbeFlowType = "DispatchProbeFlow"
	dispatchProbeStep     = "DispatchStep"
	dispatchProbeChannel  = "proceed"
	dispatchProbeSubFlow  = "DispatchSubFlowStep"
)

// dispatchProbeHandler records every dispatch it receives and can hold Executes until released.
type dispatchProbeHandler struct {
	dexpb.UnimplementedWorkerServiceServer
	subFlowTarget *dexpb.WorkerTarget
	release       chan struct{}

	mu         sync.Mutex
	waitFors   []*dexpb.Context
	executes   []*dexpb.Context
	running    int
	maxRunning int
}

func newDispatchProbeHandler() *dispatchProbeHandler {
	return &dispatchProbeHandler{}
}

func (h *dispatchProbeHandler) InvokeWaitForMethod(
	request *dexpb.InvokeWaitForMethodRequest,
	stream dexpb.WorkerService_InvokeWaitForMethodServer,
) error {
	h.mu.Lock()
	h.waitFors = append(h.waitFors, request.GetContext())
	h.mu.Unlock()
	response := &dexpb.InvokeWaitForMethodResponse{WaitingCondition: &dexpb.WaitingCondition{
		WaitingConditionType: dexpb.WaitingConditionType_WAITING_CONDITION_TYPE_ALL_COMPLETED,
		ChannelConditions:    []*dexpb.ChannelCondition{{ChannelName: dispatchProbeChannel}},
	}}
	if request.GetStepType() == dispatchProbeSubFlow {
		response.WaitingCondition = &dexpb.WaitingCondition{
			WaitingConditionType: dexpb.WaitingConditionType_WAITING_CONDITION_TYPE_ALL_COMPLETED,
			SubFlowConditions: []*dexpb.SubFlowCondition{{
				SubFlowType:   dispatchProbeFlowType,
				StartStepType: dispatchProbeStep,
				StepOptions:   &dexpb.StepOptions{SkipWaitFor: true},
				Options: &dexpb.SubFlowOptions{
					FlowConfigOverride: &dexpb.FlowConfig{WorkerTarget: h.subFlowTarget},
				},
			}},
		}
	}
	return stream.Send(&dexpb.InvokeWaitForMethodOutput{
		Output: &dexpb.InvokeWaitForMethodOutput_Result{Result: response},
	})
}

func (h *dispatchProbeHandler) InvokeExecuteMethod(
	request *dexpb.InvokeExecuteMethodRequest,
	stream dexpb.WorkerService_InvokeExecuteMethodServer,
) error {
	h.mu.Lock()
	h.executes = append(h.executes, request.GetContext())
	h.running++
	h.maxRunning = max(h.maxRunning, h.running)
	release := h.release
	h.mu.Unlock()
	defer func() {
		h.mu.Lock()
		h.running--
		h.mu.Unlock()
	}()
	if release != nil {
		select {
		case <-release:
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}
	return stream.Send(&dexpb.InvokeExecuteMethodOutput{
		Output: &dexpb.InvokeExecuteMethodOutput_Result{Result: &dexpb.InvokeExecuteMethodResponse{
			StepDecision: &dexpb.StepDecision{
				CloseDecision: common.GracefulCompleteDecision(stringValue(request.GetContext().GetFlowId())),
			},
		}},
	})
}

func (h *dispatchProbeHandler) observedWaitFors() []*dexpb.Context {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]*dexpb.Context(nil), h.waitFors...)
}

func (h *dispatchProbeHandler) observedExecutes() []*dexpb.Context {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]*dexpb.Context(nil), h.executes...)
}

func (h *dispatchProbeHandler) concurrency() (int, int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.running, h.maxRunning
}

// dispatchTenants is a multi-tenant server whose static-token tenants each own one Worker address.
type dispatchTenants struct {
	runtime                 *integRuntime
	alpha, beta             dexpb.FlowServiceClient
	alphaWorker, betaWorker *dispatchProbeHandler
	alphaTarget, betaTarget *dexpb.WorkerTarget
	environment             *tenancyTestEnvironment
}

func startDispatchTenants(
	t *testing.T,
	configure func(*config.TenancyConfig),
	alphaWorker *dispatchProbeHandler,
	alphaAddress string,
) *dispatchTenants {
	t.Helper()
	tenants := &dispatchTenants{alphaWorker: alphaWorker, betaWorker: newDispatchProbeHandler()}
	if alphaAddress == "" {
		tenants.alphaTarget = startStreamingWorker(t, alphaWorker)
	} else {
		tenants.alphaTarget = &dexpb.WorkerTarget{Address: alphaAddress}
	}
	tenants.betaTarget = startStreamingWorker(t, tenants.betaWorker)
	tenants.environment = newTenancyTestEnvironment(t)
	alphaToken := tenants.environment.AddStaticTokenWithClaims(t, "alpha",
		map[string]string{"worker": tenants.alphaTarget.GetAddress()})
	betaToken := tenants.environment.AddStaticTokenWithClaims(t, "beta",
		map[string]string{"worker": tenants.betaTarget.GetAddress()})
	tenants.environment.Config.WorkerTarget = config.TenantWorkerTargetConfig{
		Default: "{{ .worker }}",
		Allowed: "{{ quote .worker }}",
	}
	if configure != nil {
		configure(tenants.environment.Config)
	}
	tenants.runtime = startDexService(t, DexServiceTestConfig{
		BackendType: service.BackendTypeTemporal,
		Tenancy:     tenants.environment.Config,
		Credential:  alphaToken,
	})
	tenants.alpha = tenants.runtime.FlowClient
	tenants.beta = newFlowClientWithCredential(t, tenants.runtime.PublicAddress, betaToken)
	return tenants
}

func startDispatchProbe(
	ctx context.Context,
	client dexpb.FlowServiceClient,
	flowID string,
	startStep string,
	workerTarget *dexpb.WorkerTarget,
	stepOptions *dexpb.StepOptions,
) error {
	_, err := client.StartFlow(ctx, &dexpb.StartFlowRequest{
		RequestId:          newRequestID(),
		FlowId:             flowID,
		FlowType:           dispatchProbeFlowType,
		FlowTimeoutSeconds: 120,
		StartStepType:      startStep,
		StepOptions:        stepOptions,
		FlowStartOptions:   withWorkerTarget(nil, workerTarget),
	})
	return err
}

func TestTenantWorkerTargetTemporal(t *testing.T) {
	if !*temporalIntegTest || *dexServerAddress != "" {
		t.Skip()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	tenants := startDispatchTenants(t, nil, newDispatchProbeHandler(), "")
	requirePermissionDenied := func(t *testing.T, err error) {
		t.Helper()
		require.Equal(t, codes.PermissionDenied, status.Code(err), "error: %v", err)
	}

	t.Run("a start without a target uses the tenant's default Worker", func(t *testing.T) {
		flowID := "default-target-" + uuid.NewString()
		require.NoError(t, startDispatchProbe(ctx, tenants.alpha, flowID, dispatchProbeStep, nil, nil))
		require.Eventually(t, func() bool {
			for _, observed := range tenants.alphaWorker.observedWaitFors() {
				if observed.GetFlowId() == flowID {
					return true
				}
			}
			return false
		}, 20*time.Second, 100*time.Millisecond)
	})

	t.Run("another tenant's target is refused at start and update", func(t *testing.T) {
		requirePermissionDenied(t, startDispatchProbe(
			ctx, tenants.alpha, "foreign-target-"+uuid.NewString(), dispatchProbeStep, tenants.betaTarget, nil,
		))
		flowID := "update-target-" + uuid.NewString()
		require.NoError(t, startDispatchProbe(ctx, tenants.alpha, flowID, dispatchProbeStep, tenants.alphaTarget, nil))
		_, err := tenants.alpha.UpdateFlowConfig(ctx, &dexpb.UpdateFlowConfigRequest{
			FlowId: flowID, FlowConfig: &dexpb.FlowConfig{WorkerTarget: tenants.betaTarget},
		})
		requirePermissionDenied(t, err)
	})

	t.Run("a SubFlow targeting another tenant's Worker fails its Step", func(t *testing.T) {
		tenants.alphaWorker.subFlowTarget = tenants.betaTarget
		flowID := "subflow-target-" + uuid.NewString()
		require.NoError(t, startDispatchProbe(ctx, tenants.alpha, flowID, dispatchProbeSubFlow, tenants.alphaTarget, nil))
		result, err := tenants.alpha.WaitForFlow(ctx, &dexpb.WaitForFlowRequest{FlowId: flowID, WaitTimeSeconds: 30})
		require.NoError(t, err)
		require.Equal(t, dexpb.FlowStatus_FLOW_STATUS_FAILED, result.GetFlowStatus())
		require.Empty(t, tenants.betaWorker.observedExecutes(), "beta's Worker never receives alpha's SubFlow")
	})
}

// TestTenantDispatchRecheckTemporal shows a persisted target is matched again at dispatch after a policy change.
func TestTenantDispatchRecheckTemporal(t *testing.T) {
	if !*temporalIntegTest || *dexServerAddress != "" {
		t.Skip()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	betaWorker := newDispatchProbeHandler()
	betaTarget := startStreamingWorker(t, betaWorker)
	flowID := "recheck-" + uuid.NewString()

	t.Run("a permissive server dispatches alpha's WaitFor to the target", func(t *testing.T) {
		tenants := startDispatchTenants(t, func(cfg *config.TenancyConfig) {
			cfg.WorkerTarget.Allowed = `127\.0\.0\.1:[0-9]+`
		}, newDispatchProbeHandler(), "")
		require.NoError(t, startDispatchProbe(ctx, tenants.alpha, flowID, dispatchProbeStep, betaTarget, nil))
		require.Eventually(t, func() bool { return len(betaWorker.observedWaitFors()) == 1 },
			20*time.Second, 100*time.Millisecond)
	})

	t.Run("a strict server refuses the next dispatch of the running Flow", func(t *testing.T) {
		tenants := startDispatchTenants(t, nil, newDispatchProbeHandler(), "")
		_, err := tenants.alpha.PublishToChannel(ctx, &dexpb.PublishToChannelRequest{
			FlowId: flowID, Messages: []*dexpb.ChannelMessage{{ChannelName: dispatchProbeChannel}},
		})
		require.NoError(t, err)
		result, err := tenants.alpha.WaitForFlow(ctx, &dexpb.WaitForFlowRequest{FlowId: flowID, WaitTimeSeconds: 30})
		require.NoError(t, err)
		require.Equal(t, dexpb.FlowStatus_FLOW_STATUS_FAILED, result.GetFlowStatus())
		require.Empty(t, betaWorker.observedExecutes(), "the Execute never reaches the disallowed Worker")
	})
}

func TestTenantDispatchCapTemporal(t *testing.T) {
	if !*temporalIntegTest || *dexServerAddress != "" {
		t.Skip()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	alphaWorker := newDispatchProbeHandler()
	alphaWorker.release = make(chan struct{})
	tenants := startDispatchTenants(t, func(cfg *config.TenancyConfig) {
		cfg.Limits.MaxConcurrentDispatchesPerTenant = 2
	}, alphaWorker, "")
	skipWaitFor := &dexpb.StepOptions{SkipWaitFor: true}

	alphaFlowIDs := []string{}
	for i := 0; i < 4; i++ {
		flowID := "capped-" + uuid.NewString()
		alphaFlowIDs = append(alphaFlowIDs, flowID)
		require.NoError(t, startDispatchProbe(ctx, tenants.alpha, flowID, dispatchProbeStep, nil, skipWaitFor))
	}
	require.Eventually(t, func() bool {
		running, _ := alphaWorker.concurrency()
		return running == 2
	}, 30*time.Second, 100*time.Millisecond)
	require.Never(t, func() bool {
		_, maxRunning := alphaWorker.concurrency()
		return maxRunning > 2
	}, 3*time.Second, 100*time.Millisecond, "one tenant never holds more than its dispatch cap")

	betaFlowID := "uncapped-" + uuid.NewString()
	require.NoError(t, startDispatchProbe(ctx, tenants.beta, betaFlowID, dispatchProbeStep, nil, skipWaitFor))
	betaResult, err := tenants.beta.WaitForFlow(ctx, &dexpb.WaitForFlowRequest{FlowId: betaFlowID, WaitTimeSeconds: 30})
	require.NoError(t, err)
	require.Equal(t, dexpb.FlowStatus_FLOW_STATUS_COMPLETED, betaResult.GetFlowStatus(),
		"another tenant's Steps run while one tenant is at its cap")

	close(alphaWorker.release)
	for _, flowID := range alphaFlowIDs {
		result, err := tenants.alpha.WaitForFlow(ctx, &dexpb.WaitForFlowRequest{FlowId: flowID, WaitTimeSeconds: 30})
		require.NoError(t, err)
		require.Equal(t, dexpb.FlowStatus_FLOW_STATUS_COMPLETED, result.GetFlowStatus())
	}
	for _, executed := range alphaWorker.observedExecutes() {
		require.Equal(t, int32(1), executed.GetAttempt(), "waiting for a slot counts no attempt")
	}
}

// wakeRecorder is an operator wake endpoint that records every notification.
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

func TestTenantWakeHookTemporal(t *testing.T) {
	if !*temporalIntegTest || *dexServerAddress != "" {
		t.Skip()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	recorder := &wakeRecorder{}
	wakeServer := httptest.NewServer(recorder)
	t.Cleanup(wakeServer.Close)
	reservation, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	idleWorkerAddress := reservation.Addr().String()
	require.NoError(t, reservation.Close())

	alphaWorker := newDispatchProbeHandler()
	tenants := startDispatchTenants(t, func(cfg *config.TenancyConfig) {
		cfg.Wake = config.TenantWakeConfig{URL: wakeServer.URL, Budget: 60 * time.Second}
	}, alphaWorker, idleWorkerAddress)

	flowID := "wake-" + uuid.NewString()
	require.NoError(t, startDispatchProbe(ctx, tenants.alpha, flowID, dispatchProbeStep, nil, nil))
	require.Eventually(t, func() bool { return recorder.count("wake") > 0 }, 20*time.Second, 100*time.Millisecond)
	recorder.mu.Lock()
	require.Equal(t, map[string]string{"tenant": "alpha", "target": idleWorkerAddress, "kind": "wake"},
		recorder.notifications[0])
	require.Equal(t, "Bearer "+tenants.environment.InternalToken, recorder.credentials[0])
	recorder.mu.Unlock()
	require.Never(t, func() bool { return recorder.count("wake") > 1 }, 5*time.Second, 200*time.Millisecond,
		"one wake per target per 10 seconds")

	startStreamingWorkerOnAddress(t, alphaWorker, idleWorkerAddress)
	require.Eventually(t, func() bool { return len(alphaWorker.observedWaitFors()) == 1 },
		30*time.Second, 100*time.Millisecond)
	require.Equal(t, int32(1), alphaWorker.observedWaitFors()[0].GetAttempt(), "a held dispatch counts no attempt")
	_, err = tenants.alpha.PublishToChannel(ctx, &dexpb.PublishToChannelRequest{
		FlowId: flowID, Messages: []*dexpb.ChannelMessage{{ChannelName: dispatchProbeChannel}},
	})
	require.NoError(t, err)
	result, err := tenants.alpha.WaitForFlow(ctx, &dexpb.WaitForFlowRequest{FlowId: flowID, WaitTimeSeconds: 30})
	require.NoError(t, err)
	require.Equal(t, dexpb.FlowStatus_FLOW_STATUS_COMPLETED, result.GetFlowStatus())
	require.Eventually(t, func() bool { return recorder.count("active") == 1 }, 10*time.Second, 100*time.Millisecond)
	require.Never(t, func() bool { return recorder.count("active") > 1 }, 2*time.Second, 200*time.Millisecond,
		"active is sent at most once a minute per target")
}

// startStreamingWorkerOnAddress serves handler on a fixed address, such as one a Flow already targets.
func startStreamingWorkerOnAddress(t *testing.T, handler dexpb.WorkerServiceServer, address string) {
	t.Helper()
	listener, err := net.Listen("tcp", address)
	require.NoError(t, err)
	server := grpc.NewServer()
	dexpb.RegisterWorkerServiceServer(server, handler)
	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			t.Errorf("serve Worker: %v", err)
		}
	}()
	t.Cleanup(server.GracefulStop)
}

// TestTenantFairnessKeyTemporal needs -temporalFairness: Temporal 1.25 accepts a fairness key but does not store it.
func TestTenantFairnessKeyTemporal(t *testing.T) {
	if !*temporalIntegTest || !*temporalFairnessIntegTest || *dexServerAddress != "" {
		t.Skip()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	tenants := startDispatchTenants(t, func(cfg *config.TenancyConfig) {
		cfg.Fairness = true
	}, newDispatchProbeHandler(), "")
	flowID := "fair-" + uuid.NewString()
	require.NoError(t, startDispatchProbe(ctx, tenants.alpha, flowID, dispatchProbeStep, nil,
		&dexpb.StepOptions{SkipWaitFor: true}))
	result, err := tenants.alpha.WaitForFlow(ctx, &dexpb.WaitForFlowRequest{FlowId: flowID, WaitTimeSeconds: 30})
	require.NoError(t, err)
	require.Equal(t, dexpb.FlowStatus_FLOW_STATUS_COMPLETED, result.GetFlowStatus())

	temporalClient := createTemporalClient(t, converter.GetDefaultDataConverter(), client.MetricsNopHandler)
	defer temporalClient.Close()
	description, err := temporalClient.DescribeWorkflowExecution(ctx, tenancy.StoredFlowID("alpha", flowID), "")
	require.NoError(t, err)
	require.Equal(t, "alpha", description.GetWorkflowExecutionInfo().GetPriority().GetFairnessKey())
}
