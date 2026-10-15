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
	"net"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/superdurable/dex/config"
	"github.com/superdurable/dex/gen/dexpb"
	"github.com/superdurable/dex/service"
)

func TestGeneralWakeTemporal(t *testing.T) {
	if !*temporalIntegTest || *dexServerAddress != "" {
		t.Skip()
	}
	testGeneralWake(t, service.BackendTypeTemporal, false)
}

func TestGeneralWakeCadence(t *testing.T) {
	if !*cadenceIntegTest || *dexServerAddress != "" {
		t.Skip()
	}
	testGeneralWake(t, service.BackendTypeCadence, false)
}

func TestLegacySingleWakeTemporal(t *testing.T) {
	if !*temporalIntegTest || *dexServerAddress != "" {
		t.Skip()
	}
	testGeneralWake(t, service.BackendTypeTemporal, true)
}

func TestLegacySingleWakeCadence(t *testing.T) {
	if !*cadenceIntegTest || *dexServerAddress != "" {
		t.Skip()
	}
	testGeneralWake(t, service.BackendTypeCadence, true)
}

func testGeneralWake(t *testing.T, backend service.BackendType, isLegacy bool) {
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
	if isLegacy {
		testConfig.Wake = nil
		testConfig.Tenancy = &config.TenancyConfig{Wake: config.TenantWakeConfig{URL: wakeServer.URL, Budget: 60 * time.Second}}
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
