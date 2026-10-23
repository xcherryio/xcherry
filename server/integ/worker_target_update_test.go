// Legacy Materials in this file remain under their original licenses.
// See LEGACY_NOTICES.md.

// Modifications Copyright (c) 2026 Super Durable, Inc.
//
// Modifications after the Legacy Cutoff are licensed under the
// Sustainable Use License 1.0.
// Legacy Materials remain under their original licenses.
// See LICENSE and LEGACY_NOTICES.md.

package integ

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/superdurable/dex/gen/dexpb"
	"github.com/superdurable/dex/integ/workflow/signal"
	"github.com/superdurable/dex/service"
)

func TestUpdateWorkerTargetTemporal(t *testing.T) {
	if !*temporalIntegTest {
		t.Skip()
	}
	testUpdateWorkerTarget(t, service.BackendTypeTemporal)
}

func TestUpdateWorkerTargetCadence(t *testing.T) {
	if !*cadenceIntegTest {
		t.Skip()
	}
	testUpdateWorkerTarget(t, service.BackendTypeCadence)
}

func testUpdateWorkerTarget(t *testing.T, backendType service.BackendType) {
	requirePushWorkers(t, "switching addresses between Workers for one Flow type")
	firstHandler := signal.NewHandler()
	firstTarget := startWorker(t, firstHandler)
	secondHandler := signal.NewHandler()
	secondTarget := startWorker(t, secondHandler)
	runtime := startDexService(t, DexServiceTestConfig{BackendType: backendType})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	flowID := "worker-target-update-" + uuid.NewString()
	_, err := runtime.FlowClient.StartFlow(ctx, &dexpb.StartFlowRequest{
		RequestId:          newRequestID(),
		FlowId:             flowID,
		FlowType:           signal.WorkflowType,
		FlowTimeoutSeconds: 20,
		StartStepType:      signal.State1,
		FlowStartOptions:   withWorkerTarget(nil, firstTarget),
	})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return firstHandler.GetTestResult().InvokeHistory["S1_waitFor"] == 1
	}, 10*time.Second, 20*time.Millisecond)

	_, err = runtime.FlowClient.UpdateFlowConfig(ctx, &dexpb.UpdateFlowConfigRequest{
		FlowId: flowID,
		FlowConfig: &dexpb.FlowConfig{
			WorkerTarget: secondTarget,
		},
	})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		var dump dexpb.DebugDumpResponse
		queryErr := runtime.UnifiedClient.QueryWorkflow(
			ctx,
			&dump,
			flowID,
			"",
			service.DebugDumpQueryType,
		)
		return queryErr == nil &&
			dump.GetConfig().GetWorkerTarget().GetAddress() == secondTarget.GetAddress()
	}, 10*time.Second, 20*time.Millisecond)

	for index := 0; index < 4; index++ {
		_, err = runtime.FlowClient.PublishToChannel(ctx, &dexpb.PublishToChannelRequest{
			FlowId: flowID,
			Messages: []*dexpb.ChannelMessage{
				{
					ChannelName: signal.SignalName,
					Value:       stringValue(fmt.Sprintf("signal-%d", index)),
				},
			},
		})
		require.NoError(t, err)
	}
	_, err = runtime.FlowClient.WaitForFlow(ctx, &dexpb.WaitForFlowRequest{FlowId: flowID})
	require.NoError(t, err)

	require.Equal(t, map[string]int64{"S1_waitFor": 1}, firstHandler.GetTestResult().InvokeHistory)
	require.Equal(t, map[string]int64{
		"S1_execute": 1,
		"S2_waitFor": 1,
		"S2_execute": 1,
	}, secondHandler.GetTestResult().InvokeHistory)
}
