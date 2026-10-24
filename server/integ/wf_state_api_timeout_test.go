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
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/superdurable/dex/gen/dexpb"
	"github.com/superdurable/dex/integ/workflow/wf_state_api_timeout"
	"github.com/superdurable/dex/service"
)

func TestStateApiTimeoutTemporal(t *testing.T) {
	if !*temporalIntegTest {
		t.Skip()
	}
	for i := 0; i < *repeatIntegTest; i++ {
		doTestStateApiTimeout(t, service.BackendTypeTemporal, nil)
		smallWaitForFastTest()
		doTestStateApiTimeout(t, service.BackendTypeTemporal, minimumContinueAsNewSyncDurabilityConfig())
		smallWaitForFastTest()
	}
}

func TestStateApiTimeoutCadence(t *testing.T) {
	if !*cadenceIntegTest {
		t.Skip()
	}
	for i := 0; i < *repeatIntegTest; i++ {
		doTestStateApiTimeout(t, service.BackendTypeCadence, nil)
		smallWaitForFastTest()
		doTestStateApiTimeout(t, service.BackendTypeCadence, minimumContinueAsNewSyncDurabilityConfig())
		smallWaitForFastTest()
	}
}

func doTestStateApiTimeout(
	t *testing.T,
	backendType service.BackendType,
	flowConfig *dexpb.FlowConfig,
) {
	workerHandler := wf_state_api_timeout.NewHandler()
	workerTarget := startWorker(t, workerHandler)
	runtime := startDexService(t, DexServiceTestConfig{
		BackendType: backendType,
	})
	flowClient := runtime.FlowClient

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	flowId := wf_state_api_timeout.FlowType + uuid.NewString()
	startRequest := &dexpb.StartFlowRequest{
		RequestId:          newRequestID(),
		FlowId:             flowId,
		FlowType:           wf_state_api_timeout.FlowType,
		FlowTimeoutSeconds: 10,

		StartStepType: wf_state_api_timeout.Step1,
		StepOptions: &dexpb.StepOptions{
			WaitForTimeoutSeconds: 1,
			WaitForRetryPolicy: &dexpb.RetryPolicy{
				MaximumAttempts: 1,
			},
		},
		FlowStartOptions: withWorkerTarget(nil, workerTarget),
	}
	if flowConfig != nil {
		startRequest.FlowStartOptions = withWorkerTarget(&dexpb.FlowStartOptions{
			FlowConfigOverride: flowConfig,
		}, workerTarget)
	}
	_, err := flowClient.StartFlow(ctx, startRequest)
	require.NoError(t, err)

	resp, err := flowClient.WaitForFlow(ctx, &dexpb.WaitForFlowRequest{
		FlowId: flowId,
	})
	require.NoError(t, err)

	history := workerHandler.GetTestResult().InvokeHistory
	require.Equal(t, map[string]int64{
		"S1_waitFor": 1,
	}, history)

	require.Equal(t, dexpb.FlowStatus_FLOW_STATUS_FAILED, resp.GetFlowStatus())
	require.Equal(
		t,
		dexpb.FlowErrorType_FLOW_ERROR_TYPE_WORKER_API_FAIL,
		resp.GetErrorType(),
	)
	switch {
	case isServerZDBinaryMode():
		require.Contains(t, resp.GetErrorMessage(), "method timeout of 1s expired")
	case backendType == service.BackendTypeTemporal:
		require.Contains(t, resp.GetErrorMessage(), "activity StartToClose timeout")
	case backendType == service.BackendTypeCadence:
		// Cadence may surface StartToClose or cancel the in-flight gRPC as RST_STREAM.
		msg := resp.GetErrorMessage()
		require.Truef(
			t,
			msg == "TimeoutType: START_TO_CLOSE" ||
				msg == "stream terminated by RST_STREAM with error code: CANCEL",
			"unexpected Cadence timeout message: %q",
			msg,
		)
	default:
		t.Fatalf("unexpected backend type %v", backendType)
	}
}
