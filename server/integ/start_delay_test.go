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
	"github.com/superdurable/dex/integ/workflow/basic"
	"github.com/superdurable/dex/service"
)

func TestStartDelayTemporal(t *testing.T) {
	if !*temporalIntegTest {
		t.Skip()
	}
	// C8: server-zd rejects a Flow start delay.
	requireGoServerSetting(t, "Flow start delay")
	doTestStartDelay(t, service.BackendTypeTemporal, nil)
}

func TestStartDelayCadence(t *testing.T) {
	if !*cadenceIntegTest {
		t.Skip()
	}
	doTestStartDelay(t, service.BackendTypeCadence, nil)
}

func doTestStartDelay(
	t *testing.T,
	backendType service.BackendType,
	flowConfig *dexpb.FlowConfig,
) {
	workerTarget := startWorker(t, basic.NewHandler())
	runtime := startDexService(t, DexServiceTestConfig{BackendType: backendType})
	flowClient := runtime.FlowClient

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	flowId := basic.FlowType + "-" + uuid.NewString()
	stepInput := encodedObjectValue("json", []byte("test data"))
	timeSentReq := time.Now()
	_, err := flowClient.StartFlow(ctx, &dexpb.StartFlowRequest{
		RequestId:          newRequestID(),
		FlowId:             flowId,
		FlowType:           basic.FlowType,
		FlowTimeoutSeconds: 100,

		StartStepType: basic.Step1,
		StepInput:     stepInput,
		FlowStartOptions: withWorkerTarget(&dexpb.FlowStartOptions{
			FlowStartDelaySeconds: 10,
			FlowConfigOverride:    flowConfig,
			IdReusePolicy:         dexpb.IdReusePolicy_ID_REUSE_POLICY_DISALLOW_REUSE,
			RetryPolicy: &dexpb.FlowRetryPolicy{
				InitialIntervalSeconds: 11,
				BackoffCoefficient:     11,
				MaximumAttempts:        11,
				MaximumIntervalSeconds: 11,
			},
		}, workerTarget),

		StepOptions: &dexpb.StepOptions{
			WaitForTimeoutSeconds: 12,
			ExecuteTimeoutSeconds: 13,
			WaitForRetryPolicy: &dexpb.RetryPolicy{
				InitialIntervalSeconds: 12,
				BackoffCoefficient:     12,
				MaximumAttempts:        12,
				MaximumIntervalSeconds: 12,
			},
			ExecuteRetryPolicy: &dexpb.RetryPolicy{
				InitialIntervalSeconds: 13,
				BackoffCoefficient:     13,
				MaximumAttempts:        13,
				MaximumIntervalSeconds: 13,
			},
		},
	})
	require.NoError(t, err)

	time.Sleep(5 * time.Second)
	_, err = flowClient.WaitForFlow(ctx, &dexpb.WaitForFlowRequest{
		FlowId:          flowId,
		WaitTimeSeconds: 20,
	})
	require.NoError(t, err)

	delay := time.Since(timeSentReq)
	require.True(t, delay.Seconds() > 8, "delay is %v", delay)
	require.True(t, delay.Seconds() < 12, "delay is %v", delay)
}
