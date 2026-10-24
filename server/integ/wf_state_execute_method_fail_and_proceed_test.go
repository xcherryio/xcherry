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
	"github.com/superdurable/dex/integ/workflow/wf_execute_method_fail_and_proceed"
	"github.com/superdurable/dex/service"
)

func TestStateExecuteMethodFailAndProceedTemporal(t *testing.T) {
	if !*temporalIntegTest {
		t.Skip()
	}
	// C21: server-zd rejects Step failure policies.
	requireGoServerSetting(t, "Step failure policies")
	for i := 0; i < *repeatIntegTest; i++ {
		doTestStateExecuteMethodFailAndProceed(
			t,
			service.BackendTypeTemporal,
			nil,
			dexpb.StepDurability_STEP_DURABILITY_UNSPECIFIED,
		)
		smallWaitForFastTest()
		doTestStateExecuteMethodFailAndProceed(
			t,
			service.BackendTypeTemporal,
			minimumContinueAsNewSyncDurabilityConfig(),
			dexpb.StepDurability_STEP_DURABILITY_UNSPECIFIED,
		)
		smallWaitForFastTest()
		doTestStateExecuteMethodFailAndProceed(
			t,
			service.BackendTypeTemporal,
			asyncDurabilityConfig(),
			dexpb.StepDurability_STEP_DURABILITY_UNSPECIFIED,
		)
		smallWaitForFastTest()
		doTestStateExecuteMethodFailAndProceed(
			t,
			service.BackendTypeTemporal,
			syncDurabilityConfig(),
			dexpb.StepDurability_STEP_DURABILITY_ASYNC,
		)
		smallWaitForFastTest()
	}
}

func TestStateExecuteMethodFailAndProceedCadence(t *testing.T) {
	if !*cadenceIntegTest {
		t.Skip()
	}
	for i := 0; i < *repeatIntegTest; i++ {
		doTestStateExecuteMethodFailAndProceed(
			t,
			service.BackendTypeCadence,
			nil,
			dexpb.StepDurability_STEP_DURABILITY_UNSPECIFIED,
		)
		smallWaitForFastTest()
		doTestStateExecuteMethodFailAndProceed(
			t,
			service.BackendTypeCadence,
			minimumContinueAsNewSyncDurabilityConfig(),
			dexpb.StepDurability_STEP_DURABILITY_UNSPECIFIED,
		)
		smallWaitForFastTest()
		doTestStateExecuteMethodFailAndProceed(
			t,
			service.BackendTypeCadence,
			asyncDurabilityConfig(),
			dexpb.StepDurability_STEP_DURABILITY_UNSPECIFIED,
		)
		smallWaitForFastTest()
		doTestStateExecuteMethodFailAndProceed(
			t,
			service.BackendTypeCadence,
			syncDurabilityConfig(),
			dexpb.StepDurability_STEP_DURABILITY_ASYNC,
		)
		smallWaitForFastTest()
	}
}

func TestStateExecuteTimeoutRecoveryTemporal(t *testing.T) {
	if !*temporalIntegTest {
		t.Skip()
	}
	// C21: server-zd rejects Step failure policies.
	requireGoServerSetting(t, "Step failure policies")
	doTestStateExecuteTimeoutRecovery(t, service.BackendTypeTemporal)
}

func TestStateExecuteTimeoutRecoveryCadence(t *testing.T) {
	if !*cadenceIntegTest {
		t.Skip()
	}
	doTestStateExecuteTimeoutRecovery(t, service.BackendTypeCadence)
}

func doTestStateExecuteTimeoutRecovery(t *testing.T, backendType service.BackendType) {
	workerHandler := wf_execute_method_fail_and_proceed.NewHandler()
	workerTarget := startWorker(t, workerHandler)
	runtime := startDexService(t, DexServiceTestConfig{BackendType: backendType})
	flowClient := runtime.FlowClient

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	flowId := wf_execute_method_fail_and_proceed.FlowType + uuid.NewString()
	_, err := flowClient.StartFlow(ctx, &dexpb.StartFlowRequest{
		RequestId:          newRequestID(),
		FlowId:             flowId,
		FlowType:           wf_execute_method_fail_and_proceed.FlowType,
		FlowTimeoutSeconds: 10,
		StartStepType:      wf_execute_method_fail_and_proceed.Step1,
		StepInput: &dexpb.Value{
			Kind: &dexpb.Value_ObjValue{ObjValue: &dexpb.EncodedObject{
				Encoding: wf_execute_method_fail_and_proceed.InputDataEncoding,
				Payload:  []byte(wf_execute_method_fail_and_proceed.TimeoutInputData),
			}},
		},
		StepOptions: &dexpb.StepOptions{
			SkipWaitFor:                      true,
			ExecuteTimeoutSeconds:            1,
			ExecuteRetryPolicy:               &dexpb.RetryPolicy{MaximumAttempts: 1},
			ExecuteFailurePolicy:             dexpb.ExecuteMethodFailurePolicy_EXECUTE_METHOD_FAILURE_POLICY_PROCEED_TO_CONFIGURED_STEP,
			ExecuteFailureProceedStepType:    wf_execute_method_fail_and_proceed.StepRecover,
			ExecuteFailureProceedStepOptions: &dexpb.StepOptions{SkipWaitFor: true},
		},
		FlowStartOptions: withWorkerTarget(nil, workerTarget),
	})
	require.NoError(t, err)
	response, err := flowClient.WaitForFlow(ctx, &dexpb.WaitForFlowRequest{FlowId: flowId})
	require.NoError(t, err)
	require.Equal(t, dexpb.FlowStatus_FLOW_STATUS_COMPLETED, response.GetFlowStatus())
	require.Equal(t, map[string]int64{
		"S1_execute":      1,
		"Recover_execute": 1,
	}, workerHandler.GetTestResult().InvokeHistory)
}

func doTestStateExecuteMethodFailAndProceed(
	t *testing.T,
	backendType service.BackendType,
	flowConfig *dexpb.FlowConfig,
	executeDurabilityOverride dexpb.StepDurability,
) {
	workerHandler := wf_execute_method_fail_and_proceed.NewHandler()
	workerTarget := startWorker(t, workerHandler)
	runtime := startDexService(t, DexServiceTestConfig{
		BackendType: backendType,
	})
	flowClient := runtime.FlowClient

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	flowId := wf_execute_method_fail_and_proceed.FlowType + uuid.NewString()
	startRequest := &dexpb.StartFlowRequest{
		RequestId:          newRequestID(),
		FlowId:             flowId,
		FlowType:           wf_execute_method_fail_and_proceed.FlowType,
		FlowTimeoutSeconds: 10,

		StartStepType: wf_execute_method_fail_and_proceed.Step1,
		StepInput: &dexpb.Value{
			Kind: &dexpb.Value_ObjValue{
				ObjValue: &dexpb.EncodedObject{
					Encoding: wf_execute_method_fail_and_proceed.InputDataEncoding,
					Payload:  []byte(wf_execute_method_fail_and_proceed.InputData),
				},
			},
		},
		StepOptions: &dexpb.StepOptions{
			SkipWaitFor:               true,
			ExecuteDurabilityOverride: executeDurabilityOverride,
			ExecuteRetryPolicy: &dexpb.RetryPolicy{
				MaximumAttempts: 1,
			},
			ExecuteFailurePolicy:          dexpb.ExecuteMethodFailurePolicy_EXECUTE_METHOD_FAILURE_POLICY_PROCEED_TO_CONFIGURED_STEP,
			ExecuteFailureProceedStepType: wf_execute_method_fail_and_proceed.StepRecover,
			ExecuteFailureProceedStepOptions: &dexpb.StepOptions{
				SkipWaitFor: true,
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
		"S1_execute":      1,
		"Recover_execute": 1,
	}, history)

	require.Equal(t, dexpb.FlowStatus_FLOW_STATUS_COMPLETED, resp.GetFlowStatus())
	require.Empty(t, resp.GetResults())
}
