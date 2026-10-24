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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/superdurable/dex/gen/dexpb"
	"github.com/superdurable/dex/integ/workflow/reset"
	"github.com/superdurable/dex/service"
)

func TestResetByStateIdWorkflowTemporal(t *testing.T) {
	if !*temporalIntegTest {
		t.Skip()
	}
	for i := 0; i < *repeatIntegTest; i++ {
		doTestResetByStatIdWorkflow(t, service.BackendTypeTemporal, nil)
		doTestResetByStatIdWorkflow(t, service.BackendTypeTemporal, asyncDurabilityConfig())
		smallWaitForFastTest()
	}
}

func TestResetByStateIdWorkflowCadence(t *testing.T) {
	if !*cadenceIntegTest {
		t.Skip()
	}
	for i := 0; i < *repeatIntegTest; i++ {
		doTestResetByStatIdWorkflow(t, service.BackendTypeCadence, nil)
		doTestResetByStatIdWorkflow(t, service.BackendTypeCadence, asyncDurabilityConfig())
		smallWaitForFastTest()
	}
}

func doTestResetByStatIdWorkflow(
	t *testing.T,
	backendType service.BackendType,
	flowConfig *dexpb.FlowConfig,
) {
	workerHandler := reset.NewHandler()
	workerTarget := startWorker(t, workerHandler)
	runtime := startDexService(t, DexServiceTestConfig{BackendType: backendType})
	flowClient := runtime.FlowClient

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	flowId := reset.WorkflowType + uuid.NewString()
	startResponse, err := flowClient.StartFlow(ctx, &dexpb.StartFlowRequest{
		RequestId:          newRequestID(),
		FlowId:             flowId,
		FlowType:           reset.WorkflowType,
		FlowTimeoutSeconds: 100,

		StartStepType: reset.State1,
		StepInput:     stringValue("1"),
		StepOptions: &dexpb.StepOptions{
			SkipWaitFor: true,
		},
		FlowStartOptions: withWorkerTarget(&dexpb.FlowStartOptions{
			FlowConfigOverride: flowConfig,
			IdReusePolicy:      dexpb.IdReusePolicy_ID_REUSE_POLICY_DISALLOW_REUSE,
		}, workerTarget),
	})
	require.NoError(t, err)

	assertions := assert.New(t)

	response, err := flowClient.WaitForFlow(ctx, &dexpb.WaitForFlowRequest{
		FlowId:       flowId,
		NeedsResults: true,
	})
	require.NoError(t, err)

	history := workerHandler.GetTestResult().InvokeHistory
	assertions.Equalf(map[string]int64{
		"S1_execute": 1,
		"S2_execute": 5,
	}, history, "reset test fail, %v", history)

	assertions.Equal(dexpb.FlowStatus_FLOW_STATUS_COMPLETED, response.GetFlowStatus())
	require.Len(t, response.GetResults(), 1)
	assertions.Equal("S2", response.GetResults()[0].GetCompletedStepType())
	assertions.Equal("S2-5", response.GetResults()[0].GetCompletedStepExecutionId())
	assertions.Equal("5", string(response.GetResults()[0].GetCompletedStepOutput().GetObjValue().GetPayload()))

	timeTravelResponse, err := flowClient.ResetFlow(ctx, &dexpb.ResetFlowRequest{
		RunId:     startResponse.GetRunId(),
		FlowId:    flowId,
		ResetType: dexpb.FlowResetType_FLOW_RESET_TYPE_STEP_TYPE,
		StepType:  reset.State2,
	})
	require.NoError(t, err)

	response, err = flowClient.WaitForFlow(ctx, &dexpb.WaitForFlowRequest{
		FlowId:       flowId,
		NeedsResults: true,
	})
	require.NoError(t, err)

	resetHistory := workerHandler.GetTestResult().InvokeHistory
	expectedResetHistory := map[string]int64{
		"S1_execute": 1,
		"S2_execute": 10,
	}
	// C2: server-zd records ASYNC Steps like SYNC Steps, so time travel reruns the same Steps.
	if flowConfig.GetStepDurability() == dexpb.StepDurability_STEP_DURABILITY_ASYNC && !isServerZDBinaryMode() {
		expectedResetHistory["S1_execute"] = 2
	}
	assertions.Equalf(expectedResetHistory, resetHistory, "reset test fail, %v", resetHistory)

	assertions.Equal(dexpb.FlowStatus_FLOW_STATUS_COMPLETED, response.GetFlowStatus())
	require.Len(t, response.GetResults(), 1)
	assertions.Equal("S2", response.GetResults()[0].GetCompletedStepType())
	assertions.Equal("S2-5", response.GetResults()[0].GetCompletedStepExecutionId())
	assertions.Equal("5", string(response.GetResults()[0].GetCompletedStepOutput().GetObjValue().GetPayload()))

	timeTravelHistory, err := flowClient.GetHistoryEvents(ctx, &dexpb.GetHistoryEventsRequest{
		FlowId:           flowId,
		RunId:            timeTravelResponse.GetRunId(),
		EstimatePageSize: 1000,
	})
	require.NoError(t, err)
	var timeTravelFork *dexpb.TimeTravelForkHistoryEvent
	for _, event := range timeTravelHistory.GetEvents() {
		if event.GetTimeTravelFork() != nil {
			timeTravelFork = event.GetTimeTravelFork()
			break
		}
	}
	require.NotNil(t, timeTravelFork)
	assertions.Equal(startResponse.GetRunId(), timeTravelFork.GetPreviousRunId())

	_, err = flowClient.ResetFlow(ctx, &dexpb.ResetFlowRequest{
		RunId:           startResponse.GetRunId(),
		FlowId:          flowId,
		ResetType:       dexpb.FlowResetType_FLOW_RESET_TYPE_STEP_EXECUTION_ID,
		StepExecutionId: reset.State2 + "-4",
		StepMethod:      dexpb.FlowResetStepMethod_FLOW_RESET_STEP_METHOD_EXECUTE,
	})
	require.NoError(t, err)

	response, err = flowClient.WaitForFlow(ctx, &dexpb.WaitForFlowRequest{
		FlowId:       flowId,
		NeedsResults: true,
	})
	require.NoError(t, err)

	reset2History := workerHandler.GetTestResult().InvokeHistory
	expectedReset2History := map[string]int64{
		"S1_execute": 1,
		"S2_execute": 12,
	}
	// C2: server-zd records ASYNC Steps like SYNC Steps, so time travel reruns the same Steps.
	if flowConfig.GetStepDurability() == dexpb.StepDurability_STEP_DURABILITY_ASYNC && !isServerZDBinaryMode() {
		expectedReset2History["S1_execute"] = 3
		expectedReset2History["S2_execute"] = 15
	}
	assertions.Equalf(expectedReset2History, reset2History, "reset test fail, %v", reset2History)

	assertions.Equal(dexpb.FlowStatus_FLOW_STATUS_COMPLETED, response.GetFlowStatus())
	require.Len(t, response.GetResults(), 1)
	assertions.Equal("S2", response.GetResults()[0].GetCompletedStepType())
	assertions.Equal("S2-5", response.GetResults()[0].GetCompletedStepExecutionId())
	assertions.Equal("5", string(response.GetResults()[0].GetCompletedStepOutput().GetObjValue().GetPayload()))
}
