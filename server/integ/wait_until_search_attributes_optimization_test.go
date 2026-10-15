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
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/superdurable/dex/gen/dexpb"
	"github.com/superdurable/dex/integ/workflow/wait_until_search_attributes_optimization"
	"github.com/superdurable/dex/service"
	"github.com/superdurable/dex/service/common/ptr"
	"go.temporal.io/api/common/v1"
	"go.temporal.io/api/enums/v1"
	history "go.temporal.io/api/history/v1"
	"go.temporal.io/api/workflowservice/v1"
)

func TestWaitUntilSearchAttributesOptimizationWorkflowTemporal(t *testing.T) {
	if !*temporalIntegTest {
		t.Skip()
	}
	for i := 0; i < *repeatIntegTest; i++ {
		doTestWaitUntilHistoryCompleted(t, &dexpb.FlowConfig{
			ActiveStepSearchMode: ptr.Any(
				dexpb.ActiveStepSearchMode_ACTIVE_STEP_SEARCH_MODE_DISABLED,
			),
		})
		smallWaitForFastTest()
	}

	for i := 0; i < *repeatIntegTest; i++ {
		doTestWaitUntilHistoryCompleted(t, &dexpb.FlowConfig{
			ActiveStepSearchMode: ptr.Any(
				dexpb.ActiveStepSearchMode_ACTIVE_STEP_SEARCH_MODE_ENABLED_FOR_ALL,
			),
		})
		smallWaitForFastTest()
	}

	for i := 0; i < *repeatIntegTest; i++ {
		doTestWaitUntilHistoryCompleted(t, &dexpb.FlowConfig{
			ActiveStepSearchMode: ptr.Any(
				dexpb.ActiveStepSearchMode_ACTIVE_STEP_SEARCH_MODE_ENABLED_FOR_STEPS_WITH_WAIT_FOR,
			),
		})
		smallWaitForFastTest()
	}

	for i := 0; i < *repeatIntegTest; i++ {
		doTestWaitUntilHistoryCompleted(t, nil)
		smallWaitForFastTest()
	}
}

func doTestWaitUntilHistoryCompleted(t *testing.T, flowConfig *dexpb.FlowConfig) {
	workerHandler := wait_until_search_attributes_optimization.NewHandler()
	workerTarget := startWorker(t, workerHandler)
	runtime := startDexService(t, DexServiceTestConfig{
		BackendType:                     service.BackendTypeTemporal,
		UseBuiltInDefaultWorkflowConfig: true,
	})
	flowClient := runtime.FlowClient
	unifiedClient := runtime.UnifiedClient

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	flowId := wait_until_search_attributes_optimization.WorkflowType + uuid.NewString()
	startRequest := &dexpb.StartFlowRequest{
		RequestId:          newRequestID(),
		FlowId:             flowId,
		FlowType:           wait_until_search_attributes_optimization.WorkflowType,
		FlowTimeoutSeconds: 60,

		StartStepType:    wait_until_search_attributes_optimization.State1,
		FlowStartOptions: withWorkerTarget(nil, workerTarget),
	}
	if flowConfig != nil {
		startRequest.FlowStartOptions = withWorkerTarget(&dexpb.FlowStartOptions{
			FlowConfigOverride: flowConfig,
		}, workerTarget)
	}
	_, err := flowClient.StartFlow(ctx, startRequest)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return workerHandler.GetTestResult().InvokeHistory[wait_until_search_attributes_optimization.State4+"_waitFor"] > 0
	}, 30*time.Second, 100*time.Millisecond)

	_, err = flowClient.PublishToChannel(ctx, &dexpb.PublishToChannelRequest{
		FlowId: flowId,
		Messages: []*dexpb.ChannelMessage{
			{
				ChannelName: wait_until_search_attributes_optimization.SignalName,
				Value:       objJSONValue(`"test"`),
			},
		},
	})
	require.NoError(t, err)

	resp, err := flowClient.WaitForFlow(ctx, &dexpb.WaitForFlowRequest{
		FlowId: flowId,
	})
	require.NoError(t, err)
	require.Equal(t, dexpb.FlowStatus_FLOW_STATUS_COMPLETED, resp.GetFlowStatus(),
		"Flow %s failed: %s", flowId, resp.GetErrorMessage())

	api := unifiedClient.GetApiService().(workflowservice.WorkflowServiceClient)
	eventHistory, err := api.GetWorkflowExecutionHistory(ctx, &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: testNamespace,
		Execution: &common.WorkflowExecution{
			WorkflowId: flowId,
		},
	})
	require.NoError(t, err)

	var upsertSAEvents []*history.HistoryEvent
	for _, event := range eventHistory.History.Events {
		if event.EventType == enums.EVENT_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES {
			upsertSAEvents = append(upsertSAEvents, event)
		}
	}

	mode := dexpb.ActiveStepSearchMode_ACTIVE_STEP_SEARCH_MODE_DISABLED
	if flowConfig != nil && flowConfig.ActiveStepSearchMode != nil {
		mode = flowConfig.GetActiveStepSearchMode()
	}

	switch mode {
	case dexpb.ActiveStepSearchMode_ACTIVE_STEP_SEARCH_MODE_ENABLED_FOR_ALL:
		require.Equal(t, 10, len(upsertSAEvents))
		require.Equal(t, []string{"S1"}, historyEventSAs(upsertSAEvents[1]))
		require.Equal(t, []string{"S2"}, historyEventSAs(upsertSAEvents[2]))
		require.Equal(t, []string{"S2", "S3"}, historyEventSAs(upsertSAEvents[3]))
		require.Equal(t, []string{"S3", "S4"}, historyEventSAs(upsertSAEvents[4]))
		require.Equal(t, []string{"S3", "S5"}, historyEventSAs(upsertSAEvents[5]))
		require.Equal(t, []string{"S3", "S6", "S7"}, historyEventSAs(upsertSAEvents[6]))
		require.Equal(t, []string{"S3", "S6"}, historyEventSAs(upsertSAEvents[7]))
		require.Equal(t, []string{"S3"}, historyEventSAs(upsertSAEvents[8]))
		require.Empty(t, historyEventSAs(upsertSAEvents[9]))
	case dexpb.ActiveStepSearchMode_ACTIVE_STEP_SEARCH_MODE_DISABLED:
		require.Equal(t, 1, len(upsertSAEvents))
	default:
		require.Equal(t, 9, len(upsertSAEvents))
		require.Equal(t, []string{"S1"}, historyEventSAs(upsertSAEvents[1]))
		require.Empty(t, historyEventSAs(upsertSAEvents[2]))
		require.Equal(t, []string{"S3"}, historyEventSAs(upsertSAEvents[3]))
		require.Equal(t, []string{"S3", "S4"}, historyEventSAs(upsertSAEvents[4]))
		require.Equal(t, []string{"S3"}, historyEventSAs(upsertSAEvents[5]))
		require.Equal(t, []string{"S3", "S6"}, historyEventSAs(upsertSAEvents[6]))
		require.Equal(t, []string{"S3"}, historyEventSAs(upsertSAEvents[7]))
		require.Empty(t, historyEventSAs(upsertSAEvents[8]))
	}
}

func historyEventSAs(event *history.HistoryEvent) []string {
	attrs := event.GetAttributes().(*history.HistoryEvent_UpsertWorkflowSearchAttributesEventAttributes)
	data := string(attrs.UpsertWorkflowSearchAttributesEventAttributes.GetSearchAttributes().GetIndexedFields()[service.SearchAttributeActiveStepTypes].GetData())
	data = strings.TrimSpace(data)
	if data == "" || data == "null" || data == "[]" {
		return nil
	}
	data = strings.ReplaceAll(data, "[", "")
	data = strings.ReplaceAll(data, "]", "")
	data = strings.ReplaceAll(data, "\"", "")
	if data == "" || data == "null" {
		return nil
	}
	dataSlice := strings.Split(data, ",")
	slices.Sort(dataSlice)
	return dataSlice
}
