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
	integcommon "github.com/superdurable/dex/integ/workflow/common"
	rpcStorage "github.com/superdurable/dex/integ/workflow/rpc-external-storage"
	"github.com/superdurable/dex/service"
	dexconverter "github.com/superdurable/dex/service/common/converter"
	"github.com/superdurable/dex/service/common/ptr"
	temporalcommon "go.temporal.io/api/common/v1"
	temporalenums "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/protobuf/proto"
)

func TestRpcExternalStorageNonLockingTemporal(t *testing.T) {
	if !*temporalIntegTest {
		t.Skip()
	}
	for _, lazyLoading := range []bool{true, false} {
		t.Run(fmt.Sprintf("lazy=%v", lazyLoading), func(t *testing.T) {
			doTestRpcExternalStorage(
				t, service.BackendTypeTemporal, false, false, lazyLoading,
			)
		})
	}
}

func TestRpcExternalStorageSynchronousUpdateTemporal(t *testing.T) {
	if !*temporalIntegTest {
		t.Skip()
	}
	for _, lazyLoading := range []bool{true, false} {
		t.Run(fmt.Sprintf("lazy=%v", lazyLoading), func(t *testing.T) {
			doTestRpcExternalStorage(
				t, service.BackendTypeTemporal, false, true, lazyLoading,
			)
		})
	}
}

func TestRpcExternalStorageLockingTemporal(t *testing.T) {
	if !*temporalIntegTest {
		t.Skip()
	}
	for _, lazyLoading := range []bool{true, false} {
		t.Run(fmt.Sprintf("lazy=%v", lazyLoading), func(t *testing.T) {
			doTestRpcExternalStorage(
				t, service.BackendTypeTemporal, true, false, lazyLoading,
			)
		})
	}
}

func TestRpcExternalStorageNonLockingCadence(t *testing.T) {
	if !*cadenceIntegTest {
		t.Skip()
	}
	for _, lazyLoading := range []bool{true, false} {
		t.Run(fmt.Sprintf("lazy=%v", lazyLoading), func(t *testing.T) {
			doTestRpcExternalStorage(
				t, service.BackendTypeCadence, false, false, lazyLoading,
			)
		})
	}
}

func TestRpcSelectedChannelExternalStorageTemporal(t *testing.T) {
	if !*temporalIntegTest {
		t.Skip()
	}
	testRPCSelectedChannelExternalStorage(t, service.BackendTypeTemporal)
}

func TestRpcSelectedChannelExternalStorageCadence(t *testing.T) {
	if !*cadenceIntegTest {
		t.Skip()
	}
	testRPCSelectedChannelExternalStorage(t, service.BackendTypeCadence)
}

func testRPCSelectedChannelExternalStorage(t *testing.T, backendType service.BackendType) {
	for _, lazyLoading := range []bool{true, false} {
		t.Run(fmt.Sprintf("lazy=%v", lazyLoading), func(t *testing.T) {
			runtime := startDexService(t, DexServiceTestConfig{
				BackendType:     backendType,
				S3TestThreshold: 100,
				LazyLoading:     ptr.Any(lazyLoading),
			})
			workerHandler := rpcStorage.NewHandler(runtime.FlowClient)
			workerTarget := startWorker(t, workerHandler)
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()

			flowID := rpcStorage.WorkflowType + "-selected-channel-" + uuid.NewString()
			_, err := runtime.FlowClient.StartFlow(ctx, &dexpb.StartFlowRequest{
				RequestId:          newRequestID(),
				FlowId:             flowID,
				FlowType:           rpcStorage.WorkflowType,
				FlowTimeoutSeconds: 30,
				StartStepType:      rpcStorage.State1,
				StepInput:          objJSONValue(`"start-input"`),
				FlowStartOptions:   withWorkerTarget(nil, workerTarget),
			})
			require.NoError(t, err)
			require.Eventually(t, func() bool {
				response, getErr := runtime.FlowClient.GetAttributes(ctx, &dexpb.GetAttributesRequest{
					FlowId: flowID,
					Keys:   []string{rpcStorage.LargeDataKey},
				})
				return getErr == nil && len(response.GetAttributes()) == 1
			}, 5*time.Second, 50*time.Millisecond)

			_, err = runtime.FlowClient.PublishToChannel(ctx, &dexpb.PublishToChannelRequest{
				FlowId: flowID,
				Messages: []*dexpb.ChannelMessage{{
					ChannelName: rpcStorage.LargeChannel,
					Value:       rpcStorage.InitialLargeData,
				}},
			})
			require.NoError(t, err)
			waitForChannelMessages(t, ctx, runtime, flowID, rpcStorage.LargeChannel, 1)
			_, err = runtime.FlowClient.InvokeRPC(ctx, &dexpb.InvokeRPCRequest{
				RequestId:        newRequestID(),
				FlowId:           flowID,
				RpcName:          rpcStorage.UpdateDataAttributesRPC,
				Input:            rpcStorage.TestInput,
				TimeoutSeconds:   10,
				LoadChannelNames: []string{rpcStorage.LargeChannel},
			})
			require.NoError(t, err)

			testData := workerHandler.GetTestResult().InvokeData
			rawChannels := testData[rpcStorage.UpdateDataAttributesRPC+"-raw-channels"].(map[string]*dexpb.ChannelValues)
			rawMessage := rawChannels[rpcStorage.LargeChannel].GetMessages()[0]
			require.NotEmpty(t, rawMessage.GetMessageId())
			require.Equal(t, lazyLoading, integcommon.BlobIdFromValue(rawMessage.GetValue()) != "")
			resolvedChannels := testData[rpcStorage.UpdateDataAttributesRPC+"-received-channels"].(map[string]*dexpb.ChannelValues)
			resolvedMessage := resolvedChannels[rpcStorage.LargeChannel].GetMessages()[0]
			require.Equal(t, rawMessage.GetMessageId(), resolvedMessage.GetMessageId())
			require.True(t, proto.Equal(rpcStorage.InitialLargeData, resolvedMessage.GetValue()))
			_, err = runtime.FlowClient.WaitForFlow(ctx, &dexpb.WaitForFlowRequest{
				FlowId:          flowID,
				WaitTimeSeconds: 5,
			})
			require.NoError(t, err)
		})
	}
}

func doTestRpcExternalStorage(
	t *testing.T,
	backendType service.BackendType,
	useLocking bool,
	useSynchronousUpdate bool,
	lazyLoading bool,
) {
	runtime := startDexService(t, DexServiceTestConfig{
		BackendType:                            backendType,
		S3TestThreshold:                        100,
		LazyLoading:                            ptr.Any(lazyLoading),
		AsyncStepInputSnapshotsEnabled:         ptr.Any(false),
		UseTemporalSynchronousUpdateForAllRPCs: useSynchronousUpdate,
	})
	workerHandler := rpcStorage.NewHandler(runtime.FlowClient)
	workerTarget := startWorker(t, workerHandler)
	flowClient := runtime.FlowClient

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	flowId := rpcStorage.WorkflowType + uuid.NewString()
	startResponse, err := flowClient.StartFlow(ctx, &dexpb.StartFlowRequest{
		RequestId:          newRequestID(),
		FlowId:             flowId,
		FlowType:           rpcStorage.WorkflowType,
		FlowTimeoutSeconds: 30,

		StartStepType:    rpcStorage.State1,
		StepInput:        objJSONValue(`"start-input"`),
		FlowStartOptions: withWorkerTarget(nil, workerTarget),
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		getResult, getErr := flowClient.GetAttributes(ctx, &dexpb.GetAttributesRequest{
			FlowId: flowId,
			Keys:   []string{rpcStorage.SmallDataKey, rpcStorage.LargeDataKey},
		})
		return getErr == nil && len(getResult.GetAttributes()) == 2
	}, 5*time.Second, 50*time.Millisecond)
	rpcRequest := &dexpb.InvokeRPCRequest{
		RequestId:      newRequestID(),
		FlowId:         flowId,
		RpcName:        rpcStorage.UpdateDataAttributesRPC,
		Input:          rpcStorage.TestInput,
		TimeoutSeconds: 10,
	}
	if useLocking {
		rpcRequest.LockAttributeKeys = []string{
			rpcStorage.SmallDataKey,
			rpcStorage.LargeDataKey,
		}
		rpcRequest.RequestId = uuid.NewString()
	}
	objectsBeforeRPC, err := runtime.BlobStore.CountWorkflowObjectsForTesting(ctx, flowId)
	require.NoError(t, err)

	rpcResp, err := flowClient.InvokeRPC(ctx, rpcRequest)
	require.NoError(t, err)
	rpcOutput := rpcResp.GetOutput()
	require.Equal(t, lazyLoading && (useLocking || useSynchronousUpdate), integcommon.BlobIdFromValue(rpcOutput) != "")
	rpcOutput, err = integcommon.LoadBlobsValue(ctx, flowClient, flowId, rpcOutput)
	require.NoError(t, err)
	require.True(t, proto.Equal(rpcStorage.TestOutput, rpcOutput))

	require.Eventually(t, func() bool {
		testData := workerHandler.GetTestResult().InvokeData
		_, exists := testData[rpcStorage.UpdateDataAttributesRPC+"-received-data"]
		return exists
	}, 2*time.Second, 50*time.Millisecond)

	testData := workerHandler.GetTestResult().InvokeData
	rawInput, exists := testData[rpcStorage.UpdateDataAttributesRPC+"-raw-input"]
	require.True(t, exists)
	require.Equal(
		t,
		lazyLoading && (useLocking || useSynchronousUpdate),
		integcommon.BlobIdFromValue(rawInput.(*dexpb.Value)) != "",
	)
	receivedInput, exists := testData[rpcStorage.UpdateDataAttributesRPC+"-input"]
	require.True(t, exists)
	require.True(t, proto.Equal(rpcStorage.TestInput, receivedInput.(*dexpb.Value)))
	rpcInputData, exists := testData[rpcStorage.UpdateDataAttributesRPC+"-received-data"]
	require.True(t, exists)

	receivedAttributes, ok := rpcInputData.([]*dexpb.KV)
	require.True(t, ok)

	receivedDataMap := make(map[string]*dexpb.Value)
	for _, attribute := range receivedAttributes {
		receivedDataMap[attribute.GetKey()] = attribute.GetValue()
	}

	smallValue, exists := receivedDataMap[rpcStorage.SmallDataKey]
	require.True(t, exists)
	require.True(t, hasObjPayload(smallValue))

	largeValue, exists := receivedDataMap[rpcStorage.LargeDataKey]
	require.True(t, exists)
	require.True(t, hasObjPayload(largeValue))

	resp, err := flowClient.WaitForFlow(ctx, &dexpb.WaitForFlowRequest{
		FlowId:          flowId,
		WaitTimeSeconds: 5,
	})
	require.NoError(t, err)
	require.Equal(t, dexpb.FlowStatus_FLOW_STATUS_COMPLETED, resp.GetFlowStatus())
	if !lazyLoading {
		historyText := flowHistoryText(t, ctx, runtime, flowId)
		require.NotContains(t, historyText, rpcStorage.InitialLargeDataContent)
	}
	if !useLocking && !useSynchronousUpdate {
		objectsAfterRPC, err := runtime.BlobStore.CountWorkflowObjectsForTesting(ctx, flowId)
		require.NoError(t, err)
		require.Equal(t, objectsBeforeRPC+1, objectsAfterRPC)
	}
	if backendType == service.BackendTypeTemporal && (useLocking || useSynchronousUpdate) {
		assertTemporalRpcBlobHistory(
			t,
			ctx,
			runtime,
			flowId,
			startResponse.GetRunId(),
			rpcRequest.GetRequestId(),
		)
	} else {
		assertSignalRpcHistory(
			t,
			ctx,
			runtime,
			flowId,
			startResponse.GetRunId(),
			rpcRequest.GetRequestId(),
		)
	}
}

func assertTemporalRpcBlobHistory(
	t *testing.T,
	ctx context.Context,
	runtime *integRuntime,
	flowID string,
	runID string,
	requestID string,
) {
	t.Helper()
	api := runtime.UnifiedClient.GetApiService().(workflowservice.WorkflowServiceClient)
	response, err := api.GetWorkflowExecutionHistory(
		ctx,
		&workflowservice.GetWorkflowExecutionHistoryRequest{
			Namespace: testNamespace,
			Execution: &temporalcommon.WorkflowExecution{
				WorkflowId: flowID,
				RunId:      runID,
			},
		},
	)
	require.NoError(t, err)
	dataConverter := dexconverter.NewTemporalDataConverter()
	var acceptedInput *dexpb.Value
	var completedOutput *dexpb.Value
	acceptedCount := 0
	completedCount := 0
	for _, event := range response.GetHistory().GetEvents() {
		switch event.GetEventType() {
		case temporalenums.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED:
			accepted := event.GetWorkflowExecutionUpdateAcceptedEventAttributes().GetAcceptedRequest()
			if accepted.GetMeta().GetUpdateId() != requestID {
				continue
			}
			for _, payload := range accepted.GetInput().GetArgs().GetPayloads() {
				require.NotContains(
					t,
					string(payload.GetData()),
					string(rpcStorage.TestInput.GetObjValue().GetPayload()),
				)
			}
			require.Equal(t, "IRPC", accepted.GetInput().GetName())
			var request dexpb.InvokeRPCRequest
			require.NoError(t, dataConverter.FromPayloads(accepted.GetInput().GetArgs(), &request))
			acceptedInput = request.GetInput()
			acceptedCount++
		case temporalenums.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_COMPLETED:
			completed := event.GetWorkflowExecutionUpdateCompletedEventAttributes()
			if completed.GetMeta().GetUpdateId() != requestID {
				continue
			}
			for _, payload := range completed.GetOutcome().GetSuccess().GetPayloads() {
				require.NotContains(
					t,
					string(payload.GetData()),
					string(rpcStorage.TestOutput.GetObjValue().GetPayload()),
				)
			}
			var result dexpb.InvokeRpcUpdateResult
			require.NoError(t, dataConverter.FromPayloads(completed.GetOutcome().GetSuccess(), &result))
			completedOutput = result.GetResponse().GetOutput()
			completedCount++
		case temporalenums.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED:
			signalName := event.GetWorkflowExecutionSignaledEventAttributes().GetSignalName()
			require.NotEqual(t, "ERPC", signalName)
		}
	}
	require.Equal(t, 1, acceptedCount)
	require.Equal(t, 1, completedCount)
	require.NotEmpty(t, integcommon.BlobIdFromValue(acceptedInput))
	require.NotEmpty(t, integcommon.BlobIdFromValue(completedOutput))
	events, _ := getAllWebHistoryEvents(t, ctx, runtime.FlowClient, flowID, runID)
	var rpcEvent *dexpb.RpcExecutionCompletedEvent
	for _, event := range events {
		if event.GetRpcExecutionCompleted().GetRpcName() == rpcStorage.UpdateDataAttributesRPC {
			rpcEvent = event.GetRpcExecutionCompleted()
			break
		}
	}
	require.NotNil(t, rpcEvent)
	require.NotEmpty(t, integcommon.BlobIdFromValue(rpcEvent.GetInput()))
	require.NotEmpty(t, integcommon.BlobIdFromValue(rpcEvent.GetOutput()))
	require.Len(t, rpcEvent.GetUpsertAttributes(), 2)
	require.Len(t, rpcEvent.GetPublishToChannel(), 1)
	loadedInput, err := integcommon.LoadBlobsValue(ctx, runtime.FlowClient, flowID, rpcEvent.GetInput())
	require.NoError(t, err)
	require.True(t, proto.Equal(rpcStorage.TestInput, loadedInput))
	loadedOutput, err := integcommon.LoadBlobsValue(ctx, runtime.FlowClient, flowID, rpcEvent.GetOutput())
	require.NoError(t, err)
	require.True(t, proto.Equal(rpcStorage.TestOutput, loadedOutput))
}

func assertSignalRpcHistory(
	t *testing.T,
	ctx context.Context,
	runtime *integRuntime,
	flowID string,
	runID string,
	requestID string,
) {
	t.Helper()
	if runtime.UnifiedClient.GetBackendType() == service.BackendTypeTemporal {
		assertTemporalRpcSignalHistory(
			t,
			ctx,
			runtime,
			flowID,
			runID,
			requestID,
		)
	}
	events, _ := getAllWebHistoryEvents(t, ctx, runtime.FlowClient, flowID, runID)
	for _, event := range events {
		require.Nil(t, event.GetRpcExecutionCompleted())
	}
}

func assertTemporalRpcSignalHistory(
	t *testing.T,
	ctx context.Context,
	runtime *integRuntime,
	flowID string,
	runID string,
	requestID string,
) {
	t.Helper()
	api := runtime.UnifiedClient.GetApiService().(workflowservice.WorkflowServiceClient)
	response, err := api.GetWorkflowExecutionHistory(
		ctx,
		&workflowservice.GetWorkflowExecutionHistoryRequest{
			Namespace: testNamespace,
			Execution: &temporalcommon.WorkflowExecution{
				WorkflowId: flowID,
				RunId:      runID,
			},
		},
	)
	require.NoError(t, err)
	dataConverter := dexconverter.NewTemporalDataConverter()
	resultSignals := 0
	for _, event := range response.GetHistory().GetEvents() {
		switch event.GetEventType() {
		case temporalenums.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED:
			accepted := event.GetWorkflowExecutionUpdateAcceptedEventAttributes().GetAcceptedRequest()
			require.NotEqual(t, requestID, accepted.GetMeta().GetUpdateId())
		case temporalenums.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED:
			attributes := event.GetWorkflowExecutionSignaledEventAttributes()
			if attributes.GetSignalName() != "ERPC" {
				continue
			}
			var request dexpb.ExecuteRpcSignalRequest
			require.NoError(t, dataConverter.FromPayloads(attributes.GetInput(), &request))
			for _, payload := range attributes.GetInput().GetPayloads() {
				require.NotContains(
					t,
					string(payload.GetData()),
					string(rpcStorage.TestInput.GetObjValue().GetPayload()),
				)
				require.NotContains(
					t,
					string(payload.GetData()),
					string(rpcStorage.TestOutput.GetObjValue().GetPayload()),
				)
			}
			require.Len(t, request.GetUpsertAttributes(), 2)
			require.NotEmpty(t, integcommon.BlobIdFromValue(request.GetUpsertAttributes()[1].GetValue()))
			resultSignals++
		}
	}
	require.Equal(t, 1, resultSignals)
}

func hasObjPayload(value *dexpb.Value) bool {
	if value == nil {
		return false
	}
	return value.GetObjValue() != nil && len(value.GetObjValue().GetPayload()) > 0
}
