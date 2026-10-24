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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/superdurable/dex/gen/dexpb"
	"github.com/superdurable/dex/integ/workflow/common"
	"github.com/superdurable/dex/integ/workflow/rpc"
	"github.com/superdurable/dex/service"
	"github.com/superdurable/dex/service/common/ptr"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestChannelMessageQueueTemporal(t *testing.T) {
	if !*temporalIntegTest {
		t.Skip()
	}
	t.Run("list and delete", func(t *testing.T) {
		testChannelMessageListAndDelete(t, service.BackendTypeTemporal)
	})
	t.Run("large value hydration", func(t *testing.T) {
		testChannelMessageLargeValueHydration(t, service.BackendTypeTemporal)
	})
	t.Run("transactional RPC deletion", testChannelMessageTransactionalRpcDeletion)
	t.Run("signal missing deletion", func(t *testing.T) {
		testChannelMessageSignalMissingDeletion(t, service.BackendTypeTemporal, false)
	})
	t.Run("concurrent deletion", testConcurrentChannelMessageDelete)
}

func TestChannelMessageQueueCadence(t *testing.T) {
	if !*cadenceIntegTest {
		t.Skip()
	}
	t.Run("list and delete", func(t *testing.T) {
		testChannelMessageListAndDelete(t, service.BackendTypeCadence)
	})
	t.Run("large value hydration", func(t *testing.T) {
		testChannelMessageLargeValueHydration(t, service.BackendTypeCadence)
	})
	t.Run("signal missing deletion", func(t *testing.T) {
		testChannelMessageSignalMissingDeletion(t, service.BackendTypeCadence, false)
	})
	t.Run("transactional signal missing deletion", func(t *testing.T) {
		testChannelMessageSignalMissingDeletion(t, service.BackendTypeCadence, true)
	})
}

func testChannelMessageListAndDelete(t *testing.T, backendType service.BackendType) {
	runtime, flowID, runID, ctx := startRpcQueueFlow(t, backendType)
	_, err := runtime.FlowClient.PublishToChannel(ctx, &dexpb.PublishToChannelRequest{
		FlowId: flowID,
		Messages: []*dexpb.ChannelMessage{
			{ChannelName: "pending", MessageId: "client-one", Value: stringValue("first")},
			{ChannelName: "pending", MessageId: "client-two", Value: stringValue("second")},
		},
	})
	require.NoError(t, err)

	messages := waitForChannelMessages(t, ctx, runtime, flowID, "pending", 2)
	require.Equal(t, "first", messages[0].GetValue().GetStringValue())
	require.Equal(t, "second", messages[1].GetValue().GetStringValue())
	require.NotEqual(t, "client-one", messages[0].GetMessageId())
	require.NotEqual(t, "client-two", messages[1].GetMessageId())
	require.NotEqual(t, messages[0].GetMessageId(), messages[1].GetMessageId())
	for _, message := range messages {
		parsed, parseErr := uuid.Parse(message.GetMessageId())
		require.NoError(t, parseErr)
		require.Equal(t, uuid.Version(7), parsed.Version())
	}
	state, err := runtime.FlowClient.GetFlowState(ctx, &dexpb.GetFlowStateRequest{FlowId: flowID})
	require.NoError(t, err)
	require.Equal(t, messages[0].GetMessageId(), state.GetPendingChannelMessages()["pending"].GetMessages()[0].GetMessageId())
	require.Equal(t, messages[1].GetMessageId(), state.GetPendingChannelMessages()["pending"].GetMessages()[1].GetMessageId())

	// C3: server-zd returns Unimplemented for TriggerContinueAsNew.
	if !isServerZDBinaryMode() {
		_, err = runtime.FlowClient.TriggerContinueAsNew(ctx, &dexpb.TriggerContinueAsNewRequest{
			FlowId: flowID,
			RunId:  runID,
		})
		require.NoError(t, err)
		require.Eventually(t, func() bool {
			currentRunID, queryErr := currentFlowRunID(ctx, runtime, flowID)
			if queryErr != nil || currentRunID == runID {
				return false
			}
			runID = currentRunID
			return true
		}, 10*time.Second, 50*time.Millisecond)
		messagesAfterContinueAsNew := waitForChannelMessages(t, ctx, runtime, flowID, "pending", 2)
		require.Equal(t, messages[0].GetMessageId(), messagesAfterContinueAsNew[0].GetMessageId())
		require.Equal(t, messages[1].GetMessageId(), messagesAfterContinueAsNew[1].GetMessageId())
	}

	_, err = runtime.FlowClient.DeleteChannelMessage(ctx, &dexpb.DeleteChannelMessageRequest{
		FlowId:      flowID,
		ChannelName: "pending",
		MessageId:   messages[0].GetMessageId(),
		RequestId:   newRequestID(),
	})
	require.NoError(t, err)
	remaining := waitForChannelMessages(t, ctx, runtime, flowID, "pending", 1)
	require.Equal(t, messages[1].GetMessageId(), remaining[0].GetMessageId())
	waitForChannelDeleteHistory(t, ctx, runtime, flowID, runID, messages[0].GetMessageId())

	_, err = runtime.FlowClient.DeleteChannelMessage(ctx, &dexpb.DeleteChannelMessageRequest{
		FlowId:      flowID,
		ChannelName: "pending",
		MessageId:   messages[0].GetMessageId(),
		RequestId:   newRequestID(),
	})
	require.Equal(t, codes.NotFound, status.Code(err))
	require.Equal(
		t,
		dexpb.ErrorSubStatus_ERROR_SUB_STATUS_CHANNEL_MESSAGE_NOT_FOUND,
		grpcServiceErrorResponse(t, err).GetSubStatus(),
	)

	_, err = runtime.FlowClient.PublishToChannel(ctx, &dexpb.PublishToChannelRequest{
		FlowId: flowID,
		Messages: []*dexpb.ChannelMessage{
			{ChannelName: "mapped/one", Value: stringValue("map-one")},
			{ChannelName: "mapped/two", Value: stringValue("map-two")},
		},
	})
	require.NoError(t, err)
	mappedOne := waitForChannelMessages(t, ctx, runtime, flowID, "mapped/one", 1)
	mappedTwo := waitForChannelMessages(t, ctx, runtime, flowID, "mapped/two", 1)
	_, err = runtime.FlowClient.DeleteChannelMessage(ctx, &dexpb.DeleteChannelMessageRequest{
		FlowId:      flowID,
		ChannelName: "mapped/one",
		MessageId:   mappedOne[0].GetMessageId(),
		RequestId:   newRequestID(),
	})
	require.NoError(t, err)
	waitForChannelMessages(t, ctx, runtime, flowID, "mapped/one", 0)
	require.Equal(
		t,
		mappedTwo[0].GetMessageId(),
		waitForChannelMessages(t, ctx, runtime, flowID, "mapped/two", 1)[0].GetMessageId(),
	)

	_, err = runtime.FlowClient.PublishToChannel(ctx, &dexpb.PublishToChannelRequest{
		FlowId: flowID,
		Messages: []*dexpb.ChannelMessage{{
			ChannelName: rpc.TestInterStateChannelName,
			Value:       stringValue("consume"),
		}},
	})
	require.NoError(t, err)
	waitForFlowCompleted(t, ctx, runtime, flowID)
	waitForChannelMessages(t, ctx, runtime, flowID, rpc.TestInterStateChannelName, 0)

	resetResponse, err := runtime.FlowClient.ResetFlow(ctx, &dexpb.ResetFlowRequest{
		FlowId:    flowID,
		RunId:     runID,
		ResetType: dexpb.FlowResetType_FLOW_RESET_TYPE_BEGINNING,
	})
	require.NoError(t, err)
	waitForFlowCompleted(t, ctx, runtime, flowID)
	resetPending := waitForChannelMessages(t, ctx, runtime, flowID, "pending", 1)
	require.Equal(t, remaining[0].GetMessageId(), resetPending[0].GetMessageId())
	if isServerZDBinaryMode() {
		// C1: server-zd keeps the run ID across time travel.
		require.Equal(t, runID, resetResponse.GetRunId())
	} else {
		require.NotEqual(t, runID, resetResponse.GetRunId())
	}
}

func testConcurrentChannelMessageDelete(t *testing.T) {
	runtime, flowID, _, ctx := startRpcQueueFlow(t, service.BackendTypeTemporal)
	_, err := runtime.FlowClient.PublishToChannel(ctx, &dexpb.PublishToChannelRequest{
		FlowId: flowID,
		Messages: []*dexpb.ChannelMessage{{
			ChannelName: "race",
			Value:       stringValue("only-once"),
		}},
	})
	require.NoError(t, err)
	message := waitForChannelMessages(t, ctx, runtime, flowID, "race", 1)[0]

	start := make(chan struct{})
	results := make(chan error, 2)
	var waitGroup sync.WaitGroup
	for range 2 {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			<-start
			_, deleteErr := runtime.FlowClient.DeleteChannelMessage(ctx, &dexpb.DeleteChannelMessageRequest{
				FlowId:      flowID,
				ChannelName: "race",
				MessageId:   message.GetMessageId(),
				RequestId:   newRequestID(),
			})
			results <- deleteErr
		}()
	}
	close(start)
	waitGroup.Wait()
	close(results)

	successCount := 0
	notFoundCount := 0
	for deleteErr := range results {
		switch status.Code(deleteErr) {
		case codes.OK:
			successCount++
		case codes.NotFound:
			notFoundCount++
		default:
			require.NoError(t, deleteErr)
		}
	}
	require.Equal(t, 1, successCount)
	require.Equal(t, 1, notFoundCount)
	waitForChannelMessages(t, ctx, runtime, flowID, "race", 0)
}

func testChannelMessageTransactionalRpcDeletion(t *testing.T) {
	runtime, flowID, runID, ctx := startRpcQueueFlow(t, service.BackendTypeTemporal)
	waitForRpcInitialAttribute(t, ctx, runtime, flowID)

	_, err := runtime.FlowClient.InvokeRPC(ctx, &dexpb.InvokeRPCRequest{
		FlowId:          flowID,
		RpcName:         rpc.RPCNameDeleteMissingTransactional,
		RequestId:       newRequestID(),
		IsTransactional: true,
	})
	require.Equal(t, codes.NotFound, status.Code(err))
	require.Equal(
		t,
		dexpb.ErrorSubStatus_ERROR_SUB_STATUS_CHANNEL_MESSAGE_NOT_FOUND,
		grpcServiceErrorResponse(t, err).GetSubStatus(),
	)

	attributes, err := runtime.FlowClient.GetAttributes(ctx, &dexpb.GetAttributesRequest{
		FlowId: flowID,
		Keys:   []string{rpc.TestDataAttributeKey},
	})
	require.NoError(t, err)
	attributeValue, err := common.LoadBlobsValue(
		ctx,
		runtime.FlowClient,
		flowID,
		attributes.GetAttributes()[0].GetValue(),
	)
	require.NoError(t, err)
	require.Equal(
		t,
		rpc.TestDataAttributeVal1.GetObjValue().GetPayload(),
		attributeValue.GetObjValue().GetPayload(),
	)
	response, err := runtime.FlowClient.GetChannelMessages(ctx, &dexpb.GetChannelMessagesRequest{
		FlowId:      flowID,
		ChannelName: "destination",
	})
	require.NoError(t, err)
	require.Empty(t, response.GetMessages())
	state, err := runtime.FlowClient.GetFlowState(ctx, &dexpb.GetFlowStateRequest{FlowId: flowID})
	require.NoError(t, err)
	hasStateOne := false
	for _, step := range state.GetActiveStepExecutions() {
		require.NotEqual(t, rpc.State2, step.GetStepType())
		hasStateOne = hasStateOne || step.GetStepType() == rpc.State1
	}
	require.True(t, hasStateOne)
	require.Empty(t, state.GetQueuedSteps())
	require.Empty(t, state.GetCompletedSteps())
	historyResponse, err := runtime.FlowClient.GetHistoryEvents(ctx, &dexpb.GetHistoryEventsRequest{
		FlowId: flowID,
		RunId:  runID,
	})
	require.NoError(t, err)
	for _, event := range historyResponse.GetEvents() {
		require.NotEqual(t, rpc.RPCNameDeleteMissingTransactional, event.GetRpcExecutionCompleted().GetRpcName())
	}

	_, err = runtime.FlowClient.PublishToChannel(ctx, &dexpb.PublishToChannelRequest{
		FlowId: flowID,
		Messages: []*dexpb.ChannelMessage{{
			ChannelName: "source",
			Value:       stringValue("move-value"),
		}},
	})
	require.NoError(t, err)
	source := waitForChannelMessages(t, ctx, runtime, flowID, "source", 1)[0]
	_, err = runtime.FlowClient.InvokeRPC(ctx, &dexpb.InvokeRPCRequest{
		FlowId:           flowID,
		RpcName:          rpc.RPCNameMove,
		Input:            stringValue(source.GetMessageId()),
		RequestId:        newRequestID(),
		IsTransactional:  true,
		LoadChannelNames: []string{"source"},
	})
	require.NoError(t, err)
	waitForChannelMessages(t, ctx, runtime, flowID, "source", 0)
	destination := waitForChannelMessages(t, ctx, runtime, flowID, "destination", 1)
	require.Equal(t, "move-value", destination[0].GetValue().GetStringValue())

	_, err = runtime.FlowClient.InvokeRPC(ctx, &dexpb.InvokeRPCRequest{
		FlowId:           flowID,
		RpcName:          rpc.RPCNameMove,
		Input:            stringValue(source.GetMessageId()),
		RequestId:        newRequestID(),
		IsTransactional:  true,
		LoadChannelNames: []string{"source"},
	})
	require.Equal(t, codes.NotFound, status.Code(err))
	require.Len(t, waitForChannelMessages(t, ctx, runtime, flowID, "destination", 1), 1)
}

func testChannelMessageLargeValueHydration(t *testing.T, backendType service.BackendType) {
	runtime, flowID, _, ctx := startRpcQueueFlowWithConfig(t, DexServiceTestConfig{
		BackendType:        backendType,
		LocalBlobDirectory: t.TempDir(),
		LocalBlobThreshold: 10,
		LazyLoading:        ptr.Any(false),
	})
	largeValue := strings.Repeat("large-channel-value-", 100)
	_, err := runtime.FlowClient.PublishToChannel(ctx, &dexpb.PublishToChannelRequest{
		FlowId: flowID,
		Messages: []*dexpb.ChannelMessage{{
			ChannelName: "large",
			Value:       stringValue(largeValue),
		}},
	})
	require.NoError(t, err)
	messages := waitForChannelMessages(t, ctx, runtime, flowID, "large", 1)
	require.Equal(t, largeValue, messages[0].GetValue().GetStringValue())
	require.Empty(t, messages[0].GetValue().GetInternalBlobIdForStringValue())
}

func testChannelMessageSignalMissingDeletion(
	t *testing.T,
	backendType service.BackendType,
	isTransactional bool,
) {
	runtime, flowID, runID, ctx := startRpcQueueFlow(t, backendType)
	waitForRpcInitialAttribute(t, ctx, runtime, flowID)

	_, err := runtime.FlowClient.InvokeRPC(ctx, &dexpb.InvokeRPCRequest{
		FlowId:          flowID,
		RpcName:         rpc.RPCNameDeleteMissingSignal,
		RequestId:       newRequestID(),
		IsTransactional: isTransactional,
	})
	require.NoError(t, err)
	destination := waitForChannelMessages(t, ctx, runtime, flowID, "destination", 1)
	require.NotEqual(t, "worker-supplied", destination[0].GetMessageId())
	parsed, err := uuid.Parse(destination[0].GetMessageId())
	require.NoError(t, err)
	require.Equal(t, uuid.Version(7), parsed.Version())
	waitForRpcDeleteHistory(t, ctx, runtime, flowID, runID)
	require.Eventually(t, func() bool {
		attributes, getErr := runtime.FlowClient.GetAttributes(ctx, &dexpb.GetAttributesRequest{
			FlowId: flowID,
			Keys:   []string{rpc.TestDataAttributeKey},
		})
		return getErr == nil && len(attributes.GetAttributes()) == 1 &&
			string(attributes.GetAttributes()[0].GetValue().GetObjValue().GetPayload()) == rpc.RPCNameDeleteMissingSignal
	}, 10*time.Second, 50*time.Millisecond)
}

func waitForChannelDeleteHistory(
	t *testing.T,
	ctx context.Context,
	runtime *integRuntime,
	flowID string,
	runID string,
	messageID string,
) {
	t.Helper()
	require.Eventually(t, func() bool {
		response, err := runtime.FlowClient.GetHistoryEvents(ctx, &dexpb.GetHistoryEventsRequest{
			FlowId: flowID,
			RunId:  runID,
		})
		if err != nil {
			return false
		}
		for _, event := range response.GetEvents() {
			for _, deletion := range event.GetChannelExternalDelete().GetMessages() {
				if deletion.GetMessageId() == messageID {
					return true
				}
			}
		}
		return false
	}, 10*time.Second, 50*time.Millisecond)
}

func waitForRpcDeleteHistory(
	t *testing.T,
	ctx context.Context,
	runtime *integRuntime,
	flowID string,
	runID string,
) {
	t.Helper()
	require.Eventually(t, func() bool {
		response, err := runtime.FlowClient.GetHistoryEvents(ctx, &dexpb.GetHistoryEventsRequest{
			FlowId: flowID,
			RunId:  runID,
		})
		if err != nil {
			return false
		}
		for _, event := range response.GetEvents() {
			deletions := event.GetRpcExecutionCompleted().GetDeleteFromChannel()
			if len(deletions) == 1 && deletions[0].GetMessageId() == "missing" {
				return true
			}
		}
		return false
	}, 10*time.Second, 50*time.Millisecond)
}

func startRpcQueueFlow(
	t *testing.T,
	backendType service.BackendType,
) (*integRuntime, string, string, context.Context) {
	t.Helper()
	return startRpcQueueFlowWithConfig(t, DexServiceTestConfig{BackendType: backendType})
}

func startRpcQueueFlowWithConfig(
	t *testing.T,
	testConfig DexServiceTestConfig,
) (*integRuntime, string, string, context.Context) {
	t.Helper()
	workerHandler := rpc.NewHandler()
	workerTarget := startWorker(t, workerHandler)
	runtime := startDexService(t, testConfig)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	flowID := "channel-message-queue-" + uuid.NewString()
	response, err := runtime.FlowClient.StartFlow(ctx, &dexpb.StartFlowRequest{
		RequestId:          newRequestID(),
		FlowId:             flowID,
		FlowType:           rpc.WorkflowType,
		FlowTimeoutSeconds: 25,
		StartStepType:      rpc.State1,
		FlowStartOptions:   withWorkerTarget(nil, workerTarget),
	})
	require.NoError(t, err)
	return runtime, flowID, response.GetRunId(), ctx
}

func waitForChannelMessages(
	t *testing.T,
	ctx context.Context,
	runtime *integRuntime,
	flowID string,
	channelName string,
	expectedCount int,
) []*dexpb.ChannelMessage {
	t.Helper()
	var messages []*dexpb.ChannelMessage
	require.Eventually(t, func() bool {
		response, err := runtime.FlowClient.GetChannelMessages(ctx, &dexpb.GetChannelMessagesRequest{
			FlowId:      flowID,
			ChannelName: channelName,
		})
		if err != nil {
			return false
		}
		messages = response.GetMessages()
		return len(messages) == expectedCount
	}, 10*time.Second, 50*time.Millisecond)
	return messages
}

func waitForFlowCompleted(
	t *testing.T,
	ctx context.Context,
	runtime *integRuntime,
	flowID string,
) {
	t.Helper()
	response, err := runtime.FlowClient.WaitForFlow(ctx, &dexpb.WaitForFlowRequest{
		FlowId:          flowID,
		WaitTimeSeconds: 20,
	})
	require.NoError(t, err)
	require.Equal(t, dexpb.FlowStatus_FLOW_STATUS_COMPLETED, response.GetFlowStatus())
}

func waitForRpcInitialAttribute(
	t *testing.T,
	ctx context.Context,
	runtime *integRuntime,
	flowID string,
) {
	t.Helper()
	require.Eventually(t, func() bool {
		attributes, err := runtime.FlowClient.GetAttributes(ctx, &dexpb.GetAttributesRequest{
			FlowId: flowID,
			Keys:   []string{rpc.TestDataAttributeKey},
		})
		return err == nil && len(attributes.GetAttributes()) == 1
	}, 10*time.Second, 50*time.Millisecond)
}
