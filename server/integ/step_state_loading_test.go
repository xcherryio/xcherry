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
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/superdurable/dex/gen/dexpb"
	"github.com/superdurable/dex/integ/workflow/common"
	"github.com/superdurable/dex/service"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	stepStateLoadingFlowType   = "step-state-loading"
	stepStateLoadingRootStep   = "root"
	stepStateLoadingFinishStep = "finish"
	stepStateLoadingCommands   = "commands"
	stepStateLoadingFinish     = "finish"
	stepStateLoadingEffects    = "effects"
)

type stepStateLoadingHandler struct {
	dexpb.UnimplementedWorkerServiceServer
	waitForRequests chan *dexpb.InvokeWaitForMethodRequest
	executeRequests chan *dexpb.InvokeExecuteMethodRequest
}

func newStepStateLoadingHandler() *stepStateLoadingHandler {
	return &stepStateLoadingHandler{
		waitForRequests: make(chan *dexpb.InvokeWaitForMethodRequest, 1),
		executeRequests: make(chan *dexpb.InvokeExecuteMethodRequest, 1),
	}
}

func (h *stepStateLoadingHandler) InvokeWaitForMethod(
	_ context.Context,
	request *dexpb.InvokeWaitForMethodRequest,
) (*dexpb.InvokeWaitForMethodResponse, error) {
	if request.GetFlowType() != stepStateLoadingFlowType {
		return nil, status.Errorf(codes.InvalidArgument, "unexpected Flow type %q", request.GetFlowType())
	}
	if request.GetStepType() == stepStateLoadingFinishStep {
		return &dexpb.InvokeWaitForMethodResponse{
			WaitingCondition: &dexpb.WaitingCondition{
				WaitingConditionType: dexpb.WaitingConditionType_WAITING_CONDITION_TYPE_ALL_COMPLETED,
				ChannelConditions: []*dexpb.ChannelCondition{{
					ChannelName: stepStateLoadingFinish,
				}},
			},
		}, nil
	}
	if request.GetStepType() != stepStateLoadingRootStep {
		return nil, status.Errorf(codes.InvalidArgument, "unexpected Step type %q", request.GetStepType())
	}
	commands := request.GetLoadedChannelMessages()[stepStateLoadingCommands].GetMessages()
	if len(commands) == 0 {
		return nil, status.Error(codes.InvalidArgument, "commands were not loaded")
	}
	h.waitForRequests <- request
	return &dexpb.InvokeWaitForMethodResponse{
		UpsertAttributes: []*dexpb.AttributeWrite{{Key: "wait-effect", Value: stringValue("written")}},
		DeleteFromChannel: []*dexpb.ChannelMessageDeletion{
			{ChannelName: stepStateLoadingCommands, MessageId: commands[0].GetMessageId()},
			{ChannelName: stepStateLoadingCommands, MessageId: "missing"},
		},
		PublishToChannel: []*dexpb.ChannelMessage{{
			ChannelName: stepStateLoadingEffects,
			Value:       stringValue("wait"),
		}},
		WaitingCondition: &dexpb.WaitingCondition{
			WaitingConditionType: dexpb.WaitingConditionType_WAITING_CONDITION_TYPE_ALL_COMPLETED,
			ChannelConditions: []*dexpb.ChannelCondition{{
				ChannelName: stepStateLoadingCommands,
			}},
		},
	}, nil
}

func (h *stepStateLoadingHandler) InvokeExecuteMethod(
	_ context.Context,
	request *dexpb.InvokeExecuteMethodRequest,
) (*dexpb.InvokeExecuteMethodResponse, error) {
	if request.GetFlowType() != stepStateLoadingFlowType {
		return nil, status.Errorf(codes.InvalidArgument, "unexpected Flow type %q", request.GetFlowType())
	}
	if request.GetStepType() == stepStateLoadingFinishStep {
		return &dexpb.InvokeExecuteMethodResponse{
			StepDecision: &dexpb.StepDecision{
				CloseDecision: common.ForceCompleteDecision(stringValue("complete")),
			},
		}, nil
	}
	if request.GetStepType() != stepStateLoadingRootStep {
		return nil, status.Errorf(codes.InvalidArgument, "unexpected Step type %q", request.GetStepType())
	}
	commands := request.GetLoadedChannelMessages()[stepStateLoadingCommands].GetMessages()
	channelMapOne := request.GetLoadedChannelMessages()["channel-map/one"].GetMessages()
	if len(commands) == 0 || len(channelMapOne) == 0 {
		return nil, status.Error(codes.InvalidArgument, "selected messages were not loaded")
	}
	h.executeRequests <- request
	return &dexpb.InvokeExecuteMethodResponse{
		StepDecision: &dexpb.StepDecision{NextSteps: []*dexpb.StepMovement{{
			StepType: stepStateLoadingFinishStep,
		}}},
		UpsertAttributes: []*dexpb.AttributeWrite{{Key: "execute-effect", Value: stringValue("written")}},
		DeleteFromChannel: []*dexpb.ChannelMessageDeletion{
			{ChannelName: stepStateLoadingCommands, MessageId: commands[0].GetMessageId()},
			{ChannelName: "channel-map/one", MessageId: channelMapOne[0].GetMessageId()},
			{ChannelName: stepStateLoadingCommands, MessageId: "missing"},
		},
		PublishToChannel: []*dexpb.ChannelMessage{{
			ChannelName: stepStateLoadingEffects,
			Value:       stringValue("execute"),
		}},
	}, nil
}

func TestStepStateLoadingTemporal(t *testing.T) {
	if !*temporalIntegTest {
		t.Skip()
	}
	// C8: server-zd rejects the Flow start delay that orders this scenario.
	requireGoServerSetting(t, "Flow start delay")
	for iteration := 0; iteration < *repeatIntegTest; iteration++ {
		doTestStepStateLoading(t, service.BackendTypeTemporal)
	}
}

func TestStepStateLoadingCadence(t *testing.T) {
	if !*cadenceIntegTest {
		t.Skip()
	}
	for iteration := 0; iteration < *repeatIntegTest; iteration++ {
		doTestStepStateLoading(t, service.BackendTypeCadence)
	}
}

func doTestStepStateLoading(t *testing.T, backendType service.BackendType) {
	t.Helper()
	handler := newStepStateLoadingHandler()
	workerTarget := startWorker(t, handler)
	runtime := startDexService(t, DexServiceTestConfig{BackendType: backendType})
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	flowID := stepStateLoadingFlowType + "-" + uuid.NewString()
	startResponse, err := runtime.FlowClient.StartFlow(ctx, &dexpb.StartFlowRequest{
		RequestId:          newRequestID(),
		FlowId:             flowID,
		FlowType:           stepStateLoadingFlowType,
		FlowTimeoutSeconds: 45,
		StartStepType:      stepStateLoadingRootStep,
		StepOptions: &dexpb.StepOptions{
			WaitForLoadAttributeMapInstances: []string{"attribute-map/one"},
			WaitForLoadChannelNames:          []string{stepStateLoadingCommands},
			WaitForLoadChannelMapInstances:   []string{"channel-map/"},
			ExecuteLoadAttributeMapInstances: []string{"attribute-map/two"},
			ExecuteLoadChannelNames:          []string{stepStateLoadingCommands},
			ExecuteLoadChannelMapInstances:   []string{"channel-map/one"},
		},
		FlowStartOptions: withWorkerTarget(&dexpb.FlowStartOptions{
			FlowStartDelaySeconds: 3,
			Attributes: []*dexpb.AttributeWrite{
				{Key: "ordinary", Value: stringValue("ordinary")},
				{Key: "attribute-map/one", Value: stringValue("one")},
				{Key: "attribute-map/two", Value: stringValue("two")},
			},
		}, workerTarget),
	})
	require.NoError(t, err)

	publishStepStateLoadingMessages(t, ctx, runtime, flowID, startResponse.GetRunId())
	waitForRequest := receiveStepWaitForRequest(t, ctx, handler.waitForRequests)
	requireStepWaitForState(t, waitForRequest)
	executeRequest := receiveStepExecuteRequest(t, ctx, handler.executeRequests)
	requireStepExecuteState(t, executeRequest)
	requireStepStateLoadingEffects(t, ctx, runtime, flowID)

	_, err = runtime.FlowClient.PublishToChannel(ctx, &dexpb.PublishToChannelRequest{
		FlowId: flowID,
		RunId:  startResponse.GetRunId(),
		Messages: []*dexpb.ChannelMessage{{
			ChannelName: stepStateLoadingFinish,
			Value:       stringValue("finish"),
		}},
	})
	require.NoError(t, err)
	result, err := runtime.FlowClient.WaitForFlow(ctx, &dexpb.WaitForFlowRequest{
		FlowId:          flowID,
		RunId:           startResponse.GetRunId(),
		NeedsResults:    true,
		WaitTimeSeconds: 30,
	})
	require.NoError(t, err)
	require.Equal(t, dexpb.FlowStatus_FLOW_STATUS_COMPLETED, result.GetFlowStatus())
	require.Len(t, result.GetResults(), 1)
	require.Equal(t, "complete", result.GetResults()[0].GetCompletedStepOutput().GetStringValue())
	requireStepDeletionHistory(t, ctx, runtime, flowID, startResponse.GetRunId())
}

func publishStepStateLoadingMessages(
	t *testing.T,
	ctx context.Context,
	runtime *integRuntime,
	flowID string,
	runID string,
) {
	t.Helper()
	_, err := runtime.FlowClient.PublishToChannel(ctx, &dexpb.PublishToChannelRequest{
		FlowId: flowID,
		RunId:  runID,
		Messages: []*dexpb.ChannelMessage{
			{ChannelName: stepStateLoadingCommands, Value: stringValue("delete")},
			{ChannelName: stepStateLoadingCommands, Value: stringValue("consume")},
			{ChannelName: stepStateLoadingCommands, Value: stringValue("execute-delete")},
			{ChannelName: "other", Value: stringValue("other")},
			{ChannelName: "channel-map/one", Value: stringValue("one")},
			{ChannelName: "channel-map/two", Value: stringValue("two")},
		},
	})
	require.NoError(t, err)
}

func receiveStepWaitForRequest(
	t *testing.T,
	ctx context.Context,
	requests <-chan *dexpb.InvokeWaitForMethodRequest,
) *dexpb.InvokeWaitForMethodRequest {
	t.Helper()
	select {
	case request := <-requests:
		return request
	case <-ctx.Done():
		require.FailNow(t, "WaitFor was not invoked", ctx.Err())
		return nil
	}
}

func receiveStepExecuteRequest(
	t *testing.T,
	ctx context.Context,
	requests <-chan *dexpb.InvokeExecuteMethodRequest,
) *dexpb.InvokeExecuteMethodRequest {
	t.Helper()
	select {
	case request := <-requests:
		return request
	case <-ctx.Done():
		require.FailNow(t, "Execute was not invoked", ctx.Err())
		return nil
	}
}

func requireStepWaitForState(t *testing.T, request *dexpb.InvokeWaitForMethodRequest) {
	t.Helper()
	require.ElementsMatch(t, []string{"ordinary", "attribute-map/one"}, attributeKeys(request.GetAttributes()))
	require.Equal(t, []string{"attribute-map/one"}, request.GetLoadedAttributeMapInstances())
	require.Equal(t, []string{stepStateLoadingCommands}, request.GetLoadedChannelNames())
	require.Equal(t, []string{"channel-map/"}, request.GetLoadedChannelMapInstances())
	require.Len(t, request.GetLoadedChannelMessages()[stepStateLoadingCommands].GetMessages(), 3)
	require.Len(t, request.GetLoadedChannelMessages()["channel-map/one"].GetMessages(), 1)
	require.Len(t, request.GetLoadedChannelMessages()["channel-map/two"].GetMessages(), 1)
	require.Equal(t, int32(3), request.GetChannelInfos()[stepStateLoadingCommands].GetSize())
	require.Equal(t, int32(1), request.GetChannelInfos()["other"].GetSize())
	require.Equal(t, int32(1), request.GetChannelInfos()["channel-map/one"].GetSize())
	require.Equal(t, int32(1), request.GetChannelInfos()["channel-map/two"].GetSize())
}

func requireStepExecuteState(t *testing.T, request *dexpb.InvokeExecuteMethodRequest) {
	t.Helper()
	require.ElementsMatch(
		t,
		[]string{"ordinary", "wait-effect", "attribute-map/two"},
		attributeKeys(request.GetAttributes()),
	)
	require.Equal(t, []string{"attribute-map/two"}, request.GetLoadedAttributeMapInstances())
	require.Equal(t, []string{stepStateLoadingCommands}, request.GetLoadedChannelNames())
	require.Equal(t, []string{"channel-map/one"}, request.GetLoadedChannelMapInstances())
	require.Len(t, request.GetLoadedChannelMessages()[stepStateLoadingCommands].GetMessages(), 1)
	require.Equal(
		t,
		"execute-delete",
		request.GetLoadedChannelMessages()[stepStateLoadingCommands].GetMessages()[0].GetValue().GetStringValue(),
	)
	require.Len(t, request.GetLoadedChannelMessages()["channel-map/one"].GetMessages(), 1)
	require.NotContains(t, request.GetLoadedChannelMessages(), "channel-map/two")
	require.Equal(t, int32(1), request.GetChannelInfos()[stepStateLoadingCommands].GetSize())
	require.Equal(t, int32(1), request.GetChannelInfos()[stepStateLoadingEffects].GetSize())
}

func requireStepStateLoadingEffects(
	t *testing.T,
	ctx context.Context,
	runtime *integRuntime,
	flowID string,
) {
	t.Helper()
	require.Eventually(t, func() bool {
		commands, commandsErr := runtime.FlowClient.GetChannelMessages(ctx, &dexpb.GetChannelMessagesRequest{
			FlowId:      flowID,
			ChannelName: stepStateLoadingCommands,
		})
		channelMapOne, channelMapErr := runtime.FlowClient.GetChannelMessages(ctx, &dexpb.GetChannelMessagesRequest{
			FlowId:      flowID,
			ChannelName: "channel-map/one",
		})
		effects, effectsErr := runtime.FlowClient.GetChannelMessages(ctx, &dexpb.GetChannelMessagesRequest{
			FlowId:      flowID,
			ChannelName: stepStateLoadingEffects,
		})
		attributes, attributesErr := runtime.FlowClient.GetAttributes(ctx, &dexpb.GetAttributesRequest{
			FlowId: flowID,
			Keys:   []string{"wait-effect", "execute-effect"},
		})
		return commandsErr == nil && channelMapErr == nil && effectsErr == nil && attributesErr == nil &&
			len(commands.GetMessages()) == 0 && len(channelMapOne.GetMessages()) == 0 &&
			len(effects.GetMessages()) == 2 && len(attributes.GetAttributes()) == 2
	}, 10*time.Second, 50*time.Millisecond)
	require.Len(t, waitForChannelMessages(t, ctx, runtime, flowID, "other", 1), 1)
	require.Len(t, waitForChannelMessages(t, ctx, runtime, flowID, "channel-map/two", 1), 1)
}

func requireStepDeletionHistory(
	t *testing.T,
	ctx context.Context,
	runtime *integRuntime,
	flowID string,
	runID string,
) {
	t.Helper()
	history, err := runtime.FlowClient.GetHistoryEvents(ctx, &dexpb.GetHistoryEventsRequest{
		FlowId: flowID,
		RunId:  runID,
	})
	require.NoError(t, err)
	var waitForDeletions []*dexpb.ChannelMessageDeletion
	var executeDeletions []*dexpb.ChannelMessageDeletion
	for _, historyEvent := range history.GetEvents() {
		if completed := historyEvent.GetStepWaitForCompleted(); completed.GetContext().GetStepType() == stepStateLoadingRootStep {
			waitForDeletions = completed.GetOutput().GetDeleteFromChannel()
		}
		if completed := historyEvent.GetStepExecuteCompleted(); completed.GetContext().GetStepType() == stepStateLoadingRootStep {
			executeDeletions = completed.GetOutput().GetDeleteFromChannel()
		}
	}
	require.Len(t, waitForDeletions, 2)
	require.Len(t, executeDeletions, 3)
}

func attributeKeys(attributes []*dexpb.KV) []string {
	keys := make([]string, 0, len(attributes))
	for _, attribute := range attributes {
		keys = append(keys, attribute.GetKey())
	}
	return keys
}
