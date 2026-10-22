// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package api

import (
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/superdurable/dex/gen/dexpb"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const blobReferenceField = "__dexBlobReference"

func mapSearchEntry(entry *dexpb.SearchFlowsResponseEntry) flowExecution {
	return flowExecution{
		FlowID:            entry.GetFlowId(),
		RunID:             entry.GetRunId(),
		FlowType:          entry.GetFlowType(),
		FlowStatus:        flowStatusLabel(entry.GetFlowStatus()),
		FlowStatusCode:    int32(entry.GetFlowStatus()),
		StartTime:         timestamp(entry.GetStartTime()),
		CloseTime:         timestamp(entry.GetCloseTime()),
		IndexedAttributes: mapKeyValues(entry.GetIndexedAttributes()),
	}
}

func mapSummary(summary *dexpb.GetFlowSummaryResponse) flowSummary {
	execution := summary.GetFlowExecutionId()
	return flowSummary{
		FlowID:         execution.GetFlowId(),
		RunID:          execution.GetRunId(),
		FirstRunID:     summary.GetFirstRunId(),
		RequestID:      summary.GetRequestId(),
		FlowType:       summary.GetFlowType(),
		FlowStatus:     flowStatusLabel(summary.GetFlowStatus()),
		FlowStatusCode: int32(summary.GetFlowStatus()),
		StartTime:      timestamp(summary.GetStartTime()),
		CloseTime:      timestamp(summary.GetCloseTime()),
	}
}

func mapStreamMessage(message *dexpb.StreamMessage) streamMessage {
	return streamMessage{
		Value:       dexValue(message.GetValue()),
		ResumeToken: message.GetResumeToken(),
		CreatedTime: timestamp(message.GetCreatedTime()),
		Source:      message.GetSource(),
	}
}

func mapHistoryEvent(event *dexpb.FlowHistoryEvent) (historyEvent, error) {
	var (
		eventType string
		payload   proto.Message
	)
	switch value := event.GetPayload().(type) {
	case *dexpb.FlowHistoryEvent_FlowStartedOrContinued:
		eventType, payload = "FlowStartedOrContinued", value.FlowStartedOrContinued
	case *dexpb.FlowHistoryEvent_FlowClosed:
		eventType, payload = "FlowClosed", value.FlowClosed
	case *dexpb.FlowHistoryEvent_StepWaitForCompleted:
		eventType, payload = "StepWaitForCompleted", value.StepWaitForCompleted
	case *dexpb.FlowHistoryEvent_StepWaitForFailed:
		eventType, payload = "StepWaitForFailed", value.StepWaitForFailed
	case *dexpb.FlowHistoryEvent_StepExecuteCompleted:
		eventType, payload = "StepExecuteCompleted", value.StepExecuteCompleted
	case *dexpb.FlowHistoryEvent_StepExecuteFailed:
		eventType, payload = "StepExecuteFailed", value.StepExecuteFailed
	case *dexpb.FlowHistoryEvent_StepWaitForPending:
		eventType, payload = "StepWaitForPending", value.StepWaitForPending
	case *dexpb.FlowHistoryEvent_StepExecutePending:
		eventType, payload = "StepExecutePending", value.StepExecutePending
	case *dexpb.FlowHistoryEvent_RpcExecutionCompleted:
		eventType, payload = "RpcExecutionCompleted", value.RpcExecutionCompleted
	case *dexpb.FlowHistoryEvent_ChannelExternalPublish:
		eventType, payload = "ChannelExternalPublish", value.ChannelExternalPublish
	case *dexpb.FlowHistoryEvent_TimeTravelFork:
		eventType, payload = "TimeTravelFork", value.TimeTravelFork
	default:
		return historyEvent{}, fmt.Errorf("history event %d has no Dex payload", event.GetEventId())
	}
	mappedPayload, err := protoMap(payload)
	if err != nil {
		return historyEvent{}, fmt.Errorf("map history event %d: %w", event.GetEventId(), err)
	}
	return historyEvent{
		EventID:   event.GetEventId(),
		EventTime: timestamp(event.GetEventTime()),
		Type:      eventType,
		Payload:   mappedPayload,
	}, nil
}

func mapFlowState(response *dexpb.GetFlowStateResponse) (flowState, error) {
	flowConfig, err := protoMap(response.GetFlowConfig())
	if err != nil {
		return flowState{}, fmt.Errorf("map flow config: %w", err)
	}
	wholeResponse, err := protoMap(response)
	if err != nil {
		return flowState{}, fmt.Errorf("map flow state: %w", err)
	}
	activeSteps := make([]activeStepExecution, 0, len(response.GetActiveStepExecutions()))
	for _, step := range response.GetActiveStepExecutions() {
		mapped, mapErr := mapActiveStep(step)
		if mapErr != nil {
			return flowState{}, mapErr
		}
		activeSteps = append(activeSteps, mapped)
	}
	return flowState{
		FlowConfig:             flowConfig,
		StickyAppWorkerID:      response.GetStickyAppWorkerId(),
		Attributes:             mapKeyValues(response.GetAttributes()),
		ActiveStepExecutions:   activeSteps,
		QueuedSteps:            interfaceSlice(wholeResponse["queuedSteps"]),
		PendingChannelMessages: interfaceMap(wholeResponse["pendingChannelMessages"]),
		CompletedSteps:         interfaceSlice(wholeResponse["completedSteps"]),
	}, nil
}

func mapActiveStep(step *dexpb.ActiveStepExecutionState) (activeStepExecution, error) {
	movement, err := protoMap(step.GetMovement())
	if err != nil {
		return activeStepExecution{}, err
	}
	waitingCondition, err := protoMap(step.GetWaitingCondition())
	if err != nil {
		return activeStepExecution{}, err
	}
	completedConditions, err := protoMap(step.GetCompletedConditions())
	if err != nil {
		return activeStepExecution{}, err
	}
	lastFailureInfo, err := protoMap(step.GetLastFailureInfo())
	if err != nil {
		return activeStepExecution{}, err
	}
	wholeStep, err := protoMap(step)
	if err != nil {
		return activeStepExecution{}, err
	}
	waitForRouting, err := protoMap(step.GetWaitForPullRouting())
	if err != nil {
		return activeStepExecution{}, err
	}
	executeRouting, err := protoMap(step.GetExecutePullRouting())
	if err != nil {
		return activeStepExecution{}, err
	}
	return activeStepExecution{
		WaitForPullRouting:  waitForRouting,
		ExecutePullRouting:  executeRouting,
		StepExecutionID:     step.GetStepExecutionId(),
		FromStepExecutionID: step.GetFromStepExecutionId(),
		StepType:            step.GetStepType(),
		Phase:               activeStepPhase(step.GetPhase()),
		Movement:            movement,
		WaitingCondition:    waitingCondition,
		CompletedConditions: completedConditions,
		StepExecutionLocals: mapKeyValues(step.GetStepExecutionLocals()),
		Timers:              interfaceSlice(wholeStep["timers"]),
		LastFailureInfo:     lastFailureInfo,
	}, nil
}

func mapKeyValues(values []*dexpb.KV) []keyValue {
	mapped := make([]keyValue, 0, len(values))
	for _, value := range values {
		mapped = append(mapped, keyValue{Key: value.GetKey(), Value: dexValue(value.GetValue())})
	}
	return mapped
}

func dexValue(value *dexpb.Value) interface{} {
	if value == nil {
		return nil
	}
	switch kind := value.GetKind().(type) {
	case *dexpb.Value_InternalBlobIdForStringValue:
		return mappedBlobReference(kind.InternalBlobIdForStringValue, "string")
	case *dexpb.Value_InternalBlobIdForObjValue:
		return mappedBlobReference(kind.InternalBlobIdForObjValue, "object")
	case *dexpb.Value_StringValue:
		return kind.StringValue
	case *dexpb.Value_IntValue:
		return kind.IntValue
	case *dexpb.Value_DoubleValue:
		return kind.DoubleValue
	case *dexpb.Value_BoolValue:
		return kind.BoolValue
	case *dexpb.Value_NullValue:
		return nil
	case *dexpb.Value_ObjValue:
		if kind.ObjValue.GetEncoding() == "json" {
			var decoded interface{}
			if err := json.Unmarshal(kind.ObjValue.GetPayload(), &decoded); err == nil {
				return decoded
			}
			return map[string]interface{}{
				"encoding": kind.ObjValue.GetEncoding(),
				"payload":  string(kind.ObjValue.GetPayload()),
			}
		}
		return map[string]interface{}{
			"encoding": kind.ObjValue.GetEncoding(),
			"payload":  base64.StdEncoding.EncodeToString(kind.ObjValue.GetPayload()),
		}
	default:
		return nil
	}
}

func protoMap(message proto.Message) (map[string]interface{}, error) {
	if message == nil || !message.ProtoReflect().IsValid() {
		return nil, nil
	}
	data, err := (protojson.MarshalOptions{UseEnumNumbers: true}).Marshal(message)
	if err != nil {
		return nil, err
	}
	result := make(map[string]interface{})
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, err
	}
	return normalizeBlobReferences(result).(map[string]interface{}), nil
}

func mappedBlobReference(id string, kind string) map[string]interface{} {
	return map[string]interface{}{
		blobReferenceField: map[string]interface{}{"id": id, "kind": kind},
	}
}

func normalizeBlobReferences(value interface{}) interface{} {
	switch current := value.(type) {
	case map[string]interface{}:
		if len(current) == 1 {
			if id, ok := current["internalBlobIdForStringValue"].(string); ok {
				return mappedBlobReference(id, "string")
			}
			if id, ok := current["internalBlobIdForObjValue"].(string); ok {
				return mappedBlobReference(id, "object")
			}
		}
		for key, entry := range current {
			current[key] = normalizeBlobReferences(entry)
		}
		return current
	case []interface{}:
		for index, entry := range current {
			current[index] = normalizeBlobReferences(entry)
		}
		return current
	default:
		return value
	}
}

func interfaceSlice(value interface{}) []interface{} {
	if value == nil {
		return []interface{}{}
	}
	values, ok := value.([]interface{})
	if !ok {
		return []interface{}{}
	}
	return values
}

func interfaceMap(value interface{}) map[string]interface{} {
	if value == nil {
		return map[string]interface{}{}
	}
	values, ok := value.(map[string]interface{})
	if !ok {
		return map[string]interface{}{}
	}
	return values
}

func timestamp(value *timestamppb.Timestamp) *string {
	if value == nil {
		return nil
	}
	formatted := value.AsTime().Format("2006-01-02T15:04:05.999999999Z07:00")
	return &formatted
}

func flowStatusLabel(status dexpb.FlowStatus) string {
	switch status {
	case dexpb.FlowStatus_FLOW_STATUS_RUNNING:
		return "Running"
	case dexpb.FlowStatus_FLOW_STATUS_COMPLETED:
		return "Completed"
	case dexpb.FlowStatus_FLOW_STATUS_FAILED:
		return "Failed"
	case dexpb.FlowStatus_FLOW_STATUS_SERVER_SIDE_TIMEOUT_INTERNAL_ONLY:
		return "Server-side timeout (internal only)"
	case dexpb.FlowStatus_FLOW_STATUS_TERMINATED:
		return "Terminated"
	case dexpb.FlowStatus_FLOW_STATUS_CANCELED:
		return "Canceled"
	case dexpb.FlowStatus_FLOW_STATUS_CONTINUED_AS_NEW:
		return "Continued as new"
	default:
		return "Unspecified"
	}
}

func activeStepPhase(phase dexpb.ActiveStepPhase) string {
	switch phase {
	case dexpb.ActiveStepPhase_ACTIVE_STEP_PHASE_ACTIVE:
		return "Active"
	case dexpb.ActiveStepPhase_ACTIVE_STEP_PHASE_WAITING:
		return "Waiting"
	default:
		return "Unspecified"
	}
}
