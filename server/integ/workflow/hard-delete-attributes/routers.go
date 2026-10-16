// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package hardDeleteAttributes

import (
	"context"
	"fmt"
	"sync"

	"github.com/superdurable/dex/gen/dexpb"
	"github.com/superdurable/dex/integ/workflow/common"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

// S0 starts a SubFlow, S1 writes the secret, S2 deletes hard-delete Attributes, and S3 completes.
const (
	WorkflowType          = "hard-delete-attributes"
	State0                = "S0"
	State1                = "S1"
	State2                = "S2"
	State3                = "S3"
	ProceedChannelName    = "proceed"
	CompletionChannelName = "completion"
	RPCNameRotateSecret   = "rotateSecret"

	SecretKey       = "secret"
	TokenMapName    = "tokens"
	TokenKey        = TokenMapName + "/a"
	PlainKey        = "plain"
	InitialSecret   = "initial-secret-4b1f0e"
	InitialToken    = "token-a-8c2d7a"
	StepSecret      = "step-secret-19a3c4"
	PlainAttribute  = "plain-visible-value"
	SubFlowSecret   = "subflow-secret-d42a77"
	AbsentAttribute = "<absent>"
	// RPCSecretPrefix keeps RPC-written secrets out of the RPC input, which history records.
	RPCSecretPrefix = "rpc-secret-7f3e0a-"
)

// StepOptions loads every token map instance for both Step methods.
var StepOptions = &dexpb.StepOptions{
	WaitForLoadAttributeMapInstances: []string{TokenMapName + "/"},
	ExecuteLoadAttributeMapInstances: []string{TokenMapName + "/"},
}

type handler struct {
	dexpb.UnimplementedWorkerServiceServer
	invokeData sync.Map
}

func NewHandler() *handler {
	return &handler{}
}

// SeenAttributes returns the Attribute values a Step method or RPC observed, keyed by Attribute key.
func (h *handler) SeenAttributes(invocation string) (map[string]string, bool) {
	seen, isFound := h.invokeData.Load(invocation)
	if !isFound {
		return nil, false
	}
	return seen.(map[string]string), true
}

func (h *handler) InvokeWaitForMethod(
	_ context.Context,
	request *dexpb.InvokeWaitForMethodRequest,
) (*dexpb.InvokeWaitForMethodResponse, error) {
	if request.GetFlowType() != WorkflowType {
		return nil, status.Error(codes.InvalidArgument, "invalid flow type")
	}
	if err := h.recordAttributes(request.GetStepType()+"_waitFor", request.GetAttributes()); err != nil {
		return nil, err
	}
	var channelName string
	switch request.GetStepType() {
	case State0:
		return &dexpb.InvokeWaitForMethodResponse{
			WaitingCondition: &dexpb.WaitingCondition{
				WaitingConditionType: dexpb.WaitingConditionType_WAITING_CONDITION_TYPE_ALL_COMPLETED,
				SubFlowConditions: []*dexpb.SubFlowCondition{{
					SubFlowType:   WorkflowType,
					StartStepType: State2,
					StepOptions:   StepOptions,
					Options: &dexpb.SubFlowOptions{
						Attributes: []*dexpb.AttributeWrite{HardDeleteStringWrite(SecretKey, SubFlowSecret)},
					},
				}},
			},
		}, nil
	case State2:
		channelName = ProceedChannelName
	case State3:
		channelName = CompletionChannelName
	default:
		return nil, status.Error(codes.InvalidArgument, "invalid step type")
	}
	return &dexpb.InvokeWaitForMethodResponse{
		WaitingCondition: &dexpb.WaitingCondition{
			WaitingConditionType: dexpb.WaitingConditionType_WAITING_CONDITION_TYPE_ALL_COMPLETED,
			ChannelConditions:    []*dexpb.ChannelCondition{{ChannelName: channelName}},
		},
	}, nil
}

func (h *handler) InvokeExecuteMethod(
	_ context.Context,
	request *dexpb.InvokeExecuteMethodRequest,
) (*dexpb.InvokeExecuteMethodResponse, error) {
	if request.GetFlowType() != WorkflowType {
		return nil, status.Error(codes.InvalidArgument, "invalid flow type")
	}
	if err := h.recordAttributes(request.GetStepType()+"_execute", request.GetAttributes()); err != nil {
		return nil, err
	}
	switch request.GetStepType() {
	case State0:
		return &dexpb.InvokeExecuteMethodResponse{
			StepDecision: &dexpb.StepDecision{CloseDecision: common.GracefulCompleteDecision(nil)},
		}, nil
	case State1:
		return &dexpb.InvokeExecuteMethodResponse{
			UpsertAttributes: []*dexpb.AttributeWrite{HardDeleteStringWrite(SecretKey, StepSecret)},
			StepDecision:     nextStepDecision(State2),
		}, nil
	case State2:
		return &dexpb.InvokeExecuteMethodResponse{
			UpsertAttributes: []*dexpb.AttributeWrite{
				HardDeleteNullWrite(SecretKey),
				HardDeleteNullWrite(TokenKey),
			},
			StepDecision: nextStepDecision(State3),
		}, nil
	case State3:
		return &dexpb.InvokeExecuteMethodResponse{
			StepDecision: &dexpb.StepDecision{CloseDecision: common.GracefulCompleteDecision(nil)},
		}, nil
	default:
		return nil, status.Error(codes.InvalidArgument, "invalid step type")
	}
}

func (h *handler) InvokeWorkerRPC(
	_ context.Context,
	request *dexpb.InvokeWorkerRPCRequest,
) (*dexpb.InvokeWorkerRPCResponse, error) {
	if request.GetFlowType() != WorkflowType || request.GetRpcName() != RPCNameRotateSecret {
		return nil, status.Error(codes.InvalidArgument, "invalid RPC")
	}
	secretSelector := request.GetInput().GetStringValue()
	nextSecret := RPCSecretPrefix + secretSelector
	if err := h.recordAttributes(RPCNameRotateSecret+"_"+secretSelector, request.GetAttributes()); err != nil {
		return nil, err
	}
	return &dexpb.InvokeWorkerRPCResponse{
		UpsertAttributes: []*dexpb.AttributeWrite{HardDeleteStringWrite(SecretKey, nextSecret)},
	}, nil
}

// recordAttributes stores what the Worker received and rejects any hard-delete blob reference.
func (h *handler) recordAttributes(invocation string, attributes []*dexpb.KV) error {
	seen := map[string]string{SecretKey: AbsentAttribute, TokenKey: AbsentAttribute}
	for _, attribute := range attributes {
		value := attribute.GetValue()
		switch {
		case value.GetInternalBlobIdForStringValue() != "" || value.GetInternalBlobIdForObjValue() != "":
			if attribute.GetKey() == SecretKey || attribute.GetKey() == TokenKey {
				return status.Error(codes.InvalidArgument, fmt.Sprintf("Attribute %q arrived as a blob reference", attribute.GetKey()))
			}
		case value.GetObjValue() != nil:
			seen[attribute.GetKey()] = string(value.GetObjValue().GetPayload())
		default:
			seen[attribute.GetKey()] = value.GetStringValue()
		}
	}
	h.invokeData.Store(invocation, seen)
	return nil
}

func nextStepDecision(stepType string) *dexpb.StepDecision {
	return &dexpb.StepDecision{
		NextSteps: []*dexpb.StepMovement{{StepType: stepType, StepOptions: StepOptions}},
	}
}

// HardDeleteStringWrite writes a hard-delete string Attribute.
func HardDeleteStringWrite(key string, value string) *dexpb.AttributeWrite {
	return &dexpb.AttributeWrite{
		Key:                key,
		Value:              &dexpb.Value{Kind: &dexpb.Value_StringValue{StringValue: value}},
		IsHardDeleteOnNull: true,
	}
}

// HardDeleteJSONWrite writes a hard-delete JSON object Attribute.
func HardDeleteJSONWrite(key string, payload string) *dexpb.AttributeWrite {
	return &dexpb.AttributeWrite{
		Key: key,
		Value: &dexpb.Value{Kind: &dexpb.Value_ObjValue{ObjValue: &dexpb.EncodedObject{
			Encoding: "json",
			Payload:  []byte(payload),
		}}},
		IsHardDeleteOnNull: true,
	}
}

// HardDeleteNullWrite permanently deletes a hard-delete Attribute.
func HardDeleteNullWrite(key string) *dexpb.AttributeWrite {
	return &dexpb.AttributeWrite{
		Key:                key,
		Value:              &dexpb.Value{Kind: &dexpb.Value_NullValue{NullValue: structpb.NullValue_NULL_VALUE}},
		IsHardDeleteOnNull: true,
	}
}
