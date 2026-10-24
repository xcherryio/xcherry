// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package step_cancellation

import (
	"context"
	"sync"
	"time"

	"github.com/superdurable/dex/gen/dexpb"
	"github.com/superdurable/dex/service/common/ptr"
	"google.golang.org/grpc"
)

const (
	FlowType            = "step-cancellation"
	Root                = "Root"
	ParentA             = "ParentA"
	ParentB             = "ParentB"
	Winner              = "Winner"
	SiblingWait         = "SiblingWait"
	SiblingExecute      = "SiblingExecute"
	GlobalWait          = "GlobalWait"
	GlobalWaitForMethod = "GlobalWaitForMethod"
	GlobalExecute       = "GlobalExecute"
	GlobalNoHeartbeat   = "GlobalNoHeartbeat"
	CanceledRecovery    = "CanceledRecovery"
	Final               = "Final"
	QueuedRoot          = "QueuedRoot"
	QueuedProducer      = "QueuedProducer"
	QueuedWinner        = "QueuedWinner"
	QueuedLoser         = "QueuedLoser"
	QueuedWaitingLoser  = "QueuedWaitingLoser"
	QueuedFinal         = "QueuedFinal"
	LocalRoot           = "LocalRoot"
	LocalWinner         = "LocalWinner"
	LocalLoser          = "LocalLoser"
	LocalFinal          = "LocalFinal"
	LocalTimeoutRoot    = "LocalTimeoutRoot"
	LocalTimeoutWinner  = "LocalTimeoutWinner"
	LocalTimeoutLoser   = "LocalTimeoutLoser"
	LocalTimeoutFinal   = "LocalTimeoutFinal"
	RpcA                = "cancelRpcA"
	RpcB                = "cancelRpcB"
	RpcSpawn            = "spawn"
	RpcCancel           = "cancel"
	RpcRejectSibling    = "reject-sibling"
	RpcGlobal           = "RpcGlobal"
	RpcFinal            = "RpcFinal"
)

type Handler struct {
	dexpb.UnimplementedWorkerServiceServer
	mu                       sync.Mutex
	canceledHandlers         map[string]bool
	hasNoHeartbeatLateReturn bool
	wasQueuedLoserExecuted   bool
	wasWaitingLoserExecuted  bool
	localTimeoutLoserAttempt int32
	heartbeatTimeoutSeconds  int32
}

func NewHandler() *Handler {
	return NewHandlerWithHeartbeatTimeout(3)
}

func NewHandlerWithHeartbeatTimeout(heartbeatTimeoutSeconds int32) *Handler {
	return &Handler{canceledHandlers: map[string]bool{}, heartbeatTimeoutSeconds: heartbeatTimeoutSeconds}
}

func (h *Handler) InvokeWorkerRPC(
	_ context.Context,
	request *dexpb.InvokeWorkerRPCRequest,
) (*dexpb.InvokeWorkerRPCResponse, error) {
	if request.GetInput().GetStringValue() == RpcRejectSibling {
		return &dexpb.InvokeWorkerRPCResponse{
			Output: request.GetInput(),
			StepDecision: &dexpb.StepDecision{
				CancelSiblingStepTypes: []string{RpcGlobal},
			},
		}, nil
	}
	if request.GetRpcName() == RpcA && request.GetInput().GetStringValue() == RpcCancel {
		return &dexpb.InvokeWorkerRPCResponse{
			Output: request.GetInput(),
			StepDecision: &dexpb.StepDecision{
				NextSteps:       []*dexpb.StepMovement{movement(RpcFinal, nil)},
				CancelStepTypes: []string{RpcGlobal, RpcFinal},
			},
		}, nil
	}
	if (request.GetRpcName() == RpcA || request.GetRpcName() == RpcB) &&
		request.GetInput().GetStringValue() == RpcSpawn {
		return &dexpb.InvokeWorkerRPCResponse{
			Output: request.GetInput(),
			StepDecision: &dexpb.StepDecision{NextSteps: []*dexpb.StepMovement{
				movement(RpcGlobal, nil),
			}},
		}, nil
	}
	return &dexpb.InvokeWorkerRPCResponse{Output: request.GetInput()}, nil
}

func (h *Handler) InvokeWaitForMethod(
	request *dexpb.InvokeWaitForMethodRequest,
	stream grpc.ServerStreamingServer[dexpb.InvokeWaitForMethodOutput],
) error {
	if request.GetStepType() == GlobalWaitForMethod {
		return h.streamWaitForHeartbeatsUntilCanceled(stream)
	}
	response, err := h.invokeWaitForMethod(stream.Context(), request)
	if err != nil {
		return err
	}
	return stream.Send(&dexpb.InvokeWaitForMethodOutput{
		Output: &dexpb.InvokeWaitForMethodOutput_Result{Result: response},
	})
}

func (h *Handler) streamWaitForHeartbeatsUntilCanceled(
	stream grpc.ServerStreamingServer[dexpb.InvokeWaitForMethodOutput],
) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := stream.Send(&dexpb.InvokeWaitForMethodOutput{
			Output: &dexpb.InvokeWaitForMethodOutput_Heartbeat{
				Heartbeat: &dexpb.StepMethodHeartbeat{},
			},
		}); err != nil {
			h.markCanceled(GlobalWaitForMethod)
			return err
		}
		select {
		case <-stream.Context().Done():
			h.markCanceled(GlobalWaitForMethod)
			return stream.Context().Err()
		case <-ticker.C:
		}
	}
}

func (h *Handler) invokeWaitForMethod(
	ctx context.Context,
	request *dexpb.InvokeWaitForMethodRequest,
) (*dexpb.InvokeWaitForMethodResponse, error) {
	switch request.GetStepType() {
	case Winner:
		return timerWait(2 * time.Second), nil
	case LocalTimeoutWinner:
		return timerWait(3 * time.Second), nil
	case SiblingWait:
		if request.GetContext().GetFromStepExecutionId() == ParentA+"-1" {
			return channelWait(), nil
		}
		return timerWait(3 * time.Second), nil
	case GlobalWait:
		return channelWait(), nil
	case RpcGlobal:
		return channelWait(), nil
	case QueuedWaitingLoser:
		return channelWait(), nil
	case RpcFinal:
		return timerWait(3 * time.Second), nil
	case GlobalWaitForMethod:
		<-ctx.Done()
		h.markCanceled(GlobalWaitForMethod)
		return nil, ctx.Err()
	default:
		return &dexpb.InvokeWaitForMethodResponse{}, nil
	}
}

func (h *Handler) InvokeExecuteMethod(
	request *dexpb.InvokeExecuteMethodRequest,
	stream grpc.ServerStreamingServer[dexpb.InvokeExecuteMethodOutput],
) error {
	if request.GetStepType() == SiblingExecute || request.GetStepType() == GlobalExecute {
		return h.streamExecuteHeartbeatsUntilCanceled(request.GetStepType(), stream)
	}
	response, err := h.invokeExecuteMethod(stream.Context(), request)
	if err != nil {
		return err
	}
	return stream.Send(&dexpb.InvokeExecuteMethodOutput{
		Output: &dexpb.InvokeExecuteMethodOutput_Result{Result: response},
	})
}

func (h *Handler) streamExecuteHeartbeatsUntilCanceled(
	stepType string,
	stream grpc.ServerStreamingServer[dexpb.InvokeExecuteMethodOutput],
) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := stream.Send(&dexpb.InvokeExecuteMethodOutput{
			Output: &dexpb.InvokeExecuteMethodOutput_Heartbeat{
				Heartbeat: &dexpb.StepMethodHeartbeat{},
			},
		}); err != nil {
			h.markCanceled(stepType)
			return err
		}
		select {
		case <-stream.Context().Done():
			h.markCanceled(stepType)
			return stream.Context().Err()
		case <-ticker.C:
		}
	}
}

func (h *Handler) invokeExecuteMethod(
	ctx context.Context,
	request *dexpb.InvokeExecuteMethodRequest,
) (*dexpb.InvokeExecuteMethodResponse, error) {
	switch request.GetStepType() {
	case Root:
		return next(movement(ParentA, nil)), nil
	case ParentA:
		return next(
			movement(Winner, asyncExecuteAfterWaitOptions()),
			movement(SiblingWait, nil),
			movement(SiblingExecute, syncExecuteOptions(h.heartbeatTimeoutSeconds)),
			movement(GlobalWait, nil),
			movement(GlobalWaitForMethod, syncWaitForOptions(h.heartbeatTimeoutSeconds)),
			movement(GlobalExecute, syncExecuteOptions(h.heartbeatTimeoutSeconds)),
			movement(GlobalNoHeartbeat, syncExecuteOptions(0)),
			movement(ParentB, nil),
		), nil
	case ParentB:
		return next(movement(SiblingWait, nil)), nil
	case Winner:
		return &dexpb.InvokeExecuteMethodResponse{StepDecision: &dexpb.StepDecision{
			NextSteps: []*dexpb.StepMovement{movement(Final, nil)},
			CancelStepTypes: []string{
				GlobalWait,
				GlobalWaitForMethod,
				GlobalExecute,
				GlobalNoHeartbeat,
			},
			CancelSiblingStepTypes: []string{SiblingWait, SiblingExecute},
		}}, nil
	case SiblingExecute, GlobalExecute:
		<-ctx.Done()
		h.markCanceled(request.GetStepType())
		return nil, ctx.Err()
	case GlobalNoHeartbeat:
		<-time.After(6 * time.Second)
		h.mu.Lock()
		h.hasNoHeartbeatLateReturn = true
		h.mu.Unlock()
		return &dexpb.InvokeExecuteMethodResponse{
			StepDecision: &dexpb.StepDecision{
				NextSteps: []*dexpb.StepMovement{movement(CanceledRecovery, nil)},
			},
			UpsertAttributes: []*dexpb.AttributeWrite{{
				Key:   "canceled-write",
				Value: &dexpb.Value{Kind: &dexpb.Value_StringValue{StringValue: "late"}},
			}},
		}, nil
	case Final, SiblingWait:
		return graceful(), nil
	case QueuedRoot:
		return next(
			movement(QueuedProducer, syncExecuteOptions(0)),
			movement(QueuedWinner, syncExecuteOptions(0)),
			movement(QueuedWaitingLoser, nil),
		), nil
	case QueuedProducer:
		time.Sleep(time.Second)
		return next(movement(QueuedLoser, nil)), nil
	case QueuedWinner:
		time.Sleep(2 * time.Second)
		return &dexpb.InvokeExecuteMethodResponse{StepDecision: &dexpb.StepDecision{
			NextSteps:       []*dexpb.StepMovement{movement(QueuedFinal, nil)},
			CancelStepTypes: []string{QueuedLoser, QueuedWaitingLoser},
		}}, nil
	case QueuedLoser:
		h.mu.Lock()
		h.wasQueuedLoserExecuted = true
		h.mu.Unlock()
		return graceful(), nil
	case QueuedWaitingLoser:
		h.mu.Lock()
		h.wasWaitingLoserExecuted = true
		h.mu.Unlock()
		return graceful(), nil
	case QueuedFinal:
		return graceful(), nil
	case LocalRoot:
		return next(
			movement(LocalWinner, asyncExecuteOptions()),
			movement(LocalLoser, asyncExecuteOptions()),
		), nil
	case LocalWinner:
		time.Sleep(time.Second)
		return &dexpb.InvokeExecuteMethodResponse{StepDecision: &dexpb.StepDecision{
			NextSteps:       []*dexpb.StepMovement{movement(LocalFinal, nil)},
			CancelStepTypes: []string{LocalLoser},
		}}, nil
	case LocalLoser:
		<-ctx.Done()
		h.markCanceled(LocalLoser)
		return nil, ctx.Err()
	case LocalFinal:
		return graceful(), nil
	case LocalTimeoutRoot:
		return next(
			movement(LocalTimeoutWinner, asyncExecuteAfterWaitOptions()),
			movement(LocalTimeoutLoser, asyncExecuteOptions()),
		), nil
	case LocalTimeoutWinner:
		return &dexpb.InvokeExecuteMethodResponse{StepDecision: &dexpb.StepDecision{
			NextSteps:       []*dexpb.StepMovement{movement(LocalTimeoutFinal, nil)},
			CancelStepTypes: []string{LocalTimeoutLoser},
		}}, nil
	case LocalTimeoutLoser:
		h.mu.Lock()
		h.localTimeoutLoserAttempt = max(
			h.localTimeoutLoserAttempt,
			request.GetContext().GetAttempt(),
		)
		h.mu.Unlock()
		<-ctx.Done()
		return nil, ctx.Err()
	case LocalTimeoutFinal:
		return graceful(), nil
	case RpcFinal:
		return forceComplete(), nil
	case CanceledRecovery:
		return nil, context.Canceled
	default:
		return graceful(), nil
	}
}

func (h *Handler) IsCancellationObserved(stepType string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.canceledHandlers[stepType]
}

func (h *Handler) HasNoHeartbeatLateReturn() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.hasNoHeartbeatLateReturn
}

func (h *Handler) WasQueuedLoserExecuted() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.wasQueuedLoserExecuted
}

func (h *Handler) WasWaitingLoserExecuted() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.wasWaitingLoserExecuted
}

func (h *Handler) LocalTimeoutLoserAttempt() int32 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.localTimeoutLoserAttempt
}

func (h *Handler) markCanceled(stepType string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.canceledHandlers[stepType] = true
}

func next(movements ...*dexpb.StepMovement) *dexpb.InvokeExecuteMethodResponse {
	return &dexpb.InvokeExecuteMethodResponse{
		StepDecision: &dexpb.StepDecision{NextSteps: movements},
	}
}

func movement(stepType string, options *dexpb.StepOptions) *dexpb.StepMovement {
	return &dexpb.StepMovement{StepType: stepType, StepOptions: options}
}

func syncExecuteOptions(heartbeatTimeoutSeconds int32) *dexpb.StepOptions {
	return &dexpb.StepOptions{
		SkipWaitFor:               true,
		ExecuteDurabilityOverride: dexpb.StepDurability_STEP_DURABILITY_SYNC,
		ExecuteTimeoutSeconds:     20,
		HeartbeatTimeoutSeconds:   heartbeatTimeoutSeconds,
	}
}

func syncWaitForOptions(heartbeatTimeoutSeconds int32) *dexpb.StepOptions {
	return &dexpb.StepOptions{
		WaitForDurabilityOverride: dexpb.StepDurability_STEP_DURABILITY_SYNC,
		WaitForTimeoutSeconds:     20,
		HeartbeatTimeoutSeconds:   heartbeatTimeoutSeconds,
	}
}

func asyncExecuteOptions() *dexpb.StepOptions {
	return &dexpb.StepOptions{
		SkipWaitFor:               true,
		ExecuteDurabilityOverride: dexpb.StepDurability_STEP_DURABILITY_ASYNC,
		ExecuteTimeoutSeconds:     20,
	}
}

func asyncExecuteAfterWaitOptions() *dexpb.StepOptions {
	return &dexpb.StepOptions{
		ExecuteDurabilityOverride: dexpb.StepDurability_STEP_DURABILITY_ASYNC,
		ExecuteTimeoutSeconds:     20,
	}
}

func timerWait(duration time.Duration) *dexpb.InvokeWaitForMethodResponse {
	return &dexpb.InvokeWaitForMethodResponse{WaitingCondition: &dexpb.WaitingCondition{
		WaitingConditionType: dexpb.WaitingConditionType_WAITING_CONDITION_TYPE_ALL_COMPLETED,
		TimerConditions: []*dexpb.TimerCondition{{
			DurationSeconds: int64(duration / time.Second),
		}},
	}}
}

func channelWait() *dexpb.InvokeWaitForMethodResponse {
	return &dexpb.InvokeWaitForMethodResponse{WaitingCondition: &dexpb.WaitingCondition{
		WaitingConditionType: dexpb.WaitingConditionType_WAITING_CONDITION_TYPE_ALL_COMPLETED,
		ChannelConditions: []*dexpb.ChannelCondition{{
			ChannelName: "never",
			AtLeast:     ptr.Any(int32(1)),
		}},
	}}
}

func graceful() *dexpb.InvokeExecuteMethodResponse {
	return &dexpb.InvokeExecuteMethodResponse{StepDecision: &dexpb.StepDecision{
		CloseDecision: &dexpb.CloseDecision{
			CloseDecisionType: dexpb.CloseDecisionType_CLOSE_DECISION_TYPE_GRACEFUL_COMPLETE,
		},
	}}
}

func forceComplete() *dexpb.InvokeExecuteMethodResponse {
	return &dexpb.InvokeExecuteMethodResponse{StepDecision: &dexpb.StepDecision{
		CloseDecision: &dexpb.CloseDecision{
			CloseDecisionType: dexpb.CloseDecisionType_CLOSE_DECISION_TYPE_FORCE_COMPLETE,
		},
	}}
}
