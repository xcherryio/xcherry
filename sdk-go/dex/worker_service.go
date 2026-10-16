// Legacy Materials in this file remain under their original licenses.
// See LEGACY_NOTICES.md.

// Modifications Copyright (c) 2026 Super Durable, Inc.
//
// Modifications after the Legacy Cutoff are licensed under the
// Sustainable Use License 1.0.
// Legacy Materials remain under their original licenses.
// See LICENSE and LEGACY_NOTICES.md.

package dex

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"

	"github.com/superdurable/dex/sdk-go/gen/dexpb"
	"google.golang.org/grpc/codes"
)

const timeoutHandlerStepType = "sys:timeout_handler"

type workerService struct {
	dexpb.UnimplementedWorkerServiceServer
	registry       *Registry
	hydrator       valueHydrator
	logger         Logger
	serverProtocol *negotiatedServerProtocol
}

func newWorkerService(
	registry *Registry,
	hydrator valueHydrator,
	logger Logger,
	serverProtocol *negotiatedServerProtocol,
) *workerService {
	if registry == nil {
		panic("dex: WorkerService requires registry")
	}
	if hydrator == nil {
		panic("dex: WorkerService requires value hydrator")
	}
	if serverProtocol == nil {
		panic("dex: WorkerService requires negotiated Server protocol")
	}
	return &workerService{
		registry:       registry,
		hydrator:       hydrator,
		logger:         resolveLogger(logger, nil),
		serverProtocol: serverProtocol,
	}
}

func (service *workerService) InvokeWaitForMethod(
	request *dexpb.InvokeWaitForMethodRequest,
	stream dexpb.WorkerService_InvokeWaitForMethodServer,
) (err error) {
	defer func() {
		if finalErr := finishWorkerCall(service.logger, recover(), err); finalErr != nil {
			err = finalErr
		}
	}()
	return service.invokeWaitForMethod(request, newWaitForOutputStream(stream))
}

func (service *workerService) invokeWaitForMethod(
	request *dexpb.InvokeWaitForMethodRequest,
	output *waitForOutputStream,
) error {
	if request == nil {
		return newWorkerFailure(
			codes.InvalidArgument,
			fmt.Errorf("dex: Worker request is nil"),
		)
	}
	if err := validateStepWorkerRequest(
		request.Context,
		request.FlowType,
		request.StepType,
		request.StepInput,
	); err != nil {
		return newWorkerFailure(codes.InvalidArgument, err)
	}
	flow, step, err := service.lookupStep(request.FlowType, request.StepType)
	if err != nil {
		return err
	}
	if step.skipWaitFor {
		return newWorkerFailure(
			codes.FailedPrecondition,
			fmt.Errorf("dex: step %q is execute-only", step.stepType),
		)
	}
	if err := validateKVEnvelopes("attribute", request.Attributes); err != nil {
		return newWorkerFailure(codes.InvalidArgument, err)
	}
	if err := service.hydrator.HydrateValuesInPlace(
		output.stream.Context(),
		waitForRequestValuePointers(request),
	); err != nil {
		return err
	}
	input, err := decodeHandlerInput(request.StepInput, step.inputType)
	if err != nil {
		return newWorkerFailure(codes.InvalidArgument, err)
	}
	invocation, err := newInvocationContext(
		output.stream.Context(),
		invocationWaitFor,
		flow,
		output,
		request.Context,
		request.Attributes,
		nil,
		nil,
		request.ChannelInfos,
		request.LoadedChannelMessages,
		request.LoadedAttributeMapInstances,
		request.LoadedChannelNames,
		request.LoadedChannelMapInstances,
	)
	if err != nil {
		return newWorkerFailure(codes.InvalidArgument, err)
	}
	wait, err := callWaitForHandler(step, invocation, input)
	if err != nil {
		return err
	}
	waiting, err := mapRegisteredWait(service.registry, flow, wait)
	if err != nil {
		return newWorkerFailure(codes.InvalidArgument, &InvalidStepResultError{
			FlowType: flow.flowType,
			StepType: step.stepType,
			Method:   "WaitFor",
			Err:      err,
		})
	}
	attributeWrites, actionPermissionMappings :=
		invocation.mappedAttributeWritesWithActionPermissionMappings()
	if err := service.serverProtocol.requireHardDeleteAttributeSupport(attributeWrites); err != nil {
		return err
	}
	for _, subFlow := range waiting.GetSubFlowConditions() {
		if err := service.serverProtocol.requireHardDeleteAttributeSupport(subFlow.GetOptions().GetAttributes()); err != nil {
			return err
		}
	}
	return output.sendResult(&dexpb.InvokeWaitForMethodResponse{
		UpsertAttributes:         attributeWrites,
		WaitingCondition:         waiting,
		UpsertStepExeLocals:      invocation.mappedLocalWrites(),
		RecordEvents:             invocation.recordedEvents,
		PublishToChannel:         invocation.publications,
		DeleteFromChannel:        invocation.deletions,
		ActionPermissionMappings: actionPermissionMappings,
	})
}

func (service *workerService) InvokeExecuteMethod(
	request *dexpb.InvokeExecuteMethodRequest,
	stream dexpb.WorkerService_InvokeExecuteMethodServer,
) (err error) {
	defer func() {
		if finalErr := finishWorkerCall(service.logger, recover(), err); finalErr != nil {
			err = finalErr
		}
	}()
	return service.invokeExecuteMethod(request, newExecuteOutputStream(stream))
}

func (service *workerService) invokeExecuteMethod(
	request *dexpb.InvokeExecuteMethodRequest,
	output *executeOutputStream,
) error {
	if request == nil {
		return newWorkerFailure(
			codes.InvalidArgument,
			fmt.Errorf("dex: Worker request is nil"),
		)
	}
	if request.StepType == timeoutHandlerStepType {
		return service.invokeTimeoutHandler(request, output)
	}
	if err := validateStepWorkerRequest(
		request.Context,
		request.FlowType,
		request.StepType,
		request.StepInput,
	); err != nil {
		return newWorkerFailure(codes.InvalidArgument, err)
	}
	flow, step, err := service.lookupStep(request.FlowType, request.StepType)
	if err != nil {
		return err
	}
	if err := validateKVEnvelopes("attribute", request.Attributes); err != nil {
		return newWorkerFailure(codes.InvalidArgument, err)
	}
	if err := validateKVEnvelopes("step-execution local", request.StepExeLocals); err != nil {
		return newWorkerFailure(codes.InvalidArgument, err)
	}
	valuePointers, err := executeRequestValuePointers(request)
	if err != nil {
		return newWorkerFailure(codes.InvalidArgument, err)
	}
	if err := service.hydrator.HydrateValuesInPlace(
		output.stream.Context(), valuePointers,
	); err != nil {
		return err
	}
	input, err := decodeHandlerInput(request.StepInput, step.inputType)
	if err != nil {
		return newWorkerFailure(codes.InvalidArgument, err)
	}
	invocation, err := newInvocationContext(
		output.stream.Context(),
		invocationExecute,
		flow,
		output,
		request.Context,
		request.Attributes,
		request.StepExeLocals,
		request.ConditionResults,
		request.ChannelInfos,
		request.LoadedChannelMessages,
		request.LoadedAttributeMapInstances,
		request.LoadedChannelNames,
		request.LoadedChannelMapInstances,
	)
	if err != nil {
		return newWorkerFailure(codes.InvalidArgument, err)
	}
	decision, err := callExecuteHandler(step, invocation, input)
	if err != nil {
		return err
	}
	mapped, err := mapRegisteredDecision(flow, decision)
	if err != nil {
		return newWorkerFailure(codes.InvalidArgument, &InvalidStepResultError{
			FlowType: flow.flowType,
			StepType: step.stepType,
			Method:   "Execute",
			Err:      err,
		})
	}
	attributeWrites, actionPermissionMappings :=
		invocation.mappedAttributeWritesWithActionPermissionMappings()
	if err := service.serverProtocol.requireHardDeleteAttributeSupport(attributeWrites); err != nil {
		return err
	}
	return output.sendResult(&dexpb.InvokeExecuteMethodResponse{
		StepDecision:             mapped,
		UpsertAttributes:         attributeWrites,
		RecordEvents:             invocation.recordedEvents,
		PublishToChannel:         invocation.publications,
		DeleteFromChannel:        invocation.deletions,
		ActionPermissionMappings: actionPermissionMappings,
	})
}

func (service *workerService) invokeTimeoutHandler(
	request *dexpb.InvokeExecuteMethodRequest,
	output *executeOutputStream,
) error {
	if err := validateWorkerContext(request.Context, true); err != nil {
		return newWorkerFailure(codes.InvalidArgument, err)
	}
	if request.FlowType == "" || request.StepInput != nil {
		return newWorkerFailure(
			codes.InvalidArgument,
			fmt.Errorf("dex: timeout handler request has invalid flow or input"),
		)
	}
	flow, isRegistered := service.registry.lookupFlow(request.FlowType)
	if !isRegistered {
		return newWorkerFailure(
			codes.NotFound,
			fmt.Errorf("dex: flow %q is not registered", request.FlowType),
		)
	}
	if flow.timeoutHandler == nil {
		return newWorkerFailure(
			codes.FailedPrecondition,
			fmt.Errorf("dex: flow %q has no timeout handler", request.FlowType),
		)
	}
	if err := validateKVEnvelopes("attribute", request.Attributes); err != nil {
		return newWorkerFailure(codes.InvalidArgument, err)
	}
	if err := validateKVEnvelopes("step-execution local", request.StepExeLocals); err != nil {
		return newWorkerFailure(codes.InvalidArgument, err)
	}
	valuePointers, err := executeRequestValuePointers(request)
	if err != nil {
		return newWorkerFailure(codes.InvalidArgument, err)
	}
	if err := service.hydrator.HydrateValuesInPlace(
		output.stream.Context(), valuePointers,
	); err != nil {
		return err
	}
	invocation, err := newInvocationContext(
		output.stream.Context(),
		invocationExecute,
		flow,
		output,
		request.Context,
		request.Attributes,
		request.StepExeLocals,
		request.ConditionResults,
		request.ChannelInfos,
		request.LoadedChannelMessages,
		request.LoadedAttributeMapInstances,
		request.LoadedChannelNames,
		request.LoadedChannelMapInstances,
	)
	if err != nil {
		return newWorkerFailure(codes.InvalidArgument, err)
	}
	decision, err := callFlowTimeoutHandler(flow.timeoutHandler, invocation)
	if err != nil {
		return err
	}
	mapped, err := mapRegisteredDecision(flow, decision)
	if err != nil {
		return newWorkerFailure(codes.InvalidArgument, &InvalidStepResultError{
			FlowType: flow.flowType,
			StepType: timeoutHandlerStepType,
			Method:   "Execute",
			Err:      err,
		})
	}
	attributeWrites, actionPermissionMappings :=
		invocation.mappedAttributeWritesWithActionPermissionMappings()
	if err := service.serverProtocol.requireHardDeleteAttributeSupport(attributeWrites); err != nil {
		return err
	}
	return output.sendResult(&dexpb.InvokeExecuteMethodResponse{
		StepDecision:             mapped,
		UpsertAttributes:         attributeWrites,
		UpsertStepExeLocals:      invocation.mappedLocalWrites(),
		RecordEvents:             invocation.recordedEvents,
		PublishToChannel:         invocation.publications,
		DeleteFromChannel:        invocation.deletions,
		ActionPermissionMappings: actionPermissionMappings,
	})
}

func (service *workerService) InvokeWorkerRPC(
	ctx context.Context,
	request *dexpb.InvokeWorkerRPCRequest,
) (response *dexpb.InvokeWorkerRPCResponse, err error) {
	defer func() {
		if finalErr := finishWorkerCall(service.logger, recover(), err); finalErr != nil {
			response = nil
			err = finalErr
		}
	}()
	return service.invokeWorkerRPC(ctx, request)
}

func (service *workerService) invokeWorkerRPC(
	ctx context.Context,
	request *dexpb.InvokeWorkerRPCRequest,
) (*dexpb.InvokeWorkerRPCResponse, error) {
	if err := validateRPCWorkerRequest(request); err != nil {
		return nil, newWorkerFailure(codes.InvalidArgument, err)
	}
	flow, found := service.registry.lookupFlow(request.FlowType)
	if !found {
		return nil, newWorkerFailure(
			codes.NotFound,
			fmt.Errorf("dex: flow %q is not registered", request.FlowType),
		)
	}
	rpc, found := flow.lookupRPC(request.RpcName)
	if !found {
		return nil, newWorkerFailure(
			codes.NotFound,
			fmt.Errorf("dex: RPC %q is not registered in flow %q", request.RpcName, request.FlowType),
		)
	}
	if err := validateKVEnvelopes("attribute", request.Attributes); err != nil {
		return nil, newWorkerFailure(codes.InvalidArgument, err)
	}
	if err := service.hydrator.HydrateValuesInPlace(
		ctx,
		rpcRequestValuePointers(request),
	); err != nil {
		return nil, err
	}
	input, err := decodeHandlerInput(request.Input, rpc.input)
	if err != nil {
		return nil, newWorkerFailure(codes.InvalidArgument, err)
	}
	invocation, err := newInvocationContext(
		ctx,
		invocationRPC,
		flow,
		nil,
		request.Context,
		request.Attributes,
		nil,
		nil,
		request.ChannelInfos,
		request.LoadedChannelMessages,
		request.LoadedAttributeMapInstances,
		request.LoadedChannelNames,
		request.LoadedChannelMapInstances,
	)
	if err != nil {
		return nil, newWorkerFailure(codes.InvalidArgument, err)
	}
	result, err := callRPCHandler(rpc, invocation, input)
	if err != nil {
		return nil, err
	}
	response, err := mapRegisteredRPCResult(flow, result)
	if err != nil {
		return nil, newWorkerFailure(codes.InvalidArgument, &InvalidStepResultError{
			FlowType: flow.flowType,
			Method:   "RPC " + request.RpcName,
			Err:      err,
		})
	}
	attributeWrites, actionPermissionMappings :=
		invocation.mappedAttributeWritesWithActionPermissionMappings()
	if err := service.serverProtocol.requireHardDeleteAttributeSupport(attributeWrites); err != nil {
		return nil, err
	}
	response.UpsertAttributes = attributeWrites
	response.ActionPermissionMappings = actionPermissionMappings
	response.RecordEvents = invocation.recordedEvents
	response.PublishToChannel = invocation.publications
	response.DeleteFromChannel = invocation.deletions
	return response, nil
}

func (service *workerService) lookupStep(
	flowType string,
	stepType string,
) (*registeredFlow, *registeredStep, error) {
	flow, found := service.registry.lookupFlow(flowType)
	if !found {
		return nil, nil, newWorkerFailure(
			codes.NotFound,
			fmt.Errorf("dex: flow %q is not registered", flowType),
		)
	}
	step, found := flow.lookupStep(stepType)
	if !found {
		return nil, nil, newWorkerFailure(
			codes.NotFound,
			fmt.Errorf("dex: step %q is not registered in flow %q", stepType, flowType),
		)
	}
	return flow, step, nil
}

func callWaitForHandler(
	step *registeredStep,
	invocation *invocationContext,
	input any,
) (wait *Wait, err error) {
	defer func() {
		err = errors.Join(err, invocation.finish())
	}()
	return step.handler.waitFor(invocation, input)
}

func callExecuteHandler(
	step *registeredStep,
	invocation *invocationContext,
	input any,
) (decision *StepDecision, err error) {
	defer func() {
		err = errors.Join(err, invocation.finish())
	}()
	return step.handler.execute(invocation, input)
}

func callFlowTimeoutHandler(
	handler FlowTimeoutHandler,
	invocation *invocationContext,
) (decision *StepDecision, err error) {
	defer func() {
		err = errors.Join(err, invocation.finish())
	}()
	return handler.HandleTimeout(invocation)
}

func callRPCHandler(
	rpc *registeredRPC,
	invocation *invocationContext,
	input any,
) (result rpcResult, err error) {
	defer func() {
		err = errors.Join(err, invocation.finish())
	}()
	return rpc.invoke(invocation, input)
}

func validateStepWorkerRequest(
	metadata *dexpb.Context,
	flowType string,
	stepType string,
	input *dexpb.Value,
) error {
	if err := validateWorkerContext(metadata, true); err != nil {
		return err
	}
	if flowType == "" || stepType == "" || input == nil {
		return fmt.Errorf("dex: Worker step request is missing flow, step, or input")
	}
	return nil
}

func validateRPCWorkerRequest(request *dexpb.InvokeWorkerRPCRequest) error {
	if request == nil {
		return fmt.Errorf("dex: Worker RPC request is nil")
	}
	if err := validateWorkerContext(request.Context, false); err != nil {
		return err
	}
	if request.FlowType == "" || request.RpcName == "" || request.Input == nil {
		return fmt.Errorf("dex: Worker RPC request is missing flow, RPC, or input")
	}
	return nil
}

func validateWorkerContext(metadata *dexpb.Context, step bool) error {
	if metadata == nil {
		return fmt.Errorf("dex: Worker request Context is nil")
	}
	if metadata.FlowId == "" || metadata.RunId == "" ||
		metadata.FlowStartedTimestamp == 0 {
		return fmt.Errorf("dex: Worker request Context is missing flow metadata")
	}
	if step && (metadata.StepExecutionId == "" || metadata.Attempt < 1 ||
		metadata.FirstAttemptTimestamp == 0) {
		return fmt.Errorf("dex: Worker request Context is missing step attempt metadata")
	}
	return nil
}

func validateKVEnvelopes(kind string, values []*dexpb.KV) error {
	seen := make(map[string]struct{}, len(values))
	for index, value := range values {
		if value == nil || value.Key == "" || value.Value == nil {
			return fmt.Errorf("dex: invalid %s at index %d", kind, index)
		}
		if _, found := seen[value.Key]; found {
			return fmt.Errorf("dex: duplicate %s key %q", kind, value.Key)
		}
		seen[value.Key] = struct{}{}
	}
	return nil
}

func rpcRequestValuePointers(request *dexpb.InvokeWorkerRPCRequest) []flowValuePointer {
	flowID := request.GetContext().GetFlowId()
	valuePointers := stepRequestValuePointers(flowID, "RPC input", &request.Input, request.Attributes)
	return appendLoadedChannelMessageValuePointers(flowID, valuePointers, request.LoadedChannelMessages)
}

func waitForRequestValuePointers(
	request *dexpb.InvokeWaitForMethodRequest,
) []flowValuePointer {
	flowID := request.GetContext().GetFlowId()
	valuePointers := stepRequestValuePointers(flowID, "Step input", &request.StepInput, request.Attributes)
	if request.Context.LastHeartbeatValue != nil {
		valuePointers = append(valuePointers, flowValuePointer{
			flowID: flowID, valueDescription: "last heartbeat", valuePointer: &request.Context.LastHeartbeatValue,
		})
	}
	return appendLoadedChannelMessageValuePointers(flowID, valuePointers, request.LoadedChannelMessages)
}

func executeRequestValuePointers(
	request *dexpb.InvokeExecuteMethodRequest,
) ([]flowValuePointer, error) {
	flowID := request.GetContext().GetFlowId()
	valuePointers := stepRequestValuePointers(flowID, "Step input", &request.StepInput, request.Attributes)
	if request.Context.LastHeartbeatValue != nil {
		valuePointers = append(valuePointers, flowValuePointer{
			flowID: flowID, valueDescription: "last heartbeat", valuePointer: &request.Context.LastHeartbeatValue,
		})
	}
	for _, local := range request.StepExeLocals {
		valuePointers = append(valuePointers, flowValuePointer{
			flowID: flowID, valueDescription: fmt.Sprintf("Step-execution local %q", local.Key), valuePointer: &local.Value,
		})
	}
	valuePointers = appendLoadedChannelMessageValuePointers(flowID, valuePointers, request.LoadedChannelMessages)
	if request.ConditionResults == nil {
		return valuePointers, nil
	}
	for index, result := range request.ConditionResults.ChannelResults {
		if result == nil {
			return nil, fmt.Errorf("dex: channel result at index %d is nil", index)
		}
		for valueIndex := range result.Values {
			valuePointers = append(valuePointers, flowValuePointer{
				flowID:           flowID,
				valueDescription: fmt.Sprintf("Channel %q condition %q value at index %d", result.ChannelName, result.ConditionId, valueIndex),
				valuePointer:     &result.Values[valueIndex],
			})
		}
	}
	for resultIndex, result := range request.ConditionResults.SubFlowResults {
		if result == nil {
			return nil, fmt.Errorf("dex: SubFlow result at index %d is nil", resultIndex)
		}
		for completionIndex, completion := range result.Results {
			if completion == nil {
				return nil, fmt.Errorf(
					"dex: SubFlow result %d completion %d is nil",
					resultIndex,
					completionIndex,
				)
			}
			valuePointers = append(valuePointers, flowValuePointer{
				flowID:           flowID,
				valueDescription: fmt.Sprintf("SubFlow result at index %d completion at index %d Step output", resultIndex, completionIndex),
				valuePointer:     &completion.CompletedStepOutput,
			})
		}
	}
	return valuePointers, nil
}

func stepRequestValuePointers(
	flowID string,
	inputDescription string,
	input **dexpb.Value,
	attributes []*dexpb.KV,
) []flowValuePointer {
	valuePointers := make([]flowValuePointer, 0, 1+len(attributes))
	if *input != nil {
		valuePointers = append(valuePointers, flowValuePointer{
			flowID: flowID, valueDescription: inputDescription, valuePointer: input,
		})
	}
	for _, attribute := range attributes {
		valuePointers = append(valuePointers, flowValuePointer{
			flowID: flowID, valueDescription: fmt.Sprintf("Attribute %q", attribute.Key), valuePointer: &attribute.Value,
		})
	}
	return valuePointers
}

func appendLoadedChannelMessageValuePointers(
	flowID string,
	valuePointers []flowValuePointer,
	loadedMessages map[string]*dexpb.ChannelValues,
) []flowValuePointer {
	channelNames := make([]string, 0, len(loadedMessages))
	for channelName := range loadedMessages {
		channelNames = append(channelNames, channelName)
	}
	sort.Strings(channelNames)
	for _, channelName := range channelNames {
		for messageIndex, message := range loadedMessages[channelName].Messages {
			valuePointers = append(valuePointers, flowValuePointer{
				flowID:           flowID,
				valueDescription: fmt.Sprintf("Channel %q message %q at index %d", channelName, message.MessageId, messageIndex),
				valuePointer:     &message.Value,
			})
		}
	}
	return valuePointers
}

func decodeHandlerInput(value *dexpb.Value, inputType reflect.Type) (any, error) {
	decoded, err := decodeReflectValue(value, inputType)
	if err != nil {
		return nil, err
	}
	return decoded.Interface(), nil
}
