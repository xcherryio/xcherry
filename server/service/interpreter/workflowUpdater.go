// Legacy Materials in this file remain under their original licenses.
// See LEGACY_NOTICES.md.

// Modifications Copyright (c) 2026 Super Durable, Inc.
//
// Modifications after the Legacy Cutoff are licensed under the
// Sustainable Use License 1.0.
// Legacy Materials remain under their original licenses.
// See LICENSE and LEGACY_NOTICES.md.

package interpreter

import (
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/superdurable/dex/config"
	"github.com/superdurable/dex/gen/dexpb"
	"github.com/superdurable/dex/service"
	"github.com/superdurable/dex/service/common/event"
	"github.com/superdurable/dex/service/common/rpc"
	interpreterconfig "github.com/superdurable/dex/service/interpreter/config"
	"github.com/superdurable/dex/service/interpreter/cont"
	"github.com/superdurable/dex/service/interpreter/interfaces"
	"google.golang.org/protobuf/types/known/emptypb"
)

type WorkflowUpdater struct {
	activities            *Activities
	apiCfg                *config.ApiConfig
	persistenceManager    *PersistenceManager
	provider              interfaces.WorkflowProvider
	globalVersioner       *GlobalVersioner
	continueAsNewer       *ContinueAsNewer
	continueAsNewCounter  *cont.ContinueAsNewCounter
	channelStore          *ChannelStore
	signalReceiver        *SignalReceiver
	terminalCoordinator   *TerminalCoordinator
	stepRequestQueue      *StepRequestQueue
	stepExecutionCounter  *StepExecutionCounter
	stepExecutionRegistry *StepExecutionRegistry
	flowConfiger          *interpreterconfig.FlowConfiger
	basicInfo             service.BasicInfo
}

func NewWorkflowUpdater(
	apiCfg *config.ApiConfig,
	activities *Activities,
	ctx interfaces.UnifiedContext,
	provider interfaces.WorkflowProvider,
	globalVersioner *GlobalVersioner,
	persistenceManager *PersistenceManager,
	stepRequestQueue *StepRequestQueue,
	continueAsNewer *ContinueAsNewer,
	continueAsNewCounter *cont.ContinueAsNewCounter,
	channelStore *ChannelStore,
	signalReceiver *SignalReceiver,
	terminalCoordinator *TerminalCoordinator,
	stepExecutionCounter *StepExecutionCounter,
	stepExecutionRegistry *StepExecutionRegistry,
	flowConfiger *interpreterconfig.FlowConfiger,
	basicInfo service.BasicInfo,
) error {
	if apiCfg == nil || activities == nil || provider == nil || globalVersioner == nil ||
		persistenceManager == nil || stepRequestQueue == nil ||
		continueAsNewer == nil ||
		continueAsNewCounter == nil || channelStore == nil ||
		signalReceiver == nil || terminalCoordinator == nil ||
		stepExecutionCounter == nil || stepExecutionRegistry == nil || flowConfiger == nil {
		panic("WorkflowUpdater requires non-nil dependencies")
	}
	updater := &WorkflowUpdater{
		activities:            activities,
		apiCfg:                apiCfg,
		persistenceManager:    persistenceManager,
		provider:              provider,
		globalVersioner:       globalVersioner,
		continueAsNewer:       continueAsNewer,
		continueAsNewCounter:  continueAsNewCounter,
		channelStore:          channelStore,
		signalReceiver:        signalReceiver,
		terminalCoordinator:   terminalCoordinator,
		stepRequestQueue:      stepRequestQueue,
		stepExecutionCounter:  stepExecutionCounter,
		stepExecutionRegistry: stepExecutionRegistry,
		flowConfiger:          flowConfiger,
		basicInfo:             basicInfo,
	}
	if err := provider.SetInvokeRPCUpdateHandler(
		ctx,
		updater.validateWorkerRpc,
		updater.handleWorkerRpc,
	); err != nil {
		return err
	}
	if err := provider.SetDeleteChannelMessageUpdateHandler(
		ctx,
		updater.validateDeleteChannelMessage,
		updater.handleDeleteChannelMessage,
	); err != nil {
		return err
	}
	if err := provider.SetWaitForStepCompletionUpdateHandler(
		ctx,
		updater.validateWaitForStepCompletion,
		updater.handleWaitForStepCompletion,
	); err != nil {
		return err
	}
	if err := provider.SetWaitForAttributeUpdateHandler(
		ctx,
		updater.validateWaitForAttribute,
		updater.handleWaitForAttribute,
	); err != nil {
		return err
	}
	return nil
}

type stepCompletionWait struct {
	updater             *WorkflowUpdater
	request             *dexpb.WaitForStepCompletionRequest
	timeout             internalHandlerTimeout
	stepExecutionNumber int32
	matched             bool
}

type attributeWait struct {
	updater      *WorkflowUpdater
	request      *dexpb.WaitForAttributeRequest
	timeout      internalHandlerTimeout
	matchedValue *dexpb.Value
	matchErr     error
}

type internalHandlerTimeout struct {
	updater  *WorkflowUpdater
	deadline time.Time
	timer    interfaces.Future
}

func (u *WorkflowUpdater) handleWorkerRpc(
	ctx interfaces.UnifiedContext,
	input *dexpb.InvokeRPCRequest,
) (output *dexpb.InvokeRpcUpdateResult, err error) {
	u.continueAsNewer.IncreaseInflightOperation()
	defer u.continueAsNewer.DecreaseInflightOperation()
	u.signalReceiver.DrainAllReceivedButUnprocessedSignals(ctx)

	info := u.provider.GetWorkflowInfo(ctx)
	rpcExecutionStartTime := u.provider.Now(ctx).UnixMilli()
	defer func() {
		if !u.provider.IsReplaying(ctx) {
			event.Handle(event.Event{
				FlowId:             info.WorkflowExecution.ID,
				RunId:              info.WorkflowExecution.RunID,
				FlowType:           u.basicInfo.FlowType,
				RpcName:            input.GetRpcName(),
				EventType:          event.EventTypeRPCExecution,
				StartTimestampInMs: rpcExecutionStartTime,
				Attributes:         u.persistenceManager.GetAllAttributes(),
			})
		}
	}()

	keysToLock, err := normalizeLockKeys(input.GetLockAttributeKeys())
	if err != nil {
		return nil, u.provider.NewUpdateError(
			dexpb.UpdateErrorType_UPDATE_ERROR_TYPE_INVALID_ARGUMENT,
			err.Error(),
		)
	}
	selection, err := rpc.ValidateAndSortSelections(
		input.GetLoadAttributeMapInstances(),
		input.GetLoadChannelNames(),
		input.GetLoadChannelMapInstances(),
	)
	if err != nil {
		return nil, u.provider.NewUpdateError(
			dexpb.UpdateErrorType_UPDATE_ERROR_TYPE_INVALID_ARGUMENT,
			err.Error(),
		)
	}
	attributes, locked := u.persistenceManager.TryLoadRPCAttributes(
		keysToLock,
		selection.AttributeMapInstances,
	)
	if !locked {
		return nil, u.rpcLockError()
	}

	rpcPrep := &dexpb.PrepareRpcQueryResponse{
		Attributes:                  attributes,
		RunId:                       info.WorkflowExecution.RunID,
		FlowStartedTimestamp:        info.WorkflowStartTime.Unix(),
		FlowType:                    u.basicInfo.FlowType,
		WorkerTarget:                u.flowConfiger.GetWorkerTarget(),
		ChannelInfos:                u.channelStore.GetInfos(),
		LoadedChannelMessages:       u.channelStore.GetLoadedMessages(selection.ChannelNames, selection.ChannelMapInstances),
		LoadedAttributeMapInstances: selection.AttributeMapInstances,
		LoadedChannelNames:          selection.ChannelNames,
		LoadedChannelMapInstances:   selection.ChannelMapInstances,
	}
	budget := u.effectiveRPCBudget(input.GetTimeoutSeconds())
	activityOptions := interfaces.ActivityOptions{
		StartToCloseTimeout:                 budget,
		LocalActivityScheduleToCloseTimeout: budget,
		RetryPolicy: &config.RetryPolicy{
			MaximumAttempts: maxWorkerRpcActivityAttempts,
		},
	}
	ctx = u.provider.WithActivityOptions(ctx, activityOptions)
	var activityOutput dexpb.InvokeWorkerRPCActivityOutput
	err = u.provider.ExecuteLocalActivity(
		&activityOutput,
		ctx,
		u.activities.IWRPC,
		&dexpb.InvokeWorkerRPCActivityInput{
			RpcPrep: rpcPrep,
			Request: input,
		},
	)
	u.persistenceManager.UnlockKeys(keysToLock)
	if err != nil {
		if u.provider.IsApplicationError(err) {
			return nil, u.provider.NewFlowErrorFromActivityError(err)
		}
		return nil, err
	}
	response := activityOutput.GetResponse()
	decision := response.GetStepDecision()
	if missing := u.channelStore.CanDeleteAll(response.GetDeleteFromChannel()); missing != nil {
		return nil, u.channelMessageNotFoundError(missing)
	}
	err = u.persistenceManager.ApplyAttributeWritesWithActionPermissionMappings(
		ctx,
		response.GetUpsertAttributes(),
		response.GetActionPermissionMappings(),
	)
	if err != nil {
		return nil, err
	}
	u.channelStore.DeleteAll(response.GetDeleteFromChannel())
	u.channelStore.ProcessPublishing(response.GetPublishToChannel())
	if err := u.stepExecutionRegistry.CancelByStepTypes(ctx, decision.GetCancelStepTypes()); err != nil {
		return nil, err
	}
	u.stepRequestQueue.AddStepStartRequests(decision.GetNextSteps())
	u.continueAsNewCounter.IncSyncUpdateReceived()
	return &dexpb.InvokeRpcUpdateResult{
		Response: &dexpb.InvokeRPCResponse{Output: response.GetOutput()},
	}, nil
}

func (u *WorkflowUpdater) handleDeleteChannelMessage(
	ctx interfaces.UnifiedContext,
	request *dexpb.DeleteChannelMessageRequest,
) (*emptypb.Empty, error) {
	u.continueAsNewer.IncreaseInflightOperation()
	defer u.continueAsNewer.DecreaseInflightOperation()
	u.signalReceiver.DrainAllReceivedButUnprocessedSignals(ctx)
	deletion := &dexpb.ChannelMessageDeletion{
		ChannelName: request.GetChannelName(),
		MessageId:   request.GetMessageId(),
	}
	if missing := u.channelStore.CanDeleteAll([]*dexpb.ChannelMessageDeletion{deletion}); missing != nil {
		return nil, u.channelMessageNotFoundError(missing)
	}
	u.channelStore.DeleteAll([]*dexpb.ChannelMessageDeletion{deletion})
	u.continueAsNewCounter.IncSyncUpdateReceived()
	return &emptypb.Empty{}, nil
}

func (u *WorkflowUpdater) validateDeleteChannelMessage(
	_ interfaces.UnifiedContext,
	request *dexpb.DeleteChannelMessageRequest,
) error {
	if err := u.rejectTerminalUpdate(); err != nil {
		return err
	}
	if request == nil || request.GetChannelName() == "" || request.GetMessageId() == "" {
		return u.provider.NewUpdateError(
			dexpb.UpdateErrorType_UPDATE_ERROR_TYPE_INVALID_ARGUMENT,
			"channel name and message ID are required",
		)
	}
	return nil
}

func (u *WorkflowUpdater) channelMessageNotFoundError(deletion *dexpb.ChannelMessageDeletion) error {
	return u.provider.NewUpdateError(
		dexpb.UpdateErrorType_UPDATE_ERROR_TYPE_CHANNEL_MESSAGE_NOT_FOUND,
		fmt.Sprintf(
			"Channel message %q was not found in channel %q",
			deletion.GetMessageId(),
			deletion.GetChannelName(),
		),
	)
}

func (u *WorkflowUpdater) validateWorkerRpc(
	_ interfaces.UnifiedContext,
	input *dexpb.InvokeRPCRequest,
) error {
	if err := u.rejectTerminalUpdate(); err != nil {
		return err
	}
	if input == nil || input.GetRpcName() == "" {
		return u.provider.NewUpdateError(
			dexpb.UpdateErrorType_UPDATE_ERROR_TYPE_INVALID_ARGUMENT,
			"RPC name is required",
		)
	}
	if input.GetTimeoutSeconds() < 0 {
		return u.provider.NewUpdateError(
			dexpb.UpdateErrorType_UPDATE_ERROR_TYPE_INVALID_ARGUMENT,
			"RPC timeout must be non-negative",
		)
	}
	keys, err := normalizeLockKeys(input.GetLockAttributeKeys())
	if err != nil {
		return u.provider.NewUpdateError(
			dexpb.UpdateErrorType_UPDATE_ERROR_TYPE_INVALID_ARGUMENT,
			err.Error(),
		)
	}
	if !u.persistenceManager.CanLockKeys(keys) {
		return u.rpcLockError()
	}
	if _, err := rpc.ValidateAndSortSelections(
		input.GetLoadAttributeMapInstances(),
		input.GetLoadChannelNames(),
		input.GetLoadChannelMapInstances(),
	); err != nil {
		return u.provider.NewUpdateError(
			dexpb.UpdateErrorType_UPDATE_ERROR_TYPE_INVALID_ARGUMENT,
			err.Error(),
		)
	}
	return nil
}

func (u *WorkflowUpdater) rpcLockError() error {
	return u.provider.NewUpdateError(
		dexpb.UpdateErrorType_UPDATE_ERROR_TYPE_RPC_ACQUIRE_LOCK_FAILURE,
		"one or more attribute keys are locked",
	)
}

func (u *WorkflowUpdater) effectiveRPCBudget(requestedSeconds int32) time.Duration {
	maximumSeconds := u.apiCfg.EffectiveMaxWaitSeconds()
	if requestedSeconds > 0 && int64(requestedSeconds) < maximumSeconds {
		maximumSeconds = int64(requestedSeconds)
	}
	return time.Duration(maximumSeconds) * time.Second
}

func normalizeLockKeys(keys []string) ([]string, error) {
	for _, key := range keys {
		if key == "" {
			return nil, fmt.Errorf("lock attribute key is empty")
		}
	}
	return sortedUniqueStrings(keys), nil
}

func (u *WorkflowUpdater) validateWaitForStepCompletion(
	_ interfaces.UnifiedContext,
	request *dexpb.WaitForStepCompletionRequest,
) error {
	if err := u.rejectTerminalUpdate(); err != nil {
		return err
	}
	if request == nil {
		return u.provider.NewUpdateError(
			dexpb.UpdateErrorType_UPDATE_ERROR_TYPE_INVALID_ARGUMENT,
			"request is nil",
		)
	}
	if request.GetRequestTimeoutSeconds() < 0 ||
		request.GetInternalHandlerTimeoutSeconds() < 0 {
		return u.provider.NewUpdateError(
			dexpb.UpdateErrorType_UPDATE_ERROR_TYPE_INVALID_ARGUMENT,
			"request and internal handler timeouts must be non-negative",
		)
	}
	if request.GetStepType() == "" || request.GetStepExecutionNumber() == "" {
		return u.provider.NewUpdateError(
			dexpb.UpdateErrorType_UPDATE_ERROR_TYPE_INVALID_ARGUMENT,
			"step type and step execution number are required",
		)
	}
	_, err := parseWaitForStepExecutionNumber(
		request.GetStepExecutionNumber(),
	)
	if err != nil {
		return u.provider.NewUpdateError(
			dexpb.UpdateErrorType_UPDATE_ERROR_TYPE_INVALID_ARGUMENT,
			err.Error(),
		)
	}
	return nil
}

func (u *WorkflowUpdater) handleWaitForStepCompletion(
	ctx interfaces.UnifiedContext,
	request *dexpb.WaitForStepCompletionRequest,
) (*dexpb.WaitForStepCompletionResponse, error) {
	u.continueAsNewer.IncreaseInflightOperation()
	defer func() {
		if u.globalVersioner.UsesTimerlessWaitUpdates() {
			u.continueAsNewCounter.IncSyncUpdateReceived()
		}
		u.continueAsNewer.DecreaseInflightOperation()
	}()
	stepExecutionNumber, err := parseWaitForStepExecutionNumber(
		request.GetStepExecutionNumber(),
	)
	if err != nil {
		return nil, u.provider.NewUpdateError(
			dexpb.UpdateErrorType_UPDATE_ERROR_TYPE_INVALID_ARGUMENT,
			err.Error(),
		)
	}
	timeout, cancelTimeout := u.newInternalHandlerTimeout(
		ctx,
		request.GetInternalHandlerTimeoutSeconds(),
	)
	defer cancelTimeout()
	wait := &stepCompletionWait{
		updater:             u,
		request:             request,
		timeout:             timeout,
		stepExecutionNumber: stepExecutionNumber,
	}
	isReady := func() bool { return wait.ready(ctx) }
	if !isReady() {
		if err := u.provider.Await(ctx, isReady); err != nil {
			return nil, err
		}
	}
	if wait.matched {
		return &dexpb.WaitForStepCompletionResponse{}, nil
	}
	if wait.hasTimedOut(ctx) {
		return nil, u.provider.NewUpdateError(
			dexpb.UpdateErrorType_UPDATE_ERROR_TYPE_DEADLINE_EXCEEDED,
			"step completion wait timed out",
		)
	}
	return nil, u.provider.NewUpdateError(
		dexpb.UpdateErrorType_UPDATE_ERROR_TYPE_CONTINUE_AS_NEW_PREEMPTED,
		"continue-as-new preempted wait",
	)
}

func (w *stepCompletionWait) ready(ctx interfaces.UnifiedContext) bool {
	w.matched = w.updater.stepExecutionCounter.IsStepExecutionCompleted(
		w.request.GetStepType(),
		w.stepExecutionNumber,
	)
	return w.matched ||
		w.updater.terminalCoordinator.HasStartedFinalizing() ||
		w.updater.continueAsNewCounter.IsThresholdMet() ||
		w.hasTimedOut(ctx)
}

func (w *stepCompletionWait) hasTimedOut(ctx interfaces.UnifiedContext) bool {
	return w.timeout.hasElapsed(ctx)
}

func parseWaitForStepExecutionNumber(value string) (int32, error) {
	number, err := strconv.ParseInt(value, 10, 32)
	if err != nil || number <= 0 {
		return 0, fmt.Errorf("step execution number must be a positive integer")
	}
	return int32(number), nil
}

func (u *WorkflowUpdater) validateWaitForAttribute(
	_ interfaces.UnifiedContext,
	request *dexpb.WaitForAttributeRequest,
) error {
	if err := u.rejectTerminalUpdate(); err != nil {
		return err
	}
	if request == nil || request.GetMatch() == nil {
		return u.provider.NewUpdateError(
			dexpb.UpdateErrorType_UPDATE_ERROR_TYPE_INVALID_ARGUMENT,
			"attribute match is required",
		)
	}
	if request.GetRequestTimeoutSeconds() < 0 ||
		request.GetInternalHandlerTimeoutSeconds() < 0 {
		return u.provider.NewUpdateError(
			dexpb.UpdateErrorType_UPDATE_ERROR_TYPE_INVALID_ARGUMENT,
			"request and internal handler timeouts must be non-negative",
		)
	}
	match := request.GetMatch()
	if match.GetKey() == "" || match.GetOperand() == nil || match.GetOperand().GetKind() == nil {
		return u.provider.NewUpdateError(
			dexpb.UpdateErrorType_UPDATE_ERROR_TYPE_INVALID_ARGUMENT,
			"attribute match key, operator, and scalar operand are required",
		)
	}
	// TODO: hydrate blob-backed attributes deterministically without losing concurrent writes.
	if isBlobValue(match.GetOperand()) {
		return u.provider.NewUpdateError(
			dexpb.UpdateErrorType_UPDATE_ERROR_TYPE_FAILED_PRECONDITION,
			"blob-backed WaitForAttribute values are not supported",
		)
	}
	if err := validateAttributeMatch(match); err != nil {
		return u.provider.NewUpdateError(
			dexpb.UpdateErrorType_UPDATE_ERROR_TYPE_INVALID_ARGUMENT,
			err.Error(),
		)
	}
	return nil
}

func (u *WorkflowUpdater) rejectTerminalUpdate() error {
	if !u.terminalCoordinator.HasStartedFinalizing() {
		return nil
	}
	return u.provider.NewUpdateError(
		dexpb.UpdateErrorType_UPDATE_ERROR_TYPE_FAILED_PRECONDITION,
		"flow terminal cleanup is in progress",
	)
}

func (u *WorkflowUpdater) handleWaitForAttribute(
	ctx interfaces.UnifiedContext,
	request *dexpb.WaitForAttributeRequest,
) (*dexpb.WaitForAttributeResponse, error) {
	u.continueAsNewer.IncreaseInflightOperation()
	defer func() {
		if u.globalVersioner.UsesTimerlessWaitUpdates() {
			u.continueAsNewCounter.IncSyncUpdateReceived()
		}
		u.continueAsNewer.DecreaseInflightOperation()
	}()
	timeout, cancelTimeout := u.newInternalHandlerTimeout(
		ctx,
		u.attributeWaitInternalHandlerTimeoutSeconds(request),
	)
	defer cancelTimeout()
	wait := &attributeWait{
		updater: u,
		request: request,
		timeout: timeout,
	}
	isReady := func() bool { return wait.isReady(ctx) }
	if !isReady() {
		if err := u.provider.Await(ctx, isReady); err != nil {
			return nil, err
		}
	}
	if wait.matchErr != nil {
		return nil, u.provider.NewUpdateError(
			dexpb.UpdateErrorType_UPDATE_ERROR_TYPE_FAILED_PRECONDITION,
			wait.matchErr.Error(),
		)
	}
	if wait.matchedValue != nil {
		return &dexpb.WaitForAttributeResponse{MatchedValue: wait.matchedValue}, nil
	}
	if wait.hasTimedOut(ctx) {
		return nil, u.provider.NewUpdateError(
			dexpb.UpdateErrorType_UPDATE_ERROR_TYPE_DEADLINE_EXCEEDED,
			"attribute wait timed out",
		)
	}
	return nil, u.provider.NewUpdateError(
		dexpb.UpdateErrorType_UPDATE_ERROR_TYPE_CONTINUE_AS_NEW_PREEMPTED,
		"continue-as-new preempted wait",
	)
}

func (u *WorkflowUpdater) attributeWaitInternalHandlerTimeoutSeconds(
	request *dexpb.WaitForAttributeRequest,
) int32 {
	if u.globalVersioner.UsesSplitWaitForAttributeTimeoutSemantics() {
		return request.GetInternalHandlerTimeoutSeconds()
	}
	return request.GetRequestTimeoutSeconds()
}

func (w *attributeWait) isReady(ctx interfaces.UnifiedContext) bool {
	w.matchedValue, w.matchErr = w.updater.matchAttribute(w.request)
	return w.matchedValue != nil ||
		w.matchErr != nil ||
		w.updater.terminalCoordinator.HasStartedFinalizing() ||
		w.updater.continueAsNewCounter.IsThresholdMet() ||
		w.hasTimedOut(ctx)
}

func (w *attributeWait) hasTimedOut(ctx interfaces.UnifiedContext) bool {
	return w.timeout.hasElapsed(ctx)
}

func (u *WorkflowUpdater) matchAttribute(
	request *dexpb.WaitForAttributeRequest,
) (*dexpb.Value, error) {
	match := request.GetMatch()
	current, exists := u.persistenceManager.GetAttribute(match.GetKey())
	if !exists {
		return nil, nil
	}
	if isBlobValue(current) {
		return nil, fmt.Errorf("stored attribute %q is blob-backed", match.GetKey())
	}
	if err := validateStoredAttributeMatchValue(current); err != nil {
		return nil, fmt.Errorf("stored attribute %q: %w", match.GetKey(), err)
	}
	isMatched, err := attributeValuesMatch(current, match.GetOperator(), match.GetOperand())
	if err != nil {
		return nil, err
	}
	if !isMatched {
		return nil, nil
	}
	return current, nil
}

func isBlobValue(value *dexpb.Value) bool {
	if value == nil {
		return false
	}
	switch value.GetKind().(type) {
	case *dexpb.Value_InternalBlobIdForStringValue,
		*dexpb.Value_InternalBlobIdForObjValue:
		return true
	default:
		return false
	}
}

func (u *WorkflowUpdater) newInternalHandlerTimeout(
	ctx interfaces.UnifiedContext,
	timeoutSeconds int32,
) (internalHandlerTimeout, func()) {
	timeout := internalHandlerTimeout{updater: u}
	if timeoutSeconds == 0 {
		return timeout, func() {}
	}
	if u.globalVersioner.UsesTimerlessWaitUpdates() {
		timeout.deadline = u.provider.Now(ctx).Add(time.Duration(timeoutSeconds) * time.Second)
		return timeout, func() {}
	}
	timerContext, cancelTimeout := u.provider.WithCancel(ctx)
	timeout.timer = u.provider.NewTimer(
		timerContext,
		time.Duration(timeoutSeconds)*time.Second,
	)
	return timeout, cancelTimeout
}

func (t internalHandlerTimeout) hasElapsed(ctx interfaces.UnifiedContext) bool {
	if t.timer != nil {
		return t.timer.IsReady()
	}
	return !t.deadline.IsZero() && !t.updater.provider.Now(ctx).Before(t.deadline)
}

func validateAttributeMatch(match *dexpb.AttributeMatch) error {
	if !isAttributeMatchOperator(match.GetOperator()) {
		return fmt.Errorf("attribute match operator is invalid")
	}
	switch match.GetOperand().GetKind().(type) {
	case *dexpb.Value_StringValue, *dexpb.Value_BoolValue:
		if isAttributeOrderingOperator(match.GetOperator()) {
			return fmt.Errorf("attribute match ordering requires an integer or double operand")
		}
	case *dexpb.Value_IntValue:
	case *dexpb.Value_DoubleValue:
		if !isFiniteDouble(match.GetOperand().GetDoubleValue()) {
			return fmt.Errorf("attribute match double operand must be finite")
		}
	default:
		return fmt.Errorf("attribute match supports only string, boolean, integer, or double operands")
	}
	return nil
}

func attributeValuesMatch(
	current *dexpb.Value,
	operator dexpb.AttributeMatchOperator,
	operand *dexpb.Value,
) (bool, error) {
	switch operandKind := operand.GetKind().(type) {
	case *dexpb.Value_StringValue:
		currentKind, ok := current.GetKind().(*dexpb.Value_StringValue)
		if !ok {
			return false, nil
		}
		return equalityMatches(currentKind.StringValue == operandKind.StringValue, operator), nil
	case *dexpb.Value_BoolValue:
		currentKind, ok := current.GetKind().(*dexpb.Value_BoolValue)
		if !ok {
			return false, nil
		}
		return equalityMatches(currentKind.BoolValue == operandKind.BoolValue, operator), nil
	case *dexpb.Value_IntValue:
		currentKind, ok := current.GetKind().(*dexpb.Value_IntValue)
		if !ok {
			return false, nil
		}
		return orderedValuesMatch(currentKind.IntValue, operandKind.IntValue, operator), nil
	case *dexpb.Value_DoubleValue:
		currentKind, ok := current.GetKind().(*dexpb.Value_DoubleValue)
		if !ok {
			return false, nil
		}
		if !isFiniteDouble(currentKind.DoubleValue) {
			return false, fmt.Errorf("stored double attribute is not finite")
		}
		return orderedValuesMatch(currentKind.DoubleValue, operandKind.DoubleValue, operator), nil
	default:
		return false, fmt.Errorf("attribute match operand kind is invalid")
	}
}

func validateStoredAttributeMatchValue(value *dexpb.Value) error {
	switch value.GetKind().(type) {
	case *dexpb.Value_StringValue, *dexpb.Value_BoolValue, *dexpb.Value_IntValue:
		return nil
	case *dexpb.Value_DoubleValue:
		if !isFiniteDouble(value.GetDoubleValue()) {
			return fmt.Errorf("double value is not finite")
		}
		return nil
	default:
		return fmt.Errorf("value must be a string, boolean, integer, or double")
	}
}

func equalityMatches(isEqual bool, operator dexpb.AttributeMatchOperator) bool {
	if operator == dexpb.AttributeMatchOperator_ATTRIBUTE_MATCH_OPERATOR_EQUAL {
		return isEqual
	}
	return !isEqual
}

func orderedValuesMatch[T int64 | float64](
	current T,
	operand T,
	operator dexpb.AttributeMatchOperator,
) bool {
	switch operator {
	case dexpb.AttributeMatchOperator_ATTRIBUTE_MATCH_OPERATOR_EQUAL:
		return current == operand
	case dexpb.AttributeMatchOperator_ATTRIBUTE_MATCH_OPERATOR_NOT_EQUAL:
		return current != operand
	case dexpb.AttributeMatchOperator_ATTRIBUTE_MATCH_OPERATOR_GREATER_THAN:
		return current > operand
	case dexpb.AttributeMatchOperator_ATTRIBUTE_MATCH_OPERATOR_GREATER_THAN_OR_EQUAL:
		return current >= operand
	case dexpb.AttributeMatchOperator_ATTRIBUTE_MATCH_OPERATOR_LESS_THAN:
		return current < operand
	case dexpb.AttributeMatchOperator_ATTRIBUTE_MATCH_OPERATOR_LESS_THAN_OR_EQUAL:
		return current <= operand
	default:
		return false
	}
}

func isAttributeMatchOperator(operator dexpb.AttributeMatchOperator) bool {
	return operator >= dexpb.AttributeMatchOperator_ATTRIBUTE_MATCH_OPERATOR_EQUAL &&
		operator <= dexpb.AttributeMatchOperator_ATTRIBUTE_MATCH_OPERATOR_LESS_THAN_OR_EQUAL
}

func isAttributeOrderingOperator(operator dexpb.AttributeMatchOperator) bool {
	return operator >= dexpb.AttributeMatchOperator_ATTRIBUTE_MATCH_OPERATOR_GREATER_THAN
}

func isFiniteDouble(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0)
}
