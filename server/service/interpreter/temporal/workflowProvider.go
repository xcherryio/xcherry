// Legacy Materials in this file remain under their original licenses.
// See LEGACY_NOTICES.md.

// Modifications Copyright (c) 2026 Super Durable, Inc.
//
// Modifications after the Legacy Cutoff are licensed under the
// Sustainable Use License 1.0.
// Legacy Materials remain under their original licenses.
// See LICENSE and LEGACY_NOTICES.md.

package temporal

import (
	"errors"
	"fmt"
	"time"

	"github.com/superdurable/dex/gen/dexpb"
	"github.com/superdurable/dex/service"
	"github.com/superdurable/dex/service/common/ptr"
	"github.com/superdurable/dex/service/common/retry"
	"github.com/superdurable/dex/service/interpreter/interfaces"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

type workflowProvider struct {
	threadCount        int
	pendingThreadNames map[string]int
	it                 InterpreterWorker
}

var _ interfaces.WorkflowProvider = (*workflowProvider)(nil)

func newTemporalWorkflowProvider() interfaces.WorkflowProvider {
	return &workflowProvider{
		pendingThreadNames: map[string]int{},
	}
}

func (w *workflowProvider) NewFlowError(
	errType dexpb.FlowErrorType,
	detail string,
) error {
	return temporal.NewApplicationError(
		"",
		errType.String(),
		&dexpb.InternalFlowError{
			Failure: &dexpb.InternalFlowError_ServerDetail{ServerDetail: detail},
		},
	)
}

func (w *workflowProvider) NewFlowErrorFromActivityError(err error) error {
	var applicationError *temporal.ApplicationError
	if !errors.As(err, &applicationError) {
		panic("Temporal application error required")
	}
	activityError, detailsErr := decodeTemporalStepErrorDetails(applicationError)
	if detailsErr != nil {
		panic(fmt.Sprintf("decode Temporal activity error: %v", detailsErr))
	}
	return temporal.NewApplicationError(
		"",
		applicationError.Type(),
		&dexpb.InternalFlowError{
			Failure: &dexpb.InternalFlowError_ActivityError{ActivityError: activityError},
		},
	)
}

func (w *workflowProvider) NewCanceledError(reason string) error {
	if reason == "" {
		return temporal.NewCanceledError()
	}
	return temporal.NewCanceledError(reason)
}

func (w *workflowProvider) NewUpdateError(
	errType dexpb.UpdateErrorType,
	detail string,
) error {
	return temporal.NewApplicationError("", errType.String(), detail)
}

func (w *workflowProvider) IsApplicationError(err error) bool {
	var applicationError *temporal.ApplicationError
	return errors.As(err, &applicationError)
}

func (w *workflowProvider) IsSubFlowStartRejection(err error) (string, bool, error) {
	var applicationError *temporal.ApplicationError
	if !errors.As(err, &applicationError) ||
		applicationError.Type() != interfaces.SubFlowStartRejectionErrorType {
		return "", false, nil
	}
	var detail string
	detailsErr := applicationError.Details(&detail)
	if detailsErr != nil {
		return "", false, detailsErr
	}
	return detail, true, nil
}

func (w *workflowProvider) MapToFlowResultError(
	err error,
) (dexpb.FlowErrorType, *dexpb.RecoveryErrorInfo, error) {
	var applicationError *temporal.ApplicationError
	if !errors.As(err, &applicationError) {
		recoveryError, mappingErr := w.MapToRecoveryError(err)
		if mappingErr != nil {
			return dexpb.FlowErrorType_FLOW_ERROR_TYPE_UNSPECIFIED, nil, mappingErr
		}
		return dexpb.FlowErrorType_FLOW_ERROR_TYPE_UNSPECIFIED, recoveryError, nil
	}
	value, ok := dexpb.FlowErrorType_value[applicationError.Type()]
	if !ok {
		recoveryError, mappingErr := w.MapToRecoveryError(err)
		if mappingErr != nil {
			return dexpb.FlowErrorType_FLOW_ERROR_TYPE_UNSPECIFIED, nil, mappingErr
		}
		return dexpb.FlowErrorType_FLOW_ERROR_TYPE_UNSPECIFIED, recoveryError, nil
	}
	flowError, detailsErr := decodeTemporalFlowErrorDetails(applicationError)
	if detailsErr != nil {
		return dexpb.FlowErrorType_FLOW_ERROR_TYPE_UNSPECIFIED, nil,
			fmt.Errorf("decode Temporal Flow failure details: %w", detailsErr)
	}
	return dexpb.FlowErrorType(value), temporalFlowRecoveryError(
		flowError, applicationError.Message(), applicationError.Type(),
	), nil
}

func temporalFlowRecoveryError(
	flowError *dexpb.InternalFlowError,
	backendDetail string,
	backendType string,
) *dexpb.RecoveryErrorInfo {
	if activityError := flowError.GetActivityError(); activityError != nil {
		return temporalRecoveryError(activityError, backendDetail, backendType)
	}
	detail := flowError.GetServerDetail()
	if detail == "" {
		detail = backendDetail
	}
	return &dexpb.RecoveryErrorInfo{Detail: detail, ErrorType: backendType}
}

func (w *workflowProvider) MapToRecoveryError(err error) (*dexpb.RecoveryErrorInfo, error) {
	var timeoutError *temporal.TimeoutError
	if errors.As(err, &timeoutError) {
		return &dexpb.RecoveryErrorInfo{
			Detail:    timeoutError.Message(),
			ErrorType: timeoutError.TimeoutType().String(),
		}, nil
	}

	var applicationError *temporal.ApplicationError
	if errors.As(err, &applicationError) {
		activityError := &dexpb.InternalActivityError{}
		if applicationError.HasDetails() {
			var detailsErr error
			activityError, detailsErr = decodeTemporalStepErrorDetails(applicationError)
			if detailsErr != nil {
				return nil, fmt.Errorf("decode Temporal Step failure details: %w", detailsErr)
			}
		}
		return temporalRecoveryError(activityError, applicationError.Message(), applicationError.Type()), nil
	}

	return &dexpb.RecoveryErrorInfo{Detail: err.Error(), ErrorType: err.Error()}, nil
}

func temporalRecoveryError(
	activityError *dexpb.InternalActivityError,
	backendDetail string,
	backendType string,
) *dexpb.RecoveryErrorInfo {
	workerError := activityError.GetWorkerError()
	if activityError.GetWorkerGrpcStatus() != 0 || workerError != nil {
		detail := workerError.GetDetail()
		if detail == "" {
			detail = backendDetail
		}
		errorType := workerError.GetErrorType()
		if errorType == "" {
			errorType = backendType
		}
		return &dexpb.RecoveryErrorInfo{
			Detail:    detail,
			ErrorType: errorType,
		}
	}
	detail := activityError.GetServerDetail()
	if detail == "" {
		detail = backendDetail
	}
	return &dexpb.RecoveryErrorInfo{Detail: detail, ErrorType: backendType}
}

func (w *workflowProvider) IsCanceledError(err error) bool {
	return temporal.IsCanceledError(err)
}

func (w *workflowProvider) IsContinueAsNewError(err error) bool {
	return workflow.IsContinueAsNewError(err)
}

func (w *workflowProvider) NewDisconnectedContext(ctx interfaces.UnifiedContext) interfaces.UnifiedContext {
	wfCtx, ok := ctx.GetContext().(workflow.Context)
	if !ok {
		panic("cannot convert to temporal workflow context")
	}
	disconnected, _ := workflow.NewDisconnectedContext(wfCtx)
	return interfaces.NewUnifiedContext(disconnected)
}

func (w *workflowProvider) WithCancel(
	parent interfaces.UnifiedContext,
) (interfaces.UnifiedContext, func()) {
	wfCtx, ok := parent.GetContext().(workflow.Context)
	if !ok {
		panic("cannot convert to temporal workflow context")
	}
	cancelCtx, cancel := workflow.WithCancel(wfCtx)
	return interfaces.NewUnifiedContext(cancelCtx), cancel
}

func (w *workflowProvider) NewInterpreterContinueAsNewError(
	ctx interfaces.UnifiedContext, input *dexpb.InterpreterWorkflowInput,
) error {
	wfCtx, ok := ctx.GetContext().(workflow.Context)
	if !ok {
		panic("cannot convert to temporal workflow context")
	}
	return workflow.NewContinueAsNewError(wfCtx, w.it.Engine, input)
}

func (w *workflowProvider) UpsertSearchAttributes(
	ctx interfaces.UnifiedContext, attributes map[string]interface{},
) error {
	wfCtx, ok := ctx.GetContext().(workflow.Context)
	if !ok {
		panic("cannot convert to temporal workflow context")
	}
	return workflow.UpsertSearchAttributes(wfCtx, attributes)
}

func (w *workflowProvider) NewTimer(ctx interfaces.UnifiedContext, d time.Duration) interfaces.Future {
	wfCtx, ok := ctx.GetContext().(workflow.Context)
	if !ok {
		panic("cannot convert to temporal workflow context")
	}
	f := workflow.NewTimer(wfCtx, d)
	return &futureImpl{
		future: f,
	}
}

func (w *workflowProvider) GetWorkflowInfo(ctx interfaces.UnifiedContext) interfaces.WorkflowInfo {
	wfCtx, ok := ctx.GetContext().(workflow.Context)
	if !ok {
		panic("cannot convert to temporal workflow context")
	}
	info := workflow.GetInfo(wfCtx)
	workflowInfo := interfaces.WorkflowInfo{
		WorkflowExecution: interfaces.WorkflowExecution{
			ID:    info.WorkflowExecution.ID,
			RunID: info.WorkflowExecution.RunID,
		},
		WorkflowStartTime: info.WorkflowStartTime,
		FirstRunID:        info.FirstRunID,
		CurrentRunID:      info.WorkflowExecution.RunID,
		Attempt:           info.Attempt,
	}
	if info.RetryPolicy != nil {
		workflowInfo.RetryMaximumAttempts = ptr.Any(info.RetryPolicy.MaximumAttempts)
	}
	return workflowInfo
}

func (w *workflowProvider) GetSearchAttributeKeyword(
	ctx interfaces.UnifiedContext,
	key string,
) (string, error) {
	wfCtx, ok := ctx.GetContext().(workflow.Context)
	if !ok {
		panic("cannot convert to temporal workflow context")
	}
	field, ok := workflow.GetInfo(wfCtx).SearchAttributes.GetIndexedFields()[key]
	if !ok {
		return "", nil
	}
	var value string
	if err := converter.GetDefaultDataConverter().FromPayload(field, &value); err != nil {
		return "", err
	}
	return value, nil
}

func (w *workflowProvider) GetSearchAttributeKeywordArray(
	ctx interfaces.UnifiedContext,
	key string,
) ([]string, error) {
	wfCtx, ok := ctx.GetContext().(workflow.Context)
	if !ok {
		panic("cannot convert to temporal workflow context")
	}
	field, ok := workflow.GetInfo(wfCtx).SearchAttributes.GetIndexedFields()[key]
	if !ok {
		return nil, nil
	}
	var values []string
	if err := converter.GetDefaultDataConverter().FromPayload(field, &values); err != nil {
		return nil, err
	}
	return values, nil
}

func (w *workflowProvider) SetQueryHandler(
	ctx interfaces.UnifiedContext, queryType string, handler interface{},
) error {
	wfCtx, ok := ctx.GetContext().(workflow.Context)
	if !ok {
		panic("cannot convert to temporal workflow context")
	}
	return workflow.SetQueryHandler(wfCtx, queryType, handler)
}

func (w *workflowProvider) SetInvokeRPCUpdateHandler(
	ctx interfaces.UnifiedContext,
	validator interfaces.InvokeRPCUpdateValidator,
	handler interfaces.InvokeRPCUpdateHandler,
) error {
	return setUpdateHandler(ctx, service.InvokeRpcUpdateType, validator, handler)
}

func (w *workflowProvider) SetDeleteChannelMessageUpdateHandler(
	ctx interfaces.UnifiedContext,
	validator interfaces.DeleteChannelMessageUpdateValidator,
	handler interfaces.DeleteChannelMessageUpdateHandler,
) error {
	return setUpdateHandler(ctx, service.DeleteChannelMessageUpdateType, validator, handler)
}

func (w *workflowProvider) SetWaitForStepCompletionUpdateHandler(
	ctx interfaces.UnifiedContext,
	validator interfaces.WaitForStepCompletionUpdateValidator,
	handler interfaces.WaitForStepCompletionUpdateHandler,
) error {
	return setUpdateHandler(ctx, service.WaitForStepCompletionUpdateType, validator, handler)
}

func (w *workflowProvider) SetWaitForAttributeUpdateHandler(
	ctx interfaces.UnifiedContext,
	validator interfaces.WaitForAttributeUpdateValidator,
	handler interfaces.WaitForAttributeUpdateHandler,
) error {
	return setUpdateHandler(ctx, service.WaitForAttributeUpdateType, validator, handler)
}

func (w *workflowProvider) ExtendContextWithValue(
	parent interfaces.UnifiedContext, key string, val interface{},
) interfaces.UnifiedContext {
	wfCtx, ok := parent.GetContext().(workflow.Context)
	if !ok {
		panic("cannot convert to temporal workflow context")
	}
	return interfaces.NewUnifiedContext(workflow.WithValue(wfCtx, key, val))
}

func (w *workflowProvider) GoNamed(
	ctx interfaces.UnifiedContext, name string, f func(ctx interfaces.UnifiedContext),
) {
	wfCtx, ok := ctx.GetContext().(workflow.Context)
	if !ok {
		panic("cannot convert to temporal workflow context")
	}
	f2 := func(ctx workflow.Context) {
		ctx2 := interfaces.NewUnifiedContext(ctx)
		w.pendingThreadNames[name]++
		w.threadCount++
		f(ctx2)
		w.pendingThreadNames[name]--
		if w.pendingThreadNames[name] == 0 {
			delete(w.pendingThreadNames, name)
		}
		w.threadCount--
	}
	workflow.GoNamed(wfCtx, name, f2)
}

func (w *workflowProvider) GetPendingThreadNames() map[string]int {
	return w.pendingThreadNames
}

func (w *workflowProvider) GetThreadCount() int {
	return w.threadCount
}

func (w *workflowProvider) Await(ctx interfaces.UnifiedContext, condition func() bool) error {
	wfCtx, ok := ctx.GetContext().(workflow.Context)
	if !ok {
		panic("cannot convert to temporal workflow context")
	}
	return workflow.Await(wfCtx, condition)
}

func (w *workflowProvider) WithActivityOptions(
	ctx interfaces.UnifiedContext, options interfaces.ActivityOptions,
) interfaces.UnifiedContext {
	wfCtx, ok := ctx.GetContext().(workflow.Context)
	if !ok {
		panic("cannot convert to temporal workflow context")
	}

	wfCtx2 := workflow.WithActivityOptions(wfCtx, temporalActivityOptions(options))

	wfCtx3 := workflow.WithLocalActivityOptions(wfCtx2, temporalLocalActivityOptions(options))
	return interfaces.NewUnifiedActivityContext(wfCtx3, options)
}

type futureImpl struct {
	future workflow.Future
}

func (t *futureImpl) IsReady() bool {
	return t.future.IsReady()
}

func (t *futureImpl) Get(ctx interfaces.UnifiedContext, valuePtr interface{}) error {
	wfCtx, ok := ctx.GetContext().(workflow.Context)
	if !ok {
		panic("cannot convert to temporal workflow context")
	}

	return t.future.Get(wfCtx, valuePtr)
}

func (w *workflowProvider) ExecuteActivity(
	valuePtr interface{},
	durability dexpb.StepDurability,
	ctx interfaces.UnifiedContext,
	activity interface{},
	regularInput interface{},
	localActivityOnlyInput interface{},
) (err error) {
	wfCtx, ok := ctx.GetContext().(workflow.Context)
	if !ok {
		panic("cannot convert to temporal workflow context")
	}
	regularArgs := []interface{}{regularInput}
	localArgs := []interface{}{regularInput}
	if localActivityOnlyInput != nil {
		regularArgs = append(regularArgs, nil)
		localArgs = append(localArgs, localActivityOnlyInput)
	}
	switch durability {
	case dexpb.StepDurability_STEP_DURABILITY_SYNC:
		return workflow.ExecuteActivity(wfCtx, activity, regularArgs...).Get(wfCtx, valuePtr)
	case dexpb.StepDurability_STEP_DURABILITY_ASYNC:
		options, optionsFound := interfaces.ActivityOptionsFromContext(ctx)
		if !optionsFound {
			panic("activity options required for ASYNC durability")
		}
		localOptions := temporalLocalActivityOptions(options)
		if interfaces.IsPullStepActivityInput(regularInput) {
			localOptions.RetryPolicy = &temporal.RetryPolicy{MaximumAttempts: 1}
		}
		localCtx := workflow.WithLocalActivityOptions(wfCtx, localOptions)
		firstAttemptTime := workflow.Now(wfCtx)
		isStepMethodActivity := interfaces.IsStepActivityInput(regularInput)
		err = workflow.ExecuteLocalActivity(localCtx, activity, localArgs...).Get(localCtx, valuePtr)
		if err == nil {
			return nil
		}
		if wfCtx.Err() != nil {
			return wfCtx.Err()
		}
		if !isStepMethodActivity {
			return workflow.ExecuteActivity(wfCtx, activity, regularArgs...).Get(wfCtx, valuePtr)
		}
		applicationError, localFailure, hasLocalFailure := temporalLocalStepActivityError(err)
		previousAttempts := int32(1)
		firstAttemptTimestamp := firstAttemptTime.Unix()
		if hasLocalFailure {
			previousAttempts = localFailure.GetAttempt()
			firstAttemptTimestamp = localFailure.GetFirstAttemptTimestamp()
		}
		remainingPolicy, canFallback := retry.RemainingActivityRetryPolicy(
			options.RetryPolicy,
			previousAttempts,
			workflow.Now(wfCtx).Sub(firstAttemptTime),
		)
		if !canFallback {
			if hasLocalFailure {
				return temporalFinalFlowError(applicationError, localFailure.GetActivityError())
			}
			return err
		}
		regularInput = interfaces.StepActivityInputWithAttemptContext(
			regularInput,
			previousAttempts,
			firstAttemptTimestamp,
		)
		options.RetryPolicy = remainingPolicy
		regularCtx := workflow.WithActivityOptions(wfCtx, temporalActivityOptions(options))
		regularArgs = []interface{}{regularInput}
		if localActivityOnlyInput != nil {
			regularArgs = append(regularArgs, nil)
		}
		return workflow.ExecuteActivity(regularCtx, activity, regularArgs...).Get(regularCtx, valuePtr)
	default:
		return fmt.Errorf("unsupported step durability %s", durability)
	}
}

func temporalActivityOptions(options interfaces.ActivityOptions) workflow.ActivityOptions {
	// in Temporal, scheduled to close timeout is the timeout include all retries
	scheduleToCloseTimeout := time.Duration(0)
	if options.RetryPolicy != nil && options.RetryPolicy.TotalDuration > 0 {
		scheduleToCloseTimeout = options.RetryPolicy.TotalDuration
	}
	retryPolicy := retry.ConvertTemporalActivityRetryPolicy(options.RetryPolicy)
	if retryPolicy != nil {
		retryPolicy.NonRetryableErrorTypes = options.NonRetryableErrorTypes
	}
	return workflow.ActivityOptions{
		ActivityID:             options.ActivityID,
		ScheduleToCloseTimeout: scheduleToCloseTimeout,
		StartToCloseTimeout:    options.StartToCloseTimeout,
		RetryPolicy:            retryPolicy,
		HeartbeatTimeout:       options.HeartbeatTimeout,
	}
}

func temporalLocalActivityOptions(options interfaces.ActivityOptions) workflow.LocalActivityOptions {
	// Local activity optimization defaults to 7s so the workflow does not need a heartbeat.
	localActivityTimeout := 7 * time.Second
	if options.LocalActivityScheduleToCloseTimeout > 0 {
		localActivityTimeout = options.LocalActivityScheduleToCloseTimeout
	}
	if options.RetryPolicy != nil && options.RetryPolicy.TotalDuration > 0 && options.RetryPolicy.TotalDuration < localActivityTimeout {
		localActivityTimeout = options.RetryPolicy.TotalDuration
	}
	localRetryPolicy := retry.LocalActivityRetryPolicy(options.RetryPolicy, localActivityTimeout)
	retryPolicy := retry.ConvertTemporalActivityRetryPolicy(localRetryPolicy)
	retryPolicy.NonRetryableErrorTypes = options.NonRetryableErrorTypes
	return workflow.LocalActivityOptions{
		ScheduleToCloseTimeout: localActivityTimeout,
		RetryPolicy:            retryPolicy,
	}
}

func temporalLocalStepActivityError(
	err error,
) (*temporal.ApplicationError, *dexpb.InternalLocalStepActivityFailure, bool) {
	var applicationError *temporal.ApplicationError
	if !errors.As(err, &applicationError) {
		return nil, nil, false
	}
	if !applicationError.HasDetails() {
		return nil, nil, false
	}
	failure, detailErr := decodeTemporalLocalStepErrorDetails(applicationError)
	if detailErr != nil {
		return nil, nil, false
	}
	if failure.GetActivityError() == nil || failure.GetAttempt() <= 0 {
		return nil, nil, false
	}
	return applicationError, failure, true
}

func temporalFinalFlowError(
	applicationError *temporal.ApplicationError,
	activityError *dexpb.InternalActivityError,
) error {
	return temporal.NewApplicationError("", applicationError.Type(), activityError)
}

func decodeTemporalStepErrorDetails(
	applicationError *temporal.ApplicationError,
) (*dexpb.InternalActivityError, error) {
	return decodeTemporalApplicationErrorDetail[dexpb.InternalActivityError](applicationError)
}

func decodeTemporalFlowErrorDetails(
	applicationError *temporal.ApplicationError,
) (*dexpb.InternalFlowError, error) {
	return decodeTemporalApplicationErrorDetail[dexpb.InternalFlowError](applicationError)
}

func decodeTemporalLocalStepErrorDetails(
	applicationError *temporal.ApplicationError,
) (*dexpb.InternalLocalStepActivityFailure, error) {
	return decodeTemporalApplicationErrorDetail[dexpb.InternalLocalStepActivityFailure](applicationError)
}

func decodeTemporalApplicationErrorDetail[Detail any](
	applicationError *temporal.ApplicationError,
) (*Detail, error) {
	detail := new(Detail)
	valueErr := temporalApplicationErrorDetails(applicationError, detail)
	if valueErr == nil {
		return detail, nil
	}

	var detailPointer *Detail
	pointerErr := temporalApplicationErrorDetails(applicationError, &detailPointer)
	if pointerErr == nil {
		if detailPointer != nil {
			return detailPointer, nil
		}
		pointerErr = fmt.Errorf("decoded detail pointer is nil")
	}
	return nil, fmt.Errorf("decode value: %v; decode pointer: %w", valueErr, pointerErr)
}

func temporalApplicationErrorDetails(
	applicationError *temporal.ApplicationError,
	details ...interface{},
) (detailsErr error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			detailsErr = fmt.Errorf("Temporal detail type mismatch: %v", recovered)
		}
	}()
	return applicationError.Details(details...)
}

func (w *workflowProvider) ExecuteLocalActivity(
	valuePtr interface{}, ctx interfaces.UnifiedContext, activity interface{}, args ...interface{},
) error {
	wfCtx, ok := ctx.GetContext().(workflow.Context)
	if !ok {
		panic("cannot convert to temporal workflow context")
	}
	return workflow.ExecuteLocalActivity(wfCtx, activity, args...).Get(wfCtx, valuePtr)
}

func (w *workflowProvider) Now(ctx interfaces.UnifiedContext) time.Time {
	wfCtx, ok := ctx.GetContext().(workflow.Context)
	if !ok {
		panic("cannot convert to temporal workflow context")
	}
	return workflow.Now(wfCtx)
}

func (w *workflowProvider) Sleep(ctx interfaces.UnifiedContext, d time.Duration) (err error) {
	wfCtx, ok := ctx.GetContext().(workflow.Context)
	if !ok {
		panic("cannot convert to temporal workflow context")
	}
	return workflow.Sleep(wfCtx, d)
}

func (w *workflowProvider) IsReplaying(ctx interfaces.UnifiedContext) bool {
	wfCtx, ok := ctx.GetContext().(workflow.Context)
	if !ok {
		panic("cannot convert to temporal workflow context")
	}
	return workflow.IsReplaying(wfCtx)
}

func (w *workflowProvider) GetVersion(
	ctx interfaces.UnifiedContext, changeID string, minSupported, maxSupported int,
) int {
	wfCtx, ok := ctx.GetContext().(workflow.Context)
	if !ok {
		panic("cannot convert to temporal workflow context")
	}

	version := workflow.GetVersion(wfCtx, changeID, workflow.Version(minSupported), workflow.Version(maxSupported))
	return int(version)
}

type temporalReceiveChannel struct {
	channel workflow.ReceiveChannel
}

func (t *temporalReceiveChannel) ReceiveAsync(valuePtr interface{}) (ok bool) {
	return t.channel.ReceiveAsync(valuePtr)
}

func (t *temporalReceiveChannel) ReceiveBlocking(ctx interfaces.UnifiedContext, valuePtr interface{}) (ok bool) {
	wfCtx, ok := ctx.GetContext().(workflow.Context)
	if !ok {
		panic("cannot convert to temporal workflow context")
	}

	return t.channel.Receive(wfCtx, valuePtr)
}

func (w *workflowProvider) GetSignalChannel(
	ctx interfaces.UnifiedContext, signalName string,
) interfaces.ReceiveChannel {
	wfCtx, ok := ctx.GetContext().(workflow.Context)
	if !ok {
		panic("cannot convert to temporal workflow context")
	}
	wfChan := workflow.GetSignalChannel(wfCtx, signalName)
	return &temporalReceiveChannel{
		channel: wfChan,
	}
}

func (w *workflowProvider) GetContextValue(ctx interfaces.UnifiedContext, key string) interface{} {
	wfCtx, ok := ctx.GetContext().(workflow.Context)
	if !ok {
		panic("cannot convert to temporal workflow context")
	}
	return wfCtx.Value(key)
}

func (w *workflowProvider) GetLogger(ctx interfaces.UnifiedContext) interfaces.UnifiedLogger {
	wfCtx, ok := ctx.GetContext().(workflow.Context)
	if !ok {
		panic("cannot convert to temporal workflow context")
	}
	return workflow.GetLogger(wfCtx)
}

func setUpdateHandler[Request any, Response any](
	ctx interfaces.UnifiedContext,
	updateType string,
	validator func(interfaces.UnifiedContext, *Request) error,
	handler func(interfaces.UnifiedContext, *Request) (*Response, error),
) error {
	wfCtx, ok := ctx.GetContext().(workflow.Context)
	if !ok {
		panic("cannot convert to temporal workflow context")
	}
	temporalValidator := func(ctx workflow.Context, request *Request) error {
		return validator(interfaces.NewUnifiedContext(ctx), request)
	}
	temporalHandler := func(ctx workflow.Context, request *Request) (*Response, error) {
		return handler(interfaces.NewUnifiedContext(ctx), request)
	}
	return workflow.SetUpdateHandlerWithOptions(
		wfCtx,
		updateType,
		temporalHandler,
		workflow.UpdateHandlerOptions{Validator: temporalValidator},
	)
}
