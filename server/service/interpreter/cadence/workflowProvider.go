// Legacy Materials in this file remain under their original licenses.
// See LEGACY_NOTICES.md.

// Modifications Copyright (c) 2026 Super Durable, Inc.
//
// Modifications after the Legacy Cutoff are licensed under the
// Sustainable Use License 1.0.
// Legacy Materials remain under their original licenses.
// See LICENSE and LEGACY_NOTICES.md.

package cadence

import (
	"errors"
	"fmt"
	"time"

	"github.com/superdurable/dex/gen/dexpb"
	"github.com/superdurable/dex/service/common/ptr"
	"github.com/superdurable/dex/service/common/retry"
	"github.com/superdurable/dex/service/interpreter/interfaces"
	"go.uber.org/cadence"
	"go.uber.org/cadence/client"
	"go.uber.org/cadence/workflow"
)

type workflowProvider struct {
	threadCount        int
	pendingThreadNames map[string]int
	it                 InterpreterWorker
}

var _ interfaces.WorkflowProvider = (*workflowProvider)(nil)

func newCadenceWorkflowProvider() interfaces.WorkflowProvider {
	return &workflowProvider{
		pendingThreadNames: map[string]int{},
	}
}

func (w *workflowProvider) NewFlowError(
	errType dexpb.FlowErrorType,
	detail string,
) error {
	return cadence.NewCustomError(
		errType.String(),
		&dexpb.InternalFlowError{
			Failure: &dexpb.InternalFlowError_ServerDetail{ServerDetail: detail},
		},
	)
}

func (w *workflowProvider) NewFlowErrorFromActivityError(err error) error {
	var customError *cadence.CustomError
	if !errors.As(err, &customError) {
		panic("Cadence custom error required")
	}
	activityError, detailsErr := decodeCadenceStepErrorDetails(customError)
	if detailsErr != nil {
		panic(fmt.Sprintf("decode Cadence activity error: %v", detailsErr))
	}
	return cadence.NewCustomError(
		customError.Reason(),
		&dexpb.InternalFlowError{
			Failure: &dexpb.InternalFlowError_ActivityError{ActivityError: activityError},
		},
	)
}

func (w *workflowProvider) NewCanceledError(reason string) error {
	if reason == "" {
		return cadence.NewCanceledError()
	}
	return cadence.NewCanceledError(reason)
}

func (w *workflowProvider) NewUpdateError(
	errType dexpb.UpdateErrorType,
	detail string,
) error {
	return cadence.NewCustomError(errType.String(), detail)
}

func (w *workflowProvider) IsApplicationError(err error) bool {
	var applicationError *cadence.CustomError
	return errors.As(err, &applicationError)
}

func (w *workflowProvider) MapToFlowResultError(
	err error,
) (dexpb.FlowErrorType, *dexpb.RecoveryErrorInfo, error) {
	var applicationError *cadence.CustomError
	if !errors.As(err, &applicationError) {
		recoveryError, mappingErr := w.MapToRecoveryError(err)
		if mappingErr != nil {
			return dexpb.FlowErrorType_FLOW_ERROR_TYPE_UNSPECIFIED, nil, mappingErr
		}
		return dexpb.FlowErrorType_FLOW_ERROR_TYPE_UNSPECIFIED, recoveryError, nil
	}
	value, ok := dexpb.FlowErrorType_value[applicationError.Reason()]
	if !ok {
		recoveryError, mappingErr := w.MapToRecoveryError(err)
		if mappingErr != nil {
			return dexpb.FlowErrorType_FLOW_ERROR_TYPE_UNSPECIFIED, nil, mappingErr
		}
		return dexpb.FlowErrorType_FLOW_ERROR_TYPE_UNSPECIFIED, recoveryError, nil
	}
	flowError, detailsErr := decodeCadenceFlowErrorDetails(applicationError)
	if detailsErr != nil {
		return dexpb.FlowErrorType_FLOW_ERROR_TYPE_UNSPECIFIED, nil,
			fmt.Errorf("decode Cadence Flow failure details: %w", detailsErr)
	}
	return dexpb.FlowErrorType(value), cadenceFlowRecoveryError(
		flowError, applicationError.Error(), applicationError.Reason(),
	), nil
}

func cadenceFlowRecoveryError(
	flowError *dexpb.InternalFlowError,
	backendDetail string,
	backendType string,
) *dexpb.RecoveryErrorInfo {
	if activityError := flowError.GetActivityError(); activityError != nil {
		return cadenceRecoveryError(activityError, backendDetail, backendType)
	}
	detail := flowError.GetServerDetail()
	if detail == "" {
		detail = backendDetail
	}
	return &dexpb.RecoveryErrorInfo{Detail: detail, ErrorType: backendType}
}

func (w *workflowProvider) MapToRecoveryError(err error) (*dexpb.RecoveryErrorInfo, error) {
	var timeoutError *workflow.TimeoutError
	if errors.As(err, &timeoutError) {
		return &dexpb.RecoveryErrorInfo{
			Detail:    timeoutError.Error(),
			ErrorType: timeoutError.TimeoutType().String(),
		}, nil
	}

	var customError *cadence.CustomError
	if errors.As(err, &customError) {
		var activityError *dexpb.InternalActivityError
		if customError.HasDetails() {
			var detailsErr error
			activityError, detailsErr = decodeCadenceStepErrorDetails(customError)
			if detailsErr != nil {
				return nil, fmt.Errorf("decode Cadence Step failure details: %w", detailsErr)
			}
		}
		if activityError == nil {
			activityError = &dexpb.InternalActivityError{}
		}
		return cadenceRecoveryError(activityError, customError.Error(), customError.Reason()), nil
	}

	return &dexpb.RecoveryErrorInfo{Detail: err.Error(), ErrorType: err.Error()}, nil
}

func cadenceRecoveryError(
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
	return cadence.IsCanceledError(err)
}

func (w *workflowProvider) IsContinueAsNewError(err error) bool {
	var continueAsNewError *workflow.ContinueAsNewError
	return errors.As(err, &continueAsNewError)
}

func (w *workflowProvider) NewDisconnectedContext(ctx interfaces.UnifiedContext) interfaces.UnifiedContext {
	wfCtx, ok := ctx.GetContext().(workflow.Context)
	if !ok {
		panic("cannot convert to cadence workflow context")
	}
	disconnected, _ := workflow.NewDisconnectedContext(wfCtx)
	return interfaces.NewUnifiedContext(disconnected)
}

func (w *workflowProvider) WithCancel(
	parent interfaces.UnifiedContext,
) (interfaces.UnifiedContext, func()) {
	wfCtx, ok := parent.GetContext().(workflow.Context)
	if !ok {
		panic("cannot convert to cadence workflow context")
	}
	cancelCtx, cancel := workflow.WithCancel(wfCtx)
	return interfaces.NewUnifiedContext(cancelCtx), cancel
}

func (w *workflowProvider) NewInterpreterContinueAsNewError(
	ctx interfaces.UnifiedContext, input *dexpb.InterpreterWorkflowInput,
) error {
	wfCtx, ok := ctx.GetContext().(workflow.Context)
	if !ok {
		panic("cannot convert to cadence workflow context")
	}
	return workflow.NewContinueAsNewError(wfCtx, w.it.Engine, input)
}

func (w *workflowProvider) UpsertSearchAttributes(
	ctx interfaces.UnifiedContext, attributes map[string]interface{},
) error {
	wfCtx, ok := ctx.GetContext().(workflow.Context)
	if !ok {
		panic("cannot convert to cadence workflow context")
	}
	return workflow.UpsertSearchAttributes(wfCtx, attributes)
}

func (w *workflowProvider) NewTimer(ctx interfaces.UnifiedContext, d time.Duration) interfaces.Future {
	wfCtx, ok := ctx.GetContext().(workflow.Context)
	if !ok {
		panic("cannot convert to cadence workflow context")
	}
	f := workflow.NewTimer(wfCtx, d)
	return &futureImpl{
		future: f,
	}
}

func (w *workflowProvider) GetWorkflowInfo(ctx interfaces.UnifiedContext) interfaces.WorkflowInfo {
	wfCtx, ok := ctx.GetContext().(workflow.Context)
	if !ok {
		panic("cannot convert to cadence workflow context")
	}
	info := workflow.GetInfo(wfCtx)
	workflowInfo := interfaces.WorkflowInfo{
		WorkflowExecution: interfaces.WorkflowExecution{
			ID:    info.WorkflowExecution.ID,
			RunID: info.WorkflowExecution.RunID,
		},
		WorkflowStartTime: time.UnixMilli(0),            // TODO need support from Cadence client: https://github.com/uber-go/cadence-client/issues/1204
		FirstRunID:        info.WorkflowExecution.RunID, // Cadence does not provide FirstRunID TODO https://github.com/uber-go/cadence-client/issues/1371 use firstRunID when available
		CurrentRunID:      info.WorkflowExecution.RunID,
		Attempt:           info.Attempt + 1,
	}
	if info.RetryPolicy != nil {
		workflowInfo.RetryMaximumAttempts = ptr.Any(info.RetryPolicy.GetMaximumAttempts())
	}
	return workflowInfo
}

func (w *workflowProvider) GetSearchAttributeKeyword(
	ctx interfaces.UnifiedContext,
	key string,
) (string, error) {
	wfCtx, ok := ctx.GetContext().(workflow.Context)
	if !ok {
		panic("cannot convert to cadence workflow context")
	}
	field, ok := workflow.GetInfo(wfCtx).SearchAttributes.GetIndexedFields()[key]
	if !ok {
		return "", nil
	}
	var value string
	if err := client.NewValue(field).Get(&value); err != nil {
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
		panic("cannot convert to cadence workflow context")
	}
	field, ok := workflow.GetInfo(wfCtx).SearchAttributes.GetIndexedFields()[key]
	if !ok {
		return nil, nil
	}
	var values []string
	if err := client.NewValue(field).Get(&values); err != nil {
		return nil, err
	}
	return values, nil
}

func (w *workflowProvider) SetQueryHandler(
	ctx interfaces.UnifiedContext, queryType string, handler interface{},
) error {
	wfCtx, ok := ctx.GetContext().(workflow.Context)
	if !ok {
		panic("cannot convert to cadence workflow context")
	}
	return workflow.SetQueryHandler(wfCtx, queryType, handler)
}

func (w *workflowProvider) SetInvokeRPCUpdateHandler(
	interfaces.UnifiedContext,
	interfaces.InvokeRPCUpdateValidator,
	interfaces.InvokeRPCUpdateHandler,
) error {
	return nil
}

func (w *workflowProvider) SetDeleteChannelMessageUpdateHandler(
	interfaces.UnifiedContext,
	interfaces.DeleteChannelMessageUpdateValidator,
	interfaces.DeleteChannelMessageUpdateHandler,
) error {
	return nil
}

func (w *workflowProvider) SetWaitForStepCompletionUpdateHandler(
	interfaces.UnifiedContext,
	interfaces.WaitForStepCompletionUpdateValidator,
	interfaces.WaitForStepCompletionUpdateHandler,
) error {
	return nil
}

func (w *workflowProvider) SetWaitForAttributeUpdateHandler(
	interfaces.UnifiedContext,
	interfaces.WaitForAttributeUpdateValidator,
	interfaces.WaitForAttributeUpdateHandler,
) error {
	return nil
}

func (w *workflowProvider) ExtendContextWithValue(
	parent interfaces.UnifiedContext, key string, val interface{},
) interfaces.UnifiedContext {
	wfCtx, ok := parent.GetContext().(workflow.Context)
	if !ok {
		panic("cannot convert to cadence workflow context")
	}
	return interfaces.NewUnifiedContext(workflow.WithValue(wfCtx, key, val))
}

func (w *workflowProvider) GoNamed(
	ctx interfaces.UnifiedContext, name string, f func(ctx interfaces.UnifiedContext),
) {
	wfCtx, ok := ctx.GetContext().(workflow.Context)
	if !ok {
		panic("cannot convert to cadence workflow context")
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
		panic("cannot convert to cadence workflow context")
	}
	return workflow.Await(wfCtx, condition)
}

func (w *workflowProvider) WithActivityOptions(
	ctx interfaces.UnifiedContext, options interfaces.ActivityOptions,
) interfaces.UnifiedContext {
	wfCtx, ok := ctx.GetContext().(workflow.Context)
	if !ok {
		panic("cannot convert to cadence workflow context")
	}

	wfCtx2 := workflow.WithActivityOptions(wfCtx, cadenceActivityOptions(options))

	wfCtx3 := workflow.WithLocalActivityOptions(wfCtx2, cadenceLocalActivityOptions(options))
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
		panic("cannot convert to cadence workflow context")
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
		panic("cannot convert to cadence workflow context")
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
		localOptions := cadenceLocalActivityOptions(options)
		if interfaces.IsPullStepActivityInput(regularInput) {
			localOptions.RetryPolicy = nil
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
		customError, localFailure, hasLocalFailure := cadenceLocalStepActivityError(err)
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
				return cadenceFinalFlowError(customError, localFailure.GetActivityError())
			}
			return err
		}
		regularInput = interfaces.StepActivityInputWithAttemptContext(
			regularInput,
			previousAttempts,
			firstAttemptTimestamp,
		)
		options.RetryPolicy = remainingPolicy
		regularCtx := workflow.WithActivityOptions(wfCtx, cadenceActivityOptions(options))
		regularArgs = []interface{}{regularInput}
		if localActivityOnlyInput != nil {
			regularArgs = append(regularArgs, nil)
		}
		return workflow.ExecuteActivity(regularCtx, activity, regularArgs...).Get(regularCtx, valuePtr)
	default:
		return fmt.Errorf("unsupported step durability %s", durability)
	}
}

func cadenceActivityOptions(options interfaces.ActivityOptions) workflow.ActivityOptions {
	unlimited := time.Hour * 24 * 365
	startToCloseTimeout := options.StartToCloseTimeout
	if startToCloseTimeout == 0 {
		// unlimited to match Temporal for default
		startToCloseTimeout = unlimited
	}
	return workflow.ActivityOptions{
		ActivityID:             options.ActivityID,
		StartToCloseTimeout:    startToCloseTimeout,
		ScheduleToStartTimeout: time.Second * 10,
		HeartbeatTimeout:       options.HeartbeatTimeout,
		RetryPolicy:            retry.ConvertCadenceActivityRetryPolicy(options.RetryPolicy),
	}
}

func cadenceLocalActivityOptions(options interfaces.ActivityOptions) workflow.LocalActivityOptions {
	// Local activity optimization defaults to 7s so the workflow does not need a heartbeat.
	localActivityTimeout := 7 * time.Second
	if options.LocalActivityScheduleToCloseTimeout > 0 {
		localActivityTimeout = options.LocalActivityScheduleToCloseTimeout
	}
	if options.RetryPolicy != nil && options.RetryPolicy.TotalDuration > 0 && options.RetryPolicy.TotalDuration < localActivityTimeout {
		localActivityTimeout = options.RetryPolicy.TotalDuration
	}
	localRetryPolicy := retry.LocalActivityRetryPolicy(options.RetryPolicy, localActivityTimeout)
	return workflow.LocalActivityOptions{
		ScheduleToCloseTimeout: localActivityTimeout,
		RetryPolicy:            retry.ConvertCadenceLocalActivityRetryPolicy(localRetryPolicy),
	}
}

func cadenceLocalStepActivityError(
	err error,
) (*cadence.CustomError, *dexpb.InternalLocalStepActivityFailure, bool) {
	var customError *cadence.CustomError
	if !errors.As(err, &customError) {
		return nil, nil, false
	}
	if !customError.HasDetails() {
		return nil, nil, false
	}
	var failure *dexpb.InternalLocalStepActivityFailure
	if detailErr := customError.Details(&failure); detailErr != nil {
		return nil, nil, false
	}
	if failure == nil || failure.GetActivityError() == nil || failure.GetAttempt() <= 0 {
		return nil, nil, false
	}
	return customError, failure, true
}

func cadenceFinalFlowError(
	customError *cadence.CustomError,
	activityError *dexpb.InternalActivityError,
) error {
	return cadence.NewCustomError(customError.Reason(), activityError)
}

func decodeCadenceStepErrorDetails(
	customError *cadence.CustomError,
) (*dexpb.InternalActivityError, error) {
	var activityError *dexpb.InternalActivityError
	if detailsErr := customError.Details(&activityError); detailsErr != nil {
		return nil, fmt.Errorf("decode activity error: %w", detailsErr)
	}
	if activityError == nil {
		return nil, fmt.Errorf("Cadence Step failure details are nil")
	}
	return activityError, nil
}

func decodeCadenceFlowErrorDetails(
	customError *cadence.CustomError,
) (*dexpb.InternalFlowError, error) {
	var flowError *dexpb.InternalFlowError
	if detailsErr := customError.Details(&flowError); detailsErr != nil {
		return nil, fmt.Errorf("decode Flow error: %w", detailsErr)
	}
	if flowError == nil {
		return nil, fmt.Errorf("Cadence Flow failure details are nil")
	}
	return flowError, nil
}

func (w *workflowProvider) ExecuteLocalActivity(
	valuePtr interface{}, ctx interfaces.UnifiedContext, activity interface{}, args ...interface{},
) error {
	wfCtx, ok := ctx.GetContext().(workflow.Context)
	if !ok {
		panic("cannot convert to cadence workflow context")
	}
	// Cadence local activities that call back into Cadence (DumpFlow query) can
	// stall decision tasks; use a regular activity so CAN resume stays healthy.
	return workflow.ExecuteActivity(wfCtx, activity, args...).Get(wfCtx, valuePtr)
}

func (w *workflowProvider) Now(ctx interfaces.UnifiedContext) time.Time {
	wfCtx, ok := ctx.GetContext().(workflow.Context)
	if !ok {
		panic("cannot convert to cadence workflow context")
	}
	return workflow.Now(wfCtx)
}

func (w *workflowProvider) IsReplaying(ctx interfaces.UnifiedContext) bool {
	wfCtx, ok := ctx.GetContext().(workflow.Context)
	if !ok {
		panic("cannot convert to cadence workflow context")
	}
	return workflow.IsReplaying(wfCtx)
}

func (w *workflowProvider) Sleep(ctx interfaces.UnifiedContext, d time.Duration) (err error) {
	wfCtx, ok := ctx.GetContext().(workflow.Context)
	if !ok {
		panic("cannot convert to cadence workflow context")
	}
	return workflow.Sleep(wfCtx, d)
}

func (w *workflowProvider) GetVersion(
	ctx interfaces.UnifiedContext, changeID string, minSupported, maxSupported int,
) int {
	wfCtx, ok := ctx.GetContext().(workflow.Context)
	if !ok {
		panic("cannot convert to cadence workflow context")
	}

	version := workflow.GetVersion(wfCtx, changeID, workflow.Version(minSupported), workflow.Version(maxSupported))
	return int(version)
}

type cadenceReceiveChannel struct {
	channel workflow.Channel
}

func (t *cadenceReceiveChannel) ReceiveAsync(valuePtr interface{}) (ok bool) {
	return t.channel.ReceiveAsync(valuePtr)
}

func (t *cadenceReceiveChannel) ReceiveBlocking(ctx interfaces.UnifiedContext, valuePtr interface{}) (ok bool) {
	wfCtx, ok := ctx.GetContext().(workflow.Context)
	if !ok {
		panic("cannot convert to cadence workflow context")
	}

	return t.channel.Receive(wfCtx, valuePtr)
}

func (w *workflowProvider) GetSignalChannel(
	ctx interfaces.UnifiedContext, signalName string,
) interfaces.ReceiveChannel {
	wfCtx, ok := ctx.GetContext().(workflow.Context)
	if !ok {
		panic("cannot convert to cadence workflow context")
	}
	wfChan := workflow.GetSignalChannel(wfCtx, signalName)
	return &cadenceReceiveChannel{
		channel: wfChan,
	}
}

func (w *workflowProvider) GetContextValue(ctx interfaces.UnifiedContext, key string) interface{} {
	wfCtx, ok := ctx.GetContext().(workflow.Context)
	if !ok {
		panic("cannot convert to cadence workflow context")
	}
	return wfCtx.Value(key)
}

func (w *workflowProvider) GetLogger(ctx interfaces.UnifiedContext) interfaces.UnifiedLogger {
	wfCtx, ok := ctx.GetContext().(workflow.Context)
	if !ok {
		panic("cannot convert to cadence workflow context")
	}

	zLogger := workflow.GetLogger(wfCtx)
	return &loggerImpl{
		zlogger: zLogger,
	}
}
