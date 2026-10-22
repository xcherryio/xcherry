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
	"time"

	"github.com/superdurable/dex/config"
	"github.com/superdurable/dex/gen/dexpb"
	"github.com/superdurable/dex/service"
	"github.com/superdurable/dex/service/common/event"
	"github.com/superdurable/dex/service/common/pulldispatch"
	"github.com/superdurable/dex/service/common/retry"
	"github.com/superdurable/dex/service/interpreter/condition"
	interpreterconfig "github.com/superdurable/dex/service/interpreter/config"
	"github.com/superdurable/dex/service/interpreter/cont"
	"github.com/superdurable/dex/service/interpreter/interfaces"
	"github.com/superdurable/dex/service/interpreter/timers"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/structpb"
)

const (
	defaultStepMethodTimeout    = 2 * time.Hour
	defaultStepHeartbeatTimeout = time.Minute
)

type Interpreter struct {
	activities   *Activities
	sharedConfig *config.Config
}

func NewInterpreter(sharedConfig *config.Config, activities *Activities) *Interpreter {
	if sharedConfig == nil || activities == nil {
		panic("Interpreter requires non-nil dependencies")
	}
	return &Interpreter{
		activities:   activities,
		sharedConfig: sharedConfig,
	}
}

// NewInterpreterForReplay supplies config while replaying recorded Activity results.
func NewInterpreterForReplay() *Interpreter {
	sharedConfig := &config.Config{}
	return NewInterpreter(sharedConfig, &Activities{cfg: sharedConfig})
}

func (i *Interpreter) StartEngineFlow(
	ctx interfaces.UnifiedContext,
	provider interfaces.WorkflowProvider,
	input *dexpb.InterpreterWorkflowInput,
) (out *dexpb.InterpreterWorkflowOutput, retErr error) {
	if provider == nil || input == nil {
		panic("Interpreter requires non-nil dependencies")
	}

	var persistenceManager *PersistenceManager

	globalVersioner := NewGlobalVersioner(provider, ctx)
	flowConfiger := interpreterconfig.NewFlowConfiger(input.GetConfig())
	runStartedTimestamp := provider.Now(ctx).Unix()
	basicInfo := service.BasicInfo{
		FlowType:            input.GetFlowType(),
		RunStartedTimestamp: runStartedTimestamp,
	}
	flowTimeout := NewFlowTimeout(input)
	subFlowParentFlowID, parentFlowErr := provider.GetSearchAttributeKeyword(
		ctx, service.SearchAttributeDexParentFlowID,
	)
	if parentFlowErr != nil {
		return nil, fmt.Errorf("read SubFlow parent identity: %w", parentFlowErr)
	}

	var channelStore *ChannelStore
	var stepRequestQueue *StepRequestQueue
	var timerProcessor interfaces.TimerProcessor
	var continueAsNewCounter *cont.ContinueAsNewCounter
	var signalReceiver *SignalReceiver
	var stepExecutionCounter *StepExecutionCounter
	var outputCollector *OutputCollector
	var continueAsNewer *ContinueAsNewer
	var attributeSynchronizer *AttributeSynchronizer
	var resumeInfos []*dexpb.StepExecutionResumeInfo
	if input.GetIsResumeFromContinueAsNew() {
		previous, err := i.LoadInternalsFromPreviousRun(
			ctx,
			provider,
			input.GetContinueAsNewInput().GetPreviousInternalRunId(),
			flowConfiger.EffectiveContinueAsNewPageSizeInBytes(),
		)
		if err != nil {
			return nil, err
		}
		if flowTimeout.IsEnabled() && provider.GetWorkflowInfo(ctx).Attempt > 1 {
			flowTimeout.ResetSnapshotForRetry(ctx, provider, previous)
		}
		resumeInfos = previous.GetStepExecutionsToResume()

		// The below initialization order should be the same as for non-continueAsNew

		channelStore = RebuildChannelStore(previous.GetChannelReceived())
		stepRequestQueue = NewStepRequestQueueWithResumeRequests(
			previous.GetStepsToStartFromBeginning(),
			previous.GetStepExecutionsToResume(),
		)
		continueAsNewCounter = cont.NewContinueAsCounter(flowConfiger)
		attributeSynchronizer = NewAttributeSynchronizer(
			&i.sharedConfig.AttributeStore,
			i.activities,
			provider,
			ctx,
			continueAsNewCounter,
			previous.GetPendingAttributeSyncItems(),
		)
		persistenceManager = NewPersistenceManager(
			provider,
			previous.GetAttributes(),
			attributeSynchronizer,
			flowConfiger,
		)
		timerProcessor = timers.NewGreedyTimerProcessor(
			ctx,
			provider,
			continueAsNewCounter,
			previous.GetStaleSkipTimers(),
		)
		stepExecutionCounter = RebuildStepExecutionCounter(
			provider,
			flowConfiger,
			continueAsNewCounter,
			previous.GetCounterInfo(),
		)
		outputCollector = NewOutputCollector(previous.GetStepOutputs())
		continueAsNewer = NewContinueAsNewer(
			&i.sharedConfig.Api,
			provider,
			channelStore,
			stepExecutionCounter,
			persistenceManager,
			stepRequestQueue,
			outputCollector,
			timerProcessor,
			attributeSynchronizer,
			previous.GetDexTaskQueue(),
		)
	} else {
		channelStore = NewChannelStore()
		stepRequestQueue = NewStepRequestQueue()
		continueAsNewCounter = cont.NewContinueAsCounter(flowConfiger)
		attributeSynchronizer = NewAttributeSynchronizer(
			&i.sharedConfig.AttributeStore,
			i.activities,
			provider,
			ctx,
			continueAsNewCounter,
			nil,
		)
		persistenceManager = NewPersistenceManager(
			provider,
			attributeWritesToKVs(input.GetInitAttributes()),
			attributeSynchronizer,
			flowConfiger,
		)
		attributeSynchronizer.AppendingToPendings(
			ctx,
			input.GetInitAttributes(),
			flowConfiger.Get().GetAttributeStoreNames().GetNames(),
		)
		timerProcessor = timers.NewGreedyTimerProcessor(
			ctx,
			provider,
			continueAsNewCounter,
			nil,
		)
		stepExecutionCounter = NewStepExecutionCounter(
			provider,
			flowConfiger,
			continueAsNewCounter,
		)
		outputCollector = NewOutputCollector(nil)
		continueAsNewer = NewContinueAsNewer(
			&i.sharedConfig.Api,
			provider,
			channelStore,
			stepExecutionCounter,
			persistenceManager,
			stepRequestQueue,
			outputCollector,
			timerProcessor,
			attributeSynchronizer,
			"",
		)
	} // end of NOT continueAsNew
	subFlowTracker := NewSubFlowTracker(
		provider.GetWorkflowInfo(ctx).WorkflowExecution.ID,
		resumeInfos,
	)
	stepExecutionRegistry := NewStepExecutionRegistry(
		provider,
		stepRequestQueue,
		stepExecutionCounter,
		continueAsNewer,
		timerProcessor,
		subFlowTracker,
	)
	signalReceiver = NewSignalReceiver(
		ctx,
		provider,
		channelStore,
		stepRequestQueue,
		stepExecutionRegistry,
		persistenceManager,
		timerProcessor,
		continueAsNewCounter,
		flowConfiger,
		subFlowTracker,
	)
	attributeSynchronizer.Start(ctx)

	// we need these global varirables because sub threads(goroutine) need to report error back
	// to main goroutine to return.
	// Note that different errors could overwrite each other.
	// We only support last one wins. we may use multiError to improve.
	var errToFailWf error
	var forceCompleteWf bool
	var isGracefulCompletionRequested bool

	terminalCoordinator := NewTerminalCoordinator(
		provider,
		continueAsNewer,
		attributeSynchronizer,
		signalReceiver,
		stepExecutionRegistry,
	)
	updateErr := NewWorkflowUpdater(
		&i.sharedConfig.Api,
		i.activities,
		ctx,
		provider,
		globalVersioner,
		persistenceManager,
		stepRequestQueue,
		continueAsNewer,
		continueAsNewCounter,
		channelStore,
		signalReceiver,
		terminalCoordinator,
		stepExecutionCounter,
		stepExecutionRegistry,
		flowConfiger,
		basicInfo,
	)
	if updateErr != nil {
		return nil, updateErr
	}

	defer func() {
		// Every normal return must provide a non-nil output or a non-nil error.
		// Cadence cache eviction exits suspended workflow goroutines without a result; it must not finalize the Flow.
		if i.sharedConfig.Interpreter.Cadence != nil && out == nil && retErr == nil {
			return
		}
		retErr = terminalCoordinator.CoordinateAndFinalizeError(ctx, retErr)
		if shouldReportSubFlowCompletion(provider, ctx, subFlowParentFlowID, retErr) {
			if reportErr := i.reportSubFlowCompletion(
				ctx, provider, subFlowParentFlowID, outputCollector.GetAll(), retErr,
			); reportErr != nil {
				retErr = reportErr
			}
		}
		if provider.IsReplaying(ctx) {
			return
		}
		eventType := event.EventTypeUnspecified
		if retErr == nil {
			eventType = event.EventTypeFlowComplete
		} else if provider.IsCanceledError(retErr) {
			eventType = event.EventTypeFlowCancel
		} else if provider.IsApplicationError(retErr) {
			eventType = event.EventTypeFlowFail
		}
		if eventType == event.EventTypeUnspecified {
			return
		}
		info := provider.GetWorkflowInfo(ctx)
		var attributes []*dexpb.KV
		if persistenceManager != nil {
			attributes = persistenceManager.GetAllAttributes()
		}
		// send metrics for the workflow result
		event.Handle(event.Event{
			FlowId:             info.WorkflowExecution.ID,
			RunId:              info.WorkflowExecution.RunID,
			FlowType:           input.GetFlowType(),
			EventType:          eventType,
			StartTimestampInMs: info.WorkflowStartTime.UnixMilli(),
			Attributes:         attributes,
		})
	}()
	// We intentionally set the query handler after the continueAsNew/dumpInternal activity.
	// This is to ensure the correctness. If we set the query handler before that,
	// the query handler could return empty data (since the loading hasn't completed), which will be incorrect response.
	// We would rather return server errors and let the client retry later.
	if err := SetQueryHandlers(
		ctx,
		provider,
		timerProcessor,
		persistenceManager,
		channelStore,
		continueAsNewer,
		stepExecutionCounter,
		flowConfiger,
		basicInfo,
	); err != nil {
		return nil, err
	}

	if !input.GetIsResumeFromContinueAsNew() {
		if flowTimeout.IsEnabled() {
			stepRequestQueue.AddStepResumeRequest(
				flowTimeout.NewStepExecutionResumeInfo(ctx, provider),
			)
		}
		// it's possible that a flow is started without any starting step
		// it will wait for a new step coming in (by RPC results)
		if input.GetStartStepType() != "" {
			stepRequestQueue.AddSingleStepStartRequest(
				input.GetStartStepType(),
				input.GetStepInput(),
				input.GetStepOptions(),
				service.StartingStepFromStepExecutionId,
				nil,
			)
		}

		if !provider.IsReplaying(ctx) {
			info := provider.GetWorkflowInfo(ctx)
			event.Handle(event.Event{
				FlowId:             info.WorkflowExecution.ID,
				RunId:              info.WorkflowExecution.RunID,
				FlowType:           basicInfo.FlowType,
				EventType:          event.EventTypeFlowStart,
				StartTimestampInMs: info.WorkflowStartTime.UnixMilli(),
				Attributes:         persistenceManager.GetAllAttributes(),
			})
		}
	}

	for {
		if err := provider.Await(ctx, func() bool {
			return !stepRequestQueue.IsEmpty() || signalReceiver.IsStopFlowRequested() ||
				errToFailWf != nil || forceCompleteWf ||
				isGracefulCompletionReady(
					isGracefulCompletionRequested,
					stepRequestQueue,
					stepExecutionCounter,
				) ||
				continueAsNewCounter.IsThresholdMet()
		}); err != nil {
			return nil, err
		}

		if stopBySignal, stopErr := signalReceiver.GetIfStopFlowRequested(); stopBySignal {
			return &dexpb.InterpreterWorkflowOutput{
				StepCompletionOutputs: outputCollector.GetAll(),
			}, stopErr
		}
		if errToFailWf != nil || forceCompleteWf {
			return &dexpb.InterpreterWorkflowOutput{
				StepCompletionOutputs: outputCollector.GetAll(),
			}, errToFailWf
		}

		// gracefully complete flow after all user Steps finish
		shouldCompleteFlow, err := prepareGracefulCompletion(
			ctx,
			isGracefulCompletionRequested,
			stepRequestQueue,
			stepExecutionCounter,
			stepExecutionRegistry,
		)
		if err != nil {
			return nil, err
		}
		if shouldCompleteFlow {
			break
		}

		for !stepRequestQueue.IsEmpty() {
			var stepsToExecute []StepRequest
			if !continueAsNewCounter.IsThresholdMet() {
				stepsToExecute = stepRequestQueue.TakeAll()
				if err := stepExecutionCounter.MarkStepTypeActiveIfNotYet(
					ctx,
					stepsToExecute,
				); err != nil {
					return nil, err
				}
			}

			for _, stepReqForLoopingOnly := range stepsToExecute {
				// execute in another thread for parallelism
				// step must be passed via parameter https://stackoverflow.com/questions/67263092
				stepCtx := provider.ExtendContextWithValue(
					ctx,
					"stepRequest",
					stepReqForLoopingOnly,
				)
				stepCtx, stepExecutionID := stepExecutionRegistry.Register(
					stepCtx,
					stepReqForLoopingOnly,
				)
				stepCtx = provider.ExtendContextWithValue(
					stepCtx,
					"stepExecutionID",
					stepExecutionID,
				)
				attributeSynchronizer.ProducerStarted()
				provider.GoNamed(stepCtx, "step-execution-thread:"+stepReqForLoopingOnly.GetStepType(), func(ctx interfaces.UnifiedContext) {
					defer attributeSynchronizer.ProducerFinished()
					stepRequest, ok := provider.GetContextValue(
						ctx,
						"stepRequest",
					).(StepRequest)
					if !ok {
						errToFailWf = provider.NewFlowError(
							dexpb.FlowErrorType_FLOW_ERROR_TYPE_INTERNAL,
							"cannot read step request from workflow context",
						)
						return
					}

					step := stepRequest.GetStepMovement()
					stepExeId, ok := provider.GetContextValue(
						ctx,
						"stepExecutionID",
					).(string)
					if !ok {
						errToFailWf = provider.NewFlowError(
							dexpb.FlowErrorType_FLOW_ERROR_TYPE_INTERNAL,
							"cannot read step execution ID from workflow context",
						)
						return
					}

					decision, stepExecutionStatus, stepExeErr := i.processStepExecution(ctx, provider,
						basicInfo,
						stepRequest,
						stepExeId,
						persistenceManager,
						channelStore,
						timerProcessor,
						continueAsNewer,
						continueAsNewCounter,
						stepExecutionCounter,
						flowConfiger,
						globalVersioner,
						signalReceiver,
						subFlowTracker,
						flowTimeout,
					)
					isCanceled := stepExecutionRegistry.IsCanceled(stepExeId)
					if stepExecutionStatus != service.StepExecutionStatusWaitingAborted {
						// WaitingAborted ends only the current run's goroutine; the logical Step execution remains resumable.
						// Keep it registered through continue-as-new finalization because drained RPC signals or parallel
						// Step decisions can still cancel it. Unregistering it here would consume such cancellation as a
						// no-op, after which the next run would register and incorrectly resume the execution.
						stepExecutionRegistry.Unregister(stepExeId)
					}
					if isCanceled {
						return
					}
					if stepExecutionStatus == service.StepExecutionStatusInternalError {
						errToFailWf = stepExeErr
						return
					}
					if stepExecutionStatus == service.StepExecutionStatusFlowTimeout {
						if stepExeErr == nil {
							panic("FlowTimeout Step execution requires an error")
						}
						continueAsNewer.RemoveActiveStep(stepExeId)
						errToFailWf = stepExeErr
						return
					}
					if stepExecutionStatus == service.StepExecutionStatusFailedNoProceed && stepExeErr != nil {
						// this is the case where stepExecutionStatus == FailureStepExecutionStatus
						errToFailWf = normalizeStepFailureError(provider, stepExeErr)
						// step execution fail should fail the flow, no more processing
						return
					}
					if stepExecutionStatus == service.StepExecutionStatusCompleted {
						// NOTE: decision is only available on this CompletedStepExecutionStatus
						if cancelErr := stepExecutionRegistry.CancelByStepTypesAndSiblingStepTypes(
							ctx,
							decision,
							step.GetFromStepExecutionIdInternalOnly(),
						); cancelErr != nil {
							errToFailWf = cancelErr
							continueAsNewer.RemoveActiveStep(stepExeId)
							if markErr := stepExecutionCounter.MarkStepExecutionCompleted(
								ctx,
								step,
								stepExeId,
								nil,
							); markErr != nil {
								errToFailWf = markErr
							}
							return
						}
						canGoNext, gracefulComplete, forceComplete, forceFail, output, checkErr := checkClosingWorkflow(
							ctx,
							provider,
							decision,
							step.GetStepType(),
							stepExeId,
							channelStore,
							signalReceiver,
						)
						if checkErr != nil {
							errToFailWf = checkErr
							// no return so that it can fall through to call MarkStepExecutionCompleted
						}
						if gracefulComplete {
							isGracefulCompletionRequested = true
						}
						if (gracefulComplete || forceComplete) && output != nil {
							outputCollector.Add(output)
						}
						if forceComplete {
							forceCompleteWf = true
						}
						if forceFail {
							detail := ""
							if output != nil {
								detail = getFlowFailedDetailFromValue(output.GetCompletedStepOutput())
							}
							errToFailWf = provider.NewFlowError(
								dexpb.FlowErrorType_FLOW_ERROR_TYPE_STEP_DECISION_FAILING_FLOW,
								detail,
							)
						}
						if canGoNext {
							stepRequestQueue.AddStepStartRequests(decision.GetNextSteps())
						}
						// finally, mark step completed and may also update system search attribute
						continueAsNewer.RemoveActiveStep(stepExeId)
						if err := stepExecutionCounter.MarkStepExecutionCompleted(
							ctx,
							step,
							stepExeId,
							decision.GetNextSteps(),
						); err != nil {
							errToFailWf = err
						}
					} else if stepExecutionStatus == service.StepExecutionStatusFailedAndProceed {
						recoveryError, mappingErr := provider.MapToRecoveryError(stepExeErr)
						if mappingErr != nil {
							errToFailWf = provider.NewFlowError(
								dexpb.FlowErrorType_FLOW_ERROR_TYPE_INTERNAL,
								mappingErr.Error(),
							)
							return
						}
						options := step.GetStepOptions()
						stepRequestQueue.AddSingleStepStartRequest(
							options.GetExecuteFailureProceedStepType(),
							stepFailureInput(step),
							options.ExecuteFailureProceedStepOptions,
							stepExeId,
							recoveryError,
						)
						// finally, mark state completed and may also update activeStepType search attribute
						continueAsNewer.RemoveActiveStep(stepExeId)
						err := stepExecutionCounter.MarkStepExecutionCompleted(
							ctx,
							step,
							stepExeId,
							decision.GetNextSteps(),
						)
						if err != nil {
							errToFailWf = err
						}
					} else if stepExecutionStatus == service.StepExecutionStatusWaitingAborted {
						// NOTE: noop for WaitingCommandsStepExecutionStatus, because it means continueAsNew
					}

				}) // end of executing one step movement goroutine
			} // end loop of executing all steps from the queue for one pass

			// The conditions here are quite tricky:
			// For !stepRequestQueue.IsEmpty(): We need some condition to wait here because all the step executions are running in different threads.
			//    Right after the queue is popped it becomes empty. When it's not empty, it means there are new steps to execute pushed into the queue,
			//    and it's time to wake up the outer loop to go to next iteration. Alternatively, waiting for all current started in this iteration to complete will also work,
			//    but not as efficient as this one because it will take much longer time.
			// For errToFailFlow != nil || forceCompleteFlow: this means we need to close flow immediately
			// For stepExecutionCounter.GetTotalCurrentlyExecutingCount() == 0: this means all user Step executions have reached "Dead Ends" so the flow can complete gracefully without output
			// For continueAsNewCounter.IsThresholdMet(): this means flow needs to continueAsNew
			awaitError := provider.Await(ctx, func() bool {
				stopBySignal, stopErr := signalReceiver.GetIfStopFlowRequested()
				if stopBySignal {
					errToFailWf = stopErr
					return true
				}
				return !stepRequestQueue.IsEmpty() ||
					errToFailWf != nil ||
					forceCompleteWf ||
					stepExecutionCounter.GetTotalCurrentlyExecutingCount() == 0 ||
					continueAsNewCounter.IsThresholdMet()
			})
			if continueAsNewCounter.IsThresholdMet() {
				// NOTE: drain thread before checking errToFailFlow/forceCompleteFlow so that we can close the flow if possible
				if err := continueAsNewer.DrainThreads(ctx); err != nil {
					awaitError = err
				}
			}

			if errToFailWf != nil || forceCompleteWf {
				return &dexpb.InterpreterWorkflowOutput{
					StepCompletionOutputs: outputCollector.GetAll(),
				}, errToFailWf
			}
			if awaitError != nil {
				// this could happen for cancellation
				return nil, awaitError
			}
			if continueAsNewCounter.IsThresholdMet() {
				// the outer logic will do the actual continue as new
				break
			}
		} // end loop until no more step can be executed (dead end)

		if continueAsNewCounter.IsThresholdMet() {
			// we have to drain this again because this can be from non-step cases
			if err := continueAsNewer.DrainThreads(ctx); err != nil {
				errToFailWf = err
				break
			}
			// NOTE: This must be the last thing before continueAsNew!!!
			// Otherwise, there could be signals unhandled
			signalReceiver.DrainAllReceivedButUnprocessedSignals(ctx)

			// after draining signals, there could be some changes like
			// last fail flow signal, return the flow so that we don't carry over the fail request
			if stopBySignal, stopErr := signalReceiver.GetIfStopFlowRequested(); stopBySignal {
				return &dexpb.InterpreterWorkflowOutput{
					StepCompletionOutputs: outputCollector.GetAll(),
				}, stopErr
			}
			if errToFailWf != nil || forceCompleteWf {
				return &dexpb.InterpreterWorkflowOutput{
					StepCompletionOutputs: outputCollector.GetAll(),
				}, errToFailWf
			}
			shouldCompleteFlow, err := prepareGracefulCompletion(
				ctx,
				isGracefulCompletionRequested,
				stepRequestQueue,
				stepExecutionCounter,
				stepExecutionRegistry,
			)
			if err != nil {
				return nil, err
			}
			if stepRequestQueue.IsEmpty() &&
				!continueAsNewer.HasAnyStepExecutionToResume() &&
				shouldCompleteFlow {
				// if it is empty and no stepExecutionsToResume and request a graceful complete just complete the loop
				// so that we don't carry over the graceful completion request
				return &dexpb.InterpreterWorkflowOutput{
					StepCompletionOutputs: outputCollector.GetAll(),
				}, nil
			}
			// last update config, do it here because we use input to carry over config, not continueAsNewer query
			input.Config = flowConfiger.Get()
			input.IsResumeFromContinueAsNew = true
			input.ContinueAsNewInput = &dexpb.ContinueAsNewInput{
				PreviousInternalRunId: provider.GetWorkflowInfo(ctx).WorkflowExecution.RunID,
			}
			// nix the unused data
			input.StartStepType = ""
			input.StepInput = nil
			input.StepOptions = nil
			input.InitAttributes = nil
			return nil, provider.NewInterpreterContinueAsNewError(ctx, input)
		}

	} // end main loop

	// gracefully complete workflow when all user Steps finish
	return &dexpb.InterpreterWorkflowOutput{
		StepCompletionOutputs: outputCollector.GetAll(),
	}, errToFailWf
}

func prepareGracefulCompletion(
	ctx interfaces.UnifiedContext,
	isRequested bool,
	stepRequestQueue *StepRequestQueue,
	stepExecutionCounter *StepExecutionCounter,
	stepExecutionRegistry *StepExecutionRegistry,
) (bool, error) {
	if !isGracefulCompletionReady(isRequested, stepRequestQueue, stepExecutionCounter) {
		return false, nil
	}
	if err := stepExecutionRegistry.CancelByStepTypes(
		ctx,
		[]string{service.FlowTimeoutStepType},
	); err != nil {
		return false, err
	}
	return true, nil
}

func isGracefulCompletionReady(
	isGracefulCompletionRequested bool,
	stepRequestQueue *StepRequestQueue,
	stepExecutionCounter *StepExecutionCounter,
) bool {
	return isGracefulCompletionRequested &&
		!stepRequestQueue.HasUserRequests() &&
		stepExecutionCounter.GetTotalCurrentlyExecutingCount() == 0
}

func normalizeStepFailureError(
	provider interfaces.WorkflowProvider,
	err error,
) error {
	if err == nil {
		return nil
	}
	if provider.IsApplicationError(err) {
		return provider.NewFlowErrorFromActivityError(err)
	}
	// Non-application activity failures (timeout, cancel, etc.) as worker API fail.
	return provider.NewFlowError(
		dexpb.FlowErrorType_FLOW_ERROR_TYPE_WORKER_API_FAIL,
		err.Error(),
	)
}

func checkClosingWorkflow(
	ctx interfaces.UnifiedContext,
	provider interfaces.WorkflowProvider,
	decision *dexpb.StepDecision,
	currentStepType, currentStepExeId string,
	channelStore *ChannelStore,
	signalReceiver *SignalReceiver,
) (
	canGoNext bool,
	gracefulComplete bool,
	forceComplete bool,
	forceFail bool,
	completeOutput *dexpb.StepCompletionOutput,
	err error,
) {
	closeDecision := decision.GetCloseDecision()
	if closeDecision == nil {
		canGoNext = true
		return
	}

	switch closeDecision.GetCloseDecisionType() {
	case dexpb.CloseDecisionType_CLOSE_DECISION_TYPE_FORCE_COMPLETE_ON_CHANNELS_EMPTY:
		// trigger a signal draining so that all the channel messages are processed
		signalReceiver.DrainAllReceivedButUnprocessedSignals(ctx)
		// Messages of channels could be published via step executions, within the same workflow task.
		// If we don't do any draining and process them, the conditional completion could lose the messages
		// Just yield, by waiting on an empty lambda, nothing else.
		// It will let other workflow threads/coroutines run.
		// This will drain the messages published from step APIs.
		// NOTE that this is extremely tricky in Cadence/Temporal programming model.
		// Read more: https://stackoverflow.com/questions/71356668/how-does-multi-threading-works-in-cadence-temporal-workflow
		// https://docs.temporal.io/encyclopedia/go-sdk-multithreading
		if err = provider.Await(ctx, func() bool { return true }); err != nil {
			return
		}

		conditionMet := true
		for _, channelName := range closeDecision.GetConditionalChannelNames() {
			if channelStore.HasPendingData(channelName) {
				conditionMet = false
			}
		}

		if conditionMet {
			// condition is met, force complete the workflow
			forceComplete = true
			completeOutput = &dexpb.StepCompletionOutput{
				CompletedStepType:        currentStepType,
				CompletedStepExecutionId: currentStepExeId,
				CompletedStepOutput:      closeDecision.GetCloseInput(),
			}
			return
		}
		canGoNext = true
		return
	case dexpb.CloseDecisionType_CLOSE_DECISION_TYPE_GRACEFUL_COMPLETE:
		gracefulComplete = true
	case dexpb.CloseDecisionType_CLOSE_DECISION_TYPE_FORCE_COMPLETE:
		forceComplete = true
	case dexpb.CloseDecisionType_CLOSE_DECISION_TYPE_FORCE_FAIL:
		forceFail = true
	case dexpb.CloseDecisionType_CLOSE_DECISION_TYPE_DEAD_END:
		return
	default:
		err = provider.NewFlowError(
			dexpb.FlowErrorType_FLOW_ERROR_TYPE_INTERNAL,
			"invalid close decision type",
		)
		return
	}

	completeOutput = &dexpb.StepCompletionOutput{
		CompletedStepType:        currentStepType,
		CompletedStepExecutionId: currentStepExeId,
		CompletedStepOutput:      closeDecision.GetCloseInput(),
	}
	return
}

func (i *Interpreter) processStepExecution(
	ctx interfaces.UnifiedContext,
	provider interfaces.WorkflowProvider,
	basicInfo service.BasicInfo,
	stepRequest StepRequest,
	stepExeId string,
	persistenceManager *PersistenceManager,
	channelStore *ChannelStore,
	timerProcessor interfaces.TimerProcessor,
	continueAsNewer *ContinueAsNewer,
	continueAsNewCounter *cont.ContinueAsNewCounter,
	stepExecutionCounter *StepExecutionCounter,
	flowConfiger *interpreterconfig.FlowConfiger,
	globalVersioner *GlobalVersioner,
	signalReceiver *SignalReceiver,
	subFlowTracker *SubFlowTracker,
	flowTimeout *FlowTimeout,
) (*dexpb.StepDecision, service.StepExecutionStatus, error) {
	info := provider.GetWorkflowInfo(ctx)
	step := stepRequest.GetStepMovement()
	executionContext := &dexpb.Context{
		FlowId:               info.WorkflowExecution.ID,
		RunId:                info.FirstRunID,
		FlowStartedTimestamp: info.WorkflowStartTime.Unix(),
		StepExecutionId:      stepExeId,
		FromStepExecutionId:  step.GetFromStepExecutionIdInternalOnly(),
		RecoveryError:        step.GetRecoveryErrorInternalOnly(),
	}
	activityOptions := stepActivityOptions(
		step.GetStepOptions().GetWaitForRetryPolicy(),
		step.GetStepOptions().GetWaitForTimeoutSeconds(),
		step.GetStepOptions().GetHeartbeatTimeoutSeconds(),
	)
	if globalVersioner.UsesDeterministicStepActivityIDs() {
		activityOptions.ActivityID = service.WaitForStepActivityID(stepExeId)
	}

	var waitForMethErr error
	var stepExeLocals []*dexpb.KV
	var waitingCondition *dexpb.WaitingConditionState

	//This variable tells all (timer) condition threads to stop waiting and exit, even if their specific condition has not been completed.
	waitingConditionDoneOrCanceled := false
	completedTimerConditions := map[int32]dexpb.InternalTimerStatus{}
	completedSubFlowResults := map[int32]*dexpb.FlowResult{}

	isResumeFromContinueAsNew := stepRequest.IsResumeRequest()

	options := step.GetStepOptions()
	if options.GetSkipWaitFor() {
		return i.invokeExecuteMethod(
			ctx,
			provider,
			basicInfo,
			step,
			stepExeId,
			persistenceManager,
			channelStore,
			executionContext,
			nil,
			continueAsNewer,
			flowConfiger,
			stepExeLocals,
			globalVersioner,
		)
	}

	if isResumeFromContinueAsNew {
		resumeRequest := stepRequest.GetStepResumeRequest()
		if globalVersioner.UsesPullRoutingVisibility() {
			continueAsNewer.RecordWaitForPullRouting(stepExeId, resumeRequest.GetWaitForPullRouting())
		}
		stepExeLocals = resumeRequest.GetStepExeLocals()
		waitingCondition = resumeRequest.GetWaitingCondition()
		if resumeRequest.GetCompletedConditions() != nil &&
			resumeRequest.GetCompletedConditions().GetCompletedTimerConditions() != nil {
			completedTimerConditions = resumeRequest.GetCompletedConditions().GetCompletedTimerConditions()
		}

		if len(waitingCondition.GetSubFlowConditions()) > 0 {
			completedSubFlowResults = subFlowTracker.MustGetCompletedResults(stepExeId)
		}
	} else {
		ctx = provider.WithActivityOptions(ctx, activityOptions)

		lockAttributeKeys := options.GetWaitForLockAttributeKeys()
		selection, err := service.ValidateAndSortStateSelections(
			options.GetWaitForLoadAttributeMapInstances(),
			options.GetWaitForLoadChannelNames(),
			options.GetWaitForLoadChannelMapInstances(),
		)
		if err != nil {
			return nil, service.StepExecutionStatusInternalError, err
		}
		attributes, err := persistenceManager.LoadSelectedAttributes(
			ctx,
			lockAttributeKeys,
			selection.AttributeMapInstances,
		)
		if err != nil {
			return nil, service.StepExecutionStatusInternalError, err
		}

		activityInput := &dexpb.InvokeWaitForMethodActivityInput{
			WorkerTarget: flowConfiger.GetWorkerTarget(),
			Request: &dexpb.InvokeWaitForMethodRequest{
				Context:      executionContext,
				FlowType:     basicInfo.FlowType,
				StepType:     step.GetStepType(),
				StepInput:    step.GetStepInput(),
				Attributes:   attributes,
				ChannelInfos: channelStore.GetInfos(),
				LoadedChannelMessages: channelStore.GetLoadedMessages(
					selection.ChannelNames,
					selection.ChannelMapInstances,
				),
				LoadedAttributeMapInstances: selection.AttributeMapInstances,
				LoadedChannelNames:          selection.ChannelNames,
				LoadedChannelMapInstances:   selection.ChannelMapInstances,
			},
		}
		if flowConfiger.GetWorkerTarget().GetIsPullMode() {
			activityInput.DexTaskQueue = continueAsNewer.DexTaskQueue()
			activityInput.DispatchWait = durationpb.New(i.activities.MatchingConfig().DispatchWait)
			activityInput.StickyWait = durationpb.New(i.activities.MatchingConfig().StickyWait)
		}
		if globalVersioner.UsesPullRoutingVisibility() {
			continueAsNewer.RecordWaitForPullRouting(stepExeId, pulldispatch.Routing(activityInput.GetWorkerTarget(), activityInput.GetDexTaskQueue(), ""))
		}
		var activityOutput dexpb.InvokeWaitForMethodActivityOutput
		waitForMethErr = provider.ExecuteActivity(
			&activityOutput,
			flowConfiger.ResolveWaitForDurability(options),
			ctx,
			i.activities.IWaitForM,
			activityInput,
			&dexpb.InternalLocalActivityInput{
				CurrentRunStartedTimestamp: basicInfo.RunStartedTimestamp,
				MethodOptions:              stepMethodOptions(activityOptions),
			},
		)
		persistenceManager.UnlockKeys(lockAttributeKeys)

		if err := ctx.Err(); err != nil {
			return nil, service.StepExecutionStatusInternalError, err
		}

		if globalVersioner.UsesPullRoutingVisibility() && waitForMethErr == nil {
			continueAsNewer.RecordWaitForPullRouting(stepExeId, activityOutput.GetPullRouting())
		}

		if waitForMethErr != nil && !shouldProceedOnWaitForMethodError(step) {
			return nil, service.StepExecutionStatusFailedNoProceed, waitForMethErr
		}

		var returnedWaitingCondition *dexpb.WaitingCondition
		if waitForMethErr == nil {
			returnedWaitingCondition = activityOutput.Response.GetWaitingCondition()
			if err := persistenceManager.ApplyAttributeWritesWithActionPermissionMappings(
				ctx,
				activityOutput.Response.GetUpsertAttributes(),
				activityOutput.Response.GetActionPermissionMappings(),
			); err != nil {
				return nil, service.StepExecutionStatusInternalError, err
			}
			channelStore.DeleteAll(activityOutput.Response.GetDeleteFromChannel())
			channelStore.ProcessPublishing(activityOutput.Response.GetPublishToChannel())
			stepExeLocals = activityOutput.Response.GetUpsertStepExeLocals()
			continueAsNewer.RecordServedByAppWorkerID(activityOutput.GetServedByAppWorkerId())

		}

		waitingCondition = convertToWaitingConditionState(
			ctx, provider, returnedWaitingCondition,
		)

		if len(waitingCondition.GetSubFlowConditions()) > 0 {
			subFlowTracker.Register(stepExeId, waitingCondition, completedSubFlowResults)
			starter := NewSubFlowStarter(
				provider,
				globalVersioner,
				i.activities,
				subFlowTracker,
				stepExeId,
				basicInfo,
				flowConfiger.Get(),
				returnedWaitingCondition,
				&i.sharedConfig.Interpreter.InterpreterActivityConfig,
			)
			if err := starter.StartAll(ctx); err != nil {
				if globalVersioner.UsesTypedSubFlowStartFailures() && !provider.IsCanceledError(err) {
					detail, isRejection, rejectionErr := provider.IsSubFlowStartRejection(err)
					if rejectionErr != nil {
						return nil, service.StepExecutionStatusInternalError, fmt.Errorf("read SubFlow start rejection: %w", rejectionErr)
					}
					if isRejection {
						err = provider.NewFlowError(
							dexpb.FlowErrorType_FLOW_ERROR_TYPE_INVALID_USER_FLOW_CODE,
							"SubFlow start rejected: "+detail,
						)
					}
				}
				return nil, service.StepExecutionStatusInternalError, err
			}
		}
	} // end of NOT isResumeFromContinueAsNew

	waitForThreads := map[string]bool{}

	if len(waitingCondition.GetTimerConditions()) > 0 {
		timerProcessor.AddTimers(
			stepExeId,
			waitingCondition.GetTimerConditions(),
			completedTimerConditions,
		)
		for idx, timerCondition := range waitingCondition.GetTimerConditions() {
			if _, ok := completedTimerConditions[int32(idx)]; ok {
				// skip the completed timers(from continueAsNew)
				continue
			}
			timerCtx := provider.ExtendContextWithValue(ctx, "timerIndex", idx)
			//Start timer in a new thread
			threadName := fmt.Sprintf(
				"%s-timer-%d-%s",
				stepExeId,
				idx,
				timerCondition.GetConditionId(),
			)
			waitForThreads[threadName] = false
			provider.GoNamed(timerCtx, threadName, func(ctx interfaces.UnifiedContext) {
				index, ok := provider.GetContextValue(ctx, "timerIndex").(int)
				if !ok {
					panic("cannot read timer index from workflow context")
				}

				// Note that waitingConditionDoneOrCanceled is needed for two cases:
				// 1. will be true when trigger type of the waitingCondition is completed(e.g. AnyCompleted) so we don't need to wait for all conditions. Returning the thread to avoid thread leakage.
				// 2. will be true to cancel the wait for unblocking continueAsNew(continueAsNew will wait for all threads to complete)
				status := timerProcessor.WaitForTimerFiredOrSkipped(
					ctx,
					stepExeId,
					index,
					&waitingConditionDoneOrCanceled,
				)
				if status == dexpb.InternalTimerStatus_INTERNAL_TIMER_STATUS_FIRED ||
					status == dexpb.InternalTimerStatus_INTERNAL_TIMER_STATUS_SKIPPED {
					completedTimerConditions[int32(index)] = status
				}
				waitForThreads[threadName] = true
			})
		}
	}

	//Passing a map of references of completed or soon to be completed conditions (once the above threads are complete) and the step execution variables to the continueAsNewer.
	//After this method completes and if continueAsNewCounter.IsThresholdMet() is true, this snapshot will be used to start a new continueAsNew flow while preserving the state of the flow at the end of this method.
	//This snapshot is also used to query the flow state, which can be done at anytime.
	continueAsNewer.AddPotentialStepExecutionToResume(
		&dexpb.StepExecutionResumeInfo{
			StepExecutionId: stepExeId,
			Step:            step,
			CompletedConditions: &dexpb.StepExecutionCompletedConditions{
				CompletedTimerConditions: completedTimerConditions,
				CompletedSubFlowResults:  completedSubFlowResults,
			},
			WaitingCondition: waitingCondition,
			StepExeLocals:    stepExeLocals,
		},
	)

	var matchPlan *condition.MatchPlan
	var conditionMet bool

	// Wait for condition met, stop signal, or continue-as-new threshold
	if err := provider.Await(ctx, func() bool {
		matchPlan, conditionMet = condition.Plan(
			waitingCondition,
			channelStore.Availability(),
			completedTimerConditions,
			completedSubFlowResults,
		)
		return conditionMet || signalReceiver.IsStopFlowRequested() ||
			continueAsNewCounter.IsThresholdMet()
	}); err != nil {
		waitingConditionDoneOrCanceled = true
		return nil, service.StepExecutionStatusInternalError, err
	}

	waitingConditionDoneOrCanceled = true

	if err := provider.Await(ctx, func() bool {
		for _, isCompleted := range waitForThreads {
			if !isCompleted {
				return false
			}
		}
		return true
	}); err != nil {
		return nil, service.StepExecutionStatusInternalError, err
	}

	if signalReceiver.IsStopFlowRequested() || !conditionMet {
		// this means stop was requested or continueAsNewCounter.IsThresholdMet == true
		// not using only continueAsNewCounter.IsThresholdMet because matchPlan has higher priority without a terminal request
		// it won't continueAsNew in those cases
		// 1. waitFor method fail with proceed policy
		// 2. empty condition
		// 3. both condition and continueAsNewThreshold are met
		return nil, service.StepExecutionStatusWaitingAborted, nil
	}

	if len(waitingCondition.GetTimerConditions()) > 0 {
		timerProcessor.RemovePendingTimersOfStep(stepExeId)
	}

	consumed := channelStore.CommitMatch(matchPlan)
	defer channelStore.CompleteMatch(matchPlan)
	conditionResults := condition.BuildConditionResults(
		waitingCondition,
		completedTimerConditions,
		consumed,
		completedSubFlowResults,
	)
	subFlowTracker.Unregister(stepExeId)
	if waitForMethErr != nil {
		conditionResults.WaitForFailed = true
		recoveryError, mappingErr := provider.MapToRecoveryError(waitForMethErr)
		if mappingErr != nil {
			return nil, service.StepExecutionStatusInternalError, mappingErr
		}
		executionContext.RecoveryError = recoveryError
	}
	if step.GetStepType() == service.FlowTimeoutStepType && !flowTimeout.UsesHandler() {
		continueAsNewer.RemoveStepExecutionToResume(stepExeId)
		return nil, service.StepExecutionStatusFlowTimeout,
			flowTimeout.NewTerminalError(provider)
	}
	return i.invokeExecuteMethod(
		ctx,
		provider,
		basicInfo,
		step,
		stepExeId,
		persistenceManager,
		channelStore,
		executionContext,
		conditionResults,
		continueAsNewer,
		flowConfiger,
		stepExeLocals,
		globalVersioner,
	)
}

func shouldReportSubFlowCompletion(
	provider interfaces.WorkflowProvider,
	ctx interfaces.UnifiedContext,
	parentFlowID string,
	flowErr error,
) bool {
	if parentFlowID == "" || provider.IsContinueAsNewError(flowErr) {
		return false
	}
	if provider.IsCanceledError(flowErr) {
		return true
	}
	info := provider.GetWorkflowInfo(ctx)
	// We support retry policy for subFlow.
	// When using retry policy and having error, we will let subFlow retry until the last attempt
	if flowErr == nil || info.RetryMaximumAttempts == nil {
		return true
	}
	return *info.RetryMaximumAttempts > 0 && info.Attempt >= *info.RetryMaximumAttempts
}

func (i *Interpreter) reportSubFlowCompletion(
	ctx interfaces.UnifiedContext,
	provider interfaces.WorkflowProvider,
	parentFlowID string,
	outputs []*dexpb.StepCompletionOutput,
	flowErr error,
) error {
	info := provider.GetWorkflowInfo(ctx)
	result := &dexpb.FlowResult{
		FlowStatus: dexpb.FlowStatus_FLOW_STATUS_COMPLETED,
		Results:    outputs,
	}
	if flowErr != nil {
		result.FlowStatus = dexpb.FlowStatus_FLOW_STATUS_FAILED
		if provider.IsCanceledError(flowErr) {
			result.FlowStatus = dexpb.FlowStatus_FLOW_STATUS_CANCELED
		}
		errorType, recoveryError, mappingErr := provider.MapToFlowResultError(flowErr)
		if mappingErr != nil {
			return fmt.Errorf("map SubFlow completion error: %w", mappingErr)
		}
		result.ErrorType = errorType
		result.ErrorMessage = recoveryError.GetDetail()
		ctx = provider.NewDisconnectedContext(ctx)
	}
	ctx = provider.WithActivityOptions(ctx, interfaces.ActivityOptions{
		StartToCloseTimeout:                 30 * time.Second,
		LocalActivityScheduleToCloseTimeout: 2 * time.Minute,
	})
	var activityOutput dexpb.ReportSubFlowCompletionActivityOutput
	return provider.ExecuteLocalActivity(
		&activityOutput,
		ctx,
		i.activities.ReportSubFlowCompletion,
		&dexpb.ReportSubFlowCompletionActivityInput{
			ParentFlowId: parentFlowID,
			Request: &dexpb.SubFlowCompletionSignalRequest{
				SubFlowId:  info.WorkflowExecution.ID,
				FlowResult: result,
			},
		},
	)
}

func (i *Interpreter) invokeExecuteMethod(
	ctx interfaces.UnifiedContext,
	provider interfaces.WorkflowProvider,
	basicInfo service.BasicInfo,
	step *dexpb.StepMovement,
	stepExeId string,
	persistenceManager *PersistenceManager,
	channelStore *ChannelStore,
	executionContext *dexpb.Context,
	conditionResults *dexpb.ConditionResults,
	continueAsNewer *ContinueAsNewer,
	flowConfiger *interpreterconfig.FlowConfiger,
	stepExeLocals []*dexpb.KV,
	globalVersioner *GlobalVersioner,
) (*dexpb.StepDecision, service.StepExecutionStatus, error) {
	var err error
	activityOptions := stepActivityOptions(
		step.GetStepOptions().GetExecuteRetryPolicy(),
		step.GetStepOptions().GetExecuteTimeoutSeconds(),
		step.GetStepOptions().GetHeartbeatTimeoutSeconds(),
	)
	if globalVersioner.UsesDeterministicStepActivityIDs() {
		activityOptions.ActivityID = service.ExecuteStepActivityID(stepExeId)
	}
	ctx = provider.WithActivityOptions(ctx, activityOptions)

	options := step.GetStepOptions()
	lockAttributeKeys := options.GetExecuteLockAttributeKeys()
	selection, err := service.ValidateAndSortStateSelections(
		options.GetExecuteLoadAttributeMapInstances(),
		options.GetExecuteLoadChannelNames(),
		options.GetExecuteLoadChannelMapInstances(),
	)
	if err != nil {
		return nil, service.StepExecutionStatusInternalError, err
	}
	attributes, err := persistenceManager.LoadSelectedAttributes(
		ctx,
		lockAttributeKeys,
		selection.AttributeMapInstances,
	)
	if err != nil {
		return nil, service.StepExecutionStatusInternalError, err
	}

	continueAsNewer.RemoveStepExecutionToResume(stepExeId)
	activityInput := &dexpb.InvokeExecuteMethodActivityInput{
		WorkerTarget: flowConfiger.GetWorkerTarget(),
		Request: &dexpb.InvokeExecuteMethodRequest{
			Context:          executionContext,
			FlowType:         basicInfo.FlowType,
			StepType:         step.GetStepType(),
			StepInput:        step.GetStepInput(),
			Attributes:       attributes,
			StepExeLocals:    stepExeLocals,
			ConditionResults: conditionResults,
			ChannelInfos:     channelStore.GetInfos(),
			LoadedChannelMessages: channelStore.GetLoadedMessages(
				selection.ChannelNames,
				selection.ChannelMapInstances,
			),
			LoadedAttributeMapInstances: selection.AttributeMapInstances,
			LoadedChannelNames:          selection.ChannelNames,
			LoadedChannelMapInstances:   selection.ChannelMapInstances,
		},
	}
	if flowConfiger.GetWorkerTarget().GetIsPullMode() {
		activityInput.DexTaskQueue = continueAsNewer.DexTaskQueue()
		activityInput.DispatchWait = durationpb.New(i.activities.MatchingConfig().DispatchWait)
		activityInput.StickyWait = durationpb.New(i.activities.MatchingConfig().StickyWait)
	}
	if globalVersioner.UsesPullRoutingVisibility() {
		continueAsNewer.RecordExecutePullRouting(stepExeId, pulldispatch.Routing(activityInput.GetWorkerTarget(), activityInput.GetDexTaskQueue(), ""))
	}
	var activityOutput dexpb.InvokeExecuteMethodActivityOutput
	exeMethErr := provider.ExecuteActivity(
		&activityOutput,
		flowConfiger.ResolveExecuteDurability(step.GetStepOptions()),
		ctx,
		i.activities.IExecuteM,
		activityInput,
		&dexpb.InternalLocalActivityInput{
			CurrentRunStartedTimestamp: basicInfo.RunStartedTimestamp,
			MethodOptions:              stepMethodOptions(activityOptions),
		},
	)
	// always unlock regardless of step success/failure
	persistenceManager.UnlockKeys(lockAttributeKeys)

	if err := ctx.Err(); err != nil {
		return nil, service.StepExecutionStatusInternalError, err
	}
	if exeMethErr != nil {
		if shouldProceedOnExecuteMethodError(step) {
			return nil, service.StepExecutionStatusFailedAndProceed, exeMethErr
		}
		return nil, service.StepExecutionStatusFailedNoProceed, exeMethErr
	}
	if globalVersioner.UsesPullRoutingVisibility() {
		continueAsNewer.RecordExecutePullRouting(stepExeId, activityOutput.GetPullRouting())
	}
	executeResponse := activityOutput.GetResponse()
	if err := persistenceManager.ApplyAttributeWritesWithActionPermissionMappings(
		ctx,
		executeResponse.GetUpsertAttributes(),
		executeResponse.GetActionPermissionMappings(),
	); err != nil {
		return nil, service.StepExecutionStatusInternalError, err
	}
	channelStore.DeleteAll(executeResponse.GetDeleteFromChannel())
	channelStore.ProcessPublishing(executeResponse.GetPublishToChannel())
	continueAsNewer.RecordServedByAppWorkerID(activityOutput.GetServedByAppWorkerId())

	return executeResponse.GetStepDecision(), service.StepExecutionStatusCompleted, nil
}

func stepMethodOptions(options interfaces.ActivityOptions) *dexpb.StepMethodOptions {
	return &dexpb.StepMethodOptions{
		TimeoutSeconds:          int32(options.StartToCloseTimeout / time.Second),
		RetryPolicy:             retry.ActivityRetryPolicyToProto(options.RetryPolicy),
		HeartbeatTimeoutSeconds: int32(options.HeartbeatTimeout / time.Second),
	}
}

func stepActivityOptions(
	retryPolicy *dexpb.RetryPolicy,
	timeoutSeconds int32,
	heartbeatTimeoutSeconds int32,
) interfaces.ActivityOptions {
	options := interfaces.ActivityOptions{
		StartToCloseTimeout: defaultStepMethodTimeout,
		HeartbeatTimeout:    defaultStepHeartbeatTimeout,
		RetryPolicy:         retry.ActivityRetryPolicyFromProto(retryPolicy),
	}
	if timeoutSeconds > 0 {
		options.StartToCloseTimeout = time.Duration(timeoutSeconds) * time.Second
	}
	if heartbeatTimeoutSeconds > 0 {
		options.HeartbeatTimeout = time.Duration(heartbeatTimeoutSeconds) * time.Second
	}
	return options
}

func shouldProceedOnWaitForMethodError(step *dexpb.StepMovement) bool {
	return step.GetStepOptions().GetWaitForFailurePolicy() ==
		dexpb.WaitForMethodFailurePolicy_WAIT_FOR_METHOD_FAILURE_POLICY_PROCEED_ON_FAILURE
}

func shouldProceedOnExecuteMethodError(step *dexpb.StepMovement) bool {
	options := step.GetStepOptions()
	return options.GetExecuteFailureProceedStepType() != "" &&
		options.GetExecuteFailurePolicy() ==
			dexpb.ExecuteMethodFailurePolicy_EXECUTE_METHOD_FAILURE_POLICY_PROCEED_TO_CONFIGURED_STEP
}

func stepFailureInput(step *dexpb.StepMovement) *dexpb.Value {
	if step.GetStepType() != service.FlowTimeoutStepType {
		return step.GetStepInput()
	}
	return &dexpb.Value{
		Kind: &dexpb.Value_NullValue{
			NullValue: structpb.NullValue_NULL_VALUE,
		},
	}
}

func (i *Interpreter) BlobStoreCleanup(
	ctx interfaces.UnifiedContext,
	provider interfaces.WorkflowProvider,
	storeId string,
) (int, error) {
	activityCtx := provider.WithActivityOptions(ctx, interfaces.ActivityOptions{
		StartToCloseTimeout: 24 * time.Hour,
		RetryPolicy:         &config.RetryPolicy{MaximumAttempts: 10},
	})
	var output dexpb.CleanupBlobStoreActivityOutput
	if err := provider.ExecuteActivity(
		&output,
		dexpb.StepDurability_STEP_DURABILITY_SYNC,
		activityCtx,
		i.activities.CleanupBlobsAfterAllRunsDeleted,
		&dexpb.CleanupBlobStoreActivityInput{
			StoreId: storeId,
		},
		nil,
	); err != nil {
		return 0, err
	}
	return int(output.GetTotalDeleted()), nil
}

func getFlowFailedDetailFromValue(value *dexpb.Value) string {
	if value == nil {
		return ""
	}
	return value.GetStringValue()
}
