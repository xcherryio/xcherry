// Legacy Materials in this file remain under their original licenses.
// See LEGACY_NOTICES.md.

// Modifications Copyright (c) 2026 Super Durable, Inc.
//
// Modifications after the Legacy Cutoff are licensed under the
// Sustainable Use License 1.0.
// Legacy Materials remain under their original licenses.
// See LICENSE and LEGACY_NOTICES.md.

package history

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/superdurable/dex/gen/dexpb"
	"github.com/superdurable/dex/service/common/blobstore"
	"github.com/superdurable/dex/service/common/pulldispatch"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const rpcSourcePrefix = "__rpc/"

type Builder struct {
	stickyAppWorkerID    string
	flowID               string
	runID                string
	events               []*dexpb.FlowHistoryEvent
	scheduledActivities  map[int64]*scheduledActivity
	pendingLocalFailures map[string]*pendingLocalActivityFailure
	startEvent           *dexpb.FlowHistoryEvent
	continueDumpPages    map[int32][]byte
	continueDumpTotal    int32
	canceledActivities   map[int64]struct{}
}

type scheduledActivity struct {
	scheduledTime     time.Time
	durability        dexpb.StepDurability
	methodOptions     *dexpb.StepMethodOptions
	waitInput         *dexpb.InvokeWaitForMethodActivityInput
	executeInput      *dexpb.InvokeExecuteMethodActivityInput
	firstStartedTime  time.Time
	firstAttemptTime  time.Time
	priorAttemptCount int32
	finalAttempt      int32
	lastFailure       *dexpb.StepMethodFailure
}

type pendingLocalActivityFailure struct {
	eventID   int64
	eventTime time.Time
	method    string
	failure   *dexpb.StepMethodFailure
	metadata  *dexpb.InternalLocalStepActivityFailure
}

func NewBuilder(flowID string, runID string) *Builder {
	return &Builder{
		flowID:               flowID,
		runID:                runID,
		scheduledActivities:  map[int64]*scheduledActivity{},
		pendingLocalFailures: map[string]*pendingLocalActivityFailure{},
		continueDumpPages:    map[int32][]byte{},
		canceledActivities:   map[int64]struct{}{},
	}
}

func (b *Builder) RecordStart(
	eventID int64,
	eventTime time.Time,
	input *dexpb.InterpreterWorkflowInput,
) {
	payload := &dexpb.FlowStartedOrContinuedHistoryEvent{
		FlowExecutionId:       &dexpb.FlowExecutionID{FlowId: b.flowID, RunId: b.runID},
		FlowType:              input.GetFlowType(),
		FlowConfig:            input.GetConfig(),
		FlowTimeoutPolicy:     input.GetFlowTimeoutPolicy(),
		TimeoutHandlerOptions: input.GetTimeoutHandlerOptions(),
	}
	if input.GetConfiguredFlowTimeoutSeconds() > 0 {
		payload.FlowTimeout = durationpb.New(
			time.Duration(input.GetConfiguredFlowTimeoutSeconds()) * time.Second,
		)
	}
	if input.GetIsResumeFromContinueAsNew() {
		payload.StartOrContinue = &dexpb.FlowStartedOrContinuedHistoryEvent_ContinuedStart{
			ContinuedStart: &dexpb.FlowContinuedStart{
				PreviousRunId: input.GetContinueAsNewInput().GetPreviousInternalRunId(),
			},
		}
	} else {
		payload.StartOrContinue = &dexpb.FlowStartedOrContinuedHistoryEvent_InitialStart{
			InitialStart: &dexpb.FlowInitialStart{
				StartStepType:     input.GetStartStepType(),
				StepInput:         input.GetStepInput(),
				StepOptions:       input.GetStepOptions(),
				InitialAttributes: attributeWritesToKVs(input.GetInitAttributes()),
			},
		}
	}
	b.startEvent = newEvent(
		eventID,
		eventTime,
		&dexpb.FlowHistoryEvent_FlowStartedOrContinued{FlowStartedOrContinued: payload},
	)
}

func (b *Builder) RecordTimeTravelFork(
	eventID int64,
	eventTime time.Time,
	previousRunID string,
) {
	b.events = append(b.events, newEvent(
		eventID,
		eventTime,
		&dexpb.FlowHistoryEvent_TimeTravelFork{
			TimeTravelFork: &dexpb.TimeTravelForkHistoryEvent{PreviousRunId: previousRunID},
		},
	))
}

func attributeWritesToKVs(writes []*dexpb.AttributeWrite) []*dexpb.KV {
	attributes := make([]*dexpb.KV, 0, len(writes))
	for _, write := range writes {
		if write == nil {
			continue
		}
		attributes = append(attributes, &dexpb.KV{Key: write.GetKey(), Value: write.GetValue()})
	}
	return attributes
}

func (b *Builder) RecordWaitScheduled(
	eventID int64,
	eventTime time.Time,
	input *dexpb.InvokeWaitForMethodActivityInput,
	durability dexpb.StepDurability,
	methodOptions *dexpb.StepMethodOptions,
) {
	if metadata := b.takeLocalFailure(
		blobstore.StepEventInputMethodWaitFor,
		input.GetRequest().GetContext().GetStepExecutionId(),
	); metadata != nil {
		durability = dexpb.StepDurability_STEP_DURABILITY_ASYNC
		methodOptions = metadata.GetMethodOptions()
		b.scheduledActivities[eventID] = &scheduledActivity{
			scheduledTime:     eventTime,
			durability:        durability,
			methodOptions:     methodOptions,
			waitInput:         input,
			firstAttemptTime:  time.Unix(metadata.GetFirstAttemptTimestamp(), 0),
			priorAttemptCount: metadata.GetAttempt(),
		}
		return
	}
	b.scheduledActivities[eventID] = &scheduledActivity{
		scheduledTime: eventTime,
		durability:    durability,
		methodOptions: methodOptions,
		waitInput:     input,
	}
}

func (b *Builder) RecordExecuteScheduled(
	eventID int64,
	eventTime time.Time,
	input *dexpb.InvokeExecuteMethodActivityInput,
	durability dexpb.StepDurability,
	methodOptions *dexpb.StepMethodOptions,
) {
	if metadata := b.takeLocalFailure(
		blobstore.StepEventInputMethodExecute,
		input.GetRequest().GetContext().GetStepExecutionId(),
	); metadata != nil {
		durability = dexpb.StepDurability_STEP_DURABILITY_ASYNC
		methodOptions = metadata.GetMethodOptions()
		b.scheduledActivities[eventID] = &scheduledActivity{
			scheduledTime:     eventTime,
			durability:        durability,
			methodOptions:     methodOptions,
			executeInput:      input,
			firstAttemptTime:  time.Unix(metadata.GetFirstAttemptTimestamp(), 0),
			priorAttemptCount: metadata.GetAttempt(),
		}
		return
	}
	b.scheduledActivities[eventID] = &scheduledActivity{
		scheduledTime: eventTime,
		durability:    durability,
		methodOptions: methodOptions,
		executeInput:  input,
	}
}

func (b *Builder) RecordActivityStarted(
	eventTime time.Time,
	scheduledEventID int64,
	attempt int32,
	lastFailure *dexpb.StepMethodFailure,
) {
	scheduled := b.scheduledActivities[scheduledEventID]
	if scheduled == nil {
		return
	}
	backendAttempt := attempt
	attempt += scheduled.previousAttempts()
	if scheduled.firstStartedTime.IsZero() {
		scheduled.firstStartedTime = eventTime
	}
	if attempt > scheduled.finalAttempt {
		scheduled.finalAttempt = attempt
	}
	if backendAttempt <= 1 {
		return
	}
	scheduled.lastFailure = lastFailure
	if lastFailure != nil {
		lastFailure.Attempt = attempt - 1
	}
}

func (b *Builder) RecordLocalActivityFailed(
	eventID int64,
	eventTime time.Time,
	method string,
	failure *dexpb.StepMethodFailure,
	metadata *dexpb.InternalLocalStepActivityFailure,
) {
	stepExecutionID := metadata.GetLocalActivityMetadata().GetCurrentStepExecutionId()
	if stepExecutionID == "" {
		return
	}
	failure.Attempt = metadata.GetAttempt()
	b.pendingLocalFailures[localFailureKey(
		method,
		stepExecutionID,
	)] = &pendingLocalActivityFailure{
		eventID:   eventID,
		eventTime: eventTime,
		method:    method,
		failure:   failure,
		metadata:  metadata,
	}
}

func (b *Builder) RecordActivityCompleted(
	eventID int64,
	eventTime time.Time,
	scheduledEventID int64,
	waitOutput *dexpb.InvokeWaitForMethodActivityOutput,
	executeOutput *dexpb.InvokeExecuteMethodActivityOutput,
) error {
	scheduled := b.scheduledActivities[scheduledEventID]
	if _, wasCanceled := b.canceledActivities[scheduledEventID]; wasCanceled {
		delete(b.scheduledActivities, scheduledEventID)
		return nil
	}
	if scheduled == nil {
		return fmt.Errorf("scheduled activity %d is missing", scheduledEventID)
	}
	startedTime, finalAttempt := scheduled.executionTiming()
	switch {
	case scheduled.waitInput != nil && waitOutput != nil:
		b.recordServingWorker(waitOutput.GetServedByAppWorkerId())
		request := scheduled.waitInput.GetRequest()
		response := waitOutput.GetResponse()
		b.events = append(b.events, newEvent(
			eventID,
			eventTime,
			&dexpb.FlowHistoryEvent_StepWaitForCompleted{
				StepWaitForCompleted: &dexpb.StepWaitForCompletedEvent{
					Input:  waitEventInput(request),
					Output: waitCompletedOutput(response),
					Context: stepMethodEventContext(
						request.GetContext(),
						request.GetStepType(),
						scheduled.durability,
						startedTime,
						eventTime,
						finalAttempt,
						scheduled.methodOptions,
						scheduled.lastFailure,
						pulldispatch.Routing(scheduled.waitInput.GetWorkerTarget(), scheduled.waitInput.GetDexTaskQueue(), waitOutput.GetServedByAppWorkerId()),
					),
				},
			},
		))
	case scheduled.executeInput != nil && executeOutput != nil:
		b.recordServingWorker(executeOutput.GetServedByAppWorkerId())
		request := scheduled.executeInput.GetRequest()
		response := executeOutput.GetResponse()
		b.events = append(b.events, newEvent(
			eventID,
			eventTime,
			&dexpb.FlowHistoryEvent_StepExecuteCompleted{
				StepExecuteCompleted: &dexpb.StepExecuteCompletedEvent{
					Input:  executeEventInput(request),
					Output: executeCompletedOutput(response),
					Context: stepMethodEventContext(
						request.GetContext(),
						request.GetStepType(),
						scheduled.durability,
						startedTime,
						eventTime,
						finalAttempt,
						scheduled.methodOptions,
						scheduled.lastFailure,
						pulldispatch.Routing(scheduled.executeInput.GetWorkerTarget(), scheduled.executeInput.GetDexTaskQueue(), executeOutput.GetServedByAppWorkerId()),
					),
				},
			},
		))
	default:
		return fmt.Errorf("activity %d output does not match its input", scheduledEventID)
	}
	delete(b.scheduledActivities, scheduledEventID)
	return nil
}

func (b *Builder) RecordActivityFailed(
	eventID int64,
	eventTime time.Time,
	scheduledEventID int64,
	failure *dexpb.StepMethodFailure,
) error {
	scheduled := b.scheduledActivities[scheduledEventID]
	if _, wasCanceled := b.canceledActivities[scheduledEventID]; wasCanceled {
		delete(b.scheduledActivities, scheduledEventID)
		return nil
	}
	if scheduled == nil {
		return fmt.Errorf("scheduled activity %d is missing", scheduledEventID)
	}
	startedTime, finalAttempt := scheduled.executionTiming()
	failure.Attempt = finalAttempt
	switch {
	case scheduled.waitInput != nil:
		request := scheduled.waitInput.GetRequest()
		b.events = append(b.events, newEvent(
			eventID,
			eventTime,
			&dexpb.FlowHistoryEvent_StepWaitForFailed{
				StepWaitForFailed: &dexpb.StepWaitForFailedEvent{
					Input:  waitEventInput(request),
					Output: &dexpb.StepMethodFailedOutput{Failure: failure},
					Context: stepMethodEventContext(
						request.GetContext(),
						request.GetStepType(),
						scheduled.durability,
						startedTime,
						eventTime,
						finalAttempt,
						scheduled.methodOptions,
						scheduled.lastFailure,
						failurePullRouting(failure, scheduled.waitInput.GetWorkerTarget(), scheduled.waitInput.GetDexTaskQueue()),
					),
				},
			},
		))
	case scheduled.executeInput != nil:
		request := scheduled.executeInput.GetRequest()
		b.events = append(b.events, newEvent(
			eventID,
			eventTime,
			&dexpb.FlowHistoryEvent_StepExecuteFailed{
				StepExecuteFailed: &dexpb.StepExecuteFailedEvent{
					Input:  executeEventInput(request),
					Output: &dexpb.StepMethodFailedOutput{Failure: failure},
					Context: stepMethodEventContext(
						request.GetContext(),
						request.GetStepType(),
						scheduled.durability,
						startedTime,
						eventTime,
						finalAttempt,
						scheduled.methodOptions,
						scheduled.lastFailure,
						failurePullRouting(failure, scheduled.executeInput.GetWorkerTarget(), scheduled.executeInput.GetDexTaskQueue()),
					),
				},
			},
		))
	default:
		return fmt.Errorf("activity %d is not a step method", scheduledEventID)
	}
	delete(b.scheduledActivities, scheduledEventID)
	return nil
}

func (b *Builder) RecordActivityCanceled(scheduledEventID int64) {
	b.canceledActivities[scheduledEventID] = struct{}{}
}

func (b *Builder) RecordLocalWaitCompleted(
	eventID int64,
	eventTime time.Time,
	output *dexpb.InvokeWaitForMethodActivityOutput,
	finalAttempt int32,
) {
	b.recordServingWorker(output.GetServedByAppWorkerId())
	response := output.GetResponse()
	b.events = append(b.events, newEvent(
		eventID,
		eventTime,
		&dexpb.FlowHistoryEvent_StepWaitForCompleted{
			StepWaitForCompleted: &dexpb.StepWaitForCompletedEvent{
				Output: waitCompletedOutput(response),
				Context: localStepMethodEventContext(
					response.GetLocalActivityMetadata(),
					finalAttempt,
					output.GetPullRouting(),
				),
			},
		},
	))
}

func (b *Builder) RecordLocalExecuteCompleted(
	eventID int64,
	eventTime time.Time,
	output *dexpb.InvokeExecuteMethodActivityOutput,
	finalAttempt int32,
) {
	b.recordServingWorker(output.GetServedByAppWorkerId())
	response := output.GetResponse()
	localMetadata := response.GetLocalActivityMetadata()
	b.events = append(b.events, newEvent(
		eventID,
		eventTime,
		&dexpb.FlowHistoryEvent_StepExecuteCompleted{
			StepExecuteCompleted: &dexpb.StepExecuteCompletedEvent{
				Output: executeCompletedOutput(response),
				Context: localStepMethodEventContext(
					localMetadata,
					finalAttempt,
					output.GetPullRouting(),
				),
			},
		},
	))
}

func (b *Builder) RecordContinueDump(output *dexpb.DumpFlowForContinueAsNewActivityOutput) {
	response := output.GetResponse()
	b.continueDumpPages[response.GetPageNum()] = response.GetPageContent()
	b.continueDumpTotal = response.GetTotalPages()
}

func (b *Builder) RecordSignal(
	eventID int64,
	eventTime time.Time,
	request *dexpb.ExecuteRpcSignalRequest,
) {
	if request.GetIsDeleteChannelMessageApi() {
		b.RecordExternalDelete(eventID, eventTime, request.GetDeleteFromChannel())
		return
	}
	if isExternalPublish(request) {
		b.events = append(b.events, newEvent(
			eventID,
			eventTime,
			&dexpb.FlowHistoryEvent_ChannelExternalPublish{
				ChannelExternalPublish: &dexpb.ChannelExternalPublishEvent{
					Messages: request.GetPublishToChannel(),
				},
			},
		))
		return
	}
	rpcName := rpcNameFromDecision(request.GetStepDecision())
	if !request.GetIsSetAttributeApi() && rpcName == "" && len(request.GetDeleteFromChannel()) == 0 {
		return
	}
	b.events = append(b.events, newEvent(
		eventID,
		eventTime,
		&dexpb.FlowHistoryEvent_RpcExecutionCompleted{
			RpcExecutionCompleted: &dexpb.RpcExecutionCompletedEvent{
				RpcName:             rpcName,
				ServedByAppWorkerId: request.GetServedByAppWorkerId(),
				StepDecision:        request.GetStepDecision(),
				UpsertAttributes:    request.GetUpsertAttributes(),
				RecordEvents:        request.GetRecordEvents(),
				PublishToChannel:    request.GetPublishToChannel(),
				IsSetAttributeApi:   request.GetIsSetAttributeApi(),
				DeleteFromChannel:   request.GetDeleteFromChannel(),
			},
		},
	))
}

func (b *Builder) RecordInvokeRpcUpdate(
	eventID int64,
	eventTime time.Time,
	request *dexpb.InvokeRPCRequest,
	result *dexpb.InvokeRpcUpdateResult,
	workerOutput *dexpb.InvokeWorkerRPCActivityOutput,
) {
	b.recordServingWorker(workerOutput.GetServedByAppWorkerId())
	workerResponse := workerOutput.GetResponse()
	b.events = append(b.events, newEvent(
		eventID,
		eventTime,
		&dexpb.FlowHistoryEvent_RpcExecutionCompleted{
			RpcExecutionCompleted: &dexpb.RpcExecutionCompletedEvent{
				RpcName:             request.GetRpcName(),
				ServedByAppWorkerId: workerOutput.GetServedByAppWorkerId(),
				Input:               request.GetInput(),
				Output:              result.GetResponse().GetOutput(),
				StepDecision:        workerResponse.GetStepDecision(),
				UpsertAttributes:    workerResponse.GetUpsertAttributes(),
				RecordEvents:        workerResponse.GetRecordEvents(),
				PublishToChannel:    workerResponse.GetPublishToChannel(),
				DeleteFromChannel:   workerResponse.GetDeleteFromChannel(),
			},
		},
	))
}

func (b *Builder) RecordExternalDelete(
	eventID int64,
	eventTime time.Time,
	deletions []*dexpb.ChannelMessageDeletion,
) {
	b.events = append(b.events, newEvent(
		eventID,
		eventTime,
		&dexpb.FlowHistoryEvent_ChannelExternalDelete{
			ChannelExternalDelete: &dexpb.ChannelExternalDeleteEvent{Messages: deletions},
		},
	))
}

func (b *Builder) RecordClose(
	eventID int64,
	eventTime time.Time,
	payload *dexpb.FlowClosedHistoryEvent,
) {
	payload.StickyAppWorkerId = b.stickyAppWorkerID
	b.events = append(
		b.events,
		b.pendingStepMethodEvents(eventTime, shouldIncludeCanceledActivities(payload.GetFlowStatus()))...,
	)
	b.events = append(b.events, newEvent(
		eventID,
		eventTime,
		&dexpb.FlowHistoryEvent_FlowClosed{FlowClosed: payload},
	))
}

func shouldIncludeCanceledActivities(flowStatus dexpb.FlowStatus) bool {
	switch flowStatus {
	case dexpb.FlowStatus_FLOW_STATUS_CANCELED,
		dexpb.FlowStatus_FLOW_STATUS_TERMINATED,
		dexpb.FlowStatus_FLOW_STATUS_SERVER_SIDE_TIMEOUT_INTERNAL_ONLY:
		return true
	default:
		return false
	}
}

func (b *Builder) pendingStepMethodEvents(
	closedTime time.Time,
	includeCanceledActivities bool,
) []*dexpb.FlowHistoryEvent {
	scheduledEventIDs := make([]int64, 0, len(b.scheduledActivities))
	for scheduledEventID := range b.scheduledActivities {
		_, wasCanceled := b.canceledActivities[scheduledEventID]
		if wasCanceled && !includeCanceledActivities {
			continue
		}
		scheduledEventIDs = append(scheduledEventIDs, scheduledEventID)
	}
	sort.Slice(scheduledEventIDs, func(left int, right int) bool {
		return scheduledEventIDs[left] < scheduledEventIDs[right]
	})
	pendingEvents := make([]*dexpb.FlowHistoryEvent, 0, len(scheduledEventIDs))
	for _, scheduledEventID := range scheduledEventIDs {
		pendingEvents = append(
			pendingEvents,
			b.scheduledActivities[scheduledEventID].pendingStepMethodEvent(scheduledEventID, closedTime),
		)
	}
	return pendingEvents
}

func (b *Builder) EventsInRange(
	startInternalEventID int64,
	nextInternalEventID int64,
) ([]*dexpb.FlowHistoryEvent, error) {
	b.flushLocalFailures()
	if b.startEvent != nil {
		if err := b.populateContinuedStart(); err != nil {
			return nil, err
		}
		b.events = append(b.events, b.startEvent)
	}
	sort.Slice(b.events, func(left int, right int) bool {
		return b.events[left].GetEventId() < b.events[right].GetEventId()
	})
	for startIndex, event := range b.events {
		if event.GetEventId() < startInternalEventID {
			continue
		}
		for endIndex := startIndex; endIndex < len(b.events); endIndex++ {
			if b.events[endIndex].GetEventId() >= nextInternalEventID {
				return b.events[startIndex:endIndex], nil
			}
		}
		return b.events[startIndex:], nil
	}
	return nil, nil
}

func (b *Builder) flushLocalFailures() {
	for key, pending := range b.pendingLocalFailures {
		metadata := pending.metadata
		localMetadata := metadata.GetLocalActivityMetadata()
		startedTime := time.Unix(metadata.GetFirstAttemptTimestamp(), 0)
		context := stepMethodEventContext(
			&dexpb.Context{
				StepExecutionId:     localMetadata.GetCurrentStepExecutionId(),
				FromStepExecutionId: localMetadata.GetFromStepExecutionId(),
			},
			stepTypeFromExecutionID(localMetadata.GetCurrentStepExecutionId()),
			dexpb.StepDurability_STEP_DURABILITY_ASYNC,
			startedTime,
			pending.eventTime,
			metadata.GetAttempt(),
			metadata.GetMethodOptions(),
			nil,
			metadata.GetPullRouting(),
		)
		switch pending.method {
		case blobstore.StepEventInputMethodWaitFor:
			b.events = append(b.events, newEvent(
				pending.eventID,
				pending.eventTime,
				&dexpb.FlowHistoryEvent_StepWaitForFailed{
					StepWaitForFailed: &dexpb.StepWaitForFailedEvent{
						Output:  &dexpb.StepMethodFailedOutput{Failure: pending.failure},
						Context: context,
					},
				},
			))
		case blobstore.StepEventInputMethodExecute:
			b.events = append(b.events, newEvent(
				pending.eventID,
				pending.eventTime,
				&dexpb.FlowHistoryEvent_StepExecuteFailed{
					StepExecuteFailed: &dexpb.StepExecuteFailedEvent{
						Output:  &dexpb.StepMethodFailedOutput{Failure: pending.failure},
						Context: context,
					},
				},
			))
		}
		delete(b.pendingLocalFailures, key)
	}
}

func (b *Builder) takeLocalFailure(
	method string,
	stepExecutionID string,
) *dexpb.InternalLocalStepActivityFailure {
	key := localFailureKey(method, stepExecutionID)
	pending := b.pendingLocalFailures[key]
	delete(b.pendingLocalFailures, key)
	if pending == nil {
		return nil
	}
	return pending.metadata
}

func localFailureKey(method string, stepExecutionID string) string {
	return method + "\x00" + stepExecutionID
}

func (b *Builder) populateContinuedStart() error {
	payload := b.startEvent.GetFlowStartedOrContinued()
	continued := payload.GetContinuedStart()
	if continued == nil || b.continueDumpTotal == 0 {
		return nil
	}
	var marshaled []byte
	for pageNumber := int32(0); pageNumber < b.continueDumpTotal; pageNumber++ {
		page, ok := b.continueDumpPages[pageNumber]
		if !ok {
			return fmt.Errorf("continue-as-new dump page %d is missing", pageNumber)
		}
		marshaled = append(marshaled, page...)
	}
	var dump dexpb.ContinueAsNewDump
	if err := proto.Unmarshal(marshaled, &dump); err != nil {
		return fmt.Errorf("decode continue-as-new dump: %w", err)
	}
	continued.StepsToStart = dump.GetStepsToStartFromBeginning()
	continued.StepsToResume = dump.GetStepExecutionsToResume()
	continued.PendingChannelMessages = dump.GetChannelReceived()
	continued.Attributes = dump.GetAttributes()
	continued.CompletedSteps = dump.GetStepOutputs()
	continued.StickyAppWorkerId = dump.GetDexTaskQueue()
	for _, event := range b.events {
		if closed := event.GetFlowClosed(); closed != nil && closed.GetStickyAppWorkerId() == "" {
			closed.StickyAppWorkerId = dump.GetDexTaskQueue()
		}
	}
	return nil
}

func (s *scheduledActivity) pendingStepMethodEvent(
	scheduledEventID int64,
	closedTime time.Time,
) *dexpb.FlowHistoryEvent {
	startedTime, finalAttempt := s.executionTiming()
	phase := dexpb.PendingStepMethodPhase_PENDING_STEP_METHOD_PHASE_STARTED
	eventTime := s.firstStartedTime
	if s.firstStartedTime.IsZero() {
		phase = dexpb.PendingStepMethodPhase_PENDING_STEP_METHOD_PHASE_SCHEDULED
		eventTime = s.scheduledTime
	}
	payload := &dexpb.StepMethodPendingEvent{
		Phase: phase,
	}
	switch {
	case s.waitInput != nil:
		request := s.waitInput.GetRequest()
		payload.Input = waitEventInput(request)
		payload.Context = stepMethodEventContext(
			request.GetContext(),
			request.GetStepType(),
			s.durability,
			startedTime,
			closedTime,
			finalAttempt,
			s.methodOptions,
			s.lastFailure,
			pulldispatch.Routing(s.waitInput.GetWorkerTarget(), s.waitInput.GetDexTaskQueue(), ""),
		)
		return newEvent(scheduledEventID, eventTime, &dexpb.FlowHistoryEvent_StepWaitForPending{
			StepWaitForPending: payload,
		})
	case s.executeInput != nil:
		request := s.executeInput.GetRequest()
		payload.Input = executeEventInput(request)
		payload.Context = stepMethodEventContext(
			request.GetContext(),
			request.GetStepType(),
			s.durability,
			startedTime,
			closedTime,
			finalAttempt,
			s.methodOptions,
			s.lastFailure,
			pulldispatch.Routing(s.executeInput.GetWorkerTarget(), s.executeInput.GetDexTaskQueue(), ""),
		)
		return newEvent(scheduledEventID, eventTime, &dexpb.FlowHistoryEvent_StepExecutePending{
			StepExecutePending: payload,
		})
	default:
		panic("scheduled step activity has no method input")
	}
}

func (s *scheduledActivity) executionTiming() (time.Time, int32) {
	startedTime := s.firstStartedTime
	if !s.firstAttemptTime.IsZero() {
		startedTime = s.firstAttemptTime
	}
	if startedTime.IsZero() {
		startedTime = s.scheduledTime
	}
	finalAttempt := s.finalAttempt
	if finalAttempt <= 0 {
		switch {
		case s.waitInput != nil:
			finalAttempt = s.waitInput.GetRequest().GetContext().GetAttempt()
		case s.executeInput != nil:
			finalAttempt = s.executeInput.GetRequest().GetContext().GetAttempt()
		}
	}
	if finalAttempt <= 0 {
		finalAttempt = s.previousAttempts() + 1
	}
	return startedTime, finalAttempt
}

func (s *scheduledActivity) previousAttempts() int32 {
	return s.priorAttemptCount
}

func newEvent(
	eventID int64,
	eventTime time.Time,
	payload any,
) *dexpb.FlowHistoryEvent {
	event := &dexpb.FlowHistoryEvent{
		EventId:   eventID,
		EventTime: timestamppb.New(eventTime),
	}
	switch payload := payload.(type) {
	case *dexpb.FlowHistoryEvent_FlowStartedOrContinued:
		event.Payload = payload
	case *dexpb.FlowHistoryEvent_FlowClosed:
		event.Payload = payload
	case *dexpb.FlowHistoryEvent_StepWaitForCompleted:
		event.Payload = payload
	case *dexpb.FlowHistoryEvent_StepWaitForFailed:
		event.Payload = payload
	case *dexpb.FlowHistoryEvent_StepExecuteCompleted:
		event.Payload = payload
	case *dexpb.FlowHistoryEvent_StepExecuteFailed:
		event.Payload = payload
	case *dexpb.FlowHistoryEvent_StepWaitForPending:
		event.Payload = payload
	case *dexpb.FlowHistoryEvent_StepExecutePending:
		event.Payload = payload
	case *dexpb.FlowHistoryEvent_RpcExecutionCompleted:
		event.Payload = payload
	case *dexpb.FlowHistoryEvent_ChannelExternalPublish:
		event.Payload = payload
	case *dexpb.FlowHistoryEvent_ChannelExternalDelete:
		event.Payload = payload
	case *dexpb.FlowHistoryEvent_TimeTravelFork:
		event.Payload = payload
	default:
		panic("unsupported flow history payload")
	}
	return event
}

func waitEventInput(request *dexpb.InvokeWaitForMethodRequest) *dexpb.StepMethodEventInput {
	return &dexpb.StepMethodEventInput{
		StepInput:  request.GetStepInput(),
		Attributes: request.GetAttributes(),
	}
}

func executeEventInput(request *dexpb.InvokeExecuteMethodRequest) *dexpb.StepMethodEventInput {
	return &dexpb.StepMethodEventInput{
		StepInput:           request.GetStepInput(),
		ConditionResults:    request.GetConditionResults(),
		Attributes:          request.GetAttributes(),
		StepExecutionLocals: request.GetStepExeLocals(),
	}
}

func waitCompletedOutput(response *dexpb.InvokeWaitForMethodResponse) *dexpb.StepWaitForCompletedOutput {
	return &dexpb.StepWaitForCompletedOutput{
		WaitForCondition:          response.GetWaitingCondition(),
		UpsertAttributes:          response.GetUpsertAttributes(),
		PublishToChannel:          response.GetPublishToChannel(),
		RecordEvents:              response.GetRecordEvents(),
		UpsertStepExecutionLocals: response.GetUpsertStepExeLocals(),
		DeleteFromChannel:         response.GetDeleteFromChannel(),
	}
}

func executeCompletedOutput(response *dexpb.InvokeExecuteMethodResponse) *dexpb.StepExecuteCompletedOutput {
	return &dexpb.StepExecuteCompletedOutput{
		StepDecision:              response.GetStepDecision(),
		UpsertAttributes:          response.GetUpsertAttributes(),
		PublishToChannel:          response.GetPublishToChannel(),
		RecordEvents:              response.GetRecordEvents(),
		UpsertStepExecutionLocals: response.GetUpsertStepExeLocals(),
		DeleteFromChannel:         response.GetDeleteFromChannel(),
	}
}

func stepMethodEventContext(
	context *dexpb.Context,
	stepType string,
	durability dexpb.StepDurability,
	startedTime time.Time,
	completedTime time.Time,
	finalAttempt int32,
	methodOptions *dexpb.StepMethodOptions,
	lastFailure *dexpb.StepMethodFailure,
	pullRouting *dexpb.PullRouting,
) *dexpb.StepMethodEventContext {
	return &dexpb.StepMethodEventContext{
		StepExecutionId:     context.GetStepExecutionId(),
		FromStepExecutionId: context.GetFromStepExecutionId(),
		StepType:            stepType,
		Durability:          durability,
		FinalAttempt:        finalAttempt,
		StartedTime:         timestamppb.New(startedTime),
		Duration:            durationpb.New(completedTime.Sub(startedTime)),
		MethodOptions:       methodOptions,
		LastFailureInfo:     lastFailure,
		PullRouting:         pullRouting,
	}
}

func localStepMethodEventContext(
	input *dexpb.LocalActivityMetadata,
	finalAttempt int32,
	routing *dexpb.PullRouting,
) *dexpb.StepMethodEventContext {
	if finalAttempt <= 0 {
		finalAttempt = 1
	}
	return &dexpb.StepMethodEventContext{
		StepExecutionId:     input.GetCurrentStepExecutionId(),
		FromStepExecutionId: input.GetFromStepExecutionId(),
		StepType:            stepTypeFromExecutionID(input.GetCurrentStepExecutionId()),
		Durability:          dexpb.StepDurability_STEP_DURABILITY_ASYNC,
		PullRouting:         routing,
		FinalAttempt:        finalAttempt,
	}
}

func stepTypeFromExecutionID(stepExecutionID string) string {
	separator := strings.LastIndex(stepExecutionID, "-")
	if separator < 1 {
		return stepExecutionID
	}
	if _, err := strconv.ParseInt(stepExecutionID[separator+1:], 10, 32); err != nil {
		return stepExecutionID
	}
	return stepExecutionID[:separator]
}

func isExternalPublish(request *dexpb.ExecuteRpcSignalRequest) bool {
	return len(request.GetPublishToChannel()) > 0 &&
		len(request.GetDeleteFromChannel()) == 0 &&
		request.GetStepDecision() == nil &&
		len(request.GetUpsertAttributes()) == 0 &&
		len(request.GetRecordEvents()) == 0
}

func rpcNameFromDecision(decision *dexpb.StepDecision) string {
	for _, movement := range decision.GetNextSteps() {
		source := movement.GetFromStepExecutionIdInternalOnly()
		if strings.HasPrefix(source, rpcSourcePrefix) {
			return strings.TrimPrefix(source, rpcSourcePrefix)
		}
	}
	return ""
}

func failurePullRouting(failure *dexpb.StepMethodFailure, target *dexpb.WorkerTarget, stickyAppWorkerID string) *dexpb.PullRouting {
	if failure.GetPullRouting() != nil {
		return failure.GetPullRouting()
	}
	return pulldispatch.Routing(target, stickyAppWorkerID, "")
}

func (b *Builder) recordServingWorker(workerID string) {
	if workerID != "" {
		b.stickyAppWorkerID = workerID
	}
}
