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
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"github.com/superdurable/dex/config"
	"github.com/superdurable/dex/gen/dexpb"
	"github.com/superdurable/dex/service"
	"github.com/superdurable/dex/service/interpreter/interfaces"
	"google.golang.org/protobuf/proto"
)

type ContinueAsNewer struct {
	provider     interfaces.WorkflowProvider
	apiCfg       *config.ApiConfig
	dexTaskQueue string

	StepExecutionToResumeMap map[string]*dexpb.StepExecutionResumeInfo // stepExeId to StepExecutionResumeInfo
	inflightUpdateOperations int

	stepRequestQueue      *StepRequestQueue
	channelStore          *ChannelStore
	stepExecutionCounter  *StepExecutionCounter
	activeStepMovements   map[string]*dexpb.StepMovement
	persistenceManager    *PersistenceManager
	outputCollector       *OutputCollector
	timerProcessor        interfaces.TimerProcessor
	attributeSynchronizer *AttributeSynchronizer
}

func NewContinueAsNewer(
	apiCfg *config.ApiConfig,
	provider interfaces.WorkflowProvider,
	channelStore *ChannelStore, stepExecutionCounter *StepExecutionCounter,
	persistenceManager *PersistenceManager, stepRequestQueue *StepRequestQueue, collector *OutputCollector,
	timerProcessor interfaces.TimerProcessor,
	attributeSynchronizer *AttributeSynchronizer,
	dexTaskQueue string,
) *ContinueAsNewer {
	if apiCfg == nil || provider == nil || stepRequestQueue == nil || channelStore == nil ||
		stepExecutionCounter == nil || persistenceManager == nil || collector == nil ||
		timerProcessor == nil || attributeSynchronizer == nil {
		panic("ContinueAsNewer requires non-nil dependencies")
	}
	return &ContinueAsNewer{
		provider:     provider,
		apiCfg:       apiCfg,
		dexTaskQueue: dexTaskQueue,

		StepExecutionToResumeMap: map[string]*dexpb.StepExecutionResumeInfo{},

		stepRequestQueue:      stepRequestQueue,
		channelStore:          channelStore,
		stepExecutionCounter:  stepExecutionCounter,
		activeStepMovements:   map[string]*dexpb.StepMovement{},
		persistenceManager:    persistenceManager,
		outputCollector:       collector,
		timerProcessor:        timerProcessor,
		attributeSynchronizer: attributeSynchronizer,
	}
}

func (i *Interpreter) LoadInternalsFromPreviousRun(
	ctx interfaces.UnifiedContext,
	provider interfaces.WorkflowProvider,
	previousRunId string,
	continueAsNewPageSizeInBytes int32,
) (*dexpb.ContinueAsNewDump, error) {
	activityOptions := interfaces.ActivityOptions{
		StartToCloseTimeout: 5 * time.Second,
		RetryPolicy:         &config.RetryPolicy{MaximumInterval: 5 * time.Second},
	}
	activityCfg := i.sharedConfig.Interpreter.InterpreterActivityConfig
	if activityConfig := activityCfg.DumpWorkflowInternalActivityConfig; activityConfig != nil {
		activityOptions.StartToCloseTimeout = activityConfig.StartToCloseTimeout
		if activityConfig.RetryPolicy != nil {
			activityOptions.RetryPolicy = activityConfig.RetryPolicy
		}
	}
	ctx = provider.WithActivityOptions(ctx, activityOptions)
	workflowId := provider.GetWorkflowInfo(ctx).WorkflowExecution.ID
	pageSize := continueAsNewPageSizeInBytes
	if pageSize == 0 {
		pageSize = service.DefaultContinueAsNewPageSizeInBytes
	}
	var wholeData []byte
	lastChecksum := ""
	pageNum := int32(0)
	for {
		var activityOutput dexpb.DumpFlowForContinueAsNewActivityOutput
		err := provider.ExecuteLocalActivity(
			&activityOutput,
			ctx,
			i.activities.DumpFlowForContinueAsNew,
			&dexpb.DumpFlowForContinueAsNewActivityInput{
				Request: &dexpb.ContinueAsNewDumpRequest{
					FlowId:          workflowId,
					RunId:           previousRunId,
					PageNum:         pageNum,
					PageSizeInBytes: pageSize,
				},
			},
		)
		if err != nil {
			return nil, err
		}
		if lastChecksum != "" && lastChecksum != activityOutput.Response.GetChecksum() {
			// reset to start from beginning
			pageNum = 0
			wholeData = nil
			provider.GetLogger(ctx).Error(
				"checksum has changed during the loading",
				lastChecksum,
				activityOutput.Response.GetChecksum(),
			)
			lastChecksum = ""
			continue
		}
		lastChecksum = activityOutput.Response.GetChecksum()
		wholeData = append(wholeData, activityOutput.Response.GetPageContent()...)
		pageNum++
		if pageNum >= activityOutput.Response.GetTotalPages() {
			break
		}
	}

	var resp dexpb.ContinueAsNewDump
	if err := proto.Unmarshal(wholeData, &resp); err != nil {
		return nil, provider.NewFlowError(
			dexpb.FlowErrorType_FLOW_ERROR_TYPE_INTERNAL,
			fmt.Sprintf("unmarshal continue-as-new dump: %v", err),
		)
	}
	return &resp, nil
}

func (c *ContinueAsNewer) GetSnapshot() *dexpb.ContinueAsNewDump {
	localStepExecutionToResumeMap := map[string]*dexpb.StepExecutionResumeInfo{}
	for _, key := range DeterministicKeys(c.StepExecutionToResumeMap) {
		localStepExecutionToResumeMap[key] = c.StepExecutionToResumeMap[key]
	}
	// NOTE: there could be more resume from even more previous run that hasn't started yet
	for key, value := range c.stepRequestQueue.GetAllStepResumeRequests() {
		localStepExecutionToResumeMap[key] = value
	}
	var stepExecutionsToResume []*dexpb.StepExecutionResumeInfo
	for _, key := range DeterministicKeys(localStepExecutionToResumeMap) {
		stepExecutionsToResume = append(
			stepExecutionsToResume,
			localStepExecutionToResumeMap[key],
		)
	}
	return &dexpb.ContinueAsNewDump{
		DexTaskQueue:              c.dexTaskQueue,
		ChannelReceived:           c.channelStore.GetAllReceived(),
		CounterInfo:               c.stepExecutionCounter.Dump(),
		Attributes:                c.persistenceManager.GetAllAttributes(),
		StepsToStartFromBeginning: c.stepRequestQueue.GetAllStepStartRequests(),
		StepExecutionsToResume:    stepExecutionsToResume,
		StepOutputs:               c.outputCollector.GetAll(),
		StaleSkipTimers: c.timerProcessor.Dump(
			c.stepExecutionCounter.IsStepExecutionActive,
		),
		PendingAttributeSyncItems: c.attributeSynchronizer.PendingItems(),
	}
}

func (c *ContinueAsNewer) DexTaskQueue() string {
	return c.dexTaskQueue
}

func (c *ContinueAsNewer) RecordServedByAppWorkerID(appWorkerID string) {
	if appWorkerID != "" {
		c.dexTaskQueue = appWorkerID
	}
}

func (c *ContinueAsNewer) GetActiveStepExecutionStates() []*dexpb.ActiveStepExecutionState {
	queuedResumeRequests := c.stepRequestQueue.GetAllStepResumeRequests()
	timerInfos := c.timerProcessor.GetTimerInfos()
	var states []*dexpb.ActiveStepExecutionState
	for _, stepType := range DeterministicKeys(c.stepExecutionCounter.stepActiveExecutionNums) {
		for _, executionNumber := range c.stepExecutionCounter.stepActiveExecutionNums[stepType] {
			stepExecutionID := formatStepExecutionId(stepType, executionNumber)
			if _, queued := queuedResumeRequests[stepExecutionID]; queued {
				continue
			}
			states = append(
				states,
				c.activeStepExecutionState(stepExecutionID, stepType, timerInfos[stepExecutionID]),
			)
		}
	}
	timeoutStepExecutionID := service.FlowTimeoutStepExecutionID
	_, isTimeoutActive := c.activeStepMovements[timeoutStepExecutionID]
	_, isTimeoutQueued := queuedResumeRequests[timeoutStepExecutionID]
	if isTimeoutActive && !isTimeoutQueued {
		states = append(states, c.activeStepExecutionState(
			timeoutStepExecutionID,
			service.FlowTimeoutStepType,
			timerInfos[timeoutStepExecutionID],
		))
	}
	return states
}

func (c *ContinueAsNewer) activeStepExecutionState(
	stepExecutionID string,
	stepType string,
	timers []*dexpb.TimerInfo,
) *dexpb.ActiveStepExecutionState {
	state := &dexpb.ActiveStepExecutionState{
		StepExecutionId: stepExecutionID,
		StepType:        stepType,
		Phase:           dexpb.ActiveStepPhase_ACTIVE_STEP_PHASE_ACTIVE,
		Movement:        c.activeStepMovements[stepExecutionID],
		Timers:          timers,
	}
	resumeInfo := c.StepExecutionToResumeMap[stepExecutionID]
	if resumeInfo == nil {
		if state.GetMovement() != nil {
			state.FromStepExecutionId = state.GetMovement().GetFromStepExecutionIdInternalOnly()
		}
		return state
	}
	state.Phase = dexpb.ActiveStepPhase_ACTIVE_STEP_PHASE_WAITING
	state.Movement = resumeInfo.GetStep()
	state.FromStepExecutionId = resumeInfo.GetStep().GetFromStepExecutionIdInternalOnly()
	state.WaitingCondition = resumeInfo.GetWaitingCondition()
	state.CompletedConditions = resumeInfo.GetCompletedConditions()
	state.StepExecutionLocals = resumeInfo.GetStepExeLocals()
	return state
}

func (c *ContinueAsNewer) SetQueryHandlersForContinueAsNew(
	ctx interfaces.UnifiedContext,
) error {
	return c.provider.SetQueryHandler(
		ctx,
		service.ContinueAsNewDumpByPageQueryType,
		// return the current page of the whole snapshot
		func(request *dexpb.ContinueAsNewDumpRequest) (*dexpb.ContinueAsNewDumpResponse, error) {
			if request == nil {
				return nil, fmt.Errorf("continue-as-new dump request is nil")
			}
			pageSize := int32(service.DefaultContinueAsNewPageSizeInBytes)
			if request.GetPageSizeInBytes() > 0 {
				pageSize = request.GetPageSizeInBytes()
			}
			maxPageSize := c.apiCfg.EffectiveGrpcMaxMessageBytes()
			if int(pageSize) > maxPageSize {
				return nil, fmt.Errorf("page size must be at most %d bytes", maxPageSize)
			}
			if request.GetPageNum() < 0 {
				return nil, fmt.Errorf("page number must be non-negative")
			}

			wholeSnapshot := c.GetSnapshot()
			wholeData, err := proto.MarshalOptions{Deterministic: true}.Marshal(wholeSnapshot)
			if err != nil {
				return nil, fmt.Errorf("marshal continue-as-new dump: %w", err)
			}
			checksum := md5.Sum(wholeData)
			totalPages := int32((len(wholeData) + int(pageSize) - 1) / int(pageSize))
			if totalPages == 0 {
				totalPages = 1
			}
			if request.GetPageNum() >= totalPages {
				return nil, fmt.Errorf("page number %d is out of range", request.GetPageNum())
			}
			start := int(request.GetPageNum() * pageSize)
			end := start + int(pageSize)
			if end > len(wholeData) {
				end = len(wholeData)
			}
			return &dexpb.ContinueAsNewDumpResponse{
				PageContent: wholeData[start:end],
				PageNum:     request.GetPageNum(),
				TotalPages:  totalPages,
				Checksum:    hex.EncodeToString(checksum[:]),
			}, nil
		},
	)
}

func (c *ContinueAsNewer) AddPotentialStepExecutionToResume(
	resumeInfo *dexpb.StepExecutionResumeInfo,
) {
	if resumeInfo == nil || resumeInfo.GetStepExecutionId() == "" {
		panic("step resume info requires an execution ID")
	}
	c.StepExecutionToResumeMap[resumeInfo.GetStepExecutionId()] = resumeInfo
}

func (c *ContinueAsNewer) TrackActiveStep(
	stepExecutionID string,
	step *dexpb.StepMovement,
) {
	if stepExecutionID == "" || step == nil {
		panic("active step requires an execution ID and movement")
	}
	c.activeStepMovements[stepExecutionID] = step
}

func (c *ContinueAsNewer) RemoveActiveStep(stepExecutionID string) {
	delete(c.activeStepMovements, stepExecutionID)
}

func (c *ContinueAsNewer) HasAnyStepExecutionToResume() bool {
	return len(c.StepExecutionToResumeMap) > 0
}

func (c *ContinueAsNewer) RemoveStepExecutionToResume(executionId string) {
	delete(c.StepExecutionToResumeMap, executionId)
}

func (c *ContinueAsNewer) DrainThreads(ctx interfaces.UnifiedContext) error {
	// TODO: add metric for before and after Await to monitor stuck
	// NOTE: consider using AwaitWithTimeout to get an alert when workflow stuck due to a bug in the draining logic for continueAsNew

	errWait := c.provider.Await(ctx, func() bool {
		return c.allThreadsDrained(ctx)
	})
	c.provider.GetLogger(ctx).Info("done draining threads for continueAsNew", errWait)

	return errWait
}

func (c *ContinueAsNewer) IncreaseInflightOperation() {
	c.inflightUpdateOperations++
}

func (c *ContinueAsNewer) DecreaseInflightOperation() {
	c.inflightUpdateOperations--
}

// if the DrainThreads await is being called more than a few times and cannot get through,
// there is likely something wrong in the continueAsNew logic (unless worker API is stuck)
// the key is runId, the value is how many times it has been called in this worker
// Using this in memory counter sot hat we don't have to use AwaitWithTimeout which will consume a timer
// TODO add TTL support because we don't have to keep the value in memory forever(likely a few hours or a day is enough)
var inMemoryContinueAsNewMonitor sync.Map // runId -> time.Time

const warnThreshold = time.Second * 5
const errThreshold = time.Second * 15

func (c *ContinueAsNewer) allThreadsDrained(ctx interfaces.UnifiedContext) bool {
	runId := c.provider.GetWorkflowInfo(ctx).WorkflowExecution.RunID

	remainingThreadCount := c.provider.GetThreadCount()
	if remainingThreadCount == 0 && c.inflightUpdateOperations == 0 {
		inMemoryContinueAsNewMonitor.Delete(runId)
		return true
	}

	initTimeValue, ok := inMemoryContinueAsNewMonitor.Load(runId)
	c.provider.GetLogger(ctx).Debug("continueAsNew is in draining remainingThreadCount, attempt, threadNames, inflightUpdateOperations",
		remainingThreadCount, initTimeValue, c.provider.GetPendingThreadNames(), c.inflightUpdateOperations)

	if !ok {
		inMemoryContinueAsNewMonitor.Store(runId, time.Now())
		return false
	}
	initTime := initTimeValue.(time.Time)

	elapsed := time.Since(initTime)

	if elapsed >= errThreshold {
		c.provider.GetLogger(ctx).Warn(
			"continueAsNew is likely stuck (unless worker API is stuck) in draining remainingThreadCount, attempt, threadNames, inflightUpdateOperations",
			remainingThreadCount, initTime, c.provider.GetPendingThreadNames(), c.inflightUpdateOperations)
		return false
	}
	if elapsed >= warnThreshold {
		c.provider.GetLogger(ctx).Warn(
			"continueAsNew may be stuck (unless worker API is stuck) in draining remainingThreadCount, attempt, threadNames, inflightUpdateOperations",
			remainingThreadCount, initTime, c.provider.GetPendingThreadNames(), c.inflightUpdateOperations)
	}
	return false
}
