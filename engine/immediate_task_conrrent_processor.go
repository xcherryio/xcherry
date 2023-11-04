// Copyright 2023 XDBLab organization
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package engine

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/xdblab/xdb-apis/goapi/xdbapi"
	"github.com/xdblab/xdb/common/log"
	"github.com/xdblab/xdb/common/log/tag"
	"github.com/xdblab/xdb/common/ptr"
	"github.com/xdblab/xdb/common/urlautofix"
	"github.com/xdblab/xdb/config"
	"github.com/xdblab/xdb/persistence"
)

type immediateTaskConcurrentProcessor struct {
	rootCtx           context.Context
	cfg               config.Config
	taskToProcessChan chan persistence.ImmediateTask
	// for quickly checking if the shardId is being processed
	currentShards map[int32]bool
	// shardId to the channel
	taskToCommitChans map[int32]chan<- persistence.ImmediateTask
	taskNotifier      TaskNotifier
	store             persistence.ProcessStore
	logger            log.Logger
}

func NewImmediateTaskConcurrentProcessor(
	ctx context.Context, cfg config.Config, notifier TaskNotifier,
	store persistence.ProcessStore, logger log.Logger,
) ImmediateTaskProcessor {
	bufferSize := cfg.AsyncService.ImmediateTaskQueue.ProcessorBufferSize
	return &immediateTaskConcurrentProcessor{
		rootCtx:           ctx,
		cfg:               cfg,
		taskToProcessChan: make(chan persistence.ImmediateTask, bufferSize),
		currentShards:     map[int32]bool{},
		taskToCommitChans: make(map[int32]chan<- persistence.ImmediateTask),
		taskNotifier:      notifier,
		store:             store,
		logger:            logger,
	}
}

func (w *immediateTaskConcurrentProcessor) Stop(context.Context) error {
	return nil
}
func (w *immediateTaskConcurrentProcessor) GetTasksToProcessChan() chan<- persistence.ImmediateTask {
	return w.taskToProcessChan
}

func (w *immediateTaskConcurrentProcessor) AddImmediateTaskQueue(
	shardId int32, tasksToCommitChan chan<- persistence.ImmediateTask,
) (alreadyExisted bool) {
	exists := w.currentShards[shardId]
	w.currentShards[shardId] = true
	w.taskToCommitChans[shardId] = tasksToCommitChan
	return exists
}

func (w *immediateTaskConcurrentProcessor) Start() error {
	concurrency := w.cfg.AsyncService.ImmediateTaskQueue.ProcessorConcurrency

	for i := 0; i < concurrency; i++ {
		go func() {
			for {
				select {
				case <-w.rootCtx.Done():
					return
				case task, ok := <-w.taskToProcessChan:
					if !ok {
						return
					}
					if !w.currentShards[task.ShardId] {
						w.logger.Info("skip the stale task that is due to shard movement", tag.Shard(task.ShardId), tag.ID(task.GetTaskId()))
						continue
					}

					err := w.processImmediateTask(w.rootCtx, task)

					if w.currentShards[task.ShardId] { // check again
						commitChan := w.taskToCommitChans[task.ShardId]
						if err != nil {
							// put it back to the queue for immediate retry
							// Note that if the error is because of invoking worker APIs, it will be sent to
							// timer task instead
							// TODO add a counter to a task, and when exceeding certain limit, put the task into a different channel to process "slowly"
							w.logger.Info("failed to process immediate task due to internal error, put back to queue for immediate retry", tag.Error(err))
							w.taskToProcessChan <- task
						} else {
							commitChan <- task
						}
					}
				}
			}
		}()
	}
	return nil
}

func (w *immediateTaskConcurrentProcessor) processImmediateTask(
	ctx context.Context, task persistence.ImmediateTask,
) error {

	w.logger.Debug("start executing immediate task", tag.ID(task.GetTaskId()), tag.ImmediateTaskType(task.TaskType.String()))

	if task.TaskType == persistence.ImmediateTaskTypeNewLocalQueueMessages {
		return w.processLocalQueueMessagesTask(ctx, task)
	}

	prep, err := w.store.PrepareStateExecution(ctx, persistence.PrepareStateExecutionRequest{
		ProcessExecutionId: task.ProcessExecutionId,
		StateExecutionId: persistence.StateExecutionId{
			StateId:         task.StateId,
			StateIdSequence: task.StateIdSequence,
		},
	})
	if err != nil {
		return err
	}

	iwfWorkerBaseUrl := urlautofix.FixWorkerUrl(prep.Info.WorkerURL)
	apiClient := xdbapi.NewAPIClient(&xdbapi.Configuration{
		Servers: []xdbapi.ServerConfiguration{
			{
				URL: iwfWorkerBaseUrl,
			},
		},
	})

	if prep.Status == persistence.StateExecutionStatusWaitUntilRunning {
		return w.processWaitUntilTask(ctx, task, *prep, apiClient)
	} else if prep.Status == persistence.StateExecutionStatusExecuteRunning {
		return w.processExecuteTask(ctx, task, *prep, apiClient)
	} else {
		w.logger.Warn("noop for immediate task ",
			tag.ID(tag.AnyToStr(task.TaskSequence)),
			tag.Value(fmt.Sprintf("status %v", prep.Status)))
		return nil
	}
}

func (w *immediateTaskConcurrentProcessor) processWaitUntilTask(
	ctx context.Context, task persistence.ImmediateTask,
	prep persistence.PrepareStateExecutionResponse, apiClient *xdbapi.APIClient,
) error {

	workerApiCtx, cancF := w.createContextWithTimeout(ctx, task.TaskType, prep.Info.StateConfig)
	defer cancF()

	if task.ImmediateTaskInfo.WorkerTaskBackoffInfo == nil {
		task.ImmediateTaskInfo.WorkerTaskBackoffInfo = createWorkerTaskBackoffInfo()
	}
	task.ImmediateTaskInfo.WorkerTaskBackoffInfo.CompletedAttempts++

	req := apiClient.DefaultAPI.ApiV1XdbWorkerAsyncStateWaitUntilPost(workerApiCtx)
	resp, httpResp, err := req.AsyncStateWaitUntilRequest(
		xdbapi.AsyncStateWaitUntilRequest{
			Context: createApiContext(
				prep,
				task,
				prep.Info.RecoverFromStateExecutionId,
				prep.Info.RecoverFromApi),
			ProcessType: prep.Info.ProcessType,
			StateId:     task.StateId,
			StateInput: &xdbapi.EncodedObject{
				Encoding: prep.Input.Encoding,
				Data:     prep.Input.Data,
			},
		},
	).Execute()
	if httpResp != nil {
		defer httpResp.Body.Close()
	}
	if w.checkResponseAndError(err, httpResp) {
		status, details := w.composeHttpError(err, httpResp, prep.Info, task)

		nextIntervalSecs, shouldRetry := w.checkRetry(task, prep.Info)
		if shouldRetry {
			return w.retryTask(ctx, task, prep, nextIntervalSecs, status, details)
		}

		return w.applyStateFailureRecoveryPolicy(ctx,
			task,
			prep,
			status,
			details,
			task.ImmediateTaskInfo.WorkerTaskBackoffInfo.CompletedAttempts,
			xdbapi.WAIT_UNTIL_API)
	}

	compResp, err := w.store.ProcessWaitUntilExecution(ctx, persistence.ProcessWaitUntilExecutionRequest{
		ProcessExecutionId: task.ProcessExecutionId,
		StateExecutionId: persistence.StateExecutionId{
			StateId:         task.StateId,
			StateIdSequence: task.StateIdSequence,
		},
		Prepare:             prep,
		CommandRequest:      resp.GetCommandRequest(),
		PublishToLocalQueue: resp.GetPublishToLocalQueue(),
		TaskShardId:         task.ShardId,
	})
	if err != nil {
		return err
	}

	if compResp.HasNewImmediateTask {
		w.notifyNewImmediateTask(prep, task)
	}

	if len(compResp.FireTimestamps) > 0 {
		w.taskNotifier.NotifyNewTimerTasks(xdbapi.NotifyTimerTasksRequest{
			ShardId:            task.ShardId,
			Namespace:          &prep.Info.Namespace,
			ProcessId:          &prep.Info.ProcessId,
			ProcessExecutionId: ptr.Any(task.ProcessExecutionId.String()),
			FireTimestamps:     compResp.FireTimestamps,
		})
	}

	return nil
}

func (w *immediateTaskConcurrentProcessor) applyStateFailureRecoveryPolicy(
	ctx context.Context,
	task persistence.ImmediateTask,
	prep persistence.PrepareStateExecutionResponse,
	status int32,
	details string,
	completedAttempts int32,
	stateApiType xdbapi.StateApiType,
) error {
	stateRecoveryPolicy := xdbapi.StateFailureRecoveryOptions{
		Policy: xdbapi.FAIL_PROCESS_ON_STATE_FAILURE,
	}
	if prep.Info.StateConfig != nil && prep.Info.StateConfig.StateFailureRecoveryOptions != nil {
		stateRecoveryPolicy = *prep.Info.StateConfig.StateFailureRecoveryOptions
	}
	switch stateRecoveryPolicy.Policy {
	case xdbapi.FAIL_PROCESS_ON_STATE_FAILURE:
		resp, errStopProcess := w.store.StopProcess(ctx, persistence.StopProcessRequest{
			Namespace:       prep.Info.Namespace,
			ProcessId:       prep.Info.ProcessId,
			ProcessStopType: xdbapi.FAIL,
		})

		if errStopProcess != nil {
			return errStopProcess
		}
		if resp.NotExists {
			// this should not happen
			return fmt.Errorf("process does not exist when stopping process for state failure")
		}
	case xdbapi.PROCEED_TO_CONFIGURED_STATE:
		if prep.Info.StateConfig == nil || prep.Info.StateConfig.StateFailureRecoveryOptions == nil ||
			prep.Info.StateConfig.StateFailureRecoveryOptions.StateFailureProceedStateId == nil {
			return fmt.Errorf("cannot proceed to configured state because of missing state config")
		}

		err := w.store.RecoverFromStateExecutionFailure(ctx, persistence.RecoverFromStateExecutionFailureRequest{
			Namespace:          prep.Info.Namespace,
			ProcessExecutionId: task.ProcessExecutionId,
			SourceStateExecutionId: persistence.StateExecutionId{
				StateId:         task.StateId,
				StateIdSequence: task.StateIdSequence,
			},
			SourceFailedStateApi:         stateApiType,
			LastFailureStatus:            status,
			LastFailureDetails:           details,
			LastFailureCompletedAttempts: completedAttempts,
			Prepare:                      prep,
			DestinationStateId:           *prep.Info.StateConfig.StateFailureRecoveryOptions.StateFailureProceedStateId,
			DestinationStateConfig:       prep.Info.StateConfig.StateFailureRecoveryOptions.StateFailureProceedStateConfig,
			DestinationStateInput:        prep.Input,
			ShardId:                      task.ShardId,
		})
		if err != nil {
			return err
		}
		nextImmediateTask := persistence.ImmediateTask{
			ShardId:            task.ShardId,
			ProcessExecutionId: task.ProcessExecutionId,
			StateExecutionId: persistence.StateExecutionId{
				StateId:         *prep.Info.StateConfig.StateFailureRecoveryOptions.StateFailureProceedStateId,
				StateIdSequence: 1,
			},
		}

		proceedStateConfig := prep.Info.StateConfig.StateFailureRecoveryOptions.StateFailureProceedStateConfig
		if proceedStateConfig == nil || !proceedStateConfig.GetSkipWaitUntil() {
			nextImmediateTask.TaskType = persistence.ImmediateTaskTypeWaitUntil
		} else {
			nextImmediateTask.TaskType = persistence.ImmediateTaskTypeExecute
		}
		w.notifyNewImmediateTask(prep, nextImmediateTask)
	default:
		return fmt.Errorf("unknown state failure recovery policy %v", stateRecoveryPolicy.Policy)
	}

	return nil
}

func createWorkerTaskBackoffInfo() *persistence.WorkerTaskBackoffInfoJson {
	return &persistence.WorkerTaskBackoffInfoJson{
		CompletedAttempts:            int32(0),
		FirstAttemptTimestampSeconds: time.Now().Unix(),
	}
}

func createApiContext(
	prep persistence.PrepareStateExecutionResponse,
	task persistence.ImmediateTask,
	recoverFromStateExecutionId *string,
	RecoverFromApi *xdbapi.StateApiType,
) xdbapi.Context {
	return xdbapi.Context{
		ProcessId:          prep.Info.ProcessId,
		ProcessExecutionId: task.ProcessExecutionId.String(),
		StateExecutionId:   ptr.Any(task.StateExecutionId.GetStateExecutionId()),

		Attempt:                     ptr.Any(task.ImmediateTaskInfo.WorkerTaskBackoffInfo.CompletedAttempts),
		FirstAttemptTimestamp:       ptr.Any(task.ImmediateTaskInfo.WorkerTaskBackoffInfo.FirstAttemptTimestampSeconds),
		RecoverFromStateExecutionId: recoverFromStateExecutionId,
		RecoverFromApi:              RecoverFromApi,
		// TODO add processStartTime
	}
}

func (w *immediateTaskConcurrentProcessor) processExecuteTask(
	ctx context.Context, task persistence.ImmediateTask,
	prep persistence.PrepareStateExecutionResponse, apiClient *xdbapi.APIClient,
) error {

	if task.ImmediateTaskInfo.WorkerTaskBackoffInfo == nil {
		task.ImmediateTaskInfo.WorkerTaskBackoffInfo = createWorkerTaskBackoffInfo()
	}
	task.ImmediateTaskInfo.WorkerTaskBackoffInfo.CompletedAttempts++

	ctx, cancF := w.createContextWithTimeout(ctx, task.TaskType, prep.Info.StateConfig)
	defer cancF()

	var resp *xdbapi.AsyncStateExecuteResponse
	var httpResp *http.Response
	loadedGlobalAttributesResp, errToCheck := w.loadGlobalAttributesIfNeeded(ctx, prep, task)
	if errToCheck == nil {
		req := apiClient.DefaultAPI.ApiV1XdbWorkerAsyncStateExecutePost(ctx)
		resp, httpResp, errToCheck = req.AsyncStateExecuteRequest(
			xdbapi.AsyncStateExecuteRequest{
				Context: createApiContext(
					prep,
					task,
					prep.Info.RecoverFromStateExecutionId,
					prep.Info.RecoverFromApi),
				ProcessType: prep.Info.ProcessType,
				StateId:     task.StateId,
				StateInput: &xdbapi.EncodedObject{
					Encoding: prep.Input.Encoding,
					Data:     prep.Input.Data,
				},
				CommandResults:         &prep.WaitUntilCommandResults,
				LoadedGlobalAttributes: &loadedGlobalAttributesResp.Response,
			},
		).Execute()
		if httpResp != nil {
			defer httpResp.Body.Close()
		}

		if errToCheck == nil {
			errToCheck = checkDecision(resp.StateDecision)
		}
	}

	if w.checkResponseAndError(errToCheck, httpResp) {
		status, details := w.composeHttpError(errToCheck, httpResp, prep.Info, task)

		nextIntervalSecs, shouldRetry := w.checkRetry(task, prep.Info)
		if shouldRetry {
			return w.retryTask(ctx, task, prep, nextIntervalSecs, status, details)
		}
		return w.applyStateFailureRecoveryPolicy(ctx,
			task,
			prep,
			status,
			details,
			task.ImmediateTaskInfo.WorkerTaskBackoffInfo.CompletedAttempts,
			xdbapi.EXECUTE_API)
	}

	compResp, err := w.store.CompleteExecuteExecution(ctx, persistence.CompleteExecuteExecutionRequest{
		ProcessExecutionId: task.ProcessExecutionId,
		StateExecutionId: persistence.StateExecutionId{
			StateId:         task.StateId,
			StateIdSequence: task.StateIdSequence,
		},
		Prepare:                    prep,
		StateDecision:              resp.StateDecision,
		PublishToLocalQueue:        resp.GetPublishToLocalQueue(),
		TaskShardId:                task.ShardId,
		GlobalAttributeTableConfig: prep.Info.GlobalAttributeConfig,
		UpdateGlobalAttributes:     resp.WriteToGlobalAttributes,
	})
	if err != nil {
		return err
	}
	if compResp.FailAtUpdatingGlobalAttributes {
		// TODO this should be treated as user error, we should use the same logic as backoff+applyStateFailureRecoveryPolicy
		// for now we just retry the task for demo purpose
		w.logger.Warn("failed to update global attributes", tag.ID(task.GetTaskId()))
		return fmt.Errorf("failed to update global attributes")
	}
	if compResp.HasNewImmediateTask {
		w.notifyNewImmediateTask(prep, task)
	}
	return nil
}

func (w *immediateTaskConcurrentProcessor) createContextWithTimeout(
	ctx context.Context, taskType persistence.ImmediateTaskType, stateConfig *xdbapi.AsyncStateConfig,
) (context.Context, context.CancelFunc) {
	qCfg := w.cfg.AsyncService.ImmediateTaskQueue
	timeout := qCfg.DefaultAsyncStateAPITimeout
	if stateConfig != nil {
		if taskType == persistence.ImmediateTaskTypeWaitUntil {
			if stateConfig.GetWaitUntilApiTimeoutSeconds() > 0 {
				timeout = time.Duration(stateConfig.GetWaitUntilApiTimeoutSeconds()) * time.Second
			}
		} else if taskType == persistence.ImmediateTaskTypeExecute {
			if stateConfig.GetExecuteApiTimeoutSeconds() > 0 {
				timeout = time.Duration(stateConfig.GetExecuteApiTimeoutSeconds()) * time.Second
			}
		} else {
			panic("invalid taskType " + string(taskType) + ", critical code bug")
		}
		if timeout > qCfg.MaxAsyncStateAPITimeout {
			timeout = qCfg.MaxAsyncStateAPITimeout
		}
	}
	return context.WithTimeout(ctx, timeout)
}

func (w *immediateTaskConcurrentProcessor) notifyNewImmediateTask(
	prep persistence.PrepareStateExecutionResponse, task persistence.ImmediateTask,
) {
	w.taskNotifier.NotifyNewImmediateTasks(xdbapi.NotifyImmediateTasksRequest{
		ShardId:            persistence.DefaultShardId,
		Namespace:          &prep.Info.Namespace,
		ProcessId:          &prep.Info.ProcessId,
		ProcessExecutionId: ptr.Any(task.ProcessExecutionId.String()),
	})
}

func (w *immediateTaskConcurrentProcessor) checkRetry(
	task persistence.ImmediateTask, info persistence.AsyncStateExecutionInfoJson,
) (nextBackoffSeconds int32, shouldRetry bool) {
	if task.TaskType == persistence.ImmediateTaskTypeWaitUntil {
		return GetNextBackoff(
			task.ImmediateTaskInfo.WorkerTaskBackoffInfo.CompletedAttempts,
			task.ImmediateTaskInfo.WorkerTaskBackoffInfo.FirstAttemptTimestampSeconds,
			info.StateConfig.WaitUntilApiRetryPolicy)
	} else if task.TaskType == persistence.ImmediateTaskTypeExecute {
		return GetNextBackoff(
			task.ImmediateTaskInfo.WorkerTaskBackoffInfo.CompletedAttempts,
			task.ImmediateTaskInfo.WorkerTaskBackoffInfo.FirstAttemptTimestampSeconds,
			info.StateConfig.ExecuteApiRetryPolicy)
	}

	panic("invalid task type " + string(task.TaskType))
}

func (w *immediateTaskConcurrentProcessor) retryTask(
	ctx context.Context, task persistence.ImmediateTask,
	prep persistence.PrepareStateExecutionResponse, nextIntervalSecs int32,
	LastFailureStatus int32, LastFailureDetails string,
) error {
	fireTimeUnixSeconds := time.Now().Unix() + int64(nextIntervalSecs)
	err := w.store.BackoffImmediateTask(ctx, persistence.BackoffImmediateTaskRequest{
		LastFailureStatus:    LastFailureStatus,
		LastFailureDetails:   LastFailureDetails,
		Prep:                 prep,
		FireTimestampSeconds: fireTimeUnixSeconds,
		Task:                 task,
	})
	if err != nil {
		return err
	}
	w.taskNotifier.NotifyNewTimerTasks(xdbapi.NotifyTimerTasksRequest{
		ShardId:            persistence.DefaultShardId,
		Namespace:          &prep.Info.Namespace,
		ProcessId:          &prep.Info.ProcessId,
		ProcessExecutionId: ptr.Any(task.ProcessExecutionId.String()),
		FireTimestamps:     []int64{fireTimeUnixSeconds},
	})
	w.logger.Debug("retry is scheduled", tag.Value(nextIntervalSecs), tag.Value(time.Unix(fireTimeUnixSeconds, 0)))
	return nil
}

func checkDecision(decision xdbapi.StateDecision) error {
	if decision.HasThreadCloseDecision() && len(decision.GetNextStates()) > 0 {
		return fmt.Errorf("cannot have both thread decision and next states")
	}
	return nil
}

func (w *immediateTaskConcurrentProcessor) checkResponseAndError(err error, httpResp *http.Response) bool {
	status := 0
	if httpResp != nil {
		status = httpResp.StatusCode
	}
	w.logger.Debug("immediate task executed", tag.Error(err), tag.StatusCode(status))

	if err != nil || (httpResp != nil && httpResp.StatusCode != http.StatusOK) {
		return true
	}
	return false
}

func (w *immediateTaskConcurrentProcessor) composeHttpError(
	err error, httpResp *http.Response,
	info persistence.AsyncStateExecutionInfoJson, task persistence.ImmediateTask,
) (int32, string) {
	responseBody := "None"
	var statusCode int32
	if httpResp != nil {
		body, err := ioutil.ReadAll(httpResp.Body)
		if err != nil {
			responseBody = "cannot read body from http response"
		} else {
			responseBody = string(body)
		}
		statusCode = int32(httpResp.StatusCode)
	}

	details := fmt.Sprintf("errMsg: %v, responseBody: %v", err, responseBody)
	maxDetailSize := w.cfg.AsyncService.ImmediateTaskQueue.MaxStateAPIFailureDetailSize
	if len(details) > maxDetailSize {
		details = details[:maxDetailSize] + "...(truncated)"
	}

	w.logger.Info(task.TaskType.String()+" API return error",
		tag.Error(err),
		tag.StatusCode(int(statusCode)),
		tag.Namespace(info.Namespace),
		tag.ProcessType(info.ProcessType),
		tag.ProcessId(info.ProcessId),
		tag.ProcessExecutionId(task.ProcessExecutionId.String()),
		tag.StateExecutionId(task.GetStateExecutionId()),
	)

	return statusCode, details
}

func (w *immediateTaskConcurrentProcessor) processLocalQueueMessagesTask(
	ctx context.Context, task persistence.ImmediateTask,
) error {
	resp, err := w.store.ProcessLocalQueueMessages(ctx, persistence.ProcessLocalQueueMessagesRequest{
		TaskShardId:        task.ShardId,
		TaskSequence:       task.GetTaskSequence(),
		ProcessExecutionId: task.ProcessExecutionId,
		Messages:           task.ImmediateTaskInfo.LocalQueueMessageInfo,
	})
	if err != nil {
		return err
	}

	if resp.HasNewImmediateTask {
		processExecutionIdString := task.ProcessExecutionId.String()
		w.taskNotifier.NotifyNewImmediateTasks(xdbapi.NotifyImmediateTasksRequest{
			ShardId:            task.ShardId,
			ProcessExecutionId: &processExecutionIdString,
		})
	}
	return nil
}

func (w *immediateTaskConcurrentProcessor) loadGlobalAttributesIfNeeded(
	ctx context.Context, prep persistence.PrepareStateExecutionResponse, task persistence.ImmediateTask,
) (*persistence.LoadGlobalAttributesResponse, error) {
	if prep.Info.StateConfig == nil ||
		prep.Info.StateConfig.LoadGlobalAttributesRequest == nil {
		return &persistence.LoadGlobalAttributesResponse{}, nil
	}

	if prep.Info.GlobalAttributeConfig == nil {
		return &persistence.LoadGlobalAttributesResponse{},
			fmt.Errorf("global attribute config is not available")
	}

	w.logger.Debug("loading global attributes for state execute",
		tag.StateExecutionId(task.GetStateExecutionId()),
		tag.JsonValue(prep.Info.StateConfig),
		tag.JsonValue(prep.Info.GlobalAttributeConfig))

	resp, err := w.store.LoadGlobalAttributes(ctx, persistence.LoadGlobalAttributesRequest{
		TableConfig: *prep.Info.GlobalAttributeConfig,
		Request:     *prep.Info.StateConfig.LoadGlobalAttributesRequest,
	})

	w.logger.Debug("loaded global attributes for state execute",
		tag.StateExecutionId(task.GetStateExecutionId()),
		tag.JsonValue(resp),
		tag.Error(err))

	return resp, err
}
