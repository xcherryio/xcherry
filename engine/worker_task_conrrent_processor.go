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

type workerTaskConcurrentProcessor struct {
	rootCtx           context.Context
	cfg               config.Config
	taskToProcessChan chan persistence.WorkerTask
	// for quickly checking if the shardId is being processed
	currentShards map[int32]bool
	// shardId to the channel
	taskToCommitChans map[int32]chan<- persistence.WorkerTask
	taskNotifier      TaskNotifier
	store             persistence.ProcessStore
	logger            log.Logger
}

func NewWorkerTaskConcurrentProcessor(
	ctx context.Context, cfg config.Config, notifier TaskNotifier,
	store persistence.ProcessStore, logger log.Logger,
) WorkerTaskProcessor {
	bufferSize := cfg.AsyncService.WorkerTaskQueue.ProcessorBufferSize
	return &workerTaskConcurrentProcessor{
		rootCtx:           ctx,
		cfg:               cfg,
		taskToProcessChan: make(chan persistence.WorkerTask, bufferSize),
		currentShards:     map[int32]bool{},
		taskToCommitChans: make(map[int32]chan<- persistence.WorkerTask),
		taskNotifier:      notifier,
		store:             store,
		logger:            logger,
	}
}

func (w *workerTaskConcurrentProcessor) Stop(context.Context) error {
	return nil
}
func (w *workerTaskConcurrentProcessor) GetTasksToProcessChan() chan<- persistence.WorkerTask {
	return w.taskToProcessChan
}

func (w *workerTaskConcurrentProcessor) AddWorkerTaskQueue(
	shardId int32, tasksToCommitChan chan<- persistence.WorkerTask,
) (alreadyExisted bool) {
	exists := w.currentShards[shardId]
	w.currentShards[shardId] = true
	w.taskToCommitChans[shardId] = tasksToCommitChan
	return exists
}

func (w *workerTaskConcurrentProcessor) Start() error {
	concurrency := w.cfg.AsyncService.WorkerTaskQueue.ProcessorConcurrency

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

					err := w.processWorkerTask(w.rootCtx, task)

					if w.currentShards[task.ShardId] { // check again
						commitChan := w.taskToCommitChans[task.ShardId]
						if err != nil {
							// put it back to the queue for immediate retry
							// Note that if the error is because of invoking worker APIs, it will be sent to
							// timer task instead
							// TODO add a counter to a task, and when exceeding certain limit, put the task into a different channel to process "slowly"
							w.logger.Warn("failed to process worker task due to internal error, put back to queue for immediate retry", tag.Error(err))
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

func (w *workerTaskConcurrentProcessor) processWorkerTask(
	ctx context.Context, task persistence.WorkerTask,
) error {

	w.logger.Info("start executing worker task", tag.ID(task.GetTaskId()))

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

	if prep.WaitUntilStatus == persistence.StateExecutionStatusRunning {
		return w.processWaitUntilTask(ctx, task, *prep, apiClient)
	} else if prep.ExecuteStatus == persistence.StateExecutionStatusRunning {
		return w.processExecuteTask(ctx, task, *prep, apiClient)
	} else {
		w.logger.Warn("noop for worker task ",
			tag.ID(tag.AnyToStr(task.TaskSequence)),
			tag.Value(fmt.Sprintf("waitUntilStatus %v, executeStatus %v",
				prep.WaitUntilStatus, prep.ExecuteStatus)))
		return nil
	}
}

func (w *workerTaskConcurrentProcessor) processWaitUntilTask(
	ctx context.Context, task persistence.WorkerTask,
	prep persistence.PrepareStateExecutionResponse, apiClient *xdbapi.APIClient,
) error {

	workerApiCtx, cancF := w.createContextWithTimeout(ctx, task.TaskType, prep.Info.StateConfig)
	defer cancF()

	if task.WorkerTaskInfo.WorkerTaskBackoffInfo == nil {
		task.WorkerTaskInfo.WorkerTaskBackoffInfo = createWorkerTaskBackoffInfo()
	}
	task.WorkerTaskInfo.WorkerTaskBackoffInfo.CompletedAttempts++

	req := apiClient.DefaultAPI.ApiV1XdbWorkerAsyncStateWaitUntilPost(workerApiCtx)
	resp, httpResp, err := req.AsyncStateWaitUntilRequest(
		xdbapi.AsyncStateWaitUntilRequest{
			Context:     createApiContext(prep, task),
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
		status, details, err := w.composeHttpError(err, httpResp)
		w.logger.Debug("state waitUntil API return error", tag.Error(err))
		// TODO add a new field in async_state_execution to record the current failure info for debugging

		nextIntervalSecs, shouldRetry := w.checkRetry(task, prep.Info)
		if shouldRetry {
			return w.retryTask(ctx, task, prep, nextIntervalSecs, status, details)
		}
		// TODO otherwise we should fail the state and process execution if the backoff is exhausted, unless using a recovery policy
		return err
	}

	commandRequest := resp.GetCommandRequest()

	compResp, err := w.store.CompleteWaitUntilExecution(ctx, persistence.CompleteWaitUntilExecutionRequest{
		ProcessExecutionId: task.ProcessExecutionId,
		StateExecutionId: persistence.StateExecutionId{
			StateId:         task.StateId,
			StateIdSequence: task.StateIdSequence,
		},
		Prepare:        prep,
		CommandRequest: commandRequest,
		TaskShardId:    task.ShardId,
	})
	if err != nil {
		return err
	}
	if compResp.HasNewWorkerTask {
		w.notifyNewWorkerTask(prep, task)
	}
	return nil
}

func createWorkerTaskBackoffInfo() *persistence.WorkerTaskBackoffInfoJson {
	return &persistence.WorkerTaskBackoffInfoJson{
		CompletedAttempts:            int32(0),
		FirstAttemptTimestampSeconds: time.Now().Unix(),
	}
}

func createApiContext(prep persistence.PrepareStateExecutionResponse, task persistence.WorkerTask) xdbapi.Context {
	return xdbapi.Context{
		ProcessId:          prep.Info.ProcessId,
		ProcessExecutionId: task.ProcessExecutionId.String(),
		StateExecutionId:   ptr.Any(task.StateExecutionId.GetStateExecutionId()),

		Attempt:               ptr.Any(task.WorkerTaskInfo.WorkerTaskBackoffInfo.CompletedAttempts),
		FirstAttemptTimestamp: ptr.Any(task.WorkerTaskInfo.WorkerTaskBackoffInfo.FirstAttemptTimestampSeconds),

		// TODO add processStartTime
	}
}

func (w *workerTaskConcurrentProcessor) processExecuteTask(
	ctx context.Context, task persistence.WorkerTask,
	prep persistence.PrepareStateExecutionResponse, apiClient *xdbapi.APIClient,
) error {

	if task.WorkerTaskInfo.WorkerTaskBackoffInfo == nil {
		task.WorkerTaskInfo.WorkerTaskBackoffInfo = createWorkerTaskBackoffInfo()
	}
	task.WorkerTaskInfo.WorkerTaskBackoffInfo.CompletedAttempts++

	ctx, cancF := w.createContextWithTimeout(ctx, task.TaskType, prep.Info.StateConfig)
	defer cancF()

	req := apiClient.DefaultAPI.ApiV1XdbWorkerAsyncStateExecutePost(ctx)
	resp, httpResp, err := req.AsyncStateExecuteRequest(
		xdbapi.AsyncStateExecuteRequest{
			Context:     createApiContext(prep, task),
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
	if err == nil {
		err = checkDecision(resp.StateDecision)
	}
	if w.checkResponseAndError(err, httpResp) {
		status, details, err := w.composeHttpError(err, httpResp)
		w.logger.Debug("state execute API return error", tag.Error(err))
		// TODO add a new field in async_state_execution to record the current failure info for debugging

		nextIntervalSecs, shouldRetry := w.checkRetry(task, prep.Info)
		if shouldRetry {
			return w.retryTask(ctx, task, prep, nextIntervalSecs, status, details)
		}
		// TODO otherwise we should fail the state and process execution if the backoff is exhausted(unless using a state recovery policy)
		// Also need to abort all other state executions
		return err
	}

	compResp, err := w.store.CompleteExecuteExecution(ctx, persistence.CompleteExecuteExecutionRequest{
		ProcessExecutionId: task.ProcessExecutionId,
		StateExecutionId: persistence.StateExecutionId{
			StateId:         task.StateId,
			StateIdSequence: task.StateIdSequence,
		},
		Prepare:       prep,
		StateDecision: resp.StateDecision,
		TaskShardId:   task.ShardId,
	})
	if err != nil {
		return err
	}
	if compResp.HasNewWorkerTask {
		w.notifyNewWorkerTask(prep, task)
	}
	return nil
}

func (w *workerTaskConcurrentProcessor) createContextWithTimeout(
	ctx context.Context, taskType persistence.WorkerTaskType, stateConfig *xdbapi.AsyncStateConfig,
) (context.Context, context.CancelFunc) {
	qCfg := w.cfg.AsyncService.WorkerTaskQueue
	timeout := qCfg.DefaultAsyncStateAPITimeout
	if stateConfig != nil {
		if taskType == persistence.WorkerTaskTypeWaitUntil {
			if stateConfig.GetWaitUntilApiTimeoutSeconds() > 0 {
				timeout = time.Duration(stateConfig.GetWaitUntilApiTimeoutSeconds()) * time.Second
			}
		} else if taskType == persistence.WorkerTaskTypeExecute {
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

func (w *workerTaskConcurrentProcessor) notifyNewWorkerTask(
	prep persistence.PrepareStateExecutionResponse, task persistence.WorkerTask,
) {
	w.taskNotifier.NotifyNewWorkerTasks(xdbapi.NotifyWorkerTasksRequest{
		ShardId:            persistence.DefaultShardId,
		Namespace:          &prep.Info.Namespace,
		ProcessId:          &prep.Info.ProcessId,
		ProcessExecutionId: ptr.Any(task.ProcessExecutionId.String()),
	})
}

func (w *workerTaskConcurrentProcessor) checkRetry(
	task persistence.WorkerTask, info persistence.AsyncStateExecutionInfoJson,
) (nextBackoffSeconds int32, shouldRetry bool) {
	return GetNextBackoff(
		task.WorkerTaskInfo.WorkerTaskBackoffInfo.CompletedAttempts,
		task.WorkerTaskInfo.WorkerTaskBackoffInfo.FirstAttemptTimestampSeconds,
		info.StateConfig.WaitUntilApiRetryPolicy)
}

func (w *workerTaskConcurrentProcessor) retryTask(
	ctx context.Context, task persistence.WorkerTask,
	prep persistence.PrepareStateExecutionResponse, nextIntervalSecs int32,
	LastFailureStatus int32, LastFailureDetails string,
) error {
	fireTimeUnixSeconds := time.Now().Unix() + int64(nextIntervalSecs)
	err := w.store.BackoffWorkerTask(ctx, persistence.BackoffWorkerTaskRequest{
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
	w.logger.Info("retry is scheduled", tag.Value(time.Unix(fireTimeUnixSeconds, 0)))
	return nil
}

func checkDecision(decision xdbapi.StateDecision) error {
	if decision.HasThreadCloseDecision() && len(decision.GetNextStates()) > 0 {
		return fmt.Errorf("cannot have both thread decision and next states")
	}
	return nil
}

func (w *workerTaskConcurrentProcessor) checkResponseAndError(err error, httpResp *http.Response) bool {
	status := 0
	if httpResp != nil {
		status = httpResp.StatusCode
	}
	w.logger.Info("worker task executed", tag.Error(err), tag.StatusCode(status))

	if err != nil || (httpResp != nil && httpResp.StatusCode != http.StatusOK) {
		return true
	}
	return false
}

func (w *workerTaskConcurrentProcessor) composeHttpError(err error, httpResp *http.Response) (int32, string, error) {
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
	maxDetailSize := w.cfg.AsyncService.WorkerTaskQueue.MaxStateAPIFailureDetailSize
	if len(details) > maxDetailSize {
		details = details[:maxDetailSize] + "...(truncated)"
	}

	return statusCode, details, fmt.Errorf("statusCode: %v, errMsg: %w, responseBody: %v", statusCode, err, responseBody)
}
