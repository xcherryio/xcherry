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
	// shardId to the notifier
	workerTaskNotifier map[int32]LocalNotifyNewWorkerTask
	store              persistence.ProcessStore
	logger             log.Logger
}

func NewWorkerTaskConcurrentProcessor(
	ctx context.Context, cfg config.Config,
	store persistence.ProcessStore, logger log.Logger) WorkerTaskProcessor {
	bufferSize := cfg.AsyncService.WorkerTaskQueue.ProcessorBufferSize
	return &workerTaskConcurrentProcessor{
		rootCtx:            ctx,
		cfg:                cfg,
		taskToProcessChan:  make(chan persistence.WorkerTask, bufferSize),
		currentShards:      map[int32]bool{},
		taskToCommitChans:  make(map[int32]chan<- persistence.WorkerTask),
		workerTaskNotifier: make(map[int32]LocalNotifyNewWorkerTask),
		store:              store,
		logger:             logger,
	}
}

func (w *workerTaskConcurrentProcessor) Stop(context.Context) error {
	return nil
}
func (w *workerTaskConcurrentProcessor) GetTasksToProcessChan() chan<- persistence.WorkerTask {
	return w.taskToProcessChan
}

func (w *workerTaskConcurrentProcessor) AddWorkerTaskQueue(
	shardId int32, tasksToCommitChan chan<- persistence.WorkerTask, notifier LocalNotifyNewWorkerTask,
) (alreadyExisted bool) {
	exists := w.currentShards[shardId]
	w.currentShards[shardId] = true
	w.workerTaskNotifier[shardId] = notifier
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
						w.logger.Info("skip the stale task that is due to shard movement", tag.Shard(task.ShardId), tag.ID(task.GetId()))
						continue
					}

					notifier := w.workerTaskNotifier[task.ShardId]
					err := w.processWorkerTask(w.rootCtx, task, notifier)

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
	ctx context.Context, task persistence.WorkerTask, notifier LocalNotifyNewWorkerTask,
) error {

	w.logger.Info("execute worker task", tag.ID(task.GetId()))

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
		return w.processWaitUntilTask(ctx, task, *prep, apiClient, notifier)
	} else if prep.ExecuteStatus == persistence.StateExecutionStatusRunning {
		return w.processExecuteTask(ctx, task, *prep, apiClient, notifier)
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
	notifier LocalNotifyNewWorkerTask,
) error {
	// TODO fix using backoff retry when worker API returns error by pushing the task into timer queue
	attempt := int32(1)

	req := apiClient.DefaultAPI.ApiV1XdbWorkerAsyncStateWaitUntilPost(ctx)
	resp, httpResp, err := req.AsyncStateWaitUntilRequest(
		xdbapi.AsyncStateWaitUntilRequest{
			Context: &xdbapi.Context{
				ProcessId:          prep.Info.ProcessId,
				ProcessExecutionId: task.ProcessExecutionId.String(),
				Attempt:            ptr.Any(attempt),
				StateExecutionId:   ptr.Any(task.StateExecutionId.GetId()),
				// TODO more fields
			},
			ProcessType: ptr.Any(prep.Info.ProcessType),
			StateId:     ptr.Any(task.StateId),
			StateInput: &xdbapi.EncodedObject{
				Encoding: prep.Input.Encoding,
				Data:     prep.Input.Data,
			},
		},
	).Execute()
	if httpResp != nil {
		defer httpResp.Body.Close()
	}
	if checkHttpError(err, httpResp) {
		err := composeHttpError(err, httpResp)
		w.logger.Info("worker API return error", tag.Error(err))
		// TODO instead of returning error, we should do backoff retry by pushing this task into timer queue
		return err
	}
	commandRequest := xdbapi.CommandRequest{
		WaitingType: xdbapi.EMPTY_COMMAND.Ptr(),
	}
	if resp.CommandRequest != nil {
		commandRequest = resp.GetCommandRequest()
	}

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
		notifier(time.Now())
	}
	return nil
}

func (w *workerTaskConcurrentProcessor) processExecuteTask(
	ctx context.Context, task persistence.WorkerTask,
	prep persistence.PrepareStateExecutionResponse, apiClient *xdbapi.APIClient,
	notifyNewTask LocalNotifyNewWorkerTask,
) (retErr error) {
	// TODO fix using backoff retry when worker API returns error by pushing the task into timer queue
	attempt := int32(1)

	req := apiClient.DefaultAPI.ApiV1XdbWorkerAsyncStateExecutePost(ctx)
	resp, httpResp, err := req.AsyncStateExecuteRequest(
		xdbapi.AsyncStateExecuteRequest{
			Context: &xdbapi.Context{
				ProcessId:          prep.Info.ProcessId,
				ProcessExecutionId: task.ProcessExecutionId.String(),
				Attempt:            ptr.Any(attempt),
				StateExecutionId:   ptr.Any(task.StateExecutionId.GetId()),
				// TODO more fields
			},
			ProcessType: ptr.Any(prep.Info.ProcessType),
			StateId:     ptr.Any(task.StateId),
			StateInput: &xdbapi.EncodedObject{
				Encoding: prep.Input.Encoding,
				Data:     prep.Input.Data,
			},
		},
	).Execute()
	if httpResp != nil {
		defer httpResp.Body.Close()
	}
	if checkHttpError(err, httpResp) {
		err := composeHttpError(err, httpResp)
		w.logger.Info("worker API return error", tag.Error(err))
		// TODO instead of returning error, we should do backoff retry by pushing this task into timer queue
		return err
	}
	err = checkDecision(resp.StateDecision)
	if err != nil {
		return err
	}

	compResp, err := w.store.CompleteExecuteExecution(ctx, persistence.CompleteExecuteExecutionRequest{
		ProcessExecutionId: task.ProcessExecutionId,
		StateExecutionId: persistence.StateExecutionId{
			StateId:         task.StateId,
			StateIdSequence: task.StateIdSequence,
		},
		Prepare:       prep,
		StateDecision: *resp.StateDecision,
		TaskShardId:   task.ShardId,
	})
	if err != nil {
		return err
	}
	if compResp.HasNewWorkerTask {
		notifyNewTask(time.Now())
	}
	return nil
}

func checkDecision(decision *xdbapi.StateDecision) error {
	if decision == nil {
		return fmt.Errorf(" a decision is required")
	}
	if decision.HasThreadCloseDecision() && len(decision.GetNextStates()) > 0 {
		return fmt.Errorf("cannot have both thread decision and next states")
	}
	return nil
}

func checkHttpError(err error, httpResp *http.Response) bool {
	if err != nil || (httpResp != nil && httpResp.StatusCode != http.StatusOK) {
		return true
	}
	return false
}

func composeHttpError(err error, httpResp *http.Response) error {
	responseBody := "None"
	var statusCode int
	if httpResp != nil {
		body, err := ioutil.ReadAll(httpResp.Body)
		if err != nil {
			responseBody = "cannot read body from http response"
		} else {
			responseBody = string(body)
		}
		statusCode = httpResp.StatusCode
	}
	return fmt.Errorf("statusCode: %v, responseBody: %v, errMsg: %w", statusCode, responseBody, err)
}
