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
	"github.com/xdblab/xdb-apis/goapi/xdbapi"
	"github.com/xdblab/xdb/common/log"
	"github.com/xdblab/xdb/common/log/tag"
	"github.com/xdblab/xdb/common/ptr"
	"github.com/xdblab/xdb/config"
	"github.com/xdblab/xdb/persistence"
)

type timerTaskConcurrentProcessor struct {
	rootCtx           context.Context
	cfg               config.Config
	taskToProcessChan chan persistence.TimerTask
	// for quickly checking if the shardId is being processed
	currentShards map[int32]bool
	// shardId to the channel
	taskToCommitChans map[int32]chan<- persistence.TimerTask
	taskNotifier      TaskNotifier
	store             persistence.ProcessStore
	logger            log.Logger
}

func NewTimerTaskConcurrentProcessor(
	ctx context.Context, cfg config.Config, notifier TaskNotifier,
	store persistence.ProcessStore, logger log.Logger,
) TimerTaskProcessor {
	bufferSize := cfg.AsyncService.TimerTaskQueue.ProcessorBufferSize
	return &timerTaskConcurrentProcessor{
		rootCtx:           ctx,
		cfg:               cfg,
		taskToProcessChan: make(chan persistence.TimerTask, bufferSize),
		currentShards:     map[int32]bool{},
		taskToCommitChans: make(map[int32]chan<- persistence.TimerTask),
		taskNotifier:      notifier,
		store:             store,
		logger:            logger,
	}
}

func (w *timerTaskConcurrentProcessor) Stop(context.Context) error {
	return nil
}
func (w *timerTaskConcurrentProcessor) GetTasksToProcessChan() chan<- persistence.TimerTask {
	return w.taskToProcessChan
}

func (w *timerTaskConcurrentProcessor) AddTimerTaskQueue(
	shardId int32, tasksToCommitChan chan<- persistence.TimerTask,
) (alreadyExisted bool) {
	exists := w.currentShards[shardId]
	w.currentShards[shardId] = true
	w.taskToCommitChans[shardId] = tasksToCommitChan
	return exists
}

func (w *timerTaskConcurrentProcessor) Start() error {
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
						w.logger.Info("skip the stale task that is due to shard movement", tag.Shard(task.ShardId), tag.ID(task.GetStateExecutionId()))
						continue
					}

					err := w.processTimerTask(task)

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

func (w *timerTaskConcurrentProcessor) processTimerTask(
	task persistence.TimerTask,
) error {

	w.logger.Info("execute timer task", tag.ID(task.GetStateExecutionId()))

	switch task.TaskType {
	case persistence.TimerTaskTypeWorkerTaskBackoff:
		return w.processTimerTaskWorkerTaskBackoff(task)
	case persistence.TimerTaskTypeProcessTimeout:
		panic("TODO")
	case persistence.TimerTaskTypeTimerCommand:
		panic("TODO")
	default:
		panic(fmt.Sprintf("unknown timer task type %v", task.TaskType))
	}
}

func (w *timerTaskConcurrentProcessor) processTimerTaskWorkerTaskBackoff(
	task persistence.TimerTask,
) error {
	err := w.store.ConvertTimerTaskToWorkerTask(w.rootCtx, persistence.ConvertTimerTaskToWorkerTaskRequest{
		Task: task,
	})
	if err != nil {
		return err
	}
	notiReq := xdbapi.NotifyWorkerTasksRequest{
		ShardId:            task.ShardId,
		ProcessExecutionId: ptr.Any(task.ProcessExecutionId.String()),
	}
	if task.OptionalPartitionKey != nil {
		notiReq.ProcessId = &task.OptionalPartitionKey.ProcessId
		notiReq.Namespace = &task.OptionalPartitionKey.Namespace
	}
	w.taskNotifier.NotifyNewWorkerTasks(notiReq)
	return nil
}
