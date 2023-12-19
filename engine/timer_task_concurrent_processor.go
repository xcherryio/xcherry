// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"context"
	"fmt"
	"github.com/xcherryio/apis/goapi/xcapi"
	"github.com/xcherryio/xcherry/persistence/data_models"

	"github.com/xcherryio/xcherry/common/log"
	"github.com/xcherryio/xcherry/common/log/tag"
	"github.com/xcherryio/xcherry/common/ptr"
	"github.com/xcherryio/xcherry/config"
	"github.com/xcherryio/xcherry/persistence"
)

type timerTaskConcurrentProcessor struct {
	rootCtx           context.Context
	cfg               config.Config
	taskToProcessChan chan data_models.TimerTask
	// for quickly checking if the shardId is being processed
	currentShards map[int32]bool
	taskNotifier  TaskNotifier
	store         persistence.ProcessStore
	logger        log.Logger
}

func NewTimerTaskConcurrentProcessor(
	ctx context.Context, cfg config.Config, notifier TaskNotifier,
	store persistence.ProcessStore, logger log.Logger,
) TimerTaskProcessor {
	bufferSize := cfg.AsyncService.TimerTaskQueue.ProcessorBufferSize
	return &timerTaskConcurrentProcessor{
		rootCtx:           ctx,
		cfg:               cfg,
		taskToProcessChan: make(chan data_models.TimerTask, bufferSize),
		currentShards:     map[int32]bool{},
		taskNotifier:      notifier,
		store:             store,
		logger:            logger,
	}
}

func (w *timerTaskConcurrentProcessor) Stop(context.Context) error {
	return nil
}
func (w *timerTaskConcurrentProcessor) GetTasksToProcessChan() chan<- data_models.TimerTask {
	return w.taskToProcessChan
}

func (w *timerTaskConcurrentProcessor) AddTimerTaskQueue(
	shardId int32,
) (alreadyExisted bool) {
	exists := w.currentShards[shardId]
	w.currentShards[shardId] = true
	return exists
}

func (w *timerTaskConcurrentProcessor) Start() error {
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
						w.logger.Info("skip the stale task that is due to shard movement", tag.Shard(task.ShardId), tag.ID(task.GetStateExecutionId()))
						continue
					}

					err := w.processTimerTask(task)

					if w.currentShards[task.ShardId] { // check again
						if err != nil {
							// put it back to the queue for immediate retry
							// Note that if the error is because of invoking worker APIs, it will be sent to
							// timer task instead
							// TODO add a counter to a task, and when exceeding certain limit, put the task into a different channel to process "slowly"
							w.logger.Warn("failed to process timer task due to internal error, put back to queue for immediate retry", tag.Error(err))
							w.taskToProcessChan <- task
						}
					}
				}
			}
		}()
	}
	return nil
}

func (w *timerTaskConcurrentProcessor) processTimerTask(
	task data_models.TimerTask,
) error {

	w.logger.Debug("start executing timer task", tag.ID(task.GetStateExecutionId()))

	switch task.TaskType {
	case data_models.TimerTaskTypeWorkerTaskBackoff:
		return w.processTimerTaskWorkerTaskBackoff(task)
	case data_models.TimerTaskTypeProcessTimeout:
		return w.processTimerTaskProcessTimeout(task)
	case data_models.TimerTaskTypeTimerCommand:
		return w.processTimerTaskForTimerCommand(task)
	default:
		panic(fmt.Sprintf("unknown timer task type %v", task.TaskType))
	}
}

func (w *timerTaskConcurrentProcessor) processTimerTaskProcessTimeout(
	task data_models.TimerTask,
) error {
	resp, err := w.store.ProcessTimerTaskForProcessTimeout(w.rootCtx, data_models.ProcessTimerTaskRequest{
		Task: task,
	})
	if err != nil {
		return err
	}

	if resp.HasNewImmediateTask {
		panic("process timeout should not generate new immediate task")
	}

	return nil
}

func (w *timerTaskConcurrentProcessor) processTimerTaskWorkerTaskBackoff(
	task data_models.TimerTask,
) error {
	resp, err := w.store.ConvertTimerTaskToImmediateTask(w.rootCtx, data_models.ProcessTimerTaskRequest{
		Task: task,
	})
	if err != nil {
		return err
	}

	if resp.HasNewImmediateTask {
		notiReq := xcapi.NotifyImmediateTasksRequest{
			ShardId:            task.ShardId,
			ProcessExecutionId: ptr.Any(task.ProcessExecutionId.String()),
		}
		if task.OptionalPartitionKey != nil {
			notiReq.ProcessId = &task.OptionalPartitionKey.ProcessId
			notiReq.Namespace = &task.OptionalPartitionKey.Namespace
		}
		w.taskNotifier.NotifyNewImmediateTasks(notiReq)
	}

	return nil
}

func (w *timerTaskConcurrentProcessor) processTimerTaskForTimerCommand(
	task data_models.TimerTask,
) error {
	resp, err := w.store.ProcessTimerTaskForTimerCommand(w.rootCtx, data_models.ProcessTimerTaskRequest{
		Task: task,
	})
	if err != nil {
		return err
	}

	if resp.HasNewImmediateTask {
		notiReq := xcapi.NotifyImmediateTasksRequest{
			ShardId:            task.ShardId,
			ProcessExecutionId: ptr.Any(task.ProcessExecutionId.String()),
		}
		if task.OptionalPartitionKey != nil {
			notiReq.ProcessId = &task.OptionalPartitionKey.ProcessId
			notiReq.Namespace = &task.OptionalPartitionKey.Namespace
		}
		w.taskNotifier.NotifyNewImmediateTasks(notiReq)
	}

	return nil
}
