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

package async

import (
	"context"
	"fmt"
	"github.com/xdblab/xdb-apis/goapi/xdbapi"
	"github.com/xdblab/xdb/common/log"
	"github.com/xdblab/xdb/common/log/tag"
	"github.com/xdblab/xdb/config"
	"github.com/xdblab/xdb/engine"
	"github.com/xdblab/xdb/persistence"
	"go.uber.org/multierr"
)

type asyncService struct {
	rootCtx context.Context

	taskNotifier engine.TaskNotifier

	workerTaskQueue     engine.WorkerTaskQueue
	workerTaskProcessor engine.WorkerTaskProcessor

	timerTaskQueue     engine.TimerTaskQueue
	timerTaskProcessor engine.TimerTaskProcessor

	cfg    config.Config
	logger log.Logger
}

func NewAsyncServiceImpl(
	rootCtx context.Context, store persistence.ProcessStore, cfg config.Config, logger log.Logger,
) Service {
	notifier := newTaskNotifierImpl()

	workerTaskProcessor := engine.NewWorkerTaskConcurrentProcessor(rootCtx, cfg, notifier, store, logger)
	timerTaskProcessor := engine.NewTimerTaskConcurrentProcessor(rootCtx, cfg, notifier, store, logger)

	// TODO for config.AsyncServiceModeConsistentHashingCluster
	// the worker queue will be created/added/managed dynamically

	workerTaskQueue := engine.NewWorkerTaskQueueImpl(
		rootCtx, persistence.DefaultShardId, cfg, store, workerTaskProcessor, logger)

	timerTaskQueue := engine.NewTimerTaskQueueImpl(
		rootCtx, persistence.DefaultShardId, cfg, store, timerTaskProcessor, logger)

	notifier.AddWorkerTaskQueue(persistence.DefaultShardId, workerTaskQueue)
	notifier.AddTimerTaskQueue(persistence.DefaultShardId, timerTaskQueue)

	return &asyncService{
		workerTaskQueue:     workerTaskQueue,
		workerTaskProcessor: workerTaskProcessor,

		timerTaskQueue:     timerTaskQueue,
		timerTaskProcessor: timerTaskProcessor,

		taskNotifier: notifier,

		rootCtx: rootCtx,
		cfg:     cfg,
		logger:  logger,
	}
}

func (a asyncService) Start() error {
	err := a.workerTaskProcessor.Start()
	if err != nil {
		a.logger.Error("fail to start worker task processor", tag.Error(err))
		return err
	}
	err = a.timerTaskProcessor.Start()
	if err != nil {
		a.logger.Error("fail to start timer task processor", tag.Error(err))
		return err
	}
	err = a.workerTaskQueue.Start()
	if err != nil {
		a.logger.Error("fail to start worker task queue", tag.Error(err))
	}
	err = a.timerTaskQueue.Start()
	if err != nil {
		a.logger.Error("fail to start timer task queue", tag.Error(err))
	}
	return nil
}

func (a asyncService) NotifyPollingWorkerTask(req xdbapi.NotifyWorkerTasksRequest) error {
	if req.ShardId != persistence.DefaultShardId {
		return fmt.Errorf("the shardId %v is not owned by this instance", req.ShardId)
	}
	a.workerTaskQueue.TriggerPollingTasks(req)
	return nil
}

func (a asyncService) NotifyPollingTimerTask(req xdbapi.NotifyTimerTasksRequest) error {
	if req.ShardId != persistence.DefaultShardId {
		return fmt.Errorf("the shardId %v is not owned by this instance", req.ShardId)
	}
	a.timerTaskQueue.TriggerPollingTasks(req)
	return nil
}

func (a asyncService) Stop(ctx context.Context) error {
	err1 := a.workerTaskQueue.Stop(ctx)
	err2 := a.workerTaskProcessor.Stop(ctx)
	err3 := a.timerTaskQueue.Stop(ctx)
	err4 := a.timerTaskProcessor.Stop(ctx)

	return multierr.Combine(err1, err2, err3, err4)
}
