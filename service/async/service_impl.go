// Copyright (c) 2023 XDBLab Organization
// SPDX-License-Identifier: BUSL-1.1

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

	immediateTaskQueue     engine.ImmediateTaskQueue
	immediateTaskProcessor engine.ImmediateTaskProcessor

	timerTaskQueue     engine.TimerTaskQueue
	timerTaskProcessor engine.TimerTaskProcessor

	cfg    config.Config
	logger log.Logger
}

func NewAsyncServiceImpl(
	rootCtx context.Context, store persistence.ProcessStore, cfg config.Config, logger log.Logger,
) Service {
	notifier := newTaskNotifierImpl()

	immediateTaskProcessor := engine.NewImmediateTaskConcurrentProcessor(rootCtx, cfg, notifier, store, logger)
	timerTaskProcessor := engine.NewTimerTaskConcurrentProcessor(rootCtx, cfg, notifier, store, logger)

	// TODO for config.AsyncServiceModeConsistentHashingCluster
	// the queues will be created/added/managed dynamically

	immediateTaskQueue := engine.NewImmediateTaskQueueImpl(
		rootCtx, persistence.DefaultShardId, cfg, store, immediateTaskProcessor, logger)

	timerTaskQueue := engine.NewTimerTaskQueueImpl(
		rootCtx, persistence.DefaultShardId, cfg, store, timerTaskProcessor, logger)

	notifier.AddImmediateTaskQueue(persistence.DefaultShardId, immediateTaskQueue)
	notifier.AddTimerTaskQueue(persistence.DefaultShardId, timerTaskQueue)

	return &asyncService{
		immediateTaskQueue:     immediateTaskQueue,
		immediateTaskProcessor: immediateTaskProcessor,

		timerTaskQueue:     timerTaskQueue,
		timerTaskProcessor: timerTaskProcessor,

		taskNotifier: notifier,

		rootCtx: rootCtx,
		cfg:     cfg,
		logger:  logger,
	}
}

func (a asyncService) Start() error {
	err := a.immediateTaskProcessor.Start()
	if err != nil {
		a.logger.Error("fail to start immediate task processor", tag.Error(err))
		return err
	}
	err = a.timerTaskProcessor.Start()
	if err != nil {
		a.logger.Error("fail to start timer task processor", tag.Error(err))
		return err
	}
	err = a.immediateTaskQueue.Start()
	if err != nil {
		a.logger.Error("fail to start immediate task queue", tag.Error(err))
	}
	err = a.timerTaskQueue.Start()
	if err != nil {
		a.logger.Error("fail to start timer task queue", tag.Error(err))
	}
	return nil
}

func (a asyncService) NotifyPollingImmediateTask(req xdbapi.NotifyImmediateTasksRequest) error {
	if req.ShardId != persistence.DefaultShardId {
		return fmt.Errorf("the shardId %v is not owned by this instance", req.ShardId)
	}
	a.immediateTaskQueue.TriggerPollingTasks(req)
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
	err1 := a.immediateTaskQueue.Stop(ctx)
	err2 := a.immediateTaskProcessor.Stop(ctx)
	err3 := a.timerTaskQueue.Stop(ctx)
	err4 := a.timerTaskProcessor.Stop(ctx)

	return multierr.Combine(err1, err2, err3, err4)
}
