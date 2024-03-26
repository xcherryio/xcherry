// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: Apache-2.0

package async

import (
	"context"
	"fmt"
	"github.com/xcherryio/apis/goapi/xcapi"
	"github.com/xcherryio/xcherry/common/log"
	"github.com/xcherryio/xcherry/common/log/tag"
	"github.com/xcherryio/xcherry/config"
	"github.com/xcherryio/xcherry/engine"
	"github.com/xcherryio/xcherry/persistence"
	"go.uber.org/multierr"
	"sort"
	"strconv"
	"time"
)

type asyncService struct {
	rootCtx context.Context

	taskNotifier engine.TaskNotifier

	// shardId: queue
	immediateTaskQueueMap  map[int32]engine.ImmediateTaskQueue
	immediateTaskProcessor engine.ImmediateTaskProcessor

	// shardId: queue
	timerTaskQueueMap  map[int32]engine.TimerTaskQueue
	timerTaskProcessor engine.TimerTaskProcessor

	processStore persistence.ProcessStore

	cfg    config.Config
	logger log.Logger
}

func NewAsyncServiceImpl(
	rootCtx context.Context, processStore persistence.ProcessStore,
	visibilityStore persistence.VisibilityStore,
	cfg config.Config, logger log.Logger,
) Service {
	notifier := newTaskNotifierImpl()

	immediateTaskProcessor := engine.NewImmediateTaskConcurrentProcessor(
		rootCtx, cfg, notifier, processStore, visibilityStore, logger)
	timerTaskProcessor := engine.NewTimerTaskConcurrentProcessor(rootCtx, cfg, notifier, processStore, logger)

	return asyncService{
		// to be dynamically initialized later
		immediateTaskQueueMap: map[int32]engine.ImmediateTaskQueue{},
		timerTaskQueueMap:     map[int32]engine.TimerTaskQueue{},

		immediateTaskProcessor: immediateTaskProcessor,
		timerTaskProcessor:     timerTaskProcessor,

		taskNotifier: notifier,

		processStore: processStore,

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

	// When in the standalone mode, need to manually re-balance once to create queues
	if a.cfg.AsyncService.Mode == config.AsyncServiceModeStandalone {
		a.ReBalance([]int32{0})
	}

	return nil
}

func (a asyncService) NotifyPollingImmediateTask(req xcapi.NotifyImmediateTasksRequest) error {
	queue, ok := a.immediateTaskQueueMap[req.ShardId]
	if !ok {
		return fmt.Errorf("the shardId %v is not owned by this instance", req.ShardId)
	}

	queue.TriggerPollingTasks(req)
	return nil
}

func (a asyncService) NotifyPollingTimerTask(req xcapi.NotifyTimerTasksRequest) error {
	queue, ok := a.timerTaskQueueMap[req.ShardId]
	if !ok {
		return fmt.Errorf("the shardId %v is not owned by this instance", req.ShardId)
	}

	queue.TriggerPollingTasks(req)
	return nil
}

func (a asyncService) Stop(ctx context.Context) error {
	var errs []error

	errs = append(errs, a.immediateTaskProcessor.Stop(ctx))
	errs = append(errs, a.timerTaskProcessor.Stop(ctx))

	for _, immediateTaskQueue := range a.immediateTaskQueueMap {
		errs = append(errs, immediateTaskQueue.Stop(ctx))
	}

	for _, timerTaskQueue := range a.timerTaskQueueMap {
		errs = append(errs, timerTaskQueue.Stop(ctx))
	}

	return multierr.Combine(errs...)
}

func (a asyncService) ReBalance(assignedShardIds []int32) {
	// logging
	var oldShardIds []int
	for shardId := range a.immediateTaskQueueMap {
		oldShardIds = append(oldShardIds, int(shardId))
	}
	sort.Ints(oldShardIds)

	newShardsStr := ""
	oldShardStr := ""

	for _, shardId := range assignedShardIds {
		newShardsStr += " " + strconv.Itoa(int(shardId))
	}

	for _, shardId := range oldShardIds {
		oldShardStr += " " + strconv.Itoa(shardId)
	}

	a.logger.Info(fmt.Sprintf("ReBalance: %s -> %s", oldShardStr, newShardsStr))

	// execute
	assignedShardMap := map[int32]bool{}
	var currentShardsToRemove []int32

	for _, shardId := range assignedShardIds {
		assignedShardMap[shardId] = true
	}

	for shardId := range a.immediateTaskQueueMap {
		_, ok := assignedShardMap[shardId]
		if !ok {
			currentShardsToRemove = append(currentShardsToRemove, shardId)
		} else {
			delete(assignedShardMap, shardId)
		}
	}

	for i := 0; i < len(currentShardsToRemove); i++ {
		a.stopQueuesAndRemove(currentShardsToRemove[i])
	}

	for shardId := range assignedShardMap {
		a.createQueuesAndStart(shardId)
	}

}

func (a asyncService) createQueuesAndStart(shardId int32) {
	a.logger.Info(fmt.Sprintf("createQueuesAndStart: %d", shardId))

	// immediateTaskQueue
	immediateTaskQueue := engine.NewImmediateTaskQueueImpl(
		a.rootCtx, shardId, a.cfg, a.processStore, a.immediateTaskProcessor, a.logger)

	a.taskNotifier.AddImmediateTaskQueue(shardId, immediateTaskQueue)
	a.immediateTaskQueueMap[shardId] = immediateTaskQueue

	err := immediateTaskQueue.Start()
	if err != nil {
		a.logger.Error(fmt.Sprintf("fail to start immediate task queue with shard %d", shardId), tag.Error(err))
	}

	// timerTaskQueue
	timerTaskQueue := engine.NewTimerTaskQueueImpl(
		a.rootCtx, shardId, a.cfg, a.processStore, a.timerTaskProcessor, a.logger)

	a.taskNotifier.AddTimerTaskQueue(shardId, timerTaskQueue)
	a.timerTaskQueueMap[shardId] = timerTaskQueue

	err = timerTaskQueue.Start()
	if err != nil {
		a.logger.Error(fmt.Sprintf("fail to start timer task queue with shard %d", shardId), tag.Error(err))
	}
}

func (a asyncService) stopQueuesAndRemove(shardId int32) {
	a.logger.Info(fmt.Sprintf("stopQueuesAndRemove: %d", shardId))

	// immediateTaskQueue
	immediateTaskQueue, ok := a.immediateTaskQueueMap[shardId]
	if !ok {
		a.logger.Error(fmt.Sprintf("fail to get immediate task queue with shard %d", shardId))
	} else {
		err := immediateTaskQueue.Stop(a.rootCtx)
		if err != nil {
			a.logger.Error(fmt.Sprintf("fail to stop immediate task queue with shard %d", shardId), tag.Error(err))
		}

		a.taskNotifier.RemoveImmediateTaskQueue(shardId)
		delete(a.immediateTaskQueueMap, shardId)
	}

	// timerTaskQueue
	timerTaskQueue, ok := a.timerTaskQueueMap[shardId]
	if !ok {
		a.logger.Error(fmt.Sprintf("fail to get timer task queue with shard %d", shardId))
	} else {
		err := timerTaskQueue.Stop(a.rootCtx)
		if err != nil {
			a.logger.Error(fmt.Sprintf("fail to stop timer task queue with shard %d", shardId), tag.Error(err))
		}

		a.taskNotifier.RemoveTimerTaskQueue(shardId)
		delete(a.timerTaskQueueMap, shardId)
	}
}

func (a asyncService) NotifyRemoteImmediateTaskAsyncInCluster(req xcapi.NotifyImmediateTasksRequest, serverAddress string) {
	go func() {

		ctx, canf := context.WithTimeout(context.Background(), time.Second*10)
		defer canf()

		apiClient := xcapi.NewAPIClient(&xcapi.Configuration{
			Servers: []xcapi.ServerConfiguration{
				{
					URL: "http://" + serverAddress,
				},
			},
		})

		request := apiClient.DefaultAPI.InternalApiV1XcherryNotifyImmediateTasksPost(ctx)
		httpResp, err := request.NotifyImmediateTasksRequest(req).Execute()
		if httpResp != nil {
			defer httpResp.Body.Close()
		}
		if err != nil {
			a.logger.Error("failed to notify remote immediate task in cluster", tag.Error(err))
			// TODO add backoff and retry
			return
		}
	}()
}

func (a asyncService) NotifyRemoteTimerTaskAsyncInCluster(req xcapi.NotifyTimerTasksRequest, serverAddress string) {
	// execute in the background as best effort
	go func() {

		ctx, canf := context.WithTimeout(context.Background(), time.Second*10)
		defer canf()

		apiClient := xcapi.NewAPIClient(&xcapi.Configuration{
			Servers: []xcapi.ServerConfiguration{
				{
					URL: "http://" + serverAddress,
				},
			},
		})

		request := apiClient.DefaultAPI.InternalApiV1XcherryNotifyTimerTasksPost(ctx)
		httpResp, err := request.NotifyTimerTasksRequest(req).Execute()
		if httpResp != nil {
			defer httpResp.Body.Close()
		}
		if err != nil {
			a.logger.Error("failed to notify remote timer task in cluster", tag.Error(err))
			// TODO add backoff and retry
			return
		}
	}()
}
