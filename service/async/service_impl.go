// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: Apache-2.0

package async

import (
	"context"
	"fmt"
	"github.com/hashicorp/memberlist"
	"github.com/xcherryio/apis/goapi/xcapi"
	"github.com/xcherryio/xcherry/cluster"
	"github.com/xcherryio/xcherry/common/log"
	"github.com/xcherryio/xcherry/common/log/tag"
	"github.com/xcherryio/xcherry/config"
	"github.com/xcherryio/xcherry/engine"
	"github.com/xcherryio/xcherry/persistence"
	"go.uber.org/multierr"
	"strconv"
	"strings"
	"time"
)

type asyncService struct {
	rootCtx context.Context

	taskNotifier engine.TaskNotifier

	immediateTaskQueueMap  map[int32]engine.ImmediateTaskQueue
	immediateTaskProcessor engine.ImmediateTaskProcessor

	timerTaskQueueMap  map[int32]engine.TimerTaskQueue
	timerTaskProcessor engine.TimerTaskProcessor

	cfg    config.Config
	logger log.Logger

	clusterConfig *memberlist.Config

	serverAddress    string
	advertiseAddress string
}

func NewAsyncServiceImpl(
	serverAddress string,
	advertiseAddress string,
	advertiseAddressToJoin string,
	rootCtx context.Context, processStore persistence.ProcessStore,
	visibilityStore persistence.VisibilityStore,
	cfg config.Config, logger log.Logger,
) (Service, error) {
	notifier := newTaskNotifierImpl()

	immediateTaskProcessor := engine.NewImmediateTaskConcurrentProcessor(
		rootCtx, cfg, notifier, processStore, visibilityStore, logger)
	timerTaskProcessor := engine.NewTimerTaskConcurrentProcessor(rootCtx, cfg, notifier, processStore, logger)

	var conf *memberlist.Config

	// create member list config in the AsyncServiceModeConsistentHashingCluster mode
	if cfg.AsyncService.Mode == config.AsyncServiceModeConsistentHashingCluster {
		parts := strings.Split(advertiseAddress, ":")
		port, err := strconv.Atoi(parts[1])
		if err != nil {
			return nil, err
		}

		conf = memberlist.DefaultLocalConfig()
		conf.Name = "async_" + advertiseAddress + "_" + serverAddress
		conf.BindAddr = parts[0]
		conf.BindPort = port
		conf.AdvertisePort = conf.BindPort

		conf.Events = &cluster.ClusterEventDelegate{
			ServerAddress: serverAddress,
		}

		conf.Delegate = &cluster.ClusterDelegate{
			Meta: cluster.ClusterDelegateMetaData{
				ServerAddress: serverAddress,
			},
		}

		list, err := memberlist.Create(conf)
		if err != nil {
			return nil, err
		}

		if advertiseAddressToJoin != "" {
			_, err = list.Join([]string{advertiseAddressToJoin})
			if err != nil {
				return nil, err
			}
		}

		// update to real address
		local := list.LocalNode()
		advertiseAddress = cluster.BuildHostAddress(local)
	}

	return &asyncService{
		rootCtx: rootCtx,

		taskNotifier: notifier,

		immediateTaskProcessor: immediateTaskProcessor,
		timerTaskProcessor:     timerTaskProcessor,

		// to initialize after creating all the async servers
		immediateTaskQueueMap: map[int32]engine.ImmediateTaskQueue{},
		timerTaskQueueMap:     map[int32]engine.TimerTaskQueue{},

		cfg:    cfg,
		logger: logger,

		clusterConfig: conf,

		serverAddress:    serverAddress,
		advertiseAddress: advertiseAddress,
	}, nil
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

	for _, immediateTaskQueue := range a.immediateTaskQueueMap {
		err = immediateTaskQueue.Start()
		if err != nil {
			a.logger.Error("fail to start immediate task queue", tag.Error(err))
		}
	}

	for _, timerTaskQueue := range a.timerTaskQueueMap {
		err = timerTaskQueue.Start()
		if err != nil {
			a.logger.Error("fail to start timer task queue", tag.Error(err))
		}
	}

	return nil
}

func (a asyncService) NotifyPollingImmediateTask(req xcapi.NotifyImmediateTasksRequest) error {
	targetAddress := a.GetServerAddressFor(req.ShardId)

	if targetAddress != a.GetServerAddress() {
		a.logger.Info(fmt.Sprintf("NotifyPollingImmediateTask: %s -> %s", a.GetServerAddress(), targetAddress))
		a.notifyRemoteImmediateTaskAsyncInCluster(req, targetAddress)
		return nil
	}

	queue, ok := a.immediateTaskQueueMap[req.ShardId]
	if !ok {
		return fmt.Errorf("the shardId %v is not owned by this instance", req.ShardId)
	}

	queue.TriggerPollingTasks(req)
	return nil
}

func (a asyncService) NotifyPollingTimerTask(req xcapi.NotifyTimerTasksRequest) error {
	targetAddress := a.GetServerAddressFor(req.ShardId)

	if targetAddress != a.GetServerAddress() {
		a.logger.Info(fmt.Sprintf("NotifyPollingTimerTask: %s -> %s", a.GetServerAddress(), targetAddress))
		a.notifyRemoteTimerTaskAsyncInCluster(req, targetAddress)
		return nil
	}

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

func (a asyncService) CreateQueues(shardId int32, processStore persistence.ProcessStore) {
	immediateTaskQueue := engine.NewImmediateTaskQueueImpl(
		a.rootCtx, shardId, a.cfg, processStore, a.immediateTaskProcessor, a.logger)

	timerTaskQueue := engine.NewTimerTaskQueueImpl(
		a.rootCtx, shardId, a.cfg, processStore, a.timerTaskProcessor, a.logger)

	a.taskNotifier.AddImmediateTaskQueue(shardId, immediateTaskQueue)
	a.taskNotifier.AddTimerTaskQueue(shardId, timerTaskQueue)

	a.immediateTaskQueueMap[shardId] = immediateTaskQueue
	a.timerTaskQueueMap[shardId] = timerTaskQueue
}

func (a asyncService) GetServerAddress() string {
	return a.serverAddress
}

func (a asyncService) GetServerAddressFor(shardId int32) string {
	if a.cfg.AsyncService.Mode == config.AsyncServiceModeStandalone {
		return a.serverAddress
	}

	eventDelegate, ok := a.clusterConfig.Events.(*cluster.ClusterEventDelegate)
	if !ok {
		a.logger.Error(fmt.Sprintf("failed to get delegate in %s", a.serverAddress))
	}
	return eventDelegate.GetServerAddressFor(shardId)
}

func (a asyncService) GetAdvertiseAddress() string {
	return a.advertiseAddress
}

func (a asyncService) notifyRemoteImmediateTaskAsyncInCluster(req xcapi.NotifyImmediateTasksRequest, serverAddress string) {
	go func() {

		ctx, canf := context.WithTimeout(context.Background(), time.Second*10)
		defer canf()

		apiClient := xcapi.NewAPIClient(&xcapi.Configuration{
			Servers: []xcapi.ServerConfiguration{
				{
					URL: serverAddress,
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

func (a asyncService) notifyRemoteTimerTaskAsyncInCluster(req xcapi.NotifyTimerTasksRequest, serverAddress string) {
	// execute in the background as best effort
	go func() {

		ctx, canf := context.WithTimeout(context.Background(), time.Second*10)
		defer canf()

		apiClient := xcapi.NewAPIClient(&xcapi.Configuration{
			Servers: []xcapi.ServerConfiguration{
				{
					URL: serverAddress,
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
