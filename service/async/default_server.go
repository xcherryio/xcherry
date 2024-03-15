// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: Apache-2.0

package async

import (
	"context"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/xcherryio/xcherry/common/log"
	"github.com/xcherryio/xcherry/common/log/tag"
	"github.com/xcherryio/xcherry/config"
	"github.com/xcherryio/xcherry/persistence"
	"go.uber.org/multierr"
	"net"
	"net/http"
	"strings"
)

const PathNotifyImmediateTasks = "/internal/api/v1/xcherry/notify-immediate-tasks"
const PathNotifyTimerTasks = "/internal/api/v1/xcherry/notify-timer-tasks"

type defaultSever struct {
	rootCtx context.Context
	cfg     config.Config
	logger  log.Logger

	engine     *gin.Engine
	httpServer *http.Server
	svc        Service
}

func NewDefaultAsyncServersWithGin(
	rootCtx context.Context,
	cfg config.Config,
	processStore persistence.ProcessStore,
	visibilityStore persistence.VisibilityStore,
	logger log.Logger,
) []Server {
	var servers []Server
	advertiseAddressToServerMap := map[string]Server{}

	serverAddresses := strings.Split(cfg.AsyncService.ClientAddress, ",")
	advertiseAddresses := []string{""}

	if cfg.AsyncService.Mode == config.AsyncServiceModeConsistentHashingCluster {
		advertiseAddresses = strings.Split(cfg.AsyncService.InternalHttpServer.ClusterAdvertiseAddresses, ",")
	}

	advertiseAddressToJoin := ""
	for i, serverAddress := range serverAddresses {
		server, err := NewDefaultAsyncServerWithGin(rootCtx, serverAddress, advertiseAddresses[i],
			advertiseAddressToJoin, cfg, processStore, visibilityStore, logger)
		if err != nil {
			logger.Fatal(fmt.Sprintf("Failed to create async service with serverAddress %s "+
				"and advertiseAddress %s", serverAddress, advertiseAddresses[i]), tag.Error(err))
		}

		servers = append(servers, server)

		advertiseAddressToServerMap[server.GetAdvertiseAddress()] = server

		if advertiseAddressToJoin == "" {
			advertiseAddressToJoin = server.GetAdvertiseAddress()
		}
	}

	for i := 0; i < cfg.AsyncService.Shard; i++ {
		advertiseAddress := servers[0].GetAdvertiseAddressFor(int32(i))

		server, ok := advertiseAddressToServerMap[advertiseAddress]
		if !ok {
			logger.Fatal(fmt.Sprintf("advertise address %s does not exist", advertiseAddress))
		}

		server.CreateQueues(int32(i), processStore)
	}

	return servers
}

func NewDefaultAsyncServerWithGin(
	rootCtx context.Context,
	serverAddress string,
	advertiseAddress string,
	advertiseAddressToJoin string,
	cfg config.Config,
	processStore persistence.ProcessStore,
	visibilityStore persistence.VisibilityStore,
	logger log.Logger,
) (Server, error) {
	engine := gin.Default()

	svc, err := NewAsyncServiceImpl(serverAddress, advertiseAddress, advertiseAddressToJoin, rootCtx, processStore, visibilityStore, cfg, logger)
	if err != nil {
		return nil, err
	}

	handler := newGinHandler(cfg, svc, logger)

	engine.POST(PathNotifyImmediateTasks, handler.NotifyImmediateTasks)
	engine.POST(PathNotifyTimerTasks, handler.NotifyTimerTasks)

	svrCfg := cfg.AsyncService.InternalHttpServer
	httpServer := &http.Server{
		Addr:              strings.Split(serverAddress, "//")[1],
		ReadTimeout:       svrCfg.ReadTimeout,
		WriteTimeout:      svrCfg.WriteTimeout,
		ReadHeaderTimeout: svrCfg.ReadHeaderTimeout,
		IdleTimeout:       svrCfg.IdleTimeout,
		MaxHeaderBytes:    svrCfg.MaxHeaderBytes,
		TLSConfig:         svrCfg.TLSConfig,
		Handler:           engine,
		BaseContext: func(listener net.Listener) context.Context {
			// for graceful shutdown
			return rootCtx
		},
	}

	return &defaultSever{
		rootCtx:    rootCtx,
		cfg:        cfg,
		logger:     logger,
		engine:     engine,
		httpServer: httpServer,
		svc:        svc,
	}, nil
}

func (s defaultSever) Start() error {

	go func() {
		err := s.httpServer.ListenAndServe()
		s.logger.Info("Internal Http Server for Async service is closed", tag.Error(err))
	}()

	return s.svc.Start()
}

func (s defaultSever) Stop(ctx context.Context) error {
	err1 := s.httpServer.Shutdown(ctx)
	err2 := s.svc.Stop(ctx)
	return multierr.Combine(err1, err2)
}

func (s defaultSever) CreateQueues(shardId int32, processStore persistence.ProcessStore) {
	s.svc.CreateQueues(shardId, processStore)
}

func (s defaultSever) GetServerAddress() string {
	return s.svc.GetServerAddress()
}

func (s defaultSever) GetAdvertiseAddress() string {
	return s.svc.GetAdvertiseAddress()
}

func (s defaultSever) GetAdvertiseAddressFor(shardId int32) string {
	return s.svc.GetAdvertiseAddressFor(shardId)
}
