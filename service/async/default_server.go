// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: Apache-2.0

package async

import (
	"context"
	"github.com/gin-gonic/gin"
	"github.com/xcherryio/xcherry/common/log"
	"github.com/xcherryio/xcherry/common/log/tag"
	"github.com/xcherryio/xcherry/config"
	"github.com/xcherryio/xcherry/memberlist"
	"github.com/xcherryio/xcherry/persistence"
	"go.uber.org/multierr"
	"net"
	"net/http"
	"strconv"
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
	rootCtx context.Context, cfg config.Config, store persistence.ProcessStore, logger log.Logger,
) ([]Server, []*memberlist.EventDelegate) {
	var servers []Server
	var eventDelegates []*memberlist.EventDelegate

	addresses := strings.Split(cfg.AsyncService.InternalHttpServer.Address, ",")
	advertisePorts := strings.Split(cfg.AsyncService.InternalHttpServer.AdvertisePort, ",")

	for i, address := range addresses {
		servers = append(servers, NewDefaultAsyncServerWithGin(rootCtx, cfg, address, store, logger))

		parts := strings.Split(address, ":")
		bindPort, err := strconv.Atoi(parts[len(parts)-1])
		if err != nil {
			logger.Fatal("Failed to get the port of "+address, tag.Error(err))
		}

		advertisePort, err := strconv.Atoi(advertisePorts[i])
		if err != nil {
			logger.Fatal("Failed to get the advertisePort from "+advertisePorts[i], tag.Error(err))
		}

		eventDelegate, err := memberlist.NewMember(address, bindPort, advertisePort, addresses[0])
		if err != nil {
			logger.Fatal("Failed to create member for "+address, tag.Error(err))
		}

		eventDelegates = append(eventDelegates, eventDelegate)
	}

	return servers, eventDelegates
}

func NewDefaultAsyncServerWithGin(
	rootCtx context.Context, cfg config.Config, address string, store persistence.ProcessStore, logger log.Logger,
) Server {

	engine := gin.Default()

	svc := NewAsyncServiceImpl(rootCtx, store, cfg, logger)

	handler := newGinHandler(cfg, svc, logger)

	engine.POST(PathNotifyImmediateTasks, handler.NotifyImmediateTasks)
	engine.POST(PathNotifyTimerTasks, handler.NotifyTimerTasks)

	svrCfg := cfg.AsyncService.InternalHttpServer
	httpServer := &http.Server{
		Addr:              address,
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
	}
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
