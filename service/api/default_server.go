// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: BUSL-1.1

package api

import (
	"context"
	"github.com/gin-gonic/gin"
	"github.com/xcherryio/xcherry/common/log"
	"github.com/xcherryio/xcherry/common/log/tag"
	"github.com/xcherryio/xcherry/config"
	"github.com/xcherryio/xcherry/persistence"
	"net"
	"net/http"
)

const PathStartProcessExecution = "/api/v1/xcherry/service/process-execution/start"
const PathDescribeProcessExecution = "/api/v1/xcherry/service/process-execution/describe"
const PathStopProcessExecution = "/api/v1/xcherry/service/process-execution/stop"
const PathPublishToLocalQueue = "/api/v1/xcherry/service/process-execution/publish-to-local-queue"
const PathProcessExecutionRpc = "/api/v1/xcherry/service/process-execution/rpc"

type defaultSever struct {
	rootCtx context.Context
	cfg     config.Config
	logger  log.Logger

	engine     *gin.Engine
	httpServer *http.Server
}

func NewDefaultAPIServerWithGin(
	rootCtx context.Context, cfg config.Config, store persistence.ProcessStore, logger log.Logger,
) Server {
	engine := gin.Default()

	handler := newGinHandler(cfg, store, logger)

	engine.GET("/", func(c *gin.Context) {
		c.String(http.StatusOK, "Hello from xCherry server!")
	})
	engine.POST(PathStartProcessExecution, handler.StartProcess)
	engine.POST(PathDescribeProcessExecution, handler.DescribeProcess)
	engine.POST(PathStopProcessExecution, handler.StopProcess)
	engine.POST(PathPublishToLocalQueue, handler.PublishToLocalQueue)
	engine.POST(PathProcessExecutionRpc, handler.Rpc)

	svrCfg := cfg.ApiService.HttpServer
	httpServer := &http.Server{
		Addr:              svrCfg.Address,
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
	}
}

func (s defaultSever) Start() error {
	go func() {
		err := s.httpServer.ListenAndServe()
		s.logger.Info("Http Server for API service is closed", tag.Error(err))
	}()

	return nil
}

func (s defaultSever) Stop(ctx context.Context) error {
	return s.httpServer.Shutdown(ctx)
}
