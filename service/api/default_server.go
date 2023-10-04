package api

import (
	"context"
	"github.com/gin-gonic/gin"
	"github.com/xdblab/xdb/common/log"
	"github.com/xdblab/xdb/common/log/tag"
	"github.com/xdblab/xdb/config"
	"github.com/xdblab/xdb/persistence"
	"net"
	"net/http"
)

const PathStartProcessExecution = "/api/v1/xdb/service/process-execution/start"
const PathDescribeProcessExecution = "/api/v1/xdb/service/process-execution/describe"

type defaultSever struct {
	rootCtx    context.Context
	cfg        config.Config
	logger     log.Logger
	engine     *gin.Engine
	httpServer *http.Server
}

func NewDefaultAPIServerWithGin(
	rootCtx context.Context, cfg config.Config, store persistence.ProcessStore, logger log.Logger,
) Server {
	engine := gin.Default()

	handler := newGinHandler(cfg, store, logger)

	engine.POST(PathStartProcessExecution, handler.StartProcess)
	engine.POST(PathDescribeProcessExecution, handler.DescribeProcess)

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