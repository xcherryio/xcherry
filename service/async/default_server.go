package async

import (
	"context"
	"github.com/gin-gonic/gin"
	"github.com/xdblab/xdb/common/log"
	"github.com/xdblab/xdb/common/log/tag"
	"github.com/xdblab/xdb/config"
	"github.com/xdblab/xdb/engine/persistence"
	"net"
	"net/http"
)

const PathNotifyWorkerTask = "/api/v1/xdb/internal/notify-worker-task"

type defaultSever struct {
	rootCtx    context.Context
	cfg        config.Config
	logger     log.Logger
	engine     *gin.Engine
	httpServer *http.Server
	svc        Service
}

func NewDefaultAPIServerWithGin(
	rootCtx context.Context, cfg config.Config, store persistence.ProcessStore, logger log.Logger,
) Server {
	engine := gin.Default()

	svc := NewAsyncServiceImpl(rootCtx, store, cfg, logger)

	handler := newGinHandler(cfg, svc, logger)

	engine.GET(PathNotifyWorkerTask, handler.NotifyWorkerTask)

	svrCfg := cfg.AsyncService.InternalHttpServer
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
	return s.httpServer.Shutdown(ctx)
}
