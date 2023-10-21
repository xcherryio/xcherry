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
	"github.com/gin-gonic/gin"
	"github.com/xdblab/xdb/common/log"
	"github.com/xdblab/xdb/common/log/tag"
	"github.com/xdblab/xdb/config"
	"github.com/xdblab/xdb/persistence"
	"go.uber.org/multierr"
	"net"
	"net/http"
)

const PathNotifyImmediateTasks = "/internal/api/v1/xdb/notify-immediate-tasks"
const PathNotifyTimerTasks = "/internal/api/v1/xdb/notify-timer-tasks"

type defaultSever struct {
	rootCtx context.Context
	cfg     config.Config
	logger  log.Logger

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

	engine.POST(PathNotifyImmediateTasks, handler.NotifyImmediateTasks)
	engine.POST(PathNotifyTimerTasks, handler.NotifyTimerTasks)

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
	err1 := s.httpServer.Shutdown(ctx)
	err2 := s.svc.Stop(ctx)
	return multierr.Combine(err1, err2)
}
