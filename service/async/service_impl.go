package async

import (
	"context"
	"github.com/xdblab/xdb/common/log"
	"github.com/xdblab/xdb/config"
	"github.com/xdblab/xdb/engine"
	"github.com/xdblab/xdb/engine/persistence"
)

type asyncService struct {
	rootCtx context.Context
	cfg     config.Config
	logger  log.Logger
}

func NewAsyncServiceImpl(
	rootCtx context.Context, store persistence.ProcessStore, cfg config.Config, logger log.Logger,
) Service {
	workerQueue := engine.NewWorkerTaskProcessorSQLImpl(rootCtx, persistence.DefaultShardId, cfg, store logger)
	return &asyncService{
		rootCtx: rootCtx,
		cfg:     cfg,
		logger:  logger,
	}
}

func (a asyncService) Start() error {
	//TODO implement me
	panic("implement me")
}

func (a asyncService) NotifyPollingWorkerTask(shardId int32) {
	//TODO implement me
	panic("implement me")
}

func (a asyncService) Stop(ctx context.Context) error {
	//TODO implement me
	panic("implement me")
}
