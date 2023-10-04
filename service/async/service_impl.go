package async

import (
	"context"
	"fmt"
	"github.com/xdblab/xdb/common/log"
	"github.com/xdblab/xdb/common/log/tag"
	"github.com/xdblab/xdb/config"
	"github.com/xdblab/xdb/engine"
	"github.com/xdblab/xdb/persistence"
	"time"
)

type asyncService struct {
	rootCtx context.Context

	workerTaskQueue     engine.TaskQueue
	workerTaskProcessor engine.WorkerTaskProcessor

	cfg    config.Config
	logger log.Logger
}

func NewAsyncServiceImpl(
	rootCtx context.Context, store persistence.ProcessStore, cfg config.Config, logger log.Logger,
) Service {
	workerTaskProcessor := engine.NewWorkerTaskConcurrentProcessor(rootCtx, cfg, store, logger)

	// TODO for config.AsyncServiceModeConsistentHashingCluster
	// the worker queue will be created dynamically
	workerTaskQueue := engine.NewWorkerTaskProcessorSQLImpl(
		rootCtx, persistence.DefaultShardId, cfg, store, workerTaskProcessor, logger)

	return &asyncService{
		workerTaskQueue:     workerTaskQueue,
		workerTaskProcessor: workerTaskProcessor,

		rootCtx: rootCtx,
		cfg:     cfg,
		logger:  logger,
	}
}

func (a asyncService) Start() error {
	err := a.workerTaskProcessor.Start()
	if err != nil {
		a.logger.Error("fail to start worker task processor", tag.Error(err))
		return err
	}
	return a.workerTaskQueue.Start()
}

func (a asyncService) NotifyPollingWorkerTask(shardId int32) error {
	if shardId != persistence.DefaultShardId {
		return fmt.Errorf("the shardId %v is not owned by this instance", shardId)
	}
	a.workerTaskQueue.TriggerPolling(time.Now())
	return nil
}

func (a asyncService) Stop(ctx context.Context) error {
	err1 := a.workerTaskQueue.Stop(ctx)
	err2 := a.workerTaskProcessor.Stop(ctx)
	// TODO use multi error library
	if err1 != nil {
		return err1
	}
	if err2 != nil {
		return err2
	}
	return nil
}
