package engine

import (
	"context"
	"github.com/xdblab/xdb/persistence"
	"time"
)

// TaskQueue is task queue
type TaskQueue interface {
	Start() error
	TriggerPolling(pollTime time.Time)
	Stop(ctx context.Context) error
}

type WorkerTaskProcessor interface {
	Start() error
	Stop(context.Context) error

	GetTasksToProcessChan() chan<- persistence.WorkerTask

	AddWorkerTaskQueue(
		shardId int32, tasksToCommitChan chan<- persistence.WorkerTask, notifier LocalNotifyNewWorkerTask,
	) (alreadyExisted bool)
}
