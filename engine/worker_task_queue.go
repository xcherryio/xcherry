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

package engine

import (
	"context"
	"math/rand"
	"time"

	"github.com/xdblab/xdb/common/log"
	"github.com/xdblab/xdb/common/log/tag"
	"github.com/xdblab/xdb/config"
	"github.com/xdblab/xdb/persistence"
)

type workerTaskQueueImpl struct {
	shardId int32
	store   persistence.ProcessStore
	logger  log.Logger
	rootCtx context.Context
	cfg     config.Config

	pollTimer         TimerGate
	commitTimer       TimerGate
	processor         WorkerTaskProcessor
	tasksToCommitChan chan persistence.WorkerTask
	// the starting sequenceId(inclusive) to read worker tasks
	currentReadCursor         int64
	pendingTaskSequenceToPage map[int64]*workerTaskPage
	completedPages            []*workerTaskPage
}

type workerTaskPage struct {
	minTaskSequence int64
	maxTaskSequence int64
	pendingCount    int
}

func NewWorkerTaskQueueImpl(
	rootCtx context.Context, shardId int32, cfg config.Config, store persistence.ProcessStore,
	processor WorkerTaskProcessor, logger log.Logger,
) TaskQueue {
	qCfg := cfg.AsyncService.WorkerTaskQueue

	return &workerTaskQueueImpl{
		shardId: shardId,
		store:   store,
		logger:  logger.WithTags(tag.Shard(shardId)),
		rootCtx: rootCtx,
		cfg:     cfg,

		pollTimer:                 NewLocalTimerGate(logger),
		commitTimer:               NewLocalTimerGate(logger),
		processor:                 processor,
		tasksToCommitChan:         make(chan persistence.WorkerTask, qCfg.ProcessorBufferSize),
		currentReadCursor:         0,
		pendingTaskSequenceToPage: make(map[int64]*workerTaskPage),
	}
}

func (w *workerTaskQueueImpl) Stop(ctx context.Context) error {
	// a final attempt to commit the completed page
	return w.commitCompletedPages(ctx)
}

func (w *workerTaskQueueImpl) TriggerPolling(pollTime time.Time) {
	w.pollTimer.Update(pollTime)
}

type LocalNotifyNewWorkerTask func(pollTime time.Time)

func (w *workerTaskQueueImpl) Start() error {
	qCfg := w.cfg.AsyncService.WorkerTaskQueue

	w.processor.AddWorkerTaskQueue(w.shardId, w.tasksToCommitChan, w.TriggerPolling)

	w.pollTimer.Update(time.Now()) // fire immediately to make the first poll for the first page

	go func() {
		for {
			select {
			case <-w.pollTimer.FireChan():
				w.pollAndDispatch()
				w.pollTimer.Update(w.getNextPollTime(qCfg.MaxPollInterval, qCfg.IntervalJitter))
			case <-w.commitTimer.FireChan():
				_ = w.commitCompletedPages(w.rootCtx)
				w.commitTimer.Update(w.getNextPollTime(qCfg.CommitInterval, qCfg.IntervalJitter))
			case task, ok := <-w.tasksToCommitChan:
				if ok {
					w.receiveCompletedTask(task)
				}
			case <-w.rootCtx.Done():
				w.logger.Info("processor is being closed")
				return
			}
		}
	}()
	return nil
}

func (w *workerTaskQueueImpl) getNextPollTime(interval, jitter time.Duration) time.Time {
	jitterD := time.Duration(rand.Int63n(int64(jitter)))
	return time.Now().Add(interval).Add(jitterD)
}

func (w *workerTaskQueueImpl) pollAndDispatch() {
	qCfg := w.cfg.AsyncService.WorkerTaskQueue

	resp, err := w.store.GetWorkerTasks(
		w.rootCtx, persistence.GetWorkerTasksRequest{
			ShardId:                w.shardId,
			StartSequenceInclusive: w.currentReadCursor,
			PageSize:               qCfg.PollPageSize,
		})

	if err != nil {
		w.logger.Error("failed at polling worker task", tag.Error(err))
		// TODO maybe schedule an earlier next time?
	} else {
		if len(resp.Tasks) > 0 {
			w.currentReadCursor = resp.MaxSequenceInclusive + 1

			page := &workerTaskPage{
				minTaskSequence: resp.MinSequenceInclusive,
				maxTaskSequence: resp.MaxSequenceInclusive,
				pendingCount:    len(resp.Tasks),
			}

			for _, task := range resp.Tasks {
				w.processor.GetTasksToProcessChan() <- task
				w.pendingTaskSequenceToPage[*task.TaskSequence] = page
			}
		}
	}
}

func (w *workerTaskQueueImpl) commitCompletedPages(ctx context.Context) error {
	if len(w.completedPages) > 0 {
		// TODO optimize by combining into single query (as long as it won't exceed certain limit)
		for idx, page := range w.completedPages {
			err := w.store.DeleteWorkerTasks(ctx, persistence.DeleteWorkerTasksRequest{
				ShardId:                  w.shardId,
				MinTaskSequenceInclusive: page.minTaskSequence,
				MaxTaskSequenceInclusive: page.maxTaskSequence,
			})
			if err != nil {
				w.logger.Error("failed at deleting completed worker tasks", tag.Error(err))
				// fix the completed pages -- current page to the end
				// return and wait for next time
				w.completedPages = w.completedPages[idx:]
				return err
			}
		}
		// reset to empty
		w.completedPages = nil
	}
	return nil
}

func (w *workerTaskQueueImpl) receiveCompletedTask(task persistence.WorkerTask) {
	page := w.pendingTaskSequenceToPage[*task.TaskSequence]
	delete(w.pendingTaskSequenceToPage, *task.TaskSequence)

	page.pendingCount--
	if page.pendingCount == 0 {
		w.completedPages = append(w.completedPages, page)
	}
}
