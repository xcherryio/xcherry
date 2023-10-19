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
	"github.com/xdblab/xdb-apis/goapi/xdbapi"
	"math/rand"
	"sort"
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

	processor WorkerTaskProcessor

	// timers for polling worker tasks and dispatch to processor
	pollTimer TimerGate
	// timers for committing(deleting) completed worker tasks
	commitTimer TimerGate

	// tasksToCommitChan is the channel to receive completed tasks from processor
	tasksToCommitChan chan persistence.WorkerTask
	// currentReadCursor is the starting sequenceId(inclusive) to read next worker tasks
	currentReadCursor int64
	// pendingTaskSequenceToPage is the mapping from task sequence to page
	pendingTaskSequenceToPage map[int64]*workerTaskPage
	// completedPages is the pages that are ready to be committed
	completedPages []*workerTaskPage
}

type workerTaskPage struct {
	minTaskSequence int64
	maxTaskSequence int64
	pendingCount    int
}

func NewWorkerTaskQueueImpl(
	rootCtx context.Context, shardId int32, cfg config.Config, store persistence.ProcessStore,
	processor WorkerTaskProcessor, logger log.Logger,
) WorkerTaskQueue {
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
	// close timer to prevent goroutine leakage
	w.pollTimer.Close()
	w.commitTimer.Close()

	// a final attempt to commit the completed page
	return w.commitCompletedPages(ctx)
}

func (w *workerTaskQueueImpl) TriggerPollingTasks(_ xdbapi.NotifyWorkerTasksRequest) {
	w.pollTimer.Update(time.Now())
}

func (w *workerTaskQueueImpl) Start() error {
	qCfg := w.cfg.AsyncService.WorkerTaskQueue

	w.processor.AddWorkerTaskQueue(w.shardId, w.tasksToCommitChan)

	// fire immediately to make the first poll for the first page
	w.pollTimer.Update(time.Now())
	// schedule the first commit timer firing
	w.commitTimer.Update(w.getNextPollTime(qCfg.CommitInterval, qCfg.IntervalJitter))

	go func() {
		for {
			select {
			case <-w.pollTimer.FireChan():
				w.pollAndDispatchAndPrepareNext()
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

func (w *workerTaskQueueImpl) pollAndDispatchAndPrepareNext() {
	qCfg := w.cfg.AsyncService.WorkerTaskQueue

	resp, err := w.store.GetWorkerTasks(
		w.rootCtx, persistence.GetWorkerTasksRequest{
			ShardId:                w.shardId,
			StartSequenceInclusive: w.currentReadCursor,
			PageSize:               qCfg.PollPageSize,
		})

	if err != nil {
		w.logger.Error("failed at polling worker task", tag.Error(err))
		// schedule an earlier next poll
		w.pollTimer.Update(w.getNextPollTime(0, qCfg.IntervalJitter))
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
		w.logger.Debug("poll time succeeded", tag.Value(len(resp.Tasks)))

		w.pollTimer.Update(w.getNextPollTime(qCfg.MaxPollInterval, qCfg.IntervalJitter))

	}
}

func (w *workerTaskQueueImpl) commitCompletedPages(ctx context.Context) error {
	if len(w.completedPages) > 0 {
		// merge pages, e.g.,
		// [1, 2], [3, 4], [7, 8] -> [1, 4], [7, 8]
		sort.Slice(w.completedPages, func(i, j int) bool {
			return w.completedPages[i].minTaskSequence < w.completedPages[j].minTaskSequence
		})
		var pages []*workerTaskPage
		for _, page := range w.completedPages {
			if len(pages) == 0 || pages[len(pages)-1].maxTaskSequence+1 < page.minTaskSequence {
				pages = append(pages, page)
			} else {
				if pages[len(pages)-1].maxTaskSequence < page.maxTaskSequence {
					pages[len(pages)-1].maxTaskSequence = page.maxTaskSequence
				}
			}
		}

		w.completedPages = pages

		for idx, page := range w.completedPages {
			req := persistence.DeleteWorkerTasksRequest{
				ShardId:                  w.shardId,
				MinTaskSequenceInclusive: page.minTaskSequence,
				MaxTaskSequenceInclusive: page.maxTaskSequence,
			}
			w.logger.Debug("completing worker task page", tag.Value(req))

			err := w.store.DeleteWorkerTasks(ctx, req)
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
	} else {
		w.logger.Debug("no worker tasks to commit/delete")
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
