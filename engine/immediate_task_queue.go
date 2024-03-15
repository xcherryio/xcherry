// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"context"
	"fmt"
	"github.com/xcherryio/apis/goapi/xcapi"
	"github.com/xcherryio/xcherry/persistence/data_models"
	"math/rand"
	"sort"
	"time"

	"github.com/xcherryio/xcherry/common/log"
	"github.com/xcherryio/xcherry/common/log/tag"
	"github.com/xcherryio/xcherry/config"
	"github.com/xcherryio/xcherry/persistence"
)

type immediateTaskQueueImpl struct {
	shardId int32
	store   persistence.ProcessStore
	logger  log.Logger
	rootCtx context.Context
	cfg     config.Config

	processor ImmediateTaskProcessor

	// timers for polling immediate tasks and dispatch to processor
	pollTimer TimerGate

	// tasksToCommitChan is the channel to receive completed tasks from processor
	tasksToCommitChan chan data_models.ImmediateTask
	// currentReadCursor is the starting sequenceId(inclusive) to read next immediate tasks
	currentReadCursor int64
	// pendingTaskSequenceToPage is the mapping from task sequence to page
	pendingTaskSequenceToPage map[int64]*immediateTaskPage
	// completedPages is the pages that are ready to be committed
	completedPages []*immediateTaskPage
}

type immediateTaskPage struct {
	minTaskSequence int64
	maxTaskSequence int64
	pendingCount    int
}

func NewImmediateTaskQueueImpl(
	rootCtx context.Context, shardId int32, cfg config.Config, store persistence.ProcessStore,
	processor ImmediateTaskProcessor, logger log.Logger,
) ImmediateTaskQueue {
	qCfg := cfg.AsyncService.ImmediateTaskQueue

	return &immediateTaskQueueImpl{
		shardId: shardId,
		store:   store,
		logger:  logger.WithTags(tag.Shard(shardId)),
		rootCtx: rootCtx,
		cfg:     cfg,

		pollTimer:                 NewLocalTimerGate(logger),
		processor:                 processor,
		tasksToCommitChan:         make(chan data_models.ImmediateTask, qCfg.ProcessorBufferSize),
		currentReadCursor:         0,
		pendingTaskSequenceToPage: make(map[int64]*immediateTaskPage),
	}
}

func (w *immediateTaskQueueImpl) Stop(ctx context.Context) error {
	// close timer to prevent goroutine leakage
	w.pollTimer.Close()
	return nil
}

func (w *immediateTaskQueueImpl) TriggerPollingTasks(_ xcapi.NotifyImmediateTasksRequest) {
	w.pollTimer.Update(time.Now())
}

func (w *immediateTaskQueueImpl) Start() error {
	w.processor.AddImmediateTaskQueue(w.shardId, w.tasksToCommitChan)

	// fire immediately to make the first poll for the first page
	w.pollTimer.Update(time.Now())

	go func() {
		for {
			select {
			case <-w.pollTimer.FireChan():
				w.pollAndDispatchAndPrepareNext()
			case task, ok := <-w.tasksToCommitChan:
				if ok {
					w.receiveCompletedTask(task)
				}
			case <-w.rootCtx.Done():
				w.logger.Info(fmt.Sprintf("immediateTaskQueue %d is being closed", w.shardId))
				return
			}
		}
	}()
	return nil
}

func (w *immediateTaskQueueImpl) getNextPollTime(interval, jitter time.Duration) time.Time {
	jitterD := time.Duration(rand.Int63n(int64(jitter)))
	return time.Now().Add(interval).Add(jitterD)
}

func (w *immediateTaskQueueImpl) pollAndDispatchAndPrepareNext() {
	qCfg := w.cfg.AsyncService.ImmediateTaskQueue

	resp, err := w.store.GetImmediateTasks(
		w.rootCtx, data_models.GetImmediateTasksRequest{
			ShardId:                w.shardId,
			StartSequenceInclusive: w.currentReadCursor,
			PageSize:               qCfg.PollPageSize,
		})

	if err != nil {
		w.logger.Error("failed at polling immediate tasks", tag.Error(err))
		// schedule an earlier next poll
		w.pollTimer.Update(w.getNextPollTime(0, qCfg.IntervalJitter))
	} else {
		if len(resp.Tasks) > 0 {
			w.currentReadCursor = resp.MaxSequenceInclusive + 1

			page := &immediateTaskPage{
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

func mergeImmediateTaskPages(workTaskPages []*immediateTaskPage) []*immediateTaskPage {
	// merge pages, e.g.,
	// [1, 2], [3, 4], [7, 8] -> [1, 4], [7, 8]
	sort.Slice(workTaskPages, func(i, j int) bool {
		return workTaskPages[i].minTaskSequence < workTaskPages[j].minTaskSequence
	})

	var pages []*immediateTaskPage
	for _, page := range workTaskPages {
		if len(pages) == 0 || pages[len(pages)-1].maxTaskSequence+1 < page.minTaskSequence {
			pages = append(pages, page)
		} else {
			if pages[len(pages)-1].maxTaskSequence < page.maxTaskSequence {
				page = &immediateTaskPage{
					minTaskSequence: pages[len(pages)-1].minTaskSequence,
					maxTaskSequence: page.maxTaskSequence,
				}
				pages[len(pages)-1] = page
			}
		}
	}

	return pages
}

func (w *immediateTaskQueueImpl) receiveCompletedTask(task data_models.ImmediateTask) {
	page := w.pendingTaskSequenceToPage[*task.TaskSequence]
	delete(w.pendingTaskSequenceToPage, *task.TaskSequence)

	page.pendingCount--
	if page.pendingCount == 0 {
		w.completedPages = append(w.completedPages, page)
	}
}
