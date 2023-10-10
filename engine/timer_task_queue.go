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
	"container/heap"
	"context"
	"github.com/xdblab/xdb-apis/goapi/xdbapi"
	"math/rand"
	"time"

	"github.com/xdblab/xdb/common/log"
	"github.com/xdblab/xdb/common/log/tag"
	"github.com/xdblab/xdb/config"
	"github.com/xdblab/xdb/persistence"
)

type timerTaskQueueImpl struct {
	shardId int32
	store   persistence.ProcessStore
	logger  log.Logger
	rootCtx context.Context
	cfg     config.Config

	processor TimerTaskProcessor

	// the timer for next preload (by interval duration)
	nextPreloadTimer TimerGate
	// the timer for next firing of the loaded timers
	nextFiringTimer TimerGate

	// tracks the max task sequence that has been loaded
	// so that the triggered polling can start from the next sequence
	currMaxLoadedTaskSequence int64
	// similarly, this tracks the max task timestamp that has been loaded
	// so that the triggerred polling can skip the polling if the new tasks
	// to poll are beyond the max timestamp -- because the next preload will
	// poll it anyway.
	currMaxLoadedTaskTimestamp int64

	// the timers from the current preload, sorted by fire time
	// It is using heap for perf because there could be new timers coming later
	// The timer will be popped out when it is fired, and sent to the processor.
	remainingToFireTimersHeap TimerTaskPriorityQueue

	// this tracks the fired timer that are waiting for completed by processor.
	// The key is the timer sequence. When all timers are completed and no timers in remainingToFireTimersHeap,
	// it will trigger the next preload when the nextPreloadTimer fires
	firedToCompleteTimerSequenceMap map[int64]struct{}

	// tasksCompletionChan is the channel to receive completed tasks from processor.
	// Receiving the completion signal so that it can remove the timer from firedToCompleteTimerSequenceMap
	// Note that differently from worker task, this timer queue doesn't do batch deletion for "committing".
	// It relies on the processor to delete the task during processing.
	tasksCompletionChan chan persistence.TimerTask

	// the channel to receive trigger of polling for newly created timers.
	// The queue will check if the new timers are within the currentMaxLoadedTaskTimestamp, and if so,
	// it will load the new timers and add to the remainingToFireTimersHeap
	triggeredPollingChan chan xdbapi.NotifyTimerTasksRequest
}

func NewTimerTaskQueueImpl(
	rootCtx context.Context, shardId int32, cfg config.Config, store persistence.ProcessStore,
	processor TimerTaskProcessor, logger log.Logger,
) TimerTaskQueue {
	qCfg := cfg.AsyncService.TimerTaskQueue

	return &timerTaskQueueImpl{
		shardId: shardId,
		store:   store,
		logger:  logger.WithTags(tag.Shard(shardId)),
		rootCtx: rootCtx,
		cfg:     cfg,

		processor: processor,

		nextPreloadTimer: NewLocalTimerGate(logger),
		nextFiringTimer:  NewLocalTimerGate(logger),

		currMaxLoadedTaskSequence:  0,
		currMaxLoadedTaskTimestamp: 0,

		remainingToFireTimersHeap:       nil,
		firedToCompleteTimerSequenceMap: make(map[int64]struct{}),
		tasksCompletionChan:             make(chan persistence.TimerTask, qCfg.ProcessorBufferSize),
		triggeredPollingChan:            make(chan xdbapi.NotifyTimerTasksRequest, qCfg.TriggerNotificationBufferSize),
	}
}

func (w *timerTaskQueueImpl) Stop(ctx context.Context) error {
	// close timer to prevent goroutine leakage
	w.nextPreloadTimer.Close()
	w.nextFiringTimer.Close()

	return nil
}

func (w *timerTaskQueueImpl) TriggerPollingTasks(req xdbapi.NotifyTimerTasksRequest) {
	if req.ShardId != w.shardId {
		panic("shardId doesn't match")
	}
	w.triggeredPollingChan <- req
}

func (w *timerTaskQueueImpl) Start() error {
	w.processor.AddTimerTaskQueue(w.shardId, w.tasksCompletionChan)

	w.nextPreloadTimer.Update(time.Now()) // fire immediately to make the first poll

	go func() {
		for {
			select {
			case <-w.nextPreloadTimer.FireChan():
				if w.shouldLoadNextBatch() {
					w.loadAndDispatchAndPrepareNext()
				}
			case <-w.nextFiringTimer.FireChan():
				w.sendFiredTimerToProcessor()
			case task, ok := <-w.tasksCompletionChan:
				if ok {
					delete(w.firedToCompleteTimerSequenceMap, *task.TaskSequence)
					if w.shouldLoadNextBatch() {
						w.loadAndDispatchAndPrepareNext()
					}
				}
			case req, ok := <-w.triggeredPollingChan:
				if ok {
					// drain all the requests to poll in batch
					reqs := w.drainAllNotifyRequests(&req)
					w.triggeredPolling(reqs)
				}
			case <-w.rootCtx.Done():
				w.logger.Info("processor is being closed")
				return
			}
		}
	}()
	return nil
}

func (w *timerTaskQueueImpl) getNextPollTime(interval, jitter time.Duration) time.Time {
	jitterD := time.Duration(rand.Int63n(int64(jitter)))
	return time.Now().Add(interval).Add(jitterD)
}

// preload the next page of timers and dispatch them to processor
// and prepare the next preload(update the preloadTimer and reset the flag)
func (w *timerTaskQueueImpl) loadAndDispatchAndPrepareNext() {

	// as we are loading next page, we can drain all the pending requests
	// because the new timers will be loaded anyway
	w.drainAllNotifyRequests(nil)

	qCfg := w.cfg.AsyncService.TimerTaskQueue
	maxWindowTime := w.getNextPollTime(qCfg.MaxTimerPreloadLookAhead, qCfg.IntervalJitter)
	w.nextPreloadTimer.Update(maxWindowTime)

	resp, err := w.store.GetTimerTasksUpToTimestamp(
		w.rootCtx, persistence.GetTimerTasksRequest{
			ShardId:                          w.shardId,
			MaxFireTimestampSecondsInclusive: maxWindowTime.Unix(),
			PageSize:                         qCfg.MaxPreloadPageSize,
		})

	if err != nil {
		w.logger.Error("failed at loading timer task, will retry", tag.Error(err))
		// schedule an earlier next poll
		w.nextPreloadTimer.Update(w.getNextPollTime(0, qCfg.IntervalJitter))
	} else {
		if len(resp.Tasks) > 0 {
			w.currMaxLoadedTaskTimestamp = resp.MaxFireTimestampSecondsInclusive
			w.currMaxLoadedTaskSequence = resp.MaxSequenceInclusive

			w.remainingToFireTimersHeap = NewTimerTaskPriorityQueue(resp.Tasks)

			task0 := w.remainingToFireTimersHeap[0]
			w.nextFiringTimer.Update(time.Unix(task0.FireTimestampSeconds, 0))
		}
	}
}

func (w *timerTaskQueueImpl) drainAllNotifyRequests(initReq *xdbapi.NotifyTimerTasksRequest) []xdbapi.NotifyTimerTasksRequest {
	var reqs []xdbapi.NotifyTimerTasksRequest
	if initReq == nil {
		reqs = make([]xdbapi.NotifyTimerTasksRequest, 0, len(w.triggeredPollingChan))
	} else {
		reqs = make([]xdbapi.NotifyTimerTasksRequest, 0, len(w.triggeredPollingChan)+1)
		filteredReq := w.filterNotifyRequest(*initReq)
		if filteredReq != nil {
			reqs = append(reqs, *filteredReq)
		}
	}

	for len(w.triggeredPollingChan) > 0 {
		req, ok := <-w.triggeredPollingChan
		if ok {
			filteredReq := w.filterNotifyRequest(req)
			if filteredReq != nil {
				reqs = append(reqs, *filteredReq)
			}
		}
	}
	return reqs
}

func (w *timerTaskQueueImpl) shouldLoadNextBatch() bool {
	if (!w.nextPreloadTimer.IsActive() && // this means the preload timer has fired
		len(w.remainingToFireTimersHeap) == 0) && // this means all the timer tasks in the heap have been fired and sent to processor
		len(w.firedToCompleteTimerSequenceMap) == 0 { // this means all the fired timers have been completed
		return true
	}
	return false
}

func (w *timerTaskQueueImpl) sendFiredTimerToProcessor() {
	for {
		if len(w.remainingToFireTimersHeap) == 0 {
			break
		}
		minTask := w.remainingToFireTimersHeap[0]
		if minTask.FireTimestampSeconds <= time.Now().Unix() {
			heap.Pop(&w.remainingToFireTimersHeap)
			w.processor.GetTasksToProcessChan() <- *minTask
		} else {
			w.nextFiringTimer.Update(time.Unix(minTask.FireTimestampSeconds, 0))
			break
		}
	}
}

func (w *timerTaskQueueImpl) triggeredPolling(reqs []xdbapi.NotifyTimerTasksRequest) {
	if len(reqs) == 0 {
		return // nothing to poll
	}

	resp, err := w.store.GetTimerTasksForTimestamps(
		w.rootCtx, persistence.GetTimerTasksForTimestampsRequest{
			ShardId:              w.shardId,
			MinSequenceInclusive: w.currMaxLoadedTaskSequence + 1,
			DetailedRequests:     reqs,
		})

	if err != nil {
		w.logger.Error("failed at triggered polling timer task, will not retry. "+
			"The new timers will be waiting for next preload", tag.Error(err))
		// Give up on error as this notification mechanism is an optimization.
		// The next preload will poll them anyway
	} else {
		if len(resp.Tasks) > 0 {
			// update the max loaded sequence so that next time it won't load the same tasks
			// currMaxLoadedTaskTimestamp is not updated because the new tasks won't have a bigger timestamp
			w.currMaxLoadedTaskSequence = resp.MaxSequenceInclusive

			// add the new tasks into the heap
			for _, task := range resp.Tasks {
				heap.Push(&w.remainingToFireTimersHeap, &task)
			}

			minTimestamp := time.Unix(resp.MinFireTimestampSecondsInclusive, 0)
			if !w.nextFiringTimer.IsActive() || w.nextFiringTimer.FireAfter(minTimestamp) {
				// update the next firing timer if
				// 1. the nextFireTimer is not active(meaning there wasn't any more tasks to fire)
				// 2. any of the new tasks are earlier than the current min
				w.nextFiringTimer.Update(minTimestamp)
			}
		}
	}
}

// filterNotifyRequest filters out the fire timestamps that are outside current preload time window
func (w *timerTaskQueueImpl) filterNotifyRequest(req xdbapi.NotifyTimerTasksRequest) *xdbapi.NotifyTimerTasksRequest {
	var filteredFireTimestamps []int64
	for _, ts := range req.FireTimestamps {
		if ts <= w.currMaxLoadedTaskTimestamp {
			filteredFireTimestamps = append(filteredFireTimestamps, ts)
		}
	}
	if len(filteredFireTimestamps) == 0 {
		return nil
	}
	req.FireTimestamps = filteredFireTimestamps
	return &req
}
