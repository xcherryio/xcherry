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
	"fmt"
	"github.com/xdblab/xdb-apis/goapi/xdbapi"
	"math"
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

	// Note that differently from worker task, this timer queue doesn't do batch deletion for "committing".
	// It relies on the processor to delete the task during processing.
	// Therefore, the timer queue will move on once handling off the task to processor.
	processor TimerTaskProcessor

	// the timer for next preload (by interval duration)
	nextPreloadTimer TimerGate
	// the timer for next firing of the loaded timers
	nextFiringTimer TimerGate
	// the timer to trigger a polling for newly created timers
	triggerPollTimer TimerGate

	// tracks the max task sequence that has been loaded
	// so that the triggered polling can start from the next sequence
	currMaxLoadedTaskSequence int64
	// similarly, this tracks the timeframe that the current preload has loaded
	// so that the triggerred polling can skip the notifications if the new tasks
	// to poll are beyond the timestamp -- because the next preload will
	// poll them anyway.
	currWindowTimestamp int64

	// the timers from the current preload, sorted by fire time
	// It is using heap for perf because there could be new timers coming later
	// The timer will be popped out when it is fired, and sent to the processor.
	remainingToFireTimersHeap TimerTaskPriorityQueue

	// the channel to receive trigger of polling for newly created timers.
	// The queue will check if the new timers are within the currentMaxLoadedTaskTimestamp, and if so,
	// it will load the new timers and add to the remainingToFireTimersHeap
	triggeredPollingChan chan xdbapi.NotifyTimerTasksRequest

	// the current pending requests to poll
	currentNotifyRequests []xdbapi.NotifyTimerTasksRequest
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
		triggerPollTimer: NewLocalTimerGate(logger),

		currMaxLoadedTaskSequence: 0,
		currWindowTimestamp:       0,

		remainingToFireTimersHeap: nil,
		triggeredPollingChan:      make(chan xdbapi.NotifyTimerTasksRequest, qCfg.TriggerNotificationBufferSize),
		currentNotifyRequests:     nil,
	}
}

func (w *timerTaskQueueImpl) Stop(ctx context.Context) error {
	// close timer to prevent goroutine leakage
	w.nextPreloadTimer.Close()
	w.nextFiringTimer.Close()
	w.triggerPollTimer.Close()

	return nil
}

func (w *timerTaskQueueImpl) TriggerPollingTasks(req xdbapi.NotifyTimerTasksRequest) {
	if req.ShardId != w.shardId {
		panic("shardId doesn't match")
	}
	w.triggeredPollingChan <- req
}

func (w *timerTaskQueueImpl) Start() error {
	w.processor.AddTimerTaskQueue(w.shardId)

	w.nextPreloadTimer.Update(time.Now()) // fire immediately to make the first poll

	go func() {
		for {
			select {
			case <-w.nextPreloadTimer.FireChan():
				if w.shouldLoadNextWindowBatch() {
					w.loadAndDispatchAndPrepareNext()
				}
			case <-w.nextFiringTimer.FireChan():
				w.sendAllFiredTimerTasksToProcessor()
				if w.shouldLoadNextWindowBatch() {
					w.loadAndDispatchAndPrepareNext()
				}
			case req, ok := <-w.triggeredPollingChan:
				if ok {
					// drain all the requests to poll in batch
					w.drainAllNotifyRequests(&req)
				}
			case <-w.triggerPollTimer.FireChan():
				w.triggeredPolling()
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

	// as we are loading next page, we can drain all the pending requests and stop triggerPolling
	// because the new timers will be loaded anyway
	w.drainAllNotifyRequests(nil)
	w.triggerPollTimer.Stop()

	qCfg := w.cfg.AsyncService.TimerTaskQueue
	maxWindowTime := w.getNextPollTime(qCfg.MaxTimerPreloadLookAhead, qCfg.IntervalJitter)

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
			if *resp.FullPage {
				// there are a full page of timers, the server is busy,
				//truncate the window so that we can load next page earlier
				maxWindowTime = time.Unix(resp.MaxFireTimestampSecondsInclusive, 0)
			}

			w.remainingToFireTimersHeap = NewTimerTaskPriorityQueue(resp.Tasks)
			minTask := w.remainingToFireTimersHeap[0]
			w.nextFiringTimer.Update(time.Unix(minTask.FireTimestampSeconds, 0))
		}

		w.nextPreloadTimer.Update(maxWindowTime)
		w.currWindowTimestamp = maxWindowTime.Unix()
		w.currMaxLoadedTaskSequence = resp.MaxSequenceInclusive
	}
	w.logger.Debug("load and dispatch timer tasks succeeded with new currWindowTimestamp",
		tag.Value(len(resp.Tasks)), tag.UnixTimestamp(w.currWindowTimestamp))
}

func (w *timerTaskQueueImpl) drainAllNotifyRequests(initReq *xdbapi.NotifyTimerTasksRequest) {
	minTimestamp := int64(math.MaxInt64)

	if initReq != nil {
		filteredReq := w.filterNotifyRequest(*initReq, &minTimestamp)
		if filteredReq != nil {
			w.currentNotifyRequests = append(w.currentNotifyRequests, *filteredReq)
		}
	}

	for len(w.triggeredPollingChan) > 0 {
		req, ok := <-w.triggeredPollingChan
		if ok {
			filteredReq := w.filterNotifyRequest(req, &minTimestamp)
			if filteredReq != nil {
				w.currentNotifyRequests = append(w.currentNotifyRequests, *filteredReq)
			}
		}
	}

	minTime := time.Unix(minTimestamp, 0)
	if minTimestamp != math.MaxInt64 {
		if w.triggerPollTimer.InactiveOrFireAfter(minTime) {
			w.triggerPollTimer.Update(minTime)
		}
	}
	w.logger.Debug("notify is received and drained, current min:", tag.Value(minTime), tag.Value(minTimestamp))
}

func (w *timerTaskQueueImpl) shouldLoadNextWindowBatch() bool {
	if !w.nextPreloadTimer.IsActive() && // this means the preload timer has fired
		len(w.remainingToFireTimersHeap) == 0 { // this means all the timer tasks in the heap have been fired and sent to processor
		return true
	}
	w.logger.Debug(fmt.Sprintf("shouldLoadNextWindowBatch is false, %v, %v",
		w.nextPreloadTimer.IsActive(), len(w.remainingToFireTimersHeap)))
	return false
}

func (w *timerTaskQueueImpl) sendAllFiredTimerTasksToProcessor() {
	if len(w.remainingToFireTimersHeap) == 0 {
		w.logger.Error("remainingToFireTimersHeap is empty, something wrong in the logic")
		return
	}

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

func (w *timerTaskQueueImpl) triggeredPolling() {
	if len(w.currentNotifyRequests) == 0 {
		w.logger.Error("triggered polling but no pending requests, something wrong in the logic")
		return // nothing to poll
	}

	resp, err := w.store.GetTimerTasksForTimestamps(
		w.rootCtx, persistence.GetTimerTasksForTimestampsRequest{
			ShardId:              w.shardId,
			MinSequenceInclusive: w.currMaxLoadedTaskSequence + 1,
			DetailedRequests:     w.currentNotifyRequests,
		})

	if err != nil {
		w.logger.Error("failed at triggered polling timer task, will not retry. "+
			"The new timers will be waiting for next preload", tag.Error(err))
		// Give up on error as this notification mechanism is an optimization.
		// The next preload will poll them anyway
	} else {
		if len(resp.Tasks) > 0 {
			// update the max loaded sequence so that next time it won't load the same tasks
			// currWindowTimestamp is not updated because the new tasks won't have a bigger timestamp
			w.currMaxLoadedTaskSequence = resp.MaxSequenceInclusive

			// add the new tasks into the heap
			for _, task := range resp.Tasks {
				heap.Push(&w.remainingToFireTimersHeap, &task)
			}

			minTime := time.Unix(resp.MinFireTimestampSecondsInclusive, 0)
			if w.nextFiringTimer.InactiveOrFireAfter(minTime) {
				// update the next firing timer if
				// 1. the nextFireTimer is not active(meaning there wasn't any more tasks to fire)
				// 2. any of the new tasks are earlier than the current min
				w.nextFiringTimer.Update(minTime)
			}
		}
	}

	w.logger.Debug("triggered polling timer tasks succeeded", tag.Value(len(resp.Tasks)))
}

// filterNotifyRequest filters out the fire timestamps that are outside current preload time window
func (w *timerTaskQueueImpl) filterNotifyRequest(req xdbapi.NotifyTimerTasksRequest, minTimestampToUpdate *int64) *xdbapi.NotifyTimerTasksRequest {
	var filteredFireTimestamps []int64
	for _, ts := range req.FireTimestamps {
		if ts <= w.currWindowTimestamp {
			filteredFireTimestamps = append(filteredFireTimestamps, ts)
			if ts < *minTimestampToUpdate {
				*minTimestampToUpdate = ts
			}
		} else {
			w.logger.Debug("task fire timestamp is not within the current preload time window, skip",
				tag.UnixTimestamp(w.currWindowTimestamp), tag.UnixTimestamp(ts))
		}
	}
	if len(filteredFireTimestamps) == 0 {
		return nil
	}
	req.FireTimestamps = filteredFireTimestamps
	return &req
}
