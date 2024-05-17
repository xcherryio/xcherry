// Copyright 2023 xCherryIO organization

// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"fmt"
	"github.com/xcherryio/xcherry/common/log"
	"sync"
	"time"
)

type WaitForProcessCompletionChannelsPerShardImpl struct {
	shardId int32
	logger  log.Logger

	processor ImmediateTaskProcessor

	// processExecutionId : channel
	channelMap map[string]chan string
	// processExecutionId : a list of timestamps of when the waiting requests were created
	waitingRequestCreatedAt map[string][]int64
}

func NewWaitForProcessCompletionChannelsPerShardImplImpl(
	shardId int32, logger log.Logger, processor ImmediateTaskProcessor) WaitForProcessCompletionChannels {
	return &WaitForProcessCompletionChannelsPerShardImpl{
		shardId: shardId,
		logger:  logger,

		processor: processor,

		channelMap:              map[string]chan string{},
		waitingRequestCreatedAt: map[string][]int64{},
	}
}

func (w *WaitForProcessCompletionChannelsPerShardImpl) Start() {
	w.processor.AddWaitForProcessCompletionChannels(w.shardId, w)
}

func (w *WaitForProcessCompletionChannelsPerShardImpl) Stop() {
	w.processor.RemoveWaitForProcessCompletionChannels(w.shardId)

	var procIds []string

	for procId := range w.channelMap {
		procIds = append(procIds, procId)
	}

	for _, procId := range procIds {
		w.Signal(procId, WaitForProcessCompletionResultStop)
	}
}

func (w *WaitForProcessCompletionChannelsPerShardImpl) Add(processExecutionId string) chan string {
	w.logger.Info(fmt.Sprintf("Add process execution completion waiting request for %s in shard %d",
		processExecutionId, w.shardId))

	lock := sync.RWMutex{}
	lock.Lock()
	defer lock.Unlock()

	channel, ok := w.channelMap[processExecutionId]
	if !ok {
		channel = make(chan string)
		w.channelMap[processExecutionId] = channel
	}

	w.waitingRequestCreatedAt[processExecutionId] = append(w.waitingRequestCreatedAt[processExecutionId], w.now())

	w.terminationCheck(processExecutionId)

	return channel
}

func (w *WaitForProcessCompletionChannelsPerShardImpl) Signal(processExecutionId string, result string) {
	channel, ok := w.channelMap[processExecutionId]
	if !ok {
		return
	}

	lock := sync.RWMutex{}
	lock.Lock()
	defer lock.Unlock()

	count := len(w.waitingRequestCreatedAt[processExecutionId])

	for i := 0; i < count; i++ {
		select {
		case channel <- result:
			w.logger.Info(fmt.Sprintf("Signal process execution completion waiting result %d for %s: %s",
				i, processExecutionId, result))
		default:
			w.logger.Info(fmt.Sprintf("Not signal process execution completion waiting result %d for %s: %s",
				i, processExecutionId, result))
		}
	}

	w.waitingRequestCreatedAt[processExecutionId] = []int64{}

	go func() {
		// sleep 3 seconds before close the channel
		time.Sleep(time.Second * 3)

		w.cleanup(processExecutionId)
	}()
}

func (w *WaitForProcessCompletionChannelsPerShardImpl) terminationCheck(processExecutionId string) {
	// To check if the channel should be closed after a certain time period
	go func() {
		time.Sleep(time.Second * time.Duration(DEFAULT_WAIT_FOR_TIMEOUT_MAX+3))

		w.logger.Info(fmt.Sprintf("Check process execution completion waiting channel for %s in shard %d",
			processExecutionId, w.shardId))

		lock := sync.RWMutex{}
		lock.Lock()
		defer lock.Unlock()

		var validWaitingRequestCreatedAt []int64

		now := w.now()
		for _, createdAt := range w.waitingRequestCreatedAt[processExecutionId] {
			if createdAt+int64(DEFAULT_WAIT_FOR_TIMEOUT_MAX) < now {
				w.logger.Info(fmt.Sprintf(
					"Remove process execution completion waiting request created at %d for %s in shard %d",
					createdAt, processExecutionId, w.shardId))
				continue
			}

			validWaitingRequestCreatedAt = append(validWaitingRequestCreatedAt, createdAt)
		}

		w.waitingRequestCreatedAt[processExecutionId] = validWaitingRequestCreatedAt

		if len(w.waitingRequestCreatedAt) == 0 {
			w.cleanup(processExecutionId)
		}
	}()
}

func (w *WaitForProcessCompletionChannelsPerShardImpl) cleanup(processExecutionId string) {
	lock := sync.RWMutex{}
	lock.Lock()
	defer lock.Unlock()

	delete(w.waitingRequestCreatedAt, processExecutionId)

	channel, ok := w.channelMap[processExecutionId]
	if !ok {
		return
	}

	delete(w.channelMap, processExecutionId)
	close(channel)

	w.logger.Info(fmt.Sprintf("Close process execution completion waiting channel for %s in shard %d",
		processExecutionId, w.shardId))
}

func (w *WaitForProcessCompletionChannelsPerShardImpl) now() int64 {
	return time.Now().Unix()
}
