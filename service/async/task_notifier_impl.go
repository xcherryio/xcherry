// Copyright (c) XDBLab
// SPDX-License-Identifier: BUSL-1.1

package async

import (
	"github.com/xdblab/xdb-apis/goapi/xdbapi"
	"github.com/xdblab/xdb/engine"
)

type taskNotifierImpl struct {
	shardIdToImmediateTaskQueue map[int32]engine.ImmediateTaskQueue
	shardIdToTimerTaskQueue     map[int32]engine.TimerTaskQueue
}

func newTaskNotifierImpl() engine.TaskNotifier {
	return &taskNotifierImpl{
		shardIdToImmediateTaskQueue: make(map[int32]engine.ImmediateTaskQueue),
		shardIdToTimerTaskQueue:     make(map[int32]engine.TimerTaskQueue),
	}
}

func (t *taskNotifierImpl) NotifyNewImmediateTasks(request xdbapi.NotifyImmediateTasksRequest) {
	queue, ok := t.shardIdToImmediateTaskQueue[request.ShardId]
	if !ok {
		panic("the shard is not registered")
	}
	queue.TriggerPollingTasks(request)
}

func (t *taskNotifierImpl) NotifyNewTimerTasks(request xdbapi.NotifyTimerTasksRequest) {
	queue, ok := t.shardIdToTimerTaskQueue[request.ShardId]
	if !ok {
		panic("the shard is not registered")
	}
	queue.TriggerPollingTasks(request)
}

func (t *taskNotifierImpl) AddImmediateTaskQueue(shardId int32, queue engine.ImmediateTaskQueue) {
	_, ok := t.shardIdToImmediateTaskQueue[shardId]
	if ok {
		panic("the shard is already registered")
	}
	t.shardIdToImmediateTaskQueue[shardId] = queue
}

func (t *taskNotifierImpl) AddTimerTaskQueue(shardId int32, queue engine.TimerTaskQueue) {
	_, ok := t.shardIdToTimerTaskQueue[shardId]
	if ok {
		panic("the shard is already registered")
	}
	t.shardIdToTimerTaskQueue[shardId] = queue
}
