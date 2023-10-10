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

package async

import (
	"github.com/xdblab/xdb-apis/goapi/xdbapi"
	"github.com/xdblab/xdb/engine"
)

type taskNotifierImpl struct {
	shardIdToWorkerTaskQueue map[int32]engine.WorkerTaskQueue
	shardIdToTimerTaskQueue  map[int32]engine.TimerTaskQueue
}

func newTaskNotifierImpl() engine.TaskNotifier {
	return &taskNotifierImpl{
		shardIdToWorkerTaskQueue: make(map[int32]engine.WorkerTaskQueue),
		shardIdToTimerTaskQueue:  make(map[int32]engine.TimerTaskQueue),
	}
}

func (t *taskNotifierImpl) NotifyNewWorkerTasks(request xdbapi.NotifyWorkerTasksRequest) {
	queue, ok := t.shardIdToWorkerTaskQueue[request.ShardId]
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

func (t *taskNotifierImpl) AddWorkerTaskQueue(shardId int32, queue engine.WorkerTaskQueue) {
	_, ok := t.shardIdToWorkerTaskQueue[shardId]
	if ok {
		panic("the shard is already registered")
	}
	t.shardIdToWorkerTaskQueue[shardId] = queue
}

func (t *taskNotifierImpl) AddTimerTaskQueue(shardId int32, queue engine.TimerTaskQueue) {
	_, ok := t.shardIdToTimerTaskQueue[shardId]
	if ok {
		panic("the shard is already registered")
	}
	t.shardIdToTimerTaskQueue[shardId] = queue
}