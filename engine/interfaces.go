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
	"github.com/xdblab/xdb/persistence"
)

// TaskNotifier is to notify the poller(taskQueue) that there is a/some new immediate/timer tasks
// so that poller can poll the specific process execution.
// This is needed because adding a new task and polling tasks are in different threads.
// Note that this is not guaranteed to be atomic. The notification is "best effort".
// Also, because in some distributed databases don't have GlobalSecondaryIndex
// (this is not the case for traditional database like MySQL/Postgres),
// there is no partition for shards. To prevent having too many polling across partitions,
// the task notifier pass in detailed information about process execution so that
// the taskQueue(poller) can just poll the task from specific partition.
type TaskNotifier interface {
	AddImmediateTaskQueue(shardId int32, queue ImmediateTaskQueue)
	AddTimerTaskQueue(shardId int32, queue TimerTaskQueue)
	NotifyNewImmediateTasks(request xdbapi.NotifyImmediateTasksRequest)
	NotifyNewTimerTasks(request xdbapi.NotifyTimerTasksRequest)
}

// ImmediateTaskQueue is the queue for immediate tasks
type ImmediateTaskQueue interface {
	Start() error
	// TriggerPollingTasks exposes an API to be called by TaskNotifier
	TriggerPollingTasks(request xdbapi.NotifyImmediateTasksRequest)
	Stop(ctx context.Context) error
}

// TimerTaskQueue is the queue for timer tasks
type TimerTaskQueue interface {
	Start() error
	// TriggerPollingTasks exposes an API to be called by TaskNotifier
	TriggerPollingTasks(request xdbapi.NotifyTimerTasksRequest)
	Stop(ctx context.Context) error
}

type ImmediateTaskProcessor interface {
	Start() error
	Stop(context.Context) error

	// GetTasksToProcessChan exposed a channel for the queue to send tasks to processor
	GetTasksToProcessChan() chan<- persistence.ImmediateTask

	AddImmediateTaskQueue(
		shardId int32, tasksToCommitChan chan<- persistence.ImmediateTask,
	) (alreadyExisted bool)
}

type TimerTaskProcessor interface {
	Start() error
	Stop(context.Context) error

	// GetTasksToProcessChan exposed a channel for the queue to send tasks to processor
	GetTasksToProcessChan() chan<- persistence.TimerTask

	AddTimerTaskQueue(
		shardId int32,
	) (alreadyExisted bool)
}
