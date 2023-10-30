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

package persistence

import (
	"context"
)

// ProcessStore is for operating on the database for process execution
type (
	ProcessStore interface {
		Close() error

		StartProcess(ctx context.Context, request StartProcessRequest) (*StartProcessResponse, error)
		StopProcess(ctx context.Context, request StopProcessRequest) (*StopProcessResponse, error)
		DescribeLatestProcess(ctx context.Context, request DescribeLatestProcessRequest) (*DescribeLatestProcessResponse, error)
		RecoverFromStateExecutionFailure(ctx context.Context, request RecoverFromStateExecutionFailureRequest) error

		GetImmediateTasks(ctx context.Context, request GetImmediateTasksRequest) (*GetImmediateTasksResponse, error)
		DeleteImmediateTasks(ctx context.Context, request DeleteImmediateTasksRequest) error
		BackoffImmediateTask(ctx context.Context, request BackoffImmediateTaskRequest) error
		CleanUpTasksForTest(ctx context.Context, shardId int32) error

		GetTimerTasksUpToTimestamp(ctx context.Context, request GetTimerTasksRequest) (*GetTimerTasksResponse, error)
		GetTimerTasksForTimestamps(ctx context.Context, request GetTimerTasksForTimestampsRequest) (*GetTimerTasksResponse, error)
		ConvertTimerTaskToImmediateTask(ctx context.Context, request ConvertTimerTaskToImmediateTaskRequest) error

		PrepareStateExecution(ctx context.Context, request PrepareStateExecutionRequest) (*PrepareStateExecutionResponse, error)
		ProcessWaitUntilExecution(ctx context.Context, request ProcessWaitUntilExecutionRequest) (*ProcessWaitUntilExecutionResponse, error)
		CompleteExecuteExecution(ctx context.Context, request CompleteExecuteExecutionRequest) (*CompleteExecuteExecutionResponse, error)

		PublishToLocalQueue(ctx context.Context, request PublishToLocalQueueRequest) (*PublishToLocalQueueResponse, error)
		ProcessLocalQueueMessages(ctx context.Context, request ProcessLocalQueueMessagesRequest) (*ProcessLocalQueueMessagesResponse, error)
	}
)
