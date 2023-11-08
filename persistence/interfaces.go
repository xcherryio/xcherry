// Copyright (c) 2023 XDBLab Organization
// SPDX-License-Identifier: BUSL-1.1

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
		DescribeLatestProcess(
			ctx context.Context, request DescribeLatestProcessRequest,
		) (*DescribeLatestProcessResponse, error)
		RecoverFromStateExecutionFailure(ctx context.Context, request RecoverFromStateExecutionFailureRequest) error

		GetImmediateTasks(ctx context.Context, request GetImmediateTasksRequest) (*GetImmediateTasksResponse, error)
		DeleteImmediateTasks(ctx context.Context, request DeleteImmediateTasksRequest) error
		BackoffImmediateTask(ctx context.Context, request BackoffImmediateTaskRequest) error
		CleanUpTasksForTest(ctx context.Context, shardId int32) error

		GetTimerTasksUpToTimestamp(ctx context.Context, request GetTimerTasksRequest) (*GetTimerTasksResponse, error)

		GetTimerTasksForTimestamps(
			ctx context.Context, request GetTimerTasksForTimestampsRequest,
		) (*GetTimerTasksResponse, error)
		ConvertTimerTaskToImmediateTask(
			ctx context.Context, request ProcessTimerTaskRequest,
		) (*ProcessTimerTaskResponse, error)
		ProcessTimerTaskForTimerCommand(
			ctx context.Context, request ProcessTimerTaskRequest,
		) (*ProcessTimerTaskResponse, error)
		ProcessTimerTaskForProcessTimeout(
			ctx context.Context, request ProcessTimerTaskRequest,
		) (*ProcessTimerTaskResponse, error)

		PrepareStateExecution(
			ctx context.Context, request PrepareStateExecutionRequest,
		) (*PrepareStateExecutionResponse, error)
		ProcessWaitUntilExecution(
			ctx context.Context, request ProcessWaitUntilExecutionRequest,
		) (*ProcessWaitUntilExecutionResponse, error)
		CompleteExecuteExecution(
			ctx context.Context, request CompleteExecuteExecutionRequest,
		) (*CompleteExecuteExecutionResponse, error)

		PublishToLocalQueue(
			ctx context.Context, request PublishToLocalQueueRequest,
		) (*PublishToLocalQueueResponse, error)
		ProcessLocalQueueMessages(
			ctx context.Context, request ProcessLocalQueueMessagesRequest,
		) (*ProcessLocalQueueMessagesResponse, error)

		LoadGlobalAttributes(
			ctx context.Context, request LoadGlobalAttributesRequest,
		) (*LoadGlobalAttributesResponse, error)
	}
)
