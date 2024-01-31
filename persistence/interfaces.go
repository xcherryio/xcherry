// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: Apache-2.0

package persistence

import (
	"context"
	"github.com/xcherryio/xcherry/persistence/data_models"
)

// ProcessStore is for operating on the database for process execution
type (
	ProcessStore interface {
		Close() error

		StartProcess(
			ctx context.Context, request data_models.StartProcessRequest,
		) (*data_models.StartProcessResponse, error)
		StopProcess(
			ctx context.Context, request data_models.StopProcessRequest,
		) (*data_models.StopProcessResponse, error)
		DescribeLatestProcess(
			ctx context.Context, request data_models.DescribeLatestProcessRequest,
		) (*data_models.DescribeLatestProcessResponse, error)
		RecoverFromStateExecutionFailure(
			ctx context.Context, request data_models.RecoverFromStateExecutionFailureRequest,
		) error
		GetLatestProcessExecution(
			ctx context.Context, request data_models.GetLatestProcessExecutionRequest,
		) (*data_models.GetLatestProcessExecutionResponse, error)

		GetImmediateTasks(
			ctx context.Context, request data_models.GetImmediateTasksRequest,
		) (*data_models.GetImmediateTasksResponse, error)
		DeleteImmediateTasks(ctx context.Context, request data_models.DeleteImmediateTasksRequest) error
		BackoffImmediateTask(ctx context.Context, request data_models.BackoffImmediateTaskRequest) error
		CleanUpTasksForTest(ctx context.Context, shardId int32) error

		GetTimerTasksUpToTimestamp(
			ctx context.Context, request data_models.GetTimerTasksRequest,
		) (*data_models.GetTimerTasksResponse, error)

		GetTimerTasksForTimestamps(
			ctx context.Context, request data_models.GetTimerTasksForTimestampsRequest,
		) (*data_models.GetTimerTasksResponse, error)
		ConvertTimerTaskToImmediateTask(
			ctx context.Context, request data_models.ProcessTimerTaskRequest,
		) (*data_models.ProcessTimerTaskResponse, error)
		ProcessTimerTaskForTimerCommand(
			ctx context.Context, request data_models.ProcessTimerTaskRequest,
		) (*data_models.ProcessTimerTaskResponse, error)
		ProcessTimerTaskForProcessTimeout(
			ctx context.Context, request data_models.ProcessTimerTaskRequest,
		) (*data_models.ProcessTimerTaskResponse, error)

		PrepareStateExecution(
			ctx context.Context, request data_models.PrepareStateExecutionRequest,
		) (*data_models.PrepareStateExecutionResponse, error)
		ProcessWaitUntilExecution(
			ctx context.Context, request data_models.ProcessWaitUntilExecutionRequest,
		) (*data_models.ProcessWaitUntilExecutionResponse, error)
		CompleteExecuteExecution(
			ctx context.Context, request data_models.CompleteExecuteExecutionRequest,
		) (*data_models.CompleteExecuteExecutionResponse, error)

		PublishToLocalQueue(
			ctx context.Context, request data_models.PublishToLocalQueueRequest,
		) (*data_models.PublishToLocalQueueResponse, error)
		ProcessLocalQueueMessages(
			ctx context.Context, request data_models.ProcessLocalQueueMessagesRequest,
		) (*data_models.ProcessLocalQueueMessagesResponse, error)

		ReadAppDatabase(
			ctx context.Context, request data_models.AppDatabaseReadRequest,
		) (*data_models.AppDatabaseReadResponse, error)

		LoadLocalAttributes(
			ctx context.Context, request data_models.LoadLocalAttributesRequest,
		) (*data_models.LoadLocalAttributesResponse, error)

		UpdateProcessExecutionForRpc(ctx context.Context, request data_models.UpdateProcessExecutionForRpcRequest) (
			*data_models.UpdateProcessExecutionForRpcResponse, error)
	}

	VisibilityStore interface {
		Close() error
		RecordProcessExecutionStatus(ctx context.Context, req data_models.RecordProcessExecutionStatusRequest) error
		// TODO: add list process executions api
		// TODO: add count process executions api
	}
)
