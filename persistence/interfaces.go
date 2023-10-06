package persistence

import (
	"context"
)

// ProcessStore is for operating on the database for process execution
type (
	ProcessStore interface {
		Close() error

		StartProcess(ctx context.Context, request StartProcessRequest) (*StartProcessResponse, error)
		DescribeLatestProcess(ctx context.Context, request DescribeLatestProcessRequest) (*DescribeLatestProcessResponse, error)

		GetWorkerTasks(ctx context.Context, request GetWorkerTasksRequest) (*GetWorkerTasksResponse, error)
		DeleteWorkerTasks(ctx context.Context, request DeleteWorkerTasksRequest) error

		PrepareStateExecution(ctx context.Context, request PrepareStateExecutionRequest) (*PrepareStateExecutionResponse, error)
		CompleteWaitUntilExecution(ctx context.Context, request CompleteWaitUntilExecutionRequest) (*CompleteWaitUntilExecutionResponse, error)
		CompleteExecuteExecution(ctx context.Context, request CompleteExecuteExecutionRequest) (*CompleteExecuteExecutionResponse, error)
	}
)
