package engine

import (
	"context"
	"github.com/xdblab/xdb-apis/goapi/xdbapi"
	"time"
)

// APIEngine is for operating on the database for process execution
type APIEngine interface {
	StartProcess(ctx context.Context, request xdbapi.ProcessExecutionStartRequest) (
		resp *xdbapi.ProcessExecutionStartResponse, alreadyStarted bool, err error)
	DescribeLatestProcess(ctx context.Context, request xdbapi.ProcessExecutionDescribeRequest) (
		resp *xdbapi.ProcessExecutionDescribeResponse, notExists bool, err error)
	Close() error
}

// TaskQueue is task queue
type TaskQueue interface {
	Start() error
	TriggerPolling(pollTime time.Time)
	Stop(ctx context.Context) error
}
