package persistence

import (
	"context"
	"github.com/xdblab/xdb-apis/goapi/xdbapi"
)

type ProcessORM interface {
	StartProcess(ctx context.Context, request xdbapi.ProcessExecutionStartRequest) (resp *xdbapi.ProcessExecutionStartResponse, alreadyStarted bool, err error)
	DescribeLatestProcess(ctx context.Context, request xdbapi.ProcessExecutionDescribeRequest) (resp *xdbapi.ProcessExecutionDescribeResponse, notExists bool, err error)
}

type ProcessMQ interface {
	// TODO
}
