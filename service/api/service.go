package api

import (
	"context"
	"github.com/xdblab/xdb-apis/goapi/xdbapi"
)

// Service is the interface of API service, which decoupled from REST server framework like Gin
// So that users can choose to use other REST frameworks to serve requests
type Service interface {
	StartProcess(ctx context.Context, request xdbapi.ProcessExecutionStartRequest) (
		resp *xdbapi.ProcessExecutionStartResponse, err *ErrorWithStatus)
	DescribeLatestProcess(ctx context.Context, request xdbapi.ProcessExecutionDescribeRequest) (
		resp *xdbapi.ProcessExecutionDescribeResponse, err *ErrorWithStatus)
}
