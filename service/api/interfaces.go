// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"context"
	"github.com/xcherryio/apis/goapi/xcapi"
)

type Server interface {
	// Start will start running on the background
	Start() error
	Stop(ctx context.Context) error
}

// Service is the interface of API service, which decoupled from REST server framework like Gin
// So that users can choose to use other REST frameworks to serve requests
type Service interface {
	StartProcess(ctx context.Context, request xcapi.ProcessExecutionStartRequest) (
		resp *xcapi.ProcessExecutionStartResponse, err *ErrorWithStatus)
	StopProcess(ctx context.Context, request xcapi.ProcessExecutionStopRequest) *ErrorWithStatus
	DescribeLatestProcess(ctx context.Context, request xcapi.ProcessExecutionDescribeRequest) (
		resp *xcapi.ProcessExecutionDescribeResponse, err *ErrorWithStatus)
	PublishToLocalQueue(ctx context.Context, request xcapi.PublishToLocalQueueRequest) *ErrorWithStatus
	Rpc(
		ctx context.Context, request xcapi.ProcessExecutionRpcRequest,
	) (resp *xcapi.ProcessExecutionRpcResponse, err *ErrorWithStatus)
	ListProcessExecutions(ctx context.Context, request xcapi.ListProcessExecutionsRequest,
	) (response *xcapi.ListProcessExecutionsResponse, retErr *ErrorWithStatus)
	WaitForProcessCompletion(ctx context.Context, request xcapi.ProcessExecutionWaitForCompletionRequest) (
		resp *xcapi.ProcessExecutionWaitForCompletionResponse, err *ErrorWithStatus)
}
