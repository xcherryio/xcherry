// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package dex

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/superdurable/dex/sdk-go/gen/dexpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type rejectTaskClientStream struct {
	dexpb.WorkerTaskService_PollWorkerTaskClient
	sendError error
	sent      []*dexpb.PollWorkerTaskRequest
}

func (stream *rejectTaskClientStream) Send(request *dexpb.PollWorkerTaskRequest) error {
	stream.sent = append(stream.sent, request)
	return stream.sendError
}

type rejectTaskLevelLogger struct {
	Logger
	debugCount atomic.Int32
	errorCount atomic.Int32
}

func (logger *rejectTaskLevelLogger) Debug(message string, keyvals ...interface{}) {
	logger.debugCount.Add(1)
	logger.Logger.Debug(message, keyvals...)
}

func (logger *rejectTaskLevelLogger) Error(message string, keyvals ...interface{}) {
	logger.errorCount.Add(1)
	logger.Logger.Error(message, keyvals...)
}

func TestWorkerPullRejectTaskLogLevel(t *testing.T) {
	testCases := []struct {
		name             string
		isSendClosed     bool
		isStreamCanceled bool
		sendError        error
		isUnexpected     bool
	}{
		{name: "send already closed", isSendClosed: true},
		{name: "stream terminated", sendError: io.EOF},
		{name: "context canceled", sendError: context.Canceled},
		{name: "canceled status", sendError: status.Error(codes.Canceled, "context canceled")},
		{name: "unavailable after stream cancellation", isStreamCanceled: true, sendError: status.Error(codes.Unavailable, "transport is closing")},
		{name: "unavailable on open stream", sendError: status.Error(codes.Unavailable, "connection reset"), isUnexpected: true},
		{name: "internal status", sendError: status.Error(codes.Internal, "encode reject frame"), isUnexpected: true},
		{name: "local failure", sendError: errors.New("encode reject frame"), isUnexpected: true},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if testCase.isStreamCanceled {
				cancel()
			}
			logger := &rejectTaskLevelLogger{Logger: slog.Default()}
			clientStream := &rejectTaskClientStream{sendError: testCase.sendError}
			stream := newRejectTaskTestStream(ctx, logger, clientStream)
			stream.isSendClosed = testCase.isSendClosed
			stream.rejectTask("task-1", dexpb.PollWorkerTaskRejectReason_POLL_WORKER_TASK_REJECT_REASON_NO_CAPACITY)
			if testCase.isSendClosed {
				require.Empty(t, clientStream.sent)
			} else {
				require.Len(t, clientStream.sent, 1)
				require.Equal(t, "task-1", clientStream.sent[0].GetReject().GetTaskId())
			}
			if testCase.isUnexpected {
				require.Equal(t, int32(1), logger.errorCount.Load())
				require.Zero(t, logger.debugCount.Load())
			} else {
				require.Zero(t, logger.errorCount.Load())
				require.Equal(t, int32(1), logger.debugCount.Load())
			}
		})
	}
}

func TestWorkerPullRejectTaskOnCanceledGRPCStream(t *testing.T) {
	address, _ := startPullTestServer(t)
	connection, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer func() { require.NoError(t, connection.Close()) }()
	ctx, cancel := context.WithCancel(context.Background())
	clientStream, err := dexpb.NewWorkerTaskServiceClient(connection).PollWorkerTask(ctx)
	require.NoError(t, err)
	cancel()
	logger := &rejectTaskLevelLogger{Logger: slog.Default()}
	stream := newRejectTaskTestStream(ctx, logger, clientStream)
	// gRPC finishes a canceled stream asynchronously, so an early Send may still succeed.
	require.Eventually(t, func() bool {
		stream.rejectTask("task-1", dexpb.PollWorkerTaskRejectReason_POLL_WORKER_TASK_REJECT_REASON_NO_CAPACITY)
		return logger.debugCount.Load() > 0
	}, 5*time.Second, 10*time.Millisecond)
	require.Zero(t, logger.errorCount.Load())
}

func newRejectTaskTestStream(ctx context.Context, logger Logger, clientStream dexpb.WorkerTaskService_PollWorkerTaskClient) *pullStream {
	return &pullStream{
		worker: &pullWorker{service: &workerService{logger: logger}},
		queue:  "rejectTaskFlow",
		ctx:    ctx,
		stream: clientStream,
	}
}
