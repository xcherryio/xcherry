// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package rpc

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/superdurable/dex/config"
	"github.com/superdurable/dex/gen/dexpb"
	"github.com/superdurable/dex/service/common/pulldispatch"
	"github.com/superdurable/dex/service/wake"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func invokePullWorkerRPC(
	ctx context.Context,
	dispatcher pulldispatch.Dispatcher,
	matchingCfg *config.MatchingConfig,
	notifier *wake.Notifier,
	dexTaskQueue string,
	flowID string,
	flowType string,
	request *dexpb.InvokeWorkerRPCRequest,
	isLocalActivity bool,
) (*dexpb.InvokeWorkerRPCResponse, string, error) {
	if dispatcher == nil {
		panic("pull Worker RPC requires a dispatcher")
	}
	settings := config.MatchingConfig{}.WithDefaults()
	if matchingCfg != nil {
		settings = matchingCfg.WithDefaults()
	}
	dispatchWait, stickyWait := settings.DispatchWait, settings.StickyWait
	if isLocalActivity {
		dispatchWait, stickyWait = settings.LocalDispatchWait, settings.LocalStickyWait
	}
	deadline, hasDeadline := ctx.Deadline()
	if !hasDeadline {
		deadline = time.Now().Add(2 * time.Hour)
	}
	remaining := time.Until(deadline)
	if isLocalActivity {
		localBudget := remaining - settings.LocalExecutionReserve
		if localBudget <= 0 {
			localBudget = remaining / 2
		}
		remaining = localBudget
	}
	if remaining < dispatchWait {
		dispatchWait = remaining
	}
	if dispatchWait <= 0 {
		return nil, "", status.Error(codes.DeadlineExceeded, "no time remains for pull Worker dispatch")
	}
	if stickyWait > dispatchWait {
		stickyWait = dispatchWait
	}
	queue := dexTaskQueue
	if queue == "" {
		queue = flowType
	}
	dispatchRequest := &dexpb.DispatchTaskRequest{
		Queue:         queue,
		FallbackQueue: flowType,
		FlowId:        flowID,
		StickyWait:    durationpb.New(stickyWait),
		DispatchWait:  durationpb.New(dispatchWait),
		Deadline:      timestamppb.New(deadline),
		Request:       &dexpb.DispatchTaskRequest_Rpc{Rpc: request},
	}
	stream, err := dispatcher.Dispatch(ctx, dispatchRequest)
	if err != nil {
		return nil, "", &pulldispatch.ServerError{Err: err}
	}
	var wakeWorker func()
	if notifier != nil {
		wakeWorker = func() { notifier.NotifyWorkerUnavailable(flowType) }
	}
	frame, err := pulldispatch.ReceiveFirstFrame(ctx, stream, stickyWait, nil, wakeWorker)
	if err != nil {
		return nil, "", &pulldispatch.ServerError{Err: err}
	}
	if response := frame.GetRpc(); response != nil {
		if _, err := stream.Recv(); err != io.EOF {
			if err == nil {
				return nil, "", fmt.Errorf("Worker RPC emitted output after its result")
			}
			return nil, "", err
		}
		if notifier != nil {
			notifier.NotifyWorkerActive(flowType)
		}
		return response, frame.GetServedByAppWorkerId(), nil
	}
	if workerError := frame.GetWorkerError(); workerError != nil {
		return nil, "", pulldispatch.WorkerErrorStatus(workerError)
	}
	if dispatchError := frame.GetDispatchError(); dispatchError != nil {
		if dispatchError.GetReason() == dexpb.DispatchTaskErrorReason_DISPATCH_TASK_ERROR_REASON_NO_WORKER_AVAILABLE {
			if notifier != nil {
				notifier.NotifyWorkerUnavailable(flowType)
			}
			return nil, "", status.Errorf(codes.Unavailable, "no Worker available for queue %s: %s", flowType, dispatchError.GetDetail())
		}
		if dispatchError.GetReason() == dexpb.DispatchTaskErrorReason_DISPATCH_TASK_ERROR_REASON_WORKER_DISCONNECTED {
			return nil, "", status.Errorf(codes.Unavailable, "Worker disconnected from queue %s: %s", queue, dispatchError.GetDetail())
		}
	}
	return nil, "", status.Error(codes.Internal, "unexpected pull Worker RPC response")
}
