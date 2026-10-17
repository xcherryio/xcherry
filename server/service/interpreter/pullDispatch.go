// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package interpreter

import (
	"context"
	"fmt"
	"time"

	"github.com/superdurable/dex/config"
	"github.com/superdurable/dex/gen/dexpb"
	"github.com/superdurable/dex/service/common/pulldispatch"
	"github.com/superdurable/dex/service/wake"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type pullWaitForStream struct {
	ctx                   context.Context
	stream                grpc.ServerStreamingClient[dexpb.DispatchTaskResponse]
	stickyWait            time.Duration
	heartbeat             func()
	hasReceivedFirstFrame bool
	queue                 string
	fallbackQueue         string
	wakeNotifier          *wake.Notifier
	servedByAppWorkerID   string
}

func (s *pullWaitForStream) Recv() (*dexpb.InvokeWaitForMethodOutput, error) {
	frame, err := s.receiveFrame()
	if err != nil {
		return nil, err
	}
	if output := frame.GetWaitFor(); output != nil {
		if output.GetResult() != nil {
			s.servedByAppWorkerID = frame.GetServedByAppWorkerId()
			s.wakeNotifier.NotifyWorkerActive(s.fallbackQueue)
		}
		return output, nil
	}
	return nil, pullFrameError(frame, s.queue, s.fallbackQueue, s.wakeNotifier)
}

func (s *pullWaitForStream) receiveFrame() (*dexpb.DispatchTaskResponse, error) {
	if s.hasReceivedFirstFrame {
		return s.stream.Recv()
	}
	s.hasReceivedFirstFrame = true
	frame, err := pulldispatch.ReceiveFirstFrame(s.ctx, s.stream, s.stickyWait, s.heartbeat, func() {
		s.wakeNotifier.NotifyWorkerUnavailable(s.fallbackQueue)
	})
	if err != nil {
		return nil, &matchingDispatchError{err: err}
	}
	return frame, nil
}

type pullExecuteStream struct {
	ctx                   context.Context
	stream                grpc.ServerStreamingClient[dexpb.DispatchTaskResponse]
	stickyWait            time.Duration
	heartbeat             func()
	hasReceivedFirstFrame bool
	queue                 string
	fallbackQueue         string
	wakeNotifier          *wake.Notifier
	servedByAppWorkerID   string
}

func (s *pullExecuteStream) Recv() (*dexpb.InvokeExecuteMethodOutput, error) {
	frame, err := s.receiveFrame()
	if err != nil {
		return nil, err
	}
	if output := frame.GetExecute(); output != nil {
		if output.GetResult() != nil {
			s.servedByAppWorkerID = frame.GetServedByAppWorkerId()
			s.wakeNotifier.NotifyWorkerActive(s.fallbackQueue)
		}
		return output, nil
	}
	return nil, pullFrameError(frame, s.queue, s.fallbackQueue, s.wakeNotifier)
}

func (s *pullExecuteStream) receiveFrame() (*dexpb.DispatchTaskResponse, error) {
	if s.hasReceivedFirstFrame {
		return s.stream.Recv()
	}
	s.hasReceivedFirstFrame = true
	frame, err := pulldispatch.ReceiveFirstFrame(s.ctx, s.stream, s.stickyWait, s.heartbeat, func() {
		s.wakeNotifier.NotifyWorkerUnavailable(s.fallbackQueue)
	})
	if err != nil {
		return nil, &matchingDispatchError{err: err}
	}
	return frame, nil
}

type matchingDispatchError struct{ err error }

func (e *matchingDispatchError) Error() string { return fmt.Sprintf("matching dispatch: %v", e.err) }
func (e *matchingDispatchError) Unwrap() error { return e.err }

func pullFrameError(frame *dexpb.DispatchTaskResponse, queue string, fallbackQueue string, notifier *wake.Notifier) error {
	if workerError := frame.GetWorkerError(); workerError != nil {
		return pulldispatch.WorkerErrorStatus(workerError)
	}
	if dispatchError := frame.GetDispatchError(); dispatchError != nil {
		switch dispatchError.GetReason() {
		case dexpb.DispatchTaskErrorReason_DISPATCH_TASK_ERROR_REASON_NO_WORKER_AVAILABLE:
			if fallbackQueue != "" {
				queue = fallbackQueue
			}
			if notifier != nil {
				notifier.NotifyWorkerUnavailable(queue)
			}
			return status.Errorf(codes.Unavailable, "no Worker available for queue %s: %s", queue, dispatchError.GetDetail())
		case dexpb.DispatchTaskErrorReason_DISPATCH_TASK_ERROR_REASON_WORKER_DISCONNECTED:
			return status.Errorf(codes.Unavailable, "Worker disconnected during pull dispatch: %s", dispatchError.GetDetail())
		case dexpb.DispatchTaskErrorReason_DISPATCH_TASK_ERROR_REASON_UNKNOWN_NAME:
			return status.Error(codes.NotFound, dispatchError.GetDetail())
		}
		return &matchingDispatchError{err: status.Errorf(codes.Internal, "unknown dispatch failure for queue %s", queue)}
	}
	return &matchingDispatchError{err: status.Error(codes.Internal, "unexpected pull dispatch frame")}
}

func pullTaskRequest(
	ctx context.Context,
	queue string,
	flowType string,
	flowID string,
	dispatchWait time.Duration,
	stickyWait time.Duration,
) *dexpb.DispatchTaskRequest {
	if queue == "" {
		queue = flowType
	}
	deadline, hasDeadline := ctx.Deadline()
	if !hasDeadline {
		deadline = time.Now().Add(2 * time.Hour)
	}
	return &dexpb.DispatchTaskRequest{
		Queue:                   queue,
		FallbackQueue:           flowType,
		FlowId:                  flowID,
		DispatchWait:            durationpb.New(dispatchWait),
		StickyWait:              durationpb.New(stickyWait),
		Deadline:                timestamppb.New(deadline),
		SupportsAssignmentState: true,
	}
}

func activityPullWaits(
	ctx context.Context,
	matchingConfig *config.MatchingConfig,
	isLocalActivity bool,
	dispatchWait time.Duration,
	stickyWait time.Duration,
) (time.Duration, time.Duration, error) {
	if isLocalActivity {
		settings := config.MatchingConfig{}.WithDefaults()
		if matchingConfig != nil {
			settings = matchingConfig.WithDefaults()
		}
		dispatchWait = settings.LocalDispatchWait
		stickyWait = settings.LocalStickyWait
		if deadline, hasDeadline := ctx.Deadline(); hasDeadline {
			remaining := time.Until(deadline)
			remainingBudget := remaining - settings.LocalExecutionReserve
			if remaining/2 > remainingBudget {
				remainingBudget = remaining / 2
			}
			if remainingBudget < dispatchWait {
				dispatchWait = remainingBudget
			}
		}
	}
	if dispatchWait <= 0 {
		return 0, 0, status.Error(codes.DeadlineExceeded, "no time remains for pull Worker dispatch")
	}
	if stickyWait > dispatchWait {
		stickyWait = dispatchWait
	}
	return dispatchWait, stickyWait, nil
}

func (a *Activities) openPullWaitFor(
	ctx context.Context,
	input *dexpb.InvokeWaitForMethodActivityInput,
	request *dexpb.InvokeWaitForMethodRequest,
	isLocalActivity bool,
	heartbeat func(),
) (*pullWaitForStream, error) {
	dispatchWait, stickyWait, err := activityPullWaits(
		ctx, a.cfg.Matching, isLocalActivity,
		input.GetDispatchWait().AsDuration(), input.GetStickyWait().AsDuration(),
	)
	if err != nil {
		return nil, err
	}
	dispatchRequest := pullTaskRequest(ctx, input.GetDexTaskQueue(), request.GetFlowType(), request.GetContext().GetFlowId(), dispatchWait, stickyWait)
	dispatchRequest.Request = &dexpb.DispatchTaskRequest_WaitFor{WaitFor: request}
	stream, err := a.pullDispatcher.Dispatch(ctx, dispatchRequest)
	if err != nil {
		heartbeat()
		return nil, err
	}
	return &pullWaitForStream{ctx: ctx, stream: stream, stickyWait: stickyWait, heartbeat: heartbeat, queue: dispatchRequest.Queue, fallbackQueue: dispatchRequest.FallbackQueue, wakeNotifier: a.wakeNotifier}, nil
}

func (a *Activities) openPullExecute(
	ctx context.Context,
	input *dexpb.InvokeExecuteMethodActivityInput,
	request *dexpb.InvokeExecuteMethodRequest,
	isLocalActivity bool,
	heartbeat func(),
) (*pullExecuteStream, error) {
	dispatchWait, stickyWait, err := activityPullWaits(
		ctx, a.cfg.Matching, isLocalActivity,
		input.GetDispatchWait().AsDuration(), input.GetStickyWait().AsDuration(),
	)
	if err != nil {
		return nil, err
	}
	dispatchRequest := pullTaskRequest(ctx, input.GetDexTaskQueue(), request.GetFlowType(), request.GetContext().GetFlowId(), dispatchWait, stickyWait)
	dispatchRequest.Request = &dexpb.DispatchTaskRequest_Execute{Execute: request}
	stream, err := a.pullDispatcher.Dispatch(ctx, dispatchRequest)
	if err != nil {
		heartbeat()
		return nil, err
	}
	return &pullExecuteStream{ctx: ctx, stream: stream, stickyWait: stickyWait, heartbeat: heartbeat, queue: dispatchRequest.Queue, fallbackQueue: dispatchRequest.FallbackQueue, wakeNotifier: a.wakeNotifier}, nil
}

func (a *Activities) MatchingConfig() config.MatchingConfig {
	if a.cfg.Matching == nil {
		return config.MatchingConfig{}.WithDefaults()
	}
	return a.cfg.Matching.WithDefaults()
}
