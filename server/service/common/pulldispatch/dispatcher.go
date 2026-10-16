// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package pulldispatch

import (
	"context"
	"fmt"
	"time"

	"github.com/superdurable/dex/gen/dexpb"
	"github.com/superdurable/dex/service/common/workerclient"
	"github.com/superdurable/dex/service/membership"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Dispatcher sends a pull task to the matching service.
type Dispatcher interface {
	Dispatch(context.Context, *dexpb.DispatchTaskRequest) (grpc.ServerStreamingClient[dexpb.DispatchTaskResponse], error)
}

type grpcDispatcher struct {
	client *workerclient.InternalServiceClient
}

type routedDispatcher struct {
	members interface {
		Lookup(membership.Role, string) (membership.HostInfo, error)
	}
	clientForAddress func(context.Context, string) (dexpb.MatchingServiceClient, error)
}

type unavailableDispatcher struct{}

type receivedFrame struct {
	frame *dexpb.DispatchTaskResponse
	err   error
}

// ServerError marks a failure before matching assigns a Worker.
type ServerError struct{ Err error }

func (e *ServerError) Error() string { return fmt.Sprintf("matching dispatch: %v", e.Err) }
func (e *ServerError) Unwrap() error { return e.Err }

// ReceiveFirstFrame keeps the activity alive and wakes the flow type while matching waits.
func ReceiveFirstFrame(
	ctx context.Context,
	stream grpc.ServerStreamingClient[dexpb.DispatchTaskResponse],
	stickyWait time.Duration,
	heartbeat func(),
	wake func(),
) (*dexpb.DispatchTaskResponse, error) {
	frames := make(chan receivedFrame, 1)
	go receiveNextFrame(stream, frames)
	var wakeTimer *time.Timer
	var wakeChannel <-chan time.Time
	var wakeDeadline time.Time
	if wake != nil {
		wakeDeadline = time.Now().Add(stickyWait)
		wakeTimer = time.NewTimer(stickyWait)
		wakeChannel = wakeTimer.C
		defer wakeTimer.Stop()
	}
	var heartbeatTicker *time.Ticker
	var heartbeatChannel <-chan time.Time
	if heartbeat != nil {
		heartbeatTicker = time.NewTicker(time.Second)
		heartbeatChannel = heartbeatTicker.C
		defer heartbeatTicker.Stop()
	}
	isAssigned := false
	hasWoken := false
	for {
		select {
		case received := <-frames:
			if received.err != nil {
				return nil, received.err
			}
			if received.frame.GetAssigned() != nil {
				isAssigned = true
				if wakeTimer != nil {
					wakeTimer.Stop()
					wakeChannel = nil
				}
				go receiveNextFrame(stream, frames)
				continue
			}
			if received.frame.GetUnassigned() != nil {
				isAssigned = false
				if wakeTimer != nil && !hasWoken {
					remaining := time.Until(wakeDeadline)
					if remaining <= 0 {
						wake()
						hasWoken = true
					} else {
						wakeTimer.Reset(remaining)
						wakeChannel = wakeTimer.C
					}
				}
				go receiveNextFrame(stream, frames)
				continue
			}
			return received.frame, received.err
		case <-wakeChannel:
			wake()
			hasWoken = true
			wakeChannel = nil
		case <-heartbeatChannel:
			if !isAssigned {
				heartbeat()
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func receiveNextFrame(stream grpc.ServerStreamingClient[dexpb.DispatchTaskResponse], frames chan<- receivedFrame) {
	frame, err := stream.Recv()
	frames <- receivedFrame{frame: frame, err: err}
}

// NewUnavailableDispatcher preserves push-only service construction.
func NewUnavailableDispatcher() Dispatcher {
	return unavailableDispatcher{}
}

func (unavailableDispatcher) Dispatch(
	context.Context, *dexpb.DispatchTaskRequest,
) (grpc.ServerStreamingClient[dexpb.DispatchTaskResponse], error) {
	return nil, status.Error(codes.FailedPrecondition, "pull dispatch is not configured")
}

// NewDispatcher uses an existing authenticated internal connection.
func NewDispatcher(client *workerclient.InternalServiceClient) Dispatcher {
	if client == nil {
		panic("pull dispatcher requires an internal client")
	}
	return &grpcDispatcher{client: client}
}

func (d *grpcDispatcher) Dispatch(
	ctx context.Context, request *dexpb.DispatchTaskRequest,
) (grpc.ServerStreamingClient[dexpb.DispatchTaskResponse], error) {
	client, callCtx, err := d.client.MatchingClient(ctx)
	if err != nil {
		return nil, err
	}
	return client.DispatchTask(callCtx, request)
}

// NewRoutedDispatcher selects the matching owner for each pull queue.
func NewRoutedDispatcher(
	members interface {
		Lookup(membership.Role, string) (membership.HostInfo, error)
	},
	clientForAddress func(context.Context, string) (dexpb.MatchingServiceClient, error),
) Dispatcher {
	if members == nil || clientForAddress == nil {
		panic("routed dispatcher requires membership and a matching client factory")
	}
	return &routedDispatcher{members: members, clientForAddress: clientForAddress}
}

func (d *routedDispatcher) Dispatch(
	ctx context.Context, request *dexpb.DispatchTaskRequest,
) (grpc.ServerStreamingClient[dexpb.DispatchTaskResponse], error) {
	owner, err := d.members.Lookup(membership.RoleMatching, request.GetQueue())
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "find matching owner for queue %s: %v", request.GetQueue(), err)
	}
	client, err := d.clientForAddress(ctx, owner.RPCAddress)
	if err != nil {
		return nil, err
	}
	return client.DispatchTask(ctx, request)
}

// WorkerErrorStatus preserves the gRPC status and Worker details from a pull frame.
func WorkerErrorStatus(workerError *dexpb.WorkerErrorResponse) error {
	if workerError == nil {
		panic("WorkerErrorStatus requires WorkerErrorResponse")
	}
	grpcCode := codes.Code(workerError.GetGrpcStatus())
	if grpcCode <= codes.OK || grpcCode > codes.Unauthenticated {
		grpcCode = codes.Unknown
	}
	workerStatus, err := status.New(grpcCode, workerError.GetDetail()).WithDetails(workerError)
	if err != nil {
		return fmt.Errorf("encode Worker error: %w", err)
	}
	return workerStatus.Err()
}
