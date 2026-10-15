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

	"github.com/superdurable/dex/gen/dexpb"
	"github.com/superdurable/dex/service/common/workerclient"
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

type unavailableDispatcher struct{}

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
