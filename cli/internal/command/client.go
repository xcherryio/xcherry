// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package command

import (
	"context"
	"errors"
	"fmt"

	"github.com/superdurable/dex/gen/dexpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

const defaultMaxMessageBytes = 32 << 20

type flowService struct {
	connection *grpc.ClientConn
	service    dexpb.FlowServiceClient
}

func withFlowService(
	ctx context.Context,
	options options,
	call func(context.Context, *flowService) error,
) error {
	client, err := newFlowService(options.server)
	if err != nil {
		return newOperationError("connect", err)
	}
	callCtx, cancel := requestContext(ctx, options)
	callErr := call(callCtx, client)
	cancel()
	closeErr := client.connection.Close()
	if callErr != nil {
		return callErr
	}
	if closeErr != nil {
		return newOperationError("close", closeErr)
	}
	return nil
}

func withStreamingFlowService(
	ctx context.Context,
	options options,
	call func(*flowService) error,
) error {
	client, err := newFlowService(options.server)
	if err != nil {
		return newOperationError("connect", err)
	}
	callErr := call(client)
	closeErr := client.connection.Close()
	if callErr != nil {
		return callErr
	}
	if closeErr != nil {
		return newOperationError("close", closeErr)
	}
	return nil
}

func newFlowService(server string) (*flowService, error) {
	connection, err := grpc.NewClient(
		server,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(defaultMaxMessageBytes),
			grpc.MaxCallSendMsgSize(defaultMaxMessageBytes),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("create FlowService client: %w", err)
	}
	return &flowService{
		connection: connection,
		service:    dexpb.NewFlowServiceClient(connection),
	}, nil
}

func requestContext(ctx context.Context, options options) (context.Context, context.CancelFunc) {
	if options.timeout == 0 {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, options.timeout)
}

func closeWithError(primary error, closer func() error) error {
	return errors.Join(primary, closer())
}

func emptyRequest() *emptypb.Empty {
	return &emptypb.Empty{}
}
