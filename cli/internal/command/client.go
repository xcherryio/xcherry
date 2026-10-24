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
	"github.com/superdurable/dex/service/common/credential"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
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
	client, err := newFlowService(options)
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
	client, err := newFlowService(options)
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

func newFlowService(options options) (*flowService, error) {
	perRPCCredential, err := newPerRPCCredential(options)
	if err != nil {
		return nil, err
	}
	dialOptions := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(defaultMaxMessageBytes),
			grpc.MaxCallSendMsgSize(defaultMaxMessageBytes),
		),
	}
	if perRPCCredential != nil {
		dialOptions = append(dialOptions, grpc.WithPerRPCCredentials(perRPCCredential))
	}
	connection, err := grpc.NewClient(
		options.server,
		dialOptions...,
	)
	if err != nil {
		return nil, fmt.Errorf("create FlowService client: %w", err)
	}
	return &flowService{
		connection: connection,
		service:    dexpb.NewFlowServiceClient(connection),
	}, nil
}

func newPerRPCCredential(options options) (credentials.PerRPCCredentials, error) {
	if err := options.validateAPIKeySettings(); err != nil {
		return nil, newUsageError("dexcli", err)
	}
	switch {
	case options.hasAPIKeySettings():
		apiKey, err := options.readAPIKey()
		if err != nil {
			return nil, err
		}
		return newAPIKeyCredential(options.tokenURL, options.serverID, apiKey)
	case options.hasFile || options.credentialFile != "":
		return credential.File(options.credentialFile)
	case options.hasCredential || options.credential != "":
		return credential.Static(options.credential)
	default:
		return nil, nil
	}
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
