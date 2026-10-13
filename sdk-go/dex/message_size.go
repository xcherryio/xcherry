// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package dex

import (
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// DefaultGrpcMaxMessageBytes is the gRPC receive limit that a Client or Worker uses
// when ClientOptions.GrpcMaxMessageBytes or WorkerOptions.GrpcMaxMessageBytes is zero.
//
// The receive limit applies to every message the SDK receives: Client responses such
// as InvokeRPC results and state reads, responses to the Worker's own FlowService
// calls such as LoadBlobs, and the Step and RPC invocations that the Dex server sends
// to the Worker. The default is 32 MiB, which matches the Dex server's default
// api.grpcMaxMessageBytes. With the default, the SDK keeps gRPC's unlimited send
// limit, so the Dex server's receive limit bounds what the SDK sends. A positive
// GrpcMaxMessageBytes limits both directions.
//
// A message carries one or more application values plus an envelope of metadata.
// Keep the limit above the largest application value by an explicit margin; the
// default leaves 16 MiB of headroom for values up to 16 MiB. When an application
// needs larger values, raise both GrpcMaxMessageBytes and the server's
// api.grpcMaxMessageBytes, because each hop applies the smaller limit of its ends.
//
//	client, err := dex.NewClient(registry, cache, dex.ClientOptions{
//		GrpcMaxMessageBytes: 2 * dex.DefaultGrpcMaxMessageBytes,
//	})
const DefaultGrpcMaxMessageBytes = 32 << 20

// A zero sendBytes keeps gRPC's unlimited send default, which earlier SDK releases used.
type grpcMessageLimits struct {
	receiveBytes int
	sendBytes    int
}

func resolveGrpcMessageLimits(configured int) (grpcMessageLimits, error) {
	if configured < 0 {
		return grpcMessageLimits{}, fmt.Errorf("dex: gRPC max message bytes must not be negative, got %d", configured)
	}
	if configured == 0 {
		return grpcMessageLimits{receiveBytes: DefaultGrpcMaxMessageBytes}, nil
	}
	return grpcMessageLimits{receiveBytes: configured, sendBytes: configured}, nil
}

func (limits grpcMessageLimits) flowServiceDialOptions(credential Credential) []grpc.DialOption {
	callOptions := []grpc.CallOption{grpc.MaxCallRecvMsgSize(limits.receiveBytes)}
	if limits.sendBytes > 0 {
		callOptions = append(callOptions, grpc.MaxCallSendMsgSize(limits.sendBytes))
	}
	return append(
		[]grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultCallOptions(callOptions...),
		},
		credentialDialOptions(credential)...,
	)
}

func (limits grpcMessageLimits) workerServiceServerOptions() []grpc.ServerOption {
	serverOptions := []grpc.ServerOption{grpc.MaxRecvMsgSize(limits.receiveBytes)}
	if limits.sendBytes > 0 {
		serverOptions = append(serverOptions, grpc.MaxSendMsgSize(limits.sendBytes))
	}
	return serverOptions
}
