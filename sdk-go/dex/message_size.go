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

// DefaultGrpcMaxMessageBytes is the gRPC message size limit that a Client or Worker uses
// when ClientOptions.GrpcMaxMessageBytes or WorkerOptions.GrpcMaxMessageBytes is zero.
//
// The limit applies to every message the SDK sends or receives: Client requests and
// responses such as InvokeRPC results and state reads, the Worker's own FlowService
// calls such as LoadBlobs, and the requests and results that the Dex server exchanges
// with the Worker. The default is 32 MiB, which matches the Dex server's default
// api.grpcMaxMessageBytes.
//
// A message carries one or more application values plus an envelope of metadata.
// Keep the limit above the largest application value by an explicit margin; the
// default leaves 16 MiB of headroom for values up to 16 MiB. When an application
// needs larger values, raise both this option and the server's
// api.grpcMaxMessageBytes, because the smaller of the two limits applies.
//
//	client, err := dex.NewClient(registry, cache, dex.ClientOptions{
//		GrpcMaxMessageBytes: 2 * dex.DefaultGrpcMaxMessageBytes,
//	})
const DefaultGrpcMaxMessageBytes = 32 << 20

func resolveGrpcMaxMessageBytes(configured int) (int, error) {
	if configured < 0 {
		return 0, fmt.Errorf("dex: gRPC max message bytes must not be negative, got %d", configured)
	}
	if configured == 0 {
		return DefaultGrpcMaxMessageBytes, nil
	}
	return configured, nil
}

func flowServiceDialOptions(credential Credential, maxMessageBytes int) []grpc.DialOption {
	return append(
		[]grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultCallOptions(
				grpc.MaxCallRecvMsgSize(maxMessageBytes),
				grpc.MaxCallSendMsgSize(maxMessageBytes),
			),
		},
		credentialDialOptions(credential)...,
	)
}
