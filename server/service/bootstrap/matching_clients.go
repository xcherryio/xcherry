// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package bootstrap

import (
	"context"
	"errors"
	"sync"

	"github.com/superdurable/dex/gen/dexpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type matchingClientPool struct {
	mu              sync.Mutex
	connections     map[string]*grpc.ClientConn
	token           interface{ Token() string }
	maxMessageBytes int
}

func newMatchingClientPool(token interface{ Token() string }, maxMessageBytes int) *matchingClientPool {
	return &matchingClientPool{connections: make(map[string]*grpc.ClientConn), token: token, maxMessageBytes: maxMessageBytes}
}

func (p *matchingClientPool) MatchingClient(_ context.Context, address string) (dexpb.MatchingServiceClient, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if connection := p.connections[address]; connection != nil {
		return dexpb.NewMatchingServiceClient(connection), nil
	}
	options := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(p.maxMessageBytes), grpc.MaxCallSendMsgSize(p.maxMessageBytes)),
	}
	if p.token != nil {
		options = append(options, grpc.WithPerRPCCredentials(&matchingCredentials{token: p.token}))
	}
	connection, err := grpc.NewClient(address, options...)
	if err != nil {
		return nil, err
	}
	p.connections[address] = connection
	return dexpb.NewMatchingServiceClient(connection), nil
}

func (p *matchingClientPool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	var closeErrors []error
	for address, connection := range p.connections {
		if err := connection.Close(); err != nil {
			closeErrors = append(closeErrors, err)
		}
		delete(p.connections, address)
	}
	return errors.Join(closeErrors...)
}

type matchingCredentials struct{ token interface{ Token() string } }

func (c *matchingCredentials) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer " + c.token.Token()}, nil
}

func (c *matchingCredentials) RequireTransportSecurity() bool { return false }
