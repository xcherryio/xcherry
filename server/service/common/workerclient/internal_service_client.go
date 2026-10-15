// Legacy Materials in this file remain under their original licenses.
// See LEGACY_NOTICES.md.

// Modifications Copyright (c) 2026 Super Durable, Inc.
//
// Modifications after the Legacy Cutoff are licensed under the
// Sustainable Use License 1.0.
// Legacy Materials remain under their original licenses.
// See LICENSE and LEGACY_NOTICES.md.

package workerclient

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/superdurable/dex/config"
	"github.com/superdurable/dex/gen/dexpb"
	"github.com/superdurable/dex/service/common/grpctarget"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// InternalServiceClient owns the reusable continue-as-new dump connection.
type InternalServiceClient struct {
	mu     sync.Mutex
	conn   *grpc.ClientConn
	client dexpb.InternalServiceClient
	header metadata.MD
	// internalToken is nil when the internal listener has no token.
	internalToken interface{ Token() string }
	closed        bool
}

// NewInternalServiceClient creates the continue-as-new dump client.
// internalToken is nil without internal authentication; otherwise every call carries it as a bearer credential.
func NewInternalServiceClient(
	target string,
	cfg *config.Config,
	internalToken interface{ Token() string },
) (*InternalServiceClient, error) {
	if cfg == nil {
		panic("workerclient: config must not be nil")
	}
	maxMessageBytes := cfg.Api.EffectiveGrpcMaxMessageBytes()
	if maxMessageBytes <= 0 {
		return nil, fmt.Errorf("workerclient: GrpcMaxMessageBytes must be positive, got %d", cfg.Api.GrpcMaxMessageBytes)
	}
	if err := ValidateDefaultHeaders(cfg.Worker.DefaultHeaders); err != nil {
		return nil, err
	}
	normalized, err := grpctarget.NormalizeAddress(target)
	if err != nil {
		return nil, err
	}
	conn, err := grpc.NewClient(
		normalized,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(maxMessageBytes),
			grpc.MaxCallSendMsgSize(maxMessageBytes),
		),
	)
	if err != nil {
		return nil, err
	}
	return &InternalServiceClient{
		conn:          conn,
		client:        dexpb.NewInternalServiceClient(conn),
		header:        metadata.New(cfg.Worker.DefaultHeaders),
		internalToken: internalToken,
	}, nil
}

// Client returns the generated client with default headers.
func (i *InternalServiceClient) Client(
	ctx context.Context,
) (dexpb.InternalServiceClient, context.Context, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.closed || i.client == nil {
		return nil, ctx, fmt.Errorf("workerclient: internal client closed")
	}
	header := i.header
	if i.internalToken != nil {
		header = header.Copy()
		header.Set("authorization", "Bearer "+string(i.internalToken.Token()))
	}
	if len(header) == 0 {
		return i.client, ctx, nil
	}
	return i.client, metadata.NewOutgoingContext(ctx, header), nil
}

// MatchingClient returns the matching client with internal authentication.
func (i *InternalServiceClient) MatchingClient(
	ctx context.Context,
) (dexpb.MatchingServiceClient, context.Context, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.closed || i.conn == nil {
		return nil, ctx, fmt.Errorf("workerclient: internal client closed")
	}
	header := i.header
	if i.internalToken != nil {
		header = header.Copy()
		header.Set("authorization", "Bearer "+i.internalToken.Token())
	}
	if len(header) != 0 {
		ctx = metadata.NewOutgoingContext(ctx, header)
	}
	return dexpb.NewMatchingServiceClient(i.conn), ctx, nil
}

// Close closes the InternalService connection.
func (i *InternalServiceClient) Close() {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.closed {
		return
	}
	i.closed = true
	if i.conn == nil {
		return
	}
	if err := i.conn.Close(); err != nil {
		log.Printf("workerclient: close internal connection: %v", err)
	}
	i.conn = nil
	i.client = nil
}
