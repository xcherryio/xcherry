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
	"github.com/superdurable/dex/service/membership"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type matchingClientPool struct {
	mu              sync.Mutex
	connections     map[string]*matchingConnection
	token           interface{ Token() string }
	maxMessageBytes int
	membership      *membership.Monitor
	logger          *zap.Logger
	changes         <-chan membership.Change
	unsubscribe     func()
	stop            chan struct{}
	stopped         chan struct{}
	closed          bool
}

type matchingConnection struct {
	address    string
	connection *grpc.ClientConn
	pool       *matchingClientPool
	active     int
	retired    bool
}

type trackedMatchingStream struct {
	grpc.ClientStream
	pool       *matchingClientPool
	connection *matchingConnection
	once       sync.Once
	done       chan struct{}
}

func newMatchingClientPool(token interface{ Token() string }, maxMessageBytes int, members *membership.Monitor, logger *zap.Logger) *matchingClientPool {
	if members == nil || logger == nil {
		panic("matching client pool dependencies are required")
	}
	changes, unsubscribe := members.Subscribe()
	pool := &matchingClientPool{connections: make(map[string]*matchingConnection), token: token, maxMessageBytes: maxMessageBytes, membership: members, logger: logger, changes: changes, unsubscribe: unsubscribe, stop: make(chan struct{}), stopped: make(chan struct{})}
	go pool.watchMembership()
	return pool
}

func (p *matchingClientPool) MatchingClient(_ context.Context, address string) (dexpb.MatchingServiceClient, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, errors.New("matching client pool is closed")
	}
	if existing := p.connections[address]; existing != nil {
		return dexpb.NewMatchingServiceClient(existing.connection), nil
	}
	entry := &matchingConnection{address: address, pool: p}
	options := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(p.maxMessageBytes), grpc.MaxCallSendMsgSize(p.maxMessageBytes)),
		grpc.WithStreamInterceptor(entry.trackStream),
	}
	if p.token != nil {
		options = append(options, grpc.WithPerRPCCredentials(&matchingCredentials{token: p.token}))
	}
	connection, err := grpc.NewClient(address, options...)
	if err != nil {
		return nil, err
	}
	entry.connection = connection
	p.connections[address] = entry
	return dexpb.NewMatchingServiceClient(connection), nil
}

func (p *matchingClientPool) Close() error {
	close(p.stop)
	<-p.stopped
	p.mu.Lock()
	p.closed = true
	connections := p.connections
	p.connections = make(map[string]*matchingConnection)
	p.mu.Unlock()
	var closeErrors []error
	for _, entry := range connections {
		if err := entry.connection.Close(); err != nil {
			closeErrors = append(closeErrors, err)
		}
	}
	return errors.Join(closeErrors...)
}

func (p *matchingClientPool) watchMembership() {
	defer close(p.stopped)
	defer p.unsubscribe()
	for {
		select {
		case <-p.stop:
			return
		case change, isOpen := <-p.changes:
			if !isOpen {
				return
			}
			if change.Role == membership.RoleMatching {
				p.evictAbsentMembers()
			}
		}
	}
}

func (p *matchingClientPool) evictAbsentMembers() {
	addresses := make(map[string]struct{})
	for _, member := range p.membership.Members(membership.RoleMatching) {
		addresses[member.RPCAddress] = struct{}{}
	}
	p.mu.Lock()
	retired := make([]*grpc.ClientConn, 0)
	for address, entry := range p.connections {
		if _, isMember := addresses[address]; isMember {
			continue
		}
		delete(p.connections, address)
		entry.retired = true
		if entry.active == 0 {
			retired = append(retired, entry.connection)
		}
	}
	p.mu.Unlock()
	for _, connection := range retired {
		if err := connection.Close(); err != nil {
			p.logger.Error("close departed matching client", zap.Error(err))
		}
	}
}

func (entry *matchingConnection) trackStream(ctx context.Context, descriptor *grpc.StreamDesc, connection *grpc.ClientConn, method string, streamer grpc.Streamer, options ...grpc.CallOption) (grpc.ClientStream, error) {
	p := entry.pool
	p.mu.Lock()
	if entry.retired || p.closed {
		p.mu.Unlock()
		return nil, errors.New("matching owner has left membership")
	}
	entry.active++
	p.mu.Unlock()
	stream, err := streamer(ctx, descriptor, connection, method, options...)
	if err != nil {
		p.releaseStream(entry)
		return nil, err
	}
	tracked := &trackedMatchingStream{ClientStream: stream, pool: p, connection: entry, done: make(chan struct{})}
	go tracked.watchContext()
	return tracked, nil
}

func (p *matchingClientPool) releaseStream(entry *matchingConnection) {
	p.mu.Lock()
	entry.active--
	shouldClose := entry.retired && entry.active == 0
	p.mu.Unlock()
	if shouldClose {
		if err := entry.connection.Close(); err != nil {
			p.logger.Error("close departed matching client", zap.Error(err))
		}
	}
}

func (s *trackedMatchingStream) watchContext() {
	select {
	case <-s.Context().Done():
		s.finish()
	case <-s.done:
	}
}

func (s *trackedMatchingStream) RecvMsg(message any) error {
	err := s.ClientStream.RecvMsg(message)
	if err != nil {
		s.finish()
	}
	return err
}

func (s *trackedMatchingStream) SendMsg(message any) error {
	err := s.ClientStream.SendMsg(message)
	if err != nil {
		s.finish()
	}
	return err
}

func (s *trackedMatchingStream) finish() {
	s.once.Do(func() {
		close(s.done)
		s.pool.releaseStream(s.connection)
	})
}

type matchingCredentials struct{ token interface{ Token() string } }

func (c *matchingCredentials) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer " + c.token.Token()}, nil
}

func (c *matchingCredentials) RequireTransportSecurity() bool { return false }
