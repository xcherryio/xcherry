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
	"math/rand/v2"
	"sync"
	"time"

	"github.com/superdurable/dex/gen/dexpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

const (
	initialFailoverBackoff = 100 * time.Millisecond
	maxFailoverBackoff     = time.Second
)

type resolvedWorkerAddressContextKey struct{}

type resolvedWorkerAddressState struct {
	mu      sync.RWMutex
	address string
}

type routedWorkerClient struct {
	pool       *WorkerClientPool
	target     string
	isHeadless bool
	flowID     string
	routeState *resolvedWorkerAddressState

	mu       sync.Mutex
	address  string
	conn     *grpc.ClientConn
	released bool
}

func newResolvedWorkerAddressContext(
	ctx context.Context,
	address string,
) (context.Context, *resolvedWorkerAddressState) {
	state := &resolvedWorkerAddressState{address: address}
	return context.WithValue(ctx, resolvedWorkerAddressContextKey{}, state), state
}

// ResolvedWorkerAddressFromContext returns the endpoint used by the acquired client.
func ResolvedWorkerAddressFromContext(ctx context.Context) string {
	state, ok := ctx.Value(resolvedWorkerAddressContextKey{}).(*resolvedWorkerAddressState)
	if !ok {
		return ""
	}
	return state.get()
}

func (c *routedWorkerClient) InvokeWaitForMethod(
	ctx context.Context,
	req *dexpb.InvokeWaitForMethodRequest,
	opts ...grpc.CallOption,
) (grpc.ServerStreamingClient[dexpb.InvokeWaitForMethodOutput], error) {
	return newRoutedServerStream(c, ctx, func(
		client dexpb.WorkerServiceClient,
	) (grpc.ServerStreamingClient[dexpb.InvokeWaitForMethodOutput], error) {
		return client.InvokeWaitForMethod(ctx, req, opts...)
	})
}

func (c *routedWorkerClient) InvokeExecuteMethod(
	ctx context.Context,
	req *dexpb.InvokeExecuteMethodRequest,
	opts ...grpc.CallOption,
) (grpc.ServerStreamingClient[dexpb.InvokeExecuteMethodOutput], error) {
	return newRoutedServerStream(c, ctx, func(
		client dexpb.WorkerServiceClient,
	) (grpc.ServerStreamingClient[dexpb.InvokeExecuteMethodOutput], error) {
		return client.InvokeExecuteMethod(ctx, req, opts...)
	})
}

type routedServerStream[Output any] struct {
	grpc.ServerStreamingClient[Output]

	client      *routedWorkerClient
	ctx         context.Context
	open        func(dexpb.WorkerServiceClient) (grpc.ServerStreamingClient[Output], error)
	failed      map[string]struct{}
	attempts    int
	hasReceived bool
}

func newRoutedServerStream[Output any](
	client *routedWorkerClient,
	ctx context.Context,
	open func(dexpb.WorkerServiceClient) (grpc.ServerStreamingClient[Output], error),
) (grpc.ServerStreamingClient[Output], error) {
	client.mu.Lock()
	defer client.mu.Unlock()
	stream := &routedServerStream[Output]{
		client: client,
		ctx:    ctx,
		open:   open,
		failed: make(map[string]struct{}),
	}
	if err := stream.openBeforeFirstFrame(); err != nil {
		return nil, err
	}
	return stream, nil
}

func (s *routedServerStream[Output]) Recv() (*Output, error) {
	s.client.mu.Lock()
	defer s.client.mu.Unlock()
	for {
		output, err := s.ServerStreamingClient.Recv()
		if err == nil {
			s.hasReceived = true
			s.client.pool.rememberSticky(s.client.target, s.client.flowID, s.client.address)
			return output, nil
		}
		if s.hasReceived || !s.canFailover(err) {
			return nil, err
		}
		s.markCurrentAddressFailed()
		if err := s.openBeforeFirstFrame(); err != nil {
			return nil, err
		}
	}
}

func (s *routedServerStream[Output]) openBeforeFirstFrame() error {
	var lastErr error
	maxAttempts := 1
	if s.client.isHeadless {
		maxAttempts = s.client.pool.cfg.Worker.EffectiveWorkerServiceRequestMaxAttempts()
	}
	for s.attempts < maxAttempts {
		if s.attempts > 0 {
			if err := waitForFailover(s.ctx, failoverBackoff(s.attempts)); err != nil {
				return err
			}
			nextAddress, err := s.client.pool.nextFailoverAddress(
				s.client.target,
				s.client.address,
				s.failed,
			)
			if err != nil {
				return err
			}
			if err := s.client.switchAddress(nextAddress); err != nil {
				return err
			}
		}
		s.attempts++
		if s.client.released || s.client.conn == nil {
			return fmt.Errorf("workerclient: acquired client released")
		}
		stream, err := s.open(dexpb.NewWorkerServiceClient(s.client.conn))
		if err == nil {
			s.ServerStreamingClient = stream
			return nil
		}
		lastErr = err
		if !s.canFailover(err) {
			return err
		}
		s.markCurrentAddressFailed()
	}
	return lastErr
}

func (s *routedServerStream[Output]) canFailover(err error) bool {
	return s.client.isHeadless && s.client.pool.isFailoverWorkerServiceError(s.ctx, err)
}

func (s *routedServerStream[Output]) markCurrentAddressFailed() {
	s.failed[s.client.address] = struct{}{}
	s.client.pool.forgetSticky(s.client.target, s.client.flowID, s.client.address)
}

func (c *routedWorkerClient) InvokeWorkerRPC(
	ctx context.Context,
	req *dexpb.InvokeWorkerRPCRequest,
	opts ...grpc.CallOption,
) (*dexpb.InvokeWorkerRPCResponse, error) {
	response, err := c.invoke(ctx, func(client dexpb.WorkerServiceClient) (interface{}, error) {
		return client.InvokeWorkerRPC(ctx, req, opts...)
	})
	if err != nil {
		return nil, err
	}
	return response.(*dexpb.InvokeWorkerRPCResponse), nil
}

func (c *routedWorkerClient) invoke(
	ctx context.Context,
	call func(dexpb.WorkerServiceClient) (interface{}, error),
) (interface{}, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.released || c.conn == nil {
		return nil, fmt.Errorf("workerclient: acquired client released")
	}
	if !c.isHeadless {
		return call(dexpb.NewWorkerServiceClient(c.conn))
	}
	return c.invokeHeadless(ctx, call)
}

func (c *routedWorkerClient) invokeHeadless(
	ctx context.Context,
	call func(dexpb.WorkerServiceClient) (interface{}, error),
) (interface{}, error) {
	failed := make(map[string]struct{})
	var lastErr error
	for attempt := 0; attempt < c.pool.cfg.Worker.EffectiveWorkerServiceRequestMaxAttempts(); attempt++ {
		if attempt > 0 {
			if err := waitForFailover(ctx, failoverBackoff(attempt)); err != nil {
				return nil, err
			}
			nextAddress, err := c.pool.nextFailoverAddress(c.target, c.address, failed)
			if err != nil {
				return nil, err
			}
			if err := c.switchAddress(nextAddress); err != nil {
				return nil, err
			}
		}
		response, err := call(dexpb.NewWorkerServiceClient(c.conn))
		if err == nil {
			c.pool.rememberSticky(c.target, c.flowID, c.address)
			return response, nil
		}
		lastErr = err
		if !c.pool.isFailoverWorkerServiceError(ctx, err) {
			return nil, err
		}
		failed[c.address] = struct{}{}
		c.pool.forgetSticky(c.target, c.flowID, c.address)
	}
	return nil, lastErr
}

func (c *routedWorkerClient) switchAddress(address string) error {
	if address == c.address {
		return nil
	}
	c.pool.releaseConn(c.address)
	c.conn = nil
	conn, err := c.pool.acquireConn(address)
	if err != nil {
		return err
	}
	c.address = address
	c.conn = conn
	c.routeState.set(address)
	return nil
}

func (c *routedWorkerClient) release() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.released {
		return
	}
	c.released = true
	if c.conn != nil {
		c.pool.releaseConn(c.address)
		c.conn = nil
	}
}

func (p *WorkerClientPool) isFailoverWorkerServiceError(ctx context.Context, err error) bool {
	if ctx.Err() != nil || isWorkerApplicationError(err) {
		return false
	}
	_, ok := p.failoverStatusCodes[status.Code(err)]
	return ok
}

// isWorkerApplicationError reports a handler failure the Worker returned, which another endpoint would repeat.
func isWorkerApplicationError(err error) bool {
	for _, detail := range status.Convert(err).Details() {
		if _, ok := detail.(*dexpb.WorkerErrorResponse); ok {
			return true
		}
	}
	return false
}

func (s *resolvedWorkerAddressState) set(address string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.address = address
}

func (s *resolvedWorkerAddressState) get() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.address
}

func failoverBackoff(failover int) time.Duration {
	backoff := initialFailoverBackoff
	for index := 1; index < failover && backoff < maxFailoverBackoff; index++ {
		backoff *= 2
		if backoff > maxFailoverBackoff {
			backoff = maxFailoverBackoff
		}
	}
	half := backoff / 2
	return half + time.Duration(rand.Int64N(int64(half)+1))
}

func waitForFailover(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
