// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package dex

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/superdurable/dex/blob-cache-go/blobcache"
	"github.com/superdurable/dex/sdk-go/gen/dexpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

const (
	apiKeyAccessTokenA = "access-token-a"
	apiKeyAccessTokenB = "access-token-b"
)

// bearerTokenGate rejects calls whose bearer token it does not accept and records every attempt.
type bearerTokenGate struct {
	mu         sync.Mutex
	isAccepted func(authorization string) bool
	attempts   map[string][]string
	rejections map[string]int
}

func newBearerTokenGate(isAccepted func(authorization string) bool) *bearerTokenGate {
	return &bearerTokenGate{isAccepted: isAccepted, attempts: map[string][]string{}, rejections: map[string]int{}}
}

func (gate *bearerTokenGate) authorize(ctx context.Context, fullMethod string) error {
	incoming, _ := metadata.FromIncomingContext(ctx)
	authorization := strings.Join(incoming.Get("authorization"), ",")
	gate.mu.Lock()
	defer gate.mu.Unlock()
	gate.attempts[fullMethod] = append(gate.attempts[fullMethod], authorization)
	if gate.isAccepted(authorization) {
		return nil
	}
	gate.rejections[fullMethod]++
	return status.Error(codes.Unauthenticated, "bearer token rejected")
}

func (gate *bearerTokenGate) interceptUnary(
	ctx context.Context,
	request any,
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (any, error) {
	if err := gate.authorize(ctx, info.FullMethod); err != nil {
		return nil, err
	}
	return handler(ctx, request)
}

func (gate *bearerTokenGate) interceptStream(
	service any,
	stream grpc.ServerStream,
	info *grpc.StreamServerInfo,
	handler grpc.StreamHandler,
) error {
	if err := gate.authorize(stream.Context(), info.FullMethod); err != nil {
		return err
	}
	return handler(service, stream)
}

func (gate *bearerTokenGate) acceptOnly(authorization string) {
	gate.mu.Lock()
	defer gate.mu.Unlock()
	gate.isAccepted = func(received string) bool { return received == authorization }
}

func (gate *bearerTokenGate) attemptsFor(fullMethod string) []string {
	gate.mu.Lock()
	defer gate.mu.Unlock()
	return append([]string(nil), gate.attempts[fullMethod]...)
}

func (gate *bearerTokenGate) rejectionsFor(fullMethod string) int {
	gate.mu.Lock()
	defer gate.mu.Unlock()
	return gate.rejections[fullMethod]
}

type healthCheckFlowService struct {
	workerSyncFlowService
}

func (*healthCheckFlowService) HealthCheck(context.Context, *emptypb.Empty) (*dexpb.HealthInfo, error) {
	return &dexpb.HealthInfo{Condition: "OK"}, nil
}

func startBearerTokenGatedServer(
	t *testing.T,
	gate *bearerTokenGate,
	workerTaskService dexpb.WorkerTaskServiceServer,
) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	grpcServer := grpc.NewServer(grpc.UnaryInterceptor(gate.interceptUnary), grpc.StreamInterceptor(gate.interceptStream))
	dexpb.RegisterFlowServiceServer(grpcServer, &healthCheckFlowService{})
	if workerTaskService != nil {
		dexpb.RegisterWorkerTaskServiceServer(grpcServer, workerTaskService)
	}
	serveResult := make(chan error, 1)
	go func() { serveResult <- grpcServer.Serve(listener) }()
	t.Cleanup(func() {
		grpcServer.Stop()
		requireServeStopped(t, <-serveResult)
	})
	return listener.Addr().String()
}

// startSequentialAPIKeyTokenEndpoint hands out access-token-a, then access-token-b, and so on.
func startSequentialAPIKeyTokenEndpoint(t *testing.T) *apiKeyTokenEndpoint {
	return startAPIKeyTokenEndpoint(t, func(requestNumber int, writer http.ResponseWriter, _ *http.Request) {
		writeAPIKeyAccessToken(writer, fmt.Sprintf("access-token-%c", 'a'+requestNumber-1), 3600)
	})
}

func newAPIKeyTestClient(t *testing.T, address string, credential Credential) *Client {
	t.Helper()
	registry, err := NewRegistry(nil)
	require.NoError(t, err)
	cache, err := blobcache.New(&blobcache.Config{Dir: t.TempDir(), MaxBytes: 1 << 20})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cache.Close()) })
	client, err := NewClient(registry, cache, ClientOptions{FlowServiceAddress: address, Credential: credential})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	return client
}

func TestClientRetriesRejectedUnaryCallOnceWithFreshAPIKeyToken(t *testing.T) {
	gate := newBearerTokenGate(func(authorization string) bool { return authorization == "Bearer "+apiKeyAccessTokenB })
	address := startBearerTokenGatedServer(t, gate, nil)
	tokenEndpoint := startSequentialAPIKeyTokenEndpoint(t)
	credential, err := APIKeyCredential(tokenEndpoint.tokenURL(), testAPIServerID, testAPIKey)
	require.NoError(t, err)
	client := newAPIKeyTestClient(t, address, credential)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	health, err := client.HealthCheck(ctx)
	require.NoError(t, err)
	require.Equal(t, "OK", health.Condition)
	require.Equal(t,
		[]string{"Bearer " + apiKeyAccessTokenA, "Bearer " + apiKeyAccessTokenB},
		gate.attemptsFor(dexpb.FlowService_HealthCheck_FullMethodName),
		"the rejected call is retried exactly once with a fresh token",
	)
	require.Equal(t, 2, tokenEndpoint.exchangeCount(), "the rejection causes exactly one exchange")

	_, err = client.HealthCheck(ctx)
	require.NoError(t, err)
	require.Len(t, gate.attemptsFor(dexpb.FlowService_HealthCheck_FullMethodName), 3)
	require.Equal(t, 2, tokenEndpoint.exchangeCount(), "the fresh token is cached")
}

func TestClientFailsUnaryCallAfterSecondRejectedAPIKeyToken(t *testing.T) {
	gate := newBearerTokenGate(func(string) bool { return false })
	address := startBearerTokenGatedServer(t, gate, nil)
	tokenEndpoint := startSequentialAPIKeyTokenEndpoint(t)
	credential, err := APIKeyCredential(tokenEndpoint.tokenURL(), testAPIServerID, testAPIKey)
	require.NoError(t, err)
	client := newAPIKeyTestClient(t, address, credential)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err = client.HealthCheck(ctx)
	var serviceError *ServiceError
	require.True(t, errors.As(err, &serviceError), "error: %v", err)
	require.Equal(t, codes.Unauthenticated, serviceError.Code)
	require.Equal(t,
		[]string{"Bearer " + apiKeyAccessTokenA, "Bearer " + apiKeyAccessTokenB},
		gate.attemptsFor(dexpb.FlowService_HealthCheck_FullMethodName),
		"a second rejection fails the call without another attempt",
	)
	require.Equal(t, 2, tokenEndpoint.exchangeCount())
}

func TestWorkerPollStreamDiscardsRejectedAPIKeyTokenAndReconnectsWithFreshToken(t *testing.T) {
	gate := newBearerTokenGate(func(string) bool { return true })
	pullServer := &pullTestServer{registered: make(chan *pullTestStream, 32), firstReconnectQueue: make(chan string, 1)}
	address := startBearerTokenGatedServer(t, gate, pullServer)
	tokenEndpoint := startSequentialAPIKeyTokenEndpoint(t)
	credential, err := APIKeyCredential(tokenEndpoint.tokenURL(), testAPIServerID, testAPIKey)
	require.NoError(t, err)
	worker, err := newWorkerForTest(t, []Flow{workerFlow}, WorkerOptions{
		FlowServiceAddress: address,
		Credential:         credential,
		Pull:               &PullWorkerOptions{MaxConcurrentTasks: 1, AppWorkerID: "api-key-worker-12345678"},
	})
	require.NoError(t, err)
	started := make(chan error, 1)
	go func() { started <- worker.Start() }()
	registeredByQueue := map[string]*pullTestStream{}
	for len(registeredByQueue) < 2 {
		stream := awaitPullRegistration(t, pullServer)
		registeredByQueue[stream.registration.Queue] = stream
	}
	flowTypeStream := registeredByQueue[GetFinalFlowType(workerFlow)]
	appWorkerStream := registeredByQueue["api-key-worker-12345678"]
	require.NotNil(t, flowTypeStream)
	require.NotNil(t, appWorkerStream)
	require.Equal(t, "Bearer "+apiKeyAccessTokenA, pullStreamAuthorization(flowTypeStream))
	require.Equal(t, "Bearer "+apiKeyAccessTokenA, pullStreamAuthorization(appWorkerStream))
	require.Equal(t, 1, tokenEndpoint.exchangeCount())

	gate.acceptOnly("Bearer " + apiKeyAccessTokenB)
	close(flowTypeStream.abort)
	apiKey := credential.(*apiKeyCredential)
	require.Eventually(t, func() bool {
		apiKey.mu.Lock()
		defer apiKey.mu.Unlock()
		return apiKey.token == ""
	}, 5*time.Second, 10*time.Millisecond, "the rejected stream discards its token")
	require.Equal(t, 1, gate.rejectionsFor(dexpb.WorkerTaskService_PollWorkerTask_FullMethodName))
	require.Never(t, func() bool {
		return gate.rejectionsFor(dexpb.WorkerTaskService_PollWorkerTask_FullMethodName) > 1
	}, time.Second, 50*time.Millisecond, "a rejected stream gets no extra retry")
	require.Equal(t, 1, tokenEndpoint.exchangeCount(), "discarding a token does not exchange by itself")

	close(appWorkerStream.abort)
	reconnected := awaitPullRegistration(t, pullServer)
	require.Equal(t, "api-key-worker-12345678", reconnected.registration.Queue)
	require.Equal(t, "Bearer "+apiKeyAccessTokenB, pullStreamAuthorization(reconnected),
		"the existing reconnect path picks up a fresh token")
	require.Equal(t, 2, tokenEndpoint.exchangeCount())
	require.Equal(t, 1, gate.rejectionsFor(dexpb.WorkerTaskService_PollWorkerTask_FullMethodName))

	stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, worker.Stop(stopCtx))
	require.NoError(t, <-started)
}

func pullStreamAuthorization(stream *pullTestStream) string {
	incoming, _ := metadata.FromIncomingContext(stream.stream.Context())
	return strings.Join(incoming.Get("authorization"), ",")
}
