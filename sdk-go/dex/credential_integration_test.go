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
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/superdurable/dex/blob-cache-go/blobcache"
	"github.com/superdurable/dex/sdk-go/gen/dexpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/emptypb"
)

// credentialRecorder records the authorization metadata of every FlowService call it receives.
type credentialRecorder struct {
	mu             sync.Mutex
	authorizations map[string][]string
}

func (recorder *credentialRecorder) intercept(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	incoming, _ := metadata.FromIncomingContext(ctx)
	recorder.mu.Lock()
	recorder.authorizations[info.FullMethod] = append(recorder.authorizations[info.FullMethod], incoming.Get("authorization")...)
	recorder.mu.Unlock()
	return handler(ctx, req)
}

func (recorder *credentialRecorder) received(fullMethod string) []string {
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	return append([]string(nil), recorder.authorizations[fullMethod]...)
}

func startCredentialRecordingFlowService(t *testing.T, service dexpb.FlowServiceServer) (string, *credentialRecorder) {
	t.Helper()
	recorder := &credentialRecorder{authorizations: map[string][]string{}}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	grpcServer := grpc.NewServer(grpc.UnaryInterceptor(recorder.intercept))
	dexpb.RegisterFlowServiceServer(grpcServer, service)
	serveResult := make(chan error, 1)
	go func() { serveResult <- grpcServer.Serve(listener) }()
	t.Cleanup(func() {
		grpcServer.Stop()
		if err := listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			require.NoError(t, err)
		}
		requireServeStopped(t, <-serveResult)
	})
	return listener.Addr().String(), recorder
}

func writeCredentialFile(t *testing.T, path string, token string) {
	t.Helper()
	require.NoError(t, os.WriteFile(path, []byte(token+"\n"), 0o600))
}

func TestClientSendsAndRereadsFileCredential(t *testing.T) {
	address, recorder := startCredentialRecordingFlowService(t, &workerSyncFlowService{})
	tokenPath := filepath.Join(t.TempDir(), "token")
	writeCredentialFile(t, tokenPath, "first-token")
	credential, err := FileCredential(tokenPath)
	require.NoError(t, err)
	registry, err := NewRegistry(nil)
	require.NoError(t, err)
	cache, err := blobcache.New(&blobcache.Config{Dir: t.TempDir(), MaxBytes: 1 << 20})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cache.Close()) })
	client, err := NewClient(registry, cache, ClientOptions{FlowServiceAddress: address, Credential: credential})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err = client.service.GetServerInfo(ctx, &emptypb.Empty{})
	require.NoError(t, err)
	require.Equal(t, []string{"Bearer first-token"}, recorder.received(dexpb.FlowService_GetServerInfo_FullMethodName))

	writeCredentialFile(t, tokenPath, "rotated-token")
	require.Eventually(t, func() bool {
		_, err := client.service.GetServerInfo(ctx, &emptypb.Empty{})
		require.NoError(t, err)
		received := recorder.received(dexpb.FlowService_GetServerInfo_FullMethodName)
		return received[len(received)-1] == "Bearer rotated-token"
	}, 5*time.Second, 100*time.Millisecond, "a rotated token takes effect without a restart")

	require.NoError(t, os.WriteFile(tokenPath, nil, 0o600))
	callsBefore := len(recorder.received(dexpb.FlowService_GetServerInfo_FullMethodName))
	require.Eventually(t, func() bool {
		_, err := client.service.GetServerInfo(ctx, &emptypb.Empty{})
		return err != nil
	}, 5*time.Second, 100*time.Millisecond, "an emptied token file fails calls instead of sending a stale token")
	received := recorder.received(dexpb.FlowService_GetServerInfo_FullMethodName)
	require.NotContains(t, received[callsBefore:], "")

	_, err = FileCredential(filepath.Join(t.TempDir(), "missing"))
	require.Error(t, err)
}

func TestWorkerSendsCredentialWithStartupCalls(t *testing.T) {
	address, recorder := startCredentialRecordingFlowService(t, &workerSyncFlowService{})
	tokenPath := filepath.Join(t.TempDir(), "token")
	writeCredentialFile(t, tokenPath, "worker-token")
	credential, err := FileCredential(tokenPath)
	require.NoError(t, err)
	worker, err := newWorkerForTest(t, []Flow{&registrationFlow{flowType: "credential-worker"}}, WorkerOptions{
		BindAddress:        unusedWorkerAddress(t),
		FlowServiceAddress: address,
		Credential:         credential,
	})
	require.NoError(t, err)
	startResult := make(chan error, 1)
	go func() { startResult <- worker.Start() }()
	require.Eventually(t, func() bool {
		return len(recorder.received(dexpb.FlowService_SyncAttributeIndexes_FullMethodName)) == 1
	}, 10*time.Second, 50*time.Millisecond)
	require.Equal(t, []string{"Bearer worker-token"}, recorder.received(dexpb.FlowService_GetServerInfo_FullMethodName))
	require.Equal(t, []string{"Bearer worker-token"}, recorder.received(dexpb.FlowService_SyncAttributeIndexes_FullMethodName))
	require.NoError(t, worker.Stop(context.Background()))
	require.NoError(t, <-startResult)
}

func TestClientWithoutCredentialSendsNone(t *testing.T) {
	address, recorder := startCredentialRecordingFlowService(t, &workerSyncFlowService{})
	registry, err := NewRegistry(nil)
	require.NoError(t, err)
	cache, err := blobcache.New(&blobcache.Config{Dir: t.TempDir(), MaxBytes: 1 << 20})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cache.Close()) })
	client, err := NewClient(registry, cache, ClientOptions{FlowServiceAddress: address})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	_, err = client.service.GetServerInfo(context.Background(), &emptypb.Empty{})
	require.NoError(t, err)
	require.Empty(t, recorder.received(dexpb.FlowService_GetServerInfo_FullMethodName))
}
