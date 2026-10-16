// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package command

import (
	"bytes"
	"context"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/superdurable/dex/gen/dexpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/emptypb"
)

type credentialTestService struct {
	dexpb.UnimplementedFlowServiceServer
	mu      sync.Mutex
	headers []string
}

func (*credentialTestService) HealthCheck(context.Context, *emptypb.Empty) (*dexpb.HealthInfo, error) {
	return &dexpb.HealthInfo{}, nil
}

func (s *credentialTestService) recordHeader(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	requestMetadata, _ := metadata.FromIncomingContext(ctx)
	s.mu.Lock()
	s.headers = append(s.headers, requestMetadata.Get("authorization")...)
	s.mu.Unlock()
	return handler(ctx, request)
}

func (s *credentialTestService) recordedHeaders() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.headers...)
}

func startCredentialTestService(t *testing.T) (*credentialTestService, string) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	service := &credentialTestService{}
	grpcServer := grpc.NewServer(grpc.UnaryInterceptor(service.recordHeader))
	dexpb.RegisterFlowServiceServer(grpcServer, service)
	serveFinished := make(chan error, 1)
	go func() { serveFinished <- grpcServer.Serve(listener) }()
	t.Cleanup(func() {
		grpcServer.GracefulStop()
		serveErr := <-serveFinished
		if serveErr != nil {
			require.ErrorIs(t, serveErr, grpc.ErrServerStopped)
		}
	})
	return service, listener.Addr().String()
}

func TestCredentialPrecedenceAndHeader(t *testing.T) {
	service, address := startCredentialTestService(t)
	fileFromEnvironment := filepath.Join(t.TempDir(), "env-token")
	fileFromFlag := filepath.Join(t.TempDir(), "flag-token")
	require.NoError(t, os.WriteFile(fileFromEnvironment, []byte(" environment \n"), 0o600))
	require.NoError(t, os.WriteFile(fileFromFlag, []byte(" flag \n"), 0o600))
	t.Setenv("DEX_CREDENTIAL", "raw-token")
	t.Setenv("DEX_CREDENTIAL_FILE", fileFromEnvironment)

	for _, arguments := range [][]string{
		{"health", "--server", address, "--credential-file", fileFromFlag},
		{"health", "--server", address},
	} {
		app := NewApp(bytes.NewReader(nil), &bytes.Buffer{}, &bytes.Buffer{})
		require.NoError(t, app.Execute(context.Background(), arguments))
	}
	require.NoError(t, os.Unsetenv("DEX_CREDENTIAL_FILE"))
	app := NewApp(bytes.NewReader(nil), &bytes.Buffer{}, &bytes.Buffer{})
	require.NoError(t, app.Execute(context.Background(), []string{"health", "--server", address}))
	require.Equal(t, []string{"Bearer flag", "Bearer environment", "Bearer raw-token"}, service.recordedHeaders())
}

func TestCredentialFileRotationAndEmptyFileStopBeforeRPC(t *testing.T) {
	service, address := startCredentialTestService(t)
	path := filepath.Join(t.TempDir(), "token")
	require.NoError(t, os.WriteFile(path, []byte("first"), 0o600))
	client, err := newFlowService(options{server: address, credentialFile: path})
	require.NoError(t, err)
	defer func() { require.NoError(t, client.connection.Close()) }()
	_, err = client.service.HealthCheck(context.Background(), emptyRequest())
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, []byte("second-value"), 0o600))
	require.Eventually(t, func() bool {
		_, callErr := client.service.HealthCheck(context.Background(), emptyRequest())
		return callErr == nil && len(service.recordedHeaders()) > 1 && service.recordedHeaders()[len(service.recordedHeaders())-1] == "Bearer second-value"
	}, 2*time.Second, 20*time.Millisecond)
	require.NoError(t, os.WriteFile(path, []byte(" \n"), 0o600))
	require.Eventually(t, func() bool {
		_, callErr := client.service.HealthCheck(context.Background(), emptyRequest())
		return callErr != nil
	}, 2*time.Second, 20*time.Millisecond)
	beforeEmpty := len(service.recordedHeaders())
	_, err = client.service.HealthCheck(context.Background(), emptyRequest())
	require.Error(t, err)
	_, err = newFlowService(options{server: address, credentialFile: path})
	require.ErrorContains(t, err, "is empty")
	require.Len(t, service.recordedHeaders(), beforeEmpty)
}

func TestEmptyEnvironmentCredentialsAreUnset(t *testing.T) {
	service, address := startCredentialTestService(t)
	t.Setenv("DEX_CREDENTIAL", "")
	app := NewApp(bytes.NewReader(nil), &bytes.Buffer{}, &bytes.Buffer{})
	require.NoError(t, app.Execute(context.Background(), []string{"health", "--server", address}))
	require.Empty(t, service.recordedHeaders())

	t.Setenv("DEX_CREDENTIAL_FILE", "")
	app = NewApp(bytes.NewReader(nil), &bytes.Buffer{}, &bytes.Buffer{})
	require.NoError(t, app.Execute(context.Background(), []string{"health", "--server", address}))
	require.Empty(t, service.recordedHeaders())
	t.Setenv("DEX_CREDENTIAL", "raw-token")
	app = NewApp(bytes.NewReader(nil), &bytes.Buffer{}, &bytes.Buffer{})
	require.NoError(t, app.Execute(context.Background(), []string{"health", "--server", address}))
	require.Equal(t, []string{"Bearer raw-token"}, service.recordedHeaders())

	app = NewApp(bytes.NewReader(nil), &bytes.Buffer{}, &bytes.Buffer{})
	err := app.Execute(context.Background(), []string{"health", "--server", address, "--credential-file", ""})
	require.ErrorContains(t, err, "bearer credential file path is empty")
	require.Equal(t, []string{"Bearer raw-token"}, service.recordedHeaders())
}
