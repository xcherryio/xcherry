// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package integ

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/superdurable/dex/config"
	"github.com/superdurable/dex/gen/dexpb"
	"github.com/superdurable/dex/service"
	"github.com/superdurable/dex/service/bootstrap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type pullRuntime struct {
	publicAddress   string
	internalAddress string
}

type pullRegistrationAttempt struct {
	t          *testing.T
	connection *grpc.ClientConn
	queue      string
	worker     dexpb.WorkerTaskService_PollWorkerTaskClient
}

func (attempt *pullRegistrationAttempt) tryRegister() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	candidate, err := dexpb.NewWorkerTaskServiceClient(attempt.connection).PollWorkerTask(ctx)
	if err != nil {
		cancel()
		return false
	}
	if err := candidate.Send(pullRegistration(attempt.queue)); err != nil {
		cancel()
		return false
	}
	frame, err := candidate.Recv()
	if err != nil || frame.GetRegistered() == nil {
		cancel()
		return false
	}
	attempt.t.Cleanup(cancel)
	attempt.worker = candidate
	return true
}

func TestPullWorkerRoutingTemporal(t *testing.T) {
	if !*temporalIntegTest || *dexServerAddress != "" {
		t.Skip()
	}
	testPullWorkerRouting(t, service.BackendTypeTemporal)
}

func TestPullWorkerRoutingCadence(t *testing.T) {
	if !*cadenceIntegTest || *dexServerAddress != "" {
		t.Skip()
	}
	testPullWorkerRouting(t, service.BackendTypeCadence)
}

func testPullWorkerRouting(t *testing.T, backend service.BackendType) {
	t.Helper()
	publicToken := "public-" + uuid.NewString()
	publicHash := sha256.Sum256([]byte(publicToken))
	publicTokenFile := filepath.Join(t.TempDir(), "public-tokens.yaml")
	require.NoError(t, os.WriteFile(publicTokenFile, []byte("tokens:\n  - sha256: "+hex.EncodeToString(publicHash[:])+"\n"), 0o600))
	internalToken := "internal-" + uuid.NewString()
	internalTokenFile := filepath.Join(t.TempDir(), "internal-token")
	require.NoError(t, os.WriteFile(internalTokenFile, []byte(internalToken+"\n"), 0o600))

	t.Run("single server and idle ping", func(t *testing.T) {
		cfg := createTestConfig(t, DexServiceTestConfig{BackendType: backend})
		cfg.Matching = &config.MatchingConfig{PingInterval: 50 * time.Millisecond}
		runtime := startPullRuntime(t, &cfg, bootstrap.Services{API: true, Matching: true})
		publicConnection := newDexClientConnectionWithCredential(t, runtime.publicAddress, "", config.DefaultGrpcMaxMessageBytes)
		worker := registerPullWorker(t, publicConnection, "pull-flow")
		ping, err := worker.Recv()
		require.NoError(t, err)
		require.NotNil(t, ping.GetPing())
		dispatchPullTask(t, dexpb.NewMatchingServiceClient(publicConnection), worker, "pull-flow")
	})

	t.Run("authenticated listeners", func(t *testing.T) {
		cfg := createTestConfig(t, DexServiceTestConfig{BackendType: backend})
		cfg.Auth.Authenticators = []config.AuthenticatorConfig{{Kind: config.AuthenticatorKindStaticTokens, File: publicTokenFile}}
		cfg.Internal.TokenFile = internalTokenFile
		runtime := startPullRuntime(t, &cfg, bootstrap.Services{API: true, Matching: true})
		for _, credential := range []string{"", "wrong-token"} {
			connection := newDexClientConnectionWithCredential(t, runtime.publicAddress, credential, config.DefaultGrpcMaxMessageBytes)
			stream, err := dexpb.NewWorkerTaskServiceClient(connection).PollWorkerTask(pullContext(t))
			require.NoError(t, err)
			_, err = stream.Recv()
			require.Equal(t, codes.Unauthenticated, status.Code(err))
		}
		publicConnection := newDexClientConnectionWithCredential(t, runtime.publicAddress, publicToken, config.DefaultGrpcMaxMessageBytes)
		worker := registerPullWorker(t, publicConnection, "secure-flow")
		for _, credential := range []string{"", "wrong-token"} {
			unauthenticatedInternal := newDexClientConnectionWithCredential(t, runtime.internalAddress, credential, config.DefaultGrpcMaxMessageBytes)
			stream, err := dexpb.NewMatchingServiceClient(unauthenticatedInternal).DispatchTask(pullContext(t), pullDispatchRequest("secure-flow"))
			require.NoError(t, err)
			_, err = stream.Recv()
			require.Equal(t, codes.Unauthenticated, status.Code(err))
		}
		internalConnection := newDexClientConnectionWithCredential(t, runtime.internalAddress, internalToken, config.DefaultGrpcMaxMessageBytes)
		dispatchPullTask(t, dexpb.NewMatchingServiceClient(internalConnection), worker, "secure-flow")
	})

	t.Run("non-owner proxy", func(t *testing.T) {
		firstMembershipAddress := unusedLoopbackAddress(t)
		firstConfig := createTestConfig(t, DexServiceTestConfig{BackendType: backend})
		firstConfig.Internal.TokenFile = internalTokenFile
		firstConfig.Membership = &config.MembershipConfig{Listen: firstMembershipAddress, BroadcastAddress: "127.0.0.1", RefreshInterval: 50 * time.Millisecond}
		first := startPullRuntime(t, &firstConfig, bootstrap.Services{API: true})
		secondConfig := createTestConfig(t, DexServiceTestConfig{BackendType: backend})
		secondConfig.Internal.TokenFile = internalTokenFile
		secondConfig.Membership = &config.MembershipConfig{Listen: "127.0.0.1:0", BroadcastAddress: "127.0.0.1", RefreshInterval: 50 * time.Millisecond, Seeds: config.MembershipSeedsConfig{Static: []string{firstMembershipAddress}}}
		startPullRuntime(t, &secondConfig, bootstrap.Services{Matching: true})
		publicConnection := newDexClientConnectionWithCredential(t, first.publicAddress, "", config.DefaultGrpcMaxMessageBytes)
		registration := &pullRegistrationAttempt{t: t, connection: publicConnection, queue: "remote-flow"}
		require.Eventually(t, registration.tryRegister, 10*time.Second, 50*time.Millisecond)
		internalConnection := newDexClientConnectionWithCredential(t, first.internalAddress, internalToken, config.DefaultGrpcMaxMessageBytes)
		dispatchPullTask(t, dexpb.NewMatchingServiceClient(internalConnection), registration.worker, "remote-flow")
	})
}

func startPullRuntime(t *testing.T, cfg *config.Config, services bootstrap.Services) pullRuntime {
	t.Helper()
	var result pullRuntime
	var publicListener net.Listener
	var err error
	if services.API {
		publicListener, err = net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		result.publicAddress = publicListener.Addr().String()
	}
	var internalListener net.Listener
	if cfg.Auth.IsEnabled() || cfg.Membership != nil || (!services.API && services.Matching) {
		internalListener, err = net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		cfg.Internal.Listen = internalListener.Addr().String()
		result.internalAddress = cfg.Internal.Listen
	}
	runtime, err := bootstrap.New(cfg, &bootstrap.Options{Services: services, APIListener: publicListener, InternalListener: internalListener})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runtime.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case runErr := <-done:
			require.NoError(t, runErr)
		case <-time.After(10 * time.Second):
			runtime.Close()
			t.Fatal("pull runtime did not stop")
		}
	})
	return result
}

func pullContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func pullRegistration(queue string) *dexpb.PollWorkerTaskRequest {
	return &dexpb.PollWorkerTaskRequest{Frame: &dexpb.PollWorkerTaskRequest_Register{Register: &dexpb.PollWorkerTaskRegister{Queue: queue, AppWorkerId: "worker-" + uuid.NewString(), FlowTypes: []string{queue}}}}
}

func registerPullWorker(t *testing.T, connection *grpc.ClientConn, queue string) dexpb.WorkerTaskService_PollWorkerTaskClient {
	t.Helper()
	worker, err := dexpb.NewWorkerTaskServiceClient(connection).PollWorkerTask(pullContext(t))
	require.NoError(t, err)
	require.NoError(t, worker.Send(pullRegistration(queue)))
	registered, err := worker.Recv()
	require.NoError(t, err)
	require.NotNil(t, registered.GetRegistered())
	return worker
}

func pullDispatchRequest(queue string) *dexpb.DispatchTaskRequest {
	return &dexpb.DispatchTaskRequest{Queue: queue, FlowId: uuid.NewString(), DispatchWait: durationpb.New(5 * time.Second), Deadline: timestamppb.New(time.Now().Add(10 * time.Second)), Request: &dexpb.DispatchTaskRequest_Rpc{Rpc: &dexpb.InvokeWorkerRPCRequest{}}}
}

func dispatchPullTask(t *testing.T, client dexpb.MatchingServiceClient, worker dexpb.WorkerTaskService_PollWorkerTaskClient, queue string) {
	t.Helper()
	dispatch, err := client.DispatchTask(pullContext(t), pullDispatchRequest(queue))
	require.NoError(t, err)
	for {
		assigned, recvErr := worker.Recv()
		require.NoError(t, recvErr)
		if assigned.GetPing() != nil {
			continue
		}
		require.NotNil(t, assigned.GetTask())
		require.NoError(t, worker.Send(&dexpb.PollWorkerTaskRequest{Frame: &dexpb.PollWorkerTaskRequest_TaskFrame{TaskFrame: &dexpb.PollWorkerTaskFrame{TaskId: assigned.GetTask().TaskId, Output: &dexpb.PollWorkerTaskFrame_Rpc{Rpc: &dexpb.InvokeWorkerRPCResponse{}}}}}))
		break
	}
	response, err := dispatch.Recv()
	require.NoError(t, err)
	require.NotNil(t, response.GetRpc())
	require.NotEmpty(t, response.GetServedByAppWorkerId())
	_, err = dispatch.Recv()
	require.ErrorIs(t, err, io.EOF)
}

func unusedLoopbackAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	address := listener.Addr().String()
	require.NoError(t, listener.Close())
	return address
}
