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
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
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
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type pullRuntime struct {
	publicAddress   string
	internalAddress string
	runtime         *bootstrap.Runtime
	cancel          context.CancelFunc
	done            <-chan error
	stopOnce        sync.Once
	stopErr         error
}

func (runtime *pullRuntime) stop() error {
	runtime.stopOnce.Do(func() {
		runtime.cancel()
		select {
		case runtime.stopErr = <-runtime.done:
		case <-time.After(15 * time.Second):
			runtime.runtime.Close()
			runtime.stopErr = errors.New("pull runtime did not stop")
		}
	})
	return runtime.stopErr
}

func (runtime *pullRuntime) hasMembershipAddress() bool {
	return runtime.runtime.MembershipAddress() != ""
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
	requireGoServerSetting(t, "Go Server internals")
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
		requireMatchingUnavailableOnPublic(t, publicConnection)
		worker := registerPullWorker(t, publicConnection, "pull-flow")
		ping, err := worker.Recv()
		require.NoError(t, err)
		require.NotNil(t, ping.GetPing())
		internalConnection := newDexClientConnectionWithCredential(t, runtime.internalAddress, "", config.DefaultGrpcMaxMessageBytes)
		dispatchPullTask(t, dexpb.NewMatchingServiceClient(internalConnection), worker, "pull-flow")
	})

	t.Run("auth-off InternalService compatibility", func(t *testing.T) {
		cfg := createTestConfig(t, DexServiceTestConfig{BackendType: backend})
		cfg.Internal.Listen = "127.0.0.1:0"
		cfg.Internal.TokenFile = internalTokenFile
		runtime := startPullRuntime(t, &cfg, bootstrap.Services{API: true, Matching: true})
		publicConnection := newDexClientConnectionWithCredential(t, runtime.publicAddress, "", config.DefaultGrpcMaxMessageBytes)
		_, err := dexpb.NewInternalServiceClient(publicConnection).DumpFlowForContinueAsNew(pullContext(t), &dexpb.ContinueAsNewDumpRequest{})
		require.Equal(t, codes.InvalidArgument, status.Code(err))
		requireMatchingUnavailableOnPublic(t, publicConnection)
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
		requireMatchingUnavailableOnPublic(t, publicConnection)
		worker := registerPullWorker(t, publicConnection, "secure-flow")
		for _, credential := range []string{"", "wrong-token", publicToken} {
			unauthenticatedInternal := newDexClientConnectionWithCredential(t, runtime.internalAddress, credential, config.DefaultGrpcMaxMessageBytes)
			stream, err := dexpb.NewMatchingServiceClient(unauthenticatedInternal).DispatchTask(pullContext(t), pullDispatchRequest("secure-flow"))
			require.NoError(t, err)
			_, err = stream.Recv()
			require.Equal(t, codes.Unauthenticated, status.Code(err))
		}
		internalConnection := newDexClientConnectionWithCredential(t, runtime.internalAddress, internalToken, config.DefaultGrpcMaxMessageBytes)
		require.Eventually(t, func() bool {
			response, checkErr := healthpb.NewHealthClient(internalConnection).Check(pullContext(t), &healthpb.HealthCheckRequest{Service: dexpb.MatchingService_ServiceDesc.ServiceName})
			return checkErr == nil && response.Status == healthpb.HealthCheckResponse_SERVING
		}, 10*time.Second, 50*time.Millisecond)
		dispatchPullTask(t, dexpb.NewMatchingServiceClient(internalConnection), worker, "secure-flow")
	})

	t.Run("non-owner proxy", func(t *testing.T) {
		firstConfig := createTestConfig(t, DexServiceTestConfig{BackendType: backend})
		firstConfig.Auth.Authenticators = []config.AuthenticatorConfig{{Kind: config.AuthenticatorKindStaticTokens, File: publicTokenFile}}
		firstConfig.Internal.TokenFile = internalTokenFile
		firstConfig.Membership = &config.MembershipConfig{Listen: "127.0.0.1:0", BroadcastAddress: "127.0.0.1", RefreshInterval: 50 * time.Millisecond}
		first := startPullRuntime(t, &firstConfig, bootstrap.Services{API: true})
		require.Eventually(t, first.hasMembershipAddress, 10*time.Second, 50*time.Millisecond)
		secondConfig := createTestConfig(t, DexServiceTestConfig{BackendType: backend})
		secondConfig.Internal.TokenFile = internalTokenFile
		secondConfig.Membership = &config.MembershipConfig{Listen: "127.0.0.1:0", BroadcastAddress: "127.0.0.1", RefreshInterval: 50 * time.Millisecond, Seeds: config.MembershipSeedsConfig{Static: []string{first.runtime.MembershipAddress()}}}
		startPullRuntime(t, &secondConfig, bootstrap.Services{Matching: true})
		publicConnection := newDexClientConnectionWithCredential(t, first.publicAddress, publicToken, config.DefaultGrpcMaxMessageBytes)
		requireMatchingUnavailableOnPublic(t, publicConnection)
		registration := &pullRegistrationAttempt{t: t, connection: publicConnection, queue: "remote-flow"}
		require.Eventually(t, registration.tryRegister, 10*time.Second, 50*time.Millisecond)
		internalConnection := newDexClientConnectionWithCredential(t, first.internalAddress, internalToken, config.DefaultGrpcMaxMessageBytes)
		dispatchPullTask(t, dexpb.NewMatchingServiceClient(internalConnection), registration.worker, "remote-flow")
	})

	t.Run("mismatched replica token", func(t *testing.T) {
		firstConfig := createTestConfig(t, DexServiceTestConfig{BackendType: backend})
		firstConfig.Auth.Authenticators = []config.AuthenticatorConfig{{Kind: config.AuthenticatorKindStaticTokens, File: publicTokenFile}}
		firstConfig.Internal.TokenFile = internalTokenFile
		firstConfig.Membership = &config.MembershipConfig{Listen: "127.0.0.1:0", BroadcastAddress: "127.0.0.1", RefreshInterval: 50 * time.Millisecond}
		first := startPullRuntime(t, &firstConfig, bootstrap.Services{API: true})
		require.Eventually(t, first.hasMembershipAddress, 10*time.Second, 50*time.Millisecond)
		otherTokenFile := filepath.Join(t.TempDir(), "other-internal-token")
		require.NoError(t, os.WriteFile(otherTokenFile, []byte("other-"+uuid.NewString()+"\n"), 0o600))
		secondConfig := createTestConfig(t, DexServiceTestConfig{BackendType: backend})
		secondConfig.Internal.TokenFile = otherTokenFile
		secondConfig.Membership = &config.MembershipConfig{Listen: "127.0.0.1:0", BroadcastAddress: "127.0.0.1", RefreshInterval: 50 * time.Millisecond, Seeds: config.MembershipSeedsConfig{Static: []string{first.runtime.MembershipAddress()}}}
		startPullRuntime(t, &secondConfig, bootstrap.Services{Matching: true})
		publicConnection := newDexClientConnectionWithCredential(t, first.publicAddress, publicToken, config.DefaultGrpcMaxMessageBytes)
		require.Eventually(t, func() bool {
			stream, err := dexpb.NewWorkerTaskServiceClient(publicConnection).PollWorkerTask(pullContext(t))
			if err != nil || stream.Send(pullRegistration("wrong-token-flow")) != nil {
				return false
			}
			_, err = stream.Recv()
			return status.Code(err) == codes.Unauthenticated
		}, 10*time.Second, 50*time.Millisecond)
	})

	t.Run("membership change reconnects moved queue", func(t *testing.T) {
		firstConfig := createTestConfig(t, DexServiceTestConfig{BackendType: backend})
		firstConfig.Internal.TokenFile = internalTokenFile
		firstConfig.Membership = &config.MembershipConfig{Listen: "127.0.0.1:0", BroadcastAddress: "127.0.0.1", RefreshInterval: 50 * time.Millisecond}
		first := startPullRuntime(t, &firstConfig, bootstrap.Services{API: true, Matching: true})
		require.Eventually(t, first.hasMembershipAddress, 10*time.Second, 50*time.Millisecond)
		publicConnection := newDexClientConnectionWithCredential(t, first.publicAddress, "", config.DefaultGrpcMaxMessageBytes)
		reconnects := make(chan struct{}, 12)
		for index := 0; index < 12; index++ {
			worker := registerPullWorker(t, publicConnection, fmt.Sprintf("moving-flow-%d", index))
			go func() {
				for {
					frame, err := worker.Recv()
					if err != nil {
						return
					}
					if frame.GetReconnect() != nil {
						reconnects <- struct{}{}
						return
					}
				}
			}()
		}
		secondConfig := createTestConfig(t, DexServiceTestConfig{BackendType: backend})
		secondConfig.Internal.TokenFile = internalTokenFile
		secondConfig.Membership = &config.MembershipConfig{Listen: "127.0.0.1:0", BroadcastAddress: "127.0.0.1", RefreshInterval: 50 * time.Millisecond, Seeds: config.MembershipSeedsConfig{Static: []string{first.runtime.MembershipAddress()}}}
		startPullRuntime(t, &secondConfig, bootstrap.Services{Matching: true})
		require.Eventually(t, func() bool {
			select {
			case <-reconnects:
				return true
			default:
				return false
			}
		}, 10*time.Second, 50*time.Millisecond)
	})

	t.Run("proxy shutdown drains current task", func(t *testing.T) {
		firstConfig := createTestConfig(t, DexServiceTestConfig{BackendType: backend})
		firstConfig.Auth.Authenticators = []config.AuthenticatorConfig{{Kind: config.AuthenticatorKindStaticTokens, File: publicTokenFile}}
		firstConfig.Internal.TokenFile = internalTokenFile
		firstConfig.Membership = &config.MembershipConfig{Listen: "127.0.0.1:0", BroadcastAddress: "127.0.0.1", RefreshInterval: 50 * time.Millisecond}
		first := startPullRuntime(t, &firstConfig, bootstrap.Services{API: true})
		require.Eventually(t, first.hasMembershipAddress, 10*time.Second, 50*time.Millisecond)
		secondConfig := createTestConfig(t, DexServiceTestConfig{BackendType: backend})
		secondConfig.Internal.TokenFile = internalTokenFile
		secondConfig.Membership = &config.MembershipConfig{Listen: "127.0.0.1:0", BroadcastAddress: "127.0.0.1", RefreshInterval: 50 * time.Millisecond, Seeds: config.MembershipSeedsConfig{Static: []string{first.runtime.MembershipAddress()}}}
		startPullRuntime(t, &secondConfig, bootstrap.Services{Matching: true})
		publicConnection := newDexClientConnectionWithCredential(t, first.publicAddress, publicToken, config.DefaultGrpcMaxMessageBytes)
		registration := &pullRegistrationAttempt{t: t, connection: publicConnection, queue: "drain-flow"}
		require.Eventually(t, registration.tryRegister, 10*time.Second, 50*time.Millisecond)
		internalConnection := newDexClientConnectionWithCredential(t, first.internalAddress, internalToken, config.DefaultGrpcMaxMessageBytes)
		dispatch, err := dexpb.NewMatchingServiceClient(internalConnection).DispatchTask(pullContext(t), pullDispatchRequest("drain-flow"))
		require.NoError(t, err)
		assigned, err := registration.worker.Recv()
		require.NoError(t, err)
		require.NotNil(t, assigned.GetTask())
		stopped := make(chan error, 1)
		go func() { stopped <- first.stop() }()
		require.NoError(t, registration.worker.Send(&dexpb.PollWorkerTaskRequest{Frame: &dexpb.PollWorkerTaskRequest_TaskFrame{TaskFrame: &dexpb.PollWorkerTaskFrame{TaskId: assigned.GetTask().TaskId, Output: &dexpb.PollWorkerTaskFrame_Rpc{Rpc: &dexpb.InvokeWorkerRPCResponse{}}}}}))
		response := receivePullDispatchOutput(t, dispatch)
		require.NotNil(t, response.GetRpc())
		reconnect, err := registration.worker.Recv()
		require.NoError(t, err)
		require.NotNil(t, reconnect.GetReconnect())
		require.NoError(t, <-stopped)
	})
}

func startPullRuntime(t *testing.T, cfg *config.Config, services bootstrap.Services) *pullRuntime {
	t.Helper()
	result := &pullRuntime{}
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
	result.runtime = runtime
	result.internalAddress = runtime.InternalAddress()
	ctx, cancel := context.WithCancel(context.Background())
	result.cancel = cancel
	done := make(chan error, 1)
	result.done = done
	go func() { done <- runtime.Run(ctx) }()
	t.Cleanup(func() { require.NoError(t, result.stop()) })
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
	return &dexpb.DispatchTaskRequest{Queue: queue, FlowId: uuid.NewString(), DispatchWait: durationpb.New(5 * time.Second), Deadline: timestamppb.New(time.Now().Add(10 * time.Second)), Request: &dexpb.DispatchTaskRequest_Rpc{Rpc: &dexpb.InvokeWorkerRPCRequest{}}, SupportsAssignmentState: true}
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
	response := receivePullDispatchOutput(t, dispatch)
	require.NotNil(t, response.GetRpc())
	require.NotEmpty(t, response.GetServedByAppWorkerId())
	_, err = dispatch.Recv()
	require.ErrorIs(t, err, io.EOF)
}

func receivePullDispatchOutput(t *testing.T, stream dexpb.MatchingService_DispatchTaskClient) *dexpb.DispatchTaskResponse {
	t.Helper()
	assigned, err := stream.Recv()
	require.NoError(t, err)
	require.NotNil(t, assigned.GetAssigned())
	response, err := stream.Recv()
	require.NoError(t, err)
	return response
}

func requireMatchingUnavailableOnPublic(t *testing.T, connection *grpc.ClientConn) {
	t.Helper()
	stream, err := dexpb.NewMatchingServiceClient(connection).DispatchTask(pullContext(t), pullDispatchRequest("public-unavailable"))
	require.NoError(t, err)
	_, err = stream.Recv()
	require.Equal(t, codes.Unimplemented, status.Code(err))
}
