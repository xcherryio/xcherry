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
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-jose/go-jose/v3/jwt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/superdurable/dex/config"
	"github.com/superdurable/dex/gen/dexpb"
	"github.com/superdurable/dex/integ/workflow/basic"
	"github.com/superdurable/dex/service"
	"github.com/superdurable/dex/service/auth"
	"github.com/superdurable/dex/service/common/log/loggerimpl"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestGeneralAuthenticationTemporal(t *testing.T) {
	if !*temporalIntegTest || *dexServerAddress != "" {
		t.Skip()
	}
	testGeneralAuthentication(t, service.BackendTypeTemporal)
}

func TestGeneralAuthenticationCadence(t *testing.T) {
	if !*cadenceIntegTest || *dexServerAddress != "" {
		t.Skip()
	}
	testGeneralAuthentication(t, service.BackendTypeCadence)
}

func testGeneralAuthentication(t *testing.T, backend service.BackendType) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	token := "public-" + uuid.NewString()
	hash := sha256.Sum256([]byte(token))
	tokenFile := filepath.Join(t.TempDir(), "public-tokens.yaml")
	require.NoError(t, os.WriteFile(tokenFile, []byte("tokens:\n  - sha256: "+hex.EncodeToString(hash[:])+"\n"), 0o600))
	internalToken := "internal-" + uuid.NewString()
	internalTokenFile := filepath.Join(t.TempDir(), "internal-token")
	require.NoError(t, os.WriteFile(internalTokenFile, []byte(internalToken+"\n"), 0o600))
	issuer := newTestJWTIssuer(t)
	runtime := startInProcessDexService(t, DexServiceTestConfig{
		BackendType: backend,
		Auth: &config.AuthConfig{Authenticators: []config.AuthenticatorConfig{
			{Kind: config.AuthenticatorKindStaticTokens, File: tokenFile},
			{Kind: config.AuthenticatorKindJWT, Issuer: issuer.URL, Audiences: []string{"dex"}, JWKSURL: issuer.URL + "/keys"},
		}},
		Internal:   &config.InternalConfig{TokenFile: internalTokenFile},
		Credential: token,
	})
	call := func(credential string) codes.Code {
		_, err := newFlowClientWithCredential(t, runtime.PublicAddress, credential).GetServerInfo(ctx, &emptypb.Empty{})
		return status.Code(err)
	}
	require.Equal(t, codes.Unauthenticated, call(""))
	require.Equal(t, codes.Unauthenticated, call("wrong-"+uuid.NewString()))
	require.Equal(t, codes.OK, call(token))
	health, err := healthpb.NewHealthClient(newDexClientConnectionWithCredential(t, runtime.PublicAddress, "", config.DefaultGrpcMaxMessageBytes)).Check(ctx, &healthpb.HealthCheckRequest{})
	require.NoError(t, err)
	require.Equal(t, healthpb.HealthCheckResponse_SERVING, health.GetStatus())
	jwtToken := func(expiry time.Time, audience string) string {
		return signTestJWT(t, issuer.key(tenancyTestKeyID), tenancyTestKeyID, jwt.Claims{
			Issuer: issuer.URL, Subject: "caller", Audience: jwt.Audience{audience}, Expiry: jwt.NewNumericDate(expiry),
		})
	}
	require.Equal(t, codes.OK, call(jwtToken(time.Now().Add(time.Hour), "dex")))
	require.Equal(t, codes.OK, call(signTestJWT(t, issuer.key(tenancyTestKeyID), tenancyTestKeyID, jwt.Claims{
		Issuer: issuer.URL, Audience: jwt.Audience{"dex"}, Expiry: jwt.NewNumericDate(time.Now().Add(time.Hour)),
	})))
	require.Equal(t, codes.Unauthenticated, call(jwtToken(time.Now().Add(-time.Hour), "dex")))
	require.Equal(t, codes.Unauthenticated, call(jwtToken(time.Now().Add(time.Hour), "wrong")))
	dumpRequest := &dexpb.ContinueAsNewDumpRequest{FlowId: "missing-" + uuid.NewString(), PageSizeInBytes: 1024}
	dump := func(address string, credential string) codes.Code {
		client := dexpb.NewInternalServiceClient(newDexClientConnectionWithCredential(t, address, credential, config.DefaultGrpcMaxMessageBytes))
		_, err := client.DumpFlowForContinueAsNew(ctx, dumpRequest)
		return status.Code(err)
	}
	require.Equal(t, codes.Unimplemented, dump(runtime.PublicAddress, internalToken))
	require.Equal(t, codes.Unauthenticated, dump(runtime.InternalAddress, ""))
	require.Equal(t, codes.Unauthenticated, dump(runtime.InternalAddress, token))
	require.Equal(t, codes.InvalidArgument, dump(runtime.InternalAddress, internalToken))
	workerTarget := startWorker(t, basic.NewHandler())
	flowID := basic.FlowType + "-" + uuid.NewString()
	_, err = runtime.FlowClient.StartFlow(ctx, &dexpb.StartFlowRequest{
		RequestId: newRequestID(), FlowId: flowID, FlowType: basic.FlowType,
		FlowTimeoutSeconds: 100, StartStepType: basic.Step1, StepInput: jsonObjValue("authenticated input"),
		FlowStartOptions: withWorkerTarget(&dexpb.FlowStartOptions{
			FlowConfigOverride: minimumContinueAsNewAsyncDurabilityConfig(),
		}, workerTarget),
	})
	require.NoError(t, err)
	result, err := runtime.FlowClient.WaitForFlow(ctx, &dexpb.WaitForFlowRequest{FlowId: flowID, NeedsResults: true, WaitTimeSeconds: 20})
	require.NoError(t, err)
	require.Equal(t, dexpb.FlowStatus_FLOW_STATUS_COMPLETED, result.GetFlowStatus())
	runtime.requireInternalDumpHeaders(t, "authorization", "Bearer "+internalToken)
	testAuthenticatedStream(t, &config.AuthConfig{Authenticators: []config.AuthenticatorConfig{{Kind: config.AuthenticatorKindStaticTokens, File: tokenFile}}}, token)
}

type testStreamService interface{ Observe() }
type testStreamServer struct{}

func (testStreamServer) Observe() {}

func testAuthenticatedStream(t *testing.T, cfg *config.AuthConfig, token string) {
	t.Helper()
	logger, err := loggerimpl.NewDevelopment()
	require.NoError(t, err)
	authenticator, err := auth.NewAuthenticator(cfg, logger)
	require.NoError(t, err)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	interceptors := auth.NewPublicServiceInterceptors(authenticator)
	server := grpc.NewServer(grpc.StreamInterceptor(interceptors.Stream))
	server.RegisterService(&grpc.ServiceDesc{ServiceName: "test.Public", HandlerType: (*testStreamService)(nil), Streams: []grpc.StreamDesc{{
		StreamName: "Observe", ServerStreams: true,
		Handler: func(_ interface{}, stream grpc.ServerStream) error {
			if _, isAuthenticated := auth.PrincipalFromContext(stream.Context()); !isAuthenticated {
				return status.Error(codes.Internal, "authenticated stream has no principal")
			}
			return stream.SendMsg(&emptypb.Empty{})
		},
	}}}, testStreamServer{})
	go serveAuthenticatedTestStream(t, server, listener)
	t.Cleanup(server.Stop)
	for _, testCase := range []struct {
		credential string
		code       codes.Code
	}{{"", codes.Unauthenticated}, {"wrong", codes.Unauthenticated}, {token, codes.OK}} {
		connection := newDexClientConnectionWithCredential(t, listener.Addr().String(), testCase.credential, config.DefaultGrpcMaxMessageBytes)
		stream, err := connection.NewStream(context.Background(), &grpc.StreamDesc{ServerStreams: true}, "/test.Public/Observe")
		require.NoError(t, err)
		var response emptypb.Empty
		err = stream.RecvMsg(&response)
		require.Equal(t, testCase.code, status.Code(err))
	}
}

func serveAuthenticatedTestStream(t *testing.T, server *grpc.Server, listener net.Listener) {
	if err := server.Serve(listener); err != nil && err != grpc.ErrServerStopped {
		t.Errorf("serve test stream: %v", err)
	}
}
