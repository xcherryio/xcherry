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
	"crypto/rand"
	"crypto/rsa"
	"fmt"
	"testing"
	"time"

	"github.com/go-jose/go-jose/v3/jwt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/superdurable/dex/config"
	"github.com/superdurable/dex/gen/dexpb"
	"github.com/superdurable/dex/integ/workflow/basic"
	"google.golang.org/grpc/codes"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestTenantAuthenticationTemporal(t *testing.T) {
	if !*temporalIntegTest || *dexServerAddress != "" {
		t.Skip()
	}
	environment := newTenancyTestEnvironment(t)
	runtime := environment.startTenantDexService(t, "alpha")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	clientWith := func(credential string) dexpb.FlowServiceClient {
		return newFlowClientWithCredential(t, runtime.PublicAddress, credential)
	}
	requireCode := func(t *testing.T, expected codes.Code, credential string) {
		t.Helper()
		_, err := clientWith(credential).GetServerInfo(ctx, &emptypb.Empty{})
		require.Equal(t, expected, status.Code(err), "error: %v", err)
	}

	t.Run("missing credential is unauthenticated and health stays open", func(t *testing.T) {
		requireCode(t, codes.Unauthenticated, "")
		_, err := clientWith("").HealthCheck(ctx, &emptypb.Empty{})
		require.Equal(t, codes.Unauthenticated, status.Code(err))
		health, err := healthpb.NewHealthClient(
			newDexClientConnectionWithCredential(t, runtime.PublicAddress, "", config.DefaultGrpcMaxMessageBytes),
		).Check(ctx, &healthpb.HealthCheckRequest{})
		require.NoError(t, err)
		require.Equal(t, healthpb.HealthCheckResponse_SERVING, health.GetStatus())
	})

	t.Run("valid JWT and static token are accepted", func(t *testing.T) {
		requireCode(t, codes.OK, environment.TenantJWT(t, "beta"))
		staticToken := environment.AddStaticToken(t, "gamma")
		require.Eventually(t, func() bool {
			_, err := clientWith(staticToken).GetServerInfo(ctx, &emptypb.Empty{})
			return err == nil
		}, 10*time.Second, 200*time.Millisecond, "the reloaded static token file must accept the new token")
	})

	t.Run("invalid credentials are unauthenticated", func(t *testing.T) {
		otherKey, err := rsa.GenerateKey(rand.Reader, 2048)
		require.NoError(t, err)
		validClaims := func() tenancyJWTClaims {
			return tenancyJWTClaims{
				Claims: jwt.Claims{
					Issuer:   environment.Issuer,
					Subject:  fmt.Sprintf(tenancySubjectFormat, "alpha"),
					Audience: jwt.Audience{tenancyTestAudience},
					Expiry:   jwt.NewNumericDate(time.Now().Add(time.Hour)),
				},
				Namespace: "apps-alpha",
			}
		}
		cases := map[string]func() string{
			"expired": func() string {
				claims := validClaims()
				claims.Expiry = jwt.NewNumericDate(time.Now().Add(-time.Hour))
				return environment.SignJWT(t, environment.signingKey, claims)
			},
			"without expiry": func() string {
				claims := validClaims()
				claims.Expiry = nil
				return environment.SignJWT(t, environment.signingKey, claims)
			},
			"wrongly signed": func() string {
				return environment.SignJWT(t, otherKey, validClaims())
			},
			"audience mismatch": func() string {
				claims := validClaims()
				claims.Audience = jwt.Audience{"another-audience"}
				return environment.SignJWT(t, environment.signingKey, claims)
			},
			"issuer mismatch": func() string {
				claims := validClaims()
				claims.Issuer = "https://another-issuer.example"
				return environment.SignJWT(t, environment.signingKey, claims)
			},
			"subject pattern mismatch": func() string {
				claims := validClaims()
				claims.Subject = "system:serviceaccount:apps-alpha:app-extra"
				return environment.SignJWT(t, environment.signingKey, claims)
			},
			"tenant outside the alphabet": func() string {
				claims := validClaims()
				claims.Subject = fmt.Sprintf(tenancySubjectFormat, "Alpha")
				return environment.SignJWT(t, environment.signingKey, claims)
			},
			"claim mismatch": func() string {
				claims := validClaims()
				claims.Namespace = "apps-beta"
				return environment.SignJWT(t, environment.signingKey, claims)
			},
			"unknown static token": func() string { return "static-" + uuid.NewString() },
		}
		for name, credential := range cases {
			t.Run(name, func(t *testing.T) {
				requireCode(t, codes.Unauthenticated, credential())
			})
		}
	})

	t.Run("operator credentials are refused on FlowService", func(t *testing.T) {
		requireCode(t, codes.PermissionDenied, environment.OperatorToken)
		operatorJWT := environment.SignJWT(t, environment.signingKey, tenancyJWTClaims{Claims: jwt.Claims{
			Issuer:   environment.Issuer,
			Subject:  tenancyOperatorSub,
			Audience: jwt.Audience{tenancyTestAudience},
			Expiry:   jwt.NewNumericDate(time.Now().Add(time.Hour)),
		}})
		requireCode(t, codes.PermissionDenied, operatorJWT)
	})

	t.Run("SyncAttributeIndexes is refused", func(t *testing.T) {
		_, err := runtime.FlowClient.SyncAttributeIndexes(ctx, &dexpb.SyncAttributeIndexRequest{
			AttributeIndexes: map[string]dexpb.IndexType{
				"TenantOwnedIndex": dexpb.IndexType_INDEX_TYPE_KEYWORD,
			},
		})
		require.Equal(t, codes.PermissionDenied, status.Code(err), "error: %v", err)
	})

	t.Run("InternalService is only on the internal listener with its token", func(t *testing.T) {
		dumpRequest := &dexpb.ContinueAsNewDumpRequest{FlowId: "missing-" + uuid.NewString(), PageSizeInBytes: 1024}
		internalClient := func(address string, credential string) dexpb.InternalServiceClient {
			return dexpb.NewInternalServiceClient(
				newDexClientConnectionWithCredential(t, address, credential, config.DefaultGrpcMaxMessageBytes),
			)
		}
		_, err := internalClient(runtime.PublicAddress, environment.TenantJWT(t, "alpha")).
			DumpFlowForContinueAsNew(ctx, dumpRequest)
		require.Equal(t, codes.Unimplemented, status.Code(err), "error: %v", err)
		_, err = internalClient(runtime.PublicAddress, environment.InternalToken).
			DumpFlowForContinueAsNew(ctx, dumpRequest)
		require.Equal(t, codes.Unimplemented, status.Code(err), "error: %v", err)

		_, err = internalClient(runtime.InternalAddress, "").DumpFlowForContinueAsNew(ctx, dumpRequest)
		require.Equal(t, codes.Unauthenticated, status.Code(err), "error: %v", err)
		_, err = internalClient(runtime.InternalAddress, environment.TenantJWT(t, "alpha")).
			DumpFlowForContinueAsNew(ctx, dumpRequest)
		require.Equal(t, codes.Unauthenticated, status.Code(err), "error: %v", err)
		_, err = internalClient(runtime.InternalAddress, environment.InternalToken).
			DumpFlowForContinueAsNew(ctx, dumpRequest)
		require.NotContains(t, []codes.Code{codes.Unauthenticated, codes.PermissionDenied, codes.Unimplemented},
			status.Code(err), "error: %v", err)

		_, err = dexpb.NewFlowServiceClient(
			newDexClientConnectionWithCredential(t, runtime.InternalAddress, environment.InternalToken, config.DefaultGrpcMaxMessageBytes),
		).GetServerInfo(ctx, &emptypb.Empty{})
		require.Equal(t, codes.Unimplemented, status.Code(err), "error: %v", err)
	})
}

// TestTenantModeContinueAsNewTemporal proves interpreter Workers reach InternalService through the internal listener.
func TestTenantModeContinueAsNewTemporal(t *testing.T) {
	if !*temporalIntegTest || *dexServerAddress != "" {
		t.Skip()
	}
	environment := newTenancyTestEnvironment(t)
	workerTarget := startWorker(t, basic.NewHandler())
	runtime := environment.startTenantDexService(t, "alpha")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	flowID := basic.FlowType + "-" + uuid.NewString()
	_, err := runtime.FlowClient.StartFlow(ctx, &dexpb.StartFlowRequest{
		RequestId:          newRequestID(),
		FlowId:             flowID,
		FlowType:           basic.FlowType,
		FlowTimeoutSeconds: 100,
		StartStepType:      basic.Step1,
		StepInput:          jsonObjValue("tenant input"),
		FlowStartOptions: withWorkerTarget(&dexpb.FlowStartOptions{
			FlowConfigOverride: minimumContinueAsNewAsyncDurabilityConfig(),
		}, workerTarget),
	})
	require.NoError(t, err)
	result, err := runtime.FlowClient.WaitForFlow(ctx, &dexpb.WaitForFlowRequest{
		FlowId:          flowID,
		NeedsResults:    true,
		WaitTimeSeconds: 20,
	})
	require.NoError(t, err)
	require.Equal(t, dexpb.FlowStatus_FLOW_STATUS_COMPLETED, result.GetFlowStatus())
	runtime.requireInternalDumpHeaders(t, "authorization", "Bearer "+environment.InternalToken)
}
