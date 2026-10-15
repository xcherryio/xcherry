// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package auth

import (
	"context"
	"strings"

	"github.com/superdurable/dex/gen/dexpb"
	serviceerrors "github.com/superdurable/dex/service/common/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
)

// MaxCredentialBytes bounds a bearer credential before parsing.
const MaxCredentialBytes = 8 << 10

// PublicServiceInterceptors require valid credentials on public API calls.
type PublicServiceInterceptors struct{ authenticator *Authenticator }

// NewPublicServiceInterceptors creates public API authentication interceptors.
func NewPublicServiceInterceptors(authenticator *Authenticator) *PublicServiceInterceptors {
	if authenticator == nil {
		panic("authenticator must not be nil")
	}
	return &PublicServiceInterceptors{authenticator: authenticator}
}

// Unary authenticates public unary calls except health checks.
func (i *PublicServiceInterceptors) Unary(ctx context.Context, request interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	if isHealthMethod(info.FullMethod) {
		return handler(ctx, request)
	}
	principal, err := i.authenticate(ctx)
	if err != nil {
		return nil, err
	}
	return handler(context.WithValue(ctx, principalContextKey{}, principal), request)
}

// Stream authenticates public streams except health watches.
func (i *PublicServiceInterceptors) Stream(server interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	if isHealthMethod(info.FullMethod) {
		return handler(server, stream)
	}
	principal, err := i.authenticate(stream.Context())
	if err != nil {
		return err
	}
	return handler(server, &principalServerStream{ServerStream: stream, ctx: context.WithValue(stream.Context(), principalContextKey{}, principal)})
}

type principalServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (stream *principalServerStream) Context() context.Context { return stream.ctx }

func (i *PublicServiceInterceptors) authenticate(ctx context.Context) (*Principal, error) {
	credential, ok := bearerCredential(ctx)
	if !ok {
		return nil, unauthenticated("a bearer credential is required")
	}
	principal, err := i.authenticator.Authenticate(ctx, credential)
	if err != nil {
		return nil, unauthenticated("the credential is not valid")
	}
	return principal, nil
}

// InternalServiceInterceptor requires the private listener bearer token.
type InternalServiceInterceptor struct {
	token               *InternalTokenFile
	allowedServiceNames []string
}

// NewInternalServiceInterceptor creates the private listener interceptor.
func NewInternalServiceInterceptor(token *InternalTokenFile, allowedServiceNames []string) *InternalServiceInterceptor {
	if token == nil || len(allowedServiceNames) == 0 {
		panic("internal token file and allowed services must be configured")
	}
	for _, serviceName := range allowedServiceNames {
		if serviceName == "" {
			panic("allowed service name must not be empty")
		}
	}
	return &InternalServiceInterceptor{token: token, allowedServiceNames: append([]string(nil), allowedServiceNames...)}
}

// Unary authenticates allowed private service calls except health checks.
func (i *InternalServiceInterceptor) Unary(ctx context.Context, request interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	authorizedCtx, err := i.authorize(ctx, info.FullMethod)
	if err != nil {
		return nil, err
	}
	return handler(authorizedCtx, request)
}

// Stream authenticates allowed private service streams except health watches.
func (i *InternalServiceInterceptor) Stream(server interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	authorizedCtx, err := i.authorize(stream.Context(), info.FullMethod)
	if err != nil {
		return err
	}
	return handler(server, &principalServerStream{ServerStream: stream, ctx: authorizedCtx})
}

func (i *InternalServiceInterceptor) authorize(ctx context.Context, fullMethod string) (context.Context, error) {
	if isHealthMethod(fullMethod) {
		return ctx, nil
	}
	credential, ok := bearerCredential(ctx)
	if !ok || !i.token.Accepts(credential) {
		return nil, unauthenticated("the internal token is required")
	}
	for _, serviceName := range i.allowedServiceNames {
		if isServiceMethod(fullMethod, serviceName) {
			return context.WithValue(ctx, principalContextKey{}, &Principal{}), nil
		}
	}
	return nil, permissionDenied("the principal is not allowed on this method")
}

func bearerCredential(ctx context.Context) (string, bool) {
	incoming, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", false
	}
	values := incoming.Get("authorization")
	if len(values) != 1 || len(values[0]) <= len("bearer ") || len(values[0]) > MaxCredentialBytes {
		return "", false
	}
	if !strings.EqualFold(values[0][:len("bearer ")], "bearer ") {
		return "", false
	}
	credential := strings.TrimSpace(values[0][len("bearer "):])
	return credential, credential != ""
}

func isHealthMethod(fullMethod string) bool {
	return isServiceMethod(fullMethod, healthpb.Health_ServiceDesc.ServiceName)
}

func isServiceMethod(fullMethod string, serviceName string) bool {
	return strings.HasPrefix(fullMethod, "/"+serviceName+"/")
}

func unauthenticated(details string) error {
	return serviceerrors.NewErrorAndStatus(codes.Unauthenticated, dexpb.ErrorSubStatus_ERROR_SUB_STATUS_UNCATEGORIZED, details).ToGRPCError()
}

func permissionDenied(details string) error {
	return serviceerrors.NewErrorAndStatus(codes.PermissionDenied, dexpb.ErrorSubStatus_ERROR_SUB_STATUS_UNCATEGORIZED, details).ToGRPCError()
}
