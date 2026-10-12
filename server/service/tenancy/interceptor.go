// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package tenancy

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

const (
	// MaxCredentialBytes bounds a bearer credential before any parsing or hashing.
	MaxCredentialBytes       = 8 << 10
	authorizationMetadataKey = "authorization"
	bearerPrefix             = "bearer "
	// TenantAdminServiceName is the only service an operator principal may call.
	TenantAdminServiceName = "dex.TenantAdminService"
)

// PublicServiceInterceptors authenticate every public API call and allow each principal only on its own service.
type PublicServiceInterceptors struct {
	authenticator *Authenticator
}

func NewPublicServiceInterceptors(authenticator *Authenticator) *PublicServiceInterceptors {
	if authenticator == nil {
		panic("authenticator must not be nil")
	}
	return &PublicServiceInterceptors{authenticator: authenticator}
}

func (i *PublicServiceInterceptors) Unary(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	if isHealthMethod(info.FullMethod) {
		return handler(ctx, req)
	}
	principal, err := i.authenticate(ctx)
	if err != nil {
		return nil, err
	}
	if err := authorizePublicMethod(principal, info.FullMethod); err != nil {
		return nil, err
	}
	return handler(ContextWithPrincipal(ctx, principal), req)
}

// Stream refuses every streaming call except health, because no tenant-aware streaming RPC exists.
func (i *PublicServiceInterceptors) Stream(
	server interface{},
	stream grpc.ServerStream,
	info *grpc.StreamServerInfo,
	handler grpc.StreamHandler,
) error {
	if isHealthMethod(info.FullMethod) {
		return handler(server, stream)
	}
	if _, err := i.authenticate(stream.Context()); err != nil {
		return err
	}
	return permissionDenied("streaming calls are not allowed in multi-tenant mode")
}

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

func authorizePublicMethod(principal *Principal, fullMethod string) error {
	switch principal.Kind {
	case PrincipalKindTenant:
		if fullMethod == dexpb.FlowService_SyncAttributeIndexes_FullMethodName {
			return permissionDenied(
				"attribute indexes are registered by the operator in multi-tenant mode",
			)
		}
		if isServiceMethod(fullMethod, dexpb.FlowService_ServiceDesc.ServiceName) {
			return nil
		}
	case PrincipalKindOperator:
		if isServiceMethod(fullMethod, TenantAdminServiceName) {
			return nil
		}
	}
	return permissionDenied("the principal is not allowed on this method")
}

// InternalServiceInterceptor admits only callers presenting the internal token.
type InternalServiceInterceptor struct {
	token *InternalTokenFile
}

func NewInternalServiceInterceptor(token *InternalTokenFile) *InternalServiceInterceptor {
	if token == nil {
		panic("internal token file must not be nil")
	}
	return &InternalServiceInterceptor{token: token}
}

func (i *InternalServiceInterceptor) Unary(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	if isHealthMethod(info.FullMethod) {
		return handler(ctx, req)
	}
	credential, ok := bearerCredential(ctx)
	if !ok || !i.token.Accepts(credential) {
		return nil, unauthenticated("the internal token is required")
	}
	if !isServiceMethod(info.FullMethod, dexpb.InternalService_ServiceDesc.ServiceName) {
		return nil, permissionDenied("the principal is not allowed on this method")
	}
	return handler(ContextWithPrincipal(ctx, &Principal{Kind: PrincipalKindInternal}), req)
}

func bearerCredential(ctx context.Context) (string, bool) {
	incoming, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", false
	}
	values := incoming.Get(authorizationMetadataKey)
	if len(values) != 1 || len(values[0]) <= len(bearerPrefix) || len(values[0]) > MaxCredentialBytes {
		return "", false
	}
	if !strings.EqualFold(values[0][:len(bearerPrefix)], bearerPrefix) {
		return "", false
	}
	credential := strings.TrimSpace(values[0][len(bearerPrefix):])
	return credential, credential != ""
}

func isHealthMethod(fullMethod string) bool {
	return isServiceMethod(fullMethod, healthpb.Health_ServiceDesc.ServiceName)
}

func isServiceMethod(fullMethod string, serviceName string) bool {
	return strings.HasPrefix(fullMethod, "/"+serviceName+"/")
}

func unauthenticated(details string) error {
	return serviceerrors.NewErrorAndStatus(
		codes.Unauthenticated,
		dexpb.ErrorSubStatus_ERROR_SUB_STATUS_UNCATEGORIZED,
		details,
	).ToGRPCError()
}

func permissionDenied(details string) error {
	return serviceerrors.NewErrorAndStatus(
		codes.PermissionDenied,
		dexpb.ErrorSubStatus_ERROR_SUB_STATUS_UNCATEGORIZED,
		details,
	).ToGRPCError()
}
