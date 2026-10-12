// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package tenancy

import "context"

const (
	// PrincipalKindTenant is an application of one tenant, allowed on FlowService.
	PrincipalKindTenant PrincipalKind = iota + 1
	// PrincipalKindOperator manages tenants and is allowed only on TenantAdminService.
	PrincipalKindOperator
	// PrincipalKindInternal is the server's own interpreter Worker, allowed only on InternalService.
	PrincipalKindInternal
)

type PrincipalKind int

// Principal is the authenticated caller of one request.
type Principal struct {
	Kind PrincipalKind
	// TenantID is set only for PrincipalKindTenant.
	TenantID string
	// Claims are the verified values that worker target templates may use.
	Claims map[string]string
}

type principalContextKey struct{}

func ContextWithPrincipal(ctx context.Context, principal *Principal) context.Context {
	return context.WithValue(ctx, principalContextKey{}, principal)
}

// PrincipalFromContext returns the principal the authentication interceptor attached, if any.
func PrincipalFromContext(ctx context.Context) (*Principal, bool) {
	principal, ok := ctx.Value(principalContextKey{}).(*Principal)
	return principal, ok
}
