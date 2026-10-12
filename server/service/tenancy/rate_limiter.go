// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package tenancy

import (
	"container/list"
	"context"
	"sync"

	"github.com/superdurable/dex/config"
	"github.com/superdurable/dex/gen/dexpb"
	serviceerrors "github.com/superdurable/dex/service/common/errors"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

// maxRateLimitedTenants bounds the per-process limiter state; the least recently used tenant is evicted.
const maxRateLimitedTenants = 100_000

type rateLimitKind int

const (
	rateLimitStartFlow rateLimitKind = iota
	rateLimitWrite
	rateLimitSearch
	rateLimitKindCount
)

var rateLimitKindByMethod = map[string]rateLimitKind{
	dexpb.FlowService_StartFlow_FullMethodName:            rateLimitStartFlow,
	dexpb.FlowService_InvokeRPC_FullMethodName:            rateLimitWrite,
	dexpb.FlowService_PublishToChannel_FullMethodName:     rateLimitWrite,
	dexpb.FlowService_DeleteChannelMessage_FullMethodName: rateLimitWrite,
	dexpb.FlowService_SetAttributes_FullMethodName:        rateLimitWrite,
	dexpb.FlowService_WriteStream_FullMethodName:          rateLimitWrite,
	dexpb.FlowService_SearchFlows_FullMethodName:          rateLimitSearch,
}

// TenantRateLimiter holds per-process token buckets per tenant for starts, writes and searches.
type TenantRateLimiter struct {
	limits [rateLimitKindCount]config.TenantRateLimitConfig

	mu       sync.Mutex
	tenants  map[string]*list.Element
	recency  *list.List
	capacity int
}

type tenantRateLimiters struct {
	tenantID string
	buckets  [rateLimitKindCount]*rate.Limiter
}

func NewTenantRateLimiter(cfg *config.TenantLimitsConfig) *TenantRateLimiter {
	if cfg == nil {
		panic("tenant limits config must not be nil")
	}
	return newTenantRateLimiter(cfg, maxRateLimitedTenants)
}

func newTenantRateLimiter(cfg *config.TenantLimitsConfig, capacity int) *TenantRateLimiter {
	limiter := &TenantRateLimiter{tenants: map[string]*list.Element{}, recency: list.New(), capacity: capacity}
	limiter.limits[rateLimitStartFlow] = config.EffectiveTenantRateLimit(cfg.StartFlowsPerSecond, 20, 100)
	limiter.limits[rateLimitWrite] = config.EffectiveTenantRateLimit(cfg.WritesPerSecond, 100, 500)
	limiter.limits[rateLimitSearch] = config.EffectiveTenantRateLimit(cfg.SearchesPerSecond, 1, 5)
	return limiter
}

// Unary refuses a tenant call over its bucket with RESOURCE_EXHAUSTED; other calls pass unchanged.
func (l *TenantRateLimiter) Unary(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	kind, isLimited := rateLimitKindByMethod[info.FullMethod]
	tenantID := TenantID(ctx)
	if !isLimited || tenantID == "" || l.tenant(tenantID).buckets[kind].Allow() {
		return handler(ctx, req)
	}
	return nil, serviceerrors.NewErrorAndStatus(
		codes.ResourceExhausted,
		dexpb.ErrorSubStatus_ERROR_SUB_STATUS_UNCATEGORIZED,
		"the tenant's request rate limit is exceeded",
	).ToGRPCError()
}

func (l *TenantRateLimiter) tenant(tenantID string) *tenantRateLimiters {
	l.mu.Lock()
	defer l.mu.Unlock()
	if element, ok := l.tenants[tenantID]; ok {
		l.recency.MoveToFront(element)
		return element.Value.(*tenantRateLimiters)
	}
	limiters := &tenantRateLimiters{tenantID: tenantID}
	for kind, limit := range l.limits {
		limiters.buckets[kind] = rate.NewLimiter(rate.Limit(limit.Rate), limit.Burst)
	}
	l.tenants[tenantID] = l.recency.PushFront(limiters)
	if l.recency.Len() > l.capacity {
		oldest := l.recency.Back()
		l.recency.Remove(oldest)
		delete(l.tenants, oldest.Value.(*tenantRateLimiters).tenantID)
	}
	return limiters
}
