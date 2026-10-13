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
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"text/template"

	"github.com/superdurable/dex/config"
	"github.com/superdurable/dex/gen/dexpb"
)

// ErrWorkerTargetNotAllowed reports a Worker target outside the tenant's allowed pattern.
var ErrWorkerTargetNotAllowed = errors.New("worker target is not allowed for this tenant")

// WorkerTargetPolicy resolves and checks the Worker targets of a tenant's Flows from the principal's claims.
type WorkerTargetPolicy struct {
	defaultTarget *template.Template
	allowed       *template.Template
	// compiledAllowed caches each rendered pattern, so dispatches do not recompile it.
	compiledAllowed *boundedRegexpCache
}

// NewWorkerTargetPolicy returns nil in single-tenant mode, where any Worker target is accepted.
func NewWorkerTargetPolicy(cfg *config.TenancyConfig) (*WorkerTargetPolicy, error) {
	if cfg == nil {
		panic("tenancy config must not be nil")
	}
	if !cfg.IsMultiTenant() {
		return nil, nil
	}
	allowed, err := config.NewTenantClaimTemplate("allowed", cfg.WorkerTarget.Allowed)
	if err != nil {
		return nil, err
	}
	policy := &WorkerTargetPolicy{allowed: allowed, compiledAllowed: newBoundedRegexpCache(maxCompiledAllowedPatterns)}
	if cfg.WorkerTarget.Default != "" {
		if policy.defaultTarget, err = config.NewTenantClaimTemplate("default", cfg.WorkerTarget.Default); err != nil {
			return nil, err
		}
	}
	return policy, nil
}

// Resolve returns the target a tenant's Flow uses: the rendered default when none is requested, otherwise the
// requested target if the allowed pattern matches it. A nil policy returns requested unchanged.
func (p *WorkerTargetPolicy) Resolve(claims map[string]string, requested *dexpb.WorkerTarget) (*dexpb.WorkerTarget, error) {
	if p == nil {
		return requested, nil
	}
	if requested.GetAddress() == "" {
		if p.defaultTarget == nil {
			return nil, fmt.Errorf("%w: a worker target is required", ErrWorkerTargetNotAllowed)
		}
		var address strings.Builder
		if err := p.defaultTarget.Execute(&address, claims); err != nil {
			return nil, fmt.Errorf("%w: render the default target: %v", ErrWorkerTargetNotAllowed, err)
		}
		requested = &dexpb.WorkerTarget{
			Address:           address.String(),
			IsHeadlessAddress: requested.GetIsHeadlessAddress(),
		}
	}
	return requested, p.Check(claims, requested)
}

// Check matches target against the tenant's allowed pattern. A nil policy accepts every target.
func (p *WorkerTargetPolicy) Check(claims map[string]string, target *dexpb.WorkerTarget) error {
	if p == nil {
		return nil
	}
	var pattern strings.Builder
	if err := p.allowed.Execute(&pattern, claims); err != nil {
		return fmt.Errorf("%w: render the allowed pattern: %v", ErrWorkerTargetNotAllowed, err)
	}
	allowed, err := p.compiledAllowed.compile(`^(?:` + pattern.String() + `)$`)
	if err != nil {
		return fmt.Errorf("%w: the allowed pattern is invalid: %v", ErrWorkerTargetNotAllowed, err)
	}
	if !allowed.MatchString(target.GetAddress()) {
		return fmt.Errorf("%w: %q", ErrWorkerTargetNotAllowed, target.GetAddress())
	}
	return nil
}

// TenantClaims returns the claims of the request's tenant principal, or nil in single-tenant mode.
func TenantClaims(ctx context.Context) map[string]string {
	principal, ok := PrincipalFromContext(ctx)
	if !ok || principal.Kind != PrincipalKindTenant {
		return nil
	}
	return principal.Claims
}

// maxCompiledAllowedPatterns bounds the per-process cache of rendered allowed patterns, one per distinct claims set.
const maxCompiledAllowedPatterns = 10_000

// boundedRegexpCache keeps compiled expressions by source, evicting the least recently used.
type boundedRegexpCache struct {
	capacity int
	mu       sync.Mutex
	entries  map[string]*list.Element
	recency  *list.List
}

type compiledRegexpEntry struct {
	source   string
	compiled *regexp.Regexp
}

func newBoundedRegexpCache(capacity int) *boundedRegexpCache {
	return &boundedRegexpCache{capacity: capacity, entries: map[string]*list.Element{}, recency: list.New()}
}

func (c *boundedRegexpCache) compile(source string) (*regexp.Regexp, error) {
	c.mu.Lock()
	if element, ok := c.entries[source]; ok {
		c.recency.MoveToFront(element)
		compiled := element.Value.(*compiledRegexpEntry).compiled
		c.mu.Unlock()
		return compiled, nil
	}
	c.mu.Unlock()
	compiled, err := regexp.Compile(source)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.entries[source]; !ok {
		c.entries[source] = c.recency.PushFront(&compiledRegexpEntry{source: source, compiled: compiled})
		if c.recency.Len() > c.capacity {
			oldest := c.recency.Back()
			c.recency.Remove(oldest)
			delete(c.entries, oldest.Value.(*compiledRegexpEntry).source)
		}
	}
	return compiled, nil
}
