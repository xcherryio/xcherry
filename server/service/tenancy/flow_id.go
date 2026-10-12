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
	"errors"
	"fmt"
	"strings"

	"github.com/superdurable/dex/service"
)

const storedFlowIDPrefix = "T:"

// StoredFlowID returns the engine identifier of an application Flow ID. An empty tenant leaves it unchanged.
func StoredFlowID(tenantID string, applicationFlowID string) string {
	if tenantID == "" {
		return applicationFlowID
	}
	return storedFlowIDPrefix + tenantID + ":" + applicationFlowID
}

// ApplicationFlowID strips the tenant prefix from a stored Flow ID.
// It returns false when storedFlowID belongs to another tenant, which callers treat as not found.
// With an empty tenant it returns false for any tenant's stored Flow ID.
func ApplicationFlowID(tenantID string, storedFlowID string) (string, bool) {
	if tenantID == "" {
		return storedFlowID, !strings.HasPrefix(storedFlowID, storedFlowIDPrefix)
	}
	prefix := storedFlowIDPrefix + tenantID + ":"
	if !strings.HasPrefix(storedFlowID, prefix) {
		return "", false
	}
	return storedFlowID[len(prefix):], true
}

// ErrStoredFlowIDTooLong reports a Flow ID whose stored form exceeds the configured identifier limit.
var ErrStoredFlowIDTooLong = errors.New("flow ID is too long")

// ValidateStoredFlowIDLength refuses a stored Flow ID beyond the engine's identifier limit.
func ValidateStoredFlowIDLength(storedFlowID string, maxStoredFlowIDBytes int) error {
	if len(storedFlowID) > maxStoredFlowIDBytes {
		return fmt.Errorf(
			"%w: %d bytes with its tenant prefix; the limit is %d bytes",
			ErrStoredFlowIDTooLong,
			len(storedFlowID),
			maxStoredFlowIDBytes,
		)
	}
	return nil
}

// TenantID returns the tenant of the request's principal, or empty in single-tenant mode.
func TenantID(ctx context.Context) string {
	principal, ok := PrincipalFromContext(ctx)
	if !ok || principal.Kind != PrincipalKindTenant {
		return ""
	}
	return principal.TenantID
}

// DispatchFlowID returns the application Flow ID that a Worker may see for a stored Flow ID.
// It fails when the dispatching input's tenant disagrees with the Flow ID's tenant prefix.
func DispatchFlowID(tenantID string, storedFlowID string) (string, error) {
	applicationFlowID, ok := ApplicationFlowID(tenantID, storedFlowID)
	if !ok {
		return "", fmt.Errorf("dispatch tenant %q does not own Flow %q", tenantID, storedFlowID)
	}
	return applicationFlowID, nil
}

// SubFlowID returns the stored ID of a SubFlow, built from the parent's application Flow ID.
// The tenant prefix appears once however deep the nesting. It fails when the tenant does not own the parent.
func SubFlowID(tenantID string, parentStoredFlowID string, stepExecutionID string, index int32) (string, error) {
	parentApplicationFlowID, err := DispatchFlowID(tenantID, parentStoredFlowID)
	if err != nil {
		return "", err
	}
	return StoredFlowID(tenantID, service.SubFlowID(parentApplicationFlowID, stepExecutionID, index)), nil
}
