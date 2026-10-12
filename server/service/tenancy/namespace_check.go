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
	"fmt"

	"github.com/superdurable/dex/service"
	uclient "github.com/superdurable/dex/service/client"
)

// untaggedFlowsQuery matches Flows started without a tenant; server-internal workflows have no Flow type.
var untaggedFlowsQuery = fmt.Sprintf(
	"%s IS NOT NULL AND %s IS NULL",
	service.SearchAttributeDexWorkflowType,
	SearchAttributeDexTenantID,
)

// VerifyNamespaceHasOnlyTenantFlows refuses a namespace holding Flows without a tenant, which multi-tenant mode
// cannot isolate.
func VerifyNamespaceHasOnlyTenantFlows(ctx context.Context, client uclient.UnifiedClient) error {
	response, err := client.ListWorkflow(ctx, &uclient.ListWorkflowExecutionsRequest{
		PageSize: 1,
		Query:    untaggedFlowsQuery,
	})
	if err != nil {
		return fmt.Errorf("list Flows without %s: %w", SearchAttributeDexTenantID, err)
	}
	if len(response.Executions) > 0 {
		return fmt.Errorf(
			"multi-tenant mode requires a namespace without single-tenant Flows; found Flow %q",
			response.Executions[0].GetFlowId(),
		)
	}
	return nil
}
