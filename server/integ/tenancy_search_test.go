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
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/superdurable/dex/gen/dexpb"
	"github.com/superdurable/dex/integ/workflow/basic"
	"github.com/superdurable/dex/service"
	"github.com/superdurable/dex/service/tenancy"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestTenantSearchTemporal(t *testing.T) {
	if !*temporalIntegTest || !*testSearchIntegTest || *dexServerAddress != "" {
		t.Skip()
	}
	pair := startTenantPair(t)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	marker := strings.ReplaceAll(uuid.NewString(), "-", "")
	sharedFlowID := "search-" + marker
	for _, tenant := range []struct {
		client dexpb.FlowServiceClient
		target *dexpb.WorkerTarget
	}{{pair.alpha, pair.alphaTarget}, {pair.beta, pair.betaTarget}} {
		_, err := tenant.client.StartFlow(ctx, &dexpb.StartFlowRequest{
			RequestId:          newRequestID(),
			FlowId:             sharedFlowID,
			FlowType:           tenantProbeFlowType,
			FlowTimeoutSeconds: 120,
			StartStepType:      tenantProbeStep,
			FlowStartOptions: withWorkerTarget(&dexpb.FlowStartOptions{
				Attributes: []*dexpb.AttributeWrite{indexedKeywordAttribute(
					"CustomKeywordField", marker,
				)},
			}, tenant.target),
		})
		require.NoError(t, err)
	}
	parentFlowID := "search-parent-" + marker
	startTenantProbeFlow(t, ctx, pair.alpha, pair.alphaTarget, parentFlowID, tenantProbeNestingFlow, intValue(1))
	_, err := pair.alpha.WaitForFlow(ctx, &dexpb.WaitForFlowRequest{FlowId: parentFlowID, WaitTimeSeconds: 30})
	require.NoError(t, err)
	childFlowID := "SubFlow:" + parentFlowID + "-" + tenantProbeNestingStep + "-1-0"

	search := func(client dexpb.FlowServiceClient, query string) ([]*dexpb.SearchFlowsResponseEntry, error) {
		response, err := client.SearchFlows(ctx, &dexpb.SearchFlowsRequest{Query: query, PageSize: 100})
		return response.GetFlowRuns(), err
	}
	requireEventuallyFound := func(t *testing.T, client dexpb.FlowServiceClient, query string, flowIDs ...string) {
		t.Helper()
		require.Eventually(t, func() bool {
			entries, err := search(client, query)
			require.NoError(t, err, query)
			found := []string{}
			for _, entry := range entries {
				found = append(found, entry.GetFlowId())
			}
			sort.Strings(found)
			expected := append([]string(nil), flowIDs...)
			sort.Strings(expected)
			return fmt.Sprint(found) == fmt.Sprint(expected)
		}, 30*time.Second, 200*time.Millisecond, query)
	}

	t.Run("each tenant finds only its own Flows with application IDs", func(t *testing.T) {
		requireEventuallyFound(t, pair.alpha, `CustomKeywordField = "`+marker+`"`, sharedFlowID)
		requireEventuallyFound(t, pair.beta, `CustomKeywordField = "`+marker+`"`, sharedFlowID)
		requireEventuallyFound(t, pair.alpha, `WorkflowId = "`+sharedFlowID+`"`, sharedFlowID)
		requireEventuallyFound(t, pair.beta, `WorkflowId STARTS_WITH "search-parent-`+marker+`"`)
		requireEventuallyFound(t, pair.alpha,
			`CustomKeywordField = "`+marker+`" OR WorkflowId IN ("`+parentFlowID+`")`, parentFlowID, sharedFlowID)
	})

	t.Run("SubFlow parents are mapped in queries and out of results", func(t *testing.T) {
		query := service.SearchAttributeDexParentFlowID + ` = "` + parentFlowID + `"`
		requireEventuallyFound(t, pair.alpha, query, childFlowID)
		requireEventuallyFound(t, pair.beta, query)
		entries, err := search(pair.alpha, query)
		require.NoError(t, err)
		for _, attribute := range entries[0].GetIndexedAttributes() {
			require.NotContains(t, attribute.GetValue().GetStringValue(), "T:alpha:")
		}
	})

	t.Run("injection and unknown constructs are refused", func(t *testing.T) {
		refused := []string{
			`CustomKeywordField = "x") OR (CustomKeywordField = "y"`,
			`CustomKeywordField = "x" OR (1 = 1`,
			`DexTenantID = "beta"`,
			`CustomKeywordField = "x" OR DexTenantID = "beta"`,
			"`DexTenantID` IS NOT NULL",
			`CustomKeywordField = "x" ORDER BY DexTenantID`,
			`CustomKeywordField LIKE "x%"`,
			`CustomKeywordField = CustomKeywordField`,
			`1 = 1`,
			`CustomKeywordField = "x"; DROP TABLE executions`,
			`CustomKeywordField = "x" -- comment`,
			`WorkflowId = 42`,
		}
		for _, query := range refused {
			_, err := search(pair.alpha, query)
			require.Equal(t, codes.InvalidArgument, status.Code(err), "query %q: %v", query, err)
		}
		entries, err := search(pair.alpha, `CustomKeywordField = "x\") OR (CustomKeywordField != \"y"`)
		require.NoError(t, err, "a quote inside a string literal stays a literal")
		require.Empty(t, entries)
	})

	t.Run("the tenant index key cannot be written", func(t *testing.T) {
		tenantIndexWrite := []*dexpb.AttributeWrite{indexedKeywordAttribute(tenancy.SearchAttributeDexTenantID, "beta")}
		_, err := pair.alpha.SetAttributes(ctx, &dexpb.SetAttributesRequest{
			FlowId: sharedFlowID, Attributes: tenantIndexWrite, RequestId: uuid.NewString(),
		})
		require.Equal(t, codes.InvalidArgument, status.Code(err), "error: %v", err)
		_, err = pair.alpha.StartFlow(ctx, &dexpb.StartFlowRequest{
			RequestId: newRequestID(), FlowId: "tenant-index-" + marker, FlowType: tenantProbeFlowType,
			StartStepType: tenantProbeStep,
			FlowStartOptions: withWorkerTarget(&dexpb.FlowStartOptions{Attributes: tenantIndexWrite},
				pair.alphaTarget),
		})
		require.Equal(t, codes.InvalidArgument, status.Code(err), "error: %v", err)
	})

	t.Run("SyncAttributeIndexes stays refused", func(t *testing.T) {
		_, err := pair.alpha.SyncAttributeIndexes(ctx, &dexpb.SyncAttributeIndexRequest{
			AttributeIndexes: map[string]dexpb.IndexType{"CustomKeywordField": dexpb.IndexType_INDEX_TYPE_KEYWORD},
		})
		require.Equal(t, codes.PermissionDenied, status.Code(err), "error: %v", err)
	})
}

// TestTenantModeRefusesSingleTenantFlowsTemporal shows multi-tenant startup refuses a namespace with untagged Flows.
func TestTenantModeRefusesSingleTenantFlowsTemporal(t *testing.T) {
	if !*temporalIntegTest || !*testSearchIntegTest || *dexServerAddress != "" {
		t.Skip()
	}
	workerTarget := startWorker(t, basic.NewHandler())
	runtime := startDexService(t, DexServiceTestConfig{BackendType: service.BackendTypeTemporal})
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	_, err := runtime.FlowClient.StartFlow(ctx, &dexpb.StartFlowRequest{
		RequestId: newRequestID(), FlowId: basic.FlowType + "-" + uuid.NewString(), FlowType: basic.FlowType,
		StartStepType: basic.Step1, FlowStartOptions: withWorkerTarget(nil, workerTarget),
	})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return tenancy.VerifyNamespaceHasOnlyTenantFlows(ctx, runtime.UnifiedClient) != nil
	}, 30*time.Second, 200*time.Millisecond)
}
