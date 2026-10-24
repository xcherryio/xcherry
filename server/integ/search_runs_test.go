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
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/superdurable/dex/gen/dexpb"
	"github.com/superdurable/dex/integ/workflow/basic"
	"github.com/superdurable/dex/integ/workflow/deadend"
	"github.com/superdurable/dex/service"
	"github.com/superdurable/dex/service/bootstrap"
	"github.com/superdurable/dex/service/common/ptr"
	"go.uber.org/cadence/.gen/go/shared"
)

func TestSearchRunsTemporal(t *testing.T) {
	if !*temporalIntegTest {
		t.Skip()
	}
	verifySearchRunInclusion(t, service.BackendTypeTemporal)
}

func TestSearchRunsCadence(t *testing.T) {
	if !*cadenceIntegTest {
		t.Skip()
	}
	verifySearchRunInclusion(t, service.BackendTypeCadence)
}

func verifySearchRunInclusion(t *testing.T, backend service.BackendType) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cfg := DexServiceTestConfig{BackendType: backend}
	if backend == service.BackendTypeCadence {
		cfg.CadenceDomain = "dex-search-runs-" + uuid.NewString()
		cadenceService, _, closeClient, err := bootstrap.BuildCadenceServiceClient(bootstrap.DefaultCadenceHostPort)
		require.NoError(t, err)
		defer closeClient()
		require.NoError(t, cadenceService.RegisterDomain(ctx, &shared.RegisterDomainRequest{
			Name:                                   ptr.Any(cfg.CadenceDomain),
			WorkflowExecutionRetentionPeriodInDays: ptr.Any(int32(1)),
		}))
	}
	runtime := startDexService(t, cfg)
	flowClient := runtime.FlowClient
	// C3: server-zd never returns CONTINUED_AS_NEW runs.
	isContinueAsNewSupported := !isServerZDBinaryMode()
	completedID := "search-completed-" + uuid.NewString()
	workerTarget := startWorker(t, basic.NewHandler())
	_, err := flowClient.StartFlow(ctx, &dexpb.StartFlowRequest{
		FlowId: completedID, FlowType: basic.FlowType, RequestId: newRequestID(),
		StartStepType: basic.Step1,
		FlowStartOptions: withWorkerTarget(&dexpb.FlowStartOptions{
			FlowConfigOverride: minimumContinueAsNewAsyncDurabilityConfig(),
		}, workerTarget),
	})
	require.NoError(t, err)
	result, err := flowClient.WaitForFlow(ctx, &dexpb.WaitForFlowRequest{
		FlowId: completedID, WaitTimeSeconds: 20,
	})
	require.NoError(t, err)
	require.Equal(t, dexpb.FlowStatus_FLOW_STATUS_COMPLETED, result.GetFlowStatus())

	expected := map[string]dexpb.FlowStatus{completedID: dexpb.FlowStatus_FLOW_STATUS_COMPLETED}
	deadendTarget := startWorker(t, deadend.NewHandler())
	runningID := ""
	for _, scenario := range []struct {
		stop   dexpb.StopType
		status dexpb.FlowStatus
	}{
		{dexpb.StopType_STOP_TYPE_UNSPECIFIED, dexpb.FlowStatus_FLOW_STATUS_RUNNING},
		{dexpb.StopType_STOP_TYPE_FAIL, dexpb.FlowStatus_FLOW_STATUS_FAILED},
		{dexpb.StopType_STOP_TYPE_CANCEL, dexpb.FlowStatus_FLOW_STATUS_CANCELED},
		{dexpb.StopType_STOP_TYPE_TERMINATE, dexpb.FlowStatus_FLOW_STATUS_TERMINATED},
	} {
		id := "search-status-" + uuid.NewString()
		_, err := flowClient.StartFlow(ctx, &dexpb.StartFlowRequest{
			FlowId: id, FlowType: deadend.WorkflowType, RequestId: newRequestID(),
			StartStepType:    deadend.State1,
			StepOptions:      &dexpb.StepOptions{SkipWaitFor: true},
			FlowStartOptions: withWorkerTarget(&dexpb.FlowStartOptions{}, deadendTarget),
		})
		require.NoError(t, err)
		if scenario.status != dexpb.FlowStatus_FLOW_STATUS_RUNNING {
			_, err = flowClient.StopFlow(ctx, &dexpb.StopFlowRequest{
				FlowId: id, StopType: scenario.stop, Reason: "search status coverage",
			})
			require.NoError(t, err)
		} else {
			runningID = id
			t.Cleanup(func() {
				_, err := flowClient.StopFlow(context.Background(), &dexpb.StopFlowRequest{
					FlowId: id, StopType: dexpb.StopType_STOP_TYPE_TERMINATE,
				})
				require.NoError(t, err)
			})
		}
		expected[id] = scenario.status
	}
	workflowIDField := "WorkflowId"
	if backend == service.BackendTypeCadence {
		workflowIDField = "WorkflowID"
	}
	query := ""
	for id := range expected {
		if query != "" {
			query += " OR "
		}
		query += fmt.Sprintf("%s = '%s'", workflowIDField, id)
	}
	var allRuns []*dexpb.SearchFlowsResponseEntry
	require.Eventually(t, func() bool {
		var err error
		allRuns, err = searchRunPages(ctx, flowClient, query, true, 2)
		if err != nil {
			return false
		}
		statuses := map[string]dexpb.FlowStatus{}
		continued := 0
		for _, run := range allRuns {
			if run.GetFlowStatus() == dexpb.FlowStatus_FLOW_STATUS_CONTINUED_AS_NEW {
				continued++
			} else {
				statuses[run.GetFlowId()] = run.GetFlowStatus()
			}
		}
		return (continued > 0) == isContinueAsNewSupported && len(statuses) == len(expected) && sameSearchStatuses(statuses, expected)
	}, 30*time.Second, 200*time.Millisecond, "expected full visible execution chain and other statuses")

	for _, explicitFalse := range []bool{false, true} {
		t.Run(fmt.Sprintf("default-explicit-false-%t", explicitFalse), func(t *testing.T) {
			request := &dexpb.SearchFlowsRequest{Query: query, PageSize: 100}
			if explicitFalse {
				request.IncludeContinuedAsNew = false
			}
			response, err := flowClient.SearchFlows(ctx, request)
			require.NoError(t, err)
			require.Len(t, response.GetFlowRuns(), len(expected))
			for _, run := range response.GetFlowRuns() {
				require.Equal(t, expected[run.GetFlowId()], run.GetFlowStatus())
			}
		})
	}
	currentRuns, err := searchRunPages(ctx, flowClient, query, false, 1)
	require.NoError(t, err)
	require.Len(t, currentRuns, len(expected))
	allRunIDs := map[string]bool{}
	for _, run := range allRuns {
		require.False(t, allRunIDs[run.GetRunId()], "duplicate run across pages")
		allRunIDs[run.GetRunId()] = true
	}
	for _, run := range currentRuns {
		require.True(t, allRunIDs[run.GetRunId()], "missing current run from inclusive pages")
	}
	orQuery := fmt.Sprintf("%s = '%s' OR %s = '%s'", workflowIDField, completedID, workflowIDField, runningID)
	page, err := flowClient.SearchFlows(ctx, &dexpb.SearchFlowsRequest{Query: orQuery, PageSize: 100})
	require.NoError(t, err)
	require.Len(t, page.GetFlowRuns(), 2, "OR must not bypass the default exclusion")

	continuedFilter := `ExecutionStatus = "ContinuedAsNew"`
	if backend == service.BackendTypeCadence {
		continuedFilter = `CloseStatus = "CONTINUED_AS_NEW"`
	}
	continuedQuery := fmt.Sprintf("%s = '%s' AND %s", workflowIDField, completedID, continuedFilter)
	page, err = flowClient.SearchFlows(ctx, &dexpb.SearchFlowsRequest{Query: continuedQuery, PageSize: 100})
	require.NoError(t, err)
	require.Empty(t, page.GetFlowRuns())
	page, err = flowClient.SearchFlows(ctx, &dexpb.SearchFlowsRequest{
		Query: continuedQuery, PageSize: 100, IncludeContinuedAsNew: true,
	})
	require.NoError(t, err)
	if isContinueAsNewSupported {
		require.NotEmpty(t, page.GetFlowRuns())
	} else {
		require.Empty(t, page.GetFlowRuns())
	}
	for _, run := range page.GetFlowRuns() {
		require.Equal(t, dexpb.FlowStatus_FLOW_STATUS_CONTINUED_AS_NEW, run.GetFlowStatus())
	}
	// Inclusion must retain caller exclusions rather than force earlier runs into the results.
	currentQuery := fmt.Sprintf("%s = '%s' AND ExecutionStatus != \"ContinuedAsNew\"", workflowIDField, completedID)
	page, err = flowClient.SearchFlows(ctx, &dexpb.SearchFlowsRequest{
		Query: currentQuery, PageSize: 100, IncludeContinuedAsNew: true,
	})
	require.NoError(t, err)
	require.Len(t, page.GetFlowRuns(), 1)
	page, err = flowClient.SearchFlows(ctx, &dexpb.SearchFlowsRequest{
		PageSize: 1000, IncludeContinuedAsNew: true,
	})
	require.NoError(t, err)
	foundContinued := false
	for _, run := range page.GetFlowRuns() {
		foundContinued = foundContinued || run.GetFlowStatus() == dexpb.FlowStatus_FLOW_STATUS_CONTINUED_AS_NEW
	}
	if isContinueAsNewSupported {
		require.True(t, foundContinued, "inclusive empty query must allow earlier runs")
	} else {
		require.False(t, foundContinued)
	}
	for _, emptyQuery := range []string{"", "   "} {
		page, err = flowClient.SearchFlows(ctx, &dexpb.SearchFlowsRequest{Query: emptyQuery, PageSize: 10})
		require.NoError(t, err)
		for _, run := range page.GetFlowRuns() {
			require.NotEqual(t, dexpb.FlowStatus_FLOW_STATUS_CONTINUED_AS_NEW, run.GetFlowStatus())
		}
	}
}

func searchRunPages(ctx context.Context, client dexpb.FlowServiceClient, query string, include bool, pageSize int32) ([]*dexpb.SearchFlowsResponseEntry, error) {
	var runs []*dexpb.SearchFlowsResponseEntry
	seen := map[string]bool{}
	token := ""
	for pageNumber := 0; pageNumber < 100; pageNumber++ {
		response, err := client.SearchFlows(ctx, &dexpb.SearchFlowsRequest{
			Query: query, PageSize: pageSize, NextPageToken: token, IncludeContinuedAsNew: include,
		})
		if err != nil {
			return nil, err
		}
		for _, run := range response.GetFlowRuns() {
			if seen[run.GetRunId()] {
				return nil, fmt.Errorf("visibility changed during pagination: repeated run %s", run.GetRunId())
			}
			seen[run.GetRunId()] = true
		}
		runs = append(runs, response.GetFlowRuns()...)
		token = response.GetNextPageToken()
		if token == "" {
			return runs, nil
		}
	}
	return nil, fmt.Errorf("search did not finish within 100 pages")
}

func sameSearchStatuses(actual, expected map[string]dexpb.FlowStatus) bool {
	for id, status := range expected {
		if actual[id] != status {
			return false
		}
	}
	return true
}
