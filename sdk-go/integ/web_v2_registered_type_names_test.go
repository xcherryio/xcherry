// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package integ

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/superdurable/dex/sdk-go/dex"
	"github.com/superdurable/dex/sdk-go/integ/webv2approval"
	"github.com/superdurable/dex/sdk-go/integ/webv2reply"
)

const (
	dexWebV2ConvergenceTimeout = 30 * time.Second
	dexWebV2PollInterval       = 250 * time.Millisecond
	dexWebV2ActionRPCName      = "ApproveDecision"
)

type dexWebV2TestFlow struct {
	flow          dex.Flow
	flowType      string
	startStepType string
	input         any
	summaryKey    string
	summaryValue  string
	editableKey   string
	permission    string
}

type dexWebV2Client struct {
	baseURL    string
	httpClient *http.Client
	revision   string
}

type dexWebV2Catalog struct {
	Flows []struct {
		FlowType   string `json:"flowType"`
		Definition struct {
			Start *struct {
				StepType string `json:"stepType"`
			} `json:"start"`
		} `json:"definition"`
	} `json:"flows"`
	DefinitionRevision string `json:"definitionRevision"`
}

type dexWebV2SearchResponse struct {
	Flows []struct {
		FlowID   string         `json:"flowId"`
		FlowType string         `json:"flowType"`
		Summary  map[string]any `json:"summary"`
	} `json:"flows"`
}

type dexWebV2DisplayResponse struct {
	FlowType        string   `json:"flowType"`
	EligibleActions []string `json:"eligibleActions"`
}

type dexWebFlowSummary struct {
	RunID    string `json:"runId"`
	FlowType string `json:"flowType"`
}

type dexWebHistoryPage struct {
	Events []struct {
		Payload map[string]any `json:"payload"`
	} `json:"events"`
	NextPageToken       string `json:"nextPageToken"`
	NextInternalEventID int64  `json:"nextInternalEventId"`
}

type dexWebFlowDefinitions struct {
	Definitions []struct {
		FlowName      string `json:"flowName"`
		SchemaVersion string `json:"schemaVersion"`
		Valid         bool   `json:"valid"`
		Graph         struct {
			Nodes []struct {
				Kind string `json:"kind"`
				Name string `json:"name"`
			} `json:"nodes"`
		} `json:"graph"`
	} `json:"definitions"`
}

func TestDexWebV2ServesGoFlowsWithoutTypeOverrides(t *testing.T) {
	ctx := integrationContext(t)
	web := &dexWebV2Client{baseURL: "http://" + dexWebAddress(), httpClient: &http.Client{Timeout: 10 * time.Second}}
	requester := newFlowID(t, "requester")
	thread := newFlowID(t, "thread")
	flows := []dexWebV2TestFlow{
		{
			flow:          &webv2approval.ApprovalFlow{},
			flowType:      dex.GetFinalFlowType(&webv2approval.ApprovalFlow{}),
			startStepType: webv2approval.StartStepType(),
			input:         webv2approval.Input{Requester: requester},
			summaryKey:    "approval-requester",
			summaryValue:  requester,
			editableKey:   "approval-note",
			permission:    webv2approval.DecidePermission,
		},
		{
			flow:          &webv2reply.ReplyFlow{},
			flowType:      dex.GetFinalFlowType(&webv2reply.ReplyFlow{}),
			startStepType: webv2reply.StartStepType(),
			input:         webv2reply.Input{Thread: thread},
			summaryKey:    "reply-thread",
			summaryValue:  thread,
			editableKey:   "reply-draft",
			permission:    webv2reply.DecidePermission,
		},
	}
	require.Equal(t, "ApprovalFlow", flows[0].flowType)
	require.Equal(t, "ReplyFlow", flows[1].flowType)
	require.Equal(t, "awaitDecision", flows[0].startStepType)

	var catalog dexWebV2Catalog
	require.Eventually(t, func() bool {
		status, err := web.getJSON("/api/v2/catalog", &catalog)
		return err == nil && status == http.StatusOK && len(catalog.Flows) == len(flows)
	}, dexWebV2ConvergenceTimeout, dexWebV2PollInterval, "Dex Web v2 catalog did not list both Flow Definition Graphs")
	web.revision = catalog.DefinitionRevision
	catalogStartSteps := make(map[string]string, len(catalog.Flows))
	for _, entry := range catalog.Flows {
		require.NotNil(t, entry.Definition.Start, entry.FlowType)
		catalogStartSteps[entry.FlowType] = entry.Definition.Start.StepType
	}
	require.Equal(t, map[string]string{
		flows[0].flowType: flows[0].startStepType,
		flows[1].flowType: flows[1].startStepType,
	}, catalogStartSteps)

	runIDsByFlowType := make(map[string][]string, len(flows))
	for index, testFlow := range flows {
		otherFlowType := flows[1-index].flowType
		flowID := newFlowID(t, "web-v2-registered-name")
		_, err := integClient.StartFlow(ctx, testFlow.flow, flowID, testFlow.input, dex.StartFlowOptions{})
		require.NoError(t, err)
		runIDsByFlowType[testFlow.flowType] = append(runIDsByFlowType[testFlow.flowType], flowID)

		require.Eventually(t, func() bool {
			summary, found, err := web.searchSummary(testFlow.flowType, flowID)
			return err == nil && found && summary[testFlow.summaryKey] == testFlow.summaryValue
		}, dexWebV2ConvergenceTimeout, dexWebV2PollInterval, "v2 search did not return %s under %s", flowID, testFlow.flowType)
		_, foundUnderOtherType, err := web.searchSummary(otherFlowType, flowID)
		require.NoError(t, err)
		require.False(t, foundUnderOtherType, "%s appeared under %s", flowID, otherFlowType)

		var display dexWebV2DisplayResponse
		status, err := web.getJSON(web.displayPath(testFlow.flowType, flowID), &display)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, status)
		require.Equal(t, testFlow.flowType, display.FlowType)
		status, err = web.getJSON(web.displayPath(otherFlowType, flowID), nil)
		require.NoError(t, err)
		require.Equal(t, http.StatusNotFound, status)

		status, err = web.sendJSON(http.MethodPatch, "/api/v2/display", map[string]any{
			"flowType": testFlow.flowType, "flowId": flowID, "attributeKey": testFlow.editableKey, "value": "edited in Dex Web",
		}, nil)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, status)

		web.approveDecision(t, testFlow, flowID)
		require.Equal(t, dex.FlowCompleted, waitForFlow(t, flowID, false).Status)
	}

	t.Run("StartFromDexWeb", func(t *testing.T) {
		if isPullWorkerMode {
			t.Skip("Dex Web v2 start requires a push Worker address")
		}
		webStartedFlowID := newFlowID(t, "web-v2-start")
		status, err := web.sendJSON(http.MethodPost, "/api/v2/start", map[string]any{
			"flowType": flows[0].flowType, "flowId": webStartedFlowID, "workerTargetAddress": integWorkerTargetAddress,
			"input": flows[0].input,
		}, nil)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, status)
		runIDsByFlowType[flows[0].flowType] = append(runIDsByFlowType[flows[0].flowType], webStartedFlowID)
		var webStartedSummary dexWebFlowSummary
		status, err = web.getJSON("/api/flows/summary?flowId="+url.QueryEscape(webStartedFlowID), &webStartedSummary)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, status)
		require.Equal(t, flows[0].flowType, webStartedSummary.FlowType)
		web.approveDecision(t, flows[0], webStartedFlowID)
		require.Equal(t, dex.FlowCompleted, waitForFlow(t, webStartedFlowID, false).Status)
	})

	stepNames := web.definitionStepNames(t)
	for flowType, flowIDs := range runIDsByFlowType {
		require.NotEmpty(t, stepNames[flowType], flowType)
		for _, flowID := range flowIDs {
			historyStepTypes := web.historyStepTypes(t, flowID)
			require.NotEmpty(t, historyStepTypes, flowID)
			for _, stepType := range historyStepTypes {
				require.Contains(t, stepNames[flowType], stepType, "history of %s", flowID)
			}
		}
	}

	response, err := web.httpClient.Get(web.baseURL + "/v2/run/" + flows[0].flowType)
	require.NoError(t, err)
	require.NoError(t, response.Body.Close())
	require.Equal(t, http.StatusOK, response.StatusCode)
	require.True(t, strings.HasPrefix(response.Header.Get("Content-Type"), "text/html"), response.Header.Get("Content-Type"))
}

func (web *dexWebV2Client) approveDecision(t *testing.T, testFlow dexWebV2TestFlow, flowID string) {
	t.Helper()
	require.Eventually(t, func() bool {
		var display dexWebV2DisplayResponse
		status, err := web.getJSON(web.displayPath(testFlow.flowType, flowID), &display)
		if err != nil || status != http.StatusOK {
			return false
		}
		for _, action := range display.EligibleActions {
			if action == dexWebV2ActionRPCName {
				return true
			}
		}
		return false
	}, dexWebV2ConvergenceTimeout, dexWebV2PollInterval, "%s never offered %s", flowID, dexWebV2ActionRPCName)
	status, err := web.sendJSON(http.MethodPost, "/api/v2/actions", map[string]any{
		"flowType": testFlow.flowType, "flowId": flowID, "rpcName": dexWebV2ActionRPCName,
		"workQueuePermissions": []string{testFlow.permission}, "input": map[string]any{}, "attributeSnapshot": map[string]any{},
	}, nil)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, status)
}

func (web *dexWebV2Client) searchSummary(flowType string, flowID string) (map[string]any, bool, error) {
	var result dexWebV2SearchResponse
	status, err := web.sendJSON(http.MethodPost, "/api/v2/search", map[string]any{
		"flowType": flowType, "workQueuePermissions": []string{}, "filters": []any{}, "pageSize": 100,
	}, &result)
	if err != nil {
		return nil, false, err
	}
	if status != http.StatusOK {
		return nil, false, fmt.Errorf("search %s returned HTTP %d", flowType, status)
	}
	for _, row := range result.Flows {
		if row.FlowID == flowID {
			return row.Summary, true, nil
		}
	}
	return nil, false, nil
}

func (web *dexWebV2Client) definitionStepNames(t *testing.T) map[string][]string {
	t.Helper()
	var definitions dexWebFlowDefinitions
	status, err := web.getJSON("/api/flow-definitions", &definitions)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, status)
	stepNames := make(map[string][]string)
	for _, definition := range definitions.Definitions {
		if definition.SchemaVersion != "2.0" || !definition.Valid {
			continue
		}
		for _, node := range definition.Graph.Nodes {
			if node.Kind == "step" {
				stepNames[definition.FlowName] = append(stepNames[definition.FlowName], node.Name)
			}
		}
	}
	return stepNames
}

func (web *dexWebV2Client) historyStepTypes(t *testing.T, flowID string) []string {
	t.Helper()
	var summary dexWebFlowSummary
	status, err := web.getJSON("/api/flows/summary?flowId="+url.QueryEscape(flowID), &summary)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, status)
	stepTypes := make([]string, 0)
	query := url.Values{"flowId": {flowID}, "runId": {summary.RunID}, "startInternalEventId": {"0"}, "estimatePageSize": {"200"}}
	for {
		var page dexWebHistoryPage
		status, err := web.getJSON("/api/flows/history?"+query.Encode(), &page)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, status)
		for _, event := range page.Events {
			stepContext, _ := event.Payload["context"].(map[string]any)
			if stepType, isString := stepContext["stepType"].(string); isString && stepType != "" {
				stepTypes = append(stepTypes, stepType)
			}
		}
		if page.NextPageToken == "" {
			return stepTypes
		}
		query.Set("nextPageToken", page.NextPageToken)
		query.Set("startInternalEventId", fmt.Sprint(page.NextInternalEventID))
	}
}

func (web *dexWebV2Client) displayPath(flowType string, flowID string) string {
	return "/api/v2/display?" + url.Values{"flowType": {flowType}, "flowId": {flowID}}.Encode()
}

func (web *dexWebV2Client) getJSON(path string, output any) (int, error) {
	return web.doJSON(http.MethodGet, path, nil, output)
}

func (web *dexWebV2Client) sendJSON(method string, path string, body any, output any) (int, error) {
	encoded, err := json.Marshal(body)
	if err != nil {
		return 0, err
	}
	return web.doJSON(method, path, bytes.NewReader(encoded), output)
}

func (web *dexWebV2Client) doJSON(method string, path string, body io.Reader, output any) (int, error) {
	request, err := http.NewRequest(method, web.baseURL+path, body)
	if err != nil {
		return 0, err
	}
	request.Header.Set("Content-Type", "application/json")
	if web.revision != "" {
		request.Header.Set("X-Dex-Flow-Definition-Revision", web.revision)
	}
	response, err := web.httpClient.Do(request)
	if err != nil {
		return 0, err
	}
	payload, readErr := io.ReadAll(response.Body)
	if err := errors.Join(readErr, response.Body.Close()); err != nil {
		return response.StatusCode, err
	}
	if output != nil && response.StatusCode == http.StatusOK {
		if err := json.Unmarshal(payload, output); err != nil {
			return response.StatusCode, fmt.Errorf("decode %s %s: %w: %s", method, path, err, payload)
		}
	}
	return response.StatusCode, nil
}

func dexWebAddress() string {
	if address := os.Getenv("DEX_WEB_ADDRESS"); address != "" {
		return address
	}
	return "127.0.0.1:8802"
}
