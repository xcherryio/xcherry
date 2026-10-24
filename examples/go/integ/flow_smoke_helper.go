// Copyright (c) 2022-2026 Super Durable, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package integ

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type flowSmokeFlags struct {
	stepStartMayFail bool
	noStartStep      bool
}

type flowSmokeEntry struct {
	name    string
	trigger func(t *testing.T) (flowID string, runID string)
	flags   flowSmokeFlags
	// serverZDUnsupportedFeature names the feature that makes server-zd reject this sample.
	serverZDUnsupportedFeature string
}

type flowHistoryPage struct {
	FlowID string             `json:"flowId"`
	RunID  string             `json:"runId"`
	Events []flowHistoryEvent `json:"events"`
}

type flowHistoryEvent struct {
	Type    string         `json:"type"`
	Payload map[string]any `json:"payload"`
}

type flowStatePage struct {
	FlowStatus string `json:"flowStatus"`
}

type startFlowJSONResponse struct {
	FlowID string `json:"flowID"`
	RunID  string `json:"runID"`
}

var (
	runIDFromTextPattern = regexp.MustCompile(`runId\s+(\S+)`)
	dexcliPathOnce       sync.Once
	dexcliPathErr        error
)

func ensureDexcliPath(t *testing.T) {
	t.Helper()
	dexcliPathOnce.Do(func() {
		dexcliPath, dexcliPathErr = buildDexcliBinary()
	})
	require.NoError(t, dexcliPathErr)
	require.NotEmpty(t, dexcliPath)
}

func buildDexcliBinary() (string, error) {
	if path := strings.TrimSpace(os.Getenv("DEXCLI_PATH")); path != "" {
		return path, nil
	}
	repoRoot, err := examplesRepoRoot()
	if err != nil {
		return "", err
	}
	if err := ensureWebAssets(repoRoot); err != nil {
		return "", err
	}
	binaryPath := filepath.Join(os.TempDir(), fmt.Sprintf("dexcli-integ-%d", os.Getpid()))
	command := exec.Command("go", "build", "-trimpath", "-o", binaryPath, "./cmd/dexcli")
	command.Dir = filepath.Join(repoRoot, "cli")
	command.Env = append(os.Environ(), "GOWORK=off")
	output, err := command.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("build dexcli: %w\n%s", err, output)
	}
	return binaryPath, nil
}

func ensureWebAssets(repoRoot string) error {
	indexPath := filepath.Join(repoRoot, "web", "assets", "dist", "index.html")
	if _, err := os.Stat(indexPath); err == nil {
		return nil
	}
	npmCI := exec.Command("npm", "ci")
	npmCI.Dir = filepath.Join(repoRoot, "web")
	if output, err := npmCI.CombinedOutput(); err != nil {
		return fmt.Errorf("npm ci in web: %w\n%s", err, output)
	}
	npmBuild := exec.Command("npm", "run", "build")
	npmBuild.Dir = filepath.Join(repoRoot, "web")
	if output, err := npmBuild.CombinedOutput(); err != nil {
		return fmt.Errorf("npm run build in web: %w\n%s", err, output)
	}
	return nil
}

func examplesRepoRoot() (string, error) {
	wd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	dir := wd
	for {
		if _, statErr := os.Stat(filepath.Join(dir, "cli", "cmd", "dexcli", "main.go")); statErr == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("find repository root from %s", wd)
		}
		dir = parent
	}
}

func runDexcliFlowHistory(t *testing.T, flowID string, runID string) flowHistoryPage {
	t.Helper()
	args := []string{
		"flow", "history", flowID,
		"--server", flowServiceAddress(),
		"--output", "json",
		"--page-size", "50",
	}
	if runID != "" {
		args = append(args, "--run-id", runID)
	}
	var page flowHistoryPage
	runDexcliJSON(t, args, &page)
	return page
}

func runDexcliFlowState(t *testing.T, flowID string, runID string) flowStatePage {
	t.Helper()
	args := []string{
		"flow", "state", flowID,
		"--server", flowServiceAddress(),
		"--output", "json",
	}
	if runID != "" {
		args = append(args, "--run-id", runID)
	}
	var state flowStatePage
	runDexcliJSON(t, args, &state)
	return state
}

func runDexcliJSON(t *testing.T, args []string, output any) {
	t.Helper()
	ensureDexcliPath(t)
	command := exec.Command(dexcliPath, args...)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	require.NoError(t, command.Run(), "dexcli %s failed: %s", strings.Join(args, " "), stderr.String())
	require.NoError(t, json.Unmarshal(stdout.Bytes(), output), "decode dexcli output %q", stdout.String())
}

func assertFlowSmokeStartStep(t *testing.T, entry flowSmokeEntry, flowID string, runID string) {
	t.Helper()
	if entry.flags.noStartStep {
		return
	}
	require.Eventually(t, func() bool {
		history := runDexcliFlowHistory(t, flowID, runID)
		startStepType := flowStartedStartStepType(history.Events)
		if startStepType == "" {
			return false
		}
		if entry.flags.stepStartMayFail {
			return true
		}
		if hasStartStepProgress(history.Events, startStepType) {
			return true
		}
		state := runDexcliFlowState(t, flowID, runID)
		return state.FlowStatus == "FLOW_STATUS_RUNNING" && len(history.Events) > 1
	}, 30*time.Second, 200*time.Millisecond, "start step did not succeed for %s", entry.name)
}

func assertFlowSmokeNoUnexpectedFailures(t *testing.T, entry flowSmokeEntry, flowID string, runID string) {
	t.Helper()
	history := runDexcliFlowHistory(t, flowID, runID)
	for _, event := range history.Events {
		switch event.Type {
		case "StepExecuteFailed", "StepWaitForFailed":
			if entry.flags.stepStartMayFail {
				continue
			}
			require.Failf(t, "unexpected failure event", "%s: %s", entry.name, event.Type)
		case "FlowClosed":
			if isTerminalFlowClosedFailure(event.Payload) {
				if entry.flags.stepStartMayFail {
					continue
				}
				require.Failf(
					t,
					"unexpected terminal flow closure",
					"%s: payload=%v",
					entry.name,
					event.Payload,
				)
			}
		}
	}
}

func flowStartedStartStepType(events []flowHistoryEvent) string {
	for _, event := range events {
		if event.Type != "FlowStartedOrContinued" {
			continue
		}
		initialStart, ok := event.Payload["initialStart"].(map[string]any)
		if !ok {
			continue
		}
		startStepType, ok := initialStart["startStepType"].(string)
		if ok && startStepType != "" {
			return startStepType
		}
	}
	return ""
}

func hasStartStepProgress(events []flowHistoryEvent, startStepType string) bool {
	for _, event := range events {
		switch event.Type {
		case "StepWaitForCompleted", "StepExecuteCompleted":
			if historyEventStepType(event.Payload) == startStepType {
				return true
			}
		}
	}
	return false
}

func historyEventStepType(payload map[string]any) string {
	if stepType, ok := payload["stepType"].(string); ok && stepType != "" {
		return stepType
	}
	if stepContext, ok := payload["context"].(map[string]any); ok {
		if stepType, ok := stepContext["stepType"].(string); ok && stepType != "" {
			return stepType
		}
	}
	if input, ok := payload["input"].(map[string]any); ok {
		if stepType, ok := input["stepType"].(string); ok {
			return stepType
		}
	}
	return ""
}

func isTerminalFlowClosedFailure(payload map[string]any) bool {
	switch status := payload["flowStatus"].(type) {
	case string:
		switch status {
		case "FLOW_STATUS_COMPLETED", "FLOW_STATUS_CONTINUED_AS_NEW", "FLOW_STATUS_RUNNING":
			return false
		case "FLOW_STATUS_UNSPECIFIED", "":
		default:
			return true
		}
	case float64:
		if status == 2 || status == 7 {
			return false
		}
		if status != 0 {
			return true
		}
	}
	errorType, _ := payload["errorType"].(string)
	return errorType != "" && errorType != "FLOW_ERROR_TYPE_UNSPECIFIED"
}

func triggerFlowSmokeHTTP(
	t *testing.T,
	method string,
	path string,
	query url.Values,
	body any,
) (flowID string, runID string) {
	flowID, runID, _ = triggerFlowSmokeHTTPWithBody(t, method, path, query, body)
	return flowID, runID
}

func triggerFlowSmokeHTTPWithBody(
	t *testing.T,
	method string,
	path string,
	query url.Values,
	body any,
) (flowID string, runID string, responseBody []byte) {
	t.Helper()
	requestURL := examplesAPIURL + path
	if len(query) > 0 {
		requestURL += "?" + query.Encode()
	}
	var bodyReader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		require.NoError(t, err)
		bodyReader = bytes.NewReader(encoded)
	}
	request, err := http.NewRequestWithContext(integrationContext(t), method, requestURL, bodyReader)
	require.NoError(t, err)
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := http.DefaultClient.Do(request)
	require.NoError(t, err)
	defer response.Body.Close()
	responseBody, err = io.ReadAll(response.Body)
	require.NoError(t, err)
	require.True(
		t,
		response.StatusCode >= 200 && response.StatusCode < 300,
		"%s %s returned %d: %s",
		method,
		path,
		response.StatusCode,
		string(responseBody),
	)
	flowID, runID = parseFlowTriggerResponse(string(responseBody), firstNonEmpty(query.Get("workflowId"), query.Get("username")))
	return flowID, runID, responseBody
}

func parseFlowTriggerResponse(body string, workflowIDFromQuery string) (flowID string, runID string) {
	trimmed := strings.TrimSpace(body)
	var jsonResponse startFlowJSONResponse
	if err := json.Unmarshal([]byte(trimmed), &jsonResponse); err == nil {
		if jsonResponse.FlowID != "" {
			return jsonResponse.FlowID, jsonResponse.RunID
		}
		var flowIDOnly map[string]string
		if err := json.Unmarshal([]byte(trimmed), &flowIDOnly); err == nil {
			if value := flowIDOnly["flowID"]; value != "" {
				return value, ""
			}
		}
	}
	if matches := runIDFromTextPattern.FindStringSubmatch(trimmed); len(matches) == 2 {
		return workflowIDFromQuery, matches[1]
	}
	if strings.HasPrefix(trimmed, "Started workflowId: ") {
		return strings.TrimPrefix(trimmed, "Started workflowId: "), ""
	}
	if strings.HasPrefix(trimmed, "started workflowId: ") {
		return strings.TrimPrefix(trimmed, "started workflowId: "), ""
	}
	if workflowIDFromQuery != "" {
		return workflowIDFromQuery, ""
	}
	return "", trimmed
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func smokeWorkflowID(t *testing.T, prefix string) string {
	t.Helper()
	return newFlowID(t, prefix)
}
