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
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/superdurable/dex/examples/go/patterns/entity-store"
)

func flowSmokeCatalog() []flowSmokeEntry {
	return []flowSmokeEntry{
		{
			name: "products/engagement",
			trigger: func(t *testing.T) (string, string) {
				flowID, runID := triggerFlowSmokeHTTP(t, http.MethodGet, "/products/engagement/start", nil, nil)
				return flowID, runID
			},
		},
		{
			name: "products/microservices",
			trigger: func(t *testing.T) (string, string) {
				query := url.Values{"workflowId": {smokeWorkflowID(t, "microservices")}}
				return triggerFlowSmokeHTTP(t, http.MethodGet, "/products/microservices/start", query, nil)
			},
		},
		{
			name:                       "products/money-transfer",
			serverZDUnsupportedFeature: "failure policies",
			trigger: func(t *testing.T) (string, string) {
				query := url.Values{
					"amount":      {"100"},
					"fromAccount": {"from-smoke"},
					"toAccount":   {"to-smoke"},
					"notes":       {"smoke"},
				}
				flowID, runID := triggerFlowSmokeHTTP(
					t,
					http.MethodGet,
					"/products/money-transfer/start",
					query,
					nil,
				)
				return flowID, runID
			},
		},
		{
			name:                       "products/order-processing",
			serverZDUnsupportedFeature: "failure policies",
			trigger: func(t *testing.T) (string, string) {
				flowID, runID := triggerFlowSmokeHTTP(t, http.MethodGet, "/products/order-processing/start", nil, nil)
				return flowID, runID
			},
		},
		{
			name: "products/subscription",
			trigger: func(t *testing.T) (string, string) {
				flowID, runID := triggerFlowSmokeHTTP(
					t,
					http.MethodGet,
					"/products/subscription/start",
					nil,
					nil,
				)
				return flowID, runID
			},
		},
		{
			name: "products/signup",
			trigger: func(t *testing.T) (string, string) {
				username := smokeWorkflowID(t, "signup")
				query := url.Values{
					"username": {username},
					"email":    {username + "@example.com"},
				}
				flowID, runID := triggerFlowSmokeHTTP(
					t,
					http.MethodGet,
					"/products/signup/submit",
					query,
					nil,
				)
				return flowID, runID
			},
		},
		{
			name: "products/job-post",
			trigger: func(t *testing.T) (string, string) {
				query := url.Values{
					"title":       {"Smoke Test Job"},
					"description": {"Smoke test description"},
				}
				flowID, runID := triggerFlowSmokeHTTP(
					t,
					http.MethodGet,
					"/products/job-post/create",
					query,
					nil,
				)
				return flowID, runID
			},
			flags: flowSmokeFlags{noStartStep: true},
		},
		{
			name:                       "patterns/polling",
			serverZDUnsupportedFeature: "failure policies",
			trigger: func(t *testing.T) (string, string) {
				query := url.Values{"workflowId": {smokeWorkflowID(t, "pattern-polling")}}
				return triggerFlowSmokeHTTP(
					t,
					http.MethodGet,
					"/patterns/polling/start",
					query,
					nil,
				)
			},
		},
		{
			name: "patterns/interruptible",
			trigger: func(t *testing.T) (string, string) {
				query := url.Values{"workflowId": {smokeWorkflowID(t, "interruptible")}}
				return triggerFlowSmokeHTTP(
					t,
					http.MethodGet,
					"/patterns/interruptible/start",
					query,
					nil,
				)
			},
		},
		{
			name: "patterns/reminders",
			trigger: func(t *testing.T) (string, string) {
				_, _, responseBody := triggerFlowSmokeHTTPWithBody(
					t,
					http.MethodGet,
					"/patterns/reminders/start",
					nil,
					nil,
				)
				flowID, runID := parseFlowTriggerResponse(string(responseBody), "")
				return flowID, runID
			},
		},
		{
			name:                       "patterns/entity-store",
			serverZDUnsupportedFeature: "Attribute Store sync",
			trigger: func(t *testing.T) (string, string) {
				userID := smokeWorkflowID(t, "entity-store")
				body := entitystore.UserProfileRequest{
					UserID: userID,
					UserProfile: entitystore.UserProfile{
						DisplayName:    "Smoke Tester",
						Email:          userID + "@example.com",
						MarketingOptIn: true,
						Credits:        120,
						Weight:         59.5,
						LastLoggedIn:   time.Date(2026, 8, 11, 15, 30, 0, 0, time.UTC),
						Metadata: entitystore.UserProfileMetadata{
							Source: "smoke",
							Tags:   []string{"example"},
						},
					},
				}
				flowID, runID := triggerFlowSmokeHTTP(
					t,
					http.MethodPost,
					"/patterns/entity-store/profile",
					nil,
					body,
				)
				if flowID == "" {
					flowID = userID
				}
				return flowID, runID
			},
			flags: flowSmokeFlags{noStartStep: true},
		},
		{
			name: "patterns/sequentially-chunked-attribute-map",
			trigger: func(t *testing.T) (string, string) {
				return triggerFlowSmokeHTTP(
					t,
					http.MethodPost,
					"/patterns/sequentially-chunked-attribute-map/start",
					nil,
					nil,
				)
			},
			flags: flowSmokeFlags{noStartStep: true},
		},
		{
			name: "patterns/hash-partitioned-attribute-map",
			trigger: func(t *testing.T) (string, string) {
				return triggerFlowSmokeHTTP(
					t,
					http.MethodPost,
					"/patterns/hash-partitioned-attribute-map/start",
					nil,
					nil,
				)
			},
			flags: flowSmokeFlags{noStartStep: true},
		},
		{
			name:                       "patterns/manual-recovery",
			serverZDUnsupportedFeature: "failure policies",
			trigger: func(t *testing.T) (string, string) {
				query := url.Values{"workflowId": {smokeWorkflowID(t, "manual-recovery")}}
				return triggerFlowSmokeHTTP(
					t,
					http.MethodGet,
					"/patterns/manual-recovery/start",
					query,
					nil,
				)
			},
		},
		{
			name: "patterns/inactiveness-tracker-timer",
			trigger: func(t *testing.T) (string, string) {
				query := url.Values{"workflowId": {smokeWorkflowID(t, "inactiveness-tracker-timer")}}
				return triggerFlowSmokeHTTP(
					t,
					http.MethodGet,
					"/patterns/inactiveness-tracker-timer/start",
					query,
					nil,
				)
			},
		},
		{
			name: "patterns/parallel/static",
			trigger: func(t *testing.T) (string, string) {
				query := url.Values{"workflowId": {smokeWorkflowID(t, "parallel-static")}}
				return triggerFlowSmokeHTTP(
					t,
					http.MethodGet,
					"/patterns/parallel/start/static",
					query,
					nil,
				)
			},
		},
		{
			name: "patterns/parallel/dynamic",
			trigger: func(t *testing.T) (string, string) {
				query := url.Values{"workflowId": {smokeWorkflowID(t, "parallel-dynamic")}}
				return triggerFlowSmokeHTTP(
					t,
					http.MethodGet,
					"/patterns/parallel/start/dynamic",
					query,
					nil,
				)
			},
		},
		parallelSmokeEntry("await"),
		parallelSmokeEntry("first-win"),
		{
			name:                       "patterns/recovery",
			serverZDUnsupportedFeature: "failure policies",
			trigger: func(t *testing.T) (string, string) {
				query := url.Values{
					"workflowId": {smokeWorkflowID(t, "recovery")},
					"itemName":   {"smoke-item"},
					"quantity":   {"2"},
				}
				return triggerFlowSmokeHTTP(t, http.MethodGet, "/patterns/recovery/start", query, nil)
			},
			flags: flowSmokeFlags{stepStartMayFail: true},
		},
		parallelSubFlowsSmokeEntry("basic"),
		parallelSubFlowsSmokeEntry("wait-for-half"),
		parallelSubFlowsSmokeEntry("long-lived-parent"),
		parallelSubFlowsSmokeEntry("short-lived-parent"),
		{
			name: "patterns/drain-channels/internal",
			trigger: func(t *testing.T) (string, string) {
				query := url.Values{"workflowId": {smokeWorkflowID(t, "drain-internal")}}
				return triggerFlowSmokeHTTP(
					t,
					http.MethodGet,
					"/patterns/drain-channels/internal/start",
					query,
					nil,
				)
			},
		},
		{
			name: "patterns/drain-channels/external-publishing",
			trigger: func(t *testing.T) (string, string) {
				query := url.Values{"workflowId": {smokeWorkflowID(t, "drain-external")}}
				return triggerFlowSmokeHTTP(
					t,
					http.MethodGet,
					"/patterns/drain-channels/external-publishing/start-or-publish",
					query,
					nil,
				)
			},
		},
		{
			name: "patterns/wait-for-step-completion",
			trigger: func(t *testing.T) (string, string) {
				query := url.Values{"workflowId": {smokeWorkflowID(t, "wait-for-state")}}
				return triggerFlowSmokeHTTP(
					t,
					http.MethodGet,
					"/patterns/wait-for-step-completion/start",
					query,
					nil,
				)
			},
		},
		{
			name: "patterns/timeout",
			trigger: func(t *testing.T) (string, string) {
				query := url.Values{
					"workflowId":         {smokeWorkflowID(t, "timeout")},
					"successfulWorkflow": {"true"},
				}
				return triggerFlowSmokeHTTP(t, http.MethodGet, "/patterns/timeout/start", query, nil)
			},
		},
		{
			name: "primitives/step",
			trigger: func(t *testing.T) (string, string) {
				query := url.Values{
					"workflowId": {smokeWorkflowID(t, "primitive-step")},
					"inputNum":   {"1"},
				}
				return triggerFlowSmokeHTTP(t, http.MethodGet, "/primitives/step/start", query, nil)
			},
		},
		{
			name: "primitives/step/retry",
			trigger: func(t *testing.T) (string, string) {
				query := url.Values{
					"workflowId":        {smokeWorkflowID(t, "primitive-step-retry")},
					"readyAfterAttempt": {"2"},
				}
				return triggerFlowSmokeHTTP(t, http.MethodGet, "/primitives/step/retry/start", query, nil)
			},
		},
		{
			name: "primitives/step/custom-retry",
			trigger: func(t *testing.T) (string, string) {
				query := url.Values{
					"workflowId":        {smokeWorkflowID(t, "primitive-step-custom-retry")},
					"readyAfterAttempt": {"1"},
				}
				return triggerFlowSmokeHTTP(t, http.MethodGet, "/primitives/step/custom-retry/start", query, nil)
			},
		},
		{
			name: "primitives/step/durability",
			trigger: func(t *testing.T) (string, string) {
				query := url.Values{
					"workflowId": {smokeWorkflowID(t, "primitive-step-durability")},
					"mode":       {"sync"},
				}
				return triggerFlowSmokeHTTP(t, http.MethodGet, "/primitives/step/durability/start", query, nil)
			},
		},
		{
			name: "primitives/step/heartbeat",
			trigger: func(t *testing.T) (string, string) {
				query := url.Values{
					"workflowId": {smokeWorkflowID(t, "primitive-step-heartbeat")},
					"batches":    {"0"},
				}
				return triggerFlowSmokeHTTP(t, http.MethodGet, "/primitives/step/heartbeat/start", query, nil)
			},
		},
		{
			name:                       "primitives/step/options-override",
			serverZDUnsupportedFeature: "failure policies",
			trigger: func(t *testing.T) (string, string) {
				query := url.Values{
					"workflowId": {smokeWorkflowID(t, "primitive-step-options-override")},
					"input":      {"smoke"},
				}
				return triggerFlowSmokeHTTP(t, http.MethodGet, "/primitives/step/options-override/start", query, nil)
			},
		},
		{
			name: "primitives/step/step-decision",
			trigger: func(t *testing.T) (string, string) {
				query := url.Values{
					"workflowId": {smokeWorkflowID(t, "primitive-step-decision")},
					"mode":       {"graceful"},
				}
				return triggerFlowSmokeHTTP(t, http.MethodGet, "/primitives/step/step-decision/start", query, nil)
			},
		},
		{
			name: "primitives/step/wait-types",
			trigger: func(t *testing.T) (string, string) {
				query := url.Values{
					"workflowId":     {smokeWorkflowID(t, "primitive-step-wait-types")},
					"mode":           {"any"},
					"timeoutSeconds": {"1"},
				}
				return triggerFlowSmokeHTTP(t, http.MethodGet, "/primitives/step/wait-types/start", query, nil)
			},
		},
		{
			name: "primitives/attribute",
			trigger: func(t *testing.T) (string, string) {
				query := url.Values{
					"workflowId": {smokeWorkflowID(t, "primitive-attribute")},
					"message":    {"smoke"},
				}
				return triggerFlowSmokeHTTP(t, http.MethodGet, "/primitives/attribute/start", query, nil)
			},
		},
		{
			name: "primitives/channel",
			trigger: func(t *testing.T) (string, string) {
				query := url.Values{
					"workflowId": {smokeWorkflowID(t, "primitive-channel")},
					"inputNum":   {"1"},
				}
				return triggerFlowSmokeHTTP(t, http.MethodGet, "/primitives/channel/start", query, nil)
			},
		},
		{
			name: "primitives/stream",
			trigger: func(t *testing.T) (string, string) {
				query := url.Values{
					"workflowId": {smokeWorkflowID(t, "primitive-stream")},
					"input":      {"smoke"},
				}
				return triggerFlowSmokeHTTP(t, http.MethodGet, "/primitives/stream/start", query, nil)
			},
		},
		{
			name: "primitives/timer",
			trigger: func(t *testing.T) (string, string) {
				query := url.Values{
					"workflowId": {smokeWorkflowID(t, "primitive-timer")},
					"seconds":    {"1"},
				}
				return triggerFlowSmokeHTTP(t, http.MethodGet, "/primitives/timer/start", query, nil)
			},
		},
		{
			name: "primitives/rpc",
			trigger: func(t *testing.T) (string, string) {
				query := url.Values{"workflowId": {smokeWorkflowID(t, "primitive-rpc")}}
				return triggerFlowSmokeHTTP(t, http.MethodGet, "/primitives/rpc/start", query, nil)
			},
		},
		{
			name: "primitives/subflow",
			trigger: func(t *testing.T) (string, string) {
				query := url.Values{
					"workflowId": {smokeWorkflowID(t, "primitive-subflow")},
					"inputNum":   {"1"},
				}
				return triggerFlowSmokeHTTP(t, http.MethodGet, "/primitives/subflow/start", query, nil)
			},
		},
		{
			name: "primitives/client-apis",
			trigger: func(t *testing.T) (string, string) {
				query := url.Values{
					"workflowId": {smokeWorkflowID(t, "primitive-client-apis")},
					"keyword":    {"smoke"},
				}
				return triggerFlowSmokeHTTP(t, http.MethodGet, "/primitives/client-apis/start", query, nil)
			},
		},
	}
}

func parallelSmokeEntry(kind string) flowSmokeEntry {
	return flowSmokeEntry{
		name: "patterns/parallel/" + kind,
		trigger: func(t *testing.T) (string, string) {
			query := url.Values{"workflowId": {smokeWorkflowID(t, "parallel-"+kind)}}
			return triggerFlowSmokeHTTP(
				t,
				http.MethodGet,
				"/patterns/parallel/start/"+kind,
				query,
				nil,
			)
		},
	}
}

func parallelSubFlowsSmokeEntry(kind string) flowSmokeEntry {
	return flowSmokeEntry{
		name: "patterns/parallel-subflows/" + kind,
		trigger: func(t *testing.T) (string, string) {
			query := url.Values{"workflowId": {smokeWorkflowID(t, "parallel-subflows-"+kind)}}
			return triggerFlowSmokeHTTP(
				t,
				http.MethodGet,
				"/patterns/parallel-subflows/start/"+kind,
				query,
				nil,
			)
		},
	}
}

func TestFlowSmokeCatalogSize(t *testing.T) {
	catalog := flowSmokeCatalog()
	if len(catalog) == 0 {
		t.Fatal("flow smoke catalog is empty")
	}
	t.Logf("flow smoke catalog size: %d", len(catalog))
}

func TestFlowSmokeAllRegisteredFlowsViaController(t *testing.T) {
	for _, entry := range flowSmokeCatalog() {
		entry := entry
		t.Run(entry.name, func(t *testing.T) {
			if entry.serverZDUnsupportedFeature != "" {
				skipWhenServerZD(t, entry.serverZDUnsupportedFeature)
			}
			flowID, runID := entry.trigger(t)
			requireFlowSmokeIDs(t, entry.name, flowID)
			assertFlowSmokeStartStep(t, entry, flowID, runID)
			assertFlowSmokeNoUnexpectedFailures(t, entry, flowID, runID)
		})
	}
}

func requireFlowSmokeIDs(t *testing.T, name string, flowID string) {
	t.Helper()
	if flowID == "" {
		t.Fatalf("%s: controller response did not include flowID", name)
	}
}
