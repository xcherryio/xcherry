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
	"net"
	"net/http"
	"sort"
	"testing"
	"time"

	clientprom "github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/superdurable/dex/gen/dexpb"
	"github.com/superdurable/dex/integ/workflow/basic"
	"github.com/superdurable/dex/integ/workflow/rpc"
	"github.com/superdurable/dex/service"
	"github.com/uber-go/tally/v4"
	tallyprom "github.com/uber-go/tally/v4/prometheus"
	"go.temporal.io/sdk/client"
	sdktally "go.temporal.io/sdk/contrib/tally"
)

type prometheusMetricExpectation struct {
	name            string
	labels          map[string]string
	forbiddenLabels []string
}

func TestTemporalMetrics(t *testing.T) {
	if !*temporalIntegTest {
		t.Skip()
	}
	if *dexServerAddress != "" {
		t.Skip("metrics handler injection requires an in-process Dex server")
	}
	// C10: server-zd does not emit Temporal mechanism metrics.
	requireGoServerSetting(t, "Temporal metrics handler")

	metricsHandler, metricsURL := startPrometheusMetricsEndpoint(t)
	configureMetrics := func(testConfig *DexServiceTestConfig) {
		testConfig.TemporalMetricsHandler = metricsHandler
	}

	t.Run("sync-step", func(t *testing.T) {
		doTestBasicFlow(t, service.BackendTypeTemporal, syncDurabilityConfig(), configureMetrics)
	})
	t.Run("async-step-and-continue-as-new", func(t *testing.T) {
		doTestBasicFlow(
			t,
			service.BackendTypeTemporal,
			minimumContinueAsNewAsyncDurabilityConfig(),
			configureMetrics,
		)
	})
	t.Run("rpc", func(t *testing.T) {
		doTestRpcWorkflow(t, service.BackendTypeTemporal, nil, func(testConfig *DexServiceTestConfig) {
			configureMetrics(testConfig)
			testConfig.UseTemporalSynchronousUpdateForAllRPCs = true
		})
	})
	t.Run("sub-flow", func(t *testing.T) {
		doTestSubFlowCondition(
			t,
			service.BackendTypeTemporal,
			serviceDefaultSubFlowReusePolicy(),
			false,
			configureMetrics,
		)
	})
	t.Run("system-workflow", func(t *testing.T) {
		runtime := startDexService(t, DexServiceTestConfig{
			BackendType:            service.BackendTypeTemporal,
			TemporalMetricsHandler: metricsHandler,
		})
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		workflowID := "metrics-cleanup-" + newRequestID()
		require.NoError(t, runtime.UnifiedClient.StartBlobStoreCleanupWorkflow(
			ctx,
			service.TaskQueue,
			workflowID,
			"",
			"local-store-id",
		))
		_, err := runtime.UnifiedClient.GetWorkflowResult(ctx, nil, workflowID, "")
		require.NoError(t, err)
	})

	expectations := []prometheusMetricExpectation{
		{name: "dex_flow_completed", labels: map[string]string{"flow_type": basic.FlowType}},
		{name: "dex_sync_step_execution_latency", labels: map[string]string{
			"flow_type": basic.FlowType,
			"step_type": basic.Step1,
		}},
		{name: "dex_async_step_execution_latency", labels: map[string]string{
			"flow_type": basic.FlowType,
			"step_type": basic.Step1,
		}},
		{name: "dex_async_invoke_rpc_execution_latency", labels: map[string]string{
			"flow_type": rpc.WorkflowType,
			"rpc_name":  rpc.RPCName,
		}},
		{name: "dex_async_start_sub_flow_execution_latency", labels: map[string]string{
			"flow_type":     subFlowParentType,
			"sub_flow_type": subFlowChildType,
		}},
		{name: "dex_async_sys_step_execution_latency", labels: map[string]string{
			"activity_type": "DumpFlowForContinueAsNew",
			"flow_type":     basic.FlowType,
		}},
		{name: "dex_async_sys_step_execution_latency", labels: map[string]string{
			"activity_type": "ReportSubFlowCompletion",
			"flow_type":     subFlowChildType,
		}},
		{name: "dex_sync_sys_step_execution_latency", labels: map[string]string{
			"activity_type": "CleanupBlobsAfterAllRunsDeleted",
			"flow_type":     "none",
		}},
		{name: "dex_sys_flow_completed", forbiddenLabels: []string{"flow_type"}},
	}

	var lastFamilies map[string]*dto.MetricFamily
	matched := assert.Eventually(t, func() bool {
		families, err := readPrometheusMetricFamilies(metricsURL)
		if err != nil {
			return false
		}
		lastFamilies = families
		for _, expectation := range expectations {
			if !prometheusMetricMatches(families, expectation) {
				return false
			}
		}
		return true
	}, 10*time.Second, 100*time.Millisecond)
	if !matched {
		t.Logf("Prometheus metric families: %v", sortedMetricFamilyNames(lastFamilies))
		for _, expectation := range expectations {
			if family := lastFamilies[expectation.name]; family != nil {
				t.Logf("%s labels: %v", expectation.name, prometheusMetricLabelSets(family))
			}
		}
		return
	}

	for name := range lastFamilies {
		require.NotContains(t, name, "temporal_samples")
		require.False(t, len(name) >= len("temporal_") && name[:len("temporal_")] == "temporal_", name)
	}
}

func serviceDefaultSubFlowReusePolicy() dexpb.SubFlowReusePolicy {
	return dexpb.SubFlowReusePolicy_SUB_FLOW_REUSE_POLICY_RESTART_IF_PREVIOUS_EXITS_ABNORMALLY
}

func startPrometheusMetricsEndpoint(t *testing.T) (client.MetricsHandler, string) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	address := listener.Addr().String()
	require.NoError(t, listener.Close())

	reporter, err := (tallyprom.Configuration{
		ListenAddress: address,
		TimerType:     "histogram",
	}).NewReporter(tallyprom.ConfigurationOptions{Registry: clientprom.NewRegistry()})
	require.NoError(t, err)
	scope, closer := tally.NewRootScope(tally.ScopeOptions{
		CachedReporter: reporter,
		Separator:      tallyprom.DefaultSeparator,
	}, 20*time.Millisecond)
	t.Cleanup(func() { require.NoError(t, closer.Close()) })
	return sdktally.NewMetricsHandler(scope), "http://" + address + "/metrics"
}

func readPrometheusMetricFamilies(metricsURL string) (map[string]*dto.MetricFamily, error) {
	response, err := http.Get(metricsURL)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("metrics endpoint returned %s", response.Status)
	}
	return new(expfmt.TextParser).TextToMetricFamilies(response.Body)
}

func prometheusMetricMatches(
	families map[string]*dto.MetricFamily,
	expectation prometheusMetricExpectation,
) bool {
	family := families[expectation.name]
	if family == nil {
		return false
	}
	for _, metric := range family.Metric {
		labels := make(map[string]string, len(metric.Label))
		for _, label := range metric.Label {
			labels[label.GetName()] = label.GetValue()
		}
		matches := true
		for key, value := range expectation.labels {
			if labels[key] != value {
				matches = false
				break
			}
		}
		for _, forbidden := range expectation.forbiddenLabels {
			if _, ok := labels[forbidden]; ok {
				matches = false
				break
			}
		}
		if matches {
			return true
		}
	}
	return false
}

func sortedMetricFamilyNames(families map[string]*dto.MetricFamily) []string {
	names := make([]string, 0, len(families))
	for name := range families {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func prometheusMetricLabelSets(family *dto.MetricFamily) []map[string]string {
	labelSets := make([]map[string]string, 0, len(family.Metric))
	for _, metric := range family.Metric {
		labels := make(map[string]string, len(metric.Label))
		for _, label := range metric.Label {
			labels[label.GetName()] = label.GetValue()
		}
		labelSets = append(labelSets, labels)
	}
	return labelSets
}
