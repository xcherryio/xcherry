// Legacy Materials in this file remain under their original licenses.
// See LEGACY_NOTICES.md.

// Modifications Copyright (c) 2026 Super Durable, Inc.
//
// Modifications after the Legacy Cutoff are licensed under the
// Sustainable Use License 1.0.
// Legacy Materials remain under their original licenses.
// See LICENSE and LEGACY_NOTICES.md.

package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWebEnvironmentOverridesYAML(t *testing.T) {
	t.Setenv("DEX_WEB_FLOW_RENDERING_DIRECTORY", "/tmp/flow-definitions")
	t.Setenv("DEX_WEB_START_FLOW_WORKER_TARGET_HEADLESS", "true")
	path := writeTestConfig(t, `
web:
  flowRenderingDirectory: /tmp/yaml-definitions
  startFlowWorkerTargetHeadless: false
`)
	cfg, err := NewConfig(path)
	require.NoError(t, err)
	require.Equal(t, "/tmp/flow-definitions", cfg.Web.FlowRenderingDirectory)
	require.True(t, cfg.Web.IsStartFlowWorkerTargetHeadless)
}

func TestRemovedTenancyConfigReportsMigration(t *testing.T) {
	configurations := map[string]string{
		"tenancy section":               "tenancy:\n  mode: multi\n",
		"blob cleanup delay":            "blobStore:\n  tenantCleanupConfirmationDelay: 5s\n",
		"attribute store tenant column": "attributeStore:\n  stores:\n    primary:\n      tenantColumn: tenant_id\n",
	}
	for name, configuration := range configurations {
		t.Run(name, func(t *testing.T) {
			_, err := NewConfig(writeTestConfig(t, configuration))
			require.ErrorContains(t, err, "multi-tenancy was removed")
			require.ErrorContains(t, err, "auth.authenticators")
			require.ErrorContains(t, err, "to internal")
			require.ErrorContains(t, err, "worker.wake")
			require.ErrorContains(t, err, "drop subjectPattern and claims")
			require.ErrorContains(t, err, "only sha256 entries")
		})
	}
}

func TestWebStartFlowWorkerTargetHeadlessDefaultsFalseAndReadsYAML(t *testing.T) {
	defaultConfig, err := NewConfig(writeTestConfig(t, "web: {}\n"))
	require.NoError(t, err)
	require.False(t, defaultConfig.Web.IsStartFlowWorkerTargetHeadless)

	yamlConfig, err := NewConfig(writeTestConfig(t, "web:\n  startFlowWorkerTargetHeadless: true\n"))
	require.NoError(t, err)
	require.True(t, yamlConfig.Web.IsStartFlowWorkerTargetHeadless)
}

func TestWebEnvironmentRejectsInvalidStartFlowHeadlessRouting(t *testing.T) {
	t.Setenv("DEX_WEB_START_FLOW_WORKER_TARGET_HEADLESS", "sometimes")
	_, err := NewConfig(writeTestConfig(t, "web: {}\n"))
	require.ErrorContains(t, err, "DEX_WEB_START_FLOW_WORKER_TARGET_HEADLESS must be a boolean")
}

func TestTemporalCloudOpsConfig(t *testing.T) {
	path := writeTestConfig(t, `
interpreter:
  temporal:
    namespace: test.account
    cloudAPIKey: secret
    cloudOps:
      hostPort: saas-api.tmprl.cloud:443
      apiVersion: v0.19.1
`)
	cfg, err := NewConfig(path)
	require.NoError(t, err)
	require.Equal(t, "saas-api.tmprl.cloud:443", cfg.Interpreter.Temporal.CloudOps.HostPort)
	require.Equal(t, "v0.19.1", cfg.Interpreter.Temporal.CloudOps.APIVersion)
}

func TestTemporalCloudOpsConfigValidation(t *testing.T) {
	testCases := []struct {
		name          string
		temporalYAML  string
		errorContains string
	}{
		{
			name: "API key required",
			temporalYAML: `
    cloudOps:
      hostPort: saas-api.tmprl.cloud:443
      apiVersion: v0.19.1`,
			errorContains: "cloudAPIKey or apiKeyFile is required",
		},
		{
			name: "host and port required",
			temporalYAML: `
    cloudAPIKey: secret
    cloudOps:
      hostPort: saas-api.tmprl.cloud
      apiVersion: v0.19.1`,
			errorContains: "hostPort must be host:port",
		},
		{
			name: "version required",
			temporalYAML: `
    cloudAPIKey: secret
    cloudOps:
      hostPort: saas-api.tmprl.cloud:443`,
			errorContains: "apiVersion must be a version",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			path := writeTestConfig(t, "interpreter:\n  temporal:"+testCase.temporalYAML+"\n")
			_, err := NewConfig(path)
			require.ErrorContains(t, err, testCase.errorContains)
		})
	}
}

func TestTemporalConnectionConfigValidation(t *testing.T) {
	testCases := []struct {
		name          string
		temporalYAML  string
		errorContains string
	}{
		{"both API key sources", "cloudAPIKey: static\n    apiKeyFile: /tmp/key", "cannot both be configured"},
		{"certificate without key", "tls:\n      certFile: /tmp/cert", "must be configured together"},
		{"negative refresh interval", "apiKeyFileRefreshInterval: -1s", "must be non-negative"},
		{"disabled TLS with CA", "tls:\n      enabled: false\n      caFile: /tmp/ca.pem", "enabled cannot be false"},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			path := writeTestConfig(t, "interpreter:\n  temporal:\n    "+testCase.temporalYAML+"\n")
			_, err := NewConfig(path)
			require.ErrorContains(t, err, testCase.errorContains)
		})
	}
}

func TestTemporalCloudOpsAcceptsFileAPIKey(t *testing.T) {
	path := writeTestConfig(t, "interpreter:\n  temporal:\n    apiKeyFile: /tmp/key\n    cloudOps:\n      hostPort: saas-api.tmprl.cloud:443\n      apiVersion: v0.19.1\n")
	_, err := NewConfig(path)
	require.NoError(t, err)
}

func TestTemporalConnectionConfigDecode(t *testing.T) {
	path := writeTestConfig(t, `
interpreter:
  temporal:
    apiKeyFile: /var/run/secrets/dex/temporal-token
    apiKeyFileRefreshInterval: 45s
    tls:
      enabled: true
      caFile: /etc/dex/ca.pem
      serverName: temporal.example
      certFile: /etc/dex/client.crt
      keyFile: /etc/dex/client.key
`)
	cfg, err := NewConfig(path)
	require.NoError(t, err)
	temporalConfig := cfg.Interpreter.Temporal
	require.Equal(t, "/var/run/secrets/dex/temporal-token", temporalConfig.APIKeyFile)
	require.Equal(t, 45*time.Second, temporalConfig.APIKeyFileRefreshInterval)
	require.True(t, *temporalConfig.TLS.Enabled)
	require.Equal(t, "/etc/dex/ca.pem", temporalConfig.TLS.CAFile)
	require.Equal(t, "temporal.example", temporalConfig.TLS.ServerName)
	require.Equal(t, "/etc/dex/client.crt", temporalConfig.TLS.CertFile)
	require.Equal(t, "/etc/dex/client.key", temporalConfig.TLS.KeyFile)
}

func TestConfigAcceptsExternallyManagedIndexesOption(t *testing.T) {
	path := writeTestConfig(t, `
interpreter:
  attributeIndexesManagedExternally: true
`)
	cfg, err := NewConfig(path)
	require.NoError(t, err)
	require.True(t, cfg.Interpreter.AttributeIndexesManagedExternally)
}

func TestRetryPolicyConfigUsesDurations(t *testing.T) {
	path := writeTestConfig(t, `
api:
  queryWorkflowFailedRetryPolicy:
    initialInterval: 125ms
    backoffCoefficient: 1.25
    maximumInterval: 750ms
    maximumAttempts: 7
    totalDuration: 3s
  invokeRPCContinuedAsNewErrorRetryPolicy:
    initialInterval: 100ms
    backoffCoefficient: 2
    maximumInterval: 1s
    totalDuration: 5s
interpreter:
  interpreterActivityConfig:
    dumpWorkflowInternalActivityConfig:
      retryPolicy:
        initialInterval: 250ms
        backoffCoefficient: 1.5
        maximumInterval: 2s
        maximumAttempts: 4
        totalDuration: 10s
    subFlowStartActivityConfig:
      scheduleToCloseTimeout: 15s
      retryPolicy:
        initialInterval: 350ms
        backoffCoefficient: 1.25
        maximumInterval: 3s
        maximumAttempts: 9
        totalDuration: 2m
`)
	cfg, err := NewConfig(path)
	require.NoError(t, err)
	require.Equal(t, 125*time.Millisecond, cfg.Api.QueryWorkflowFailedRetryPolicy.InitialInterval)
	require.Equal(t, 750*time.Millisecond, cfg.Api.QueryWorkflowFailedRetryPolicy.MaximumInterval)
	require.Equal(t, 100*time.Millisecond, cfg.Api.InvokeRPCContinuedAsNewErrorRetryPolicy.InitialInterval)
	require.Equal(t, 5*time.Second, cfg.Api.InvokeRPCContinuedAsNewErrorRetryPolicy.TotalDuration)
	activityPolicy := cfg.Interpreter.InterpreterActivityConfig.DumpWorkflowInternalActivityConfig.RetryPolicy
	require.Equal(t, 250*time.Millisecond, activityPolicy.InitialInterval)
	require.Equal(t, 10*time.Second, activityPolicy.TotalDuration)
	subFlowStartConfig := cfg.Interpreter.InterpreterActivityConfig.EffectiveSubFlowStartActivityConfig()
	require.Equal(t, 15*time.Second, subFlowStartConfig.ScheduleToCloseTimeout)
	require.Equal(t, 350*time.Millisecond, subFlowStartConfig.RetryPolicy.InitialInterval)
	require.Equal(t, int32(9), subFlowStartConfig.RetryPolicy.MaximumAttempts)
	require.Equal(t, 2*time.Minute, subFlowStartConfig.RetryPolicy.TotalDuration)
}

func TestSubFlowStartActivityConfigDefaultsToLocalRetryWindow(t *testing.T) {
	activityConfig := (InterpreterActivityConfig{}).EffectiveSubFlowStartActivityConfig()

	require.Equal(t, 2*time.Minute, activityConfig.ScheduleToCloseTimeout)
	require.Equal(t, 100*time.Millisecond, activityConfig.RetryPolicy.InitialInterval)
	require.Equal(t, 2.0, activityConfig.RetryPolicy.BackoffCoefficient)
	require.Equal(t, time.Minute, activityConfig.RetryPolicy.MaximumInterval)
	require.Zero(t, activityConfig.RetryPolicy.MaximumAttempts)
	require.Zero(t, activityConfig.RetryPolicy.TotalDuration)
}

func TestRetryPolicyConfigRejectsInvalidDuration(t *testing.T) {
	path := writeTestConfig(t, `
api:
  invokeRPCContinuedAsNewErrorRetryPolicy:
    initialInterval: 2s
    maximumInterval: 100ms
`)
	_, err := NewConfig(path)
	require.ErrorContains(t, err, "maximumInterval must not be less than initialInterval")
}

func TestSubFlowStartActivityConfigRejectsInvalidRetryPolicy(t *testing.T) {
	path := writeTestConfig(t, `
interpreter:
  interpreterActivityConfig:
    subFlowStartActivityConfig:
      retryPolicy:
        initialInterval: 2s
        maximumInterval: 100ms
`)

	_, err := NewConfig(path)

	require.ErrorContains(t, err, "maximumInterval must not be less than initialInterval")
}

func TestSubFlowStartActivityConfigRejectsNegativeRetryWindow(t *testing.T) {
	path := writeTestConfig(t, `
interpreter:
  interpreterActivityConfig:
    subFlowStartActivityConfig:
      scheduleToCloseTimeout: -1s
`)

	_, err := NewConfig(path)

	require.ErrorContains(t, err, "scheduleToCloseTimeout must be non-negative")
}

func TestMinimumStepHeartbeatTimeoutConfig(t *testing.T) {
	require.Equal(t, 10*time.Second,
		(InterpreterActivityConfig{}).EffectiveMinimumStepHeartbeatTimeout())

	path := writeTestConfig(t, `
interpreter:
  interpreterActivityConfig:
    minimumStepHeartbeatTimeout: 2s
`)
	cfg, err := NewConfig(path)
	require.NoError(t, err)
	require.Equal(t, 2*time.Second,
		cfg.Interpreter.InterpreterActivityConfig.EffectiveMinimumStepHeartbeatTimeout())

	path = writeTestConfig(t, `
interpreter:
  interpreterActivityConfig:
    minimumStepHeartbeatTimeout: -1s
`)
	_, err = NewConfig(path)
	require.ErrorContains(t, err, "minimumStepHeartbeatTimeout must be non-negative")
}

func TestCleanupStrategyCronSchedule(t *testing.T) {
	testCases := []struct {
		name             string
		strategy         CleanupStrategy
		expectedSchedule string
		expectError      bool
	}{
		{
			name: "default strategy disabled",
		},
		{
			name: "default strategy daily",
			strategy: CleanupStrategy{
				CleanupFrequencyInDays: 1,
			},
			expectedSchedule: "0 0 * * *",
		},
		{
			name: "explicit strategy every three days",
			strategy: CleanupStrategy{
				CleanupStrategyType:    CleanupStrategyTypeAfterAllRunsDeleted,
				CleanupFrequencyInDays: 3,
			},
			expectedSchedule: "0 0 */3 * *",
		},
		{
			name: "unsupported strategy",
			strategy: CleanupStrategy{
				CleanupStrategyType: "unsupported",
			},
			expectError: true,
		},
		{
			name: "negative frequency",
			strategy: CleanupStrategy{
				CleanupFrequencyInDays: -1,
			},
			expectError: true,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			schedule, err := testCase.strategy.CronSchedule()
			if testCase.expectError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, testCase.expectedSchedule, schedule)
		})
	}
}
