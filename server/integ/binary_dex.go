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
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/superdurable/dex/config"
	"github.com/superdurable/dex/gen/dexpb"
	"github.com/superdurable/dex/service"
	uclient "github.com/superdurable/dex/service/client"
	"github.com/superdurable/dex/service/common/ptr"
	"google.golang.org/grpc/health/grpc_health_v1"
	"gopkg.in/yaml.v3"
)

type binaryDexProcess struct {
	command *exec.Cmd
	done    chan struct{}
	waitErr error
}

type temporalOnlyUnifiedClient struct {
	uclient.UnifiedClient
	test *testing.T
}

func startBinaryDexService(t *testing.T, testConfig DexServiceTestConfig) *integRuntime {
	t.Helper()
	require.Equal(t, "zd", *dexServerEngine, "dex-server-engine must be zd in binary mode")
	if testConfig.BackendType != service.BackendTypeTemporal {
		t.Skip("server-zd does not support Cadence")
	}
	require.Equal(t, "pull", *integWorkerMode, "binary mode requires pull Workers")
	configuration, unsupportedSetting := binaryDexConfig(testConfig)
	if unsupportedSetting != "" {
		t.Skipf("server-zd does not support %s", unsupportedSetting)
	}
	dataDirectory := t.TempDir()
	address, err := reserveBinaryDexAddress()
	require.NoError(t, err)
	arguments := []string{"dev", "--data-dir", dataDirectory, "--listen", address}
	if len(configuration) > 0 {
		configurationPath := filepath.Join(dataDirectory, "integ-config.yaml")
		configurationBytes, marshalErr := yaml.Marshal(configuration)
		require.NoError(t, marshalErr)
		require.NoError(t, os.WriteFile(configurationPath, configurationBytes, 0o600))
		arguments = append(arguments, "--config", configurationPath)
	}
	outputPath := filepath.Join(dataDirectory, "server.log")
	outputFile, err := os.Create(outputPath)
	require.NoError(t, err)
	command := exec.Command(*dexServerBinary, arguments...)
	command.Stdout = outputFile
	command.Stderr = outputFile
	require.NoError(t, command.Start())
	process := &binaryDexProcess{command: command, done: make(chan struct{})}
	go func() {
		process.waitErr = command.Wait()
		close(process.done)
	}()
	t.Cleanup(func() {
		process.stop(t)
		require.NoError(t, outputFile.Close())
	})
	require.NoError(t, waitForBinaryDex(address, process, 15*time.Second), "server log: %s", outputPath)
	connection, err := newDexClientConnection(address, config.DefaultGrpcMaxMessageBytes)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, connection.Close()) })
	runtime := &integRuntime{
		PublicAddress: address,
		FlowClient: &externalDexFlowClient{
			FlowServiceClient: dexpb.NewFlowServiceClient(connection),
			maxWaitSeconds:    int32(12),
		},
		defaultFlowConfig: &dexpb.FlowConfig{StepDurability: ptr.Any(dexpb.StepDurability_STEP_DURABILITY_SYNC)},
		UnifiedClient:     &temporalOnlyUnifiedClient{test: t},
	}
	if testConfig.MaxWaitSeconds > 0 {
		runtime.FlowClient.(*externalDexFlowClient).maxWaitSeconds = int32(testConfig.MaxWaitSeconds)
	}
	return configurePullWorkerClient(t, runtime)
}

func binaryDexConfig(testConfig DexServiceTestConfig) (map[string]any, string) {
	if testConfig.CadenceDomain != "" || testConfig.BackendType == service.BackendTypeCadence {
		return nil, "Cadence"
	}
	if testConfig.MemoEncryption {
		return nil, "memo encryption"
	}
	if len(testConfig.DefaultHeaders) > 0 {
		return nil, "default Worker headers"
	}
	if testConfig.S3TestThreshold > 0 {
		return nil, "S3 Blob Store"
	}
	if testConfig.S3KeyPrefix != "" {
		return nil, "S3 key prefix"
	}
	if testConfig.BlobCacheDirectory != "" {
		return nil, "Blob cache directory"
	}
	if testConfig.BlobStoreEnabled != nil && !*testConfig.BlobStoreEnabled {
		return nil, "disabled Blob Store"
	}
	if testConfig.AsyncStepInputSnapshotsEnabled != nil {
		return nil, "async Step input snapshots"
	}
	if testConfig.UseTemporalSynchronousUpdateForAllRPCs {
		return nil, "Temporal synchronous RPC updates"
	}
	if testConfig.TemporalMetricsHandler != nil {
		return nil, "Temporal metrics handler"
	}
	if len(testConfig.AttributeStore.Stores) > 0 || testConfig.AttributeStore.SchemaSyncInterval != 0 ||
		testConfig.AttributeStore.SyncBatchSize != 0 || testConfig.AttributeStore.SyncAttemptTimeout != 0 ||
		testConfig.AttributeStore.SyncRetryPolicy != nil {
		return nil, "Attribute Store"
	}
	if testConfig.StreamStore.Backend != "" && testConfig.StreamStore.Backend != config.StreamStoreBackendMemory {
		return nil, "external Stream Store"
	}
	if testConfig.Auth != nil {
		return nil, "API authentication"
	}
	if testConfig.Internal != nil {
		return nil, "internal listener"
	}
	if testConfig.Wake != nil {
		return nil, "Worker wake hook"
	}
	if testConfig.Credential != "" {
		return nil, "API credential"
	}
	if testConfig.UseBuiltInDefaultWorkflowConfig {
		return nil, "built-in default Flow configuration"
	}
	if *disableStickyCache {
		return nil, "disabled sticky cache"
	}
	configuration := make(map[string]any)
	apiConfig := make(map[string]any)
	if testConfig.MaxWaitSeconds > 0 {
		apiConfig["maxWaitSeconds"] = testConfig.MaxWaitSeconds
	}
	if testConfig.GrpcMaxMessageBytes > 0 {
		apiConfig["grpcMaxMessageBytes"] = testConfig.GrpcMaxMessageBytes
	}
	if len(apiConfig) > 0 {
		configuration["api"] = apiConfig
	}
	if testConfig.LocalBlobThreshold > 0 {
		configuration["values"] = map[string]any{"wireInlineThresholdBytes": testConfig.LocalBlobThreshold}
	}
	if testConfig.LazyLoading != nil {
		valuesConfig, ok := configuration["values"].(map[string]any)
		if !ok {
			valuesConfig = make(map[string]any)
			configuration["values"] = valuesConfig
		}
		valuesConfig["hydrateBeforeDispatch"] = !*testConfig.LazyLoading
	}
	if testConfig.Matching != nil {
		matchingConfig := binaryMatchingConfig(testConfig.Matching)
		if len(matchingConfig) > 0 {
			configuration["matching"] = matchingConfig
		}
	}
	if streamStoreConfig := binaryStreamStoreConfig(testConfig.StreamStore); len(streamStoreConfig) > 0 {
		configuration["streamStore"] = streamStoreConfig
	}
	return configuration, ""
}

func binaryMatchingConfig(settings *config.MatchingConfig) map[string]any {
	configuration := make(map[string]any)
	for name, duration := range map[string]time.Duration{
		"dispatchWait": settings.DispatchWait, "stickyWait": settings.StickyWait,
		"localDispatchWait": settings.LocalDispatchWait, "localStickyWait": settings.LocalStickyWait,
		"localExecutionReserve": settings.LocalExecutionReserve, "pingInterval": settings.PingInterval,
	} {
		if duration != 0 {
			configuration[name] = duration.String()
		}
	}
	for name, value := range map[string]int{
		"maxConcurrentTasksCap": settings.MaxConcurrentTasksCap, "maxStreams": settings.MaxStreams,
		"maxQueueLength": settings.MaxQueueLength, "maxFlowTypes": settings.MaxFlowTypes,
		"maxFlowTypeLength": settings.MaxFlowTypeLength,
	} {
		if value != 0 {
			configuration[name] = value
		}
	}
	return configuration
}

func binaryStreamStoreConfig(settings config.StreamStoreConfig) map[string]any {
	configuration := make(map[string]any)
	if settings.Backend != "" {
		configuration["backend"] = settings.Backend
	}
	for name, value := range map[string]int64{
		"maxMessageBytes":               settings.MaxMessageBytes,
		"estimatedMessageOverheadBytes": settings.EstimatedMessageOverheadBytes,
	} {
		if value != 0 {
			configuration[name] = value
		}
	}
	for name, value := range map[string]int32{
		"maxReadMessages":    settings.MaxReadMessages,
		"trimTriggerPercent": settings.TrimTriggerPercent,
		"trimTargetPercent":  settings.TrimTargetPercent,
	} {
		if value != 0 {
			configuration[name] = value
		}
	}
	if settings.BackgroundTrimBatchSize != 0 {
		configuration["backgroundTrimBatchSize"] = settings.BackgroundTrimBatchSize
	}
	if settings.TrimWorkers != 0 {
		configuration["trimWorkers"] = settings.TrimWorkers
	}
	for name, duration := range map[string]time.Duration{
		"trimLeaseTTL": settings.TrimLeaseTTL, "trimLeaseRetry": settings.TrimLeaseRetry,
		"trimBatchYieldTime": settings.TrimBatchYieldTime,
	} {
		if duration != 0 {
			configuration[name] = duration.String()
		}
	}
	return configuration
}

func reserveBinaryDexAddress() (string, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	address := listener.Addr().String()
	return address, listener.Close()
}

func waitForBinaryDex(address string, process *binaryDexProcess, timeout time.Duration) (waitErr error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	connection, err := newDexClientConnection(address, config.DefaultGrpcMaxMessageBytes)
	if err != nil {
		return err
	}
	defer func() { waitErr = errors.Join(waitErr, connection.Close()) }()
	client := grpc_health_v1.NewHealthClient(connection)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	var lastErr error
	for {
		checkCtx, cancelCheck := context.WithTimeout(ctx, time.Second)
		response, checkErr := client.Check(checkCtx, &grpc_health_v1.HealthCheckRequest{Service: ""})
		cancelCheck()
		if checkErr == nil && response.GetStatus() == grpc_health_v1.HealthCheckResponse_SERVING {
			return nil
		}
		if checkErr != nil {
			lastErr = checkErr
		} else {
			lastErr = fmt.Errorf("health status is %s", response.GetStatus())
		}
		select {
		case <-process.done:
			return fmt.Errorf("Dex Server exited before serving: %w", process.waitErr)
		case <-ctx.Done():
			return fmt.Errorf("Dex Server readiness timed out: %w", errors.Join(ctx.Err(), lastErr))
		case <-ticker.C:
		}
	}
}

func (process *binaryDexProcess) stop(t *testing.T) {
	t.Helper()
	select {
	case <-process.done:
		return
	default:
	}
	if err := process.command.Process.Signal(syscall.SIGTERM); err != nil && !errors.Is(err, os.ErrProcessDone) {
		t.Errorf("terminate Dex Server: %v", err)
	}
	select {
	case <-process.done:
	case <-time.After(10 * time.Second):
		if err := process.command.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
			t.Errorf("kill Dex Server: %v", err)
		}
		<-process.done
	}
}

func requireGoServerSetting(t *testing.T, setting string) {
	t.Helper()
	if *dexServerBinary != "" {
		t.Skipf("server-zd does not support %s", setting)
	}
}

func requireTemporalOnly(t *testing.T) {
	t.Helper()
	if *dexServerBinary != "" {
		t.Skip("Temporal-only assertion")
	}
}

func (client *temporalOnlyUnifiedClient) QueryWorkflow(context.Context, interface{}, string, string, string, ...interface{}) error {
	requireTemporalOnly(client.test)
	return nil
}

func (client *temporalOnlyUnifiedClient) DescribeWorkflowExecution(context.Context, string, string, map[string]dexpb.IndexType) (*uclient.DescribeWorkflowExecutionResponse, error) {
	requireTemporalOnly(client.test)
	return nil, nil
}

func (client *temporalOnlyUnifiedClient) GetWorkflowHistory(context.Context, *uclient.GetWorkflowHistoryRequest) (*uclient.WorkflowHistory, error) {
	requireTemporalOnly(client.test)
	return nil, nil
}

func (client *temporalOnlyUnifiedClient) GetBackendType() service.BackendType {
	requireTemporalOnly(client.test)
	return ""
}

func (client *temporalOnlyUnifiedClient) GetApiService() interface{} {
	requireTemporalOnly(client.test)
	return nil
}

func (client *temporalOnlyUnifiedClient) StartBlobStoreCleanupWorkflow(context.Context, string, string, string, string) error {
	requireTemporalOnly(client.test)
	return nil
}

func (client *temporalOnlyUnifiedClient) GetWorkflowResult(context.Context, interface{}, string, string) (dexpb.FlowStatus, error) {
	requireTemporalOnly(client.test)
	return dexpb.FlowStatus_FLOW_STATUS_UNSPECIFIED, nil
}

func (client *temporalOnlyUnifiedClient) StartInterpreterWorkflow(context.Context, uclient.StartWorkflowOptions, ...interface{}) (string, error) {
	requireTemporalOnly(client.test)
	return "", nil
}

func (client *temporalOnlyUnifiedClient) SignalWorkflow(context.Context, string, string, string, interface{}) error {
	requireTemporalOnly(client.test)
	return nil
}

func (client *temporalOnlyUnifiedClient) TerminateWorkflow(context.Context, string, string, string) error {
	requireTemporalOnly(client.test)
	return nil
}

func (client *temporalOnlyUnifiedClient) SynchronousUpdateWorkflow(context.Context, interface{}, string, string, string, string, interface{}) error {
	requireTemporalOnly(client.test)
	return nil
}

func (client *temporalOnlyUnifiedClient) ListAttributeIndexes(context.Context) (map[string]dexpb.IndexType, error) {
	requireTemporalOnly(client.test)
	return nil, nil
}

func (client *temporalOnlyUnifiedClient) NormalizeAttributeIndexType(dexpb.IndexType) dexpb.IndexType {
	requireTemporalOnly(client.test)
	return dexpb.IndexType_INDEX_TYPE_UNSPECIFIED
}
