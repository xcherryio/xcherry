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
	"net"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/superdurable/dex/config"
	"github.com/superdurable/dex/gen/dexpb"
	"github.com/superdurable/dex/service"
	"github.com/superdurable/dex/service/common/ptr"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func TestBinaryDexLauncher(t *testing.T) {
	executable, err := os.Executable()
	require.NoError(t, err)
	previousBinary, previousEngine, previousMode := *dexServerBinary, *dexServerEngine, *integWorkerMode
	*dexServerBinary, *dexServerEngine, *integWorkerMode = executable, "zd", "pull"
	t.Cleanup(func() {
		*dexServerBinary, *dexServerEngine, *integWorkerMode = previousBinary, previousEngine, previousMode
	})
	t.Setenv("DEX_INTEG_FAKE_BINARY", "1")
	receiptPath := t.TempDir() + "/arguments"
	t.Setenv("DEX_INTEG_FAKE_RECEIPT", receiptPath)
	runtime := startBinaryDexService(t, DexServiceTestConfig{
		BackendType: service.BackendTypeTemporal, MaxWaitSeconds: 7, GrpcMaxMessageBytes: 8192,
	})
	require.NotEmpty(t, runtime.PublicAddress)
	receiptBytes, err := os.ReadFile(receiptPath)
	require.NoError(t, err)
	arguments := strings.Split(string(receiptBytes), "\n")
	require.Equal(t, "dev", arguments[0])
	require.Contains(t, arguments, "--data-dir")
	require.Contains(t, arguments, "--listen")
	configIndex := -1
	for index, argument := range arguments {
		if argument == "--config" {
			configIndex = index
		}
	}
	require.Greater(t, configIndex, 0)
	configurationBytes, err := os.ReadFile(arguments[configIndex+1])
	require.NoError(t, err)
	require.Contains(t, string(configurationBytes), "maxWaitSeconds: 7")
	require.Contains(t, string(configurationBytes), "grpcMaxMessageBytes: 8192")
}

func TestBinaryDexConfigUnsupportedSetting(t *testing.T) {
	_, unsupportedSetting := binaryDexConfig(DexServiceTestConfig{MemoEncryption: true})
	require.Equal(t, "memo encryption", unsupportedSetting)
}

func TestBinaryDexConfigMapping(t *testing.T) {
	configuration, unsupportedSetting := binaryDexConfig(DexServiceTestConfig{
		BackendType:        service.BackendTypeTemporal,
		LocalBlobThreshold: 256,
		LazyLoading:        ptr.Any(false),
		Matching:           &config.MatchingConfig{DispatchWait: 2 * time.Second, MaxStreams: 16},
		StreamStore:        config.StreamStoreConfig{Backend: config.StreamStoreBackendMemory, MaxReadMessages: 25},
	})
	require.Empty(t, unsupportedSetting)
	require.Equal(t, map[string]any{"wireInlineThresholdBytes": 256, "hydrateBeforeDispatch": true}, configuration["values"])
	require.Equal(t, map[string]any{"dispatchWait": "2s", "maxStreams": 16}, configuration["matching"])
	require.Equal(t, map[string]any{"backend": config.StreamStoreBackendMemory, "maxReadMessages": int32(25)}, configuration["streamStore"])
}

func TestBinaryDexLauncherEarlyExit(t *testing.T) {
	address, process := startFakeBinaryDexProcess(t, "exit")
	err := waitForBinaryDex(address, process, time.Second)
	require.ErrorContains(t, err, "exited before serving")
}

func TestBinaryTemporalOnlyScenarioSkipsBeforePolling(t *testing.T) {
	previousBinary := *dexServerBinary
	*dexServerBinary = "fake-dex-server-zd"
	t.Cleanup(func() { *dexServerBinary = previousBinary })
	conditionRan := false
	passed := t.Run("Temporal-only scenario", func(t *testing.T) {
		requireTemporalOnlyScenarioName(t, "TestDeadEndFlowTemporal")
		conditionRan = true
	})
	require.True(t, passed)
	require.False(t, conditionRan)
}

func TestBinaryDexLauncherReadinessTimeout(t *testing.T) {
	address, process := startFakeBinaryDexProcess(t, "not-serving")
	err := waitForBinaryDex(address, process, 3*time.Second)
	require.ErrorContains(t, err, "readiness timed out")
}

func TestBinaryDexLauncherKillsUnresponsiveProcess(t *testing.T) {
	address, process := startFakeBinaryDexProcess(t, "ignore-sigterm")
	previousGracePeriod := binaryDexTerminationGracePeriod
	binaryDexTerminationGracePeriod = 50 * time.Millisecond
	t.Cleanup(func() { binaryDexTerminationGracePeriod = previousGracePeriod })
	require.NoError(t, waitForBinaryDex(address, process, 5*time.Second))
	process.stop(t)
	select {
	case <-process.done:
	case <-time.After(time.Second):
		t.Fatal("fake Dex Server was not reaped")
	}
	require.Error(t, process.waitErr)
}

func startFakeBinaryDexProcess(t *testing.T, mode string) (string, *binaryDexProcess) {
	t.Helper()
	executable, err := os.Executable()
	require.NoError(t, err)
	address, err := reserveBinaryDexAddress()
	require.NoError(t, err)
	command := exec.Command(executable, "dev", "--data-dir", t.TempDir(), "--listen", address)
	command.Env = append(os.Environ(), "DEX_INTEG_FAKE_BINARY=1", "DEX_INTEG_FAKE_MODE="+mode,
		"DEX_INTEG_FAKE_RECEIPT="+t.TempDir()+"/arguments")
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	require.NoError(t, command.Start())
	process := &binaryDexProcess{command: command, done: make(chan struct{})}
	go func() {
		process.waitErr = command.Wait()
		close(process.done)
	}()
	t.Cleanup(func() { process.stop(t) })
	return address, process
}

func TestBinaryDexLauncherDisablesStreamsByDefault(t *testing.T) {
	executable, err := os.Executable()
	require.NoError(t, err)
	previousBinary, previousEngine, previousMode := *dexServerBinary, *dexServerEngine, *integWorkerMode
	*dexServerBinary, *dexServerEngine, *integWorkerMode = executable, "zd", "pull"
	t.Cleanup(func() {
		*dexServerBinary, *dexServerEngine, *integWorkerMode = previousBinary, previousEngine, previousMode
	})
	t.Setenv("DEX_INTEG_FAKE_BINARY", "1")
	receiptPath := t.TempDir() + "/arguments"
	t.Setenv("DEX_INTEG_FAKE_RECEIPT", receiptPath)
	startBinaryDexService(t, DexServiceTestConfig{BackendType: service.BackendTypeTemporal})
	receiptBytes, err := os.ReadFile(receiptPath)
	require.NoError(t, err)
	arguments := strings.Split(string(receiptBytes), "\n")
	require.Equal(t, "--config", arguments[len(arguments)-2])
	configurationBytes, err := os.ReadFile(arguments[len(arguments)-1])
	require.NoError(t, err)
	require.Equal(t, "streamStore:\n    backend: disabled\n", string(configurationBytes))
}

func runFakeBinaryDex() int {
	arguments := os.Args[1:]
	if len(arguments) < 5 || arguments[0] != "dev" {
		return 2
	}
	address := ""
	for index := 0; index+1 < len(arguments); index++ {
		if arguments[index] == "--listen" {
			address = arguments[index+1]
		}
	}
	if address == "" {
		return 2
	}
	if err := os.WriteFile(os.Getenv("DEX_INTEG_FAKE_RECEIPT"), []byte(strings.Join(arguments, "\n")), 0o600); err != nil {
		return 3
	}
	if os.Getenv("DEX_INTEG_FAKE_MODE") == "exit" {
		return 6
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return 4
	}
	server := grpc.NewServer()
	healthServer := health.NewServer()
	healthStatus := healthpb.HealthCheckResponse_SERVING
	if os.Getenv("DEX_INTEG_FAKE_MODE") == "not-serving" {
		healthStatus = healthpb.HealthCheckResponse_NOT_SERVING
	}
	healthServer.SetServingStatus("", healthStatus)
	healthpb.RegisterHealthServer(server, healthServer)
	dexpb.RegisterFlowServiceServer(server, &fakeBinaryFlowService{})
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM)
	go func() {
		<-signals
		if os.Getenv("DEX_INTEG_FAKE_MODE") != "ignore-sigterm" {
			server.GracefulStop()
		}
	}()
	if err := server.Serve(listener); err != nil {
		return 5
	}
	return 0
}

type fakeBinaryFlowService struct {
	dexpb.UnimplementedFlowServiceServer
}

func (*fakeBinaryFlowService) SyncAttributeIndexes(
	context.Context,
	*dexpb.SyncAttributeIndexRequest,
) (*dexpb.SyncAttributeIndexResponse, error) {
	return &dexpb.SyncAttributeIndexResponse{}, nil
}
