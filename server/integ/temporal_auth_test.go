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
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/superdurable/dex/config"
	"github.com/superdurable/dex/gen/dexpb"
	"github.com/superdurable/dex/integ/workflow/basic"
	"github.com/superdurable/dex/service"
	"github.com/superdurable/dex/service/bootstrap"
	"github.com/superdurable/dex/service/common/ptr"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestTemporalAuthTokenRotation(t *testing.T) {
	if !*temporalIntegTest {
		t.Skip("Temporal integration tests disabled")
	}
	requireGoServerSetting(t, "Temporal client authentication")
	keyPath := filepath.Join(t.TempDir(), "api-key")
	require.NoError(t, os.WriteFile(keyPath, []byte("initial-token\n"), 0600))
	proxy := newTemporalAuthProxy(t, nil, "initial-token")
	temporalConfig := &config.TemporalConfig{HostPort: proxy.listener.Addr().String(), APIKeyFile: keyPath, APIKeyFileRefreshInterval: 20 * time.Millisecond, TLS: &config.TemporalTLSConfig{Enabled: ptr.Any(false)}}
	workerTarget := startWorker(t, basic.NewHandler())
	logPath := filepath.Join(t.TempDir(), "dex.log")
	flowClient := startTemporalAuthDex(t, temporalConfig, logPath)
	runTemporalAuthFlow(t, flowClient, workerTarget)
	proxy.setCurrentKey("rotated-token")
	require.NoError(t, os.WriteFile(keyPath, []byte("rotated-token\n"), 0600))
	runTemporalAuthFlow(t, flowClient, workerTarget)
	accepted, _, _ := proxy.snapshot()
	require.Contains(t, accepted, "Bearer initial-token")
	require.Contains(t, accepted, "Bearer rotated-token")
	require.True(t, proxy.observedNamespaceHeader())
	_, rejectedBefore, _ := proxy.snapshot()
	runTemporalAuthFlow(t, flowClient, workerTarget)
	_, rejectedAfter, _ := proxy.snapshot()
	require.Equal(t, rejectedBefore, rejectedAfter)
	require.NoError(t, os.WriteFile(keyPath, nil, 0600))
	runTemporalAuthFlow(t, flowClient, workerTarget)
	require.Eventually(t, func() bool {
		content, err := os.ReadFile(logPath)
		return err == nil && strings.Contains(string(content), "Temporal API key file is empty")
	}, 5*time.Second, 50*time.Millisecond)
	_, rejectedAfterEmptyFile, _ := proxy.snapshot()
	require.Equal(t, rejectedAfter, rejectedAfterEmptyFile)
}

func TestTemporalAuthAPIKeyFileStartupValidation(t *testing.T) {
	if !*temporalIntegTest {
		t.Skip("Temporal integration tests disabled")
	}
	keyPath := filepath.Join(t.TempDir(), "api-key")
	for _, testCase := range []struct {
		name             string
		shouldCreateFile bool
		errorContains    string
	}{
		{name: "missing", errorContains: "stat Temporal API key file"},
		{name: "empty", shouldCreateFile: true, errorContains: "Temporal API key file is empty"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			if testCase.shouldCreateFile {
				require.NoError(t, os.WriteFile(keyPath, []byte(" \n"), 0600))
			}
			cfg := createTestConfig(t, DexServiceTestConfig{BackendType: service.BackendTypeTemporal})
			cfg.Interpreter.Temporal = &config.TemporalConfig{HostPort: "127.0.0.1:7233", Namespace: testNamespace, APIKeyFile: keyPath}
			_, err := bootstrap.New(&cfg, &bootstrap.Options{Services: bootstrap.Services{Interpreter: true}})
			require.ErrorContains(t, err, testCase.errorContains)
		})
	}
}

func TestTemporalAuthCustomCAAndServerName(t *testing.T) {
	if !*temporalIntegTest {
		t.Skip("Temporal integration tests disabled")
	}
	requireGoServerSetting(t, "Temporal client authentication")
	testCA := newTemporalTestCA(t)
	proxy := newTemporalAuthProxy(t, &tls.Config{Certificates: []tls.Certificate{testCA.serverCertificate}, MinVersion: tls.VersionTLS12}, "")
	workerTarget := startWorker(t, basic.NewHandler())
	temporalConfig := &config.TemporalConfig{HostPort: proxy.listener.Addr().String(), TLS: &config.TemporalTLSConfig{Enabled: ptr.Any(true), CAFile: testCA.caPath, ServerName: "temporal-auth.test"}}
	flowClient := startTemporalAuthDex(t, temporalConfig, "")
	runTemporalAuthFlow(t, flowClient, workerTarget)
	withoutCA := *temporalConfig
	withoutCA.TLS = &config.TemporalTLSConfig{Enabled: ptr.Any(true), ServerName: "temporal-auth.test"}
	cfg := createTestConfig(t, DexServiceTestConfig{BackendType: service.BackendTypeTemporal})
	cfg.Interpreter.Temporal = &withoutCA
	cfg.Interpreter.Temporal.Namespace = testNamespace
	_, err := bootstrap.New(&cfg, &bootstrap.Options{Services: bootstrap.Services{Interpreter: true}})
	require.ErrorContains(t, err, "certificate")
}

func TestTemporalAuthClientCertificateRotation(t *testing.T) {
	if !*temporalIntegTest {
		t.Skip("Temporal integration tests disabled")
	}
	requireGoServerSetting(t, "Temporal client authentication")
	testCA := newTemporalTestCA(t)
	pool := x509.NewCertPool()
	caPEM, err := os.ReadFile(testCA.caPath)
	require.NoError(t, err)
	require.True(t, pool.AppendCertsFromPEM(caPEM))
	proxy := newTemporalAuthProxy(t, &tls.Config{Certificates: []tls.Certificate{testCA.serverCertificate}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: pool, MinVersion: tls.VersionTLS12}, "")
	clientDirectory := t.TempDir()
	certPath, keyPath := testCA.writeClientCertificate(t, clientDirectory, "first-client")
	temporalConfig := &config.TemporalConfig{HostPort: proxy.listener.Addr().String(), TLS: &config.TemporalTLSConfig{CAFile: testCA.caPath, ServerName: "temporal-auth.test", CertFile: certPath, KeyFile: keyPath}}
	workerTarget := startWorker(t, basic.NewHandler())
	flowClient := startTemporalAuthDex(t, temporalConfig, "")
	runTemporalAuthFlow(t, flowClient, workerTarget)
	testCA.writeClientCertificate(t, clientDirectory, "second-client")
	require.NoError(t, proxy.listener.closeConnections())
	require.Eventually(t, func() bool {
		_, _, certificates := proxy.snapshot()
		for _, name := range certificates {
			if name == "second-client" {
				return true
			}
		}
		return false
	}, 15*time.Second, 100*time.Millisecond)
	runTemporalAuthFlow(t, flowClient, workerTarget)
}

func TestTemporalAuthPlaintextKeyCompatibility(t *testing.T) {
	if !*temporalIntegTest {
		t.Skip("Temporal integration tests disabled")
	}
	requireGoServerSetting(t, "Temporal client authentication")
	proxy := newTemporalAuthProxy(t, nil, "static-token")
	logPath := filepath.Join(t.TempDir(), "dex.log")
	temporalConfig := &config.TemporalConfig{HostPort: proxy.listener.Addr().String(), CloudAPIKey: "static-token", TLS: &config.TemporalTLSConfig{Enabled: ptr.Any(false)}}
	workerTarget := startWorker(t, basic.NewHandler())
	flowClient := startTemporalAuthDex(t, temporalConfig, logPath)
	runTemporalAuthFlow(t, flowClient, workerTarget)
	logContent, err := os.ReadFile(logPath)
	require.NoError(t, err)
	require.Contains(t, string(logContent), "Temporal API key will be sent over plaintext")
	accepted, _, _ := proxy.snapshot()
	require.Contains(t, accepted, "Bearer static-token")
}

func startTemporalAuthDex(t *testing.T, temporalConfig *config.TemporalConfig, logPath string) dexpb.FlowServiceClient {
	t.Helper()
	apiListener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	cfg := createTestConfig(t, DexServiceTestConfig{BackendType: service.BackendTypeTemporal})
	cfg.Interpreter.Temporal = temporalConfig
	cfg.Interpreter.Temporal.Namespace = testNamespace
	cfg.Interpreter.InterpreterActivityConfig.InternalServiceTarget = apiListener.Addr().String()
	if logPath != "" {
		cfg.Log.OutputFile = logPath
	}
	runtime, err := bootstrap.New(&cfg, &bootstrap.Options{Services: bootstrap.Services{API: true, Interpreter: true}, APIListener: apiListener})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	runError := make(chan error, 1)
	go func() { runError <- runtime.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		runtime.Close()
		require.NoError(t, <-runError)
		if err := apiListener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			require.NoError(t, err)
		}
	})
	flowClient := newFlowClientWithCredential(t, apiListener.Addr().String(), "")
	require.Eventually(t, func() bool {
		readyContext, readyCancel := context.WithTimeout(context.Background(), time.Second)
		defer readyCancel()
		_, readyErr := flowClient.GetServerInfo(readyContext, &emptypb.Empty{})
		return readyErr == nil
	}, 30*time.Second, 50*time.Millisecond)
	return flowClient
}

func runTemporalAuthFlow(t *testing.T, flowClient dexpb.FlowServiceClient, workerTarget *dexpb.WorkerTarget) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
	defer cancel()
	flowID := basic.FlowType + "-" + uuid.NewString()
	startRequest := &dexpb.StartFlowRequest{
		RequestId: newRequestID(), FlowId: flowID, FlowType: basic.FlowType, FlowTimeoutSeconds: 100,
		StartStepType:    basic.Step1,
		StepInput:        &dexpb.Value{Kind: &dexpb.Value_ObjValue{ObjValue: &dexpb.EncodedObject{Encoding: "json", Payload: []byte("test data")}}},
		FlowStartOptions: withWorkerTarget(&dexpb.FlowStartOptions{FlowConfigOverride: syncDurabilityConfig()}, workerTarget),
	}
	var startErr error
	require.Eventually(t, func() bool {
		_, startErr = flowClient.StartFlow(ctx, startRequest)
		return startErr == nil
	}, 15*time.Second, 50*time.Millisecond)
	_, err := flowClient.WaitForFlow(ctx, &dexpb.WaitForFlowRequest{FlowId: flowID, NeedsResults: true, WaitTimeSeconds: 25})
	require.NoError(t, err)
}
