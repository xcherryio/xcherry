// Legacy Materials in this file remain under their original licenses.
// See LEGACY_NOTICES.md.

// Modifications Copyright (c) 2026 Super Durable, Inc.
//
// Modifications after the Legacy Cutoff are licensed under the
// Sustainable Use License 1.0.
// Legacy Materials remain under their original licenses.
// See LICENSE and LEGACY_NOTICES.md.

package integ

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/superdurable/dex/blob-cache-go/blobcache"
	"github.com/superdurable/dex/sdk-go/dex"
	"github.com/superdurable/dex/sdk-go/integ/webv2approval"
	"github.com/superdurable/dex/sdk-go/integ/webv2reply"
)

var integClient *dex.Client

var integWorkerTargetAddress string

var isPullWorkerMode bool

type emptyFlowSchema struct {
	dex.FlowDefaults
}

func (emptyFlowSchema) GetPersistenceSchema() dex.PersistenceSchema {
	return dex.PersistenceSchema{}
}

type integrationEnvironment struct {
	cache        *blobcache.Cache
	cacheDir     string
	client       *dex.Client
	worker       *dex.Worker
	workerResult chan error
}

func newIntegrationEnvironment() (*integrationEnvironment, error) {
	registry, err := dex.NewRegistry(integrationFlows())
	if err != nil {
		return nil, err
	}
	cacheDir, err := os.MkdirTemp("", "dex-go-sdk-integ-")
	if err != nil {
		return nil, err
	}
	cache, err := blobcache.New(&blobcache.Config{
		Dir:      cacheDir,
		MaxBytes: 64 << 20,
	})
	if err != nil {
		return nil, errors.Join(err, os.RemoveAll(cacheDir))
	}
	workerOptions := dex.WorkerOptions{FlowServiceAddress: flowServiceAddress()}
	if isPullWorkerMode {
		workerOptions.Pull = &dex.PullWorkerOptions{}
	} else {
		workerPort, portErr := integrationWorkerPort()
		if portErr != nil {
			return nil, errors.Join(portErr, cache.Close(), os.RemoveAll(cacheDir))
		}
		workerOptions.BindAddress = net.JoinHostPort("127.0.0.1", workerPort)
		workerOptions.WorkerTarget = dex.WorkerTarget{Address: net.JoinHostPort(workerHost(), workerPort)}
	}
	worker, err := dex.NewWorker(registry, cache, workerOptions)
	if err != nil {
		return nil, errors.Join(err, cache.Close(), os.RemoveAll(cacheDir))
	}
	workerResult := make(chan error, 1)
	go func() {
		workerResult <- worker.Start()
	}()
	client, err := dex.NewClient(registry, cache, dex.ClientOptions{
		FlowServiceAddress: flowServiceAddress(),
		WorkerTarget:       worker.WorkerTarget(),
	})
	if err != nil {
		stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return nil, errors.Join(err, worker.Stop(stopCtx), cache.Close())
	}
	environment := &integrationEnvironment{
		cache:        cache,
		cacheDir:     cacheDir,
		client:       client,
		worker:       worker,
		workerResult: workerResult,
	}
	if !isPullWorkerMode {
		if err := environment.waitUntilReady(); err != nil {
			return nil, errors.Join(err, environment.Close())
		}
	}
	return environment, nil
}

func (environment *integrationEnvironment) waitUntilReady() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		connection, err := net.DialTimeout(
			"tcp",
			environment.worker.WorkerTarget().Address,
			100*time.Millisecond,
		)
		if err == nil {
			return connection.Close()
		}
		select {
		case workerErr := <-environment.workerResult:
			environment.workerResult <- workerErr
			return fmt.Errorf("start integration Worker: %w", workerErr)
		case <-ticker.C:
		case <-ctx.Done():
			return fmt.Errorf("wait for integration Worker: %w", ctx.Err())
		}
	}
}

func (environment *integrationEnvironment) Close() error {
	clientErr := environment.client.Close()
	stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stopErr := environment.worker.Stop(stopCtx)
	workerErr := <-environment.workerResult
	cacheErr := environment.cache.Close()
	removeErr := os.RemoveAll(environment.cacheDir)
	return errors.Join(clientErr, stopErr, workerErr, cacheErr, removeErr)
}

func TestMain(tests *testing.M) {
	workerMode := os.Getenv("DEX_INTEG_WORKER_MODE")
	if workerMode != "" && workerMode != "push" && workerMode != "pull" {
		fmt.Fprintf(os.Stderr, "invalid DEX_INTEG_WORKER_MODE %q: expected push or pull\n", workerMode)
		os.Exit(2)
	}
	isPullWorkerMode = workerMode == "pull"
	environment, err := newIntegrationEnvironment()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	integClient = environment.client
	integWorkerTargetAddress = environment.worker.WorkerTarget().Address
	exitCode := tests.Run()
	if err := environment.Close(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		exitCode = 1
	}
	os.Exit(exitCode)
}

func integrationFlows() []dex.Flow {
	return []dex.Flow{
		actionPermissionFlow{},
		actionScalarFlow{},
		actionStepFlow{},
		actionSubFlowParent{},
		actionSubFlowChild{},
		abnormalExitFlow{},
		basicFlow{},
		multiOutputFlow{},
		proceedOnWaitForFailureFlow{},
		executeRecoveryFlow{},
		interStepFlow{},
		noStartStepFlow{},
		noStepFlow{},
		persistenceFlow{},
		hardDeleteAttributeFlow{},
		rpcFlow{},
		selectiveRPCFlow{},
		channelFlow{},
		streamTestFlow{},
		heartbeatRecoveryFlow{},
		asyncHeartbeatFlow{},
		executeOnlyFlow{},
		executeOnlyValueFlow{},
		forceFailFlow{},
		waitForFailureFlow{},
		waitForMethodTimeoutFlow{},
		timeoutHandlerFlow{},
		timerFlow{},
		subFlowParentFlow{},
		subFlowAllParentFlow{},
		subFlowAnyParentFlow{},
		subFlowAttachParentFlow{},
		subFlowAlwaysRestartParentFlow{},
		subFlowAbnormalParentFlow{},
		subFlowContinueAsNewParentFlow{},
		subFlowTimerFlow{},
		subFlowFailingFlow{},
		subFlowImmediateFlow{},
		stepCancellationFlow{},
		workerRetryAfterWaitForFlow{},
		workerRetryAfterExecuteFlow{},
		workerOriginStackWaitForFlow{},
		messageSizeFlow{},
		&webv2approval.ApprovalFlow{},
		&webv2reply.ReplyFlow{},
	}
}

func integrationContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	t.Cleanup(cancel)
	return ctx
}

func newFlowID(t *testing.T, prefix string) string {
	t.Helper()
	return prefix + "-" + uuid.NewString()
}

func waitForFlow(t *testing.T, flowID string, needsResults bool) dex.FlowResult {
	t.Helper()
	result, err := integClient.WaitForFlow(
		integrationContext(t),
		flowID,
		dex.WaitForFlowOptions{NeedsResults: needsResults},
	)
	require.NoError(t, err)
	return result
}

func waitForUncompletedFlow(
	t *testing.T,
	flowID string,
	needsResults bool,
) dex.FlowResult {
	t.Helper()
	result, err := integClient.WaitForFlow(
		integrationContext(t),
		flowID,
		dex.WaitForFlowOptions{NeedsResults: needsResults},
	)
	require.NoError(t, err)
	require.True(t, result.IsTerminal())
	require.NotEqual(t, dex.FlowCompleted, result.Status)
	return result
}

func availablePort() (string, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	port := listener.Addr().(*net.TCPAddr).Port
	if err := listener.Close(); err != nil {
		return "", err
	}
	return fmt.Sprintf("%d", port), nil
}

func flowServiceAddress() string {
	if address := os.Getenv("DEX_FLOW_SERVICE_ADDRESS"); address != "" {
		return address
	}
	return "127.0.0.1:8801"
}

func workerHost() string {
	if host := os.Getenv("DEX_WORKER_HOST"); host != "" {
		return host
	}
	return "127.0.0.1"
}

func integrationWorkerPort() (string, error) {
	if port := os.Getenv("DEX_WORKER_PORT"); port != "" {
		return port, nil
	}
	return availablePort()
}
