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
	"context"
	"errors"
	"fmt"
	"net"
	"net/http/httptest"
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/superdurable/dex/blob-cache-go/blobcache"
	exampleserver "github.com/superdurable/dex/examples/go/cmd/server/dex"
	"github.com/superdurable/dex/examples/go/registry"
	"github.com/superdurable/dex/examples/go/shared/service"
	"github.com/superdurable/dex/examples/go/shared/workermode"
	"github.com/superdurable/dex/sdk-go/dex"
)

var (
	integClient    *dex.Client
	examplesAPIURL string
	dexcliPath     string
	flowCounter    atomic.Int64
)

type integrationEnvironment struct {
	cache         *blobcache.Cache
	cacheDir      string
	client        *dex.Client
	apiServer     *httptest.Server
	worker        *dex.Worker
	workerAddress string
	workerMode    workermode.WorkerMode
	workerResult  chan error
}

func newIntegrationEnvironment() (*integrationEnvironment, error) {
	var client *dex.Client
	flows := registry.New(service.NewMyService(), func() *dex.Client { return client })
	registry, err := dex.NewRegistry(flows)
	if err != nil {
		return nil, err
	}
	cacheDir, err := os.MkdirTemp("", "dex-go-examples-integ-")
	if err != nil {
		return nil, err
	}
	cache, err := blobcache.New(&blobcache.Config{Dir: cacheDir, MaxBytes: 64 << 20})
	if err != nil {
		return nil, errors.Join(err, os.RemoveAll(cacheDir))
	}
	workerMode, err := workermode.ReadWorkerModeFromEnvironment()
	if err != nil {
		return nil, errors.Join(err, cache.Close(), os.RemoveAll(cacheDir))
	}
	workerPort, err := availablePort()
	if err != nil {
		return nil, errors.Join(err, cache.Close(), os.RemoveAll(cacheDir))
	}
	worker, err := dex.NewWorker(registry, cache, integrationWorkerOptions(workerMode, workerPort))
	if err != nil {
		return nil, errors.Join(err, cache.Close(), os.RemoveAll(cacheDir))
	}
	workerResult := make(chan error, 1)
	go func() {
		workerResult <- worker.Start()
	}()
	client, err = dex.NewClient(registry, cache, dex.ClientOptions{
		FlowServiceAddress: flowServiceAddress(),
		WorkerTarget:       worker.WorkerTarget(),
	})
	if err != nil {
		stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return nil, errors.Join(err, worker.Stop(stopCtx), cache.Close(), os.RemoveAll(cacheDir))
	}
	apiServer := httptest.NewServer(exampleserver.NewRouter(client))
	environment := &integrationEnvironment{
		cache:         cache,
		cacheDir:      cacheDir,
		client:        client,
		apiServer:     apiServer,
		worker:        worker,
		workerAddress: net.JoinHostPort("127.0.0.1", workerPort),
		workerMode:    workerMode,
		workerResult:  workerResult,
	}
	integClient = client
	if err := environment.waitUntilReady(); err != nil {
		return nil, errors.Join(err, environment.Close())
	}
	return environment, nil
}

func (environment *integrationEnvironment) waitUntilReady() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if environment.workerMode.IsPull() {
		if err := environment.worker.WaitPullRegistered(ctx); err != nil {
			return fmt.Errorf("wait for pull Worker registration: %w", err)
		}
		return nil
	}
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		connection, err := net.DialTimeout("tcp", environment.workerAddress, 100*time.Millisecond)
		if err == nil {
			if closeErr := connection.Close(); closeErr != nil {
				return fmt.Errorf("close Worker readiness connection: %w", closeErr)
			}
			return nil
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return fmt.Errorf("wait for Worker readiness: %w", ctx.Err())
		}
	}
}

func integrationWorkerOptions(workerMode workermode.WorkerMode, workerPort string) dex.WorkerOptions {
	if workerMode.IsPull() {
		return dex.WorkerOptions{
			FlowServiceAddress: flowServiceAddress(),
			Pull:               workermode.NewExamplePullWorkerOptions(),
		}
	}
	return dex.WorkerOptions{
		BindAddress:        net.JoinHostPort("127.0.0.1", workerPort),
		FlowServiceAddress: flowServiceAddress(),
		WorkerTarget: dex.WorkerTarget{
			Address: net.JoinHostPort(workerHost(), workerPort),
		},
	}
}

func (environment *integrationEnvironment) Close() error {
	environment.apiServer.Close()
	clientErr := environment.client.Close()
	stopCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	stopErr := environment.worker.Stop(stopCtx)
	workerErr := <-environment.workerResult
	cacheErr := environment.cache.Close()
	removeErr := os.RemoveAll(environment.cacheDir)
	return errors.Join(clientErr, stopErr, workerErr, cacheErr, removeErr)
}

func TestMain(tests *testing.M) {
	environment, err := newIntegrationEnvironment()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	integClient = environment.client
	examplesAPIURL = environment.apiServer.URL
	exitCode := tests.Run()
	if err := environment.Close(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		exitCode = 1
	}
	os.Exit(exitCode)
}

func integrationContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	t.Cleanup(cancel)
	return ctx
}

func newFlowID(t *testing.T, prefix string) string {
	t.Helper()
	sequence := flowCounter.Add(1)
	return prefix + "-" + strconv.FormatInt(time.Now().UnixNano(), 10) + "-" + strconv.FormatInt(sequence, 10)
}

func waitForFlow(t *testing.T, flowID string) dex.FlowResult {
	t.Helper()
	result, err := integClient.WaitForFlow(
		integrationContext(t),
		flowID,
		dex.WaitForFlowOptions{NeedsResults: true},
	)
	require.NoError(t, err)
	return result
}

// skipWhenServerZD skips a sample whose feature server-zd rejects.
func skipWhenServerZD(t *testing.T, feature string) {
	t.Helper()
	if os.Getenv("DEX_INTEG_ENGINE") == "zd" {
		t.Skipf("server-zd does not support %s", feature)
	}
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
	return strconv.Itoa(port), nil
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
