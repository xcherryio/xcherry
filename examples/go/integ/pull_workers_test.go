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
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/superdurable/dex/blob-cache-go/blobcache"
	"github.com/superdurable/dex/examples/go/production/pull-workers"
	"github.com/superdurable/dex/sdk-go/dex"
)

func TestPullWorkerServesGreetingFlow(t *testing.T) {
	cache, err := blobcache.New(&blobcache.Config{Dir: t.TempDir(), MaxBytes: 64 << 20})
	require.NoError(t, err)
	flow, worker, client, workerResult, err := pullworkers.StartPullGreeting(cache, flowServiceAddress())
	require.NoError(t, err)
	t.Cleanup(func() {
		stopContext, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		require.NoError(t, pullworkers.StopPullGreeting(stopContext, worker, client, workerResult))
		require.NoError(t, cache.Close())
	})

	readinessResponse := httptest.NewRecorder()
	pullworkers.PullGreetingReadiness(worker).ServeHTTP(readinessResponse, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	require.Equal(t, http.StatusOK, readinessResponse.Code)

	flowID := newFlowID(t, "pull-greeting")
	_, err = client.StartFlow(integrationContext(t), flow, flowID, "Dex", dex.StartFlowOptions{})
	require.NoError(t, err)
	result, err := client.WaitForFlow(
		integrationContext(t), flowID, dex.WaitForFlowOptions{NeedsResults: true},
	)
	require.NoError(t, err)
	require.Equal(t, dex.FlowCompleted, result.Status)
	var output string
	require.NoError(t, result.DecodeSingleOutput(&output))
	require.Equal(t, "Hello, Dex!", output)
	var greeting string
	require.NoError(t, client.InvokeRPC(integrationContext(t), flowID, flow.GetGreeting, nil, &greeting))
	require.Equal(t, output, greeting)
}

func TestPullWorkerReadinessTransitions(t *testing.T) {
	cache, err := blobcache.New(&blobcache.Config{Dir: t.TempDir(), MaxBytes: 64 << 20})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cache.Close()) })
	registry, err := dex.NewRegistry([]dex.Flow{&pullworkers.GreetingFlow{}})
	require.NoError(t, err)
	worker, err := dex.NewWorker(registry, cache, dex.WorkerOptions{
		FlowServiceAddress: flowServiceAddress(),
		Pull:               &dex.PullWorkerOptions{MaxConcurrentTasks: 8},
	})
	require.NoError(t, err)
	workerResult := make(chan error, 1)
	t.Cleanup(func() {
		stopContext := integrationContext(t)
		require.NoError(t, worker.Stop(stopContext))
		select {
		case workerRunErr := <-workerResult:
			require.NoError(t, workerRunErr)
		case <-stopContext.Done():
			t.Fatal(stopContext.Err())
		}
	})
	readinessResponse := httptest.NewRecorder()
	pullworkers.PullGreetingReadiness(worker).ServeHTTP(readinessResponse, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	require.Equal(t, http.StatusServiceUnavailable, readinessResponse.Code)
	go func() { workerResult <- worker.Start() }()
	require.NoError(t, worker.WaitPullRegistered(integrationContext(t)))
	readinessResponse = httptest.NewRecorder()
	pullworkers.PullGreetingReadiness(worker).ServeHTTP(readinessResponse, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	require.Equal(t, http.StatusOK, readinessResponse.Code)
}
