// Copyright (c) 2026 Super Durable, Inc.
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

	"github.com/stretchr/testify/require"
	"github.com/superdurable/dex/examples/go/patterns/polling"
	"github.com/superdurable/dex/examples/go/registry"
	"github.com/superdurable/dex/sdk-go/dex"
)

func TestPollingStreamsJobProgressAndCompletesWithJobResult(t *testing.T) {
	skipWhenServerZD(t, "failure policies")
	ctx := integrationContext(t)
	flowID := newFlowID(t, "pattern-polling")
	_, err := integClient.StartFlow(ctx, registry.Polling, flowID, nil, dex.StartFlowOptions{})
	require.NoError(t, err)

	var queued polling.JobStatus
	queuedMessage, err := integClient.ReadStream(ctx, flowID, polling.JobProgress, "", &queued)
	require.NoError(t, err)
	require.Equal(t, polling.JobQueued, queued.State)

	var running polling.JobStatus
	_, err = integClient.ReadStream(ctx, flowID, polling.JobProgress, queuedMessage.ResumeToken, &running)
	require.NoError(t, err)
	require.Equal(t, polling.JobRunning, running.State)

	completeJobURL := examplesAPIURL + "/patterns/polling/complete-job?" + url.Values{"workflowId": {flowID}}.Encode()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, completeJobURL, nil)
	require.NoError(t, err)
	response, err := http.DefaultClient.Do(request)
	require.NoError(t, err)
	require.NoError(t, response.Body.Close())
	require.Equal(t, http.StatusOK, response.StatusCode)

	result := waitForFlow(t, flowID)
	require.Equal(t, dex.FlowCompleted, result.Status)
	require.Len(t, result.Completions, 1)
	var output string
	require.NoError(t, result.Completions[0].Output.Decode(&output))
	require.Equal(t, "artifact for "+flowID, output)
}
