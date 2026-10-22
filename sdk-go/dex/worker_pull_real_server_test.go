// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//go:build pullintegration

package dex

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/superdurable/dex/blob-cache-go/blobcache"
)

type pullBurstFlow struct {
	FlowDefaults
	entered chan struct{}
	release chan struct{}
}

func (flow *pullBurstFlow) GetSteps() []StepDef {
	return []StepDef{DefineStartStep(pullBurstStep{entered: flow.entered, release: flow.release})}
}

func (*pullBurstFlow) GetPersistenceSchema() PersistenceSchema { return PersistenceSchema{} }

type pullBurstStep struct {
	StepDefaultsNoWaitFor[string]
	entered chan struct{}
	release chan struct{}
}

func (step pullBurstStep) Execute(ctx Context, input string) (*StepDecision, error) {
	step.entered <- struct{}{}
	select {
	case <-step.release:
		return GracefulComplete(input), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func TestWorkerPullShrinksStreamsWithRealServer(t *testing.T) {
	address := os.Getenv("DEX_FLOW_SERVICE_ADDRESS")
	require.NotEmpty(t, address)
	flow := &pullBurstFlow{entered: make(chan struct{}, 3), release: make(chan struct{})}
	worker, err := newWorkerForTest(t, []Flow{flow}, WorkerOptions{FlowServiceAddress: address, Pull: &PullWorkerOptions{MaxConcurrentTasks: 3, AppWorkerID: "shrink-" + uuid.NewString()}})
	require.NoError(t, err)
	started := make(chan error, 1)
	go func() { started <- worker.Start() }()
	t.Cleanup(func() {
		require.NoError(t, worker.Stop(context.Background()))
		require.NoError(t, <-started)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	require.NoError(t, worker.WaitPullRegistered(ctx))
	cache, err := blobcache.New(&blobcache.Config{Dir: t.TempDir(), MaxBytes: 1 << 20})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cache.Close()) })
	client, err := NewClient(worker.registry, cache, ClientOptions{FlowServiceAddress: address, WorkerTarget: worker.WorkerTarget()})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	flowIDs := make([]string, 3)
	for i := range flowIDs {
		flowIDs[i] = "shrink-" + uuid.NewString()
		_, err = client.StartFlow(ctx, flow, flowIDs[i], "done", StartFlowOptions{})
		require.NoError(t, err)
	}
	for range flowIDs {
		select {
		case <-flow.entered:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	require.Eventually(t, func() bool {
		worker.pull.mu.Lock()
		defer worker.pull.mu.Unlock()
		return len(worker.pull.streams) == 5 && worker.pull.busyTasks == 3
	}, 5*time.Second, time.Millisecond)
	close(flow.release)
	for _, flowID := range flowIDs {
		result, err := client.WaitForFlow(ctx, flowID, WaitForFlowOptions{})
		require.NoError(t, err)
		require.Equal(t, FlowCompleted, result.Status)
	}
	require.Eventually(t, func() bool {
		require.True(t, worker.PullRegistrationState().IsReady)
		worker.pull.mu.Lock()
		defer worker.pull.mu.Unlock()
		return len(worker.pull.streams) == 2 && worker.pull.busyTasks == 0
	}, 5*time.Second, time.Millisecond)
}
