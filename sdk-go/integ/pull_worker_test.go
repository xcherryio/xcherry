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
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/superdurable/dex/blob-cache-go/blobcache"
	"github.com/superdurable/dex/sdk-go/dex"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type pullSDKObservation struct {
	workerID string
	method   string
}

type pullSDKFlow struct {
	emptyFlowSchema
	workerID       string
	observations   chan<- pullSDKObservation
	firstStepGate  <-chan struct{}
	secondStepGate <-chan struct{}
	omitWorkerRPC  bool
}

func (flow pullSDKFlow) GetSteps() []dex.StepDef {
	return []dex.StepDef{
		dex.DefineStartStep(pullSDKFirstStep{workerID: flow.workerID, observations: flow.observations, gate: flow.firstStepGate}),
		dex.DefineStep(pullSDKSecondStep{workerID: flow.workerID, observations: flow.observations, gate: flow.secondStepGate}),
	}
}

func (flow pullSDKFlow) GetRPCs() []dex.RPCDef {
	if flow.omitWorkerRPC {
		return nil
	}
	return []dex.RPCDef{dex.DefineRPC(flow.GetWorker, nil)}
}

func (flow pullSDKFlow) GetWorker(dex.Context, dex.None) (*dex.RPCResult[string], error) {
	flow.observations <- pullSDKObservation{workerID: flow.workerID, method: "RPC"}
	return &dex.RPCResult[string]{Output: flow.workerID}, nil
}

type pullSDKFirstStep struct {
	dex.StepDefaults
	workerID     string
	observations chan<- pullSDKObservation
	gate         <-chan struct{}
}

func (step pullSDKFirstStep) WaitFor(dex.Context, string) (*dex.Wait, error) {
	step.observations <- pullSDKObservation{workerID: step.workerID, method: "WaitFor"}
	return dex.SkipWaitImmediately(), nil
}

func (step pullSDKFirstStep) Execute(ctx dex.Context, input string) (*dex.StepDecision, error) {
	step.observations <- pullSDKObservation{workerID: step.workerID, method: "Execute"}
	select {
	case <-step.gate:
		return dex.GoTo(pullSDKSecondStep{}, input), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

type pullSDKSecondStep struct {
	dex.StepDefaults
	workerID     string
	observations chan<- pullSDKObservation
	gate         <-chan struct{}
}

func (step pullSDKSecondStep) WaitFor(dex.Context, string) (*dex.Wait, error) {
	step.observations <- pullSDKObservation{workerID: step.workerID, method: "SecondWaitFor"}
	return dex.SkipWaitImmediately(), nil
}

func (step pullSDKSecondStep) Execute(ctx dex.Context, input string) (*dex.StepDecision, error) {
	step.observations <- pullSDKObservation{workerID: step.workerID, method: "SecondExecute"}
	select {
	case <-step.gate:
		return dex.GracefulComplete(input), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

type pullSDKProbeFlow struct {
	emptyFlowSchema
	workerID string
}

func (flow pullSDKProbeFlow) GetSteps() []dex.StepDef {
	return []dex.StepDef{dex.DefineStartStep(pullSDKProbeStep{workerID: flow.workerID})}
}

type pullSDKProbeStep struct {
	dex.StepDefaultsNoWaitFor[string]
	workerID string
}

func (step pullSDKProbeStep) Execute(dex.Context, string) (*dex.StepDecision, error) {
	return dex.GracefulComplete(step.workerID), nil
}

type pullSDKInvalidRegistrationFlow struct {
	pullSDKProbeFlow
}

func (pullSDKInvalidRegistrationFlow) GetFlowType() string {
	return strings.Repeat("invalid-registration-", 20)
}

func TestPullSDKRegistrationReadiness(t *testing.T) {
	worker := startPullSDKWorker(t, []dex.Flow{pullSDKProbeFlow{}}, "pull-ready-"+uuid.NewString())
	require.NoError(t, worker.WaitPullRegistered(integrationContext(t)))
	require.True(t, worker.PullRegistrationState().IsReady)
	require.Len(t, worker.PullRegistrationState().Queues, 2)
}

func TestPullSDKRegistrationRejection(t *testing.T) {
	registry, err := dex.NewRegistry([]dex.Flow{pullSDKInvalidRegistrationFlow{}})
	require.NoError(t, err)
	worker, err := dex.NewWorker(registry, newPullSDKCache(t), dex.WorkerOptions{
		FlowServiceAddress: flowServiceAddress(),
		Pull:               &dex.PullWorkerOptions{AppWorkerID: "pull-invalid-" + uuid.NewString()},
	})
	require.NoError(t, err)
	result := make(chan error, 1)
	go func() { result <- worker.Start() }()
	t.Cleanup(func() {
		require.NoError(t, worker.Stop(context.Background()))
		require.NoError(t, <-result)
	})
	var rejection *dex.PullRegistrationError
	require.ErrorAs(t, worker.WaitPullRegistered(integrationContext(t)), &rejection)
	require.Equal(t, codes.InvalidArgument, status.Code(rejection.Err))
	require.NotEmpty(t, rejection.Queue)
	require.False(t, worker.PullRegistrationState().IsReady)
}

func TestPullSDKWorkerEndToEnd(t *testing.T) {
	observations := make(chan pullSDKObservation, 16)
	firstStepGate := make(chan struct{})
	secondStepGate := make(chan struct{})
	workerAID := "pull-go-a-" + uuid.NewString()
	workerBID := "pull-go-b-" + uuid.NewString()
	workerAFlow := pullSDKFlow{workerID: workerAID, observations: observations, firstStepGate: firstStepGate, secondStepGate: secondStepGate}
	workerBFlow := pullSDKFlow{workerID: workerBID, observations: observations, firstStepGate: firstStepGate, secondStepGate: secondStepGate}
	workerA := startPullSDKWorker(t, []dex.Flow{workerAFlow}, workerAID)
	clientRegistry, err := dex.NewRegistry([]dex.Flow{pullSDKFlow{}, pullSDKProbeFlow{}})
	require.NoError(t, err)
	clientCache := newPullSDKCache(t)
	client, err := dex.NewClient(clientRegistry, clientCache, dex.ClientOptions{FlowServiceAddress: flowServiceAddress(), WorkerTarget: &dex.WorkerTarget{Pull: true}})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	flowID := newFlowID(t, "pull-go")
	_, err = client.StartFlow(integrationContext(t), pullSDKFlow{}, flowID, "completed", dex.StartFlowOptions{})
	require.NoError(t, err)
	require.Equal(t, pullSDKObservation{workerID: workerAID, method: "WaitFor"}, awaitPullSDKObservation(t, observations))
	require.Equal(t, pullSDKObservation{workerID: workerAID, method: "Execute"}, awaitPullSDKObservation(t, observations))
	startPullSDKWorker(t, []dex.Flow{workerBFlow, pullSDKProbeFlow{workerID: workerBID}}, workerBID)
	require.Equal(t, workerBID, runPullSDKProbe(t, client))
	close(firstStepGate)
	require.Equal(t, pullSDKObservation{workerID: workerAID, method: "SecondWaitFor"}, awaitPullSDKObservation(t, observations))
	require.Equal(t, pullSDKObservation{workerID: workerAID, method: "SecondExecute"}, awaitPullSDKObservation(t, observations))
	var rpcWorkerID string
	require.NoError(t, client.InvokeRPC(integrationContext(t), flowID, pullSDKFlow{}.GetWorker, nil, &rpcWorkerID))
	require.Equal(t, workerAID, rpcWorkerID)
	require.Equal(t, pullSDKObservation{workerID: workerAID, method: "RPC"}, awaitPullSDKObservation(t, observations))
	stopResult := make(chan error, 1)
	stopStarted := make(chan struct{})
	go func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		close(stopStarted)
		stopResult <- workerA.Stop(stopCtx)
	}()
	<-stopStarted
	require.Equal(t, workerBID, runPullSDKProbe(t, client))
	select {
	case stopErr := <-stopResult:
		t.Fatalf("Worker Stop returned before the active Execute finished: %v", stopErr)
	default:
	}
	close(secondStepGate)
	require.NoError(t, <-stopResult)
	result, err := client.WaitForFlow(integrationContext(t), flowID, dex.WaitForFlowOptions{NeedsResults: true})
	require.NoError(t, err)
	require.Equal(t, dex.FlowCompleted, result.Status)
	var output string
	require.NoError(t, result.DecodeSingleOutput(&output))
	require.Equal(t, "completed", output)
}

func TestPullSDKUnknownRPCRedispatchEndToEnd(t *testing.T) {
	observations := make(chan pullSDKObservation, 16)
	firstStepGate := make(chan struct{})
	secondStepGate := make(chan struct{})
	oldWorkerID := "pull-go-old-" + uuid.NewString()
	newWorkerID := "pull-go-new-" + uuid.NewString()
	oldFlow := pullSDKFlow{workerID: oldWorkerID, observations: observations, firstStepGate: firstStepGate, secondStepGate: secondStepGate, omitWorkerRPC: true}
	newFlow := pullSDKFlow{workerID: newWorkerID, observations: observations, firstStepGate: firstStepGate, secondStepGate: secondStepGate}
	startPullSDKWorker(t, []dex.Flow{oldFlow}, oldWorkerID)
	clientRegistry, err := dex.NewRegistry([]dex.Flow{newFlow})
	require.NoError(t, err)
	client, err := dex.NewClient(clientRegistry, newPullSDKCache(t), dex.ClientOptions{FlowServiceAddress: flowServiceAddress(), WorkerTarget: &dex.WorkerTarget{Pull: true}})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	flowID := newFlowID(t, "pull-go-unknown-rpc")
	_, err = client.StartFlow(integrationContext(t), newFlow, flowID, "completed", dex.StartFlowOptions{})
	require.NoError(t, err)
	require.Equal(t, pullSDKObservation{workerID: oldWorkerID, method: "WaitFor"}, awaitPullSDKObservation(t, observations))
	require.Equal(t, pullSDKObservation{workerID: oldWorkerID, method: "Execute"}, awaitPullSDKObservation(t, observations))
	startPullSDKWorker(t, []dex.Flow{newFlow}, newWorkerID)
	var rpcWorkerID string
	require.NoError(t, client.InvokeRPC(integrationContext(t), flowID, newFlow.GetWorker, nil, &rpcWorkerID))
	require.Equal(t, newWorkerID, rpcWorkerID)
	require.Equal(t, pullSDKObservation{workerID: newWorkerID, method: "RPC"}, awaitPullSDKObservation(t, observations))
	close(firstStepGate)
	close(secondStepGate)
	result, err := client.WaitForFlow(integrationContext(t), flowID, dex.WaitForFlowOptions{NeedsResults: true})
	require.NoError(t, err)
	require.Equal(t, dex.FlowCompleted, result.Status)
}

func runPullSDKProbe(t *testing.T, client *dex.Client) string {
	t.Helper()
	flowID := newFlowID(t, "pull-go-probe")
	_, err := client.StartFlow(integrationContext(t), pullSDKProbeFlow{}, flowID, "probe", dex.StartFlowOptions{})
	require.NoError(t, err)
	result, err := client.WaitForFlow(integrationContext(t), flowID, dex.WaitForFlowOptions{NeedsResults: true})
	require.NoError(t, err)
	var workerID string
	require.NoError(t, result.DecodeSingleOutput(&workerID))
	return workerID
}

func startPullSDKWorker(t *testing.T, flows []dex.Flow, workerID string) *dex.Worker {
	t.Helper()
	registry, err := dex.NewRegistry(flows)
	require.NoError(t, err)
	cache := newPullSDKCache(t)
	worker, err := dex.NewWorker(registry, cache, dex.WorkerOptions{
		FlowServiceAddress: flowServiceAddress(),
		Pull:               &dex.PullWorkerOptions{AppWorkerID: workerID, MaxConcurrentTasks: 4},
	})
	require.NoError(t, err)
	result := make(chan error, 1)
	go func() { result <- worker.Start() }()
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		require.NoError(t, worker.Stop(stopCtx))
		require.NoError(t, <-result)
	})
	return worker
}

func newPullSDKCache(t *testing.T) *blobcache.Cache {
	t.Helper()
	cache, err := blobcache.New(&blobcache.Config{Dir: t.TempDir(), MaxBytes: 64 << 20})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cache.Close()) })
	return cache
}

func awaitPullSDKObservation(t *testing.T, observations <-chan pullSDKObservation) pullSDKObservation {
	t.Helper()
	select {
	case observation := <-observations:
		return observation
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for pull Worker handler")
		return pullSDKObservation{}
	}
}
