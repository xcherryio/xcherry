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
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/superdurable/dex/blob-cache-go/blobcache"
	"github.com/superdurable/dex/sdk-go/dex"
)

// gRPC's own default receive limit, which these tests exceed.
const grpcDefaultReceiveLimitBytes = 4 << 20

var (
	messageSizeTextNames = []string{"a", "b", "c"}
	messageSizeTexts     = map[string]dex.Attribute[string]{
		"a": dex.DefineAttribute[string]("message-size-text-a"),
		"b": dex.DefineAttribute[string]("message-size-text-b"),
		"c": dex.DefineAttribute[string]("message-size-text-c"),
	}
	messageSizeGenerated = dex.DefineChannel[string]("message-size-generated")
)

type messageSizeFlow struct {
	dex.FlowDefaults
}

type messageSizeTextInput struct {
	Name  string
	Bytes int
}

func (messageSizeFlow) GetSteps() []dex.StepDef {
	return []dex.StepDef{
		dex.DefineStartStep(messageSizeReadStep{}),
		dex.DefineStep(messageSizeOutputStep{}),
	}
}

func (flow messageSizeFlow) GetRPCs() []dex.RPCDef {
	return []dex.RPCDef{
		dex.DefineRPC(flow.EchoText, nil),
		dex.DefineRPC(flow.GenerateText, nil),
	}
}

func (messageSizeFlow) GetPersistenceSchema() dex.PersistenceSchema {
	attributes := make([]dex.AttributeDef, 0, len(messageSizeTextNames))
	for _, name := range messageSizeTextNames {
		attributes = append(attributes, messageSizeTexts[name])
	}
	return dex.PersistenceSchema{
		Attributes: attributes,
		Channels:   []dex.ChannelDef{messageSizeGenerated},
	}
}

func (messageSizeFlow) EchoText(_ dex.Context, text string) (*dex.RPCResult[string], error) {
	return &dex.RPCResult[string]{Output: text}, nil
}

func (messageSizeFlow) GenerateText(
	ctx dex.Context,
	input messageSizeTextInput,
) (*dex.RPCResult[dex.None], error) {
	if err := messageSizeTexts[input.Name].Set(ctx, strings.Repeat(input.Name, input.Bytes)); err != nil {
		return nil, err
	}
	if err := messageSizeGenerated.Publish(ctx, input.Name); err != nil {
		return nil, err
	}
	return &dex.RPCResult[dex.None]{}, nil
}

type messageSizeReadStep struct {
	dex.StepDefaults
}

func (messageSizeReadStep) WaitFor(dex.Context, dex.None) (*dex.Wait, error) {
	return dex.Until(messageSizeGenerated.ForN(len(messageSizeTextNames))), nil
}

func (messageSizeReadStep) Execute(ctx dex.Context, _ dex.None) (*dex.StepDecision, error) {
	movements := make([]dex.StepMovement, 0, len(messageSizeTextNames))
	for _, name := range messageSizeTextNames {
		if _, err := messageSizeTexts[name].Get(ctx); err != nil {
			return nil, fmt.Errorf("read text %q: %w", name, err)
		}
		movements = append(movements, dex.MovementOf(messageSizeOutputStep{}, name))
	}
	return dex.GoToMany(movements...), nil
}

type messageSizeOutputStep struct {
	dex.StepDefaultsNoWaitFor[string]
}

func (messageSizeOutputStep) Execute(ctx dex.Context, name string) (*dex.StepDecision, error) {
	text, err := messageSizeTexts[name].Get(ctx)
	if err != nil {
		return nil, err
	}
	return dex.GracefulComplete(text), nil
}

func TestMessageSizeDefaultLimitCarriesValuesAboveGrpcDefault(t *testing.T) {
	ctx := integrationContext(t)
	flowID := newFlowID(t, "message-size-default")
	_, err := integClient.StartFlow(ctx, messageSizeFlow{}, flowID, nil, dex.StartFlowOptions{})
	require.NoError(t, err)

	text := strings.Repeat("x", grpcDefaultReceiveLimitBytes+(2<<20))
	var echoed string
	require.NoError(t, integClient.InvokeRPC(ctx, flowID, messageSizeFlow{}.EchoText, text, &echoed))
	require.Equal(t, text, echoed)

	oversizedBytes := grpcDefaultReceiveLimitBytes + (1 << 20)
	textBytes := map[string]int{"a": oversizedBytes, "b": 1024, "c": 1024}
	for _, name := range messageSizeTextNames {
		require.NoError(t, integClient.InvokeRPC(ctx, flowID, messageSizeFlow{}.GenerateText,
			messageSizeTextInput{Name: name, Bytes: textBytes[name]}, nil))
	}
	requireMessageSizeOutputs(t, waitForFlow(t, flowID, true), textBytes)

	smallClient := newMessageSizeClient(t, grpcDefaultReceiveLimitBytes, nil)
	_, err = smallClient.WaitForFlow(ctx, flowID, dex.WaitForFlowOptions{NeedsResults: true})
	var tooLarge *dex.BlobMessageTooLargeError
	require.ErrorAs(t, err, &tooLarge)
	require.Equal(t, flowID, tooLarge.FlowID)
	require.NotEmpty(t, tooLarge.BlobID)
}

func TestMessageSizeHydrationSplitsBlobBatchesAboveLimit(t *testing.T) {
	skipIfServerZD(t, "push Workers")
	if isPullWorkerMode {
		t.Skip("push-only Worker listener message-size limit")
	}
	ctx := integrationContext(t)
	limitBytes := grpcDefaultReceiveLimitBytes
	worker := startMessageSizeWorker(t, limitBytes)
	client := newMessageSizeClient(t, limitBytes, worker.WorkerTarget())
	flowID := newFlowID(t, "message-size-split")
	_, err := client.StartFlow(ctx, messageSizeFlow{}, flowID, nil, dex.StartFlowOptions{})
	require.NoError(t, err)

	textBytes := map[string]int{}
	for _, name := range messageSizeTextNames {
		textBytes[name] = limitBytes * 3 / 8
		require.NoError(t, client.InvokeRPC(ctx, flowID, messageSizeFlow{}.GenerateText,
			messageSizeTextInput{Name: name, Bytes: textBytes[name]}, nil))
	}
	result, err := client.WaitForFlow(ctx, flowID, dex.WaitForFlowOptions{NeedsResults: true})
	require.NoError(t, err)
	requireMessageSizeOutputs(t, result, textBytes)
}

func requireMessageSizeOutputs(t *testing.T, result dex.FlowResult, textBytes map[string]int) {
	t.Helper()
	require.Equal(t, dex.FlowCompleted, result.Status)
	require.Len(t, result.Completions, len(textBytes))
	for _, completion := range result.Completions {
		var text string
		require.NoError(t, completion.Output.Decode(&text))
		require.NotEmpty(t, text)
		name := text[:1]
		require.Equal(t, strings.Repeat(name, textBytes[name]), text)
	}
}

// The cache rejects every hydrated text, so each read issues LoadBlobs.
func newUncachedMessageSizeCache(t *testing.T) *blobcache.Cache {
	t.Helper()
	cacheDir := t.TempDir()
	cache, err := blobcache.New(&blobcache.Config{Dir: cacheDir, MaxBytes: 1 << 10})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cache.Close()) })
	return cache
}

func newMessageSizeRegistry(t *testing.T) *dex.Registry {
	t.Helper()
	registry, err := dex.NewRegistry([]dex.Flow{messageSizeFlow{}})
	require.NoError(t, err)
	return registry
}

func newMessageSizeClient(t *testing.T, limitBytes int, workerTarget *dex.WorkerTarget) *dex.Client {
	t.Helper()
	client, err := dex.NewClient(newMessageSizeRegistry(t), newUncachedMessageSizeCache(t), dex.ClientOptions{
		FlowServiceAddress:  flowServiceAddress(),
		WorkerTarget:        workerTarget,
		GrpcMaxMessageBytes: limitBytes,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	return client
}

func startMessageSizeWorker(t *testing.T, limitBytes int) *dex.Worker {
	t.Helper()
	port, err := availablePort()
	require.NoError(t, err)
	worker, err := dex.NewWorker(newMessageSizeRegistry(t), newUncachedMessageSizeCache(t), dex.WorkerOptions{
		BindAddress:         net.JoinHostPort("127.0.0.1", port),
		FlowServiceAddress:  flowServiceAddress(),
		WorkerTarget:        dex.WorkerTarget{Address: net.JoinHostPort(workerHost(), port)},
		GrpcMaxMessageBytes: limitBytes,
	})
	require.NoError(t, err)
	workerResult := make(chan error, 1)
	go func() { workerResult <- worker.Start() }()
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		require.NoError(t, worker.Stop(stopCtx))
		require.NoError(t, <-workerResult)
	})
	require.Eventually(t, func() bool {
		connection, err := net.DialTimeout("tcp", worker.WorkerTarget().Address, 100*time.Millisecond)
		if err != nil {
			return false
		}
		return connection.Close() == nil
	}, time.Minute, 250*time.Millisecond)
	return worker
}
