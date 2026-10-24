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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/superdurable/dex/sdk-go/dex"
	"github.com/superdurable/dex/sdk-go/gen/dexpb"
)

var heartbeatRecoveryProgress = dex.DefineStream[string]("heartbeat-recovery-progress", 1<<20)
var asyncHeartbeatProgress = dex.DefineStream[string]("async-heartbeat-progress", 1<<20)

type heartbeatRecoveryFlow struct {
	dex.FlowDefaults
}

func (heartbeatRecoveryFlow) GetSteps() []dex.StepDef {
	return []dex.StepDef{dex.DefineStartStep(heartbeatRecoveryStep{})}
}

func (heartbeatRecoveryFlow) GetPersistenceSchema() dex.PersistenceSchema {
	return dex.PersistenceSchema{Streams: []dex.StreamDef{heartbeatRecoveryProgress}}
}

type heartbeatRecoveryStep struct {
	dex.StepDefaultsNoWaitFor[struct{}]
}

func (heartbeatRecoveryStep) GetStepOptions() *dex.StepOptions {
	return &dex.StepOptions{
		ExecuteRetry: &dex.RetryPolicy{
			InitialInterval: time.Second,
			MaximumAttempts: 3,
		},
		ExecuteDurability: dex.StepDurabilitySync,
	}
}

func (heartbeatRecoveryStep) Execute(
	ctx dex.Context,
	_ struct{},
) (*dex.StepDecision, error) {
	switch ctx.Attempt() {
	case 1:
		if found, err := ctx.GetLastHeartbeatValue(new(string)); err != nil || found {
			return nil, fmt.Errorf("unexpected initial heartbeat: found=%t: %w", found, err)
		}
		if err := ctx.RecordHeartbeat("checkpoint-1"); err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("retry after checkpoint")
	case 2:
		var checkpoint string
		found, err := ctx.GetLastHeartbeatValue(&checkpoint)
		if err != nil || !found || checkpoint != "checkpoint-1" {
			return nil, fmt.Errorf("heartbeat was not restored: found=%t value=%q: %w", found, checkpoint, err)
		}
		if err := ctx.RecordHeartbeat(nil); err != nil {
			return nil, err
		}
		if err := heartbeatRecoveryProgress.Write(ctx, "after-clear"); err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("retry after clearing checkpoint")
	case 3:
		checkpoint := "unchanged"
		found, err := ctx.GetLastHeartbeatValue(&checkpoint)
		if err != nil || found || checkpoint != "unchanged" {
			return nil, fmt.Errorf("cleared heartbeat was restored: found=%t value=%q: %w", found, checkpoint, err)
		}
		if err := heartbeatRecoveryProgress.Write(ctx, "completed"); err != nil {
			return nil, err
		}
		return dex.GracefulComplete("recovered"), nil
	default:
		return nil, fmt.Errorf("unexpected attempt %d", ctx.Attempt())
	}
}

type asyncHeartbeatFlow struct {
	dex.FlowDefaults
}

func (asyncHeartbeatFlow) GetSteps() []dex.StepDef {
	return []dex.StepDef{dex.DefineStartStep(asyncHeartbeatStep{})}
}

func (asyncHeartbeatFlow) GetPersistenceSchema() dex.PersistenceSchema {
	return dex.PersistenceSchema{Streams: []dex.StreamDef{asyncHeartbeatProgress}}
}

type asyncHeartbeatStep struct {
	dex.StepDefaultsNoWaitFor[struct{}]
}

func (asyncHeartbeatStep) GetStepOptions() *dex.StepOptions {
	return &dex.StepOptions{
		ExecuteRetry: &dex.RetryPolicy{
			InitialInterval: time.Second,
			MaximumAttempts: 4,
			TotalDuration:   30 * time.Second,
		},
		ExecuteDurability: dex.StepDurabilityAsync,
	}
}

func (asyncHeartbeatStep) Execute(
	ctx dex.Context,
	_ struct{},
) (*dex.StepDecision, error) {
	checkpoint := "unchanged"
	found, err := ctx.GetLastHeartbeatValue(&checkpoint)
	if err != nil {
		return nil, err
	}
	serverEngine, err := integrationServerEngine()
	if err != nil {
		return nil, err
	}
	// C2: server-zd persists ASYNC heartbeat checkpoints from the first attempt.
	isServerZD := serverEngine == dexpb.ServerEngine_SERVER_ENGINE_ZD
	shouldFindHeartbeat := (isServerZD && ctx.Attempt() > 1) || (isPullWorkerMode && ctx.Attempt() > 2)
	expectedCheckpoint := "unchanged"
	if shouldFindHeartbeat {
		expectedCheckpoint = fmt.Sprintf("attempt-%d", ctx.Attempt()-1)
	}
	if found != shouldFindHeartbeat || checkpoint != expectedCheckpoint {
		return nil, fmt.Errorf("unexpected heartbeat on attempt %d: found=%t value=%q, want found=%t value=%q", ctx.Attempt(), found, checkpoint, shouldFindHeartbeat, expectedCheckpoint)
	}
	if err := ctx.RecordHeartbeat(fmt.Sprintf("attempt-%d", ctx.Attempt())); err != nil {
		return nil, err
	}
	if err := asyncHeartbeatProgress.Write(ctx, fmt.Sprintf("attempt-%d", ctx.Attempt())); err != nil {
		return nil, err
	}
	if ctx.Attempt() < 4 {
		return nil, fmt.Errorf("retry attempt %d", ctx.Attempt())
	}
	return dex.GracefulComplete("async-recovered"), nil
}

func TestHeartbeatRecoveryAndExplicitClear(t *testing.T) {
	ctx := integrationContext(t)
	flowID := newFlowID(t, "heartbeat-recovery")
	_, err := integClient.StartFlow(ctx, heartbeatRecoveryFlow{}, flowID, struct{}{}, dex.StartFlowOptions{})
	require.NoError(t, err)

	result := waitForFlow(t, flowID, true)
	require.Equal(t, dex.FlowCompleted, result.Status)
	assertHeartbeatStreamMessages(
		t,
		ctx,
		flowID,
		heartbeatRecoveryProgress,
		[]string{"after-clear", "completed"},
	)
}

func TestAsyncLocalHeartbeatIsIgnoredAndStreamsAreRetained(t *testing.T) {
	ctx := integrationContext(t)
	flowID := newFlowID(t, "async-heartbeat")
	_, err := integClient.StartFlow(ctx, asyncHeartbeatFlow{}, flowID, struct{}{}, dex.StartFlowOptions{})
	require.NoError(t, err)

	result := waitForFlow(t, flowID, true)
	require.Equal(t, dex.FlowCompleted, result.Status, result.ErrorMessage)
	assertHeartbeatStreamMessages(t, ctx, flowID, asyncHeartbeatProgress, []string{
		"attempt-1",
		"attempt-2",
		"attempt-3",
		"attempt-4",
	})
}

func assertHeartbeatStreamMessages(
	t *testing.T,
	ctx context.Context,
	flowID string,
	stream dex.Stream[string],
	expected []string,
) {
	t.Helper()
	resumeToken := ""
	source := ""
	for _, expectedValue := range expected {
		var value string
		message, err := integClient.ReadStream(
			ctx,
			flowID,
			stream,
			resumeToken,
			&value,
		)
		require.NoError(t, err)
		require.Equal(t, expectedValue, value)
		if source == "" {
			source = message.Source
			require.NotEmpty(t, source)
			require.Equal(t, '#', rune(source[0]))
		} else {
			require.Equal(t, source, message.Source)
		}
		resumeToken = message.ResumeToken
	}
}
