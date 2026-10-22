// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package integ

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/superdurable/dex/sdk-go/dex"
)

const (
	stepErrorPlain        = "plain"
	stepErrorCustom       = "custom"
	stepErrorWrapped      = "wrapped"
	stepErrorAfterStream  = "after-stream"
	stepErrorMaxAttempts  = 2
	stepErrorRetryDelay   = time.Second
	stepErrorStepTimeout  = 10 * time.Second
	stepErrorElapsedLimit = stepErrorStepTimeout
)

var (
	stepErrorPhaseStream = dex.DefineStream[string]("step-error-phase", 1<<20)
	// stepErrorCallCounts counts Execute callbacks per Flow ID.
	stepErrorCallCounts sync.Map
)

type stepErrorProviderError struct {
	status    int
	requestID string
}

func (e *stepErrorProviderError) Error() string {
	return fmt.Sprintf("model provider HTTP %d (request %s)", e.status, e.requestID)
}

type stepErrorRetryFlow struct {
	dex.FlowDefaults
}

func (stepErrorRetryFlow) GetSteps() []dex.StepDef {
	return []dex.StepDef{dex.DefineStartStep(stepErrorRetryStep{})}
}

func (stepErrorRetryFlow) GetPersistenceSchema() dex.PersistenceSchema {
	return dex.PersistenceSchema{Streams: []dex.StreamDef{stepErrorPhaseStream}}
}

type stepErrorRetryStep struct {
	dex.StepDefaultsNoWaitFor[string]
}

func (stepErrorRetryStep) GetStepOptions() *dex.StepOptions {
	return &dex.StepOptions{
		ExecuteDurability:    dex.StepDurabilitySync,
		ExecuteMethodTimeout: stepErrorStepTimeout,
		ExecuteRetry: &dex.RetryPolicy{
			InitialInterval: stepErrorRetryDelay,
			MaximumInterval: stepErrorRetryDelay,
			MaximumAttempts: stepErrorMaxAttempts,
			TotalDuration:   30 * time.Second,
		},
	}
}

func (stepErrorRetryStep) Execute(ctx dex.Context, kind string) (*dex.StepDecision, error) {
	calls, _ := stepErrorCallCounts.LoadOrStore(ctx.FlowID(), &atomic.Int32{})
	calls.(*atomic.Int32).Add(1)
	var err error
	switch kind {
	case stepErrorPlain:
		err = errors.New("synthetic application failure")
	case stepErrorCustom:
		err = &stepErrorProviderError{status: 503, requestID: "req_mock"}
	case stepErrorWrapped:
		err = fmt.Errorf("call model provider: %w", &stepErrorProviderError{status: 429, requestID: "req_mock"})
	case stepErrorAfterStream:
		if writeErr := stepErrorPhaseStream.Write(ctx, "started"); writeErr != nil {
			return nil, writeErr
		}
		err = errors.New("synthetic application failure")
	default:
		return nil, fmt.Errorf("unknown step error kind %q", kind)
	}
	return nil, dex.ErrorWithStack(err)
}

// TestStepErrorBeforeFirstCommandUsesConfiguredAttempts guards against retrying handler errors as transport failures.
func TestStepErrorBeforeFirstCommandUsesConfiguredAttempts(t *testing.T) {
	for _, kind := range []string{stepErrorPlain, stepErrorCustom, stepErrorWrapped, stepErrorAfterStream} {
		t.Run(kind, func(t *testing.T) {
			ctx := integrationContext(t)
			flowID := newFlowID(t, "step-error-retry-"+kind)
			options := dex.StartFlowOptions{}
			if !isPullWorkerMode {
				// A headless target enables endpoint failover, which must not repeat handler errors.
				options.ConfigOverride = &dex.FlowConfig{WorkerTarget: &dex.WorkerTarget{
					Address:  integWorkerTargetAddress,
					Headless: true,
				}}
			}
			started := time.Now()
			_, err := integClient.StartFlow(ctx, stepErrorRetryFlow{}, flowID, kind, options)
			require.NoError(t, err)
			result := waitForFlow(t, flowID, false)
			elapsed := time.Since(started)

			require.Equal(t, dex.FlowFailed, result.Status)
			calls, found := stepErrorCallCounts.Load(flowID)
			require.True(t, found)
			require.Equal(t, int32(stepErrorMaxAttempts), calls.(*atomic.Int32).Load())
			require.Less(t, elapsed, stepErrorElapsedLimit, "the Step must fail before its Execute timeout")
		})
	}
}
