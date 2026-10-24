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

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/superdurable/dex/sdk-go/dex"
)

type cancellationScenario string

const (
	cancelHeartbeatExecute cancellationScenario = "heartbeat-execute"
	cancelHeartbeatWaitFor cancellationScenario = "heartbeat-wait-for"
	cancelLocalExecute     cancellationScenario = "local-execute"
	cancelLocalFallback    cancellationScenario = "local-timeout-fallback"
	cancelNoHeartbeat      cancellationScenario = "no-heartbeat"
	cancelGlobalSelector   cancellationScenario = "global-selector"
	cancelSiblingSelector  cancellationScenario = "sibling-selector"
)

const (
	cancellationHandlerTimeout = 15 * time.Second
	cancellationHeartbeat      = 10 * time.Second
	cancellationWinnerDelay    = 3 * time.Second
)

var (
	cancellationLateWrite = dex.DefineAttribute[string]("go-cancellation-late-write")
	cancellationStates    sync.Map
)

type cancellationState struct {
	scenario                cancellationScenario
	blockingStarted         chan struct{}
	cancellationObserved    chan struct{}
	lateHandlerReturned     chan struct{}
	selectorWaitsRegistered chan struct{}
	startedOnce             sync.Once
	canceledOnce            sync.Once
	returnedOnce            sync.Once
	selectorOnce            sync.Once
	selectorWaitCount       atomic.Int32
	handlerCanceled         atomic.Bool
	recoveryRan             atomic.Bool
	firstSelectorExecuted   atomic.Bool
	secondSelectorExecuted  atomic.Bool
	blockingInvocations     atomic.Int32
}

func newCancellationState(scenario cancellationScenario) *cancellationState {
	return &cancellationState{
		scenario:                scenario,
		blockingStarted:         make(chan struct{}),
		cancellationObserved:    make(chan struct{}),
		lateHandlerReturned:     make(chan struct{}),
		selectorWaitsRegistered: make(chan struct{}),
	}
}

func cancellationStateFor(ctx dex.Context) (*cancellationState, error) {
	value, found := cancellationStates.Load(ctx.FlowID())
	if !found {
		return nil, fmt.Errorf("missing cancellation state for %s", ctx.FlowID())
	}
	return value.(*cancellationState), nil
}

type stepCancellationFlow struct {
	dex.FlowDefaults
}

func (stepCancellationFlow) GetSteps() []dex.StepDef {
	return []dex.StepDef{
		dex.DefineStartStep(cancellationStartStep{}),
		dex.DefineStep(cancellationBlockingExecuteStep{}),
		dex.DefineStep(cancellationBlockingWaitForStep{}),
		dex.DefineStep(cancellationWinnerStep{}),
		dex.DefineStep(cancellationRecoveryStep{}),
		dex.DefineStep(cancellationFinalStep{}),
		dex.DefineStep(cancellationFirstParentStep{}),
		dex.DefineStep(cancellationSecondParentStep{}),
		dex.DefineStep(cancellationSelectorWinnerStep{}),
		dex.DefineStep(cancellationSelectorWaitingStep{}),
	}
}

func (stepCancellationFlow) GetPersistenceSchema() dex.PersistenceSchema {
	return dex.PersistenceSchema{Attributes: []dex.AttributeDef{cancellationLateWrite}}
}

type cancellationStartStep struct {
	dex.StepDefaultsNoWaitFor[cancellationScenario]
}

func (cancellationStartStep) Execute(
	_ dex.Context,
	scenario cancellationScenario,
) (*dex.StepDecision, error) {
	switch scenario {
	case cancelHeartbeatWaitFor:
		return dex.GoToMany(
			dex.MovementOf(cancellationBlockingWaitForStep{}, scenario),
			dex.MovementOf(cancellationWinnerStep{}, scenario),
		), nil
	case cancelGlobalSelector, cancelSiblingSelector:
		return dex.GoToMany(
			dex.MovementOf(cancellationFirstParentStep{}, scenario),
			dex.MovementOf(cancellationSecondParentStep{}, scenario),
		), nil
	default:
		return dex.GoToMany(
			dex.MovementOf(
				cancellationBlockingExecuteStep{},
				scenario,
				dex.WithStepOptions(cancellationExecuteOptions(scenario)),
			),
			dex.MovementOf(cancellationWinnerStep{}, scenario),
		), nil
	}
}

func cancellationExecuteOptions(scenario cancellationScenario) *dex.StepOptions {
	options := &dex.StepOptions{ExecuteDurability: dex.StepDurabilitySync}
	switch scenario {
	case cancelHeartbeatExecute:
		options.HeartbeatTimeout = cancellationHeartbeat
	case cancelLocalExecute:
		options.ExecuteDurability = dex.StepDurabilityAsync
	case cancelLocalFallback:
		options.ExecuteDurability = dex.StepDurabilityAsync
		options.HeartbeatTimeout = cancellationHeartbeat
	}
	return options
}

type cancellationBlockingExecuteStep struct {
	dex.StepDefaultsNoWaitFor[cancellationScenario]
}

func (cancellationBlockingExecuteStep) Execute(
	ctx dex.Context,
	scenario cancellationScenario,
) (*dex.StepDecision, error) {
	state, err := cancellationStateFor(ctx)
	if err != nil {
		return nil, err
	}
	state.blockingInvocations.Add(1)
	state.startedOnce.Do(func() { close(state.blockingStarted) })
	defer state.returnedOnce.Do(func() { close(state.lateHandlerReturned) })
	duration := 10 * time.Second
	if scenario == cancelNoHeartbeat {
		duration = 7 * time.Second
	}
	shouldHeartbeat := scenario == cancelHeartbeatExecute || scenario == cancelLocalFallback
	if err := waitForHandlerCancellation(ctx, state, duration, shouldHeartbeat); err != nil {
		return nil, err
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if err := cancellationLateWrite.Set(ctx, "late"); err != nil {
		return nil, err
	}
	return dex.GoTo(cancellationRecoveryStep{}, scenario), nil
}

func (cancellationBlockingExecuteStep) GetStepOptions() *dex.StepOptions {
	return &dex.StepOptions{
		ExecuteMethodTimeout: cancellationHandlerTimeout,
		ExecuteFailure: dex.ProceedToOnExecuteFailure(
			cancellationRecoveryStep{},
			nil,
		),
	}
}

type cancellationBlockingWaitForStep struct {
	dex.DefaultStepType
}

func (cancellationBlockingWaitForStep) WaitFor(
	ctx dex.Context,
	_ cancellationScenario,
) (*dex.Wait, error) {
	state, err := cancellationStateFor(ctx)
	if err != nil {
		return nil, err
	}
	state.blockingInvocations.Add(1)
	state.startedOnce.Do(func() { close(state.blockingStarted) })
	defer state.returnedOnce.Do(func() { close(state.lateHandlerReturned) })
	if err := waitForHandlerCancellation(ctx, state, 10*time.Second, true); err != nil {
		return nil, err
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	return dex.SkipWaitImmediately(), nil
}

func (cancellationBlockingWaitForStep) Execute(
	ctx dex.Context,
	_ cancellationScenario,
) (*dex.StepDecision, error) {
	state, err := cancellationStateFor(ctx)
	if err != nil {
		return nil, err
	}
	state.recoveryRan.Store(true)
	return dex.ForceFail("canceled WaitFor execution continued"), nil
}

func (cancellationBlockingWaitForStep) GetStepOptions() *dex.StepOptions {
	return &dex.StepOptions{
		WaitForMethodTimeout: cancellationHandlerTimeout,
		HeartbeatTimeout:     cancellationHeartbeat,
		WaitForFailure:       dex.ProceedOnWaitForFailure,
		WaitForDurability:    dex.StepDurabilitySync,
	}
}

func waitForHandlerCancellation(
	ctx dex.Context,
	state *cancellationState,
	duration time.Duration,
	shouldHeartbeat bool,
) error {
	deadline := time.NewTimer(duration)
	defer deadline.Stop()
	if !shouldHeartbeat {
		select {
		case <-ctx.Done():
			recordHandlerCancellation(state)
		case <-deadline.C:
		}
		return nil
	}
	heartbeats := time.NewTicker(100 * time.Millisecond)
	defer heartbeats.Stop()
	for {
		select {
		case <-ctx.Done():
			recordHandlerCancellation(state)
			return nil
		case <-deadline.C:
			return nil
		case <-heartbeats.C:
			if err := ctx.RecordHeartbeat(nil); err != nil {
				if ctx.Err() != nil {
					recordHandlerCancellation(state)
					return nil
				}
				return err
			}
		}
	}
}

func recordHandlerCancellation(state *cancellationState) {
	if state.scenario == cancelLocalFallback && state.blockingInvocations.Load() == 1 {
		return
	}
	state.handlerCanceled.Store(true)
	state.canceledOnce.Do(func() { close(state.cancellationObserved) })
}

type cancellationWinnerStep struct {
	dex.StepDefaults
}

func (cancellationWinnerStep) WaitFor(
	_ dex.Context,
	scenario cancellationScenario,
) (*dex.Wait, error) {
	if scenario == cancelLocalExecute {
		return dex.SkipWaitImmediately(), nil
	}
	if scenario == cancelLocalFallback {
		return dex.Until(dex.Timer(9 * time.Second)), nil
	}
	return dex.Until(dex.Timer(cancellationWinnerDelay)), nil
}

func (cancellationWinnerStep) Execute(
	ctx dex.Context,
	scenario cancellationScenario,
) (*dex.StepDecision, error) {
	if scenario == cancelLocalExecute {
		state, err := cancellationStateFor(ctx)
		if err != nil {
			return nil, err
		}
		select {
		case <-state.blockingStarted:
		case <-time.After(10 * time.Second):
			return nil, fmt.Errorf("local loser did not start")
		}
		time.Sleep(time.Second)
	}
	selected := dex.StepSelector(cancellationBlockingExecuteStep{})
	if scenario == cancelHeartbeatWaitFor {
		selected = cancellationBlockingWaitForStep{}
	}
	return dex.GoTo(cancellationFinalStep{}, scenario).CancelSteps(selected), nil
}

type cancellationRecoveryStep struct {
	dex.StepDefaultsNoWaitFor[cancellationScenario]
}

func (cancellationRecoveryStep) Execute(
	ctx dex.Context,
	_ cancellationScenario,
) (*dex.StepDecision, error) {
	state, err := cancellationStateFor(ctx)
	if err != nil {
		return nil, err
	}
	state.recoveryRan.Store(true)
	return dex.ForceFail("canceled execution reached recovery"), nil
}

type cancellationFinalStep struct {
	dex.StepDefaults
}

func (cancellationFinalStep) WaitFor(
	_ dex.Context,
	_ cancellationScenario,
) (*dex.Wait, error) {
	return dex.Until(dex.Timer(time.Second)), nil
}

func (cancellationFinalStep) Execute(
	ctx dex.Context,
	scenario cancellationScenario,
) (*dex.StepDecision, error) {
	if scenario != cancelGlobalSelector && scenario != cancelSiblingSelector {
		_, err := cancellationLateWrite.Get(ctx)
		var notFound *dex.AttributeNotFoundError
		if !errors.As(err, &notFound) {
			return nil, fmt.Errorf("canceled execution committed its late write: %w", err)
		}
	}
	return dex.GracefulComplete(string(scenario)), nil
}

type cancellationFirstParentStep struct {
	dex.StepDefaultsNoWaitFor[cancellationScenario]
}

func (cancellationFirstParentStep) Execute(
	_ dex.Context,
	scenario cancellationScenario,
) (*dex.StepDecision, error) {
	return dex.GoToMany(
		dex.MovementOf(cancellationSelectorWinnerStep{}, scenario),
		dex.MovementOf(cancellationSelectorWaitingStep{}, "first"),
	), nil
}

type cancellationSecondParentStep struct {
	dex.StepDefaultsNoWaitFor[cancellationScenario]
}

func (cancellationSecondParentStep) Execute(
	_ dex.Context,
	scenario cancellationScenario,
) (*dex.StepDecision, error) {
	return dex.GoTo(cancellationSelectorWaitingStep{}, "second"), nil
}

type cancellationSelectorWinnerStep struct {
	dex.StepDefaults
}

func (cancellationSelectorWinnerStep) WaitFor(
	_ dex.Context,
	_ cancellationScenario,
) (*dex.Wait, error) {
	return dex.Until(dex.Timer(time.Second)), nil
}

func (cancellationSelectorWinnerStep) Execute(
	ctx dex.Context,
	scenario cancellationScenario,
) (*dex.StepDecision, error) {
	state, err := cancellationStateFor(ctx)
	if err != nil {
		return nil, err
	}
	select {
	case <-state.selectorWaitsRegistered:
	case <-time.After(10 * time.Second):
		return nil, fmt.Errorf("selector Steps did not reach waiting")
	}
	decision := dex.GoTo(cancellationFinalStep{}, scenario)
	if scenario == cancelGlobalSelector {
		return decision.CancelSteps(cancellationSelectorWaitingStep{}), nil
	}
	return decision.CancelSiblingSteps(cancellationSelectorWaitingStep{}), nil
}

type cancellationSelectorWaitingStep struct {
	dex.StepDefaults
}

func (cancellationSelectorWaitingStep) WaitFor(
	ctx dex.Context,
	input string,
) (*dex.Wait, error) {
	state, err := cancellationStateFor(ctx)
	if err != nil {
		return nil, err
	}
	if state.selectorWaitCount.Add(1) == 2 {
		state.selectorOnce.Do(func() { close(state.selectorWaitsRegistered) })
	}
	duration := 2 * time.Second
	if input == "first" || state.scenario == cancelGlobalSelector {
		duration = 30 * time.Second
	}
	return dex.Until(dex.Timer(duration)), nil
}

func (cancellationSelectorWaitingStep) Execute(
	ctx dex.Context,
	input string,
) (*dex.StepDecision, error) {
	state, err := cancellationStateFor(ctx)
	if err != nil {
		return nil, err
	}
	if input == "first" {
		state.firstSelectorExecuted.Store(true)
	} else {
		state.secondSelectorExecuted.Store(true)
	}
	return dex.DeadEnd(), nil
}

func TestStepCancellation(t *testing.T) {
	scenarios := []cancellationScenario{
		cancelHeartbeatExecute,
		cancelHeartbeatWaitFor,
		cancelLocalExecute,
		cancelLocalFallback,
		cancelNoHeartbeat,
		cancelGlobalSelector,
		cancelSiblingSelector,
	}
	for _, scenario := range scenarios {
		t.Run(string(scenario), func(t *testing.T) {
			if scenario != cancelGlobalSelector && scenario != cancelSiblingSelector {
				// C21: server-zd rejects Step failure policies.
				skipIfServerZD(t, "Step failure policies")
			}
			runCancellationScenario(t, scenario)
		})
	}
}

func runCancellationScenario(t *testing.T, scenario cancellationScenario) {
	t.Helper()
	flowID := newFlowID(t, "go-cancellation-"+string(scenario))
	state := newCancellationState(scenario)
	cancellationStates.Store(flowID, state)
	t.Cleanup(func() { cancellationStates.Delete(flowID) })
	_, err := integClient.StartFlow(
		integrationContext(t),
		stepCancellationFlow{},
		flowID,
		scenario,
		dex.StartFlowOptions{},
	)
	require.NoError(t, err)

	if scenario != cancelGlobalSelector && scenario != cancelSiblingSelector {
		requireChannel(t, state.blockingStarted, 10*time.Second)
		stepType := dex.GetFinalStepType(cancellationBlockingExecuteStep{})
		if scenario == cancelHeartbeatWaitFor {
			stepType = dex.GetFinalStepType(cancellationBlockingWaitForStep{})
		}
		require.NoError(t, integClient.WaitForStepCompletion(
			integrationContext(t),
			flowID,
			dex.StepExecutionID{StepType: stepType},
			dex.WaitForStepCompletionOptions{RequestID: uuid.NewString()},
		))
	}

	result := waitForFlow(t, flowID, true)
	require.Equal(t, dex.FlowCompleted, result.Status)
	require.Len(t, result.Completions, 1)
	var output string
	require.NoError(t, result.Completions[0].Output.Decode(&output))
	require.Equal(t, string(scenario), output)

	switch scenario {
	case cancelGlobalSelector:
		require.False(t, state.firstSelectorExecuted.Load())
		require.False(t, state.secondSelectorExecuted.Load())
	case cancelSiblingSelector:
		require.False(t, state.firstSelectorExecuted.Load())
		require.True(t, state.secondSelectorExecuted.Load())
	case cancelNoHeartbeat:
		require.False(t, state.handlerCanceled.Load())
		select {
		case <-state.lateHandlerReturned:
			t.Fatal("handler returned before Flow completed")
		default:
		}
		requireChannel(t, state.lateHandlerReturned, 8*time.Second)
	default:
		requireChannel(t, state.lateHandlerReturned, 15*time.Second)
	}
	if scenario != cancelGlobalSelector && scenario != cancelSiblingSelector {
		require.False(t, state.recoveryRan.Load())
		expectedInvocations := int32(1)
		if scenario == cancelLocalExecute || scenario == cancelLocalFallback {
			expectedInvocations = 2
		}
		require.Equal(t, expectedInvocations, state.blockingInvocations.Load())
	}
}

func requireChannel(t *testing.T, channel <-chan struct{}, timeout time.Duration) {
	t.Helper()
	select {
	case <-channel:
	case <-time.After(timeout):
		t.Fatalf("condition was not observed within %s", timeout)
	}
}
