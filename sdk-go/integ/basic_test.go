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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/superdurable/dex/sdk-go/dex"
)

type basicFlow struct {
	emptyFlowSchema
}

func (basicFlow) GetSteps() []dex.StepDef {
	return []dex.StepDef{
		dex.DefineStartStep(basicFirstStep{}),
		dex.DefineStep(basicSecondStep{}),
	}
}

type basicFirstStep struct {
	dex.StepDefaults
}

func (basicFirstStep) WaitFor(ctx dex.Context, input int) (*dex.Wait, error) {
	if ctx.Attempt() < 1 || ctx.FirstAttemptAt().IsZero() {
		return nil, fmt.Errorf("invalid first-step attempt metadata")
	}
	if input < 0 {
		return nil, fmt.Errorf("input must not be negative")
	}
	return dex.SkipWaitImmediately(), nil
}

func (basicFirstStep) Execute(ctx dex.Context, input int) (*dex.StepDecision, error) {
	if ctx.Attempt() < 1 || ctx.FirstAttemptAt().IsZero() {
		return nil, fmt.Errorf("invalid first-step attempt metadata")
	}
	return dex.GoTo(basicSecondStep{}, input+1), nil
}

type basicSecondStep struct {
	dex.StepDefaultsNoWaitFor[int]
}

func (basicSecondStep) Execute(dex.Context, int) (*dex.StepDecision, error) {
	return dex.GracefulComplete(3), nil
}

type multiOutputFlow struct {
	emptyFlowSchema
}

func (multiOutputFlow) GetSteps() []dex.StepDef {
	return []dex.StepDef{
		dex.DefineStartStep(multiOutputStartStep{}),
		dex.DefineStep(multiOutputStringStep{}),
		dex.DefineStep(multiOutputIntStep{}),
	}
}

type multiOutputStartStep struct {
	dex.StepDefaultsNoWaitFor[struct{}]
}

func (multiOutputStartStep) Execute(dex.Context, struct{}) (*dex.StepDecision, error) {
	return dex.GoToMany(
		dex.MovementOf(multiOutputStringStep{}, struct{}{}),
		dex.MovementOf(multiOutputIntStep{}, struct{}{}),
	), nil
}

type multiOutputStringStep struct {
	dex.StepDefaultsNoWaitFor[struct{}]
}

func (multiOutputStringStep) Execute(dex.Context, struct{}) (*dex.StepDecision, error) {
	return dex.GracefulComplete("branch-one"), nil
}

type multiOutputIntStep struct {
	dex.StepDefaultsNoWaitFor[struct{}]
}

func (multiOutputIntStep) Execute(dex.Context, struct{}) (*dex.StepDecision, error) {
	return dex.GracefulComplete(42), nil
}

type proceedOnWaitForFailureFlow struct {
	emptyFlowSchema
}

func (proceedOnWaitForFailureFlow) GetSteps() []dex.StepDef {
	return []dex.StepDef{
		dex.DefineStartStep(proceedOnWaitForFailureFirstStep{}),
		dex.DefineStep(proceedOnWaitForFailureSecondStep{}),
	}
}

type proceedOnWaitForFailureFirstStep struct {
	dex.DefaultStepType
}

func (proceedOnWaitForFailureFirstStep) GetStepOptions() *dex.StepOptions {
	return &dex.StepOptions{
		WaitForRetry: &dex.RetryPolicy{
			InitialInterval: time.Second,
			MaximumAttempts: 2,
		},
		WaitForFailure: dex.ProceedOnWaitForFailure,
	}
}

func (proceedOnWaitForFailureFirstStep) WaitFor(
	dex.Context,
	string,
) (*dex.Wait, error) {
	return nil, fmt.Errorf("planned WaitFor failure")
}

func (proceedOnWaitForFailureFirstStep) Execute(
	ctx dex.Context,
	input string,
) (*dex.StepDecision, error) {
	if !ctx.WaitForMethodFailed() {
		return nil, fmt.Errorf("WaitFor failure was not reported")
	}
	return dex.GoTo(
		proceedOnWaitForFailureSecondStep{},
		input+"_step1_wait_for_step1_execute",
	), nil
}

type proceedOnWaitForFailureSecondStep struct {
	dex.StepDefaults
}

func (proceedOnWaitForFailureSecondStep) WaitFor(
	dex.Context,
	string,
) (*dex.Wait, error) {
	return dex.SkipWaitImmediately(), nil
}

func (proceedOnWaitForFailureSecondStep) Execute(
	_ dex.Context,
	input string,
) (*dex.StepDecision, error) {
	return dex.GracefulComplete(input + "_step2_wait_for_step2_execute"), nil
}

func TestBasicFlow(t *testing.T) {
	skipIfServerZD(t, "Flow retry")
	ctx := integrationContext(t)
	flowID := newFlowID(t, "basic")
	timeout := 30 * time.Second
	runID, err := integClient.StartFlow(ctx, basicFlow{}, flowID, 1, dex.StartFlowOptions{
		Timeout:       &timeout,
		IDReusePolicy: dex.IDReuseDisallow,
		RetryPolicy: &dex.FlowRetryPolicy{
			InitialInterval:    time.Second,
			MaximumAttempts:    3,
			MaximumInterval:    10 * time.Second,
			BackoffCoefficient: 3,
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, runID)

	_, err = integClient.StartFlow(ctx, basicFlow{}, flowID, 1, dex.StartFlowOptions{})
	var duplicate *dex.FlowAlreadyStartedError
	require.ErrorAs(t, err, &duplicate)

	result := waitForFlow(t, flowID, true)
	require.Equal(t, dex.FlowCompleted, result.Status)
	require.Len(t, result.Completions, 1)
	var output int
	require.NoError(t, result.Completions[0].Output.Decode(&output))
	require.Equal(t, 3, output)

	_, err = integClient.WaitForFlow(ctx, newFlowID(t, "missing"), dex.WaitForFlowOptions{})
	var missing *dex.FlowNotFoundError
	require.ErrorAs(t, err, &missing)
}

func TestMultiOutputFlow(t *testing.T) {
	ctx := integrationContext(t)
	flowID := newFlowID(t, "multi-output")
	_, err := integClient.StartFlow(ctx, multiOutputFlow{}, flowID, struct{}{}, dex.StartFlowOptions{})
	require.NoError(t, err)

	result := waitForFlow(t, flowID, true)
	require.Len(t, result.Completions, 2)
	outputs := make(map[string]any, len(result.Completions))
	for _, completion := range result.Completions {
		switch completion.StepType {
		case dex.GetFinalStepType(multiOutputStringStep{}):
			var output string
			require.NoError(t, completion.Output.Decode(&output))
			outputs[completion.StepType] = output
		case dex.GetFinalStepType(multiOutputIntStep{}):
			var output int
			require.NoError(t, completion.Output.Decode(&output))
			outputs[completion.StepType] = output
		default:
			t.Fatalf("unexpected Step completion %q", completion.StepType)
		}
		require.NotEmpty(t, completion.StepExecutionID)
	}
	require.Equal(t, "branch-one", outputs[dex.GetFinalStepType(multiOutputStringStep{})])
	require.Equal(t, 42, outputs[dex.GetFinalStepType(multiOutputIntStep{})])
}

func TestProceedOnWaitForFailureFlow(t *testing.T) {
	skipIfServerZD(t, "Step failure policies")
	flowID := newFlowID(t, "proceed-wait")
	_, err := integClient.StartFlow(
		integrationContext(t),
		proceedOnWaitForFailureFlow{},
		flowID,
		"input",
		dex.StartFlowOptions{},
	)
	require.NoError(t, err)
	result := waitForFlow(t, flowID, true)
	require.Equal(t, dex.FlowCompleted, result.Status)
	require.Len(t, result.Completions, 1)
	var output string
	require.NoError(t, result.Completions[0].Output.Decode(&output))
	require.Equal(
		t,
		"input_step1_wait_for_step1_execute_step2_wait_for_step2_execute",
		output,
	)
}
