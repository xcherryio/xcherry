// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package integ

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/superdurable/dex/sdk-go/dex"
	"github.com/superdurable/dex/sdk-go/dex/ptr"
)

var subFlowRunID = dex.DefineAttribute[string]("sub-flow-run-id")

type subFlowParentFlow struct{ emptyFlowSchema }

func (subFlowParentFlow) GetSteps() []dex.StepDef {
	return []dex.StepDef{dex.DefineStartStep(subFlowParentStep{})}
}

type subFlowParentStep struct{ dex.StepDefaults }

func (subFlowParentStep) WaitFor(_ dex.Context, input int) (*dex.Wait, error) {
	return dex.Until(dex.SubFlow(basicFlow{}, input, dex.SubFlowOptions{
		Timeout:       ptr.Any(time.Hour),
		TimeoutPolicy: dex.TimeoutCancel,
	})), nil
}

func (subFlowParentStep) Execute(ctx dex.Context, _ int) (*dex.StepDecision, error) {
	result, err := dex.SubFlowResult(ctx)
	if err != nil {
		return nil, err
	}
	var output int
	if err := result.DecodeSingleOutput(&output); err != nil {
		return nil, err
	}
	flowID, err := dex.SubFlowID(ctx)
	if err != nil {
		return nil, err
	}
	return dex.GracefulComplete(fmt.Sprintf("%s|%s|%d", flowID, result.Status, output)), nil
}

type subFlowAllParentFlow struct{ emptyFlowSchema }

func (subFlowAllParentFlow) GetSteps() []dex.StepDef {
	return []dex.StepDef{dex.DefineStartStep(subFlowAllParentStep{})}
}

type subFlowAllParentStep struct{ dex.StepDefaults }

func (subFlowAllParentStep) WaitFor(_ dex.Context, input int) (*dex.Wait, error) {
	return dex.AllOf(
		dex.SubFlow(basicFlow{}, input),
		dex.SubFlow(basicFlow{}, input+10),
	), nil
}

func (subFlowAllParentStep) Execute(ctx dex.Context, _ int) (*dex.StepDecision, error) {
	values := make([]string, 0, 2)
	for index := range 2 {
		result, err := dex.SubFlowResult(ctx, index)
		if err != nil {
			return nil, err
		}
		var output int
		if err := result.DecodeSingleOutput(&output); err != nil {
			return nil, err
		}
		flowID, err := dex.SubFlowID(ctx, index)
		if err != nil {
			return nil, err
		}
		values = append(values, fmt.Sprintf("%s|%s|%d", flowID, result.Status, output))
	}
	return dex.GracefulComplete(strings.Join(values, ";")), nil
}

type subFlowAnyParentFlow struct{ emptyFlowSchema }

func (subFlowAnyParentFlow) GetSteps() []dex.StepDef {
	return []dex.StepDef{dex.DefineStartStep(subFlowAnyParentStep{})}
}

type subFlowAnyParentStep struct{ dex.StepDefaults }

func (subFlowAnyParentStep) WaitFor(_ dex.Context, input int) (*dex.Wait, error) {
	return dex.AnyOf(dex.Timer(0), dex.SubFlow(subFlowTimerFlow{}, input)), nil
}

func (subFlowAnyParentStep) Execute(ctx dex.Context, _ int) (*dex.StepDecision, error) {
	result, err := dex.SubFlowResult(ctx)
	if err != nil {
		return nil, err
	}
	var output int
	rejectedOutput := result.DecodeSingleOutput(&output) != nil
	flowID, err := dex.SubFlowID(ctx)
	if err != nil {
		return nil, err
	}
	return dex.GracefulComplete(fmt.Sprintf(
		"%s|%s|%t|%t",
		flowID,
		result.Status,
		result.IsTerminal(),
		rejectedOutput,
	)), nil
}

type subFlowAttachParentFlow struct{ emptyFlowSchema }

func (subFlowAttachParentFlow) GetSteps() []dex.StepDef {
	return []dex.StepDef{dex.DefineStartStep(subFlowAttachParentStep{})}
}

type subFlowAttachParentStep struct{ dex.StepDefaults }

func (subFlowAttachParentStep) WaitFor(_ dex.Context, input int) (*dex.Wait, error) {
	return dex.Until(dex.SubFlow(subFlowTimerFlow{}, input, dex.SubFlowOptions{
		ReusePolicy: dex.AttachSubFlow,
	})), nil
}

func (subFlowAttachParentStep) Execute(ctx dex.Context, _ int) (*dex.StepDecision, error) {
	return completeSubFlowStatus(ctx)
}

type subFlowAlwaysRestartParentFlow struct{ emptyFlowSchema }

func (subFlowAlwaysRestartParentFlow) GetSteps() []dex.StepDef {
	return []dex.StepDef{dex.DefineStartStep(subFlowAlwaysRestartParentStep{})}
}

type subFlowAlwaysRestartParentStep struct{ dex.StepDefaults }

func (subFlowAlwaysRestartParentStep) WaitFor(_ dex.Context, input int) (*dex.Wait, error) {
	return dex.Until(dex.SubFlow(subFlowTimerFlow{}, input, dex.SubFlowOptions{
		ReusePolicy: dex.AlwaysRestartSubFlow,
	})), nil
}

func (subFlowAlwaysRestartParentStep) Execute(ctx dex.Context, _ int) (*dex.StepDecision, error) {
	return completeSubFlowStatus(ctx)
}

type subFlowAbnormalParentFlow struct{ emptyFlowSchema }

func (subFlowAbnormalParentFlow) GetSteps() []dex.StepDef {
	return []dex.StepDef{dex.DefineStartStep(subFlowAbnormalParentStep{})}
}

type subFlowAbnormalParentStep struct{ dex.StepDefaults }

func (subFlowAbnormalParentStep) WaitFor(_ dex.Context, input int) (*dex.Wait, error) {
	return dex.Until(dex.SubFlow(subFlowFailingFlow{}, input)), nil
}

func (subFlowAbnormalParentStep) Execute(ctx dex.Context, _ int) (*dex.StepDecision, error) {
	return completeSubFlowStatus(ctx)
}

type subFlowContinueAsNewParentFlow struct{ emptyFlowSchema }

func (subFlowContinueAsNewParentFlow) GetSteps() []dex.StepDef {
	return []dex.StepDef{dex.DefineStartStep(subFlowContinueAsNewParentStep{})}
}

type subFlowContinueAsNewParentStep struct{ dex.StepDefaults }

func (subFlowContinueAsNewParentStep) WaitFor(_ dex.Context, input int) (*dex.Wait, error) {
	options := dex.SubFlowOptions{ConfigOverride: &dex.FlowConfig{
		ContinueAsNewThreshold: ptr.Any(int32(100)),
	}}
	return dex.AllOf(
		dex.SubFlow(subFlowImmediateFlow{}, input, options),
		dex.SubFlow(subFlowTimerFlow{}, 300, options),
	), nil
}

func (subFlowContinueAsNewParentStep) Execute(
	ctx dex.Context,
	_ int,
) (*dex.StepDecision, error) {
	completed, err := dex.SubFlowResult(ctx)
	if err != nil {
		return nil, err
	}
	var completedOutput string
	if err := completed.DecodeSingleOutput(&completedOutput); err != nil {
		return nil, err
	}
	delayed, err := dex.SubFlowResult(ctx, 1)
	if err != nil {
		return nil, err
	}
	completedID, err := dex.SubFlowID(ctx)
	if err != nil {
		return nil, err
	}
	delayedID, err := dex.SubFlowID(ctx, 1)
	if err != nil {
		return nil, err
	}
	return dex.GracefulComplete(fmt.Sprintf(
		"%s|%s|%s|%s",
		completedID,
		completedOutput,
		delayedID,
		delayed.Status,
	)), nil
}

type subFlowTimerFlow struct{ dex.FlowDefaults }

func (subFlowTimerFlow) GetSteps() []dex.StepDef {
	return []dex.StepDef{dex.DefineStartStep(subFlowTimerStep{})}
}

func (subFlowTimerFlow) GetPersistenceSchema() dex.PersistenceSchema {
	return dex.PersistenceSchema{Attributes: []dex.AttributeDef{subFlowRunID}}
}

type subFlowTimerStep struct{ dex.StepDefaults }

func (subFlowTimerStep) WaitFor(ctx dex.Context, seconds int) (*dex.Wait, error) {
	if err := subFlowRunID.Set(ctx, ctx.RunID()); err != nil {
		return nil, err
	}
	return dex.Until(dex.Timer(
		time.Duration(seconds)*time.Second,
		dex.WithConditionID("test-timer-id"),
	)), nil
}

func (subFlowTimerStep) Execute(ctx dex.Context, _ int) (*dex.StepDecision, error) {
	return dex.GracefulComplete(ctx.RunID()), nil
}

type subFlowFailingFlow struct{ dex.FlowDefaults }

func (subFlowFailingFlow) GetSteps() []dex.StepDef {
	return []dex.StepDef{dex.DefineStartStep(subFlowFailingStep{})}
}

func (subFlowFailingFlow) GetPersistenceSchema() dex.PersistenceSchema {
	return dex.PersistenceSchema{Attributes: []dex.AttributeDef{subFlowRunID}}
}

type subFlowFailingStep struct{ dex.StepDefaults }

func (subFlowFailingStep) GetStepOptions() *dex.StepOptions {
	return &dex.StepOptions{ExecuteRetry: &dex.RetryPolicy{MaximumAttempts: 1}}
}

func (subFlowFailingStep) WaitFor(ctx dex.Context, _ int) (*dex.Wait, error) {
	if err := subFlowRunID.Set(ctx, ctx.RunID()); err != nil {
		return nil, err
	}
	return dex.SkipWaitImmediately(), nil
}

func (subFlowFailingStep) Execute(dex.Context, int) (*dex.StepDecision, error) {
	return nil, fmt.Errorf("SubFlow abnormal exit")
}

type subFlowImmediateFlow struct{ dex.FlowDefaults }

func (subFlowImmediateFlow) GetSteps() []dex.StepDef {
	return []dex.StepDef{dex.DefineStartStep(subFlowImmediateStep{})}
}

func (subFlowImmediateFlow) GetPersistenceSchema() dex.PersistenceSchema {
	return dex.PersistenceSchema{Attributes: []dex.AttributeDef{subFlowRunID}}
}

type subFlowImmediateStep struct{ dex.StepDefaultsNoWaitFor[int] }

func (subFlowImmediateStep) Execute(ctx dex.Context, input int) (*dex.StepDecision, error) {
	if err := subFlowRunID.Set(ctx, ctx.RunID()); err != nil {
		return nil, err
	}
	return dex.GracefulComplete(fmt.Sprintf("%s,%d", ctx.RunID(), input+2)), nil
}

func TestSubFlowReturnsIdentityAndOutput(t *testing.T) {
	flowID := newFlowID(t, "sub-flow-parent")
	_, err := integClient.StartFlow(
		integrationContext(t), subFlowParentFlow{}, flowID, 4, dex.StartFlowOptions{},
	)
	require.NoError(t, err)
	var output string
	require.NoError(t, waitForFlow(t, flowID, true).DecodeSingleOutput(&output))
	require.Equal(t, subFlowID(flowID, subFlowParentStep{}, 0)+"|completed|3", output)
}

func TestSubFlowAllOfReturnsStableTerminalResults(t *testing.T) {
	flowID := newFlowID(t, "sub-flow-all")
	_, err := integClient.StartFlow(
		integrationContext(t), subFlowAllParentFlow{}, flowID, 4, dex.StartFlowOptions{},
	)
	require.NoError(t, err)
	var output string
	require.NoError(t, waitForFlow(t, flowID, true).DecodeSingleOutput(&output))
	require.Equal(t, []string{
		subFlowID(flowID, subFlowAllParentStep{}, 0) + "|completed|3",
		subFlowID(flowID, subFlowAllParentStep{}, 1) + "|completed|3",
	}, strings.Split(output, ";"))
}

func TestSubFlowAnyOfRunningSnapshotCanBeStopped(t *testing.T) {
	flowID := newFlowID(t, "sub-flow-any")
	_, err := integClient.StartFlow(
		integrationContext(t), subFlowAnyParentFlow{}, flowID, 300, dex.StartFlowOptions{},
	)
	require.NoError(t, err)
	var output string
	require.NoError(t, waitForFlow(t, flowID, true).DecodeSingleOutput(&output))
	childID := subFlowID(flowID, subFlowAnyParentStep{}, 0)
	require.Equal(t, []string{childID, "running", "false", "true"}, strings.Split(output, "|"))
	require.NoError(t, integClient.StopFlow(
		integrationContext(t), childID, dex.StopOptions{Type: dex.CancelFlow},
	))
	require.Equal(t, dex.FlowCanceled, waitForUncompletedFlow(t, childID, false).Status)
}

func TestSubFlowAttachKeepsRunningExecutionAcrossParentReset(t *testing.T) {
	assertSubFlowRunningReuse(t, subFlowAttachParentFlow{}, subFlowAttachParentStep{}, false)
}

func TestSubFlowAlwaysRestartReplacesRunningExecutionAcrossParentReset(t *testing.T) {
	assertSubFlowRunningReuse(
		t, subFlowAlwaysRestartParentFlow{}, subFlowAlwaysRestartParentStep{}, true,
	)
}

func TestSubFlowDefaultReuseRestartsFailedExecutionAcrossParentReset(t *testing.T) {
	flowID := newFlowID(t, "sub-flow-abnormal")
	childID := subFlowID(flowID, subFlowAbnormalParentStep{}, 0)
	_, err := integClient.StartFlow(
		integrationContext(t), subFlowAbnormalParentFlow{}, flowID, 1, dex.StartFlowOptions{},
	)
	require.NoError(t, err)
	waitForFlow(t, flowID, true)
	firstRunID := getSubFlowRunID(t, childID)
	_, err = integClient.TimeTravel(integrationContext(t), flowID, dex.TimeTravelOptions{
		Type: dex.TimeTravelToBeginning, Reason: "verify SubFlow abnormal reuse",
	})
	require.NoError(t, err)
	waitForFlow(t, flowID, true)
	secondRunID := awaitSubFlowRunID(t, childID, firstRunID)
	require.NotEqual(t, firstRunID, secondRunID)
}

func TestSubFlowPartialResultsSurviveContinueAsNewWithoutRestart(t *testing.T) {
	skipIfServerZD(t, "Continue-As-New")
	flowID := newFlowID(t, "sub-flow-can")
	completedID := subFlowID(flowID, subFlowContinueAsNewParentStep{}, 0)
	delayedID := subFlowID(flowID, subFlowContinueAsNewParentStep{}, 1)
	_, err := integClient.StartFlow(
		integrationContext(t),
		subFlowContinueAsNewParentFlow{},
		flowID,
		4,
		dex.StartFlowOptions{ConfigOverride: &dex.FlowConfig{
			ContinueAsNewThreshold: ptr.Any(int32(1)),
		}},
	)
	require.NoError(t, err)
	completedRunID := awaitSubFlowRunID(t, completedID, "")
	require.NoError(t, integClient.TriggerContinueAsNew(integrationContext(t), flowID))
	skipSubFlowTimer(t, delayedID)
	var output string
	require.NoError(t, waitForFlow(t, flowID, true).DecodeSingleOutput(&output))
	parts := strings.Split(output, "|")
	require.Len(t, parts, 4)
	require.Equal(t, completedID, parts[0])
	require.Equal(t, completedRunID+",6", parts[1])
	require.Equal(t, delayedID, parts[2])
	require.Equal(t, "completed", parts[3])
	require.Equal(t, completedRunID, getSubFlowRunID(t, completedID))
}

func completeSubFlowStatus(ctx dex.Context) (*dex.StepDecision, error) {
	result, err := dex.SubFlowResult(ctx)
	if err != nil {
		return nil, err
	}
	flowID, err := dex.SubFlowID(ctx)
	if err != nil {
		return nil, err
	}
	return dex.GracefulComplete(flowID + "|" + result.Status.String()), nil
}

func assertSubFlowRunningReuse[Input any](
	t *testing.T,
	parent dex.Flow,
	parentStep dex.Step[Input],
	expectsRestart bool,
) {
	t.Helper()
	flowID := newFlowID(t, "sub-flow-reuse")
	childID := subFlowID(flowID, parentStep, 0)
	_, err := integClient.StartFlow(
		integrationContext(t), parent, flowID, 300, dex.StartFlowOptions{},
	)
	require.NoError(t, err)
	firstRunID := awaitSubFlowRunID(t, childID, "")
	_, err = integClient.TimeTravel(integrationContext(t), flowID, dex.TimeTravelOptions{
		Type: dex.TimeTravelToBeginning, Reason: "verify SubFlow running reuse",
	})
	require.NoError(t, err)
	activeRunID := firstRunID
	if expectsRestart {
		activeRunID = awaitSubFlowRunID(t, childID, firstRunID)
	}
	require.Equal(t, expectsRestart, activeRunID != firstRunID)
	skipSubFlowTimer(t, childID)
	var output string
	require.NoError(t, waitForFlow(t, flowID, true).DecodeSingleOutput(&output))
	require.Equal(t, []string{childID, "completed"}, strings.Split(output, "|"))
}

func skipSubFlowTimer(t *testing.T, flowID string) {
	t.Helper()
	var skipErr error
	require.Eventually(t, func() bool {
		skipErr = integClient.SkipTimer(
			integrationContext(t),
			flowID,
			dex.StepExecutionID{StepType: dex.GetFinalStepType(subFlowTimerStep{})},
			dex.TimerID{ConditionID: "test-timer-id"},
		)
		return skipErr == nil
	}, 30*time.Second, 20*time.Millisecond)
	require.NoError(t, skipErr)
}

func awaitSubFlowRunID(t *testing.T, flowID, excluded string) string {
	t.Helper()
	var runID string
	require.Eventually(t, func() bool {
		searchPage, err := integClient.SearchFlows(
			integrationContext(t),
			fmt.Sprintf("WorkflowId = '%s'", flowID),
			100,
			"",
		)
		if err != nil {
			return false
		}
		var latestStartedAt time.Time
		for _, entry := range searchPage.Flows {
			if entry.FlowID == flowID && entry.RunID != excluded && entry.StartedAt.After(latestStartedAt) {
				runID = entry.RunID
				latestStartedAt = entry.StartedAt
			}
		}
		return runID != ""
	}, 30*time.Second, 20*time.Millisecond)
	return runID
}

func getSubFlowRunID(t *testing.T, flowID string) string {
	t.Helper()
	return awaitSubFlowRunID(t, flowID, "")
}

func subFlowID[Input any](parentFlowID string, parentStep dex.Step[Input], index int) string {
	return fmt.Sprintf(
		"SubFlow:%s-%s-1-%d",
		parentFlowID,
		dex.GetFinalStepType(parentStep),
		index,
	)
}
