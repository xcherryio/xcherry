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
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/superdurable/dex/sdk-go/dex"
	"github.com/superdurable/dex/sdk-go/dex/ptr"
	"github.com/superdurable/dex/sdk-go/gen/dexpb"
)

var (
	actionPermissionAttribute1 = dex.DefineAttribute[string]("action-permission-attribute-1")
	actionPermissionAttribute2 = dex.DefineAttribute[string]("action-permission-attribute-2")
	actionPermissionSelfState  = dex.DefineAttribute[string]("action-permission-self-state")
	actionStepStatus           = dex.DefineAttribute[string]("action-step-status")
	actionParentState          = dex.DefineAttribute[string]("action-parent-state")
)

type actionPermissionUpdate struct {
	Attribute1       *string
	Attribute2       *string
	SelfState        *string
	DeleteAttribute1 bool
}

type actionPermissionFlow struct {
	dex.FlowDefaults
}

func (actionPermissionFlow) GetSteps() []dex.StepDef {
	return nil
}

func (flow actionPermissionFlow) GetRPCs() []dex.RPCDef {
	return []dex.RPCDef{
		dex.DefineRPC(flow.UpdateState, &dex.RPCOptions{
			Action: dex.DefineAction(
				"Update state",
				dex.WhenAttributeMatches(
					actionPermissionAttribute2,
					dex.AttributeMatchEqual("C"),
				),
				dex.ActionRequiresPermission("permission-z"),
			),
		}),
		dex.DefineRPC(flow.GetAttribute1Action, &dex.RPCOptions{Action: dex.DefineAction(
			"Get attribute one action",
			dex.WhenAttributeMatches(
				actionPermissionAttribute1,
				dex.AttributeMatchEqual("A"),
			),
			dex.ActionRequiresPermission("permission-x"),
		)}),
		dex.DefineRPC(flow.GetAttribute2Action, &dex.RPCOptions{Action: dex.DefineAction(
			"Get attribute two action",
			dex.WhenAttributeMatches(
				actionPermissionAttribute1,
				dex.AttributeMatchEqual("B"),
			),
			dex.ActionRequiresPermission("permission-y"),
		)}),
		dex.DefineRPC(flow.GetDuplicateAction, &dex.RPCOptions{Action: dex.DefineAction(
			"Get duplicate action",
			dex.WhenAttributeMatches(
				actionPermissionAttribute1,
				dex.AttributeMatchEqual("A"),
			),
			dex.ActionRequiresPermission("permission-x"),
		)}),
		dex.DefineRPC(flow.CompleteSelfAction, &dex.RPCOptions{
			Action: dex.DefineAction(
				"Complete self action",
				dex.WhenAttributeMatches(
					actionPermissionSelfState,
					dex.AttributeMatchEqual("open"),
				),
				dex.ActionRequiresPermission("permission-self"),
			),
		}),
		dex.DefineRPC(flow.SetAttribute1, nil),
		dex.DefineRPC(flow.SetAttribute2, nil),
		dex.DefineRPC(flow.FailStateUpdate, nil),
	}
}

func (actionPermissionFlow) GetPersistenceSchema() dex.PersistenceSchema {
	return dex.PersistenceSchema{Attributes: []dex.AttributeDef{
		actionPermissionAttribute1,
		actionPermissionAttribute2,
		actionPermissionSelfState,
	}}
}

func (actionPermissionFlow) UpdateState(
	ctx dex.Context,
	input actionPermissionUpdate,
) (*dex.RPCResult[dex.None], error) {
	if input.DeleteAttribute1 {
		if err := actionPermissionAttribute1.Delete(ctx); err != nil {
			return nil, err
		}
	} else if input.Attribute1 != nil {
		if err := actionPermissionAttribute1.Set(ctx, *input.Attribute1); err != nil {
			return nil, err
		}
	}
	if input.Attribute2 != nil {
		if err := actionPermissionAttribute2.Set(ctx, *input.Attribute2); err != nil {
			return nil, err
		}
	}
	if input.SelfState != nil {
		if err := actionPermissionSelfState.Set(ctx, *input.SelfState); err != nil {
			return nil, err
		}
	}
	return &dex.RPCResult[dex.None]{}, nil
}

func (actionPermissionFlow) GetAttribute1Action(
	dex.Context,
	dex.None,
) (*dex.RPCResult[dex.None], error) {
	return &dex.RPCResult[dex.None]{}, nil
}

func (actionPermissionFlow) GetAttribute2Action(
	dex.Context,
	dex.None,
) (*dex.RPCResult[dex.None], error) {
	return &dex.RPCResult[dex.None]{}, nil
}

func (actionPermissionFlow) GetDuplicateAction(
	dex.Context,
	dex.None,
) (*dex.RPCResult[dex.None], error) {
	return &dex.RPCResult[dex.None]{}, nil
}

func (actionPermissionFlow) CompleteSelfAction(
	ctx dex.Context,
	_ dex.None,
) (*dex.RPCResult[dex.None], error) {
	if err := actionPermissionSelfState.Set(ctx, "closed"); err != nil {
		return nil, err
	}
	return &dex.RPCResult[dex.None]{}, nil
}

func (actionPermissionFlow) SetAttribute1(
	ctx dex.Context,
	value string,
) (*dex.RPCResult[dex.None], error) {
	if err := actionPermissionAttribute1.Set(ctx, value); err != nil {
		return nil, err
	}
	return &dex.RPCResult[dex.None]{}, nil
}

func (actionPermissionFlow) SetAttribute2(
	ctx dex.Context,
	value string,
) (*dex.RPCResult[dex.None], error) {
	if err := actionPermissionAttribute2.Set(ctx, value); err != nil {
		return nil, err
	}
	return &dex.RPCResult[dex.None]{}, nil
}

func (actionPermissionFlow) FailStateUpdate(
	ctx dex.Context,
	_ dex.None,
) (*dex.RPCResult[dex.None], error) {
	if err := actionPermissionAttribute1.Set(ctx, "A"); err != nil {
		return nil, err
	}
	if err := actionPermissionAttribute2.Set(ctx, "C"); err != nil {
		return nil, err
	}
	return nil, errors.New("planned Action state update failure")
}

type actionScalarNamedString string
type actionScalarNamedInt int64

var (
	actionScalarBool                 = dex.DefineAttribute[bool]("action-scalar-bool")
	actionScalarInt                  = dex.DefineAttribute[int64]("action-scalar-int")
	actionScalarFloat                = dex.DefineAttribute[float64]("action-scalar-float")
	actionScalarNamedStringAttribute = dex.DefineAttribute[actionScalarNamedString](
		"action-scalar-named-string",
	)
	actionScalarNamedIntAttribute = dex.DefineAttribute[actionScalarNamedInt](
		"action-scalar-named-int",
	)
)

type actionScalarFlow struct {
	dex.FlowDefaults
}

func (actionScalarFlow) GetSteps() []dex.StepDef {
	return nil
}

func (flow actionScalarFlow) GetRPCs() []dex.RPCDef {
	return []dex.RPCDef{
		dex.DefineRPC(flow.GetBoolAction, &dex.RPCOptions{Action: dex.DefineAction(
			"Get bool action",
			dex.WhenAttributeMatches(actionScalarBool, dex.AttributeMatchEqual(true)),
			dex.ActionRequiresPermission("scalar.bool"),
		)}),
		dex.DefineRPC(flow.GetIntAction, &dex.RPCOptions{Action: dex.DefineAction(
			"Get int action",
			dex.WhenAttributeMatches(actionScalarInt, dex.AttributeMatchEqual(int64(42))),
			dex.ActionRequiresPermission("scalar.int"),
		)}),
		dex.DefineRPC(flow.GetFloatAction, &dex.RPCOptions{Action: dex.DefineAction(
			"Get float action",
			dex.WhenAttributeMatches(actionScalarFloat, dex.AttributeMatchEqual(3.5)),
			dex.ActionRequiresPermission("scalar.float"),
		)}),
		dex.DefineRPC(flow.GetNamedStringAction, &dex.RPCOptions{Action: dex.DefineAction(
			"Get named string action",
			dex.WhenAttributeMatches(
				actionScalarNamedStringAttribute,
				dex.AttributeMatchEqual(actionScalarNamedString("ready")),
			),
			dex.ActionRequiresPermission("scalar.named-string"),
		)}),
		dex.DefineRPC(flow.GetNamedIntAction, &dex.RPCOptions{Action: dex.DefineAction(
			"Get named int action",
			dex.WhenAttributeMatches(
				actionScalarNamedIntAttribute,
				dex.AttributeMatchEqual(actionScalarNamedInt(7)),
			),
			dex.ActionRequiresPermission("scalar.named-int"),
		)}),
	}
}

func (actionScalarFlow) GetPersistenceSchema() dex.PersistenceSchema {
	return dex.PersistenceSchema{Attributes: []dex.AttributeDef{
		actionScalarBool,
		actionScalarInt,
		actionScalarFloat,
		actionScalarNamedStringAttribute,
		actionScalarNamedIntAttribute,
	}}
}

func (actionScalarFlow) GetBoolAction(dex.Context, dex.None) (*dex.RPCResult[dex.None], error) {
	return &dex.RPCResult[dex.None]{}, nil
}

func (actionScalarFlow) GetIntAction(dex.Context, dex.None) (*dex.RPCResult[dex.None], error) {
	return &dex.RPCResult[dex.None]{}, nil
}

func (actionScalarFlow) GetFloatAction(dex.Context, dex.None) (*dex.RPCResult[dex.None], error) {
	return &dex.RPCResult[dex.None]{}, nil
}

func (actionScalarFlow) GetNamedStringAction(
	dex.Context,
	dex.None,
) (*dex.RPCResult[dex.None], error) {
	return &dex.RPCResult[dex.None]{}, nil
}

func (actionScalarFlow) GetNamedIntAction(
	dex.Context,
	dex.None,
) (*dex.RPCResult[dex.None], error) {
	return &dex.RPCResult[dex.None]{}, nil
}

type actionStepFlow struct {
	dex.FlowDefaults
}

func (actionStepFlow) GetSteps() []dex.StepDef {
	return []dex.StepDef{dex.DefineStartStep(actionPermissionStep{})}
}

func (flow actionStepFlow) GetRPCs() []dex.RPCDef {
	return []dex.RPCDef{
		dex.DefineRPC(flow.GetWaitingAction, &dex.RPCOptions{Action: dex.DefineAction(
			"Get waiting action",
			dex.WhenAttributeMatches(actionStepStatus, dex.AttributeMatchEqual("waiting")),
			dex.ActionRequiresPermission("step.waiting"),
		)}),
		dex.DefineRPC(flow.GetExecutedAction, &dex.RPCOptions{Action: dex.DefineAction(
			"Get executed action",
			dex.WhenAttributeMatches(actionStepStatus, dex.AttributeMatchEqual("executed")),
			dex.ActionRequiresPermission("step.executed"),
		)}),
	}
}

func (actionStepFlow) GetPersistenceSchema() dex.PersistenceSchema {
	return dex.PersistenceSchema{Attributes: []dex.AttributeDef{actionStepStatus}}
}

func (actionStepFlow) GetWaitingAction(dex.Context, dex.None) (*dex.RPCResult[dex.None], error) {
	return &dex.RPCResult[dex.None]{}, nil
}

func (actionStepFlow) GetExecutedAction(dex.Context, dex.None) (*dex.RPCResult[dex.None], error) {
	return &dex.RPCResult[dex.None]{}, nil
}

type actionPermissionStep struct {
	dex.StepDefaults
}

func (actionPermissionStep) WaitFor(ctx dex.Context, _ dex.None) (*dex.Wait, error) {
	if err := actionStepStatus.Set(ctx, "waiting"); err != nil {
		return nil, err
	}
	return dex.Until(dex.Timer(time.Hour, dex.WithConditionID("action-step-timer"))), nil
}

func (actionPermissionStep) Execute(ctx dex.Context, _ dex.None) (*dex.StepDecision, error) {
	if err := actionStepStatus.Set(ctx, "executed"); err != nil {
		return nil, err
	}
	return dex.GracefulComplete("done"), nil
}

type actionSubFlowParent struct {
	dex.FlowDefaults
}

func (actionSubFlowParent) GetSteps() []dex.StepDef {
	return []dex.StepDef{dex.DefineStartStep(actionSubFlowParentStep{})}
}

func (flow actionSubFlowParent) GetRPCs() []dex.RPCDef {
	return []dex.RPCDef{dex.DefineRPC(flow.GetParentAction, &dex.RPCOptions{Action: dex.DefineAction(
		"Get parent action",
		dex.WhenAttributeMatches(actionParentState, dex.AttributeMatchEqual("ready")),
		dex.ActionRequiresPermission("permission-parent"),
	)})}
}

func (actionSubFlowParent) GetPersistenceSchema() dex.PersistenceSchema {
	return dex.PersistenceSchema{Attributes: []dex.AttributeDef{actionParentState}}
}

func (actionSubFlowParent) GetParentAction(
	dex.Context,
	dex.None,
) (*dex.RPCResult[dex.None], error) {
	return &dex.RPCResult[dex.None]{}, nil
}

type actionSubFlowParentStep struct {
	dex.StepDefaults
}

func (actionSubFlowParentStep) WaitFor(dex.Context, dex.None) (*dex.Wait, error) {
	initial1, err := dex.InitialAttribute(actionPermissionAttribute1, "A")
	if err != nil {
		return nil, err
	}
	initial2, err := dex.InitialAttribute(actionPermissionAttribute2, "C")
	if err != nil {
		return nil, err
	}
	return dex.Until(dex.SubFlow(actionSubFlowChild{}, nil, dex.SubFlowOptions{
		Attributes: []dex.InitialAttributeDef{initial1, initial2},
	})), nil
}

func (actionSubFlowParentStep) Execute(dex.Context, dex.None) (*dex.StepDecision, error) {
	return dex.DeadEnd(), nil
}

type actionSubFlowChild struct {
	dex.FlowDefaults
}

func (actionSubFlowChild) GetSteps() []dex.StepDef {
	return []dex.StepDef{dex.DefineStartStep(actionSubFlowChildStep{})}
}

func (flow actionSubFlowChild) GetRPCs() []dex.RPCDef {
	return []dex.RPCDef{
		dex.DefineRPC(flow.GetAttribute1Action, &dex.RPCOptions{Action: dex.DefineAction(
			"Get child attribute one action",
			dex.WhenAttributeMatches(
				actionPermissionAttribute1,
				dex.AttributeMatchEqual("A"),
			),
			dex.ActionRequiresPermission("permission-x"),
		)}),
		dex.DefineRPC(flow.GetAttribute2Action, &dex.RPCOptions{Action: dex.DefineAction(
			"Get child attribute two action",
			dex.WhenAttributeMatches(
				actionPermissionAttribute1,
				dex.AttributeMatchEqual("B"),
			),
			dex.ActionRequiresPermission("permission-y"),
		)}),
		dex.DefineRPC(flow.GetRegionAction, &dex.RPCOptions{Action: dex.DefineAction(
			"Get child region action",
			dex.WhenAttributeMatches(
				actionPermissionAttribute2,
				dex.AttributeMatchEqual("C"),
			),
			dex.ActionRequiresPermission("permission-z"),
		)}),
	}
}

func (actionSubFlowChild) GetPersistenceSchema() dex.PersistenceSchema {
	return dex.PersistenceSchema{Attributes: []dex.AttributeDef{
		actionPermissionAttribute1,
		actionPermissionAttribute2,
	}}
}

func (actionSubFlowChild) GetAttribute1Action(
	dex.Context,
	dex.None,
) (*dex.RPCResult[dex.None], error) {
	return &dex.RPCResult[dex.None]{}, nil
}

func (actionSubFlowChild) GetAttribute2Action(
	dex.Context,
	dex.None,
) (*dex.RPCResult[dex.None], error) {
	return &dex.RPCResult[dex.None]{}, nil
}

func (actionSubFlowChild) GetRegionAction(
	dex.Context,
	dex.None,
) (*dex.RPCResult[dex.None], error) {
	return &dex.RPCResult[dex.None]{}, nil
}

type actionSubFlowChildStep struct {
	dex.StepDefaults
}

func (actionSubFlowChildStep) WaitFor(dex.Context, dex.None) (*dex.Wait, error) {
	return dex.Until(dex.Timer(time.Hour)), nil
}

func (actionSubFlowChildStep) Execute(dex.Context, dex.None) (*dex.StepDecision, error) {
	return dex.DeadEnd(), nil
}

func TestActionPermissionProjectionStateMatrix(t *testing.T) {
	ctx := integrationContext(t)
	flow := actionPermissionFlow{}
	flowID := newFlowID(t, "action-permission-matrix")
	requestID := uuid.NewString()
	initial1 := mustInitialActionAttribute(t, actionPermissionAttribute1, "A")
	initial2 := mustInitialActionAttribute(t, actionPermissionAttribute2, "C")
	options := dex.StartFlowOptions{
		Attributes:     []dex.InitialAttributeDef{initial1, initial2},
		RequestID:      &requestID,
		AlreadyStarted: &dex.AlreadyStartedOptions{IgnoreError: true},
	}
	runID, err := integClient.StartFlow(ctx, flow, flowID, nil, options)
	require.NoError(t, err)
	repeatedRunID, err := integClient.StartFlow(ctx, flow, flowID, nil, options)
	require.NoError(t, err)
	require.Equal(t, runID, repeatedRunID)
	requireActionPermissions(t, flowID, []string{"permission-x", "permission-z"})

	invokeActionStateUpdate(t, flowID, actionPermissionUpdate{
		Attribute1: ptr.Any("B"),
		Attribute2: ptr.Any("C"),
	})
	requireActionPermissions(t, flowID, []string{"permission-x", "permission-y", "permission-z"})

	invokeActionStateUpdate(t, flowID, actionPermissionUpdate{
		Attribute1: ptr.Any("D"),
		Attribute2: ptr.Any("C"),
	})
	requireActionPermissions(t, flowID, []string{"permission-x", "permission-y", "permission-z"})

	invokeActionStateUpdate(t, flowID, actionPermissionUpdate{
		Attribute1: ptr.Any("D"),
		Attribute2: ptr.Any("D"),
	})
	requireActionPermissions(t, flowID, []string{"permission-x", "permission-y", "permission-z"})

	invokeActionStateUpdate(t, flowID, actionPermissionUpdate{
		DeleteAttribute1: true,
		Attribute2:       ptr.Any("C"),
	})
	requireActionPermissions(t, flowID, []string{"permission-x", "permission-y", "permission-z"})

	invokeActionStateUpdate(t, flowID, actionPermissionUpdate{
		Attribute1: ptr.Any("A"),
		Attribute2: ptr.Any("C"),
	})
	requireActionPermissions(t, flowID, []string{"permission-x", "permission-y", "permission-z"})

	var noOutput dex.None
	require.NoError(t, integClient.InvokeRPC(
		ctx,
		flowID,
		flow.GetAttribute1Action,
		nil,
		&noOutput,
	))
	requireActionPermissions(t, flowID, []string{"permission-x", "permission-y", "permission-z"})
}

func TestActionPermissionProjectionInitialAndFailedUpdates(t *testing.T) {
	ctx := integrationContext(t)
	flow := actionPermissionFlow{}

	emptyFlowID := newFlowID(t, "action-permission-empty")
	_, err := integClient.StartFlow(ctx, flow, emptyFlowID, nil, dex.StartFlowOptions{})
	require.NoError(t, err)
	requireActionPermissions(t, emptyFlowID, nil)

	partialFlowID := newFlowID(t, "action-permission-partial")
	initial2 := mustInitialActionAttribute(t, actionPermissionAttribute2, "C")
	_, err = integClient.StartFlow(ctx, flow, partialFlowID, nil, dex.StartFlowOptions{
		Attributes: []dex.InitialAttributeDef{initial2},
	})
	require.NoError(t, err)
	requireActionPermissions(t, partialFlowID, []string{"permission-z"})

	var noOutput dex.None
	err = integClient.InvokeRPC(
		ctx,
		emptyFlowID,
		flow.FailStateUpdate,
		nil,
		&noOutput,
	)
	require.Error(t, err)
	requireActionPermissions(t, emptyFlowID, nil)

	invokeActionStateUpdate(t, emptyFlowID, actionPermissionUpdate{
		SelfState: ptr.Any("open"),
	})
	requireActionPermissions(t, emptyFlowID, []string{"permission-self"})
	require.NoError(t, integClient.InvokeRPC(
		ctx,
		emptyFlowID,
		flow.CompleteSelfAction,
		nil,
		&noOutput,
	))
	requireActionPermissions(t, emptyFlowID, []string{"permission-self"})
}

func TestActionPermissionProjectionScalarTypes(t *testing.T) {
	ctx := integrationContext(t)
	flowID := newFlowID(t, "action-permission-scalars")
	_, err := integClient.StartFlow(ctx, actionScalarFlow{}, flowID, nil, dex.StartFlowOptions{
		Attributes: []dex.InitialAttributeDef{
			mustInitialActionAttribute(t, actionScalarBool, true),
			mustInitialActionAttribute(t, actionScalarInt, int64(42)),
			mustInitialActionAttribute(t, actionScalarFloat, 3.5),
			mustInitialActionAttribute(
				t,
				actionScalarNamedStringAttribute,
				actionScalarNamedString("ready"),
			),
			mustInitialActionAttribute(
				t,
				actionScalarNamedIntAttribute,
				actionScalarNamedInt(7),
			),
		},
	})
	require.NoError(t, err)
	requireActionPermissions(t, flowID, []string{
		"scalar.bool",
		"scalar.float",
		"scalar.int",
		"scalar.named-int",
		"scalar.named-string",
	})
}

func TestActionPermissionProjectionWaitForAndExecute(t *testing.T) {
	ctx := integrationContext(t)
	flowID := newFlowID(t, "action-permission-step")
	_, err := integClient.StartFlow(ctx, actionStepFlow{}, flowID, nil, dex.StartFlowOptions{})
	require.NoError(t, err)
	requireActionPermissions(t, flowID, []string{"step.waiting"})

	var skipErr error
	require.Eventually(t, func() bool {
		skipErr = integClient.SkipTimer(
			ctx,
			flowID,
			dex.StepExecutionID{StepType: dex.GetFinalStepType(actionPermissionStep{})},
			dex.TimerID{ConditionID: "action-step-timer"},
		)
		return skipErr == nil
	}, 30*time.Second, 20*time.Millisecond)
	require.NoError(t, skipErr)
	require.Equal(t, dex.FlowCompleted, waitForFlow(t, flowID, false).Status)
	requireActionPermissions(t, flowID, []string{"step.executed", "step.waiting"})
}

func TestActionPermissionProjectionConcurrentRPCs(t *testing.T) {
	ctx := integrationContext(t)
	flow := actionPermissionFlow{}
	flowID := newFlowID(t, "action-permission-concurrent")
	_, err := integClient.StartFlow(ctx, flow, flowID, nil, dex.StartFlowOptions{})
	require.NoError(t, err)

	errorsByRPC := make(chan error, 2)
	go func() {
		var output dex.None
		errorsByRPC <- integClient.InvokeRPC(ctx, flowID, flow.SetAttribute1, "A", &output)
	}()
	go func() {
		var output dex.None
		errorsByRPC <- integClient.InvokeRPC(ctx, flowID, flow.SetAttribute2, "C", &output)
	}()
	require.NoError(t, <-errorsByRPC)
	require.NoError(t, <-errorsByRPC)
	requireActionPermissions(t, flowID, []string{"permission-x", "permission-z"})
}

func TestActionPermissionProjectionSubFlowIsolation(t *testing.T) {
	ctx := integrationContext(t)
	parent := actionSubFlowParent{}
	parentID := newFlowID(t, "action-permission-parent")
	initialParent := mustInitialActionAttribute(t, actionParentState, "ready")
	_, err := integClient.StartFlow(ctx, parent, parentID, nil, dex.StartFlowOptions{
		Attributes: []dex.InitialAttributeDef{initialParent},
	})
	require.NoError(t, err)

	childID := subFlowID(parentID, actionSubFlowParentStep{}, 0)
	requireActionPermissions(t, parentID, []string{"permission-parent"})
	requireActionPermissions(t, childID, []string{"permission-x", "permission-z"})
}

func TestActionPermissionProjectionContinueAsNew(t *testing.T) {
	ctx := integrationContext(t)
	flow := actionPermissionFlow{}
	flowID := newFlowID(t, "action-permission-continue")
	initial1 := mustInitialActionAttribute(t, actionPermissionAttribute1, "A")
	initial2 := mustInitialActionAttribute(t, actionPermissionAttribute2, "C")
	firstRunID, err := integClient.StartFlow(ctx, flow, flowID, nil, dex.StartFlowOptions{
		Attributes: []dex.InitialAttributeDef{initial1, initial2},
		ConfigOverride: &dex.FlowConfig{
			ContinueAsNewThreshold: ptr.Any(int32(100)),
		},
	})
	require.NoError(t, err)
	requireActionPermissions(t, flowID, []string{"permission-x", "permission-z"})

	invokeActionStateUpdate(t, flowID, actionPermissionUpdate{
		Attribute1: ptr.Any("D"),
		Attribute2: ptr.Any("D"),
	})
	requireActionPermissions(t, flowID, []string{"permission-x", "permission-z"})

	serverEngine, err := integrationServerEngine()
	require.NoError(t, err)
	// C3: server-zd never continues a run as new.
	if serverEngine != dexpb.ServerEngine_SERVER_ENGINE_ZD {
		require.NoError(t, integClient.TriggerContinueAsNew(ctx, flowID))
		continuedRunID := awaitSubFlowRunID(t, flowID, firstRunID)
		require.NotEmpty(t, continuedRunID)
	}

	var output dex.None
	require.NoError(t, integClient.InvokeRPC(
		ctx,
		flowID,
		flow.SetAttribute1,
		"B",
		&output,
	))
	requireActionPermissions(t, flowID, []string{"permission-x", "permission-y", "permission-z"})
}

func invokeActionStateUpdate(t *testing.T, flowID string, input actionPermissionUpdate) {
	t.Helper()
	var output dex.None
	require.NoError(t, integClient.InvokeRPC(
		integrationContext(t),
		flowID,
		actionPermissionFlow{}.UpdateState,
		input,
		&output,
	))
}

func mustInitialActionAttribute[T any](
	t *testing.T,
	attribute dex.Attribute[T],
	value T,
) dex.InitialAttributeDef {
	t.Helper()
	initial, err := dex.InitialAttribute(attribute, value)
	require.NoError(t, err)
	return initial
}

func requireActionPermissions(t *testing.T, flowID string, expected []string) {
	t.Helper()
	expected = append([]string(nil), expected...)
	sort.Strings(expected)
	var lastErr error
	require.Eventually(t, func() bool {
		var entry dex.SearchFlowEntry
		entry, lastErr = latestActionPermissionFlowEntry(t, flowID)
		if lastErr != nil {
			return false
		}
		indexed, found := entry.IndexedAttributes[dex.WorkQueuePermissionsIndexKey]
		if len(expected) == 0 {
			return !found
		}
		if !found {
			return false
		}
		var actual []string
		if lastErr = indexed.Decode(&actual); lastErr != nil {
			return false
		}
		return fmt.Sprint(actual) == fmt.Sprint(expected)
	}, 30*time.Second, 100*time.Millisecond, "permissions did not converge: %v", lastErr)

	permissionsToQuery := append([]string(nil), expected...)
	permissionsToQuery = append(permissionsToQuery,
		"permission-parent",
		"permission-self",
		"permission-x",
		"permission-y",
		"permission-z",
		"step.executed",
		"step.waiting",
	)
	sort.Strings(permissionsToQuery)
	permissionsToQuery = compactActionPermissions(permissionsToQuery)
	for _, permission := range permissionsToQuery {
		expectedToMatch := containsActionPermission(expected, permission)
		require.Eventually(t, func() bool {
			page, err := integClient.SearchFlows(
				integrationContext(t),
				fmt.Sprintf(
					"WorkflowId = '%s' AND %s = '%s'",
					flowID,
					dex.WorkQueuePermissionsIndexKey,
					permission,
				),
				100,
				"",
			)
			if err != nil {
				lastErr = err
				return false
			}
			matched := false
			for _, entry := range page.Flows {
				if entry.FlowID == flowID {
					matched = true
					break
				}
			}
			return matched == expectedToMatch
		}, 30*time.Second, 100*time.Millisecond, "permission query did not converge: %v", lastErr)
	}
}

func latestActionPermissionFlowEntry(
	t *testing.T,
	flowID string,
) (dex.SearchFlowEntry, error) {
	t.Helper()
	page, err := integClient.SearchFlows(
		integrationContext(t),
		fmt.Sprintf("WorkflowId = '%s'", flowID),
		100,
		"",
	)
	if err != nil {
		return dex.SearchFlowEntry{}, err
	}
	var latest dex.SearchFlowEntry
	found := false
	for _, entry := range page.Flows {
		if entry.FlowID == flowID && (!found || entry.StartedAt.After(latest.StartedAt)) {
			latest = entry
			found = true
		}
	}
	if !found {
		return dex.SearchFlowEntry{}, fmt.Errorf("Flow %q is not visible", flowID)
	}
	return latest, nil
}

func containsActionPermission(permissions []string, expected string) bool {
	for _, permission := range permissions {
		if permission == expected {
			return true
		}
	}
	return false
}

func compactActionPermissions(permissions []string) []string {
	if len(permissions) == 0 {
		return permissions
	}
	compacted := permissions[:1]
	for _, permission := range permissions[1:] {
		if permission != compacted[len(compacted)-1] {
			compacted = append(compacted, permission)
		}
	}
	return compacted
}
