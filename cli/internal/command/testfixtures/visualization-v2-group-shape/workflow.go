// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package visualizationv2groupshape

import "github.com/superdurable/dex/sdk-go/dex"

var caseState = dex.DefineAttribute[string]("case-state")

type GroupShapeInput struct {
	Path string `json:"path"`
}

type GroupShapeFlow struct {
	dex.FlowDefaults
}

func (*GroupShapeFlow) GetSteps() []dex.StepDef {
	return []dex.StepDef{
		dex.DefineStartStep(BeginStep{}),
		dex.DefineStep(DecideStep{}),
		dex.DefineStep(LookUpOrderStep{}),
		dex.DefineStep(SearchHistoryStep{}),
		dex.DefineStep(WriteReplyStep{}),
		dex.DefineStep(SaveReplyStep{}),
		dex.DefineStep(NotifyOwnerStep{}),
		dex.DefineStep(CleanUpStep{}),
	}
}

func (flow *GroupShapeFlow) GetRPCs() []dex.RPCDef {
	return []dex.RPCDef{
		dex.DefineRPC(flow.GetDexSummary, nil),
		dex.DefineRPC(flow.GetDexDisplay, nil),
	}
}

func (*GroupShapeFlow) GetPersistenceSchema() dex.PersistenceSchema {
	return dex.PersistenceSchema{Attributes: []dex.AttributeDef{caseState}}
}

func (*GroupShapeFlow) GetDexSummary(
	_ dex.Context,
	_ dex.None,
) (*dex.RPCResult[map[string]any], error) {
	return &dex.RPCResult[map[string]any]{Output: map[string]any{}}, nil
}

// dex:field attribute-key:case-state value-type:string editable:false description:"Case state"
func (*GroupShapeFlow) GetDexDisplay(
	ctx dex.Context,
	_ dex.None,
) (*dex.RPCResult[map[string]any], error) {
	value, err := caseState.Get(ctx)
	if err != nil {
		return nil, err
	}
	return &dex.RPCResult[map[string]any]{Output: map[string]any{"case-state": value}}, nil
}

// dex:group group-id:begin group-label:"Begin"
// dex:explanation text:"Start the case and choose how to handle it."
type BeginStep struct {
	dex.StepDefaultsNoWaitFor[GroupShapeInput]
}

func (BeginStep) Execute(_ dex.Context, input GroupShapeInput) (*dex.StepDecision, error) {
	if input.Path == "clean" {
		return dex.GoTo(CleanUpStep{}, input), nil
	}
	return dex.GoTo(DecideStep{}, input), nil
}

// dex:group group-id:decide group-label:"Decide"
// dex:explanation text:"Choose which tool to use, or write the reply."
type DecideStep struct {
	dex.StepDefaultsNoWaitFor[GroupShapeInput]
}

func (DecideStep) Execute(_ dex.Context, input GroupShapeInput) (*dex.StepDecision, error) {
	switch input.Path {
	case "order":
		return dex.GoTo(LookUpOrderStep{}, input), nil
	case "history":
		return dex.GoTo(SearchHistoryStep{}, input), nil
	}
	return dex.GoTo(WriteReplyStep{}, input), nil
}

// dex:group group-id:tools group-label:"Tools"
// dex:explanation text:"Look up the order for the decision."
type LookUpOrderStep struct {
	dex.StepDefaultsNoWaitFor[GroupShapeInput]
}

func (LookUpOrderStep) Execute(_ dex.Context, input GroupShapeInput) (*dex.StepDecision, error) {
	return dex.GoTo(DecideStep{}, input), nil
}

// dex:group group-id:tools group-label:"Tools"
// dex:explanation text:"Search the customer history for the decision."
type SearchHistoryStep struct {
	dex.StepDefaultsNoWaitFor[GroupShapeInput]
}

func (SearchHistoryStep) Execute(_ dex.Context, input GroupShapeInput) (*dex.StepDecision, error) {
	return dex.GoTo(DecideStep{}, input), nil
}

// dex:group group-id:reply group-label:"Reply"
// dex:explanation text:"Write the reply."
type WriteReplyStep struct {
	dex.StepDefaultsNoWaitFor[GroupShapeInput]
}

func (WriteReplyStep) Execute(_ dex.Context, input GroupShapeInput) (*dex.StepDecision, error) {
	return dex.GoTo(SaveReplyStep{}, input), nil
}

// dex:group group-id:reply group-label:"Reply"
// dex:explanation text:"Save the written reply."
type SaveReplyStep struct {
	dex.StepDefaultsNoWaitFor[GroupShapeInput]
}

func (SaveReplyStep) Execute(_ dex.Context, input GroupShapeInput) (*dex.StepDecision, error) {
	return dex.GoTo(NotifyOwnerStep{}, input), nil
}

// dex:group group-id:wrap-up group-label:"Wrap up"
// dex:explanation text:"Notify the case owner and finish."
type NotifyOwnerStep struct {
	dex.StepDefaultsNoWaitFor[GroupShapeInput]
}

func (NotifyOwnerStep) Execute(_ dex.Context, _ GroupShapeInput) (*dex.StepDecision, error) {
	return dex.GracefulComplete("notified"), nil
}

// dex:group group-id:wrap-up group-label:"Wrap up"
// dex:explanation text:"Clean up a case that needs no reply."
type CleanUpStep struct {
	dex.StepDefaultsNoWaitFor[GroupShapeInput]
}

func (CleanUpStep) Execute(_ dex.Context, _ GroupShapeInput) (*dex.StepDecision, error) {
	return dex.GracefulComplete("cleaned"), nil
}
