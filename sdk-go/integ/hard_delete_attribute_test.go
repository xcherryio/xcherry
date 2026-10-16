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
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/superdurable/dex/sdk-go/dex"
)

// Secrets are derived from selectors so Step inputs, RPC inputs, and Channel messages never carry them.
const (
	hardDeleteSecretPrefix  = "go-sdk-hard-delete-secret-"
	hardDeleteInitialSecret = hardDeleteSecretPrefix + "initial"
	hardDeleteInitialToken  = hardDeleteSecretPrefix + "token"
	hardDeleteStepSelector  = "step"
	hardDeleteRPCSelector   = "rpc"
	hardDeleteTokenInstance = "primary"
)

var (
	hardDeleteSecret         = dex.DefineAttribute[string]("hard-delete-secret", dex.HardDeleteOnNull())
	hardDeleteTokens         = dex.DefineAttributeMap[hardDeleteToken]("hard-delete-tokens", dex.HardDeleteOnNull())
	hardDeleteProceedChannel = dex.DefineChannel[string]("hard-delete-proceed")
)

type hardDeleteToken struct {
	Value string
}

type hardDeleteAttributeFlow struct {
	dex.FlowDefaults
}

func (hardDeleteAttributeFlow) GetSteps() []dex.StepDef {
	return []dex.StepDef{
		dex.DefineStartStep(hardDeleteReadStep{}),
		dex.DefineStep(hardDeleteDeleteStep{}),
		dex.DefineStep(hardDeleteVerifyStep{}),
	}
}

func (flow hardDeleteAttributeFlow) GetRPCs() []dex.RPCDef {
	return []dex.RPCDef{
		dex.DefineRPC(flow.RotateSecret, nil),
		dex.DefineRPC(flow.CheckSecret, nil),
		dex.DefineRPC(flow.Proceed, nil),
	}
}

func (hardDeleteAttributeFlow) GetPersistenceSchema() dex.PersistenceSchema {
	return dex.PersistenceSchema{
		Attributes: []dex.AttributeDef{hardDeleteSecret, hardDeleteTokens},
		Channels:   []dex.ChannelDef{hardDeleteProceedChannel},
	}
}

func (hardDeleteAttributeFlow) RotateSecret(
	ctx dex.Context,
	selector string,
) (*dex.RPCResult[dex.None], error) {
	if err := hardDeleteSecret.Set(ctx, hardDeleteSecretPrefix+selector); err != nil {
		return nil, err
	}
	return &dex.RPCResult[dex.None]{}, nil
}

func (hardDeleteAttributeFlow) CheckSecret(
	ctx dex.Context,
	selector string,
) (*dex.RPCResult[bool], error) {
	secret, err := hardDeleteSecret.Get(ctx)
	if err != nil {
		return nil, err
	}
	return &dex.RPCResult[bool]{Output: secret == hardDeleteSecretPrefix+selector}, nil
}

func (hardDeleteAttributeFlow) Proceed(
	ctx dex.Context,
	selector string,
) (*dex.RPCResult[dex.None], error) {
	if err := hardDeleteProceedChannel.Publish(ctx, selector); err != nil {
		return nil, err
	}
	return &dex.RPCResult[dex.None]{}, nil
}

type hardDeleteReadStep struct {
	dex.StepDefaultsNoWaitFor[dex.None]
}

func (hardDeleteReadStep) Execute(ctx dex.Context, _ dex.None) (*dex.StepDecision, error) {
	secret, err := hardDeleteSecret.Get(ctx)
	if err != nil {
		return nil, err
	}
	if secret != hardDeleteInitialSecret {
		return nil, fmt.Errorf("unexpected initial hard-delete secret")
	}
	token, err := hardDeleteTokens.Get(ctx, hardDeleteTokenInstance)
	if err != nil {
		return nil, err
	}
	if token.Value != hardDeleteInitialToken {
		return nil, fmt.Errorf("unexpected initial hard-delete token")
	}
	if err := hardDeleteSecret.Set(ctx, hardDeleteSecretPrefix+hardDeleteStepSelector); err != nil {
		return nil, err
	}
	return dex.GoTo(hardDeleteDeleteStep{}, nil), nil
}

func (hardDeleteReadStep) GetStepOptions() *dex.StepOptions {
	return &dex.StepOptions{ExecuteLoadAttributeMaps: []dex.AttributeDef{hardDeleteTokens}}
}

type hardDeleteDeleteStep struct {
	dex.StepDefaults
}

func (hardDeleteDeleteStep) WaitFor(dex.Context, dex.None) (*dex.Wait, error) {
	return dex.Until(hardDeleteProceedChannel.ForOne()), nil
}

func (hardDeleteDeleteStep) Execute(ctx dex.Context, _ dex.None) (*dex.StepDecision, error) {
	selectors, err := hardDeleteProceedChannel.GetConditionResults(ctx)
	if err != nil {
		return nil, err
	}
	secret, err := hardDeleteSecret.Get(ctx)
	if err != nil {
		return nil, err
	}
	if len(selectors) != 1 || secret != hardDeleteSecretPrefix+selectors[0] {
		return nil, fmt.Errorf("hard-delete secret does not match the latest write")
	}
	if err := hardDeleteSecret.Delete(ctx); err != nil {
		return nil, err
	}
	if err := hardDeleteTokens.Delete(ctx, hardDeleteTokenInstance); err != nil {
		return nil, err
	}
	return dex.GoTo(hardDeleteVerifyStep{}, nil), nil
}

func (hardDeleteDeleteStep) GetStepOptions() *dex.StepOptions {
	return &dex.StepOptions{ExecuteLoadAttributeMaps: []dex.AttributeDef{hardDeleteTokens}}
}

type hardDeleteVerifyStep struct {
	dex.StepDefaultsNoWaitFor[dex.None]
}

func (hardDeleteVerifyStep) Execute(ctx dex.Context, _ dex.None) (*dex.StepDecision, error) {
	var notFound *dex.AttributeNotFoundError
	if _, err := hardDeleteSecret.Get(ctx); !errors.As(err, &notFound) {
		return nil, fmt.Errorf("deleted hard-delete secret is still readable: %v", err)
	}
	if _, err := hardDeleteTokens.Get(ctx, hardDeleteTokenInstance); !errors.As(err, &notFound) {
		return nil, fmt.Errorf("deleted hard-delete token is still readable: %v", err)
	}
	return dex.GracefulComplete("deleted"), nil
}

func (hardDeleteVerifyStep) GetStepOptions() *dex.StepOptions {
	return &dex.StepOptions{ExecuteLoadAttributeMaps: []dex.AttributeDef{hardDeleteTokens}}
}

func TestHardDeleteAttributeFlow(t *testing.T) {
	ctx := integrationContext(t)
	flow := hardDeleteAttributeFlow{}
	flowID := newFlowID(t, "hard-delete")
	initialSecret, err := dex.InitialAttribute(hardDeleteSecret, hardDeleteInitialSecret)
	require.NoError(t, err)
	initialToken, err := dex.InitialAttributeMapValue(
		hardDeleteTokens,
		hardDeleteTokenInstance,
		hardDeleteToken{Value: hardDeleteInitialToken},
	)
	require.NoError(t, err)
	_, err = integClient.StartFlow(ctx, flow, flowID, nil, dex.StartFlowOptions{
		Attributes: []dex.InitialAttributeDef{initialSecret, initialToken},
	})
	require.NoError(t, err)

	requireHardDeleteSecret(t, flowID, hardDeleteStepSelector)
	var noOutput dex.None
	require.NoError(t, integClient.InvokeRPC(ctx, flowID, flow.RotateSecret, hardDeleteRPCSelector, &noOutput))
	requireHardDeleteSecret(t, flowID, hardDeleteRPCSelector)
	require.NoError(t, integClient.InvokeRPC(ctx, flowID, flow.Proceed, hardDeleteRPCSelector, &noOutput))

	result := waitForFlow(t, flowID, true)
	require.Equal(t, dex.FlowCompleted, result.Status)
	var output string
	require.NoError(t, result.DecodeSingleOutput(&output))
	require.Equal(t, "deleted", output)
}

func TestHardDeleteAttributeRejectsWaitForAttributeMatch(t *testing.T) {
	var matched string
	err := integClient.WaitForAttributeMatch(
		integrationContext(t),
		newFlowID(t, "hard-delete-wait"),
		hardDeleteSecret,
		dex.AttributeMatchEqual("value"),
		&matched,
		dex.WaitForAttributeOptions{RequestID: uuid.NewString()},
	)
	require.ErrorContains(t, err, "HardDeleteOnNull")
}

type hardDeleteInvalidDefinitionFlow struct {
	dex.FlowDefaults
	attribute dex.AttributeDef
}

func (hardDeleteInvalidDefinitionFlow) GetSteps() []dex.StepDef {
	return []dex.StepDef{dex.DefineStartStep(hardDeleteVerifyStep{})}
}

func (flow hardDeleteInvalidDefinitionFlow) GetPersistenceSchema() dex.PersistenceSchema {
	return dex.PersistenceSchema{Attributes: []dex.AttributeDef{flow.attribute}}
}

func TestHardDeleteAttributeRejectsInvalidDefinitions(t *testing.T) {
	invalidDefinitions := map[string]dex.AttributeDef{
		"integer": dex.DefineAttribute[int64]("hard-delete-int", dex.HardDeleteOnNull()),
		"boolean": dex.DefineAttributeMap[bool]("hard-delete-bool", dex.HardDeleteOnNull()),
		"float":   dex.DefineAttribute[*float64]("hard-delete-float", dex.HardDeleteOnNull()),
		"indexed": dex.DefineAttribute[string](
			"hard-delete-indexed",
			dex.HardDeleteOnNull(),
			dex.Indexed(dex.AttributeIndex{Type: dex.IndexKeyword}),
		),
		"synced": dex.DefineAttribute[string](
			"hard-delete-synced",
			dex.HardDeleteOnNull(),
			dex.SyncToAttributeStore(),
		),
	}
	for name, definition := range invalidDefinitions {
		t.Run(name, func(t *testing.T) {
			_, err := dex.NewRegistry([]dex.Flow{hardDeleteInvalidDefinitionFlow{attribute: definition}})
			require.ErrorContains(t, err, "HardDeleteOnNull")
		})
	}
}

func requireHardDeleteSecret(t *testing.T, flowID string, selector string) {
	t.Helper()
	require.Eventually(t, func() bool {
		var isMatched bool
		err := integClient.InvokeRPC(
			integrationContext(t),
			flowID,
			hardDeleteAttributeFlow{}.CheckSecret,
			selector,
			&isMatched,
		)
		return err == nil && isMatched
	}, 20*time.Second, 200*time.Millisecond, "hard-delete secret never matched selector %q", selector)
}
