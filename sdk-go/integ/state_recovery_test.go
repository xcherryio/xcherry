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

	"github.com/stretchr/testify/require"
	"github.com/superdurable/dex/sdk-go/dex"
)

type executeRecoveryFlow struct {
	emptyFlowSchema
}

func (executeRecoveryFlow) GetSteps() []dex.StepDef {
	return []dex.StepDef{
		dex.DefineStartStep(executeRecoveryFailStep{}),
		dex.DefineStep(executeRecoveryFinishStep{}),
	}
}

type executeRecoveryFailStep struct {
	dex.DefaultStepType
	dex.NoWaitFor[string]
}

func (executeRecoveryFailStep) GetStepOptions() *dex.StepOptions {
	return &dex.StepOptions{
		ExecuteRetry:   &dex.RetryPolicy{MaximumAttempts: 1},
		ExecuteFailure: dex.ProceedToOnExecuteFailure(executeRecoveryFinishStep{}, nil),
	}
}

func (executeRecoveryFailStep) Execute(dex.Context, string) (*dex.StepDecision, error) {
	return nil, fmt.Errorf("planned Execute failure")
}

type executeRecoveryFinishStep struct {
	dex.StepDefaultsNoWaitFor[string]
}

func (executeRecoveryFinishStep) Execute(
	dex.Context,
	string,
) (*dex.StepDecision, error) {
	return dex.GracefulComplete("this is flow step 2"), nil
}

func TestStepRecovery(t *testing.T) {
	// C21: server-zd rejects Step failure policies.
	skipIfServerZD(t, "Step failure policies")
	flowID := newFlowID(t, "execute-recovery")
	_, err := integClient.StartFlow(
		integrationContext(t),
		executeRecoveryFlow{},
		flowID,
		"unchanged input",
		dex.StartFlowOptions{},
	)
	require.NoError(t, err)
	result := waitForFlow(t, flowID, true)
	require.Equal(t, dex.FlowCompleted, result.Status)
	require.Len(t, result.Completions, 1)
	var output string
	require.NoError(t, result.Completions[0].Output.Decode(&output))
	require.Equal(t, "this is flow step 2", output)
}
