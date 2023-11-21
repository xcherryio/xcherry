// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: BUSL-1.1

package integTests

import (
	"testing"

	"github.com/xcherryio/sdk-go/integTests/global_attribute"

	"github.com/xcherryio/sdk-go/integTests/basic"
	"github.com/xcherryio/sdk-go/integTests/failure_recovery"
	"github.com/xcherryio/sdk-go/integTests/multi_states"
	"github.com/xcherryio/sdk-go/integTests/process_timeout"
	"github.com/xcherryio/sdk-go/integTests/state_decision"
	"github.com/xcherryio/sdk-go/integTests/stateretry"
)

func TestIOProcess(t *testing.T) {
	basic.TestStartIOProcess(t, client)
}

func TestStateBackoffRetry(t *testing.T) {
	stateretry.TestBackoff(t, client)
}

func TestTerminateProcess(t *testing.T) {
	multi_states.TestTerminateMultiStatesProcess(t, client)
}

func TestStopProcessByFail(t *testing.T) {
	multi_states.TestFailMultiStatesProcess(t, client)
}

func TestStateDecision(t *testing.T) {
	state_decision.TestGracefulCompleteProcess(t, client)
	state_decision.TestForceCompleteProcess(t, client)
	state_decision.TestForceFailProcess(t, client)
	state_decision.TestDeadEndProcess(t, client)
}

func TestProcessIdReusePolicyDisallowReuse(t *testing.T) {
	basic.TestProcessIdReusePolicyDisallowReuse(t, client)
}

func TestProcessIdReusePolicyAllowIfNoRunning(t *testing.T) {
	basic.TestProcessIdReusePolicyAllowIfNoRunning(t, client)
}

func TestProcessIdReusePolicyTerminateIfRunning(t *testing.T) {
	basic.TestProcessIdReusePolicyTerminateIfRunning(t, client)
}

func TestProcessIdReusePolicyAllowIfPreviousExitAbnormallyCase1(t *testing.T) {
	basic.TestProcessIdReusePolicyAllowIfPreviousExitAbnormallyCase1(t, client)
}

func TestProcessIdReusePolicyAllowIfPreviousExitAbnormallyCase2(t *testing.T) {
	basic.TestProcessIdReusePolicyAllowIfPreviousExitAbnormallyCase2(t, client)
}

func TestStateFailureRecoveryExecute(t *testing.T) {
	failure_recovery.TestStateFailureRecoveryTestExecuteProcess(t, client)
}

func TestStateFailureRecoveryWaitUntil(t *testing.T) {
	failure_recovery.TestStateFailureRecoveryTestWaitUntilProcess(t, client)
}

func TestStateFailureRecoveryExecuteNoWaitUntil(t *testing.T) {
	failure_recovery.TestStateFailureRecoveryTestExecuteNoWaitUntilProcess(t, client)
}

func TestStateFailureRecoveryExecuteFailedAtStart(t *testing.T) {
	failure_recovery.TestStateFailureRecoveryTestExecuteFailedAtStartProcess(t, client)
}

func TestGlobalAttributesWithSingleTable(t *testing.T) {
	global_attribute.TestGlobalAttributesWithSingleTable(t, client)
}

func TestGlobalAttributesWithMultiTables(t *testing.T) {
	global_attribute.TestGlobalAttributesWithMultiTables(t, client)
}

func TestProcessTimeoutCase1(t *testing.T) {
	process_timeout.TestStartTimeoutProcessCase1(t, client)
}

func TestProcessTimeoutCase2(t *testing.T) {
	process_timeout.TestStartTimeoutProcessCase2(t, client)
}

func TestProcessTimeoutCase3(t *testing.T) {
	process_timeout.TestStartTimeoutProcessCase3(t, client)
}

func TestProcessTimeoutCase4(t *testing.T) {
	process_timeout.TestStartTimeoutProcessCase4(t, client)
}
