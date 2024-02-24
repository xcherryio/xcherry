// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: Apache-2.0

package sqltest

import (
	"context"
	"fmt"
	"github.com/xcherryio/apis/goapi/xcapi"
	"github.com/xcherryio/xcherry/persistence/data_models"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/xcherryio/xcherry/common/ptr"
	"github.com/xcherryio/xcherry/persistence"
)

func SQLStateFailureRecoveryTest(t *testing.T, ass *assert.Assertions, store persistence.ProcessStore) {
	ctx := context.Background()

	processId := fmt.Sprintf("test-prcid-%v", time.Now().String())
	input := createTestInput()

	// Start the process and verify it started correctly.
	prcExeId := startProcess(ctx, t, ass, store, namespace, processId, input)

	// Describe the process.
	describeProcess(ctx, t, ass, store, namespace, processId, xcapi.RUNNING)

	// Test waitUntil API execution
	// Check initial immediate tasks.
	minSeq, maxSeq, immediateTasks := checkAndGetImmediateTasks(ctx, t, ass, store, 2)
	task := immediateTasks[0]
	verifyImmediateTaskNoInfo(ass, task, data_models.ImmediateTaskTypeWaitUntil, stateId1+"-1")
	visibilityTask := immediateTasks[1]
	ass.Equal(data_models.ImmediateTaskTypeVisibility, visibilityTask.TaskType)

	// Delete and verify immediate tasks are deleted.
	deleteAndVerifyImmediateTasksDeleted(ctx, t, ass, store, minSeq, maxSeq)

	// Prepare state execution.
	prep := prepareStateExecution(ctx, t, store, prcExeId, task.StateId, task.StateIdSequence)
	verifyStateExecution(ass, prep, processId, input, data_models.StateExecutionStatusWaitUntilRunning)

	// Complete 'WaitUntil' execution.
	completeWaitUntilExecution(ctx, t, ass, store, prcExeId, task, prep)

	// Check initial immediate tasks.
	minSeq, maxSeq, immediateTasks = checkAndGetImmediateTasks(ctx, t, ass, store, 1)
	task = immediateTasks[0]
	verifyImmediateTaskNoInfo(ass, task, data_models.ImmediateTaskTypeExecute, stateId1+"-1")

	// Delete and verify immediate tasks are deleted.
	deleteAndVerifyImmediateTasksDeleted(ctx, t, ass, store, minSeq, maxSeq)

	// Prepare state execution for Execute API
	prep = prepareStateExecution(ctx, t, store, prcExeId, task.StateId, task.StateIdSequence)
	verifyStateExecution(ass, prep, processId, input, data_models.StateExecutionStatusExecuteRunning)

	decision1 := xcapi.StateDecision{
		NextStates: []xcapi.StateMovement{
			{
				StateId:    stateId2,
				StateInput: xcapi.NewEncodedObject(input.Encoding, input.Data+"-"+stateId1+"-1"),
				StateConfig: &xcapi.AsyncStateConfig{
					SkipWaitUntil: ptr.Any(true),
					StateFailureRecoveryOptions: &xcapi.StateFailureRecoveryOptions{
						Policy:                         xcapi.PROCEED_TO_CONFIGURED_STATE,
						StateFailureProceedStateId:     ptr.Any(stateId1),
						StateFailureProceedStateConfig: &xcapi.AsyncStateConfig{SkipWaitUntil: ptr.Any(true)},
					},
				},
			},
		},
	}
	// Complete 'Execute' execution.
	completeExecuteExecution(ctx, t, ass, store, prcExeId, task, prep, decision1, true)

	minSeq, maxSeq, immediateTasks = checkAndGetImmediateTasks(ctx, t, ass, store, 1)
	task = immediateTasks[0]
	verifyImmediateTaskNoInfo(ass, task, data_models.ImmediateTaskTypeExecute, stateId2+"-1")

	// Delete and verify immediate tasks are deleted.
	deleteAndVerifyImmediateTasksDeleted(ctx, t, ass, store, minSeq, maxSeq)

	// Prepare state execution for Execute API again
	prep = prepareStateExecution(ctx, t, store, prcExeId, task.StateId, task.StateIdSequence)
	verifyStateExecution(
		ass,
		prep,
		processId,
		*xcapi.NewEncodedObject(input.Encoding, input.Data+"-"+stateId1+"-1"),
		data_models.StateExecutionStatusExecuteRunning)

	recoverFromFailure(
		t,
		ctx,
		ass,
		store,
		namespace,
		prcExeId,
		*prep,
		data_models.StateExecutionId{
			StateId:         stateId2,
			StateIdSequence: 1,
		},
		xcapi.EXECUTE_API,
		stateId1,
		&xcapi.AsyncStateConfig{
			SkipWaitUntil: ptr.Any(true),
		},
		*xcapi.NewEncodedObject(input.Encoding, input.Data+"-"+stateId1+"-1"+"-"+stateId2+"-1"),
	)

	prep = prepareStateExecution(ctx, t, store, prcExeId, stateId2, 1)
	verifyStateExecution(ass, prep, processId, *xcapi.NewEncodedObject("test-encoding", input.Data+"-"+stateId1+"-1"), data_models.StateExecutionStatusFailed)

	prep = prepareStateExecution(ctx, t, store, prcExeId, stateId1, 2)
	verifyStateExecution(
		ass,
		prep,
		processId,
		*xcapi.NewEncodedObject("test-encoding", input.Data+"-"+stateId1+"-1"+"-"+stateId2+"-1"),
		data_models.StateExecutionStatusExecuteRunning)
}
