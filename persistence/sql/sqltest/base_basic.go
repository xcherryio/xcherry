// Copyright (c) 2023 XDBLab Organization
// SPDX-License-Identifier: BUSL-1.1

package sqltest

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/xdblab/xdb-apis/goapi/xdbapi"
	"github.com/xdblab/xdb/common/ptr"
	"github.com/xdblab/xdb/persistence"
)

func CleanupEnv(ass *assert.Assertions, store persistence.ProcessStore) {
	err := store.CleanUpTasksForTest(context.Background(), persistence.DefaultShardId)
	ass.Nil(err)
}

func SQLBasicTest(t *testing.T, ass *assert.Assertions, store persistence.ProcessStore) {
	ctx := context.Background()
	namespace := "test-ns"
	processId := fmt.Sprintf("test-prcid-%v", time.Now().String())
	input := createTestInput()

	// Start the process and verify it started correctly.
	prcExeId := startProcess(ctx, t, ass, store, namespace, processId, input)

	// Try to start the process again and verify the behavior.
	retryStartProcessForFailure(ctx, t, ass, store, namespace, processId, input)

	// Describe the process.
	describeProcess(ctx, t, ass, store, namespace, processId, xdbapi.RUNNING)

	// Test waitUntil API execution
	// Check initial immediate tasks.
	minSeq, maxSeq, immediateTasks := checkAndGetImmediateTasks(ctx, t, ass, store, 1)
	task := immediateTasks[0]
	verifyImmediateTaskNoInfo(ass, task, persistence.ImmediateTaskTypeWaitUntil, stateId1+"-1")

	// Delete and verify immediate tasks are deleted.
	deleteAndVerifyImmediateTasksDeleted(ctx, t, ass, store, minSeq, maxSeq)

	// Prepare state execution.
	prep := prepareStateExecution(ctx, t, store, prcExeId, task.StateId, task.StateIdSequence)
	verifyStateExecution(ass, prep, processId, input, persistence.StateExecutionStatusWaitUntilRunning)

	// Complete 'WaitUntil' execution.
	completeWaitUntilExecution(ctx, t, ass, store, prcExeId, task, prep)

	// Check initial immediate tasks.
	minSeq, maxSeq, immediateTasks = checkAndGetImmediateTasks(ctx, t, ass, store, 1)
	task = immediateTasks[0]
	verifyImmediateTaskNoInfo(ass, task, persistence.ImmediateTaskTypeExecute, stateId1+"-1")

	// Delete and verify immediate tasks are deleted.
	deleteAndVerifyImmediateTasksDeleted(ctx, t, ass, store, minSeq, maxSeq)

	// Prepare state execution for Execute API
	prep = prepareStateExecution(ctx, t, store, prcExeId, task.StateId, task.StateIdSequence)
	verifyStateExecution(ass, prep, processId, input, persistence.StateExecutionStatusExecuteRunning)

	decision1 := xdbapi.StateDecision{
		NextStates: []xdbapi.StateMovement{
			{
				StateId: stateId2,
				// no input, skip waitUntil
				StateConfig: &xdbapi.AsyncStateConfig{SkipWaitUntil: ptr.Any(true)},
			},
			{
				StateId: stateId1, // use the same stateId
				// no input, skip waitUntil
				StateConfig: &xdbapi.AsyncStateConfig{SkipWaitUntil: ptr.Any(true)},
				StateInput:  &input,
			},
		},
	}
	// Complete 'Execute' execution.
	completeExecuteExecution(ctx, t, ass, store, prcExeId, task, prep, decision1, true)

	minSeq, maxSeq, immediateTasks = checkAndGetImmediateTasks(ctx, t, ass, store, 2)
	task = immediateTasks[0]
	verifyImmediateTaskNoInfo(ass, task, persistence.ImmediateTaskTypeExecute, stateId2+"-1")
	task = immediateTasks[1]
	verifyImmediateTaskNoInfo(ass, task, persistence.ImmediateTaskTypeExecute, stateId1+"-2")

	// Delete and verify immediate tasks are deleted.
	deleteAndVerifyImmediateTasksDeleted(ctx, t, ass, store, minSeq, maxSeq)

	// Prepare state execution for Execute API again
	prep = prepareStateExecution(ctx, t, store, prcExeId, task.StateId, task.StateIdSequence)
	verifyStateExecution(ass, prep, processId, input, persistence.StateExecutionStatusExecuteRunning)
	decision2 := xdbapi.StateDecision{
		ThreadCloseDecision: &xdbapi.ThreadCloseDecision{
			CloseType: xdbapi.FORCE_COMPLETE_PROCESS,
		},
	}
	completeExecuteExecution(ctx, t, ass, store, prcExeId, task, prep, decision2, false)

	// Verify stateId2 was aborted and process has completed
	prep = prepareStateExecution(ctx, t, store, prcExeId, stateId2, 1)
	verifyStateExecution(ass, prep, processId, createEmptyEncodedObject(), persistence.StateExecutionStatusAborted)
	describeProcess(ctx, t, ass, store, namespace, processId, xdbapi.COMPLETED)
}

func SQLProcessIdReusePolicyAllowIfPreviousExitAbnormally(
	t *testing.T, ass *assert.Assertions, store persistence.ProcessStore,
) {
	ctx := context.Background()
	namespace := "test-ns"
	processId := fmt.Sprintf("test-prcid-%v", time.Now().String())
	input := createTestInput()

	// Start the process and verify it started correctly.
	startProcess(ctx, t, ass, store, namespace, processId, input)

	// Try to start the process again and verify the behavior.
	retryStartProcessForFailure(ctx, t, ass, store, namespace, processId, input)

	// Describe the process.
	describeProcess(ctx, t, ass, store, namespace, processId, xdbapi.RUNNING)

	// stop the process with temerminated
	terminateProcess(ctx, t, ass, store, namespace, processId)

	// Describe the process, verify it's terminated
	describeProcess(ctx, t, ass, store, namespace, processId, xdbapi.TERMINATED)

	// start the process with allow if previous exit abnormally
	startProcessWithAllowIfPreviousExitAbnormally(ctx, t, ass, store, namespace, processId, input)
}

func SQLProcessIdReusePolicyDefault(t *testing.T, ass *assert.Assertions, store persistence.ProcessStore) {
	ctx := context.Background()
	namespace := "test-ns"
	processId := fmt.Sprintf("test-prcid-%v", time.Now().String())
	input := createTestInput()

	// Start the process and verify it started correctly.
	prcExeId := startProcess(ctx, t, ass, store, namespace, processId, input)

	// Try to start the process again and verify the behavior.
	retryStartProcessForFailure(ctx, t, ass, store, namespace, processId, input)

	// Describe the process.
	describeProcess(ctx, t, ass, store, namespace, processId, xdbapi.RUNNING)

	// stop the process with temerminated
	terminateProcess(ctx, t, ass, store, namespace, processId)

	// Describe the process, verify it's terminated
	describeProcess(ctx, t, ass, store, namespace, processId, xdbapi.TERMINATED)

	// start with default process id reuse policy and it should start correctly
	prcExeID2 := startProcess(ctx, t, ass, store, namespace, processId, input)

	ass.NotEqual(prcExeId.String(), prcExeID2.String())
}

func SQLProcessIdReusePolicyTerminateIfRunning(t *testing.T, ass *assert.Assertions, store persistence.ProcessStore) {
	ctx := context.Background()
	namespace := "test-ns"
	processId := fmt.Sprintf("test-prcid-%v", time.Now().String())
	input := createTestInput()

	// Start the process and verify it started correctly.
	prcExeId := startProcess(ctx, t, ass, store, namespace, processId, input)

	// Try to start the process again and verify the behavior.
	retryStartProcessForFailure(ctx, t, ass, store, namespace, processId, input)

	// Describe the process.
	describeProcess(ctx, t, ass, store, namespace, processId, xdbapi.RUNNING)

	prcExeID2 := startProcessWithTerminateIfRunningPolicy(ctx, t, ass, store, namespace, processId, input)

	ass.NotEqual(prcExeId.String(), prcExeID2.String())
}

func SQLProcessIdReusePolicyDisallowReuseTest(t *testing.T, ass *assert.Assertions, store persistence.ProcessStore) {
	ctx := context.Background()
	namespace := "test-ns"
	processId := fmt.Sprintf("test-prcid-%v", time.Now().String())
	input := createTestInput()

	// Start the process and verify it started correctly.
	prcExeId := startProcess(ctx, t, ass, store, namespace, processId, input)

	// Try to start the process again and verify the behavior.
	retryStartProcessForFailure(ctx, t, ass, store, namespace, processId, input)

	// Describe the process.
	describeProcess(ctx, t, ass, store, namespace, processId, xdbapi.RUNNING)

	// Test waitUntil API execution
	// Check initial immediate tasks.
	minSeq, maxSeq, immediateTasks := checkAndGetImmediateTasks(ctx, t, ass, store, 1)
	task := immediateTasks[0]
	verifyImmediateTaskNoInfo(ass, task, persistence.ImmediateTaskTypeWaitUntil, stateId1+"-1")

	// Delete and verify immediate tasks are deleted.
	deleteAndVerifyImmediateTasksDeleted(ctx, t, ass, store, minSeq, maxSeq)

	// Prepare state execution.
	prep := prepareStateExecution(ctx, t, store, prcExeId, task.StateId, task.StateIdSequence)
	verifyStateExecution(ass, prep, processId, input, persistence.StateExecutionStatusWaitUntilRunning)

	// Complete 'WaitUntil' execution.
	completeWaitUntilExecution(ctx, t, ass, store, prcExeId, task, prep)

	// Check initial immediate tasks.
	minSeq, maxSeq, immediateTasks = checkAndGetImmediateTasks(ctx, t, ass, store, 1)
	task = immediateTasks[0]
	verifyImmediateTaskNoInfo(ass, task, persistence.ImmediateTaskTypeExecute, stateId1+"-1")

	// Delete and verify immediate tasks are deleted.
	deleteAndVerifyImmediateTasksDeleted(ctx, t, ass, store, minSeq, maxSeq)

	// Prepare state execution for Execute API
	prep = prepareStateExecution(ctx, t, store, prcExeId, task.StateId, task.StateIdSequence)
	verifyStateExecution(ass, prep, processId, input, persistence.StateExecutionStatusExecuteRunning)

	decision1 := xdbapi.StateDecision{
		NextStates: []xdbapi.StateMovement{
			{
				StateId: stateId2,
				// no input, skip waitUntil
				StateConfig: &xdbapi.AsyncStateConfig{SkipWaitUntil: ptr.Any(true)},
			},
			{
				StateId: stateId1, // use the same stateId
				// no input, skip waitUntil
				StateConfig: &xdbapi.AsyncStateConfig{SkipWaitUntil: ptr.Any(true)},
				StateInput:  &input,
			},
		},
	}
	// Complete 'Execute' execution.
	completeExecuteExecution(ctx, t, ass, store, prcExeId, task, prep, decision1, true)

	minSeq, maxSeq, immediateTasks = checkAndGetImmediateTasks(ctx, t, ass, store, 2)
	task = immediateTasks[0]
	verifyImmediateTaskNoInfo(ass, task, persistence.ImmediateTaskTypeExecute, stateId2+"-1")
	task = immediateTasks[1]
	verifyImmediateTaskNoInfo(ass, task, persistence.ImmediateTaskTypeExecute, stateId1+"-2")

	// Delete and verify immediate tasks are deleted.
	deleteAndVerifyImmediateTasksDeleted(ctx, t, ass, store, minSeq, maxSeq)

	// Prepare state execution for Execute API again
	prep = prepareStateExecution(ctx, t, store, prcExeId, task.StateId, task.StateIdSequence)
	verifyStateExecution(ass, prep, processId, input, persistence.StateExecutionStatusExecuteRunning)
	decision2 := xdbapi.StateDecision{
		ThreadCloseDecision: &xdbapi.ThreadCloseDecision{
			CloseType: xdbapi.FORCE_COMPLETE_PROCESS,
		},
	}
	completeExecuteExecution(ctx, t, ass, store, prcExeId, task, prep, decision2, false)

	// Verify stateId2 was aborted and process has completed
	prep = prepareStateExecution(ctx, t, store, prcExeId, stateId2, 1)
	verifyStateExecution(ass, prep, processId, createEmptyEncodedObject(), persistence.StateExecutionStatusAborted)
	describeProcess(ctx, t, ass, store, namespace, processId, xdbapi.COMPLETED)

	// try to start with disallow_reuse policy, and verify it's returning already started
	startProcessWithDisallowReusePolicy(ctx, t, ass, store, namespace, processId, input)
}

func SQLProcessIdReusePolicyAllowIfNoRunning(
	t *testing.T, ass *assert.Assertions, store persistence.ProcessStore,
) {
	ctx := context.Background()
	namespace := "test-ns"
	processId := fmt.Sprintf("test-prcid-%v", time.Now().String())
	input := createTestInput()

	// Start the process and verify it started correctly.
	prcExeId := startProcess(ctx, t, ass, store, namespace, processId, input)

	// Try to start the process again and verify the behavior.
	retryStartProcessForFailure(ctx, t, ass, store, namespace, processId, input)

	// Describe the process.
	describeProcess(ctx, t, ass, store, namespace, processId, xdbapi.RUNNING)

	// Test waitUntil API execution
	// Check initial immediate tasks.
	minSeq, maxSeq, immediateTasks := checkAndGetImmediateTasks(ctx, t, ass, store, 1)
	task := immediateTasks[0]
	verifyImmediateTaskNoInfo(ass, task, persistence.ImmediateTaskTypeWaitUntil, stateId1+"-1")

	// Delete and verify immediate tasks are deleted.
	deleteAndVerifyImmediateTasksDeleted(ctx, t, ass, store, minSeq, maxSeq)

	// Prepare state execution.
	prep := prepareStateExecution(ctx, t, store, prcExeId, task.StateId, task.StateIdSequence)
	verifyStateExecution(ass, prep, processId, input, persistence.StateExecutionStatusWaitUntilRunning)

	// Complete 'WaitUntil' execution.
	completeWaitUntilExecution(ctx, t, ass, store, prcExeId, task, prep)

	// Check initial immediate tasks.
	minSeq, maxSeq, immediateTasks = checkAndGetImmediateTasks(ctx, t, ass, store, 1)
	task = immediateTasks[0]
	verifyImmediateTaskNoInfo(ass, task, persistence.ImmediateTaskTypeExecute, stateId1+"-1")

	// Delete and verify immediate tasks are deleted.
	deleteAndVerifyImmediateTasksDeleted(ctx, t, ass, store, minSeq, maxSeq)

	// Prepare state execution for Execute API
	prep = prepareStateExecution(ctx, t, store, prcExeId, task.StateId, task.StateIdSequence)
	verifyStateExecution(ass, prep, processId, input, persistence.StateExecutionStatusExecuteRunning)

	decision1 := xdbapi.StateDecision{
		NextStates: []xdbapi.StateMovement{
			{
				StateId: stateId2,
				// no input, skip waitUntil
				StateConfig: &xdbapi.AsyncStateConfig{SkipWaitUntil: ptr.Any(true)},
			},
			{
				StateId: stateId1, // use the same stateId
				// no input, skip waitUntil
				StateConfig: &xdbapi.AsyncStateConfig{SkipWaitUntil: ptr.Any(true)},
				StateInput:  &input,
			},
		},
	}
	// Complete 'Execute' execution.
	completeExecuteExecution(ctx, t, ass, store, prcExeId, task, prep, decision1, true)

	minSeq, maxSeq, immediateTasks = checkAndGetImmediateTasks(ctx, t, ass, store, 2)
	task = immediateTasks[0]
	verifyImmediateTaskNoInfo(ass, task, persistence.ImmediateTaskTypeExecute, stateId2+"-1")
	task = immediateTasks[1]
	verifyImmediateTaskNoInfo(ass, task, persistence.ImmediateTaskTypeExecute, stateId1+"-2")

	// Delete and verify immediate tasks are deleted.
	deleteAndVerifyImmediateTasksDeleted(ctx, t, ass, store, minSeq, maxSeq)

	// Prepare state execution for Execute API again
	prep = prepareStateExecution(ctx, t, store, prcExeId, task.StateId, task.StateIdSequence)
	verifyStateExecution(ass, prep, processId, input, persistence.StateExecutionStatusExecuteRunning)
	decision2 := xdbapi.StateDecision{
		ThreadCloseDecision: &xdbapi.ThreadCloseDecision{
			CloseType: xdbapi.FORCE_COMPLETE_PROCESS,
		},
	}
	completeExecuteExecution(ctx, t, ass, store, prcExeId, task, prep, decision2, false)

	// Verify stateId2 was aborted and process has completed
	prep = prepareStateExecution(ctx, t, store, prcExeId, stateId2, 1)
	verifyStateExecution(ass, prep, processId, createEmptyEncodedObject(), persistence.StateExecutionStatusAborted)
	describeProcess(ctx, t, ass, store, namespace, processId, xdbapi.COMPLETED)

	// start with allow if no running,
	startProcessWithAllowIfNoRunningPolicy(ctx, t, ass, store, namespace, processId, input)
}

func SQLGracefulCompleteTest(t *testing.T, ass *assert.Assertions, store persistence.ProcessStore) {
	ctx := context.Background()
	namespace := "test-ns-2"
	processId := fmt.Sprintf("test-graceful-complete-%v", time.Now().String())
	input := createTestInput()

	// Start the process and verify it started correctly.
	prcExeId := startProcess(ctx, t, ass, store, namespace, processId, input)

	// Get the task
	minSeq, maxSeq, immediateTasks := checkAndGetImmediateTasks(ctx, t, ass, store, 1)
	task := immediateTasks[0]

	// Delete and verify immediate tasks are deleted.
	deleteAndVerifyImmediateTasksDeleted(ctx, t, ass, store, minSeq, maxSeq)

	// Prepare state execution.
	prep := prepareStateExecution(ctx, t, store, prcExeId, task.StateId, task.StateIdSequence)
	verifyStateExecution(ass, prep, processId, input, persistence.StateExecutionStatusWaitUntilRunning)

	// Complete 'WaitUntil' execution.
	completeWaitUntilExecution(ctx, t, ass, store, prcExeId, task, prep)

	// Check initial immediate tasks.
	minSeq, maxSeq, immediateTasks = checkAndGetImmediateTasks(ctx, t, ass, store, 1)
	task = immediateTasks[0]
	verifyImmediateTaskNoInfo(ass, task, persistence.ImmediateTaskTypeExecute, stateId1+"-1")

	// Delete and verify immediate tasks are deleted.
	deleteAndVerifyImmediateTasksDeleted(ctx, t, ass, store, minSeq, maxSeq)

	// Prepare state execution for Execute API
	prep = prepareStateExecution(ctx, t, store, prcExeId, task.StateId, task.StateIdSequence)
	verifyStateExecution(ass, prep, processId, input, persistence.StateExecutionStatusExecuteRunning)

	decision1 := xdbapi.StateDecision{
		NextStates: []xdbapi.StateMovement{
			{
				StateId: stateId2,
				// no input, skip waitUntil
				StateConfig: &xdbapi.AsyncStateConfig{SkipWaitUntil: ptr.Any(true)},
			},
			{
				StateId: stateId1, // use the same stateId
				// no input, skip waitUntil
				StateConfig: &xdbapi.AsyncStateConfig{SkipWaitUntil: ptr.Any(true)},
			},
		},
	}
	// Complete 'Execute' execution.
	completeExecuteExecution(ctx, t, ass, store, prcExeId, task, prep, decision1, true)

	minSeq, maxSeq, immediateTasks = checkAndGetImmediateTasks(ctx, t, ass, store, 2)
	task = immediateTasks[0]
	verifyImmediateTaskNoInfo(ass, task, persistence.ImmediateTaskTypeExecute, stateId2+"-1")
	task = immediateTasks[1]
	verifyImmediateTaskNoInfo(ass, task, persistence.ImmediateTaskTypeExecute, stateId1+"-2")

	// Delete and verify immediate tasks are deleted.
	deleteAndVerifyImmediateTasksDeleted(ctx, t, ass, store, minSeq, maxSeq)

	// Prepare state execution for Execute API again
	prep = prepareStateExecution(ctx, t, store, prcExeId, task.StateId, task.StateIdSequence)
	verifyStateExecution(ass, prep, processId, createEmptyEncodedObject(), persistence.StateExecutionStatusExecuteRunning)
	decision2 := xdbapi.StateDecision{
		ThreadCloseDecision: &xdbapi.ThreadCloseDecision{
			CloseType: xdbapi.GRACEFUL_COMPLETE_PROCESS,
		},
	}
	completeExecuteExecution(ctx, t, ass, store, prcExeId, task, prep, decision2, false)

	// Verify both stateId2 and process are still running
	prep = prepareStateExecution(ctx, t, store, prcExeId, stateId2, 1)
	verifyStateExecution(ass, prep, processId, createEmptyEncodedObject(), persistence.StateExecutionStatusExecuteRunning)
	describeProcess(ctx, t, ass, store, namespace, processId, xdbapi.RUNNING)
}

func SQLForceFailTest(t *testing.T, ass *assert.Assertions, store persistence.ProcessStore) {
	ctx := context.Background()
	namespace := "test-ns"
	processId := fmt.Sprintf("test-force-fail-%v", time.Now().String())
	input := createTestInput()

	// Start the process and verify it started correctly.
	prcExeId := startProcess(ctx, t, ass, store, namespace, processId, input)

	// Get the task
	minSeq, maxSeq, immediateTasks := checkAndGetImmediateTasks(ctx, t, ass, store, 1)
	task := immediateTasks[0]

	// Delete and verify immediate tasks are deleted.
	deleteAndVerifyImmediateTasksDeleted(ctx, t, ass, store, minSeq, maxSeq)

	// Prepare state execution.
	prep := prepareStateExecution(ctx, t, store, prcExeId, task.StateId, task.StateIdSequence)
	verifyStateExecution(ass, prep, processId, input, persistence.StateExecutionStatusWaitUntilRunning)

	// Complete 'WaitUntil' execution.
	completeWaitUntilExecution(ctx, t, ass, store, prcExeId, task, prep)

	// Check initial immediate tasks.
	minSeq, maxSeq, immediateTasks = checkAndGetImmediateTasks(ctx, t, ass, store, 1)
	task = immediateTasks[0]
	verifyImmediateTaskNoInfo(ass, task, persistence.ImmediateTaskTypeExecute, stateId1+"-1")

	// Delete and verify immediate tasks are deleted.
	deleteAndVerifyImmediateTasksDeleted(ctx, t, ass, store, minSeq, maxSeq)

	// Prepare state execution for Execute API
	prep = prepareStateExecution(ctx, t, store, prcExeId, task.StateId, task.StateIdSequence)
	verifyStateExecution(ass, prep, processId, input, persistence.StateExecutionStatusExecuteRunning)

	decision1 := xdbapi.StateDecision{
		NextStates: []xdbapi.StateMovement{
			{
				StateId: stateId2,
				// no input, skip waitUntil
				StateConfig: &xdbapi.AsyncStateConfig{SkipWaitUntil: ptr.Any(true)},
			},
			{
				StateId: stateId1, // use the same stateId
				// no input, skip waitUntil
				StateConfig: &xdbapi.AsyncStateConfig{SkipWaitUntil: ptr.Any(true)},
			},
		},
	}
	// Complete 'Execute' execution.
	completeExecuteExecution(ctx, t, ass, store, prcExeId, task, prep, decision1, true)

	minSeq, maxSeq, immediateTasks = checkAndGetImmediateTasks(ctx, t, ass, store, 2)
	task = immediateTasks[0]
	verifyImmediateTaskNoInfo(ass, task, persistence.ImmediateTaskTypeExecute, stateId2+"-1")
	task = immediateTasks[1]
	verifyImmediateTaskNoInfo(ass, task, persistence.ImmediateTaskTypeExecute, stateId1+"-2")

	// Delete and verify immediate tasks are deleted.
	deleteAndVerifyImmediateTasksDeleted(ctx, t, ass, store, minSeq, maxSeq)

	// Prepare state execution for Execute API again
	prep = prepareStateExecution(ctx, t, store, prcExeId, task.StateId, task.StateIdSequence)
	verifyStateExecution(ass, prep, processId, createEmptyEncodedObject(), persistence.StateExecutionStatusExecuteRunning)
	decision2 := xdbapi.StateDecision{
		ThreadCloseDecision: &xdbapi.ThreadCloseDecision{
			CloseType: xdbapi.FORCE_FAIL_PROCESS,
		},
	}
	completeExecuteExecution(ctx, t, ass, store, prcExeId, task, prep, decision2, false)

	// Verify stateId2 was aborted and process has failed
	prep = prepareStateExecution(ctx, t, store, prcExeId, stateId2, 1)
	verifyStateExecution(ass, prep, processId, createEmptyEncodedObject(), persistence.StateExecutionStatusAborted)
	describeProcess(ctx, t, ass, store, namespace, processId, xdbapi.FAILED)
}
