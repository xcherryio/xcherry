// Copyright 2023 XDBLab organization
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sqltest

import (
	"context"
	"fmt"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/xdblab/xdb-apis/goapi/xdbapi"
	"github.com/xdblab/xdb/common/ptr"
	"github.com/xdblab/xdb/persistence"
)

func SQLBasicTest(ass *assert.Assertions, store persistence.ProcessStore) {
	ctx := context.Background()
	namespace := "test-ns"
	processId := fmt.Sprintf("test-prcid-%v", time.Now().String())
	input := createTestInput()

	// Start the process and verify it started correctly.
	prcExeId := startProcess(ctx, ass, store, namespace, processId, input)

	// Try to start the process again and verify the behavior.
	retryStartProcessForFailure(ctx, ass, store, namespace, processId, input)

	// Describe the process.
	describeProcess(ctx, ass, store, namespace, processId)

	// Test waitUntil API execution
	// Check initial worker tasks.
	minSeq, maxSeq, workerTasks := checkAndGetWorkerTasks(ctx, ass, store, 1)
	task := workerTasks[0]
	verifyWorkerTask(ass, task, persistence.WorkerTaskTypeWaitUntil, stateId1, 1)

	// Delete and verify worker tasks are deleted.
	deleteAndVerifyWorkerTasksDeleted(ctx, ass, store, minSeq, maxSeq)

	// Prepare state execution.
	prep := prepareStateExecution(ctx, ass, store, prcExeId, task.StateId, task.StateIdSequence)
	verifyStateExecution(ass, prep, processId, input,
		persistence.StateExecutionStatusRunning,
		persistence.StateExecutionStatusUndefined)

	// Complete 'WaitUntil' execution.
	completeWaitUntilExecution(ctx, ass, store, prcExeId, task, prep)

	// Check initial worker tasks.
	minSeq, maxSeq, workerTasks = checkAndGetWorkerTasks(ctx, ass, store, 1)
	task = workerTasks[0]
	verifyWorkerTask(ass, task, persistence.WorkerTaskTypeExecute, stateId1, 1)

	// Delete and verify worker tasks are deleted.
	deleteAndVerifyWorkerTasksDeleted(ctx, ass, store, minSeq, maxSeq)

	// Prepare state execution for Execute API
	prep = prepareStateExecution(ctx, ass, store, prcExeId, task.StateId, task.StateIdSequence)
	verifyStateExecution(ass, prep, processId, input,
		persistence.StateExecutionStatusCompleted,
		persistence.StateExecutionStatusRunning)

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
	completeExecuteExecution(ctx, ass, store, prcExeId, task, prep, decision1, true)

	minSeq, maxSeq, workerTasks = checkAndGetWorkerTasks(ctx, ass, store, 2)
	task = workerTasks[0]
	verifyWorkerTask(ass, task, persistence.WorkerTaskTypeExecute, stateId2, 1)
	task = workerTasks[1]
	verifyWorkerTask(ass, task, persistence.WorkerTaskTypeExecute, stateId1, 2)

	// Delete and verify worker tasks are deleted.
	deleteAndVerifyWorkerTasksDeleted(ctx, ass, store, minSeq, maxSeq)

	// Prepare state execution for Execute API again
	prep = prepareStateExecution(ctx, ass, store, prcExeId, task.StateId, task.StateIdSequence)
	verifyStateExecution(ass, prep, processId, input,
		persistence.StateExecutionStatusSkipped,
		persistence.StateExecutionStatusRunning)
	decision2 := xdbapi.StateDecision{
		ThreadCloseDecision: &xdbapi.ThreadCloseDecision{
			CloseType: xdbapi.FORCE_COMPLETE_PROCESS.Ptr(),
		},
	}
	completeExecuteExecution(ctx, ass, store, prcExeId, task, prep, decision2, false)
	prep = prepareStateExecution(ctx, ass, store, prcExeId, stateId2, 1)
	verifyStateExecution(ass, prep, processId, createEmptyEncodedObject(),
		persistence.StateExecutionStatusSkipped,
		persistence.StateExecutionStatusAborted)
}
