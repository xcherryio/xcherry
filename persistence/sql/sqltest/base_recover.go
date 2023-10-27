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

func SQLStateFailureRecoveryTest(ass *assert.Assertions, store persistence.ProcessStore) {
	ctx := context.Background()
	namespace := "test-ns"
	processId := fmt.Sprintf("test-prcid-%v", time.Now().String())
	input := createTestInput()

	// Start the process and verify it started correctly.
	prcExeId := startProcess(ctx, ass, store, namespace, processId, input)

	// Describe the process.
	describeProcess(ctx, ass, store, namespace, processId, xdbapi.RUNNING)

	// Test waitUntil API execution
	// Check initial immediate tasks.
	minSeq, maxSeq, immediateTasks := checkAndGetImmediateTasks(ctx, ass, store, 1)
	task := immediateTasks[0]
	verifyImmediateTaskNoInfo(ass, task, persistence.ImmediateTaskTypeWaitUntil, stateId1+"-1")

	// Delete and verify immediate tasks are deleted.
	deleteAndVerifyImmediateTasksDeleted(ctx, ass, store, minSeq, maxSeq)

	// Prepare state execution.
	prep := prepareStateExecution(ctx, ass, store, prcExeId, task.StateId, task.StateIdSequence)
	verifyStateExecution(ass, prep, processId, input, persistence.StateExecutionStatusWaitUntilRunning)

	// Complete 'WaitUntil' execution.
	completeWaitUntilExecution(ctx, ass, store, prcExeId, task, prep)

	// Check initial immediate tasks.
	minSeq, maxSeq, immediateTasks = checkAndGetImmediateTasks(ctx, ass, store, 1)
	task = immediateTasks[0]
	verifyImmediateTaskNoInfo(ass, task, persistence.ImmediateTaskTypeExecute, stateId1+"-1")

	// Delete and verify immediate tasks are deleted.
	deleteAndVerifyImmediateTasksDeleted(ctx, ass, store, minSeq, maxSeq)

	// Prepare state execution for Execute API
	prep = prepareStateExecution(ctx, ass, store, prcExeId, task.StateId, task.StateIdSequence)
	verifyStateExecution(ass, prep, processId, input, persistence.StateExecutionStatusExecuteRunning)

	decision1 := xdbapi.StateDecision{
		NextStates: []xdbapi.StateMovement{
			{
				StateId:    stateId2,
				StateInput: xdbapi.NewEncodedObject(input.Encoding, input.Data+"-"+stateId1+"-1"),
				StateConfig: &xdbapi.AsyncStateConfig{
					SkipWaitUntil: ptr.Any(true),
					StateFailureRecoveryInfo: &xdbapi.AsyncStateConfigStateFailureRecoveryInfo{
						Policy:                         xdbapi.PROCEED_TO_CONFIGURED_STATE,
						StateFailureProceedStateId:     ptr.Any(stateId1),
						StateFailureProceedStateConfig: &xdbapi.AsyncStateConfig{SkipWaitUntil: ptr.Any(true)},
					},
				},
			},
		},
	}
	// Complete 'Execute' execution.
	completeExecuteExecution(ctx, ass, store, prcExeId, task, prep, decision1, true)

	minSeq, maxSeq, immediateTasks = checkAndGetImmediateTasks(ctx, ass, store, 1)
	task = immediateTasks[0]
	verifyImmediateTaskNoInfo(ass, task, persistence.ImmediateTaskTypeExecute, stateId2+"-1")

	// Delete and verify immediate tasks are deleted.
	deleteAndVerifyImmediateTasksDeleted(ctx, ass, store, minSeq, maxSeq)

	// Prepare state execution for Execute API again
	prep = prepareStateExecution(ctx, ass, store, prcExeId, task.StateId, task.StateIdSequence)
	verifyStateExecution(ass, prep, processId, *xdbapi.NewEncodedObject(input.Encoding, input.Data+"-"+stateId1+"-1"), persistence.StateExecutionStatusExecuteRunning)

	recoverFromFailure(
		ctx,
		ass,
		store,
		namespace,
		prcExeId,
		*prep,
		persistence.StateExecutionId{
			StateId:         stateId2,
			StateIdSequence: 1,
		},
		xdbapi.EXECUTE_API,
		stateId1,
		&xdbapi.AsyncStateConfig{
			SkipWaitUntil: ptr.Any(true),
		},
		*xdbapi.NewEncodedObject(input.Encoding, input.Data+"-"+stateId1+"-1"+"-"+stateId2+"-1"),
	)

	prep = prepareStateExecution(ctx, ass, store, prcExeId, stateId2, 1)
	verifyStateExecution(ass, prep, processId, *xdbapi.NewEncodedObject("test-encoding", input.Data+"-"+stateId1+"-1"), persistence.StateExecutionStatusFailed)

	prep = prepareStateExecution(ctx, ass, store, prcExeId, stateId1, 2)
	verifyStateExecution(ass, prep, processId, *xdbapi.NewEncodedObject("test-encoding", input.Data+"-"+stateId1+"-1"+"-"+stateId2+"-1"), persistence.StateExecutionStatusExecuteRunning)
}
