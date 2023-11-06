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
	"testing"
	"time"

	"github.com/xdblab/xdb/common/ptr"

	"github.com/stretchr/testify/assert"
	"github.com/xdblab/xdb/persistence"
)

func SQLBackoffTest(t *testing.T, ass *assert.Assertions, store persistence.ProcessStore) {
	ctx := context.Background()
	firstAttmpTs := int64(123)
	fireTime1, backoffInfo1 := startProcessAndBackoffWorkerTask(t, ass, store, "test-ns-1", firstAttmpTs)
	// Check initial timer tasks.
	minSeq1, maxSeq1, timerTasks1 := getAndCheckTimerTasksUpToTs(ctx, t, ass, store, 2, fireTime1)

	verifyTimerTask(ass, timerTasks1[0], persistence.TimerTaskTypeProcessTimeout, "-0", persistence.TimerTaskInfoJson{
		WorkerTaskBackoffInfo: nil,
		WorkerTaskType:        nil,
		TimerCommandIndex:     0,
	})
	verifyTimerTask(ass, timerTasks1[1], persistence.TimerTaskTypeWorkerTaskBackoff, stateId1+"-1",
		persistence.TimerTaskInfoJson{
			WorkerTaskBackoffInfo: backoffInfo1,
			WorkerTaskType:        ptr.Any(persistence.ImmediateTaskTypeWaitUntil)})

	fireTime2, backoffInfo2 := startProcessAndBackoffWorkerTask(t, ass, store, "test-ns-2", firstAttmpTs)

	minSeq2, maxSeq2, timerTasks2 := getAndCheckTimerTasksUpForTimestamps(ctx, t, ass, store, 1, []int64{fireTime2}, maxSeq1+1)
	ass.True(maxSeq1 >= minSeq1)
	ass.True(minSeq2 > maxSeq1)
	ass.True(maxSeq2 >= minSeq2)

	verifyTimerTask(ass, timerTasks2[0], persistence.TimerTaskTypeWorkerTaskBackoff, stateId1+"-1",
		persistence.TimerTaskInfoJson{
			WorkerTaskBackoffInfo: backoffInfo2,
			WorkerTaskType:        ptr.Any(persistence.ImmediateTaskTypeWaitUntil)})

	resp, err := store.ConvertTimerTaskToImmediateTask(ctx, persistence.ProcessTimerTaskRequest{
		Task: timerTasks1[1],
	})
	ass.Nil(err)
	ass.Equal(&persistence.ProcessTimerTaskResponse{
		HasNewImmediateTask: true,
	}, resp)

	_, _, immediateTasks := checkAndGetImmediateTasks(ctx, t, ass, store, 1)
	verifyImmediateTask(ass, immediateTasks[0], persistence.ImmediateTaskTypeWaitUntil, stateId1+"-1", persistence.ImmediateTaskInfoJson{
		WorkerTaskBackoffInfo: &persistence.WorkerTaskBackoffInfoJson{
			CompletedAttempts:            1,
			FirstAttemptTimestampSeconds: firstAttmpTs,
		},
	})
}

func startProcessAndBackoffWorkerTask(
	t *testing.T,
	ass *assert.Assertions, store persistence.ProcessStore, namespace string, firstAttemptTimestampSeconds int64,
) (int64, *persistence.WorkerTaskBackoffInfoJson) {
	ctx := context.Background()
	processId := fmt.Sprintf("test-prcid-%v", time.Now().String())
	input := createTestInput()

	// Start the process and verify it started correctly.
	prcExeId := startProcess(ctx, t, ass, store, namespace, processId, input)

	// Test waitUntil API execution
	// Check initial immediate tasks.
	minSeq, maxSeq, immediateTasks := checkAndGetImmediateTasks(ctx, t, ass, store, 1)
	immediateTask := immediateTasks[0]
	verifyImmediateTaskNoInfo(ass, immediateTask, persistence.ImmediateTaskTypeWaitUntil, stateId1+"-1")

	// Delete and verify immediate tasks are deleted.
	deleteAndVerifyImmediateTasksDeleted(ctx, t, ass, store, minSeq, maxSeq)

	// Prepare state execution.
	prep := prepareStateExecution(ctx, t, store, prcExeId, immediateTask.StateId, immediateTask.StateIdSequence)
	verifyStateExecution(ass, prep, processId, input, persistence.StateExecutionStatusWaitUntilRunning)

	backoffInfo := &persistence.WorkerTaskBackoffInfoJson{
		CompletedAttempts:            int32(1),
		FirstAttemptTimestampSeconds: firstAttemptTimestampSeconds,
	}
	immediateTask.ImmediateTaskInfo.WorkerTaskBackoffInfo = backoffInfo

	fireTime := time.Now().Add(time.Second * 10).Unix()
	err := store.BackoffImmediateTask(ctx, persistence.BackoffImmediateTaskRequest{
		LastFailureStatus:    401,
		LastFailureDetails:   "test-failure-details",
		Prep:                 *prep,
		FireTimestampSeconds: fireTime,
		Task:                 immediateTask,
	})
	ass.Nil(err)
	return fireTime, backoffInfo
}
