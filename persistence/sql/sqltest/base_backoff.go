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
	"github.com/xdblab/xdb/common/ptr"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/xdblab/xdb/persistence"
)

func SQLBackoffTest(ass *assert.Assertions, store persistence.ProcessStore) {
	ctx := context.Background()
	firstAttmpTs := int64(123)
	fireTime1, backoffInfo1 := startProcessAndBackoffWorkerTask(ass, store, "test-ns-1", firstAttmpTs)
	// Check initial timer tasks.
	minSeq1, maxSeq1, timerTasks1 := getAndCheckTimerTasksUpToTs(ctx, ass, store, 1, fireTime1)

	verifyTimerTask(ass, timerTasks1[0], persistence.TimerTaskTypeWorkerTaskBackoff, stateId1+"-1",
		persistence.TimerTaskInfoJson{
			WorkerTaskBackoffInfo: backoffInfo1,
			WorkerTaskType:        ptr.Any(persistence.WorkerTaskTypeWaitUntil)})

	fireTime2, backoffInfo2 := startProcessAndBackoffWorkerTask(ass, store, "test-ns-2", firstAttmpTs)

	minSeq2, maxSeq2, timerTasks2 := getAndCheckTimerTasksUpForTimestamps(ctx, ass, store, 1, []int64{fireTime2}, maxSeq1+1)
	ass.True(maxSeq1 >= minSeq1)
	ass.True(minSeq2 > maxSeq1)
	ass.True(maxSeq2 >= minSeq2)

	verifyTimerTask(ass, timerTasks2[0], persistence.TimerTaskTypeWorkerTaskBackoff, stateId1+"-1",
		persistence.TimerTaskInfoJson{
			WorkerTaskBackoffInfo: backoffInfo2,
			WorkerTaskType:        ptr.Any(persistence.WorkerTaskTypeWaitUntil)})

	err := store.ConvertTimerTaskToWorkerTask(ctx, persistence.ConvertTimerTaskToWorkerTaskRequest{
		Task: timerTasks1[0],
	})
	ass.Nil(err)

	_, _, workerTasks := checkAndGetWorkerTasks(ctx, ass, store, 1)
	verifyWorkerTask(ass, workerTasks[0], persistence.WorkerTaskTypeWaitUntil, stateId1+"-1", persistence.WorkerTaskInfoJson{
		WorkerTaskBackoffInfo: &persistence.WorkerTaskBackoffInfoJson{
			CompletedAttempts:            1,
			FirstAttemptTimestampSeconds: firstAttmpTs,
		},
	})
}

func startProcessAndBackoffWorkerTask(
	ass *assert.Assertions, store persistence.ProcessStore, namespace string, firstAttemptTimestampSeconds int64,
) (int64, *persistence.WorkerTaskBackoffInfoJson) {
	ctx := context.Background()
	processId := fmt.Sprintf("test-prcid-%v", time.Now().String())
	input := createTestInput()

	// Start the process and verify it started correctly.
	prcExeId := startProcess(ctx, ass, store, namespace, processId, input)

	// Test waitUntil API execution
	// Check initial worker tasks.
	minSeq, maxSeq, workerTasks := checkAndGetWorkerTasks(ctx, ass, store, 1)
	workerTask := workerTasks[0]
	verifyWorkerTaskNoInfo(ass, workerTask, persistence.WorkerTaskTypeWaitUntil, stateId1+"-1")

	// Delete and verify worker tasks are deleted.
	deleteAndVerifyWorkerTasksDeleted(ctx, ass, store, minSeq, maxSeq)

	// Prepare state execution.
	prep := prepareStateExecution(ctx, ass, store, prcExeId, workerTask.StateId, workerTask.StateIdSequence)
	verifyStateExecution(ass, prep, processId, input,
		persistence.StateExecutionStatusRunning,
		persistence.StateExecutionStatusUndefined)

	backoffInfo := &persistence.WorkerTaskBackoffInfoJson{
		CompletedAttempts:            int32(1),
		FirstAttemptTimestampSeconds: firstAttemptTimestampSeconds,
	}
	workerTask.WorkerTaskInfo.WorkerTaskBackoffInfo = backoffInfo

	fireTime := time.Now().Add(time.Second * 10).Unix()
	err := store.BackoffWorkerTask(ctx, persistence.BackoffWorkerTaskRequest{
		LastFailureStatus:    401,
		LastFailureDetails:   "test-failure-details",
		Prep:                 *prep,
		FireTimestampSeconds: fireTime,
		Task:                 workerTask,
	})
	ass.Nil(err)
	return fireTime, backoffInfo
}
