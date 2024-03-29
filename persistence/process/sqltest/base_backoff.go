// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: Apache-2.0

package sqltest

import (
	"context"
	"fmt"
	"github.com/xcherryio/xcherry/persistence/data_models"
	"testing"
	"time"

	"github.com/xcherryio/xcherry/common/ptr"

	"github.com/stretchr/testify/assert"
	"github.com/xcherryio/xcherry/persistence"
)

func SQLBackoffTest(t *testing.T, ass *assert.Assertions, store persistence.ProcessStore) {
	ctx := context.Background()
	firstAttmpTs := int64(123)
	fireTime1, backoffInfo1 := startProcessAndBackoffWorkerTask(t, ass, store, "test-ns-1", firstAttmpTs)
	// Check initial timer tasks.
	minSeq1, maxSeq1, timerTasks1 := getAndCheckTimerTasksUpToTs(ctx, t, ass, store, 1, fireTime1)

	verifyTimerTask(ass, timerTasks1[0], data_models.TimerTaskTypeWorkerTaskBackoff, stateId1+"-1",
		data_models.TimerTaskInfoJson{
			WorkerTaskBackoffInfo: backoffInfo1,
			WorkerTaskType:        ptr.Any(data_models.ImmediateTaskTypeWaitUntil)})

	fireTime2, backoffInfo2 := startProcessAndBackoffWorkerTask(t, ass, store, "test-ns-2", firstAttmpTs)

	minSeq2, maxSeq2, timerTasks2 := getAndCheckTimerTasksUpForTimestamps(ctx, t, ass, store, 1, []int64{fireTime2}, maxSeq1+1)
	ass.True(maxSeq1 >= minSeq1)
	ass.True(minSeq2 > maxSeq1)
	ass.True(maxSeq2 >= minSeq2)

	verifyTimerTask(ass, timerTasks2[0], data_models.TimerTaskTypeWorkerTaskBackoff, stateId1+"-1",
		data_models.TimerTaskInfoJson{
			WorkerTaskBackoffInfo: backoffInfo2,
			WorkerTaskType:        ptr.Any(data_models.ImmediateTaskTypeWaitUntil)})

	resp, err := store.ConvertTimerTaskToImmediateTask(ctx, data_models.ProcessTimerTaskRequest{
		Task: timerTasks1[0],
	})
	ass.Nil(err)
	ass.Equal(&data_models.ProcessTimerTaskResponse{
		HasNewImmediateTask: true,
	}, resp)

	_, _, immediateTasks := checkAndGetImmediateTasks(ctx, t, ass, store, 1)
	verifyImmediateTask(ass, immediateTasks[0], data_models.ImmediateTaskTypeWaitUntil, stateId1+"-1", data_models.ImmediateTaskInfoJson{
		WorkerTaskBackoffInfo: &data_models.WorkerTaskBackoffInfoJson{
			CompletedAttempts:            1,
			FirstAttemptTimestampSeconds: firstAttmpTs,
		},
	})
}

func startProcessAndBackoffWorkerTask(
	t *testing.T,
	ass *assert.Assertions, store persistence.ProcessStore, namespace string, firstAttemptTimestampSeconds int64,
) (int64, *data_models.WorkerTaskBackoffInfoJson) {
	ctx := context.Background()
	processId := fmt.Sprintf("test-prcid-%v", time.Now().String())
	input := createTestInput()

	// Start the process and verify it started correctly.
	prcExeId := startProcess(ctx, t, ass, store, namespace, processId, input)

	// Test waitUntil API execution
	// Check initial immediate tasks.
	minSeq, maxSeq, immediateTasks := checkAndGetImmediateTasks(ctx, t, ass, store, 2)
	immediateTask := immediateTasks[0]
	verifyImmediateTaskNoInfo(ass, immediateTask, data_models.ImmediateTaskTypeWaitUntil, stateId1+"-1")
	visibilityTask := immediateTasks[1]
	ass.Equal(data_models.ImmediateTaskTypeVisibility, visibilityTask.TaskType)

	// Delete and verify immediate tasks are deleted.
	deleteAndVerifyImmediateTasksDeleted(ctx, t, ass, store, minSeq, maxSeq)

	// Prepare state execution.
	prep := prepareStateExecution(ctx, t, store, prcExeId, immediateTask.StateId, immediateTask.StateIdSequence)
	verifyStateExecution(ass, prep, processId, input, data_models.StateExecutionStatusWaitUntilRunning)

	backoffInfo := &data_models.WorkerTaskBackoffInfoJson{
		CompletedAttempts:            int32(1),
		FirstAttemptTimestampSeconds: firstAttemptTimestampSeconds,
	}
	immediateTask.ImmediateTaskInfo.WorkerTaskBackoffInfo = backoffInfo

	fireTime := time.Now().Add(time.Second * 10).Unix()
	err := store.BackoffImmediateTask(ctx, data_models.BackoffImmediateTaskRequest{
		LastFailureStatus:    401,
		LastFailureDetails:   "test-failure-details",
		Prep:                 *prep,
		FireTimestampSeconds: fireTime,
		Task:                 immediateTask,
	})
	ass.Nil(err)
	return fireTime, backoffInfo
}
