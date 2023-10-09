// Apache License 2.0

// Copyright (c) XDBLab organization

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package sql

import (
	"context"
	"fmt"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/xdblab/xdb-apis/goapi/xdbapi"
	"github.com/xdblab/xdb/common/ptr"
	"github.com/xdblab/xdb/common/uuid"
	"github.com/xdblab/xdb/persistence"
)

const testProcessType = "test-type"
const testWorkerUrl = "test-url"
const stateId1 = "state-1"
const stateId2 = "state-2"

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
}

func createTestInput() xdbapi.EncodedObject {
	return xdbapi.EncodedObject{
		Encoding: ptr.Any("test-encoding"),
		Data:     ptr.Any("test-data"),
	}
}

func startProcess(
	ctx context.Context, ass *assert.Assertions,
	store persistence.ProcessStore, namespace, processId string, input xdbapi.EncodedObject,
) uuid.UUID {
	startReq := createStartRequest(namespace, processId, input)
	startResp, err := store.StartProcess(ctx, persistence.StartProcessRequest{
		Request:        startReq,
		NewTaskShardId: persistence.DefaultShardId,
	})

	ass.Nil(err)
	ass.False(startResp.AlreadyStarted)
	ass.True(startResp.HasNewWorkerTask)
	ass.True(len(startResp.ProcessExecutionId.String()) > 0)
	return startResp.ProcessExecutionId
}

func createStartRequest(namespace, processId string, input xdbapi.EncodedObject) xdbapi.ProcessExecutionStartRequest {
	// Other values like processType, workerUrl etc. are kept constants for simplicity
	return xdbapi.ProcessExecutionStartRequest{
		Namespace:        namespace,
		ProcessId:        processId,
		ProcessType:      "test-type",
		WorkerUrl:        "test-url",
		StartStateId:     ptr.Any(stateId1),
		StartStateInput:  &input,
		StartStateConfig: nil,
		ProcessStartConfig: &xdbapi.ProcessStartConfig{
			TimeoutSeconds: ptr.Any(int32(100)),
		},
	}
}

func retryStartProcessForFailure(ctx context.Context, ass *assert.Assertions,
	store persistence.ProcessStore, namespace, processId string, input xdbapi.EncodedObject,
) {
	startReq := createStartRequest(namespace, processId, input)
	startResp2, err := store.StartProcess(ctx, persistence.StartProcessRequest{
		Request:        startReq,
		NewTaskShardId: persistence.DefaultShardId,
	})
	ass.Nil(err)
	ass.True(startResp2.AlreadyStarted)
	ass.False(startResp2.HasNewWorkerTask)
}

func describeProcess(ctx context.Context, ass *assert.Assertions, store persistence.ProcessStore, namespace, processId string) {
	// Incorrect process id description
	descResp, err := store.DescribeLatestProcess(ctx, persistence.DescribeLatestProcessRequest{
		Namespace: namespace,
		ProcessId: "some-wrong-id",
	})
	ass.Nil(err)
	ass.True(descResp.NotExists)

	// Correct process id description
	descResp, err = store.DescribeLatestProcess(ctx, persistence.DescribeLatestProcessRequest{
		Namespace: namespace,
		ProcessId: processId,
	})
	ass.Nil(err)
	ass.False(descResp.NotExists)
	ass.Equal(testProcessType, descResp.Response.GetProcessType())
	ass.Equal(testWorkerUrl, descResp.Response.GetWorkerUrl())
}

func checkAndGetWorkerTasks(
	ctx context.Context, ass *assert.Assertions, store persistence.ProcessStore, expectedLength int,
) (int64, int64, []persistence.WorkerTask) {
	getTasksResp, err := store.GetWorkerTasks(ctx, persistence.GetWorkerTasksRequest{
		ShardId:                persistence.DefaultShardId,
		StartSequenceInclusive: 0,
		PageSize:               10,
	})
	ass.Nil(err)
	ass.Equal(expectedLength, len(getTasksResp.Tasks))
	return getTasksResp.MinSequenceInclusive, getTasksResp.MaxSequenceInclusive, getTasksResp.Tasks
}

func verifyWorkerTask(ass *assert.Assertions, task persistence.WorkerTask, taskType persistence.WorkerTaskType, stateId string, stateSeq int) {
	ass.NotNil(task.StateIdSequence)
	ass.Equal(persistence.DefaultShardId, int(task.ShardId))
	ass.Equal(taskType, task.TaskType)
	ass.Equal(stateId, task.StateId)
	ass.Equal(stateSeq, int(task.StateIdSequence))
	ass.True(task.TaskSequence != nil)
}

func deleteAndVerifyWorkerTasksDeleted(
	ctx context.Context, ass *assert.Assertions, store persistence.ProcessStore, minSeq, maxSeq int64,
) {
	err := store.DeleteWorkerTasks(ctx, persistence.DeleteWorkerTasksRequest{
		ShardId:                  persistence.DefaultShardId,
		MinTaskSequenceInclusive: minSeq,
		MaxTaskSequenceInclusive: maxSeq,
	})
	ass.Nil(err)
	checkAndGetWorkerTasks(ctx, ass, store, 0) // Expect no tasks
}

func prepareStateExecution(
	ctx context.Context, ass *assert.Assertions,
	store persistence.ProcessStore, prcExeId uuid.UUID, stateId string, stateIdSeq int32,
) *persistence.PrepareStateExecutionResponse {
	stateExeId := persistence.StateExecutionId{
		StateId:         stateId,
		StateIdSequence: stateIdSeq,
	}
	prep, err := store.PrepareStateExecution(ctx, persistence.PrepareStateExecutionRequest{
		ProcessExecutionId: prcExeId,
		StateExecutionId:   stateExeId,
	})
	ass.Nil(err)
	return prep
}

func verifyStateExecution(
	ass *assert.Assertions,
	prep *persistence.PrepareStateExecutionResponse,
	processId string, input xdbapi.EncodedObject,
	expectedWaitUntilStatus, expectedExecuteStatus persistence.StateExecutionStatus,
) {
	ass.Equal(testWorkerUrl, prep.Info.WorkerURL)
	ass.Equal(testProcessType, prep.Info.ProcessType)
	ass.Equal(processId, prep.Info.ProcessId)
	ass.Equal(input, prep.Input)
	ass.Equal(expectedWaitUntilStatus, prep.WaitUntilStatus)
	ass.Equal(expectedExecuteStatus, prep.ExecuteStatus)
}

func completeWaitUntilExecution(
	ctx context.Context, ass *assert.Assertions,
	store persistence.ProcessStore, prcExeId uuid.UUID, workerTask persistence.WorkerTask, prep *persistence.PrepareStateExecutionResponse,
) {
	stateExeId := persistence.StateExecutionId{
		StateId:         workerTask.StateId,
		StateIdSequence: workerTask.StateIdSequence,
	}
	compWaitResp, err := store.CompleteWaitUntilExecution(ctx, persistence.CompleteWaitUntilExecutionRequest{
		ProcessExecutionId: prcExeId,
		StateExecutionId:   stateExeId,
		Prepare:            *prep,
		CommandRequest: xdbapi.CommandRequest{
			WaitingType: xdbapi.EMPTY_COMMAND.Ptr(),
		},
		TaskShardId: persistence.DefaultShardId,
	})
	ass.Nil(err)
	ass.True(compWaitResp.HasNewWorkerTask)
}

func completeExecuteExecution(
	ctx context.Context, ass *assert.Assertions,
	store persistence.ProcessStore, prcExeId uuid.UUID, workerTask persistence.WorkerTask, prep *persistence.PrepareStateExecutionResponse,
	stateDecision xdbapi.StateDecision, hasNewWorkerTask bool,
) {
	stateExeId := persistence.StateExecutionId{
		StateId:         workerTask.StateId,
		StateIdSequence: workerTask.StateIdSequence,
	}
	compWaitResp, err := store.CompleteExecuteExecution(ctx, persistence.CompleteExecuteExecutionRequest{
		ProcessExecutionId: prcExeId,
		StateExecutionId:   stateExeId,
		Prepare:            *prep,
		StateDecision:      stateDecision,
		TaskShardId:        persistence.DefaultShardId,
	})
	ass.Nil(err)
	ass.Equal(hasNewWorkerTask, compWaitResp.HasNewWorkerTask)
}
