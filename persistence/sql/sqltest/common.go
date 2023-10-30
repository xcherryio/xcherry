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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xdblab/xdb-apis/goapi/xdbapi"
	"github.com/xdblab/xdb/common/ptr"
	"github.com/xdblab/xdb/common/uuid"
	"github.com/xdblab/xdb/persistence"
)

const testProcessType = "test-type"
const testWorkerUrl = "test-url"
const stateId1 = "state1"
const stateId2 = "state2"

func createTestInput() xdbapi.EncodedObject {
	return xdbapi.EncodedObject{
		Encoding: "test-encoding",
		Data:     "test-data",
	}
}

func createEmptyEncodedObject() xdbapi.EncodedObject {
	return xdbapi.EncodedObject{
		Encoding: "",
		Data:     "",
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
	ass.True(startResp.HasNewImmediateTask)
	ass.True(len(startResp.ProcessExecutionId.String()) > 0)
	return startResp.ProcessExecutionId
}

func terminateProcess(
	ctx context.Context, ass *assert.Assertions,
	store persistence.ProcessStore, namespace, processId string,
) {
	resp, err := store.StopProcess(ctx, persistence.StopProcessRequest{
		Namespace:       namespace,
		ProcessId:       processId,
		ProcessStopType: xdbapi.TERMINATE,
	})

	ass.Nil(err)
	ass.False(resp.NotExists)
}

func startProcessWithAllowIfPreviousExitAbnormally(
	ctx context.Context, ass *assert.Assertions,
	store persistence.ProcessStore, namespace, processId string, input xdbapi.EncodedObject,
) uuid.UUID {
	startReq := createStartRequestWithAllowIfPreviousExitAbnormallyPolicy(namespace, processId, input)
	startResp, err := store.StartProcess(ctx, persistence.StartProcessRequest{
		Request:        startReq,
		NewTaskShardId: persistence.DefaultShardId,
	})

	ass.Nil(err)
	ass.False(startResp.AlreadyStarted)
	ass.True(startResp.HasNewImmediateTask)
	ass.True(len(startResp.ProcessExecutionId.String()) > 0)
	return startResp.ProcessExecutionId
}

func startProcessWithTerminateIfRunningPolicy(
	ctx context.Context, ass *assert.Assertions,
	store persistence.ProcessStore, namespace, processId string, input xdbapi.EncodedObject,
) uuid.UUID {
	startReq := createStartRequestWithTerminateIfRunningPolicy(namespace, processId, input)
	startResp, err := store.StartProcess(ctx, persistence.StartProcessRequest{
		Request:        startReq,
		NewTaskShardId: persistence.DefaultShardId,
	})

	ass.Nil(err)
	ass.False(startResp.AlreadyStarted)
	ass.True(startResp.HasNewImmediateTask)
	ass.True(len(startResp.ProcessExecutionId.String()) > 0)
	return startResp.ProcessExecutionId
}

func startProcessWithAllowIfNoRunningPolicy(
	ctx context.Context, ass *assert.Assertions,
	store persistence.ProcessStore, namespace, processId string, input xdbapi.EncodedObject,
) uuid.UUID {
	startReq := createStartRequestWithAllowIfNoRunningPolicy(namespace, processId, input)
	startResp, err := store.StartProcess(ctx, persistence.StartProcessRequest{
		Request:        startReq,
		NewTaskShardId: persistence.DefaultShardId,
	})

	ass.Nil(err)
	ass.False(startResp.AlreadyStarted)
	ass.True(startResp.HasNewImmediateTask)
	ass.True(len(startResp.ProcessExecutionId.String()) > 0)
	return startResp.ProcessExecutionId
}

func startProcessWithDisallowReusePolicy(
	ctx context.Context, ass *assert.Assertions,
	store persistence.ProcessStore, namespace, processId string, input xdbapi.EncodedObject,
) uuid.UUID {
	startReq := createStartRequestWithDisallowReusePolicy(namespace, processId, input)
	startResp, err := store.StartProcess(ctx, persistence.StartProcessRequest{
		Request:        startReq,
		NewTaskShardId: persistence.DefaultShardId,
	})

	ass.Nil(err)
	ass.True(startResp.AlreadyStarted)
	return startResp.ProcessExecutionId
}

func createStartRequestWithAllowIfPreviousExitAbnormallyPolicy(namespace, processId string, input xdbapi.EncodedObject) xdbapi.ProcessExecutionStartRequest {
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
			IdReusePolicy:  xdbapi.ALLOW_IF_PREVIOUS_EXIT_ABNORMALLY.Ptr().Ptr(),
		},
	}
}

func createStartRequestWithDisallowReusePolicy(namespace, processId string, input xdbapi.EncodedObject) xdbapi.ProcessExecutionStartRequest {
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
			IdReusePolicy:  xdbapi.DISALLOW_REUSE.Ptr(),
		},
	}
}

func createStartRequestWithAllowIfNoRunningPolicy(namespace, processId string, input xdbapi.EncodedObject) xdbapi.ProcessExecutionStartRequest {
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
			IdReusePolicy:  xdbapi.ALLOW_IF_NO_RUNNING.Ptr(),
		},
	}
}

func createStartRequestWithTerminateIfRunningPolicy(namespace, processId string, input xdbapi.EncodedObject) xdbapi.ProcessExecutionStartRequest {
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
			IdReusePolicy:  xdbapi.TERMINATE_IF_RUNNING.Ptr(),
		},
	}
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

func retryStartProcessForFailure(
	ctx context.Context, ass *assert.Assertions,
	store persistence.ProcessStore, namespace, processId string, input xdbapi.EncodedObject,
) {
	startReq := createStartRequest(namespace, processId, input)
	startResp2, err := store.StartProcess(ctx, persistence.StartProcessRequest{
		Request:        startReq,
		NewTaskShardId: persistence.DefaultShardId,
	})
	ass.Nil(err)
	ass.True(startResp2.AlreadyStarted)
	ass.False(startResp2.HasNewImmediateTask)
}

func describeProcess(
	ctx context.Context, ass *assert.Assertions, store persistence.ProcessStore,
	namespace, processId string, processStatus xdbapi.ProcessStatus,
) {
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
	ass.Equal(processStatus, descResp.Response.GetStatus())
}

func checkAndGetImmediateTasks(
	ctx context.Context, ass *assert.Assertions, store persistence.ProcessStore, expectedLength int,
) (int64, int64, []persistence.ImmediateTask) {
	getTasksResp, err := store.GetImmediateTasks(ctx, persistence.GetImmediateTasksRequest{
		ShardId:                persistence.DefaultShardId,
		StartSequenceInclusive: 0,
		PageSize:               10,
	})
	ass.Nil(err)
	ass.Equal(expectedLength, len(getTasksResp.Tasks))
	return getTasksResp.MinSequenceInclusive, getTasksResp.MaxSequenceInclusive, getTasksResp.Tasks
}

func getAndCheckTimerTasksUpToTs(
	ctx context.Context, ass *assert.Assertions, store persistence.ProcessStore, expectedLength int,
	upToTimestamp int64,
) (int64, int64, []persistence.TimerTask) {
	getTasksResp, err := store.GetTimerTasksUpToTimestamp(ctx, persistence.GetTimerTasksRequest{
		ShardId:                          persistence.DefaultShardId,
		MaxFireTimestampSecondsInclusive: upToTimestamp,
		PageSize:                         10,
	})
	ass.Nil(err)
	ass.Equal(expectedLength, len(getTasksResp.Tasks))
	return getTasksResp.MinSequenceInclusive, getTasksResp.MaxSequenceInclusive, getTasksResp.Tasks
}

func getAndCheckTimerTasksUpForTimestamps(
	ctx context.Context, ass *assert.Assertions, store persistence.ProcessStore, expectedLength int,
	forTimestamps []int64, minTaskSeq int64,
) (int64, int64, []persistence.TimerTask) {
	getTasksResp, err := store.GetTimerTasksForTimestamps(ctx, persistence.GetTimerTasksForTimestampsRequest{
		ShardId:              persistence.DefaultShardId,
		MinSequenceInclusive: minTaskSeq,
		DetailedRequests: []xdbapi.NotifyTimerTasksRequest{
			{
				FireTimestamps: forTimestamps,
			},
		},
	})
	ass.Nil(err)
	ass.Equal(expectedLength, len(getTasksResp.Tasks))
	return getTasksResp.MinSequenceInclusive, getTasksResp.MaxSequenceInclusive, getTasksResp.Tasks
}

func verifyImmediateTaskNoInfo(
	ass *assert.Assertions, task persistence.ImmediateTask,
	taskType persistence.ImmediateTaskType, stateExeId string,
) {
	verifyImmediateTask(ass, task, taskType, stateExeId, persistence.ImmediateTaskInfoJson{})
}

func verifyImmediateTask(
	ass *assert.Assertions, task persistence.ImmediateTask,
	taskType persistence.ImmediateTaskType, stateExeId string,
	info persistence.ImmediateTaskInfoJson,
) {
	ass.NotNil(task.StateIdSequence)
	ass.Equal(persistence.DefaultShardId, int(task.ShardId))
	ass.Equal(taskType, task.TaskType)
	ass.Equal(stateExeId, task.GetStateExecutionId())
	ass.True(task.TaskSequence != nil)
	ass.Equal(info, task.ImmediateTaskInfo)
}

func verifyTimerTask(
	ass *assert.Assertions, task persistence.TimerTask,
	taskType persistence.TimerTaskType, stateExeId string,
	taskInfo persistence.TimerTaskInfoJson,
) {
	ass.NotNil(task.StateIdSequence)
	ass.Equal(persistence.DefaultShardId, int(task.ShardId))
	ass.Equal(taskType, task.TaskType)
	ass.Equal(stateExeId, task.GetStateExecutionId())
	ass.True(task.TaskSequence != nil)
	ass.Equal(taskInfo, task.TimerTaskInfo)
}

func deleteAndVerifyImmediateTasksDeleted(
	ctx context.Context, ass *assert.Assertions, store persistence.ProcessStore, minSeq, maxSeq int64,
) {
	err := store.DeleteImmediateTasks(ctx, persistence.DeleteImmediateTasksRequest{
		ShardId:                  persistence.DefaultShardId,
		MinTaskSequenceInclusive: minSeq,
		MaxTaskSequenceInclusive: maxSeq,
	})
	ass.Nil(err)
	checkAndGetImmediateTasks(ctx, ass, store, 0) // Expect no tasks
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
	expectedStatus persistence.StateExecutionStatus,
) {
	ass.Equal(testWorkerUrl, prep.Info.WorkerURL)
	ass.Equal(testProcessType, prep.Info.ProcessType)
	ass.Equal(processId, prep.Info.ProcessId)
	ass.Equal(input, prep.Input)
	ass.Equal(expectedStatus, prep.Status)
}

func completeWaitUntilExecution(
	ctx context.Context, ass *assert.Assertions,
	store persistence.ProcessStore, prcExeId uuid.UUID, immediateTask persistence.ImmediateTask, prep *persistence.PrepareStateExecutionResponse,
) {
	stateExeId := persistence.StateExecutionId{
		StateId:         immediateTask.StateId,
		StateIdSequence: immediateTask.StateIdSequence,
	}
	compWaitResp, err := store.ProcessWaitUntilExecution(ctx, persistence.ProcessWaitUntilExecutionRequest{
		ProcessExecutionId: prcExeId,
		StateExecutionId:   stateExeId,
		Prepare:            *prep,
		CommandRequest: xdbapi.CommandRequest{
			WaitingType: xdbapi.EMPTY_COMMAND,
		},
		TaskShardId: persistence.DefaultShardId,
	})
	ass.Nil(err)
	ass.True(compWaitResp.HasNewImmediateTask)
}

func completeExecuteExecution(
	ctx context.Context, ass *assert.Assertions,
	store persistence.ProcessStore, prcExeId uuid.UUID, immediateTask persistence.ImmediateTask, prep *persistence.PrepareStateExecutionResponse,
	stateDecision xdbapi.StateDecision, hasNewImmediateTask bool,
) {
	stateExeId := persistence.StateExecutionId{
		StateId:         immediateTask.StateId,
		StateIdSequence: immediateTask.StateIdSequence,
	}
	compWaitResp, err := store.CompleteExecuteExecution(ctx, persistence.CompleteExecuteExecutionRequest{
		ProcessExecutionId: prcExeId,
		StateExecutionId:   stateExeId,
		Prepare:            *prep,
		StateDecision:      stateDecision,
		TaskShardId:        persistence.DefaultShardId,
	})
	ass.Nil(err)
	ass.Equal(hasNewImmediateTask, compWaitResp.HasNewImmediateTask)
}

func recoverFromFailure(
	t *testing.T,
	ctx context.Context,
	assert *assert.Assertions,
	store persistence.ProcessStore,
	namespace string,
	prcExeId uuid.UUID,
	prep persistence.PrepareStateExecutionResponse,
	sourceStateExecId persistence.StateExecutionId,
	sourceFailedStateApi xdbapi.StateApiType,
	destinationStateId string,
	destiantionStateConfig *xdbapi.AsyncStateConfig,
	destinationStateInput xdbapi.EncodedObject) {
	request := persistence.RecoverFromStateExecutionFailureRequest{
		Namespace:              namespace,
		ProcessExecutionId:     prcExeId,
		Prepare:                prep,
		SourceStateExecutionId: sourceStateExecId,
		SourceFailedStateApi:   sourceFailedStateApi,
		DestinationStateId:     destinationStateId,
		DestinationStateConfig: destiantionStateConfig,
		DestinationStateInput:  destinationStateInput,
		ShardId:                persistence.DefaultShardId,
	}

	err := store.RecoverFromStateExecutionFailure(ctx, request)
	require.NoError(t, err)

	// verify process execution
	descResp, err := store.DescribeLatestProcess(ctx, persistence.DescribeLatestProcessRequest{
		Namespace: namespace,
		ProcessId: prep.Info.ProcessId,
	})
	require.NoError(t, err)
	assert.Equal(xdbapi.RUNNING, *descResp.Response.Status)
}
