// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: Apache-2.0

package sqltest

import (
	"context"
	"encoding/json"
	"github.com/xcherryio/apis/goapi/xcapi"
	"github.com/xcherryio/xcherry/persistence/data_models"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xcherryio/xcherry/common/ptr"
	"github.com/xcherryio/xcherry/common/uuid"
	"github.com/xcherryio/xcherry/persistence"
)

const testProcessType = "test-type"
const testWorkerUrl = "test-url"
const stateId1 = "state1"
const stateId2 = "state2"
const namespace = "test-ns"

func createTestInput() xcapi.EncodedObject {
	return xcapi.EncodedObject{
		Encoding: "test-encoding",
		Data:     "test-data",
	}
}

func createEmptyEncodedObject() xcapi.EncodedObject {
	return xcapi.EncodedObject{
		Encoding: "",
		Data:     "",
	}
}

func startProcessWithConfigs(
	ctx context.Context, t *testing.T, ass *assert.Assertions, store persistence.ProcessStore,
	namespace, processId string,
	input xcapi.EncodedObject, appDatabaseConfig *xcapi.AppDatabaseConfig, stateCfg *xcapi.AsyncStateConfig,
) uuid.UUID {
	startReq := createStartRequest(namespace, processId, input, appDatabaseConfig, stateCfg)
	startResp, err := store.StartProcess(ctx, data_models.StartProcessRequest{
		Request:        startReq,
		NewTaskShardId: persistence.DefaultShardId,
	})

	require.NoError(t, err)
	require.NoError(t, startResp.AppDatabaseWritingError)
	ass.False(startResp.AlreadyStarted)
	ass.True(startResp.HasNewImmediateTask)
	ass.True(len(startResp.ProcessExecutionId.String()) > 0)
	return startResp.ProcessExecutionId
}

func startProcess(
	ctx context.Context, t *testing.T, ass *assert.Assertions,
	store persistence.ProcessStore, namespace, processId string, input xcapi.EncodedObject,
) uuid.UUID {
	return startProcessWithConfigs(ctx, t, ass, store, namespace, processId, input, nil, nil)
}

func terminateProcess(
	ctx context.Context, t *testing.T, ass *assert.Assertions,
	store persistence.ProcessStore, namespace, processId string,
) {
	resp, err := store.StopProcess(ctx, data_models.StopProcessRequest{
		Namespace:       namespace,
		ProcessId:       processId,
		ProcessStopType: xcapi.TERMINATE,
	})

	require.NoError(t, err)
	ass.False(resp.NotExists)
}

func startProcessWithAllowIfPreviousExitAbnormally(
	ctx context.Context, t *testing.T, ass *assert.Assertions,
	store persistence.ProcessStore, namespace, processId string, input xcapi.EncodedObject,
) uuid.UUID {
	startReq := createStartRequestWithAllowIfPreviousExitAbnormallyPolicy(namespace, processId, input)
	startResp, err := store.StartProcess(ctx, data_models.StartProcessRequest{
		Request:        startReq,
		NewTaskShardId: persistence.DefaultShardId,
	})

	require.NoError(t, err)
	ass.False(startResp.AlreadyStarted)
	ass.True(startResp.HasNewImmediateTask)
	ass.True(len(startResp.ProcessExecutionId.String()) > 0)
	return startResp.ProcessExecutionId
}

func startProcessWithTerminateIfRunningPolicy(
	ctx context.Context, t *testing.T, ass *assert.Assertions,
	store persistence.ProcessStore, namespace, processId string, input xcapi.EncodedObject,
) uuid.UUID {
	startReq := createStartRequestWithTerminateIfRunningPolicy(namespace, processId, input)
	startResp, err := store.StartProcess(ctx, data_models.StartProcessRequest{
		Request:        startReq,
		NewTaskShardId: persistence.DefaultShardId,
	})

	require.NoError(t, err)
	ass.False(startResp.AlreadyStarted)
	ass.True(startResp.HasNewImmediateTask)
	ass.True(len(startResp.ProcessExecutionId.String()) > 0)
	return startResp.ProcessExecutionId
}

func startProcessWithAllowIfNoRunningPolicy(
	ctx context.Context, t *testing.T, ass *assert.Assertions,
	store persistence.ProcessStore, namespace, processId string, input xcapi.EncodedObject,
) uuid.UUID {
	startReq := createStartRequestWithAllowIfNoRunningPolicy(namespace, processId, input)
	startResp, err := store.StartProcess(ctx, data_models.StartProcessRequest{
		Request:        startReq,
		NewTaskShardId: persistence.DefaultShardId,
	})

	require.NoError(t, err)
	ass.False(startResp.AlreadyStarted)
	ass.True(startResp.HasNewImmediateTask)
	ass.True(len(startResp.ProcessExecutionId.String()) > 0)
	return startResp.ProcessExecutionId
}

func startProcessWithDisallowReusePolicy(
	ctx context.Context, t *testing.T, ass *assert.Assertions,
	store persistence.ProcessStore, namespace, processId string, input xcapi.EncodedObject,
) uuid.UUID {
	startReq := createStartRequestWithDisallowReusePolicy(namespace, processId, input)
	startResp, err := store.StartProcess(ctx, data_models.StartProcessRequest{
		Request:        startReq,
		NewTaskShardId: persistence.DefaultShardId,
	})

	require.NoError(t, err)
	ass.True(startResp.AlreadyStarted)
	return startResp.ProcessExecutionId
}

func createStartRequestWithAllowIfPreviousExitAbnormallyPolicy(
	namespace, processId string, input xcapi.EncodedObject,
) xcapi.ProcessExecutionStartRequest {
	// Other values like processType, workerUrl etc. are kept constants for simplicity
	return xcapi.ProcessExecutionStartRequest{
		Namespace:        namespace,
		ProcessId:        processId,
		ProcessType:      "test-type",
		WorkerUrl:        "test-url",
		StartStateId:     ptr.Any(stateId1),
		StartStateInput:  &input,
		StartStateConfig: nil,
		ProcessStartConfig: &xcapi.ProcessStartConfig{
			TimeoutSeconds: ptr.Any(int32(100)),
			IdReusePolicy:  xcapi.ALLOW_IF_PREVIOUS_EXIT_ABNORMALLY.Ptr().Ptr(),
		},
	}
}

func createStartRequestWithDisallowReusePolicy(
	namespace, processId string, input xcapi.EncodedObject,
) xcapi.ProcessExecutionStartRequest {
	// Other values like processType, workerUrl etc. are kept constants for simplicity
	return xcapi.ProcessExecutionStartRequest{
		Namespace:        namespace,
		ProcessId:        processId,
		ProcessType:      "test-type",
		WorkerUrl:        "test-url",
		StartStateId:     ptr.Any(stateId1),
		StartStateInput:  &input,
		StartStateConfig: nil,
		ProcessStartConfig: &xcapi.ProcessStartConfig{
			TimeoutSeconds: ptr.Any(int32(100)),
			IdReusePolicy:  xcapi.DISALLOW_REUSE.Ptr(),
		},
	}
}

func createStartRequestWithAllowIfNoRunningPolicy(
	namespace, processId string, input xcapi.EncodedObject,
) xcapi.ProcessExecutionStartRequest {
	// Other values like processType, workerUrl etc. are kept constants for simplicity
	return xcapi.ProcessExecutionStartRequest{
		Namespace:        namespace,
		ProcessId:        processId,
		ProcessType:      "test-type",
		WorkerUrl:        "test-url",
		StartStateId:     ptr.Any(stateId1),
		StartStateInput:  &input,
		StartStateConfig: nil,
		ProcessStartConfig: &xcapi.ProcessStartConfig{
			TimeoutSeconds: ptr.Any(int32(100)),
			IdReusePolicy:  xcapi.ALLOW_IF_NO_RUNNING.Ptr(),
		},
	}
}

func createStartRequestWithTerminateIfRunningPolicy(
	namespace, processId string, input xcapi.EncodedObject,
) xcapi.ProcessExecutionStartRequest {
	// Other values like processType, workerUrl etc. are kept constants for simplicity
	return xcapi.ProcessExecutionStartRequest{
		Namespace:        namespace,
		ProcessId:        processId,
		ProcessType:      "test-type",
		WorkerUrl:        "test-url",
		StartStateId:     ptr.Any(stateId1),
		StartStateInput:  &input,
		StartStateConfig: nil,
		ProcessStartConfig: &xcapi.ProcessStartConfig{
			TimeoutSeconds: ptr.Any(int32(100)),
			IdReusePolicy:  xcapi.TERMINATE_IF_RUNNING.Ptr(),
		},
	}
}

func createStartRequest(
	namespace, processId string, input xcapi.EncodedObject,
	appDatabaseConfig *xcapi.AppDatabaseConfig, stateCfg *xcapi.AsyncStateConfig,
) xcapi.ProcessExecutionStartRequest {
	// Other values like processType, workerUrl etc. are kept constants for simplicity
	return xcapi.ProcessExecutionStartRequest{
		Namespace:        namespace,
		ProcessId:        processId,
		ProcessType:      "test-type",
		WorkerUrl:        "test-url",
		StartStateId:     ptr.Any(stateId1),
		StartStateInput:  &input,
		StartStateConfig: stateCfg,
		ProcessStartConfig: &xcapi.ProcessStartConfig{
			TimeoutSeconds:    ptr.Any(int32(100)),
			AppDatabaseConfig: appDatabaseConfig,
		},
	}
}

func retryStartProcessForFailure(
	ctx context.Context, t *testing.T, ass *assert.Assertions,
	store persistence.ProcessStore, namespace, processId string, input xcapi.EncodedObject,
) {
	startReq := createStartRequest(namespace, processId, input, nil, nil)
	startResp2, err := store.StartProcess(ctx, data_models.StartProcessRequest{
		Request:        startReq,
		NewTaskShardId: persistence.DefaultShardId,
	})
	require.NoError(t, err)
	ass.True(startResp2.AlreadyStarted)
	ass.False(startResp2.HasNewImmediateTask)
}

func describeProcess(
	ctx context.Context, t *testing.T, ass *assert.Assertions, store persistence.ProcessStore,
	namespace, processId string, processStatus xcapi.ProcessStatus,
) {
	// Incorrect process id description
	descResp, err := store.DescribeLatestProcess(ctx, data_models.DescribeLatestProcessRequest{
		Namespace: namespace,
		ProcessId: "some-wrong-id",
	})
	require.NoError(t, err)
	ass.True(descResp.NotExists)

	// Correct process id description
	descResp, err = store.DescribeLatestProcess(ctx, data_models.DescribeLatestProcessRequest{
		Namespace: namespace,
		ProcessId: processId,
	})
	require.NoError(t, err)
	ass.False(descResp.NotExists)
	ass.Equal(testProcessType, descResp.Response.GetProcessType())
	ass.Equal(testWorkerUrl, descResp.Response.GetWorkerUrl())
	ass.Equal(processStatus, descResp.Response.GetStatus())
}

func checkAndGetImmediateTasks(
	ctx context.Context, t *testing.T, ass *assert.Assertions, store persistence.ProcessStore, expectedLength int,
) (int64, int64, []data_models.ImmediateTask) {
	getTasksResp, err := store.GetImmediateTasks(ctx, data_models.GetImmediateTasksRequest{
		ShardId:                persistence.DefaultShardId,
		StartSequenceInclusive: 0,
		PageSize:               10,
	})
	require.NoError(t, err)
	ass.Equal(expectedLength, len(getTasksResp.Tasks))
	return getTasksResp.MinSequenceInclusive, getTasksResp.MaxSequenceInclusive, getTasksResp.Tasks
}

func getAndCheckTimerTasksUpToTs(
	ctx context.Context, t *testing.T, ass *assert.Assertions, store persistence.ProcessStore, expectedLength int,
	upToTimestamp int64,
) (int64, int64, []data_models.TimerTask) {
	getTasksResp, err := store.GetTimerTasksUpToTimestamp(ctx, data_models.GetTimerTasksRequest{
		ShardId:                          persistence.DefaultShardId,
		MaxFireTimestampSecondsInclusive: upToTimestamp,
		PageSize:                         10,
	})
	require.NoError(t, err)
	ass.Equal(expectedLength, len(getTasksResp.Tasks))
	return getTasksResp.MinSequenceInclusive, getTasksResp.MaxSequenceInclusive, getTasksResp.Tasks
}

func getAndCheckTimerTasksUpForTimestamps(
	ctx context.Context, t *testing.T, ass *assert.Assertions, store persistence.ProcessStore, expectedLength int,
	forTimestamps []int64, minTaskSeq int64,
) (int64, int64, []data_models.TimerTask) {
	getTasksResp, err := store.GetTimerTasksForTimestamps(ctx, data_models.GetTimerTasksForTimestampsRequest{
		ShardId:              persistence.DefaultShardId,
		MinSequenceInclusive: minTaskSeq,
		DetailedRequests: []xcapi.NotifyTimerTasksRequest{
			{
				FireTimestamps: forTimestamps,
			},
		},
	})
	require.NoError(t, err)
	ass.Equal(expectedLength, len(getTasksResp.Tasks))
	return getTasksResp.MinSequenceInclusive, getTasksResp.MaxSequenceInclusive, getTasksResp.Tasks
}

func verifyImmediateTaskNoInfo(
	ass *assert.Assertions, task data_models.ImmediateTask,
	taskType data_models.ImmediateTaskType, stateExeId string,
) {
	verifyImmediateTask(ass, task, taskType, stateExeId, data_models.ImmediateTaskInfoJson{})
}

func verifyImmediateTask(
	ass *assert.Assertions, task data_models.ImmediateTask,
	taskType data_models.ImmediateTaskType, stateExeId string,
	info data_models.ImmediateTaskInfoJson,
) {
	ass.NotNil(task.StateIdSequence)
	ass.Equal(persistence.DefaultShardId, int(task.ShardId))
	ass.Equal(taskType, task.TaskType)
	ass.Equal(stateExeId, task.GetStateExecutionId())
	ass.True(task.TaskSequence != nil)
	ass.Equal(info, task.ImmediateTaskInfo)
}

func verifyTimerTask(
	ass *assert.Assertions, task data_models.TimerTask,
	taskType data_models.TimerTaskType, stateExeId string,
	taskInfo data_models.TimerTaskInfoJson,
) {
	ass.NotNil(task.StateIdSequence)
	ass.Equal(persistence.DefaultShardId, int(task.ShardId))
	ass.Equal(taskType, task.TaskType)
	ass.Equal(stateExeId, task.GetStateExecutionId())
	ass.True(task.TaskSequence != nil)
	ass.Equal(taskInfo, task.TimerTaskInfo)
}

func deleteAndVerifyImmediateTasksDeleted(
	ctx context.Context, t *testing.T, ass *assert.Assertions, store persistence.ProcessStore, minSeq, maxSeq int64,
) {
	err := store.DeleteImmediateTasks(ctx, data_models.DeleteImmediateTasksRequest{
		ShardId:                  persistence.DefaultShardId,
		MinTaskSequenceInclusive: minSeq,
		MaxTaskSequenceInclusive: maxSeq,
	})
	require.NoError(t, err)
	checkAndGetImmediateTasks(ctx, t, ass, store, 0) // Expect no tasks
}

func prepareStateExecution(
	ctx context.Context, t *testing.T,
	store persistence.ProcessStore, prcExeId uuid.UUID, stateId string, stateIdSeq int32,
) *data_models.PrepareStateExecutionResponse {
	stateExeId := data_models.StateExecutionId{
		StateId:         stateId,
		StateIdSequence: stateIdSeq,
	}
	prep, err := store.PrepareStateExecution(ctx, data_models.PrepareStateExecutionRequest{
		ProcessExecutionId: prcExeId,
		StateExecutionId:   stateExeId,
	})
	require.NoError(t, err)
	return prep
}

func verifyStateExecution(
	ass *assert.Assertions,
	prep *data_models.PrepareStateExecutionResponse,
	processId string, input xcapi.EncodedObject,
	expectedStatus data_models.StateExecutionStatus,
) {
	ass.Equal(testWorkerUrl, prep.Info.WorkerURL)
	ass.Equal(testProcessType, prep.Info.ProcessType)
	ass.Equal(processId, prep.Info.ProcessId)
	ass.Equal(input, prep.Input)
	ass.Equal(expectedStatus, prep.Status)
}

func completeWaitUntilExecution(
	ctx context.Context, t *testing.T, ass *assert.Assertions,
	store persistence.ProcessStore, prcExeId uuid.UUID, immediateTask data_models.ImmediateTask,
	prep *data_models.PrepareStateExecutionResponse,
) {
	stateExeId := data_models.StateExecutionId{
		StateId:         immediateTask.StateId,
		StateIdSequence: immediateTask.StateIdSequence,
	}
	compWaitResp, err := store.ProcessWaitUntilExecution(ctx, data_models.ProcessWaitUntilExecutionRequest{
		ProcessExecutionId: prcExeId,
		StateExecutionId:   stateExeId,
		Prepare:            *prep,
		CommandRequest: xcapi.CommandRequest{
			WaitingType: xcapi.EMPTY_COMMAND,
		},
		TaskShardId: persistence.DefaultShardId,
	})
	require.NoError(t, err)
	ass.True(compWaitResp.HasNewImmediateTask)
}

func completeExecuteExecution(
	ctx context.Context, t *testing.T, ass *assert.Assertions,
	store persistence.ProcessStore, prcExeId uuid.UUID, immediateTask data_models.ImmediateTask,
	prep *data_models.PrepareStateExecutionResponse,
	stateDecision xcapi.StateDecision, hasNewImmediateTask bool,
) {
	completeExecuteExecutionWithAppDatabase(
		ctx, t, ass, store, prcExeId, immediateTask, prep,
		stateDecision, hasNewImmediateTask, nil, nil,
	)
}
func completeExecuteExecutionWithAppDatabase(
	ctx context.Context, t *testing.T, ass *assert.Assertions,
	store persistence.ProcessStore, prcExeId uuid.UUID, immediateTask data_models.ImmediateTask,
	prep *data_models.PrepareStateExecutionResponse,
	stateDecision xcapi.StateDecision, hasNewImmediateTask bool,
	appDatabaseConfig *data_models.InternalAppDatabaseConfig,
	appDatabaseWrite *xcapi.AppDatabaseWrite,
) {
	stateExeId := data_models.StateExecutionId{
		StateId:         immediateTask.StateId,
		StateIdSequence: immediateTask.StateIdSequence,
	}
	compResp, err := store.CompleteExecuteExecution(ctx, data_models.CompleteExecuteExecutionRequest{
		ProcessExecutionId: prcExeId,
		StateExecutionId:   stateExeId,
		Prepare:            *prep,
		StateDecision:      stateDecision,
		TaskShardId:        persistence.DefaultShardId,
		AppDatabaseConfig:  appDatabaseConfig,
		WriteAppDatabase:   appDatabaseWrite,
	})
	require.NoError(t, err)
	require.NoError(t, compResp.AppDatabaseWritingError)
	ass.Equal(hasNewImmediateTask, compResp.HasNewImmediateTask)
}

func recoverFromFailure(
	t *testing.T,
	ctx context.Context,
	assert *assert.Assertions,
	store persistence.ProcessStore,
	namespace string,
	prcExeId uuid.UUID,
	prep data_models.PrepareStateExecutionResponse,
	sourceStateExecId data_models.StateExecutionId,
	sourceFailedStateApi xcapi.WorkerApiType,
	destinationStateId string,
	destiantionStateConfig *xcapi.AsyncStateConfig,
	destinationStateInput xcapi.EncodedObject,
) {
	request := data_models.RecoverFromStateExecutionFailureRequest{
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
	descResp, err := store.DescribeLatestProcess(ctx, data_models.DescribeLatestProcessRequest{
		Namespace: namespace,
		ProcessId: prep.Info.ProcessId,
	})
	require.NoError(t, err)
	assert.Equal(xcapi.RUNNING, *descResp.Response.Status)
}

// this won't guarantee the actual equality, but it's good enough for our test
// when the objects are large, it's hard to use assert.Equal or assert.ElementsMatch
func assertProbablyEqualForIgnoringOrderByJsonEncoder(
	t *testing.T, ass *assert.Assertions, obj1, obj2 interface{},
) {
	str1, err1 := json.Marshal(obj1)
	str2, err2 := json.Marshal(obj2)
	require.NoError(t, err1)
	require.NoError(t, err2)
	ass.Equal(len(str1), len(str2))
}
