// Copyright (c) XDBLab
// SPDX-License-Identifier: BUSL-1.1

package sqltest

import (
	"context"
	"encoding/json"
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

func startProcessWithConfigs(
	ctx context.Context, t *testing.T, ass *assert.Assertions, store persistence.ProcessStore,
	namespace, processId string,
	input xdbapi.EncodedObject, gloAttCfg *xdbapi.GlobalAttributeConfig, stateCfg *xdbapi.AsyncStateConfig,
) uuid.UUID {
	startReq := createStartRequest(namespace, processId, input, gloAttCfg, stateCfg)
	startResp, err := store.StartProcess(ctx, persistence.StartProcessRequest{
		Request:        startReq,
		NewTaskShardId: persistence.DefaultShardId,
	})

	require.NoError(t, err)
	require.NoError(t, startResp.GlobalAttributeWriteError)
	ass.False(startResp.AlreadyStarted)
	ass.True(startResp.HasNewImmediateTask)
	ass.True(len(startResp.ProcessExecutionId.String()) > 0)
	return startResp.ProcessExecutionId
}

func startProcess(
	ctx context.Context, t *testing.T, ass *assert.Assertions,
	store persistence.ProcessStore, namespace, processId string, input xdbapi.EncodedObject,
) uuid.UUID {
	return startProcessWithConfigs(ctx, t, ass, store, namespace, processId, input, nil, nil)
}

func terminateProcess(
	ctx context.Context, t *testing.T, ass *assert.Assertions,
	store persistence.ProcessStore, namespace, processId string,
) {
	resp, err := store.StopProcess(ctx, persistence.StopProcessRequest{
		Namespace:       namespace,
		ProcessId:       processId,
		ProcessStopType: xdbapi.TERMINATE,
	})

	require.NoError(t, err)
	ass.False(resp.NotExists)
}

func startProcessWithAllowIfPreviousExitAbnormally(
	ctx context.Context, t *testing.T, ass *assert.Assertions,
	store persistence.ProcessStore, namespace, processId string, input xdbapi.EncodedObject,
) uuid.UUID {
	startReq := createStartRequestWithAllowIfPreviousExitAbnormallyPolicy(namespace, processId, input)
	startResp, err := store.StartProcess(ctx, persistence.StartProcessRequest{
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
	store persistence.ProcessStore, namespace, processId string, input xdbapi.EncodedObject,
) uuid.UUID {
	startReq := createStartRequestWithTerminateIfRunningPolicy(namespace, processId, input)
	startResp, err := store.StartProcess(ctx, persistence.StartProcessRequest{
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
	store persistence.ProcessStore, namespace, processId string, input xdbapi.EncodedObject,
) uuid.UUID {
	startReq := createStartRequestWithAllowIfNoRunningPolicy(namespace, processId, input)
	startResp, err := store.StartProcess(ctx, persistence.StartProcessRequest{
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
	store persistence.ProcessStore, namespace, processId string, input xdbapi.EncodedObject,
) uuid.UUID {
	startReq := createStartRequestWithDisallowReusePolicy(namespace, processId, input)
	startResp, err := store.StartProcess(ctx, persistence.StartProcessRequest{
		Request:        startReq,
		NewTaskShardId: persistence.DefaultShardId,
	})

	require.NoError(t, err)
	ass.True(startResp.AlreadyStarted)
	return startResp.ProcessExecutionId
}

func createStartRequestWithAllowIfPreviousExitAbnormallyPolicy(
	namespace, processId string, input xdbapi.EncodedObject,
) xdbapi.ProcessExecutionStartRequest {
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

func createStartRequestWithDisallowReusePolicy(
	namespace, processId string, input xdbapi.EncodedObject,
) xdbapi.ProcessExecutionStartRequest {
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

func createStartRequestWithAllowIfNoRunningPolicy(
	namespace, processId string, input xdbapi.EncodedObject,
) xdbapi.ProcessExecutionStartRequest {
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

func createStartRequestWithTerminateIfRunningPolicy(
	namespace, processId string, input xdbapi.EncodedObject,
) xdbapi.ProcessExecutionStartRequest {
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

func createStartRequest(
	namespace, processId string, input xdbapi.EncodedObject,
	gloAttCfg *xdbapi.GlobalAttributeConfig, stateCfg *xdbapi.AsyncStateConfig,
) xdbapi.ProcessExecutionStartRequest {
	// Other values like processType, workerUrl etc. are kept constants for simplicity
	return xdbapi.ProcessExecutionStartRequest{
		Namespace:        namespace,
		ProcessId:        processId,
		ProcessType:      "test-type",
		WorkerUrl:        "test-url",
		StartStateId:     ptr.Any(stateId1),
		StartStateInput:  &input,
		StartStateConfig: stateCfg,
		ProcessStartConfig: &xdbapi.ProcessStartConfig{
			TimeoutSeconds:        ptr.Any(int32(100)),
			GlobalAttributeConfig: gloAttCfg,
		},
	}
}

func retryStartProcessForFailure(
	ctx context.Context, t *testing.T, ass *assert.Assertions,
	store persistence.ProcessStore, namespace, processId string, input xdbapi.EncodedObject,
) {
	startReq := createStartRequest(namespace, processId, input, nil, nil)
	startResp2, err := store.StartProcess(ctx, persistence.StartProcessRequest{
		Request:        startReq,
		NewTaskShardId: persistence.DefaultShardId,
	})
	require.NoError(t, err)
	ass.True(startResp2.AlreadyStarted)
	ass.False(startResp2.HasNewImmediateTask)
}

func describeProcess(
	ctx context.Context, t *testing.T, ass *assert.Assertions, store persistence.ProcessStore,
	namespace, processId string, processStatus xdbapi.ProcessStatus,
) {
	// Incorrect process id description
	descResp, err := store.DescribeLatestProcess(ctx, persistence.DescribeLatestProcessRequest{
		Namespace: namespace,
		ProcessId: "some-wrong-id",
	})
	require.NoError(t, err)
	ass.True(descResp.NotExists)

	// Correct process id description
	descResp, err = store.DescribeLatestProcess(ctx, persistence.DescribeLatestProcessRequest{
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
) (int64, int64, []persistence.ImmediateTask) {
	getTasksResp, err := store.GetImmediateTasks(ctx, persistence.GetImmediateTasksRequest{
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
) (int64, int64, []persistence.TimerTask) {
	getTasksResp, err := store.GetTimerTasksUpToTimestamp(ctx, persistence.GetTimerTasksRequest{
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
	require.NoError(t, err)
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
	ctx context.Context, t *testing.T, ass *assert.Assertions, store persistence.ProcessStore, minSeq, maxSeq int64,
) {
	err := store.DeleteImmediateTasks(ctx, persistence.DeleteImmediateTasksRequest{
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
) *persistence.PrepareStateExecutionResponse {
	stateExeId := persistence.StateExecutionId{
		StateId:         stateId,
		StateIdSequence: stateIdSeq,
	}
	prep, err := store.PrepareStateExecution(ctx, persistence.PrepareStateExecutionRequest{
		ProcessExecutionId: prcExeId,
		StateExecutionId:   stateExeId,
	})
	require.NoError(t, err)
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
	ctx context.Context, t *testing.T, ass *assert.Assertions,
	store persistence.ProcessStore, prcExeId uuid.UUID, immediateTask persistence.ImmediateTask,
	prep *persistence.PrepareStateExecutionResponse,
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
	require.NoError(t, err)
	ass.True(compWaitResp.HasNewImmediateTask)
}

func completeExecuteExecution(
	ctx context.Context, t *testing.T, ass *assert.Assertions,
	store persistence.ProcessStore, prcExeId uuid.UUID, immediateTask persistence.ImmediateTask,
	prep *persistence.PrepareStateExecutionResponse,
	stateDecision xdbapi.StateDecision, hasNewImmediateTask bool,
) {
	completeExecuteExecutionWithGlobalAttributes(
		ctx, t, ass, store, prcExeId, immediateTask, prep,
		stateDecision, hasNewImmediateTask, nil, nil,
	)
}
func completeExecuteExecutionWithGlobalAttributes(
	ctx context.Context, t *testing.T, ass *assert.Assertions,
	store persistence.ProcessStore, prcExeId uuid.UUID, immediateTask persistence.ImmediateTask,
	prep *persistence.PrepareStateExecutionResponse,
	stateDecision xdbapi.StateDecision, hasNewImmediateTask bool,
	gloAttCfg *persistence.InternalGlobalAttributeConfig,
	gloAttUpdates []xdbapi.GlobalAttributeTableRowUpdate,
) {
	stateExeId := persistence.StateExecutionId{
		StateId:         immediateTask.StateId,
		StateIdSequence: immediateTask.StateIdSequence,
	}
	compResp, err := store.CompleteExecuteExecution(ctx, persistence.CompleteExecuteExecutionRequest{
		ProcessExecutionId:         prcExeId,
		StateExecutionId:           stateExeId,
		Prepare:                    *prep,
		StateDecision:              stateDecision,
		TaskShardId:                persistence.DefaultShardId,
		GlobalAttributeTableConfig: gloAttCfg,
		UpdateGlobalAttributes:     gloAttUpdates,
	})
	require.NoError(t, err)
	require.NoError(t, compResp.UpdatingGlobalAttributesError)
	ass.Equal(hasNewImmediateTask, compResp.HasNewImmediateTask)
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
	destinationStateInput xdbapi.EncodedObject,
) {
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
