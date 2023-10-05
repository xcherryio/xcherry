package tests

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/xdblab/xdb-apis/goapi/xdbapi"
	"github.com/xdblab/xdb/common/ptr"
	"github.com/xdblab/xdb/persistence"
	"time"
)

func testSQLBasicExecution(ass *assert.Assertions, store persistence.ProcessStore) {
	// TODO need to refactor this test
	ctx := context.Background()
	// start process
	namespace := "test-ns"
	processId := fmt.Sprintf("test-prcid-%v", time.Now().String())
	processType := "test-type"
	workerUrl := "test-url"
	startStateId := "init-state"
	input := xdbapi.EncodedObject{
		Encoding: ptr.Any("test-encoding"),
		Data:     ptr.Any("test-data"),
	}
	startReq := xdbapi.ProcessExecutionStartRequest{
		Namespace:        namespace,
		ProcessId:        processId,
		ProcessType:      processType,
		WorkerUrl:        workerUrl,
		StartStateId:     &startStateId,
		StartStateInput:  &input,
		StartStateConfig: nil,
		ProcessStartConfig: &xdbapi.ProcessStartConfig{
			TimeoutSeconds: ptr.Any(int32(100)),
		},
	}

	startResp, err := store.StartProcess(ctx, persistence.StartProcessRequest{
		Request:        startReq,
		NewTaskShardId: persistence.DefaultShardId,
	})
	ass.Nil(err)
	ass.False(startResp.AlreadyStarted)
	ass.True(startResp.HasNewWorkerTask)
	ass.True(len(startResp.ProcessExecutionId.String()) > 0)

	// start again
	startResp2, err := store.StartProcess(ctx, persistence.StartProcessRequest{
		Request:        startReq,
		NewTaskShardId: persistence.DefaultShardId,
	})
	ass.Nil(err)
	ass.True(startResp2.AlreadyStarted)
	ass.False(startResp2.HasNewWorkerTask)

	descResp, err := store.DescribeLatestProcess(ctx, persistence.DescribeLatestProcessRequest{
		Namespace: namespace,
		ProcessId: "some-wrong-id",
	})
	ass.Nil(err)
	ass.Equal(descResp.NotExists, true)

	descResp, err = store.DescribeLatestProcess(ctx, persistence.DescribeLatestProcessRequest{
		Namespace: namespace,
		ProcessId: processId,
	})
	ass.Nil(err)
	ass.False(descResp.NotExists)
	ass.Equal(processType, descResp.Response.GetProcessType())
	ass.Equal(workerUrl, descResp.Response.GetWorkerUrl())

	getTasksResp, err := store.GetWorkerTasks(ctx, persistence.GetWorkerTasksRequest{
		ShardId:                persistence.DefaultShardId,
		StartSequenceInclusive: 0,
		PageSize:               10,
	})
	ass.Nil(err)
	ass.Equal(1, len(getTasksResp.Tasks))
	workerTask := getTasksResp.Tasks[0]
	ass.Equal(persistence.DefaultShardId, int(workerTask.ShardId))
	ass.Equal(persistence.WorkerTaskTypeWaitUntil, workerTask.TaskType)
	ass.Equal(1, int(workerTask.StateIdSequence))
	ass.True(workerTask.TaskSequence != nil)

	err = store.DeleteWorkerTasks(ctx, persistence.DeleteWorkerTasksRequest{
		ShardId:                  persistence.DefaultShardId,
		MinTaskSequenceInclusive: getTasksResp.MinSequenceInclusive,
		MaxTaskSequenceInclusive: getTasksResp.MaxSequenceInclusive,
	})

	getTasksResp, err = store.GetWorkerTasks(ctx, persistence.GetWorkerTasksRequest{
		ShardId:                persistence.DefaultShardId,
		StartSequenceInclusive: 0,
		PageSize:               10,
	})
	ass.Equal(0, len(getTasksResp.Tasks))

	stateExeId := persistence.StateExecutionId{
		StateId:         startStateId,
		StateIdSequence: workerTask.StateIdSequence,
	}
	prep, err := store.PrepareStateExecution(ctx, persistence.PrepareStateExecutionRequest{
		ProcessExecutionId: startResp.ProcessExecutionId,
		StateExecutionId:   stateExeId,
	})
	ass.Nil(err)
	ass.Equal(workerUrl, prep.Info.WorkerURL)
	ass.Equal(processType, prep.Info.ProcessType)
	ass.Equal(processId, prep.Info.ProcessId)
	ass.Equal(input, prep.Input)
	ass.Equal(persistence.StateExecutionStatusRunning, prep.WaitUntilStatus)
	ass.Equal(persistence.StateExecutionStatusUndefined, prep.ExecuteStatus)

	compWaitResp, err := store.CompleteWaitUntilExecution(ctx, persistence.CompleteWaitUntilExecutionRequest{
		ProcessExecutionId: startResp.ProcessExecutionId,
		StateExecutionId:   stateExeId,
		Prepare:            *prep,
		CommandRequest: xdbapi.CommandRequest{
			WaitingType: xdbapi.EMPTY_COMMAND.Ptr(),
		},
		TaskShardId: persistence.DefaultShardId,
	})
	ass.Nil(err)
	ass.True(compWaitResp.HasNewWorkerTask)

	// it should fail if completing again because of the version check
	_, err = store.CompleteWaitUntilExecution(ctx, persistence.CompleteWaitUntilExecutionRequest{
		ProcessExecutionId: startResp.ProcessExecutionId,
		StateExecutionId:   stateExeId,
		Prepare:            *prep,
		CommandRequest: xdbapi.CommandRequest{
			WaitingType: xdbapi.EMPTY_COMMAND.Ptr(),
		},
		TaskShardId: persistence.DefaultShardId,
	})
	ass.NotNil(err)

	getTasksResp, err = store.GetWorkerTasks(ctx, persistence.GetWorkerTasksRequest{
		ShardId:                persistence.DefaultShardId,
		StartSequenceInclusive: 0,
		PageSize:               10,
	})
	ass.Equal(1, len(getTasksResp.Tasks))
	workerTask = getTasksResp.Tasks[0]
	ass.Equal(persistence.DefaultShardId, int(workerTask.ShardId))
	ass.Equal(persistence.WorkerTaskTypeExecute, workerTask.TaskType)
	ass.Equal(1, int(workerTask.StateIdSequence))
	ass.True(workerTask.TaskSequence != nil)

	err = store.DeleteWorkerTasks(ctx, persistence.DeleteWorkerTasksRequest{
		ShardId:                  persistence.DefaultShardId,
		MinTaskSequenceInclusive: getTasksResp.MinSequenceInclusive,
		MaxTaskSequenceInclusive: getTasksResp.MaxSequenceInclusive,
	})

	getTasksResp, err = store.GetWorkerTasks(ctx, persistence.GetWorkerTasksRequest{
		ShardId:                persistence.DefaultShardId,
		StartSequenceInclusive: 0,
		PageSize:               10,
	})
	ass.Equal(0, len(getTasksResp.Tasks))

	prep, err = store.PrepareStateExecution(ctx, persistence.PrepareStateExecutionRequest{
		ProcessExecutionId: startResp.ProcessExecutionId,
		StateExecutionId:   stateExeId,
	})
	ass.Nil(err)
	ass.Equal(persistence.StateExecutionStatusCompleted, prep.WaitUntilStatus)
	ass.Equal(persistence.StateExecutionStatusRunning, prep.ExecuteStatus)

	stateId2 := "state2"
	compExeResp, err := store.CompleteExecuteExecution(ctx, persistence.CompleteExecuteExecutionRequest{
		ProcessExecutionId: startResp.ProcessExecutionId,
		StateExecutionId:   stateExeId,
		Prepare:            *prep,
		StateDecision: xdbapi.StateDecision{
			NextStates: []xdbapi.StateMovement{
				{
					StateId: stateId2,
					// no input, skip waitUntil
					StateConfig: &xdbapi.AsyncStateConfig{SkipWaitUntil: ptr.Any(true)},
				},
				{
					StateId: startStateId, // use the same stateId
					// no input, skip waitUntil
					StateConfig: &xdbapi.AsyncStateConfig{SkipWaitUntil: ptr.Any(true)},
				},
			},
		},
		TaskShardId: persistence.DefaultShardId,
	})
	ass.Nil(err)
	ass.True(compExeResp.HasNewWorkerTask)

	getTasksResp, err = store.GetWorkerTasks(ctx, persistence.GetWorkerTasksRequest{
		ShardId:                persistence.DefaultShardId,
		StartSequenceInclusive: 0,
		PageSize:               10,
	})
	ass.Nil(err)
	ass.Equal(2, len(getTasksResp.Tasks))
	workerTask = getTasksResp.Tasks[0]
	ass.Equal(persistence.DefaultShardId, int(workerTask.ShardId))
	ass.True(workerTask.TaskSequence != nil)
	ass.Equal(persistence.WorkerTaskTypeExecute, workerTask.TaskType)
	ass.Equal(stateId2, workerTask.StateId)
	ass.Equal(1, int(workerTask.StateIdSequence))

	workerTask = getTasksResp.Tasks[1]
	ass.Equal(persistence.DefaultShardId, int(workerTask.ShardId))
	ass.True(workerTask.TaskSequence != nil)
	ass.Equal(persistence.WorkerTaskTypeExecute, workerTask.TaskType)
	ass.Equal(startStateId, workerTask.StateId)
	ass.Equal(2, int(workerTask.StateIdSequence))

	err = store.DeleteWorkerTasks(ctx, persistence.DeleteWorkerTasksRequest{
		ShardId:                  persistence.DefaultShardId,
		MinTaskSequenceInclusive: getTasksResp.MinSequenceInclusive,
		MaxTaskSequenceInclusive: getTasksResp.MaxSequenceInclusive,
	})

	stateExeId = persistence.StateExecutionId{
		StateId:         workerTask.StateId,
		StateIdSequence: workerTask.StateIdSequence,
	}

	prep, err = store.PrepareStateExecution(ctx, persistence.PrepareStateExecutionRequest{
		ProcessExecutionId: startResp.ProcessExecutionId,
		StateExecutionId:   stateExeId,
	})
	ass.Nil(err)
	ass.Equal(persistence.StateExecutionStatusSkipped, prep.WaitUntilStatus)
	ass.Equal(persistence.StateExecutionStatusRunning, prep.ExecuteStatus)

	compExeResp, err = store.CompleteExecuteExecution(ctx, persistence.CompleteExecuteExecutionRequest{
		ProcessExecutionId: startResp.ProcessExecutionId,
		StateExecutionId:   stateExeId,
		Prepare:            *prep,
		StateDecision: xdbapi.StateDecision{
			ThreadCloseDecision: &xdbapi.ThreadCloseDecision{
				CloseType: xdbapi.FORCE_COMPLETE_PROCESS.Ptr(),
			},
		},
		TaskShardId: persistence.DefaultShardId,
	})
	ass.Nil(err)
	ass.False(compExeResp.HasNewWorkerTask)
}
