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

func testSQL(ass *assert.Assertions, store persistence.ProcessStore) {
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

	// start again
	startResp, err = store.StartProcess(ctx, persistence.StartProcessRequest{
		Request:        startReq,
		NewTaskShardId: persistence.DefaultShardId,
	})
	ass.Nil(err)
	ass.True(startResp.AlreadyStarted)

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
	ass.Equal(persistence.DefaultShardId, workerTask.ShardId)
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
	ass.Equal(persistence.DefaultShardId, workerTask.ShardId)
	ass.True(workerTask.TaskSequence != nil)

	err = store.DeleteWorkerTasks(ctx, persistence.DeleteWorkerTasksRequest{
		ShardId:                  persistence.DefaultShardId,
		MinTaskSequenceInclusive: getTasksResp.MinSequenceInclusive,
		MaxTaskSequenceInclusive: getTasksResp.MaxSequenceInclusive,
	})

	getTasksResp, err = store.GetWorkerTasks(ctx, persistence.GetWorkerTasksRequest{
		ShardId:                persistence.DefaultShardId,
		StartSequenceInclusive: 0,
		PageSize:               0,
	})
	ass.Equal(0, len(getTasksResp.Tasks))

	prep, err = store.PrepareStateExecution(ctx, persistence.PrepareStateExecutionRequest{
		ProcessExecutionId: startResp.ProcessExecutionId,
		StateExecutionId:   stateExeId,
	})
	ass.Nil(err)
	ass.Equal(persistence.StateExecutionStatusCompleted, prep.WaitUntilStatus)
	ass.Equal(persistence.StateExecutionStatusRunning, prep.ExecuteStatus)

	compExeResp, err := store.CompleteExecuteExecution(ctx, persistence.CompleteExecuteExecutionRequest{
		ProcessExecutionId: startResp.ProcessExecutionId,
		StateExecutionId:   stateExeId,
		Prepare:            *prep,
		StateDecision: xdbapi.StateDecision{
			ThreadCloseDecision: &xdbapi.ThreadCloseDecision{
				CloseType: xdbapi.FORCE_COMPLETE_PROCESS.Ptr(),
			},
			// TODO test next states
			// NextStates: []xdbapi.StateMovement{
			//},
		},
		TaskShardId: persistence.DefaultShardId,
	})
	ass.Nil(err)
	ass.False(compExeResp.HasNewWorkerTask)
}
