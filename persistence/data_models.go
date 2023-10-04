package persistence

import (
	"fmt"
	"github.com/xdblab/xdb-apis/goapi/xdbapi"
	"github.com/xdblab/xdb/common/uuid"
)

type (
	StartProcessRequest struct {
		Request        xdbapi.ProcessExecutionStartRequest
		NewTaskShardId int32
	}

	StartProcessResponse struct {
		ProcessExecutionId uuid.UUID
		AlreadyStarted     bool
	}

	DescribeLatestProcessRequest struct {
		Namespace string
		ProcessId string
	}

	DescribeLatestProcessResponse struct {
		Response  *xdbapi.ProcessExecutionDescribeResponse
		NotExists bool
	}

	GetWorkerTasksRequest struct {
		ShardId                int32
		StartSequenceInclusive int64
		PageSize               int32
	}

	GetWorkerTasksResponse struct {
		Tasks []WorkerTask
		// MinSequenceInclusive is the sequence of first task in the order
		MinSequenceInclusive int64
		// MinSequenceInclusive is the sequence of last task in the order
		MaxSequenceInclusive int64
	}

	DeleteWorkerTasksRequest struct {
		ShardId int32

		MinTaskSequenceInclusive int64
		MaxTaskSequenceInclusive int64
	}

	WorkerTask struct {
		ShardId int32
		// TaskSequence represents the increasing order in the queue of the shard
		// It should be empty when inserting, because the persistence/database will
		// generate the value automatically
		TaskSequence *int64

		TaskType WorkerTaskType

		ProcessExecutionId uuid.UUID
		StateExecutionId
	}

	StateExecutionId struct {
		StateId         string
		StateIdSequence int32
	}

	PrepareStateExecutionRequest struct {
		ProcessExecutionId uuid.UUID
		StateExecutionId
	}

	PrepareStateExecutionResponse struct {
		WaitUntilStatus StateExecutionStatus
		ExecuteStatus   StateExecutionStatus
		// PreviousVersion is for conditional check in the future transactional update
		PreviousVersion int32

		Input xdbapi.EncodedObject
		Info  AsyncStateExecutionInfoJson
	}

	CompleteWaitUntilExecutionRequest struct {
		ProcessExecutionId uuid.UUID
		StateExecutionId

		Prepare        PrepareStateExecutionResponse
		CommandRequest xdbapi.CommandRequest
		TaskShardId    int32
	}

	CompleteWaitUntilExecutionResponse struct {
		HasNewWorkerTask bool
	}

	CompleteExecuteExecutionRequest struct {
		ProcessExecutionId uuid.UUID
		StateExecutionId

		Prepare       PrepareStateExecutionResponse
		StateDecision xdbapi.StateDecision
		TaskShardId   int32
	}

	CompleteExecuteExecutionResponse struct {
		HasNewWorkerTask bool
	}
)

func (t WorkerTask) GetTaskSequence() int64 {
	if t.TaskSequence == nil {
		// this shouldn't happen!
		return -1
	}
	return *t.TaskSequence
}

func (t WorkerTask) GetId() string {
	if t.TaskSequence == nil {
		return "<WRONG ID, TaskSequence IS EMPTY>"
	}
	return fmt.Sprintf("%v-%v", t.ShardId, *t.TaskSequence)
}

func (s StateExecutionId) GetId() string {
	return fmt.Sprintf("%v-%v", s.StateId, s.StateIdSequence)
}
