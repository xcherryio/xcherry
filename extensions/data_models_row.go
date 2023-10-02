package extensions

import (
	"github.com/jmoiron/sqlx/types"
	"github.com/xdblab/xdb/persistence"
	"time"
)

type (
	CurrentProcessExecutionRow struct {
		Namespace          string
		ProcessId          string
		ProcessExecutionId persistence.UUID
	}

	ProcessExecutionRowForUpdate struct {
		ProcessExecutionId persistence.UUID

		IsCurrent                bool
		Status                   ProcessExecutionStatus
		HistoryEventIdSequence   int32
		StateExecutionIdSequence types.JSONText
	}

	ProcessExecutionRow struct {
		ProcessExecutionRowForUpdate

		Namespace string

		ProcessId      string
		StartTime      time.Time
		TimeoutSeconds int32
		Info           types.JSONText
	}

	AsyncStateExecutionSelectFilter struct {
		ProcessExecutionId persistence.UUID
		StateId            string
		StateIdSequence    int32
	}

	AsyncStateExecutionRowForUpdate struct {
		AsyncStateExecutionSelectFilter

		WaitUntilStatus StateExecutionStatus
		ExecuteStatus   StateExecutionStatus
		Version         int32 // for conditional check
	}

	AsyncStateExecutionRow struct {
		AsyncStateExecutionRowForUpdate

		Input types.JSONText
		Info  types.JSONText
	}

	WorkerTaskRowForInsert struct {
		ShardId  int32
		TaskType WorkerTaskType

		ProcessExecutionId persistence.UUID
		StateId            string
		StateIdSequence    int32
	}

	WorkerTaskRow struct {
		WorkerTaskRowForInsert
		TaskSequence int64
	}

	WorkerTaskRangeDeleteFilter struct {
		ShardId                  int32
		MaxTaskSequenceInclusive int64
	}
)
