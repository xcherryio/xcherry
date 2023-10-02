package extensions

import (
	"github.com/jmoiron/sqlx/types"
	"github.com/xdblab/xdb/persistence/uuid"
	"time"
)

type (
	CurrentProcessExecutionRow struct {
		Namespace          string
		ProcessId          string
		ProcessExecutionId uuid.UUID
	}

	ProcessExecutionRowForUpdate struct {
		ProcessExecutionId uuid.UUID

		IsCurrent              bool
		Status                 ProcessExecutionStatus
		HistoryEventIdSequence int32
		StateIdSequence        types.JSONText
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
		ProcessExecutionId uuid.UUID
		StateId            string
		StateIdSequence    int32
	}

	AsyncStateExecutionRowForUpdate struct {
		AsyncStateExecutionSelectFilter

		WaitUntilStatus StateExecutionStatus
		ExecuteStatus   StateExecutionStatus
		PreviousVersion int32 // for conditional check
	}

	AsyncStateExecutionRow struct {
		AsyncStateExecutionRowForUpdate

		Input types.JSONText
		Info  types.JSONText
	}

	WorkerTaskRowForInsert struct {
		ShardId  int32
		TaskType WorkerTaskType

		ProcessExecutionId uuid.UUID
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
