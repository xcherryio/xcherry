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
		IsCurrent                bool
		Status                   string
		HistoryEventIdSequence   int32
		StateExecutionIdSequence types.JSONText
	}

	ProcessExecutionRow struct {
		ProcessExecutionRowForUpdate

		Namespace          string
		ProcessExecutionId persistence.UUID

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
	}

	AsyncStateExecutionRow struct {
		AsyncStateExecutionSelectFilter

		AsyncStateExecutionRowForUpdate

		Input []byte
		Info  types.JSONText
	}

	WorkerTaskRowForInsert struct {
		ShardId  int32
		TaskType WorkerTaskType

		ProcessExecutionId persistence.UUID
		StateId            string
		StateIdSequence    int32
	}

	WorkerTaskRowDeleteFilter struct {
		ShardId      int32
		TaskSequence int64
	}
)
