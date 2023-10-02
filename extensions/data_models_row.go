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
		// An extra field for some driver to deal with UUID using plain string, it's always empty in request
		// A db extension must implement the code to read/write from/into this field
		// xdb will not use this for any other logic
		ProcessExecutionIdString string
	}

	ProcessExecutionRowForUpdate struct {
		ProcessExecutionId uuid.UUID
		// An extra field for some driver to deal with UUID using plain string, it's always empty in request
		// A db extension must implement the code to read/write from/into this field
		// xdb will not use this for any other logic
		ProcessExecutionIdString string

		IsCurrent              bool
		Status                 ProcessExecutionStatus
		HistoryEventIdSequence int32
		StateIdSequence        types.JSONText
	}

	ProcessExecutionRow struct {
		ProcessExecutionId uuid.UUID
		// An extra field for some driver to deal with UUID using plain string, it's always empty in request
		// A db extension must implement the code to read/write from/into this field
		// xdb will not use this for any other logic
		ProcessExecutionIdString string

		IsCurrent              bool
		Status                 ProcessExecutionStatus
		HistoryEventIdSequence int32
		StateIdSequence        types.JSONText

		Namespace string

		ProcessId      string
		StartTime      time.Time
		TimeoutSeconds int32
		Info           types.JSONText
	}

	AsyncStateExecutionSelectFilter struct {
		ProcessExecutionId uuid.UUID
		// An extra field for some driver to deal with UUID using plain string, it's always empty in request
		// A db extension must implement the code to read/write from/into this field
		// xdb will not use this for any other logic
		ProcessExecutionIdString string

		StateId         string
		StateIdSequence int32
	}

	AsyncStateExecutionRowForUpdate struct {
		ProcessExecutionId uuid.UUID
		// An extra field for some driver to deal with UUID using plain string, it's always empty in request
		// A db extension must implement the code to read/write from/into this field
		// xdb will not use this for any other logic
		ProcessExecutionIdString string

		StateId         string
		StateIdSequence int32

		WaitUntilStatus StateExecutionStatus
		ExecuteStatus   StateExecutionStatus
		PreviousVersion int32 // for conditional check
	}

	AsyncStateExecutionRow struct {
		ProcessExecutionId uuid.UUID
		// An extra field for some driver to deal with UUID using plain string, it's always empty in request
		// A db extension must implement the code to read/write from/into this field
		// xdb will not use this for any other logic
		ProcessExecutionIdString string

		StateId         string
		StateIdSequence int32

		WaitUntilStatus StateExecutionStatus
		ExecuteStatus   StateExecutionStatus
		PreviousVersion int32 // for conditional check

		Input types.JSONText
		Info  types.JSONText
	}

	WorkerTaskRowForInsert struct {
		ShardId  int32
		TaskType WorkerTaskType

		ProcessExecutionId uuid.UUID
		// An extra field for some driver to deal with UUID using plain string, it's always empty in request
		// A db extension must implement the code to read/write from/into this field
		// xdb will not use this for any other logic
		ProcessExecutionIdString string

		StateId         string
		StateIdSequence int32
	}

	WorkerTaskRow struct {
		ShardId  int32
		TaskType WorkerTaskType

		ProcessExecutionId uuid.UUID
		// An extra field for some driver to deal with UUID using plain string, it's always empty in request
		// A db extension must implement the code to read/write from/into this field
		// xdb will not use this for any other logic
		ProcessExecutionIdString string

		StateId         string
		StateIdSequence int32
		
		TaskSequence int64
	}

	WorkerTaskRangeDeleteFilter struct {
		ShardId                  int32
		MaxTaskSequenceInclusive int64
	}
)
