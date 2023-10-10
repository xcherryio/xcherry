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

package extensions

import (
	"github.com/jmoiron/sqlx/types"
	"github.com/xdblab/xdb/common/uuid"
	"github.com/xdblab/xdb/persistence"
	"time"
)

type (
	CurrentProcessExecutionRow struct {
		Namespace          string
		ProcessId          string
		ProcessExecutionId uuid.UUID
		// An extra field for some driver to deal with UUID using plain string, it's always empty in request
		// A db extension must implement the code to read/write from/into this field
		// xdb persistence layer will not use this for any other logic
		ProcessExecutionIdString string
	}

	ProcessExecutionRowForUpdate struct {
		ProcessExecutionId uuid.UUID
		// An extra field for some driver to deal with UUID using plain string, it's always empty in request
		// A db extension must implement the code to read/write from/into this field
		// xdb persistence layer will not use this for any other logic
		ProcessExecutionIdString string

		IsCurrent                  bool
		Status                     persistence.ProcessExecutionStatus
		HistoryEventIdSequence     int32
		StateExecutionSequenceMaps types.JSONText
	}

	ProcessExecutionRow struct {
		ProcessExecutionId uuid.UUID
		// An extra field for some driver to deal with UUID using plain string, it's always empty in request
		// A db extension must implement the code to read/write from/into this field
		// xdb persistence layer will not use this for any other logic
		ProcessExecutionIdString string

		IsCurrent                  bool
		Status                     persistence.ProcessExecutionStatus
		HistoryEventIdSequence     int32
		StateExecutionSequenceMaps types.JSONText

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
		// xdb persistence layer will not use this for any other logic
		ProcessExecutionIdString string

		StateId         string
		StateIdSequence int32
	}

	AsyncStateExecutionRowForUpdate struct {
		ProcessExecutionId uuid.UUID
		// An extra field for some driver to deal with UUID using plain string, it's always empty in request
		// A db extension must implement the code to read/write from/into this field
		// xdb persistence layer will not use this for any other logic
		ProcessExecutionIdString string

		StateId         string
		StateIdSequence int32

		WaitUntilStatus persistence.StateExecutionStatus
		ExecuteStatus   persistence.StateExecutionStatus
		PreviousVersion int32 // for conditional check
	}

	AsyncStateExecutionRow struct {
		ProcessExecutionId uuid.UUID
		// An extra field for some driver to deal with UUID using plain string, it's always empty in request
		// A db extension must implement the code to read/write from/into this field
		// xdb persistence layer will not use this for any other logic
		ProcessExecutionIdString string

		StateId         string
		StateIdSequence int32

		WaitUntilStatus persistence.StateExecutionStatus
		ExecuteStatus   persistence.StateExecutionStatus
		PreviousVersion int32 // for conditional check

		Input types.JSONText
		Info  types.JSONText
	}

	WorkerTaskRowForInsert struct {
		ShardId  int32
		TaskType persistence.WorkerTaskType

		ProcessExecutionId uuid.UUID
		// An extra field for some driver to deal with UUID using plain string, it's always empty in request
		// A db extension must implement the code to read/write from/into this field
		// xdb persistence layer will not use this for any other logic
		ProcessExecutionIdString string

		StateId         string
		StateIdSequence int32
	}

	WorkerTaskRow struct {
		ShardId  int32
		TaskType persistence.WorkerTaskType

		ProcessExecutionId uuid.UUID
		// An extra field for some driver to deal with UUID using plain string, it's always empty in request
		// A db extension must implement the code to read/write from/into this field
		// xdb persistence layer will not use this for any other logic
		ProcessExecutionIdString string

		StateId         string
		StateIdSequence int32

		TaskSequence int64
	}

	WorkerTaskRangeDeleteFilter struct {
		ShardId int32

		MinTaskSequenceInclusive int64
		MaxTaskSequenceInclusive int64
	}
)
