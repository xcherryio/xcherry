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
	"time"

	"github.com/jmoiron/sqlx/types"
	"github.com/xdblab/xdb/common/uuid"
	"github.com/xdblab/xdb/persistence"
)

/**
* Why we need ProcessExecutionIdString field, in addition to ProcessExecutionId?
* Because different database driver has different way to deal with UUID.
* In some database like MySQL/MariaDB/Oracle, UUID is stored as binary(16) type, which is not human readable directly.
* Therefore, they provide some helper functions to convert UUID to/from string. But the queries to read/write UUID are still byte array.
* Some databases like Postgres, provide UUID type out of the box, the queries to read/write UUID are string.
* For the first type of database, the extension implementation can use the UUID form of ProcessExecutionId, which has implemented the Scan/Value interface.
* For the second type of database, the extension implementation can use the string form of ProcessExecutionId, which is the ProcessExecutionIdString field.
* Having this two fields available so that the extension implementation doesn't need to create a new struct and copy/convert the fields.
*
* Note that this field is a "helper" field, meaning that the caller of the interface(the persistence/ layer of this repo) will not read or write this field.
* The extension implementation is responsible to read/write this field. For example, before writing into database, Postgres extension will write the field
* by converting UUID to string. After reading from database, Postgres extension will read into the string field, then converting it to the UUID field.
 */

type (
	LatestProcessExecutionRow struct {
		Namespace          string
		ProcessId          string
		ProcessExecutionId uuid.UUID
		// See the top of the file for why we need this field
		ProcessExecutionIdString string
	}

	ProcessExecutionRowForUpdate struct {
		ProcessExecutionId uuid.UUID
		// See the top of the file for why we need this field
		ProcessExecutionIdString string

		Status                 persistence.ProcessExecutionStatus
		HistoryEventIdSequence int32

		StateExecutionSequenceMaps  types.JSONText
		StateExecutionWaitingQueues types.JSONText

		WaitToComplete bool
	}

	ProcessExecutionRow struct {
		ProcessExecutionId uuid.UUID
		// See the top of the file for why we need this field
		ProcessExecutionIdString string

		Status                 persistence.ProcessExecutionStatus
		HistoryEventIdSequence int32

		StateExecutionSequenceMaps  types.JSONText
		StateExecutionWaitingQueues types.JSONText

		Namespace string

		ProcessId      string
		StartTime      time.Time
		TimeoutSeconds int32
		Info           types.JSONText
	}

	AsyncStateExecutionSelectFilter struct {
		ProcessExecutionId uuid.UUID
		// See the top of the file for why we need this field
		ProcessExecutionIdString string

		StateId         string
		StateIdSequence int32
	}

	AsyncStateExecutionRowForUpdate struct {
		ProcessExecutionId uuid.UUID
		// See the top of the file for why we need this field
		ProcessExecutionIdString string

		StateId         string
		StateIdSequence int32

		Status persistence.StateExecutionStatus

		WaitUntilCommands       types.JSONText
		WaitUntilCommandResults types.JSONText

		LastFailure types.JSONText

		PreviousVersion int32 // for conditional check
	}

	AsyncStateExecutionRowForUpdateWithoutCommands struct {
		ProcessExecutionId uuid.UUID
		// See the top of the file for why we need this field
		ProcessExecutionIdString string

		StateId         string
		StateIdSequence int32

		Status persistence.StateExecutionStatus

		LastFailure types.JSONText

		PreviousVersion int32 // for conditional check
	}

	AsyncStateExecutionRow struct {
		ProcessExecutionId uuid.UUID
		// See the top of the file for why we need this field
		ProcessExecutionIdString string
		StateId                  string
		StateIdSequence          int32

		Status persistence.StateExecutionStatus

		WaitUntilCommandResults types.JSONText

		PreviousVersion int32 // for conditional check

		LastFailure types.JSONText

		Input types.JSONText
		Info  types.JSONText
	}

	ImmediateTaskRowForInsert struct {
		ShardId  int32
		TaskType persistence.ImmediateTaskType

		ProcessExecutionId uuid.UUID
		// See the top of the file for why we need this field
		ProcessExecutionIdString string
		// StateId and StateIdSequence will be "" and 0 when TaskType is persistence.ImmediateTaskTypeNewLocalQueueMessages
		StateId         string
		StateIdSequence int32

		Info types.JSONText
	}

	ImmediateTaskRow struct {
		ShardId      int32
		TaskSequence int64

		TaskType persistence.ImmediateTaskType

		ProcessExecutionId uuid.UUID
		// See the top of the file for why we need this field
		ProcessExecutionIdString string
		StateId                  string
		StateIdSequence          int32

		Info types.JSONText
	}

	ImmediateTaskRowDeleteFilter struct {
		ShardId      int32
		TaskSequence int64

		OptionalPartitionKey *persistence.PartitionKey
	}

	ImmediateTaskRangeDeleteFilter struct {
		ShardId int32

		MinTaskSequenceInclusive int64
		MaxTaskSequenceInclusive int64
	}

	TimerTaskRowForInsert struct {
		ShardId             int32
		FireTimeUnixSeconds int64
		TaskType            persistence.TimerTaskType

		ProcessExecutionId uuid.UUID
		// See the top of the file for why we need this field
		ProcessExecutionIdString string
		StateId                  string
		StateIdSequence          int32

		Info types.JSONText
	}

	TimerTaskRow struct {
		ShardId             int32
		FireTimeUnixSeconds int64
		TaskSequence        int64

		TaskType persistence.TimerTaskType

		ProcessExecutionId uuid.UUID
		// See the top of the file for why we need this field
		ProcessExecutionIdString string
		StateId                  string
		StateIdSequence          int32

		Info types.JSONText
	}

	TimerTaskRowDeleteFilter struct {
		ShardId             int32
		FireTimeUnixSeconds int64
		TaskSequence        int64

		OptionalPartitionKey *persistence.PartitionKey
	}

	TimerTaskRangeSelectFilter struct {
		ShardId int32

		MaxFireTimeUnixSecondsInclusive int64
		PageSize                        int32
	}

	TimerTaskSelectByTimestampsFilter struct {
		ShardId int32

		FireTimeUnixSeconds      []int64
		MinTaskSequenceInclusive int64
	}

	LocalQueueMessageRow struct {
		ProcessExecutionId uuid.UUID
		// See the top of the file for why we need this field
		ProcessExecutionIdString string

		QueueName string

		DedupId uuid.UUID
		// See the top of the file for why we need this field
		DedupIdString string

		Payload types.JSONText
	}
)
