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

package persistence

import (
	"fmt"

	"github.com/xdblab/xdb-apis/goapi/xdbapi"
	"github.com/xdblab/xdb/common/uuid"
	"strconv"
	"strings"
)

type (
	StartProcessRequest struct {
		Request        xdbapi.ProcessExecutionStartRequest
		NewTaskShardId int32
	}

	StartProcessResponse struct {
		ProcessExecutionId  uuid.UUID
		AlreadyStarted      bool
		HasNewImmediateTask bool
	}

	StopProcessRequest struct {
		Namespace       string
		ProcessId       string
		ProcessStopType xdbapi.ProcessExecutionStopType
	}

	StopProcessResponse struct {
		NotExists bool
	}

	DescribeLatestProcessRequest struct {
		Namespace string
		ProcessId string
	}

	DescribeLatestProcessResponse struct {
		Response  *xdbapi.ProcessExecutionDescribeResponse
		NotExists bool
	}

	MoveProcessToStateRequest struct {
		Namespace              string
		ProcessExecutionId     uuid.UUID
		Prepare                PrepareStateExecutionResponse
		SourceStateExecutionId StateExecutionId
		SourceFailedStateApi   xdbapi.StateApiType
		DestinationStateId     string
		DestinationStateConfig *xdbapi.AsyncStateConfig
		DestinationStateInput  xdbapi.EncodedObject
		ShardId                int32
	}

	GetImmediateTasksRequest struct {
		ShardId                int32
		StartSequenceInclusive int64
		PageSize               int32
	}

	GetImmediateTasksResponse struct {
		Tasks []ImmediateTask
		// MinSequenceInclusive is the sequence of first task in the order
		MinSequenceInclusive int64
		// MinSequenceInclusive is the sequence of last task in the order
		MaxSequenceInclusive int64
	}

	DeleteImmediateTasksRequest struct {
		ShardId int32

		MinTaskSequenceInclusive int64
		MaxTaskSequenceInclusive int64
	}

	BackoffImmediateTaskRequest struct {
		LastFailureStatus    int32
		LastFailureDetails   string
		Prep                 PrepareStateExecutionResponse
		Task                 ImmediateTask
		FireTimestampSeconds int64
	}

	ConvertTimerTaskToImmediateTaskRequest struct {
		Task TimerTask
	}

	ImmediateTask struct {
		ShardId int32
		// TaskSequence represents the increasing order in the queue of the shard
		// It should be empty when inserting, because the persistence/database will
		// generate the value automatically
		TaskSequence *int64

		TaskType ImmediateTaskType

		ProcessExecutionId uuid.UUID
		StateExecutionId
		ImmediateTaskInfo ImmediateTaskInfoJson

		// only needed for distributed database that doesn't support global secondary index
		OptionalPartitionKey *PartitionKey
	}

	GetTimerTasksRequest struct {
		ShardId                          int32
		MaxFireTimestampSecondsInclusive int64
		PageSize                         int32
	}

	GetTimerTasksResponse struct {
		Tasks                            []TimerTask
		MinFireTimestampSecondsInclusive int64
		// MinSequenceInclusive is the sequence of first task in the order
		MinSequenceInclusive             int64
		MaxFireTimestampSecondsInclusive int64
		// MinSequenceInclusive is the sequence of last task in the order
		MaxSequenceInclusive int64
		// indicates if the response is full page or not
		// only applicable for request with pageSize
		FullPage bool
	}

	GetTimerTasksForTimestampsRequest struct {
		// ShardId is the shardId in all DetailedRequests
		// just for convenience using xdbapi.NotifyTimerTasksRequest which also has
		// the ShardId field, but the caller will ensure the ShardId is the same in all
		ShardId int32
		// MinSequenceInclusive is the minimum sequence required for the timer tasks to load
		// because the tasks with smaller sequence are already loaded
		MinSequenceInclusive int64
		// DetailedRequests is the list of NotifyTimerTasksRequest
		// which contains the fire timestamps and other info of all timer tasks to pull
		DetailedRequests []xdbapi.NotifyTimerTasksRequest
	}

	DeleteTimerTasksRequest struct {
		ShardId int32

		MinFireTimestampSecondsInclusive int64
		MinTaskSequenceInclusive         int64

		MaxFireTimestampSecondsInclusive int64
		MaxTaskSequenceInclusive         int64
	}

	TimerTask struct {
		ShardId              int32
		FireTimestampSeconds int64
		// TaskSequence represents the increasing order in the queue of the shard
		// It should be empty when inserting, because the persistence/database will
		// generate the value automatically
		TaskSequence *int64

		TaskType TimerTaskType

		ProcessExecutionId uuid.UUID
		StateExecutionId
		TimerTaskInfo TimerTaskInfoJson

		// only needed for distributed database that doesn't support global secondary index
		OptionalPartitionKey *PartitionKey
	}

	PartitionKey struct {
		Namespace string
		ProcessId string
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
		Status                  StateExecutionStatus
		WaitUntilCommandResults xdbapi.CommandResults

		// PreviousVersion is for conditional check in the future transactional update
		PreviousVersion int32

		Input       xdbapi.EncodedObject
		Info        AsyncStateExecutionInfoJson
		LastFailure *StateExecutionFailureJson
	}

	ProcessWaitUntilExecutionRequest struct {
		ProcessExecutionId uuid.UUID
		StateExecutionId

		Prepare             PrepareStateExecutionResponse
		CommandRequest      xdbapi.CommandRequest
		PublishToLocalQueue []xdbapi.LocalQueueMessage
		TaskShardId         int32
	}

	ProcessWaitUntilExecutionResponse struct {
		HasNewImmediateTask bool
	}

	CompleteWaitUntilExecutionRequest struct {
		TaskShardId        int32
		ProcessExecutionId uuid.UUID
		StateExecutionId
		PreviousVersion int32
	}

	CompleteExecuteExecutionRequest struct {
		ProcessExecutionId uuid.UUID
		StateExecutionId

		Prepare             PrepareStateExecutionResponse
		StateDecision       xdbapi.StateDecision
		PublishToLocalQueue []xdbapi.LocalQueueMessage
		TaskShardId         int32
	}

	CompleteExecuteExecutionResponse struct {
		HasNewImmediateTask bool
	}

	PublishToLocalQueueRequest struct {
		Namespace string
		ProcessId string
		Messages  []xdbapi.LocalQueueMessage
	}

	PublishToLocalQueueResponse struct {
		ProcessExecutionId  uuid.UUID
		HasNewImmediateTask bool
		ProcessNotExists    bool
	}

	ProcessLocalQueueMessagesRequest struct {
		TaskShardId int32
		// if TaskSequence is 0, it means this request was sent to consume unconsumed messages for a new state,
		// so there is no specific ImmediateTask associated with this request.
		TaskSequence int64

		ProcessExecutionId uuid.UUID

		// being empty if TaskSequence is 0
		Messages []LocalQueueMessageInfoJson

		// the following fields will only be used when TaskSequence is 0 to determine the new state
		StateExecutionId   StateExecutionId
		CommandWaitingType xdbapi.CommandWaitingType
	}

	ProcessLocalQueueMessagesResponse struct {
		HasNewImmediateTask bool
		ProcessExecutionId  uuid.UUID
	}
)

func (t ImmediateTask) GetTaskSequence() int64 {
	if t.TaskSequence == nil {
		// this shouldn't happen!
		return -1
	}
	return *t.TaskSequence
}

func (t ImmediateTask) GetTaskId() string {
	if t.TaskSequence == nil {
		return "<WRONG ID, TaskSequence IS EMPTY>"
	}
	return fmt.Sprintf("%v-%v", t.ShardId, *t.TaskSequence)
}

func (s StateExecutionId) GetStateExecutionId() string {
	return fmt.Sprintf("%v-%v", s.StateId, s.StateIdSequence)
}

func NewStateExecutionIdFromString(s string) (*StateExecutionId, error) {
	lastHyphenIndex := strings.LastIndex(s, "-")
	if lastHyphenIndex == -1 {
		return nil, fmt.Errorf("invalid format: %s", s)
	}

	stateId := s[:lastHyphenIndex]
	stateIdSequence, err := strconv.ParseInt(s[lastHyphenIndex+1:], 10, 32)
	if err != nil {
		return nil, err
	}

	return &StateExecutionId{StateId: stateId, StateIdSequence: int32(stateIdSequence)}, nil
}
