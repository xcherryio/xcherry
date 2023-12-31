// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: Apache-2.0

package data_models

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/xcherryio/apis/goapi/xcapi"
	"github.com/xcherryio/xcherry/common/uuid"
)

type (
	StartProcessRequest struct {
		Request                xcapi.ProcessExecutionStartRequest
		NewTaskShardId         int32
		TimeoutTimeUnixSeconds int64
	}

	StartProcessResponse struct {
		ProcessExecutionId         uuid.UUID
		AlreadyStarted             bool
		HasNewImmediateTask        bool
		FailedAtWritingAppDatabase bool
		AppDatabaseWritingError    error
	}

	StopProcessRequest struct {
		Namespace       string
		ProcessId       string
		ProcessStopType xcapi.ProcessExecutionStopType
	}

	StopProcessResponse struct {
		NotExists bool
	}

	DescribeLatestProcessRequest struct {
		Namespace string
		ProcessId string
	}

	DescribeLatestProcessResponse struct {
		Response  *xcapi.ProcessExecutionDescribeResponse
		NotExists bool
	}

	GetLatestProcessExecutionRequest struct {
		Namespace string
		ProcessId string
	}

	GetLatestProcessExecutionResponse struct {
		NotExists bool

		ProcessExecutionId uuid.UUID
		Status             ProcessExecutionStatus
		StartTimestamp     int64
		AppDatabaseConfig  *InternalAppDatabaseConfig

		// the process type for SDK to look up the process definition class
		ProcessType string
		// the URL for server async service to make callback to worker
		WorkerUrl string
	}

	RecoverFromStateExecutionFailureRequest struct {
		Namespace                    string
		ProcessExecutionId           uuid.UUID
		Prepare                      PrepareStateExecutionResponse
		SourceStateExecutionId       StateExecutionId
		SourceFailedStateApi         xcapi.WorkerApiType
		LastFailureStatus            int32
		LastFailureDetails           string
		LastFailureCompletedAttempts int32
		DestinationStateId           string
		DestinationStateConfig       *xcapi.AsyncStateConfig
		DestinationStateInput        xcapi.EncodedObject
		ShardId                      int32
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

	ProcessTimerTaskRequest struct {
		Task TimerTask
	}

	ProcessTimerTaskResponse struct {
		HasNewImmediateTask bool
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
		// just for convenience using xcapi.NotifyTimerTasksRequest which also has
		// the ShardId field, but the caller will ensure the ShardId is the same in all
		ShardId int32
		// MinSequenceInclusive is the minimum sequence required for the timer tasks to load
		// because the tasks with smaller sequence are already loaded
		MinSequenceInclusive int64
		// DetailedRequests is the list of NotifyTimerTasksRequest
		// which contains the fire timestamps and other info of all timer tasks to pull
		DetailedRequests []xcapi.NotifyTimerTasksRequest
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
		Status StateExecutionStatus
		// only applicable for state execute API
		WaitUntilCommandResults xcapi.CommandResults

		// PreviousVersion is for conditional check in the future transactional update
		PreviousVersion int32

		Input       xcapi.EncodedObject
		Info        AsyncStateExecutionInfoJson
		LastFailure *StateExecutionFailureJson
	}

	ProcessWaitUntilExecutionRequest struct {
		ProcessExecutionId uuid.UUID
		StateExecutionId

		Prepare             PrepareStateExecutionResponse
		CommandRequest      xcapi.CommandRequest
		PublishToLocalQueue []xcapi.LocalQueueMessage
		TaskShardId         int32
		TaskSequence        int64
	}

	ProcessWaitUntilExecutionResponse struct {
		HasNewImmediateTask bool
		FireTimestamps      []int64
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
		StateDecision       xcapi.StateDecision
		PublishToLocalQueue []xcapi.LocalQueueMessage

		AppDatabaseConfig *InternalAppDatabaseConfig
		WriteAppDatabase  *xcapi.AppDatabaseWrite

		UpdateLocalAttributes []xcapi.KeyValue

		TaskShardId  int32
		TaskSequence int64
	}

	CompleteExecuteExecutionResponse struct {
		HasNewImmediateTask        bool
		FailedAtWritingAppDatabase bool
		AppDatabaseWritingError    error
	}

	PublishToLocalQueueRequest struct {
		Namespace string
		ProcessId string
		Messages  []xcapi.LocalQueueMessage
	}

	PublishToLocalQueueResponse struct {
		ProcessExecutionId  uuid.UUID
		HasNewImmediateTask bool
		ProcessNotExists    bool
		ProcessNotRunning   bool
	}

	ProcessLocalQueueMessagesRequest struct {
		TaskShardId        int32
		TaskSequence       int64
		ProcessExecutionId uuid.UUID
		Messages           []LocalQueueMessageInfoJson
	}

	ProcessLocalQueueMessagesResponse struct {
		HasNewImmediateTask bool
	}

	AppDatabaseReadRequest struct {
		AppDatabaseConfig InternalAppDatabaseConfig
		Request           xcapi.AppDatabaseReadRequest
	}

	AppDatabaseReadResponse struct {
		Response xcapi.AppDatabaseReadResponse
	}

	UpdateProcessExecutionForRpcRequest struct {
		Namespace          string
		ProcessId          string
		ProcessType        string
		ProcessExecutionId uuid.UUID

		StateDecision       xcapi.StateDecision
		PublishToLocalQueue []xcapi.LocalQueueMessage

		AppDatabaseConfig *InternalAppDatabaseConfig
		AppDatabaseWrite  *xcapi.AppDatabaseWrite

		WorkerUrl   string
		TaskShardId int32
	}

	UpdateProcessExecutionForRpcResponse struct {
		HasNewImmediateTask      bool
		ProcessNotExists         bool
		FailAtWritingAppDatabase bool
		WritingAppDatabaseError  error
	}

	LoadLocalAttributesRequest struct {
		ProcessExecutionId uuid.UUID
		Request            xcapi.LoadLocalAttributesRequest
	}

	LoadLocalAttributesResponse struct {
		Response xcapi.LoadLocalAttributesResponse
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
