// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: Apache-2.0

package data_models

import (
	"fmt"
	"github.com/xcherryio/xcherry/common/uuid"
)

type ImmediateTask struct {
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
