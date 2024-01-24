// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: Apache-2.0

package data_models

import "github.com/xcherryio/xcherry/common/uuid"

type TimerTask struct {
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
