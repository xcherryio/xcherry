// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: Apache-2.0

package data_models

type DeleteTimerTasksRequest struct {
	ShardId int32

	MinFireTimestampSecondsInclusive int64
	MinTaskSequenceInclusive         int64

	MaxFireTimestampSecondsInclusive int64
	MaxTaskSequenceInclusive         int64
}
