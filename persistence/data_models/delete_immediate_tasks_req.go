// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: Apache-2.0

package data_models

type DeleteImmediateTasksRequest struct {
	ShardId int32

	MinTaskSequenceInclusive int64
	MaxTaskSequenceInclusive int64
}
