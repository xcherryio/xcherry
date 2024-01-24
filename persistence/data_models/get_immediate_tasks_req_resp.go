// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: Apache-2.0

package data_models

type (
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
)
