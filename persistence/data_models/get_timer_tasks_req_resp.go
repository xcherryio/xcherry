// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: Apache-2.0

package data_models

type (
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
)
