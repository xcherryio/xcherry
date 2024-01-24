// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: Apache-2.0

package data_models

import "github.com/xcherryio/apis/goapi/xcapi"

type GetTimerTasksForTimestampsRequest struct {
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
