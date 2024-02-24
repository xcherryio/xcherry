// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: Apache-2.0

package process

import (
	"context"
	"github.com/xcherryio/xcherry/persistence/data_models"

	"github.com/xcherryio/xcherry/extensions"
)

func (p sqlProcessStoreImpl) GetTimerTasksForTimestamps(
	ctx context.Context, request data_models.GetTimerTasksForTimestampsRequest,
) (*data_models.GetTimerTasksResponse, error) {
	var ts []int64
	for _, req := range request.DetailedRequests {
		ts = append(ts, req.FireTimestamps...)
	}
	dbTimerTasks, err := p.session.SelectTimerTasksForTimestamps(
		ctx, extensions.TimerTaskSelectByTimestampsFilter{
			ShardId:                  request.ShardId,
			FireTimeUnixSeconds:      ts,
			MinTaskSequenceInclusive: request.MinSequenceInclusive,
		})
	if err != nil {
		return nil, err
	}
	return createGetTimerTaskResponse(request.ShardId, dbTimerTasks, nil)
}
