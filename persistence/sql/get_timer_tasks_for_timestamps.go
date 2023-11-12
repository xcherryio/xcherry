// Copyright (c) 2023 XDBLab Organization
// SPDX-License-Identifier: BUSL-1.1

package sql

import (
	"context"
	"github.com/xdblab/xdb/persistence/data_models"

	"github.com/xdblab/xdb/extensions"
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
