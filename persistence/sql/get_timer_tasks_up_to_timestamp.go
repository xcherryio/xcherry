// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: BUSL-1.1

package sql

import (
	"context"
	"github.com/xcherryio/xcherry/persistence/data_models"

	"github.com/xcherryio/xcherry/extensions"
)

func (p sqlProcessStoreImpl) GetTimerTasksUpToTimestamp(
	ctx context.Context, request data_models.GetTimerTasksRequest,
) (*data_models.GetTimerTasksResponse, error) {
	dbTimerTasks, err := p.session.BatchSelectTimerTasks(
		ctx, extensions.TimerTaskRangeSelectFilter{
			ShardId:                         request.ShardId,
			MaxFireTimeUnixSecondsInclusive: request.MaxFireTimestampSecondsInclusive,
			PageSize:                        request.PageSize,
		})
	if err != nil {
		return nil, err
	}
	return createGetTimerTaskResponse(request.ShardId, dbTimerTasks, &request.PageSize)
}
