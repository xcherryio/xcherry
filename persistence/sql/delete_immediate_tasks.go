// Copyright (c) 2023 XDBLab Organization
// SPDX-License-Identifier: BUSL-1.1

package sql

import (
	"context"
	"github.com/xdblab/xdb/persistence/data_models"

	"github.com/xdblab/xdb/extensions"
)

func (p sqlProcessStoreImpl) DeleteImmediateTasks(
	ctx context.Context, request data_models.DeleteImmediateTasksRequest,
) error {
	return p.session.BatchDeleteImmediateTask(ctx, extensions.ImmediateTaskRangeDeleteFilter{
		ShardId:                  request.ShardId,
		MinTaskSequenceInclusive: request.MinTaskSequenceInclusive,
		MaxTaskSequenceInclusive: request.MaxTaskSequenceInclusive,
	})
}
