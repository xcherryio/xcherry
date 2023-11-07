// Copyright (c) XDBLab
// SPDX-License-Identifier: BUSL-1.1

package sql

import (
	"context"

	"github.com/xdblab/xdb/extensions"
	"github.com/xdblab/xdb/persistence"
)

func (p sqlProcessStoreImpl) DeleteImmediateTasks(
	ctx context.Context, request persistence.DeleteImmediateTasksRequest,
) error {
	return p.session.BatchDeleteImmediateTask(ctx, extensions.ImmediateTaskRangeDeleteFilter{
		ShardId:                  request.ShardId,
		MinTaskSequenceInclusive: request.MinTaskSequenceInclusive,
		MaxTaskSequenceInclusive: request.MaxTaskSequenceInclusive,
	})
}
