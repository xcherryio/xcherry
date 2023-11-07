// Copyright (c) XDBLab
// SPDX-License-Identifier: BUSL-1.1

package sql

import "context"

func (p sqlProcessStoreImpl) CleanUpTasksForTest(ctx context.Context, shardId int32) error {
	return p.session.CleanUpTasksForTest(ctx, shardId)
}
