// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: Apache-2.0

package process

import "context"

func (p sqlProcessStoreImpl) CleanUpTasksForTest(ctx context.Context, shardId int32) error {
	return p.session.CleanUpTasksForTest(ctx, shardId)
}
