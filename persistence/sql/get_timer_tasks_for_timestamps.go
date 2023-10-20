// Copyright 2023 XDBLab organization
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sql

import (
	"context"

	"github.com/xdblab/xdb/extensions"
	"github.com/xdblab/xdb/persistence"
)

func (p sqlProcessStoreImpl) GetTimerTasksForTimestamps(
	ctx context.Context, request persistence.GetTimerTasksForTimestampsRequest,
) (*persistence.GetTimerTasksResponse, error) {
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
