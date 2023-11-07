// Copyright (c) XDBLab
// SPDX-License-Identifier: BUSL-1.1

package sql

import (
	"context"

	"github.com/xdblab/xdb/common/ptr"
	"github.com/xdblab/xdb/persistence"
)

func (p sqlProcessStoreImpl) GetImmediateTasks(
	ctx context.Context, request persistence.GetImmediateTasksRequest,
) (*persistence.GetImmediateTasksResponse, error) {
	immediateTasks, err := p.session.BatchSelectImmediateTasks(
		ctx, request.ShardId, request.StartSequenceInclusive, request.PageSize)
	if err != nil {
		return nil, err
	}
	var tasks []persistence.ImmediateTask
	for _, t := range immediateTasks {
		info, err := persistence.BytesToImmediateTaskInfo(t.Info)
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, persistence.ImmediateTask{
			ShardId:            request.ShardId,
			TaskSequence:       ptr.Any(t.TaskSequence),
			TaskType:           t.TaskType,
			ProcessExecutionId: t.ProcessExecutionId,
			StateExecutionId: persistence.StateExecutionId{
				StateId:         t.StateId,
				StateIdSequence: t.StateIdSequence,
			},
			ImmediateTaskInfo: info,
		})
	}
	resp := &persistence.GetImmediateTasksResponse{
		Tasks: tasks,
	}
	if len(immediateTasks) > 0 {
		firstTask := immediateTasks[0]
		lastTask := immediateTasks[len(immediateTasks)-1]
		resp.MinSequenceInclusive = firstTask.TaskSequence
		resp.MaxSequenceInclusive = lastTask.TaskSequence
	}
	return resp, nil
}
