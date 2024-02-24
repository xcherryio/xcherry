// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: Apache-2.0

package process

import (
	"context"
	"github.com/xcherryio/xcherry/persistence/data_models"

	"github.com/xcherryio/xcherry/common/ptr"
)

func (p sqlProcessStoreImpl) GetImmediateTasks(
	ctx context.Context, request data_models.GetImmediateTasksRequest,
) (*data_models.GetImmediateTasksResponse, error) {
	immediateTasks, err := p.session.BatchSelectImmediateTasks(
		ctx, request.ShardId, request.StartSequenceInclusive, request.PageSize)
	if err != nil {
		return nil, err
	}
	var tasks []data_models.ImmediateTask
	for _, t := range immediateTasks {
		info, err := data_models.BytesToImmediateTaskInfo(t.Info)
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, data_models.ImmediateTask{
			ShardId:            request.ShardId,
			TaskSequence:       ptr.Any(t.TaskSequence),
			TaskType:           t.TaskType,
			ProcessExecutionId: t.ProcessExecutionId,
			StateExecutionId: data_models.StateExecutionId{
				StateId:         t.StateId,
				StateIdSequence: t.StateIdSequence,
			},
			ImmediateTaskInfo: info,
		})
	}
	resp := &data_models.GetImmediateTasksResponse{
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
