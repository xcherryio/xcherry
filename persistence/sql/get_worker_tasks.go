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

	"github.com/xdblab/xdb/common/ptr"
	"github.com/xdblab/xdb/persistence"
)

func (p sqlProcessStoreImpl) GetWorkerTasks(
	ctx context.Context, request persistence.GetWorkerTasksRequest,
) (*persistence.GetWorkerTasksResponse, error) {
	workerTasks, err := p.session.BatchSelectWorkerTasks(
		ctx, request.ShardId, request.StartSequenceInclusive, request.PageSize)
	if err != nil {
		return nil, err
	}
	var tasks []persistence.WorkerTask
	for _, t := range workerTasks {
		info, err := persistence.BytesToWorkerTaskInfo(t.Info)
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, persistence.WorkerTask{
			ShardId:            request.ShardId,
			TaskSequence:       ptr.Any(t.TaskSequence),
			TaskType:           t.TaskType,
			ProcessExecutionId: t.ProcessExecutionId,
			StateExecutionId: persistence.StateExecutionId{
				StateId:         t.StateId,
				StateIdSequence: t.StateIdSequence,
			},
			WorkerTaskInfo: info,
		})
	}
	resp := &persistence.GetWorkerTasksResponse{
		Tasks: tasks,
	}
	if len(workerTasks) > 0 {
		firstTask := workerTasks[0]
		lastTask := workerTasks[len(workerTasks)-1]
		resp.MinSequenceInclusive = firstTask.TaskSequence
		resp.MaxSequenceInclusive = lastTask.TaskSequence
	}
	return resp, nil
}
