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
	"github.com/xdblab/xdb/common/ptr"
	"github.com/xdblab/xdb/extensions"
	"github.com/xdblab/xdb/persistence"
	"math"
)

func createGetTimerTaskResponse(
	shardId int32, dbTimerTasks []extensions.TimerTaskRow,
) (*persistence.GetTimerTasksResponse, error) {
	var tasks []persistence.TimerTask
	for _, t := range dbTimerTasks {
		info, err := persistence.BytesToTimerTaskInfo(t.Info)
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, persistence.TimerTask{
			ShardId:              shardId,
			FireTimestampSeconds: t.FireTimeUnixSeconds,
			TaskSequence:         ptr.Any(t.TaskSequence),

			TaskType:           t.TaskType,
			ProcessExecutionId: t.ProcessExecutionId,
			StateExecutionId: persistence.StateExecutionId{
				StateId:         t.StateId,
				StateIdSequence: t.StateIdSequence,
			},
			TimerTaskInfo: info,
		})
	}
	resp := &persistence.GetTimerTasksResponse{
		Tasks: tasks,
	}
	if len(dbTimerTasks) > 0 {
		firstTask := dbTimerTasks[0]
		lastTask := dbTimerTasks[len(dbTimerTasks)-1]
		resp.MinFireTimestampSecondsInclusive = firstTask.FireTimeUnixSeconds
		resp.MaxFireTimestampSecondsInclusive = lastTask.FireTimeUnixSeconds

		resp.MinSequenceInclusive = math.MaxInt64
		resp.MaxSequenceInclusive = math.MinInt64
		for _, t := range dbTimerTasks {
			if t.TaskSequence < resp.MinSequenceInclusive {
				resp.MinSequenceInclusive = t.TaskSequence
			}
			if t.TaskSequence > resp.MaxSequenceInclusive {
				resp.MaxSequenceInclusive = t.TaskSequence
			}
		}
	}
	return resp, nil
}
