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

	"github.com/xdblab/xdb/common/log/tag"
	"github.com/xdblab/xdb/extensions"
	"github.com/xdblab/xdb/persistence"
)

func (p sqlProcessStoreImpl) ConvertTimerTaskToImmediateTask(
	ctx context.Context, request persistence.ProcessTimerTaskRequest,
) (*persistence.ProcessTimerTaskResponse, error) {
	tx, err := p.session.StartTransaction(ctx, defaultTxOpts)
	if err != nil {
		return nil, err
	}

	err = p.doConvertTimerTaskToImmediateTaskTx(ctx, tx, request)
	if err != nil {
		err2 := tx.Rollback()
		if err2 != nil {
			p.logger.Error("error on rollback transaction", tag.Error(err2))
		}
	} else {
		err = tx.Commit()
		if err != nil {
			p.logger.Error("error on committing transaction", tag.Error(err))
			return nil, err
		}
	}

	return &persistence.ProcessTimerTaskResponse{
		HasNewImmediateTask: true,
	}, err
}

func (p sqlProcessStoreImpl) doConvertTimerTaskToImmediateTaskTx(
	ctx context.Context, tx extensions.SQLTransaction,
	request persistence.ProcessTimerTaskRequest,
) error {
	currentTask := request.Task
	timerInfo := currentTask.TimerTaskInfo
	taskInfoBytes, err := persistence.FromImmediateTaskInfoIntoBytes(persistence.ImmediateTaskInfoJson{
		WorkerTaskBackoffInfo: timerInfo.WorkerTaskBackoffInfo,
	})
	if err != nil {
		return err
	}

	err = tx.InsertImmediateTask(ctx, extensions.ImmediateTaskRowForInsert{
		ShardId:            currentTask.ShardId,
		TaskType:           *timerInfo.WorkerTaskType,
		ProcessExecutionId: currentTask.ProcessExecutionId,
		StateId:            currentTask.StateId,
		StateIdSequence:    currentTask.StateIdSequence,
		Info:               taskInfoBytes,
	})
	if err != nil {
		return err
	}
	return tx.DeleteTimerTask(ctx, extensions.TimerTaskRowDeleteFilter{
		ShardId:              currentTask.ShardId,
		FireTimeUnixSeconds:  currentTask.FireTimestampSeconds,
		TaskSequence:         *currentTask.TaskSequence,
		OptionalPartitionKey: currentTask.OptionalPartitionKey,
	})
}
