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

func (p sqlProcessStoreImpl) ProcessTimerTaskForTimerCommand(
	ctx context.Context, request persistence.ProcessTimerTaskRequest,
) (*persistence.ProcessTimerTaskResponse, error) {
	tx, err := p.session.StartTransaction(ctx, defaultTxOpts)
	if err != nil {
		return nil, err
	}

	resp, err := p.doProcessTimerTaskForTimerCommandTx(ctx, tx, request)
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

	return resp, nil
}

func (p sqlProcessStoreImpl) doProcessTimerTaskForTimerCommandTx(
	ctx context.Context, tx extensions.SQLTransaction, request persistence.ProcessTimerTaskRequest,
) (*persistence.ProcessTimerTaskResponse, error) {
	task := request.Task
	timerCommandIndex := task.TimerTaskInfo.TimerCommandIndex

	// Step 1: get localQueues from the process execution row
	prcRow, err := tx.SelectProcessExecutionForUpdate(ctx, task.ProcessExecutionId)
	if err != nil {
		return nil, err
	}

	localQueues, err := persistence.NewStateExecutionLocalQueuesFromBytes(prcRow.StateExecutionLocalQueues)
	if err != nil {
		return nil, err
	}

	// Step 2: update the state execution row
	stateRow, err := tx.SelectAsyncStateExecutionForUpdate(ctx, extensions.AsyncStateExecutionSelectFilter{
		ProcessExecutionId: task.ProcessExecutionId,
		StateId:            task.StateId,
		StateIdSequence:    task.StateIdSequence,
	})
	if err != nil {
		return nil, err
	}

	// early stop if the state is not waiting commands
	if stateRow.Status != persistence.StateExecutionStatusWaitUntilWaiting {
		return &persistence.ProcessTimerTaskResponse{
			HasNewImmediateTask: false,
		}, nil
	}

	stateRow.LastFailure = nil

	commandRequest, err := persistence.BytesToCommandRequest(stateRow.WaitUntilCommands)
	if err != nil {
		return nil, err
	}

	commandResults, err := persistence.BytesToCommandResultsJson(stateRow.WaitUntilCommandResults)
	if err != nil {
		return nil, err
	}

	p.updateCommandResultsWithFiredTimerCommand(&commandResults, timerCommandIndex)

	hasNewImmediateTask := false

	if p.hasCompletedWaitUntilWaiting(commandRequest, commandResults) {
		hasNewImmediateTask = true

		err = p.updateWhenCompletedWaitUntilWaiting(ctx, tx, task.ShardId, &localQueues, stateRow)
		if err != nil {
			return nil, err
		}
	}

	stateRow.WaitUntilCommandResults, err = persistence.FromCommandResultsJsonToBytes(commandResults)
	if err != nil {
		return nil, err
	}

	err = tx.UpdateAsyncStateExecution(ctx, *stateRow)
	if err != nil {
		return nil, err
	}

	// Step 3: update process execution row, and submit
	prcRow.StateExecutionLocalQueues, err = localQueues.ToBytes()
	if err != nil {
		return nil, err
	}

	err = tx.UpdateProcessExecution(ctx, *prcRow)
	if err != nil {
		return nil, err
	}

	// step 4: delete timer task
	err = tx.DeleteTimerTask(ctx, extensions.TimerTaskRowDeleteFilter{
		ShardId:              task.ShardId,
		FireTimeUnixSeconds:  task.FireTimestampSeconds,
		TaskSequence:         *task.TaskSequence,
		OptionalPartitionKey: task.OptionalPartitionKey,
	})
	if err != nil {
		return nil, err
	}

	return &persistence.ProcessTimerTaskResponse{
		HasNewImmediateTask: hasNewImmediateTask,
	}, nil
}
