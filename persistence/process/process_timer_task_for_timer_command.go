// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: Apache-2.0

package process

import (
	"context"
	"github.com/xcherryio/xcherry/persistence/data_models"

	"github.com/xcherryio/xcherry/common/log/tag"
	"github.com/xcherryio/xcherry/extensions"
)

func (p sqlProcessStoreImpl) ProcessTimerTaskForTimerCommand(
	ctx context.Context, request data_models.ProcessTimerTaskRequest,
) (*data_models.ProcessTimerTaskResponse, error) {
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

	return resp, err
}

func (p sqlProcessStoreImpl) doProcessTimerTaskForTimerCommandTx(
	ctx context.Context, tx extensions.SQLTransaction, request data_models.ProcessTimerTaskRequest,
) (*data_models.ProcessTimerTaskResponse, error) {
	task := request.Task
	timerCommandIndex := task.TimerTaskInfo.TimerCommandIndex

	// Step 1: get localQueues from the process execution row
	prcRow, err := tx.SelectProcessExecutionForUpdate(ctx, task.ProcessExecutionId)
	if err != nil {
		return nil, err
	}

	localQueues, err := data_models.NewStateExecutionLocalQueuesFromBytes(prcRow.StateExecutionLocalQueues)
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
	if stateRow.Status != data_models.StateExecutionStatusWaitUntilWaiting {
		return &data_models.ProcessTimerTaskResponse{
			HasNewImmediateTask: false,
		}, nil
	}

	stateRow.LastFailure = nil

	commandRequest, err := data_models.BytesToCommandRequest(stateRow.WaitUntilCommands)
	if err != nil {
		return nil, err
	}

	commandResults, err := data_models.BytesToCommandResultsJson(stateRow.WaitUntilCommandResults)
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

	stateRow.WaitUntilCommandResults, err = data_models.FromCommandResultsJsonToBytes(commandResults)
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

	return &data_models.ProcessTimerTaskResponse{
		HasNewImmediateTask: hasNewImmediateTask,
	}, nil
}
