// Copyright (c) XDBLab
// SPDX-License-Identifier: BUSL-1.1

package sql

import (
	"context"
	"fmt"

	"github.com/xdblab/xdb/common/log/tag"
	"github.com/xdblab/xdb/extensions"
	"github.com/xdblab/xdb/persistence"
)

func (p sqlProcessStoreImpl) BackoffImmediateTask(
	ctx context.Context, request persistence.BackoffImmediateTaskRequest,
) error {
	tx, err := p.session.StartTransaction(ctx, defaultTxOpts)
	if err != nil {
		return err
	}

	err = p.doBackoffImmediateTaskTx(ctx, tx, request)
	if err != nil {
		err2 := tx.Rollback()
		if err2 != nil {
			p.logger.Error("error on rollback transaction", tag.Error(err2))
		}
	} else {
		err = tx.Commit()
		if err != nil {
			p.logger.Error("error on committing transaction", tag.Error(err))
			return err
		}
	}
	return err
}

func (p sqlProcessStoreImpl) doBackoffImmediateTaskTx(
	ctx context.Context, tx extensions.SQLTransaction, request persistence.BackoffImmediateTaskRequest,
) error {
	task := request.Task
	prep := request.Prep

	if task.ImmediateTaskInfo.WorkerTaskBackoffInfo == nil {
		return fmt.Errorf("WorkerTaskBackoffInfo cannot be nil")
	}
	failureBytes, err := persistence.CreateStateExecutionFailureBytesForBackoff(
		request.LastFailureStatus, request.LastFailureDetails, task.ImmediateTaskInfo.WorkerTaskBackoffInfo.CompletedAttempts)

	if err != nil {
		return err
	}
	err = tx.UpdateAsyncStateExecutionWithoutCommands(ctx, extensions.AsyncStateExecutionRowForUpdateWithoutCommands{
		ProcessExecutionId: task.ProcessExecutionId,
		StateId:            task.StateId,
		StateIdSequence:    task.StateIdSequence,
		Status:             prep.Status,
		PreviousVersion:    prep.PreviousVersion,
		LastFailure:        failureBytes,
	})
	if err != nil {
		return err
	}
	timerInfoBytes, err := persistence.CreateTimerTaskInfoBytes(task.ImmediateTaskInfo.WorkerTaskBackoffInfo, &task.TaskType)
	if err != nil {
		return err
	}
	err = tx.InsertTimerTask(ctx, extensions.TimerTaskRowForInsert{
		ShardId:             task.ShardId,
		FireTimeUnixSeconds: request.FireTimestampSeconds,
		TaskType:            persistence.TimerTaskTypeWorkerTaskBackoff,
		ProcessExecutionId:  task.ProcessExecutionId,
		StateId:             task.StateId,
		StateIdSequence:     task.StateIdSequence,
		Info:                timerInfoBytes,
	})
	if err != nil {
		return err
	}
	return tx.DeleteImmediateTask(ctx, extensions.ImmediateTaskRowDeleteFilter{
		ShardId:      task.ShardId,
		TaskSequence: task.GetTaskSequence(),
		OptionalPartitionKey: &persistence.PartitionKey{
			Namespace: prep.Info.Namespace,
			ProcessId: prep.Info.ProcessId,
		},
	})
}
