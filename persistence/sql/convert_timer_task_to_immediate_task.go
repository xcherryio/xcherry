// Copyright (c) 2023 XDBLab Organization
// SPDX-License-Identifier: BUSL-1.1

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
