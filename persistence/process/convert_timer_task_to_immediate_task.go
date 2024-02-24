// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: Apache-2.0

package process

import (
	"context"
	"github.com/xcherryio/xcherry/persistence/data_models"

	"github.com/xcherryio/xcherry/common/log/tag"
	"github.com/xcherryio/xcherry/extensions"
)

func (p sqlProcessStoreImpl) ConvertTimerTaskToImmediateTask(
	ctx context.Context, request data_models.ProcessTimerTaskRequest,
) (*data_models.ProcessTimerTaskResponse, error) {
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

	return &data_models.ProcessTimerTaskResponse{
		HasNewImmediateTask: true,
	}, err
}

func (p sqlProcessStoreImpl) doConvertTimerTaskToImmediateTaskTx(
	ctx context.Context, tx extensions.SQLTransaction,
	request data_models.ProcessTimerTaskRequest,
) error {
	currentTask := request.Task
	timerInfo := currentTask.TimerTaskInfo
	taskInfoBytes, err := data_models.FromImmediateTaskInfoIntoBytes(data_models.ImmediateTaskInfoJson{
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
