// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: Apache-2.0

package process

import (
	"context"
	"github.com/xcherryio/xcherry/common/log/tag"
	"github.com/xcherryio/xcherry/extensions"
	"github.com/xcherryio/xcherry/persistence/data_models"
)

func (p sqlProcessStoreImpl) PublishToLocalQueue(ctx context.Context, request data_models.PublishToLocalQueueRequest) (
	*data_models.PublishToLocalQueueResponse, error) {
	tx, err := p.session.StartTransaction(ctx, defaultTxOpts)
	if err != nil {
		return nil, err
	}

	resp, err := p.doPublishToLocalQueueTx(ctx, tx, request)

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

func (p sqlProcessStoreImpl) doPublishToLocalQueueTx(
	ctx context.Context, tx extensions.SQLTransaction, request data_models.PublishToLocalQueueRequest,
) (*data_models.PublishToLocalQueueResponse, error) {
	curProcExecRow, err := p.session.SelectLatestProcessExecution(ctx, request.Namespace, request.ProcessId)
	if err != nil {
		if p.session.IsNotFoundError(err) {
			// early stop when there is no such process running
			return &data_models.PublishToLocalQueueResponse{
				ProcessNotExists: true,
			}, nil
		}
		return nil, err
	}

	// check if the process is running

	procExecRow, err := tx.SelectProcessExecutionForUpdate(ctx, curProcExecRow.ProcessExecutionId)
	if err != nil {
		return nil, err
	}

	if procExecRow.Status != data_models.ProcessExecutionStatusRunning {
		return &data_models.PublishToLocalQueueResponse{
			ProcessNotRunning: true,
		}, nil
	}

	hasNewImmediateTask, err := p.publishToLocalQueue(ctx, tx, curProcExecRow.ProcessExecutionId, procExecRow.ShardId, request.Messages)
	if err != nil {
		return nil, err
	}

	return &data_models.PublishToLocalQueueResponse{
		ProcessExecutionId:  curProcExecRow.ProcessExecutionId,
		ShardId:             procExecRow.ShardId,
		HasNewImmediateTask: hasNewImmediateTask,
	}, nil
}
