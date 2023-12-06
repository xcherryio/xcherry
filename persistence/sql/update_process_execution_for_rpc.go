// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: BUSL-1.1

package sql

import (
	"context"
	"github.com/xcherryio/xcherry/common/log/tag"
	"github.com/xcherryio/xcherry/extensions"
	"github.com/xcherryio/xcherry/persistence/data_models"
)

func (p sqlProcessStoreImpl) UpdateProcessExecutionForRpc(ctx context.Context, request data_models.UpdateProcessExecutionForRpcRequest) (
	*data_models.UpdateProcessExecutionForRpcResponse, error) {
	tx, err := p.session.StartTransaction(ctx, defaultTxOpts)
	if err != nil {
		return nil, err
	}

	resp, err := p.doUpdateProcessExecutionForRpcTx(ctx, tx, request)

	if err != nil || resp.FailAtWritingAppDatabase {
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

func (p sqlProcessStoreImpl) doUpdateProcessExecutionForRpcTx(
	ctx context.Context, tx extensions.SQLTransaction, request data_models.UpdateProcessExecutionForRpcRequest,
) (*data_models.UpdateProcessExecutionForRpcResponse, error) {
	hasNewImmediateTask := false

	// lock process execution row first
	prcRow, err := tx.SelectProcessExecutionForUpdate(ctx, request.ProcessExecutionId)
	if err != nil {
		return nil, err
	}

	// skip the writing operations on a closed process
	if prcRow.Status != data_models.ProcessExecutionStatusRunning {
		return &data_models.UpdateProcessExecutionForRpcResponse{
			ProcessNotExists: true,
		}, nil
	}

	// Step 1: update persistence

	err = p.writeToAppDatabaseIfNeeded(ctx, tx, request.GlobalAttributeTableConfig, request.AppDatabaseWrite)
	if err != nil {
		//lint:ignore nilerr reason
		return &data_models.UpdateProcessExecutionForRpcResponse{
			FailAtWritingAppDatabase: true,
			WritingAppDatabaseError:  err,
		}, nil
	}

	// Step 2: handle state decision

	sequenceMaps, err := data_models.NewStateExecutionSequenceMapsFromBytes(prcRow.StateExecutionSequenceMaps)
	if err != nil {
		return nil, err
	}

	resp, err := p.handleStateDecision(ctx, tx, HandleStateDecisionRequest{
		Namespace:          request.Namespace,
		ProcessId:          request.ProcessId,
		ProcessType:        request.ProcessType,
		ProcessExecutionId: request.ProcessExecutionId,
		StateDecision:      request.StateDecision,
		AppDatabaseConfig:  request.GlobalAttributeTableConfig,
		WorkerUrl:          request.WorkerUrl,

		ProcessExecutionRowStateExecutionSequenceMaps: &sequenceMaps,
		ProcessExecutionRowWaitToComplete:             prcRow.WaitToComplete,
		ProcessExecutionRowStatus:                     prcRow.Status,

		TaskShardId: request.TaskShardId,
	})
	if err != nil {
		return nil, err
	}
	if resp.HasNewImmediateTask {
		hasNewImmediateTask = true
	}

	prcRow.WaitToComplete = resp.ProcessExecutionRowNewWaitToComplete
	prcRow.Status = resp.ProcessExecutionRowNewStatus
	prcRow.StateExecutionSequenceMaps, err = resp.ProcessExecutionRowNewStateExecutionSequenceMaps.ToBytes()
	if err != nil {
		return nil, err
	}

	err = tx.UpdateProcessExecution(ctx, *prcRow)
	if err != nil {
		return nil, err
	}

	// Step 3: publish to local queue

	hasNewImmediateTask2, err := p.publishToLocalQueue(ctx, tx, request.ProcessExecutionId, request.PublishToLocalQueue)
	if err != nil {
		return nil, err
	}
	if hasNewImmediateTask2 {
		hasNewImmediateTask = true
	}

	return &data_models.UpdateProcessExecutionForRpcResponse{
		HasNewImmediateTask: hasNewImmediateTask,
	}, nil
}
