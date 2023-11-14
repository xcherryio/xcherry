// Copyright (c) 2023 XDBLab Organization
// SPDX-License-Identifier: BUSL-1.1

package sql

import (
	"context"
	"fmt"

	"github.com/xdblab/xdb/common/log/tag"
	"github.com/xdblab/xdb/extensions"
	"github.com/xdblab/xdb/persistence/data_models"
)

func (p sqlProcessStoreImpl) UpdateProcessExecutionFromRpc(ctx context.Context, request data_models.UpdateProcessExecutionFromRpcRequest) (
	*data_models.UpdateProcessExecutionFromRpcResponse, error) {
	tx, err := p.session.StartTransaction(ctx, defaultTxOpts)
	if err != nil {
		return nil, err
	}

	resp, err := p.doUpdateProcessExecutionFromRpcTx(ctx, tx, request)

	if err != nil || resp.FailAtUpdatingGlobalAttributes {
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

func (p sqlProcessStoreImpl) doUpdateProcessExecutionFromRpcTx(
	ctx context.Context, tx extensions.SQLTransaction, request data_models.UpdateProcessExecutionFromRpcRequest,
) (*data_models.UpdateProcessExecutionFromRpcResponse, error) {
	hasNewImmediateTask := false

	// lock process execution row first
	prcRow, err := tx.SelectProcessExecutionForUpdate(ctx, request.ProcessExecutionId)
	if err != nil {
		return nil, err
	}

	// Step 1: update persistence

	err = p.updateGlobalAttributesIfNeeded(ctx, tx, request.GlobalAttributeTableConfig, request.UpdateGlobalAttributes)
	if err != nil {
		//lint:ignore nilerr reason
		return &data_models.UpdateProcessExecutionFromRpcResponse{
			FailAtUpdatingGlobalAttributes: true,
			UpdatingGlobalAttributesError:  err,
		}, nil
	}

	// Step 2: handle state decision

	sequenceMaps, err := data_models.NewStateExecutionSequenceMapsFromBytes(prcRow.StateExecutionSequenceMaps)
	if err != nil {
		return nil, err
	}

	// if getting next states and there are running state(s), return error
	if len(request.StateDecision.GetNextStates()) > 0 && len(sequenceMaps.PendingExecutionMap) > 0 {
		return nil, fmt.Errorf("cannot start new states when there are running state(s)")
	}

	resp, err := p.handleStateDecision(ctx, tx, data_models.HandleStateDecisionRequest{
		Namespace:                  request.Namespace,
		ProcessId:                  request.ProcessId,
		ProcessType:                request.ProcessType,
		ProcessExecutionId:         request.ProcessExecutionId,
		StateDecision:              request.StateDecision,
		GlobalAttributeTableConfig: request.GlobalAttributeTableConfig,
		WorkerUrl:                  request.WorkerUrl,

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

	return &data_models.UpdateProcessExecutionFromRpcResponse{
		HasNewImmediateTask: hasNewImmediateTask,
	}, nil
}
