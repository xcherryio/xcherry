// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: Apache-2.0

package sql

import (
	"context"
	"fmt"
	"time"

	"github.com/xcherryio/xcherry/persistence/data_models"

	"github.com/xcherryio/xcherry/common/log/tag"
	"github.com/xcherryio/xcherry/extensions"
)

func (p sqlProcessStoreImpl) CompleteExecuteExecution(
	ctx context.Context, request data_models.CompleteExecuteExecutionRequest,
) (*data_models.CompleteExecuteExecutionResponse, error) {

	tx, err := p.session.StartTransaction(ctx, defaultTxOpts)
	if err != nil {
		return nil, err
	}

	resp, err := p.doCompleteExecuteExecutionTx(ctx, tx, request)
	if err != nil || resp.FailedAtWritingAppDatabase {
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

func (p sqlProcessStoreImpl) doCompleteExecuteExecutionTx(
	ctx context.Context, tx extensions.SQLTransaction, request data_models.CompleteExecuteExecutionRequest,
) (*data_models.CompleteExecuteExecutionResponse, error) {
	hasNewImmediateTask := false

	// lock process execution row first
	prcRow, err := tx.SelectProcessExecutionForUpdate(ctx, request.ProcessExecutionId)
	if err != nil {
		return nil, err
	}

	// Step 1: update persistence

	err = p.writeToAppDatabaseIfNeeded(ctx, tx, request.AppDatabaseConfig, request.WriteAppDatabase)
	if err != nil {
		//lint:ignore nilerr reason
		return &data_models.CompleteExecuteExecutionResponse{
			FailedAtWritingAppDatabase: true,
			AppDatabaseWritingError:    err,
		}, nil
	}

	err = p.updateLocalAttributesIfNeeded(
		ctx,
		tx,
		request.ProcessExecutionId,
		request.UpdateLocalAttributes)
	if err != nil {
		return nil, err
	}

	// Step 2: update state info

	currStateRow := extensions.AsyncStateExecutionRowForUpdateWithoutCommands{
		ProcessExecutionId: request.ProcessExecutionId,
		StateId:            request.StateId,
		StateIdSequence:    request.StateIdSequence,
		Status:             data_models.StateExecutionStatusCompleted,
		PreviousVersion:    request.Prepare.PreviousVersion,
		LastFailure:        nil,
	}

	err = tx.UpdateAsyncStateExecutionWithoutCommands(ctx, currStateRow)
	if err != nil {
		if p.session.IsConditionalUpdateFailure(err) {
			p.logger.Warn("UpdateAsyncStateExecutionWithoutCommands failed at conditional update")
		}
		return nil, err
	}

	// Step 3: update process info

	// at this point, it's either going to next states or closing the process
	// either will require to do transaction on process execution row
	sequenceMaps, err := data_models.NewStateExecutionSequenceMapsFromBytes(prcRow.StateExecutionSequenceMaps)
	if err != nil {
		return nil, err
	}

	// Step 3 - 1: remove current state from PendingExecutionMap

	err = sequenceMaps.CompleteNewStateExecution(request.StateId, int(request.StateIdSequence))
	if err != nil {
		return nil, fmt.Errorf("completing a non-existing state execution, maybe data is corrupted %v-%v, currentMap:%v, err:%w",
			request.StateId, request.StateIdSequence, sequenceMaps, err)
	}

	// Step 3 - 2: add next states to PendingExecutionMap

	resp, err := p.handleStateDecision(ctx, tx, HandleStateDecisionRequest{
		Namespace:          request.Prepare.Info.Namespace,
		ProcessId:          request.Prepare.Info.ProcessId,
		ProcessType:        request.Prepare.Info.ProcessType,
		ProcessExecutionId: request.ProcessExecutionId,
		StateDecision:      request.StateDecision,
		AppDatabaseConfig:  request.AppDatabaseConfig,
		WorkerUrl:          request.Prepare.Info.WorkerURL,

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

	// Step 3 - 3: update process execution row

	err = tx.UpdateProcessExecution(ctx, *prcRow)
	if err != nil {
		return nil, err
	}

	// Step 4: publish to local queue

	hasNewImmediateTask2, err := p.publishToLocalQueue(ctx, tx, request.ProcessExecutionId, request.PublishToLocalQueue)
	if err != nil {
		return nil, err
	}
	if hasNewImmediateTask2 {
		hasNewImmediateTask = true
	}

	// Step 5: record status if process is ending
	if prcRow.Status != data_models.ProcessExecutionStatusRunning {
		err := p.recordProcessExecutionStatusForVisibility(
			ctx,
			tx,
			request.TaskShardId,
			request.Prepare.Info.Namespace,
			request.Prepare.Info.ProcessId,
			request.Prepare.Info.ProcessType,
			request.ProcessExecutionId,
			prcRow.Status,
			-1,
			time.Now().Unix(),
		)
		if err != nil {
			return nil, err
		}
		hasNewImmediateTask = true
	}

	// Step 6: delete current immediate task
	err = tx.DeleteImmediateTask(ctx, extensions.ImmediateTaskRowDeleteFilter{
		ShardId:      request.TaskShardId,
		TaskSequence: request.TaskSequence,
	})
	if err != nil {
		return nil, err
	}

	return &data_models.CompleteExecuteExecutionResponse{
		HasNewImmediateTask: hasNewImmediateTask,
	}, nil
}
