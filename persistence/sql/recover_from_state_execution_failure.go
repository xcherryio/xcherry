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

func (p sqlProcessStoreImpl) RecoverFromStateExecutionFailure(
	ctx context.Context,
	request persistence.RecoverFromStateExecutionFailureRequest,
) error {
	tx, err := p.session.StartTransaction(ctx, defaultTxOpts)
	if err != nil {
		return err
	}

	err = p.doRecoverFromStateExecutionFailureTx(ctx, tx, request)
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

func (p sqlProcessStoreImpl) doRecoverFromStateExecutionFailureTx(
	ctx context.Context,
	tx extensions.SQLTransaction,
	request persistence.RecoverFromStateExecutionFailureRequest,
) error {
	// lock process execution row first
	prcRow, err := tx.SelectProcessExecutionForUpdate(ctx, request.ProcessExecutionId)
	if err != nil {
		return err
	}

	// mark the current state as failed
	failureBytes, err := persistence.CreateStateExecutionFailureBytesForBackoff(
		request.LastFailureStatus, request.LastFailureDetails, request.LastFailureCompletedAttempts)
	if err != nil {
		return err
	}

	currStateRow := extensions.AsyncStateExecutionRowForUpdateWithoutCommands{
		ProcessExecutionId: request.ProcessExecutionId,
		StateId:            request.SourceStateExecutionId.StateId,
		StateIdSequence:    request.SourceStateExecutionId.StateIdSequence,
		Status:             persistence.StateExecutionStatusFailed,
		PreviousVersion:    request.Prepare.PreviousVersion,
		LastFailure:        failureBytes,
	}

	err = tx.UpdateAsyncStateExecutionWithoutCommands(ctx, currStateRow)
	if err != nil {
		if p.session.IsConditionalUpdateFailure(err) {
			p.logger.Warn("UpdateAsyncStateExecution failed at conditional update")
		}
		return err
	}

	// update process info
	sequenceMaps, err := persistence.NewStateExecutionSequenceMapsFromBytes(prcRow.StateExecutionSequenceMaps)
	if err != nil {
		return err
	}

	// remove current state from PendingExecutionMap
	err = sequenceMaps.CompleteNewStateExecution(request.SourceStateExecutionId.StateId, int(request.SourceStateExecutionId.StateIdSequence))
	if err != nil {
		return fmt.Errorf("completing a non-existing state execution, maybe data is corrupted %v-%v, currentMap:%v, err:%w",
			request.SourceStateExecutionId.StateId, request.SourceStateExecutionId.StateIdSequence, sequenceMaps, err)
	}

	// start new state execution with state id from request
	stateInfoBytes, err := persistence.FromAsyncStateExecutionInfoToBytesForStateRecovery(
		request.Prepare.Info, request.SourceStateExecutionId.StateId, request.SourceFailedStateApi)
	if err != nil {
		return err
	}

	nextStateId := request.DestinationStateId
	nextStateIdSeq := sequenceMaps.StartNewStateExecution(request.DestinationStateId)
	stateConfig := request.DestinationStateConfig
	stateInput := request.DestinationStateInput
	stateInputBytes, err := persistence.FromEncodedObjectIntoBytes(&stateInput)
	if err != nil {
		return err
	}
	err = insertAsyncStateExecution(ctx, tx, request.ProcessExecutionId, nextStateId, nextStateIdSeq, stateConfig, stateInputBytes, stateInfoBytes)
	if err != nil {
		return err
	}

	err = insertImmediateTask(ctx, tx, request.ProcessExecutionId, nextStateId, nextStateIdSeq, stateConfig, request.ShardId)
	if err != nil {
		return err
	}

	// update process execution row
	prcRow.StateExecutionSequenceMaps, err = sequenceMaps.ToBytes()
	if err != nil {
		return err
	}

	err = tx.UpdateProcessExecution(ctx, *prcRow)
	if err != nil {
		return err
	}

	return nil
}
