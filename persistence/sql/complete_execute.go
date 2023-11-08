// Copyright (c) 2023 XDBLab Organization
// SPDX-License-Identifier: BUSL-1.1

package sql

import (
	"context"
	"fmt"

	"github.com/xdblab/xdb-apis/goapi/xdbapi"
	"github.com/xdblab/xdb/common/log/tag"
	"github.com/xdblab/xdb/extensions"
	"github.com/xdblab/xdb/persistence"
)

func (p sqlProcessStoreImpl) CompleteExecuteExecution(
	ctx context.Context, request persistence.CompleteExecuteExecutionRequest,
) (*persistence.CompleteExecuteExecutionResponse, error) {

	tx, err := p.session.StartTransaction(ctx, defaultTxOpts)
	if err != nil {
		return nil, err
	}

	resp, err := p.doCompleteExecuteExecutionTx(ctx, tx, request)
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

func (p sqlProcessStoreImpl) doCompleteExecuteExecutionTx(
	ctx context.Context, tx extensions.SQLTransaction, request persistence.CompleteExecuteExecutionRequest,
) (*persistence.CompleteExecuteExecutionResponse, error) {
	hasNewImmediateTask := false

	err := p.updateGlobalAttributesIfNeeded(ctx, tx, request)
	if err != nil {
		//lint:ignore nilerr reason
		return &persistence.CompleteExecuteExecutionResponse{
			FailAtUpdatingGlobalAttributes: true,
			UpdatingGlobalAttributesError:  err,
		}, nil
	}

	// lock process execution row first
	prcRow, err := tx.SelectProcessExecutionForUpdate(ctx, request.ProcessExecutionId)
	if err != nil {
		return nil, err
	}

	// Step 1: update state info
	currStateRow := extensions.AsyncStateExecutionRowForUpdateWithoutCommands{
		ProcessExecutionId: request.ProcessExecutionId,
		StateId:            request.StateId,
		StateIdSequence:    request.StateIdSequence,
		Status:             persistence.StateExecutionStatusCompleted,
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

	// Step 2: update the process info

	// at this point, it's either going to next states or closing the process
	// either will require to do transaction on process execution row
	sequenceMaps, err := persistence.NewStateExecutionSequenceMapsFromBytes(prcRow.StateExecutionSequenceMaps)
	if err != nil {
		return nil, err
	}

	// Step 2 - 1: remove current state from PendingExecutionMap

	err = sequenceMaps.CompleteNewStateExecution(request.StateId, int(request.StateIdSequence))
	if err != nil {
		return nil, fmt.Errorf("completing a non-existing state execution, maybe data is corrupted %v-%v, currentMap:%v, err:%w",
			request.StateId, request.StateIdSequence, sequenceMaps, err)
	}

	// Step 2 - 2: add next states to PendingExecutionMap

	if len(request.StateDecision.GetNextStates()) > 0 {
		hasNewImmediateTask = true
		prcExeId := request.ProcessExecutionId

		for _, next := range request.StateDecision.GetNextStates() {
			stateInfo, err := persistence.FromAsyncStateExecutionInfoToBytesForNextState(
				request.Prepare.Info, next.StateConfig,
			)
			if err != nil {
				return nil, err
			}
			stateId := next.StateId
			stateIdSeq := sequenceMaps.StartNewStateExecution(next.StateId)
			stateConfig := next.StateConfig

			stateInput, err := persistence.FromEncodedObjectIntoBytes(next.StateInput)
			if err != nil {
				return nil, err
			}

			err = insertAsyncStateExecution(ctx, tx, prcExeId, stateId, stateIdSeq, stateConfig, stateInput, stateInfo)
			if err != nil {
				return nil, err
			}

			err = insertImmediateTask(ctx, tx, prcExeId, stateId, stateIdSeq, stateConfig, request.TaskShardId)
			if err != nil {
				return nil, err
			}
		}
	}

	// Step 2 - 3:
	// If the process was previously configured to gracefully complete and there are no states running,
	// then gracefully complete the process regardless of the thread close type set in this state.
	// Otherwise, handle the thread close type set in this state.

	toGracefullyComplete := prcRow.WaitToComplete && len(sequenceMaps.PendingExecutionMap) == 0

	toAbortRunningAsyncStates := false

	threadDecision := request.StateDecision.GetThreadCloseDecision()
	if !toGracefullyComplete && request.StateDecision.HasThreadCloseDecision() {
		switch threadDecision.GetCloseType() {
		case xdbapi.GRACEFUL_COMPLETE_PROCESS:
			prcRow.WaitToComplete = true
			toGracefullyComplete = len(sequenceMaps.PendingExecutionMap) == 0
		case xdbapi.FORCE_COMPLETE_PROCESS:
			toAbortRunningAsyncStates = len(sequenceMaps.PendingExecutionMap) > 0

			prcRow.Status = persistence.ProcessExecutionStatusCompleted
			sequenceMaps.PendingExecutionMap = map[string]map[int]bool{}
		case xdbapi.FORCE_FAIL_PROCESS:
			toAbortRunningAsyncStates = len(sequenceMaps.PendingExecutionMap) > 0

			prcRow.Status = persistence.ProcessExecutionStatusFailed
			sequenceMaps.PendingExecutionMap = map[string]map[int]bool{}
		case xdbapi.DEAD_END:
			// do nothing
		}
	}

	if toGracefullyComplete {
		prcRow.Status = persistence.ProcessExecutionStatusCompleted
	}

	if toAbortRunningAsyncStates {
		// handle xdb_sys_async_state_executions
		// find all related rows with the processExecutionId, and
		// modify the wait_until/execute status from running to aborted
		err = tx.BatchUpdateAsyncStateExecutionsToAbortRunning(ctx, request.ProcessExecutionId)
		if err != nil {
			return nil, err
		}
	}

	// update process execution row
	prcRow.StateExecutionSequenceMaps, err = sequenceMaps.ToBytes()
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

	return &persistence.CompleteExecuteExecutionResponse{
		HasNewImmediateTask: hasNewImmediateTask,
	}, nil
}
