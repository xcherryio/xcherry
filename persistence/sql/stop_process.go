// Copyright (c) 2023 XDBLab Organization
// SPDX-License-Identifier: BUSL-1.1

package sql

import (
	"context"

	"github.com/xdblab/xdb-apis/goapi/xdbapi"
	"github.com/xdblab/xdb/common/log/tag"
	"github.com/xdblab/xdb/extensions"
	"github.com/xdblab/xdb/persistence"
)

func (p sqlProcessStoreImpl) StopProcess(
	ctx context.Context, request persistence.StopProcessRequest,
) (*persistence.StopProcessResponse, error) {
	tx, err := p.session.StartTransaction(ctx, defaultTxOpts)
	if err != nil {
		return nil, err
	}

	resp, err := p.doStopProcessTx(ctx, tx, request)
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

func (p sqlProcessStoreImpl) doStopProcessTx(
	ctx context.Context, tx extensions.SQLTransaction, request persistence.StopProcessRequest,
) (*persistence.StopProcessResponse, error) {
	curProcExecRow, err := p.session.SelectLatestProcessExecution(ctx, request.Namespace, request.ProcessId)
	if err != nil {
		if p.session.IsNotFoundError(err) {
			// early stop when there is no such process running
			return &persistence.StopProcessResponse{
				NotExists: true,
			}, nil
		}
		return nil, err
	}

	// handle xdb_sys_process_executions
	procExecRow, err := tx.SelectProcessExecutionForUpdate(ctx, curProcExecRow.ProcessExecutionId)
	if err != nil {
		return nil, err
	}

	sequenceMaps, err := persistence.NewStateExecutionSequenceMapsFromBytes(procExecRow.StateExecutionSequenceMaps)
	if err != nil {
		return nil, err
	}

	pendingExecutionMap := sequenceMaps.PendingExecutionMap

	sequenceMaps.PendingExecutionMap = map[string]map[int]bool{}
	procExecRow.StateExecutionSequenceMaps, err = sequenceMaps.ToBytes()
	if err != nil {
		return nil, err
	}

	procExecRow.Status = persistence.ProcessExecutionStatusTerminated
	if request.ProcessStopType == xdbapi.FAIL {
		procExecRow.Status = persistence.ProcessExecutionStatusFailed
	}

	err = tx.UpdateProcessExecution(ctx, *procExecRow)
	if err != nil {
		return nil, err
	}

	if len(pendingExecutionMap) > 0 {
		// handle xdb_sys_async_state_executions
		// find all related rows with the processExecutionId, and
		// modify the wait_until/execute status from running to aborted
		err = tx.BatchUpdateAsyncStateExecutionsToAbortRunning(ctx, curProcExecRow.ProcessExecutionId)
		if err != nil {
			return nil, err
		}
	}

	return &persistence.StopProcessResponse{
		NotExists: false,
	}, nil
}
