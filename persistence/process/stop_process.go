// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: Apache-2.0

package process

import (
	"context"
	"github.com/xcherryio/apis/goapi/xcapi"
	"github.com/xcherryio/xcherry/common/ptr"
	"github.com/xcherryio/xcherry/persistence/data_models"
	"time"

	"github.com/xcherryio/xcherry/common/log/tag"
	"github.com/xcherryio/xcherry/extensions"
)

func (p sqlProcessStoreImpl) StopProcess(
	ctx context.Context, request data_models.StopProcessRequest,
) (*data_models.StopProcessResponse, error) {
	tx, err := p.session.StartTransaction(ctx, defaultTxOpts)
	if err != nil {
		return nil, err
	}

	namespace := request.Namespace
	processId := request.ProcessId
	status := data_models.ProcessExecutionStatusTerminated
	if request.ProcessStopType == xcapi.FAIL {
		status = data_models.ProcessExecutionStatusFailed
	}

	resp, err := p.doStopProcessTx(ctx, tx, namespace, processId, request.NewTaskShardId, status)
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
	ctx context.Context, tx extensions.SQLTransaction, namespace string, processId string, newTaskShardId int32,
	status data_models.ProcessExecutionStatus,
) (*data_models.StopProcessResponse, error) {
	curProcExecRow, err := p.session.SelectLatestProcessExecution(ctx, namespace, processId)
	if err != nil {
		if p.session.IsNotFoundError(err) {
			// early stop when there is no such process running
			return &data_models.StopProcessResponse{
				NotExists: true,
			}, nil
		}
		return nil, err
	}

	// handle xcherry_sys_process_executions
	procExecRow, err := tx.SelectProcessExecutionForUpdate(ctx, curProcExecRow.ProcessExecutionId)
	if err != nil {
		return nil, err
	}

	if procExecRow.Status != data_models.ProcessExecutionStatusRunning {
		return &data_models.StopProcessResponse{
			NotExists: false,
		}, nil
	}

	sequenceMaps, err := data_models.NewStateExecutionSequenceMapsFromBytes(procExecRow.StateExecutionSequenceMaps)
	if err != nil {
		return nil, err
	}

	pendingExecutionMap := sequenceMaps.PendingExecutionMap

	sequenceMaps.PendingExecutionMap = map[string]map[int]bool{}
	procExecRow.StateExecutionSequenceMaps, err = sequenceMaps.ToBytes()
	if err != nil {
		return nil, err
	}

	procExecRow.Status = status

	err = tx.UpdateProcessExecution(ctx, *procExecRow)
	if err != nil {
		return nil, err
	}

	if len(pendingExecutionMap) > 0 {
		// handle xcherry_sys_async_state_executions
		// find all related rows with the processExecutionId, and
		// modify the wait_until/execute status from running to aborted
		err = tx.BatchUpdateAsyncStateExecutionsToAbortRunning(ctx, curProcExecRow.ProcessExecutionId)
		if err != nil {
			return nil, err
		}
	}

	procExecInfoJson, err := data_models.BytesToProcessExecutionInfo(curProcExecRow.Info)
	if err != nil {
		return nil, err
	}

	err = p.AddVisibilityTaskRecordProcessExecutionStatus(
		ctx,
		tx,
		newTaskShardId,
		namespace,
		processId,
		procExecInfoJson.ProcessType,
		curProcExecRow.ProcessExecutionId,
		status,
		nil,
		ptr.Any(time.Now().Unix()),
	)
	if err != nil {
		return nil, err
	}

	return &data_models.StopProcessResponse{
		NotExists: false,
	}, nil
}
