// Copyright 2023 XDBLab organization
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sql

import (
	"context"
	"fmt"
	"time"

	"github.com/xdblab/xdb-apis/goapi/xdbapi"
	"github.com/xdblab/xdb/common/log/tag"
	"github.com/xdblab/xdb/common/uuid"
	"github.com/xdblab/xdb/extensions"
	"github.com/xdblab/xdb/persistence"
)

func (p sqlProcessStoreImpl) StartProcess(
	ctx context.Context, request persistence.StartProcessRequest,
) (*persistence.StartProcessResponse, error) {
	tx, err := p.session.StartTransaction(ctx, defaultTxOpts)
	if err != nil {
		return nil, err
	}

	resp, err := p.doStartProcessTx(ctx, tx, request)
	if err != nil || resp.AlreadyStarted {
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

func (p sqlProcessStoreImpl) doStartProcessTx(
	ctx context.Context, tx extensions.SQLTransaction, request persistence.StartProcessRequest,
) (*persistence.StartProcessResponse, error) {
	req := request.Request

	requestIdReusePolicy := xdbapi.ALLOW_IF_NO_RUNNING
	if req.ProcessStartConfig != nil && req.ProcessStartConfig.IdReusePolicy != nil {
		requestIdReusePolicy = *req.ProcessStartConfig.IdReusePolicy
	}

	switch requestIdReusePolicy {
	case xdbapi.DISALLOW_REUSE:
		return p.applyDisallowReusePolicy(ctx, tx, request)
	case xdbapi.ALLOW_IF_NO_RUNNING:
		return p.applyAllowIfNoRunningPolicy(ctx, tx, request)
	case xdbapi.ALLOW_IF_PREVIOUS_EXIT_ABNORMALLY:
		return p.applyAllowIfPreviousExitAbnormallyPolicy(ctx, tx, request)
	case xdbapi.TERMINATE_IF_RUNNING:
		return p.applyTerminateIfRunningPolicy(ctx, tx, request)
	default:
		return nil, fmt.Errorf(
			"unknown id reuse policy %v",
			req.ProcessStartConfig.IdReusePolicy)
	}
}

func (p sqlProcessStoreImpl) applyDisallowReusePolicy(
	ctx context.Context,
	tx extensions.SQLTransaction,
	request persistence.StartProcessRequest,
) (*persistence.StartProcessResponse, error) {
	_, found, err := tx.SelectLatestProcessExecutionForUpdate(ctx, request.Request.Namespace, request.Request.ProcessId)
	if err != nil {
		return nil, err
	}
	if found {
		return &persistence.StartProcessResponse{
			AlreadyStarted: true,
		}, nil
	}

	hasNewWorkerTask, prcExeId, err := p.insertBrandNewLatestProcessExecution(ctx, tx, request)
	if err != nil {
		return nil, err
	}

	return &persistence.StartProcessResponse{
		ProcessExecutionId: prcExeId,
		AlreadyStarted:     false,
		HasNewWorkerTask:   hasNewWorkerTask,
	}, nil
}

func (p sqlProcessStoreImpl) applyAllowIfNoRunningPolicy(
	ctx context.Context,
	tx extensions.SQLTransaction,
	request persistence.StartProcessRequest,
) (*persistence.StartProcessResponse, error) {
	latestProcessExecution, found, err := tx.SelectLatestProcessExecutionForUpdate(ctx, request.Request.Namespace, request.Request.ProcessId)
	if err != nil {
		return nil, err
	}

	// if it is still running, return already started
	// if finished, start a new process
	// if there is no previous run with the process id, start a new process
	if found {
		processExecutionRowForUpdate, err := tx.SelectProcessExecution(ctx, latestProcessExecution.ProcessExecutionId)
		if err != nil {
			return nil, err
		}
		if processExecutionRowForUpdate.Status == persistence.ProcessExecutionStatusRunning {
			return &persistence.StartProcessResponse{
				AlreadyStarted: true,
			}, nil
		}

		hasNewWorkerTask, prcExeId, err := p.updateLatestAndInsertNewProcessExecution(ctx, tx, request)
		if err != nil {
			return nil, err
		}

		return &persistence.StartProcessResponse{
			ProcessExecutionId: prcExeId,
			AlreadyStarted:     false,
			HasNewWorkerTask:   hasNewWorkerTask,
		}, nil
	}

	hasNewWorkerTask, prcExeId, err := p.insertBrandNewLatestProcessExecution(ctx, tx, request)
	if err != nil {
		return nil, err
	}

	return &persistence.StartProcessResponse{
		ProcessExecutionId: prcExeId,
		AlreadyStarted:     false,
		HasNewWorkerTask:   hasNewWorkerTask,
	}, nil
}

func (p sqlProcessStoreImpl) applyAllowIfPreviousExitAbnormallyPolicy(
	ctx context.Context,
	tx extensions.SQLTransaction,
	request persistence.StartProcessRequest,
) (*persistence.StartProcessResponse, error) {
	latestProcessExecution, found, err := tx.SelectLatestProcessExecutionForUpdate(ctx, request.Request.Namespace, request.Request.ProcessId)
	if err != nil {
		return nil, err
	}

	if found {
		processExecutionRowForUpdate, err := tx.SelectProcessExecution(ctx, latestProcessExecution.ProcessExecutionId)
		if err != nil {
			return nil, err
		}

		// if it is still running, return already started
		if processExecutionRowForUpdate.Status == persistence.ProcessExecutionStatusRunning {
			return &persistence.StartProcessResponse{
				AlreadyStarted: true,
			}, nil
		}

		// if it is not running, but completed normally, return error
		// otherwise, start a new process
		if processExecutionRowForUpdate.Status == persistence.ProcessExecutionStatusCompleted {
			return &persistence.StartProcessResponse{
				AlreadyStarted: true,
			}, nil
		}

		hasNewWorkerTask, prcExeId, err := p.updateLatestAndInsertNewProcessExecution(ctx, tx, request)
		if err != nil {
			return nil, err
		}

		return &persistence.StartProcessResponse{
			ProcessExecutionId: prcExeId,
			AlreadyStarted:     false,
			HasNewWorkerTask:   hasNewWorkerTask,
		}, nil
	}

	// if there is no previous run with the process id, start a new process
	hasNewWorkerTask, prcExeId, err := p.insertBrandNewLatestProcessExecution(ctx, tx, request)
	if err != nil {
		return nil, err
	}

	return &persistence.StartProcessResponse{
		ProcessExecutionId: prcExeId,
		AlreadyStarted:     false,
		HasNewWorkerTask:   hasNewWorkerTask,
	}, nil
}

func (p sqlProcessStoreImpl) applyTerminateIfRunningPolicy(
	ctx context.Context,
	tx extensions.SQLTransaction,
	request persistence.StartProcessRequest,
) (*persistence.StartProcessResponse, error) {
	latestProcessExecution, found, err := tx.SelectLatestProcessExecutionForUpdate(ctx, request.Request.Namespace, request.Request.ProcessId)
	if err != nil {
		return nil, err
	}

	// if it is still running, terminate it and start a new process
	// otherwise, start a new process
	if found {
		processExecutionRowForUpdate, err := tx.SelectProcessExecutionForUpdate(ctx, latestProcessExecution.ProcessExecutionId)
		if err != nil {
			return nil, err
		}
		// mark the process as terminated
		if processExecutionRowForUpdate.Status == persistence.ProcessExecutionStatusRunning {
			err = tx.UpdateProcessExecution(ctx, extensions.ProcessExecutionRowForUpdate{
				ProcessExecutionId:         processExecutionRowForUpdate.ProcessExecutionId,
				IsCurrent:                  false,
				Status:                     persistence.ProcessExecutionStatusTerminated,
				HistoryEventIdSequence:     processExecutionRowForUpdate.HistoryEventIdSequence,
				StateExecutionSequenceMaps: processExecutionRowForUpdate.StateExecutionSequenceMaps,
			})
			if err != nil {
				return nil, err
			}
		}

		// update the latest process execution and start a new process
		hasNewWorkerTask, prcExeId, err := p.updateLatestAndInsertNewProcessExecution(ctx, tx, request)
		if err != nil {
			return nil, err
		}

		// mark the pending states as aborted
		err = tx.BatchUpdateAsyncStateExecutionsToAbortRunning(ctx, processExecutionRowForUpdate.ProcessExecutionId)
		if err != nil {
			return nil, err
		}

		return &persistence.StartProcessResponse{
			ProcessExecutionId: prcExeId,
			AlreadyStarted:     false,
			HasNewWorkerTask:   hasNewWorkerTask,
		}, nil
	}

	// if there is no previous run with the process id, start a new process
	hasNewWorkerTask, prcExeId, err := p.insertBrandNewLatestProcessExecution(ctx, tx, request)
	if err != nil {
		return nil, err
	}

	return &persistence.StartProcessResponse{
		ProcessExecutionId: prcExeId,
		AlreadyStarted:     false,
		HasNewWorkerTask:   hasNewWorkerTask,
	}, nil
}

func (p sqlProcessStoreImpl) insertBrandNewLatestProcessExecution(
	ctx context.Context,
	tx extensions.SQLTransaction,
	request persistence.StartProcessRequest,
) (bool, uuid.UUID, error) {
	prcExeId := uuid.MustNewUUID()
	hasNewWorkerTask := false
	err := tx.InsertLatestProcessExecution(ctx, extensions.LatestProcessExecutionRow{
		Namespace:          request.Request.Namespace,
		ProcessId:          request.Request.ProcessId,
		ProcessExecutionId: prcExeId,
	})
	if err != nil {
		return hasNewWorkerTask, prcExeId, err
	}

	hasNewWorkerTask, err = p.insertProcessExecution(ctx, tx, request, prcExeId)
	if err != nil {
		return hasNewWorkerTask, prcExeId, err
	}
	return hasNewWorkerTask, prcExeId, nil
}

func (p sqlProcessStoreImpl) updateLatestAndInsertNewProcessExecution(
	ctx context.Context,
	tx extensions.SQLTransaction,
	request persistence.StartProcessRequest,
) (bool, uuid.UUID, error) {
	prcExeId := uuid.MustNewUUID()
	hasNewWorkerTask := false
	err := tx.UpdateLatestProcessExecution(ctx, extensions.LatestProcessExecutionRow{
		Namespace:          request.Request.Namespace,
		ProcessId:          request.Request.ProcessId,
		ProcessExecutionId: prcExeId,
	})
	if err != nil {
		return hasNewWorkerTask, prcExeId, err
	}

	hasNewWorkerTask, err = p.insertProcessExecution(ctx, tx, request, prcExeId)
	if err != nil {
		return hasNewWorkerTask, prcExeId, err
	}

	return hasNewWorkerTask, prcExeId, nil
}

func (p sqlProcessStoreImpl) insertProcessExecution(
	ctx context.Context,
	tx extensions.SQLTransaction,
	request persistence.StartProcessRequest,
	processExecutionId uuid.UUID,
) (bool, error) {
	req := request.Request
	hasNewWorkerTask := false

	timeoutSeconds := int32(0)
	if sc, ok := req.GetProcessStartConfigOk(); ok {
		timeoutSeconds = sc.GetTimeoutSeconds()
	}

	processExeInfoBytes, err := persistence.FromStartRequestToProcessInfoBytes(req)
	if err != nil {
		return hasNewWorkerTask, err
	}

	sequenceMaps := persistence.NewStateExecutionSequenceMaps()
	if req.StartStateId != nil {
		stateId := req.GetStartStateId()
		stateIdSeq := sequenceMaps.StartNewStateExecution(req.GetStartStateId())
		stateConfig := req.StartStateConfig

		stateInputBytes, err := persistence.FromEncodedObjectIntoBytes(req.StartStateInput)
		if err != nil {
			return hasNewWorkerTask, err
		}

		stateInfoBytes, err := persistence.FromStartRequestToStateInfoBytes(req)
		if err != nil {
			return hasNewWorkerTask, err
		}

		err = insertAsyncStateExecution(ctx, tx, processExecutionId, stateId, stateIdSeq, stateConfig, stateInputBytes, stateInfoBytes)
		if err != nil {
			return hasNewWorkerTask, err
		}

		err = insertWorkerTask(ctx, tx, processExecutionId, stateId, 1, stateConfig, request.NewTaskShardId)
		if err != nil {
			return hasNewWorkerTask, err
		}

		hasNewWorkerTask = true
	}

	sequenceMapsBytes, err := sequenceMaps.ToBytes()
	if err != nil {
		return hasNewWorkerTask, err
	}

	row := extensions.ProcessExecutionRow{
		ProcessExecutionId: processExecutionId,

		IsCurrent:                  true,
		Status:                     persistence.ProcessExecutionStatusRunning,
		HistoryEventIdSequence:     0,
		StateExecutionSequenceMaps: sequenceMapsBytes,
		Namespace:                  req.Namespace,
		ProcessId:                  req.ProcessId,

		StartTime:      time.Now(),
		TimeoutSeconds: timeoutSeconds,

		Info: processExeInfoBytes,
	}

	err = tx.InsertProcessExecution(ctx, row)
	return hasNewWorkerTask, err
}
