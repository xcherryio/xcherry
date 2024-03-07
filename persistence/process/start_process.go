// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: Apache-2.0

package process

import (
	"context"
	"fmt"
	"github.com/xcherryio/xcherry/common/ptr"
	"time"

	"github.com/xcherryio/apis/goapi/xcapi"
	"github.com/xcherryio/xcherry/persistence/data_models"

	"github.com/xcherryio/xcherry/common/log/tag"
	"github.com/xcherryio/xcherry/common/uuid"
	"github.com/xcherryio/xcherry/extensions"
)

func (p sqlProcessStoreImpl) StartProcess(
	ctx context.Context, request data_models.StartProcessRequest,
) (*data_models.StartProcessResponse, error) {
	tx, err := p.session.StartTransaction(ctx, defaultTxOpts)
	if err != nil {
		return nil, err
	}

	resp, err := p.doStartProcessTx(ctx, tx, request)
	if err != nil || resp.AlreadyStarted || resp.FailedAtWritingAppDatabase {
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
	ctx context.Context, tx extensions.SQLTransaction, request data_models.StartProcessRequest,
) (*data_models.StartProcessResponse, error) {
	req := request.Request

	err := p.writeToAppDatabase(ctx, tx, req)
	if err != nil {
		//lint:ignore nilerr reason
		return &data_models.StartProcessResponse{
			FailedAtWritingAppDatabase: true,
			AppDatabaseWritingError:    err,
		}, nil
	}

	requestIdReusePolicy := xcapi.ALLOW_IF_NO_RUNNING
	if req.ProcessStartConfig != nil && req.ProcessStartConfig.IdReusePolicy != nil {
		requestIdReusePolicy = *req.ProcessStartConfig.IdReusePolicy
	}

	var resp *data_models.StartProcessResponse
	var errStartProcess error
	switch requestIdReusePolicy {
	case xcapi.DISALLOW_REUSE:
		resp, errStartProcess = p.applyDisallowReusePolicy(ctx, tx, request)
	case xcapi.ALLOW_IF_NO_RUNNING:
		resp, errStartProcess = p.applyAllowIfNoRunningPolicy(ctx, tx, request)
	case xcapi.ALLOW_IF_PREVIOUS_EXIT_ABNORMALLY:
		resp, errStartProcess = p.applyAllowIfPreviousExitAbnormallyPolicy(ctx, tx, request)
	case xcapi.TERMINATE_IF_RUNNING:
		resp, errStartProcess = p.applyTerminateIfRunningPolicy(ctx, tx, request)
	default:
		return nil, fmt.Errorf(
			"unknown id reuse policy %v",
			req.ProcessStartConfig.IdReusePolicy)
	}

	if errStartProcess != nil {
		return nil, errStartProcess
	}

	err = p.handleInitialLocalAttributesWrite(ctx, tx, req, *resp)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (p sqlProcessStoreImpl) applyDisallowReusePolicy(
	ctx context.Context,
	tx extensions.SQLTransaction,
	request data_models.StartProcessRequest,
) (*data_models.StartProcessResponse, error) {
	_, found, err := tx.SelectLatestProcessExecutionForUpdate(ctx, request.Request.Namespace, request.Request.ProcessId)
	if err != nil {
		return nil, err
	}
	if found {
		return &data_models.StartProcessResponse{
			AlreadyStarted: true,
		}, nil
	}

	hasNewImmediateTask, prcExeId, err := p.insertBrandNewLatestProcessExecution(ctx, tx, request)
	if err != nil {
		return nil, err
	}

	if request.TimeoutTimeUnixSeconds != 0 {
		err = tx.InsertTimerTask(ctx, extensions.TimerTaskRowForInsert{
			ShardId:             request.NewTaskShardId,
			FireTimeUnixSeconds: request.TimeoutTimeUnixSeconds,
			TaskType:            data_models.TimerTaskTypeProcessTimeout,
			ProcessExecutionId:  prcExeId,
		})
		if err != nil {
			return nil, err
		}
	}

	return &data_models.StartProcessResponse{
		ProcessExecutionId:  prcExeId,
		AlreadyStarted:      false,
		HasNewImmediateTask: hasNewImmediateTask,
	}, nil
}

func (p sqlProcessStoreImpl) applyAllowIfNoRunningPolicy(
	ctx context.Context,
	tx extensions.SQLTransaction,
	request data_models.StartProcessRequest,
) (*data_models.StartProcessResponse, error) {
	latestProcessExecution, found, err := tx.SelectLatestProcessExecutionForUpdate(ctx, request.Request.Namespace, request.Request.ProcessId)
	if err != nil {
		return nil, err
	}

	// if it is still running, return already started
	// if finished, start a new process
	// if there is no previous run with the process id, start a new process
	if found {
		processExecutionRow, err := tx.SelectProcessExecution(ctx, latestProcessExecution.ProcessExecutionId)
		if err != nil {
			return nil, err
		}
		if processExecutionRow.Status == data_models.ProcessExecutionStatusRunning {
			return &data_models.StartProcessResponse{
				AlreadyStarted: true,
			}, nil
		}

		hasNewImmediateTask, prcExeId, err := p.updateLatestAndInsertNewProcessExecution(ctx, tx, request)
		if err != nil {
			return nil, err
		}

		if request.TimeoutTimeUnixSeconds != 0 {
			err = tx.InsertTimerTask(ctx, extensions.TimerTaskRowForInsert{
				ShardId:             request.NewTaskShardId,
				FireTimeUnixSeconds: request.TimeoutTimeUnixSeconds,
				TaskType:            data_models.TimerTaskTypeProcessTimeout,
				ProcessExecutionId:  prcExeId,
			})
			if err != nil {
				return nil, err
			}
		}

		return &data_models.StartProcessResponse{
			ProcessExecutionId:  prcExeId,
			AlreadyStarted:      false,
			HasNewImmediateTask: hasNewImmediateTask,
		}, nil
	}

	hasNewImmediateTask, prcExeId, err := p.insertBrandNewLatestProcessExecution(ctx, tx, request)
	if err != nil {
		return nil, err
	}

	if request.TimeoutTimeUnixSeconds != 0 {
		err = tx.InsertTimerTask(ctx, extensions.TimerTaskRowForInsert{
			ShardId:             request.NewTaskShardId,
			FireTimeUnixSeconds: request.TimeoutTimeUnixSeconds,
			TaskType:            data_models.TimerTaskTypeProcessTimeout,
			ProcessExecutionId:  prcExeId,
		})
		if err != nil {
			return nil, err
		}
	}

	return &data_models.StartProcessResponse{
		ProcessExecutionId:  prcExeId,
		AlreadyStarted:      false,
		HasNewImmediateTask: hasNewImmediateTask,
	}, nil
}

func (p sqlProcessStoreImpl) applyAllowIfPreviousExitAbnormallyPolicy(
	ctx context.Context,
	tx extensions.SQLTransaction,
	request data_models.StartProcessRequest,
) (*data_models.StartProcessResponse, error) {
	latestProcessExecution, found, err := tx.SelectLatestProcessExecutionForUpdate(ctx, request.Request.Namespace, request.Request.ProcessId)
	if err != nil {
		return nil, err
	}

	if found {
		processExecutionRow, err := tx.SelectProcessExecution(ctx, latestProcessExecution.ProcessExecutionId)
		if err != nil {
			return nil, err
		}

		// if it is still running, return already started
		if processExecutionRow.Status == data_models.ProcessExecutionStatusRunning {
			return &data_models.StartProcessResponse{
				AlreadyStarted: true,
			}, nil
		}

		// if it is not running, but completed normally, return error
		// otherwise, start a new process
		if processExecutionRow.Status == data_models.ProcessExecutionStatusCompleted {
			return &data_models.StartProcessResponse{
				AlreadyStarted: true,
			}, nil
		}

		hasNewImmediateTask, prcExeId, err := p.updateLatestAndInsertNewProcessExecution(ctx, tx, request)
		if err != nil {
			return nil, err
		}

		if request.TimeoutTimeUnixSeconds != 0 {
			err = tx.InsertTimerTask(ctx, extensions.TimerTaskRowForInsert{
				ShardId:             request.NewTaskShardId,
				FireTimeUnixSeconds: request.TimeoutTimeUnixSeconds,
				TaskType:            data_models.TimerTaskTypeProcessTimeout,
				ProcessExecutionId:  prcExeId,
			})
			if err != nil {
				return nil, err
			}
		}

		return &data_models.StartProcessResponse{
			ProcessExecutionId:  prcExeId,
			AlreadyStarted:      false,
			HasNewImmediateTask: hasNewImmediateTask,
		}, nil
	}

	// if there is no previous run with the process id, start a new process
	hasNewImmediateTask, prcExeId, err := p.insertBrandNewLatestProcessExecution(ctx, tx, request)
	if err != nil {
		return nil, err
	}

	if request.TimeoutTimeUnixSeconds != 0 {
		err = tx.InsertTimerTask(ctx, extensions.TimerTaskRowForInsert{
			ShardId:             request.NewTaskShardId,
			FireTimeUnixSeconds: request.TimeoutTimeUnixSeconds,
			TaskType:            data_models.TimerTaskTypeProcessTimeout,
			ProcessExecutionId:  prcExeId,
		})
		if err != nil {
			return nil, err
		}
	}

	return &data_models.StartProcessResponse{
		ProcessExecutionId:  prcExeId,
		AlreadyStarted:      false,
		HasNewImmediateTask: hasNewImmediateTask,
	}, nil
}

func (p sqlProcessStoreImpl) applyTerminateIfRunningPolicy(
	ctx context.Context,
	tx extensions.SQLTransaction,
	request data_models.StartProcessRequest,
) (*data_models.StartProcessResponse, error) {
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
		if processExecutionRowForUpdate.Status == data_models.ProcessExecutionStatusRunning {
			err = tx.UpdateProcessExecution(ctx, extensions.ProcessExecutionRowForUpdate{
				ProcessExecutionId:         processExecutionRowForUpdate.ProcessExecutionId,
				Status:                     data_models.ProcessExecutionStatusTerminated,
				HistoryEventIdSequence:     processExecutionRowForUpdate.HistoryEventIdSequence,
				StateExecutionSequenceMaps: processExecutionRowForUpdate.StateExecutionSequenceMaps,
				StateExecutionLocalQueues:  processExecutionRowForUpdate.StateExecutionLocalQueues,
				WaitToComplete:             processExecutionRowForUpdate.WaitToComplete,
			})
			if err != nil {
				return nil, err
			}
			err = p.AddVisibilityTaskRecordProcessExecutionStatus(
				ctx,
				tx,
				request.NewTaskShardId,
				request.Request.Namespace,
				request.Request.ProcessId,
				request.Request.ProcessType,
				processExecutionRowForUpdate.ProcessExecutionId,
				data_models.ProcessExecutionStatusTerminated,
				nil,
				ptr.Any(time.Now().Unix()))
			if err != nil {
				return nil, err
			}
		}

		// update the latest process execution and start a new process
		_, prcExeId, err := p.updateLatestAndInsertNewProcessExecution(ctx, tx, request)
		if err != nil {
			return nil, err
		}

		// mark the pending states as aborted
		err = tx.BatchUpdateAsyncStateExecutionsToAbortRunning(ctx, processExecutionRowForUpdate.ProcessExecutionId)
		if err != nil {
			return nil, err
		}

		if request.TimeoutTimeUnixSeconds != 0 {
			err = tx.InsertTimerTask(ctx, extensions.TimerTaskRowForInsert{
				ShardId:             request.NewTaskShardId,
				FireTimeUnixSeconds: request.TimeoutTimeUnixSeconds,
				TaskType:            data_models.TimerTaskTypeProcessTimeout,
				ProcessExecutionId:  prcExeId,
			})
			if err != nil {
				return nil, err
			}
		}

		return &data_models.StartProcessResponse{
			ProcessExecutionId:  prcExeId,
			AlreadyStarted:      false,
			HasNewImmediateTask: true, // if the execution reach here, then it means there is at least 1 visibility task
		}, nil
	}

	// if there is no previous run with the process id, start a new process
	hasNewImmediateTask, prcExeId, err := p.insertBrandNewLatestProcessExecution(ctx, tx, request)
	if err != nil {
		return nil, err
	}

	if request.TimeoutTimeUnixSeconds != 0 {
		err = tx.InsertTimerTask(ctx, extensions.TimerTaskRowForInsert{
			ShardId:             request.NewTaskShardId,
			FireTimeUnixSeconds: request.TimeoutTimeUnixSeconds,
			TaskType:            data_models.TimerTaskTypeProcessTimeout,
			ProcessExecutionId:  prcExeId,
		})
		if err != nil {
			return nil, err
		}
	}

	return &data_models.StartProcessResponse{
		ProcessExecutionId:  prcExeId,
		AlreadyStarted:      false,
		HasNewImmediateTask: hasNewImmediateTask,
	}, nil
}

func (p sqlProcessStoreImpl) insertBrandNewLatestProcessExecution(
	ctx context.Context,
	tx extensions.SQLTransaction,
	request data_models.StartProcessRequest,
) (bool, uuid.UUID, error) {
	prcExeId := uuid.MustNewUUID()
	hasNewImmediateTask := false
	err := tx.InsertLatestProcessExecution(ctx, extensions.LatestProcessExecutionRow{
		Namespace:          request.Request.Namespace,
		ProcessId:          request.Request.ProcessId,
		ProcessExecutionId: prcExeId,
	})
	if err != nil {
		return false, prcExeId, err
	}

	hasNewImmediateTask, err = p.insertProcessExecution(ctx, tx, request, prcExeId)
	if err != nil {
		return hasNewImmediateTask, prcExeId, err
	}
	return hasNewImmediateTask, prcExeId, nil
}

func (p sqlProcessStoreImpl) updateLatestAndInsertNewProcessExecution(
	ctx context.Context,
	tx extensions.SQLTransaction,
	request data_models.StartProcessRequest,
) (bool, uuid.UUID, error) {
	prcExeId := uuid.MustNewUUID()
	hasNewImmediateTask := false
	err := tx.UpdateLatestProcessExecution(ctx, extensions.LatestProcessExecutionRow{
		Namespace:          request.Request.Namespace,
		ProcessId:          request.Request.ProcessId,
		ProcessExecutionId: prcExeId,
	})
	if err != nil {
		return false, prcExeId, err
	}

	hasNewImmediateTask, err = p.insertProcessExecution(ctx, tx, request, prcExeId)
	if err != nil {
		return hasNewImmediateTask, prcExeId, err
	}

	return hasNewImmediateTask, prcExeId, nil
}

func (p sqlProcessStoreImpl) insertProcessExecution(
	ctx context.Context,
	tx extensions.SQLTransaction,
	request data_models.StartProcessRequest,
	processExecutionId uuid.UUID,
) (bool, error) {
	req := request.Request
	hasNewImmediateTask := false

	timeoutSeconds := int32(0)
	if sc, ok := req.GetProcessStartConfigOk(); ok {
		timeoutSeconds = sc.GetTimeoutSeconds()
	}

	processExeInfoBytes, err := data_models.FromStartRequestToProcessInfoBytes(req)
	if err != nil {
		return false, err
	}

	sequenceMaps := data_models.NewStateExecutionSequenceMaps()
	if req.StartStateId != nil {
		stateId := req.GetStartStateId()
		stateIdSeq := sequenceMaps.StartNewStateExecution(req.GetStartStateId())
		stateConfig := req.StartStateConfig

		stateInputBytes, err := data_models.FromEncodedObjectIntoBytes(req.StartStateInput)
		if err != nil {
			return false, err
		}

		stateInfoBytes, err := data_models.FromStartRequestToStateInfoBytes(req)
		if err != nil {
			return false, err
		}

		err = insertAsyncStateExecution(ctx, tx, processExecutionId, stateId, stateIdSeq, stateConfig, stateInputBytes, stateInfoBytes)
		if err != nil {
			return false, err
		}

		err = insertImmediateTask(ctx, tx, processExecutionId, stateId, 1, stateConfig, request.NewTaskShardId)
		if err != nil {
			return false, err
		}

		hasNewImmediateTask = true
	}

	sequenceMapsBytes, err := sequenceMaps.ToBytes()
	if err != nil {
		return hasNewImmediateTask, err
	}

	localQueues := data_models.NewStateExecutionLocalQueues()
	localQueuesBytes, err := localQueues.ToBytes()
	if err != nil {
		return hasNewImmediateTask, err
	}

	startTime := time.Now()
	row := extensions.ProcessExecutionRow{
		ProcessExecutionId: processExecutionId,

		Status:                     data_models.ProcessExecutionStatusRunning,
		HistoryEventIdSequence:     0,
		StateExecutionSequenceMaps: sequenceMapsBytes,
		StateExecutionLocalQueues:  localQueuesBytes,
		Namespace:                  req.Namespace,
		ProcessId:                  req.ProcessId,

		StartTime:      startTime,
		TimeoutSeconds: timeoutSeconds,

		Info: processExeInfoBytes,
	}

	err = tx.InsertProcessExecution(ctx, row)
	if err != nil {
		return hasNewImmediateTask, err
	}

	err = p.AddVisibilityTaskRecordProcessExecutionStatus(
		ctx,
		tx,
		request.NewTaskShardId,
		request.Request.Namespace,
		request.Request.ProcessId,
		request.Request.ProcessType,
		processExecutionId,
		data_models.ProcessExecutionStatusRunning,
		ptr.Any(startTime.Unix()),
		nil)
	if err != nil {
		return hasNewImmediateTask, err
	}
	return true, nil
}
