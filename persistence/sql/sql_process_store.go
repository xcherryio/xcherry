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
	"github.com/xdblab/xdb/common/log"
	"github.com/xdblab/xdb/common/log/tag"
	"github.com/xdblab/xdb/common/ptr"
	"github.com/xdblab/xdb/common/uuid"
	"github.com/xdblab/xdb/config"
	"github.com/xdblab/xdb/extensions"
	"github.com/xdblab/xdb/persistence"
)

type sqlProcessStoreImpl struct {
	session extensions.SQLDBSession
	logger  log.Logger
}

func NewSQLProcessStore(sqlConfig config.SQL, logger log.Logger) (persistence.ProcessStore, error) {
	session, err := extensions.NewSQLSession(&sqlConfig)
	return &sqlProcessStoreImpl{
		session: session,
		logger:  logger,
	}, err
}

func (p sqlProcessStoreImpl) Close() error {
	return p.session.Close()
}

func (p sqlProcessStoreImpl) CleanUpTasksForTest(ctx context.Context, shardId int32) error {
	return p.session.CleanUpTasksForTest(ctx, shardId)
}

func (p sqlProcessStoreImpl) StartProcess(
	ctx context.Context, request persistence.StartProcessRequest,
) (*persistence.StartProcessResponse, error) {
	tx, err := p.session.StartTransaction(ctx)
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

func (p sqlProcessStoreImpl) StopProcess(ctx context.Context, request persistence.StopProcessRequest) (*persistence.StopProcessResponse, error) {
	tx, err := p.session.StartTransaction(ctx)
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
			p.logger.Error(err.Error())
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
			p.logger.Error(err.Error())
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
			return nil, fmt.Errorf("process %v has completed normally", request.Request.ProcessId)
		}

		hasNewWorkerTask, prcExeId, err := p.updateLatestAndInsertNewProcessExecution(ctx, tx, request)
		if err != nil {
			p.logger.Error(err.Error())
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
		p.logger.Error(err.Error())
		return nil, err
	}

	// if it is still running, terminate it and start a new process
	// otherwise, start a new process
	if found {
		processExecutionRowForUpdate, err := tx.SelectProcessExecution(ctx, latestProcessExecution.ProcessExecutionId)
		if err != nil {
			p.logger.Error(err.Error())
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
				p.logger.Error(err.Error())
				return nil, err
			}
		}
		// mark the pending states as aborted
		err = tx.BatchUpdateAsyncStateExecutionsToAbortRunning(ctx, processExecutionRowForUpdate.ProcessExecutionId)
		if err != nil {
			p.logger.Error(err.Error())
			return nil, err
		}
		// update the latest process execution and start a new process
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
		p.logger.Error(err.Error())
		return nil, err
	}

	procExecRow.IsCurrent = false

	sequenceMaps, err := persistence.NewStateExecutionSequenceMapsFromBytes(procExecRow.StateExecutionSequenceMaps)
	if err != nil {
		p.logger.Error(err.Error())
		return nil, err
	}

	pendingExecutionMap := sequenceMaps.PendingExecutionMap

	sequenceMaps.PendingExecutionMap = map[string]map[int]bool{}
	procExecRow.StateExecutionSequenceMaps, err = sequenceMaps.ToBytes()
	if err != nil {
		p.logger.Error(err.Error())
		return nil, err
	}

	procExecRow.Status = persistence.ProcessExecutionStatusTerminated
	if request.ProcessStopType == xdbapi.FAIL {
		procExecRow.Status = persistence.ProcessExecutionStatusFailed
	}

	// early stop when there are no pending tasks
	if len(pendingExecutionMap) == 0 {
		return &persistence.StopProcessResponse{
			NotExists: false,
		}, nil
	}

	// handle xdb_sys_async_state_executions
	// find all related rows with the processExecutionId, and
	// modify the wait_until/execute status from running to aborted
	err = tx.BatchUpdateAsyncStateExecutionsToAbortRunning(ctx, curProcExecRow.ProcessExecutionId)
	if err != nil {
		p.logger.Error(err.Error())
		return nil, err
	}

	err = tx.UpdateProcessExecution(ctx, *procExecRow)
	if err != nil {
		p.logger.Error(err.Error())
		return nil, err
	}

	return &persistence.StopProcessResponse{
		NotExists: false,
	}, nil
}

func (p sqlProcessStoreImpl) DescribeLatestProcess(
	ctx context.Context, request persistence.DescribeLatestProcessRequest,
) (*persistence.DescribeLatestProcessResponse, error) {
	row, err := p.session.SelectLatestProcessExecution(ctx, request.Namespace, request.ProcessId)
	if err != nil {
		if p.session.IsNotFoundError(err) {
			return &persistence.DescribeLatestProcessResponse{
				NotExists: true,
			}, nil
		}
		p.logger.Error(err.Error())
		return nil, err
	}

	info, err := persistence.BytesToProcessExecutionInfo(row.Info)
	if err != nil {
		p.logger.Error(err.Error())
		return nil, err
	}

	return &persistence.DescribeLatestProcessResponse{
		Response: &xdbapi.ProcessExecutionDescribeResponse{
			ProcessExecutionId: ptr.Any(row.ProcessExecutionId.String()),
			ProcessType:        &info.ProcessType,
			WorkerUrl:          &info.WorkerURL,
			StartTimestamp:     ptr.Any(int32(row.StartTime.Unix())),
			Status:             xdbapi.ProcessStatus(row.Status.String()).Ptr(),
		},
	}, nil
}

func (p sqlProcessStoreImpl) GetWorkerTasks(
	ctx context.Context, request persistence.GetWorkerTasksRequest,
) (*persistence.GetWorkerTasksResponse, error) {
	workerTasks, err := p.session.BatchSelectWorkerTasks(
		ctx, request.ShardId, request.StartSequenceInclusive, request.PageSize)
	if err != nil {
		return nil, err
	}
	var tasks []persistence.WorkerTask
	for _, t := range workerTasks {
		info, err := persistence.BytesToWorkerTaskInfo(t.Info)
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, persistence.WorkerTask{
			ShardId:            request.ShardId,
			TaskSequence:       ptr.Any(t.TaskSequence),
			TaskType:           t.TaskType,
			ProcessExecutionId: t.ProcessExecutionId,
			StateExecutionId: persistence.StateExecutionId{
				StateId:         t.StateId,
				StateIdSequence: t.StateIdSequence,
			},
			WorkerTaskInfo: info,
		})
	}
	resp := &persistence.GetWorkerTasksResponse{
		Tasks: tasks,
	}
	if len(workerTasks) > 0 {
		firstTask := workerTasks[0]
		lastTask := workerTasks[len(workerTasks)-1]
		resp.MinSequenceInclusive = firstTask.TaskSequence
		resp.MaxSequenceInclusive = lastTask.TaskSequence
	}
	return resp, nil
}

func (p sqlProcessStoreImpl) DeleteWorkerTasks(
	ctx context.Context, request persistence.DeleteWorkerTasksRequest,
) error {
	return p.session.BatchDeleteWorkerTask(ctx, extensions.WorkerTaskRangeDeleteFilter{
		ShardId:                  request.ShardId,
		MinTaskSequenceInclusive: request.MinTaskSequenceInclusive,
		MaxTaskSequenceInclusive: request.MaxTaskSequenceInclusive,
	})
}

func (p sqlProcessStoreImpl) PrepareStateExecution(
	ctx context.Context, request persistence.PrepareStateExecutionRequest,
) (*persistence.PrepareStateExecutionResponse, error) {
	stateRow, err := p.session.SelectAsyncStateExecutionForUpdate(
		ctx, extensions.AsyncStateExecutionSelectFilter{
			ProcessExecutionId: request.ProcessExecutionId,
			StateId:            request.StateId,
			StateIdSequence:    request.StateIdSequence,
		})
	if err != nil {
		p.logger.Error(err.Error())
		return nil, err
	}
	info, err := persistence.BytesToAsyncStateExecutionInfo(stateRow.Info)
	if err != nil {
		return nil, err
	}
	input, err := persistence.BytesToEncodedObject(stateRow.Input)
	if err != nil {
		return nil, err
	}
	return &persistence.PrepareStateExecutionResponse{
		WaitUntilStatus: stateRow.WaitUntilStatus,
		ExecuteStatus:   stateRow.ExecuteStatus,
		PreviousVersion: stateRow.PreviousVersion,
		Info:            info,
		Input:           input,
	}, nil
}

func (p sqlProcessStoreImpl) CompleteWaitUntilExecution(
	ctx context.Context, request persistence.CompleteWaitUntilExecutionRequest,
) (*persistence.CompleteWaitUntilExecutionResponse, error) {
	if request.CommandRequest.GetWaitingType() != xdbapi.EMPTY_COMMAND {
		// TODO set command request from resp
		return nil, fmt.Errorf("not supported command type %v", request.CommandRequest.GetWaitingType())
	}

	tx, err := p.session.StartTransaction(ctx)
	if err != nil {
		return nil, err
	}

	resp, err := p.doCompleteWaitUntilExecutionTx(ctx, tx, request)
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

func (p sqlProcessStoreImpl) doCompleteWaitUntilExecutionTx(
	ctx context.Context, tx extensions.SQLTransaction, request persistence.CompleteWaitUntilExecutionRequest,
) (*persistence.CompleteWaitUntilExecutionResponse, error) {
	stateRow := extensions.AsyncStateExecutionRowForUpdate{
		ProcessExecutionId: request.ProcessExecutionId,
		StateId:            request.StateId,
		StateIdSequence:    request.StateIdSequence,
		WaitUntilStatus:    persistence.StateExecutionStatusCompleted,
		ExecuteStatus:      persistence.StateExecutionStatusRunning,
		PreviousVersion:    request.Prepare.PreviousVersion,
		LastFailure:        nil,
	}

	err := tx.UpdateAsyncStateExecution(ctx, stateRow)
	if err != nil {
		if p.session.IsConditionalUpdateFailure(err) {
			p.logger.Warn("UpdateAsyncStateExecution failed at conditional update")
		}
		return nil, err
	}
	err = tx.InsertWorkerTask(ctx, extensions.WorkerTaskRowForInsert{
		ShardId:            request.TaskShardId,
		TaskType:           persistence.WorkerTaskTypeExecute,
		ProcessExecutionId: request.ProcessExecutionId,
		StateId:            request.StateId,
		StateIdSequence:    request.StateIdSequence,
	})
	if err != nil {
		return nil, err
	}
	return &persistence.CompleteWaitUntilExecutionResponse{
		HasNewWorkerTask: true,
	}, nil
}

func (p sqlProcessStoreImpl) CompleteExecuteExecution(
	ctx context.Context, request persistence.CompleteExecuteExecutionRequest,
) (*persistence.CompleteExecuteExecutionResponse, error) {

	tx, err := p.session.StartTransaction(ctx)
	if err != nil {
		return nil, err
	}

	resp, err := p.doCompleteExecuteExecutionTx(ctx, tx, request)
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

func (p sqlProcessStoreImpl) doCompleteExecuteExecutionTx(
	ctx context.Context, tx extensions.SQLTransaction, request persistence.CompleteExecuteExecutionRequest,
) (*persistence.CompleteExecuteExecutionResponse, error) {
	hasNewWorkerTask := false

	// Step 1: update state info
	currStateRow := extensions.AsyncStateExecutionRowForUpdate{
		ProcessExecutionId: request.ProcessExecutionId,
		StateId:            request.StateId,
		StateIdSequence:    request.StateIdSequence,
		WaitUntilStatus:    request.Prepare.WaitUntilStatus,
		ExecuteStatus:      persistence.StateExecutionStatusCompleted,
		PreviousVersion:    request.Prepare.PreviousVersion,
		LastFailure:        nil,
	}

	err := tx.UpdateAsyncStateExecution(ctx, currStateRow)
	if err != nil {
		if p.session.IsConditionalUpdateFailure(err) {
			p.logger.Warn("UpdateAsyncStateExecution failed at conditional update")
		}
		return nil, err
	}

	// Step 2: update the process info

	// at this point, it's either going to next states or closing the process
	// either will require to do transaction on process execution row
	prcRow, err := tx.SelectProcessExecutionForUpdate(ctx, request.ProcessExecutionId)
	if err != nil {
		return nil, err
	}

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
		hasNewWorkerTask = true

		// reuse the info from last state execution as it won't change
		stateInfo, err := persistence.FromAsyncStateExecutionInfoToBytes(request.Prepare.Info)
		if err != nil {
			return nil, err
		}

		prcExeId := request.ProcessExecutionId

		for _, next := range request.StateDecision.GetNextStates() {
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

			err = insertWorkerTask(ctx, tx, prcExeId, stateId, stateIdSeq, stateConfig, request.TaskShardId)
			if err != nil {
				return nil, err
			}
		}
	}

	// Step 2 - 3:
	// If the process was previously configured to gracefully complete and there are no states running,
	// then gracefully complete the process regardless of the thread close type set in this state.

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
			prcRow.IsCurrent = false
			sequenceMaps.PendingExecutionMap = map[string]map[int]bool{}
		case xdbapi.FORCE_FAIL_PROCESS:
			toAbortRunningAsyncStates = len(sequenceMaps.PendingExecutionMap) > 0

			prcRow.Status = persistence.ProcessExecutionStatusFailed
			prcRow.IsCurrent = false
			sequenceMaps.PendingExecutionMap = map[string]map[int]bool{}
		case xdbapi.DEAD_END:
			// do nothing
		}
	}

	if toGracefullyComplete {
		prcRow.Status = persistence.ProcessExecutionStatusCompleted
		prcRow.IsCurrent = false
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

	return &persistence.CompleteExecuteExecutionResponse{
		HasNewWorkerTask: hasNewWorkerTask,
	}, nil
}

func insertAsyncStateExecution(
	ctx context.Context,
	tx extensions.SQLTransaction,
	processExecutionId uuid.UUID,
	stateId string,
	stateIdSeq int,
	stateConfig *xdbapi.AsyncStateConfig,
	stateInput []byte,
	stateInfo []byte,
) error {
	stateRow := extensions.AsyncStateExecutionRow{
		ProcessExecutionId: processExecutionId,
		StateId:            stateId,
		StateIdSequence:    int32(stateIdSeq),
		// the waitUntil/execute status will be set later

		LastFailure:     nil,
		PreviousVersion: 1,
		Input:           stateInput,
		Info:            stateInfo,
	}

	if stateConfig.GetSkipWaitUntil() {
		stateRow.WaitUntilStatus = persistence.StateExecutionStatusSkipped
		stateRow.ExecuteStatus = persistence.StateExecutionStatusRunning
	} else {
		stateRow.WaitUntilStatus = persistence.StateExecutionStatusRunning
		stateRow.ExecuteStatus = persistence.StateExecutionStatusUndefined
	}

	return tx.InsertAsyncStateExecution(ctx, stateRow)
}

func insertWorkerTask(
	ctx context.Context,
	tx extensions.SQLTransaction,
	processExecutionId uuid.UUID,
	stateId string,
	stateIdSeq int,
	stateConfig *xdbapi.AsyncStateConfig,
	shardId int32,
) error {
	workerTaskRow := extensions.WorkerTaskRowForInsert{
		ShardId:            shardId,
		ProcessExecutionId: processExecutionId,
		StateId:            stateId,
		StateIdSequence:    int32(stateIdSeq),
	}
	if stateConfig.GetSkipWaitUntil() {
		workerTaskRow.TaskType = persistence.WorkerTaskTypeExecute
	} else {
		workerTaskRow.TaskType = persistence.WorkerTaskTypeWaitUntil
	}

	return tx.InsertWorkerTask(ctx, workerTaskRow)
}

func (p sqlProcessStoreImpl) GetTimerTasksUpToTimestamp(
	ctx context.Context, request persistence.GetTimerTasksRequest,
) (*persistence.GetTimerTasksResponse, error) {
	dbTimerTasks, err := p.session.BatchSelectTimerTasks(
		ctx, extensions.TimerTaskRangeSelectFilter{
			ShardId:                         request.ShardId,
			MaxFireTimeUnixSecondsInclusive: request.MaxFireTimestampSecondsInclusive,
			PageSize:                        request.PageSize,
		})
	if err != nil {
		return nil, err
	}
	return createGetTimerTaskResponse(request.ShardId, dbTimerTasks)
}

func (p sqlProcessStoreImpl) BackoffWorkerTask(ctx context.Context, request persistence.BackoffWorkerTaskRequest) error {
	tx, err := p.session.StartTransaction(ctx)
	if err != nil {
		return err
	}

	err = p.doBackoffWorkerTaskTx(ctx, tx, request)
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

func (p sqlProcessStoreImpl) ConvertTimerTaskToWorkerTask(
	ctx context.Context, request persistence.ConvertTimerTaskToWorkerTaskRequest,
) error {
	tx, err := p.session.StartTransaction(ctx)
	if err != nil {
		return err
	}

	err = p.doConvertTimerTaskToWorkerTaskTx(ctx, tx, request)
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

func (p sqlProcessStoreImpl) doConvertTimerTaskToWorkerTaskTx(
	ctx context.Context, tx extensions.SQLTransaction,
	request persistence.ConvertTimerTaskToWorkerTaskRequest,
) error {
	currentTask := request.Task
	timerInfo := currentTask.TimerTaskInfo
	taskInfoBytes, err := persistence.FromWorkerTaskInfoIntoBytes(persistence.WorkerTaskInfoJson{
		WorkerTaskBackoffInfo: timerInfo.WorkerTaskBackoffInfo,
	})
	if err != nil {
		return err
	}

	err = tx.InsertWorkerTask(ctx, extensions.WorkerTaskRowForInsert{
		ShardId:            currentTask.ShardId,
		TaskType:           *timerInfo.WorkerTaskType,
		ProcessExecutionId: currentTask.ProcessExecutionId,
		StateId:            currentTask.StateId,
		StateIdSequence:    currentTask.StateIdSequence,
		Info:               taskInfoBytes,
	})
	if err != nil {
		return err
	}
	return tx.DeleteTimerTask(ctx, extensions.TimerTaskRowDeleteFilter{
		ShardId:              currentTask.ShardId,
		FireTimeUnixSeconds:  currentTask.FireTimestampSeconds,
		TaskSequence:         *currentTask.TaskSequence,
		OptionalPartitionKey: currentTask.OptionalPartitionKey,
	})
}
func (p sqlProcessStoreImpl) doBackoffWorkerTaskTx(
	ctx context.Context, tx extensions.SQLTransaction, request persistence.BackoffWorkerTaskRequest,
) error {
	task := request.Task
	prep := request.Prep

	if task.WorkerTaskInfo.WorkerTaskBackoffInfo == nil {
		return fmt.Errorf("WorkerTaskBackoffInfo cannot be nil")
	}
	failureBytes, err := persistence.CreateStateExecutionFailureBytesForBackoff(
		request.LastFailureStatus, request.LastFailureDetails, task.WorkerTaskInfo.WorkerTaskBackoffInfo.CompletedAttempts)

	if err != nil {
		return err
	}
	err = tx.UpdateAsyncStateExecution(ctx, extensions.AsyncStateExecutionRowForUpdate{
		ProcessExecutionId: task.ProcessExecutionId,
		StateId:            task.StateId,
		StateIdSequence:    task.StateIdSequence,
		WaitUntilStatus:    prep.WaitUntilStatus,
		ExecuteStatus:      prep.ExecuteStatus,
		PreviousVersion:    prep.PreviousVersion,
		LastFailure:        failureBytes,
	})
	if err != nil {
		return err
	}
	timerInfoBytes, err := persistence.CreateTimerTaskInfoBytes(task.WorkerTaskInfo.WorkerTaskBackoffInfo, &task.TaskType)
	if err != nil {
		return err
	}
	err = tx.InsertTimerTask(ctx, extensions.TimerTaskRowForInsert{
		ShardId:             task.ShardId,
		FireTimeUnixSeconds: request.FireTimestampSeconds,
		TaskType:            persistence.TimerTaskTypeWorkerTaskBackoff,
		ProcessExecutionId:  task.ProcessExecutionId,
		StateId:             task.StateId,
		StateIdSequence:     task.StateIdSequence,
		Info:                timerInfoBytes,
	})
	if err != nil {
		return err
	}
	return tx.DeleteWorkerTask(ctx, extensions.WorkerTaskRowDeleteFilter{
		ShardId:      task.ShardId,
		TaskSequence: task.GetTaskSequence(),
		OptionalPartitionKey: &persistence.PartitionKey{
			Namespace: prep.Info.Namespace,
			ProcessId: prep.Info.ProcessId,
		},
	})
}

func (p sqlProcessStoreImpl) GetTimerTasksForTimestamps(
	ctx context.Context, request persistence.GetTimerTasksForTimestampsRequest,
) (*persistence.GetTimerTasksResponse, error) {
	var ts []int64
	for _, req := range request.DetailedRequests {
		ts = append(ts, req.FireTimestamps...)
	}
	dbTimerTasks, err := p.session.SelectTimerTasksForTimestamps(
		ctx, extensions.TimerTaskSelectByTimestampsFilter{
			ShardId:                  request.ShardId,
			FireTimeUnixSeconds:      ts,
			MinTaskSequenceInclusive: request.MinSequenceInclusive,
		})
	if err != nil {
		return nil, err
	}
	return createGetTimerTaskResponse(request.ShardId, dbTimerTasks)
}
