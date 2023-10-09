// Apache License 2.0

// Copyright (c) XDBLab organization

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package sql

import (
	"context"
	"fmt"
	"github.com/xdblab/xdb-apis/goapi/xdbapi"
	"github.com/xdblab/xdb/common/log"
	"github.com/xdblab/xdb/common/log/tag"
	"github.com/xdblab/xdb/common/ptr"
	"github.com/xdblab/xdb/common/uuid"
	"github.com/xdblab/xdb/config"
	"github.com/xdblab/xdb/extensions"
	"github.com/xdblab/xdb/persistence"
	"time"
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

func (p sqlProcessStoreImpl) doStartProcessTx(
	ctx context.Context, tx extensions.SQLTransaction, request persistence.StartProcessRequest,
) (*persistence.StartProcessResponse, error) {
	req := request.Request
	prcExeId := uuid.MustNewUUID()
	hasNewWorkerTask := false

	err := tx.InsertCurrentProcessExecution(ctx, extensions.CurrentProcessExecutionRow{
		Namespace:          req.Namespace,
		ProcessId:          req.ProcessId,
		ProcessExecutionId: prcExeId,
	})

	if err != nil {
		if p.session.IsDupEntryError(err) {
			// TODO support other ProcessIdReusePolicy on this error
			return &persistence.StartProcessResponse{
				AlreadyStarted: true,
			}, nil
		}
		return nil, err
	}

	timeoutSeconds := int32(0)
	if sc, ok := req.GetProcessStartConfigOk(); ok {
		timeoutSeconds = sc.GetTimeoutSeconds()
	}

	processExeInfoBytes, err := persistence.FromStartRequestToProcessInfoBytes(req)
	if err != nil {
		return nil, err
	}

	sequenceMaps := persistence.NewStateExecutionSequenceMaps()
	if req.StartStateId != nil {
		stateId := req.GetStartStateId()
		stateIdSeq := sequenceMaps.StartNewStateExecution(req.GetStartStateId())
		stateConfig := req.StartStateConfig

		stateInputBytes, err := persistence.FromEncodedObjectIntoBytes(req.StartStateInput)
		if err != nil {
			return nil, err
		}

		stateInfoBytes, err := persistence.FromStartRequestToStateInfoBytes(req)
		if err != nil {
			return nil, err
		}

		err = insertAsyncStateExecution(ctx, tx, prcExeId, stateId, stateIdSeq, stateConfig, stateInputBytes, stateInfoBytes)
		if err != nil {
			return nil, err
		}

		err = insertWorkerTask(ctx, tx, prcExeId, stateId, 1, stateConfig, request.NewTaskShardId)
		if err != nil {
			return nil, err
		}

		hasNewWorkerTask = true
	}

	sequenceMapsBytes, err := sequenceMaps.ToBytes()
	if err != nil {
		return nil, err
	}

	row := extensions.ProcessExecutionRow{
		ProcessExecutionId: prcExeId,

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
	return &persistence.StartProcessResponse{
		ProcessExecutionId: prcExeId,
		AlreadyStarted:     false,
		HasNewWorkerTask:   hasNewWorkerTask,
	}, err
}

func (p sqlProcessStoreImpl) DescribeLatestProcess(
	ctx context.Context, request persistence.DescribeLatestProcessRequest,
) (*persistence.DescribeLatestProcessResponse, error) {
	row, err := p.session.SelectCurrentProcessExecution(ctx, request.Namespace, request.ProcessId)
	if err != nil {
		if p.session.IsNotFoundError(err) {
			return &persistence.DescribeLatestProcessResponse{
				NotExists: true,
			}, nil
		}
		return nil, err
	}

	info, err := persistence.BytesToProcessExecutionInfo(row.Info)
	if err != nil {
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
		tasks = append(tasks, persistence.WorkerTask{
			ShardId:            request.ShardId,
			TaskSequence:       ptr.Any(t.TaskSequence),
			TaskType:           t.TaskType,
			ProcessExecutionId: t.ProcessExecutionId,
			StateExecutionId: persistence.StateExecutionId{
				StateId:         t.StateId,
				StateIdSequence: t.StateIdSequence,
			},
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

	currStateRow := extensions.AsyncStateExecutionRowForUpdate{
		ProcessExecutionId: request.ProcessExecutionId,
		StateId:            request.StateId,
		StateIdSequence:    request.StateIdSequence,
		WaitUntilStatus:    request.Prepare.WaitUntilStatus,
		ExecuteStatus:      persistence.StateExecutionStatusCompleted,
		PreviousVersion:    request.Prepare.PreviousVersion,
	}

	err := tx.UpdateAsyncStateExecution(ctx, currStateRow)
	if err != nil {
		if p.session.IsConditionalUpdateFailure(err) {
			p.logger.Warn("UpdateAsyncStateExecution failed at conditional update")
		}
		return nil, err
	}

	threadDecision := request.StateDecision.GetThreadCloseDecision()
	if request.StateDecision.HasThreadCloseDecision() {
		if threadDecision.GetCloseType() == xdbapi.DEAD_END {
			return &persistence.CompleteExecuteExecutionResponse{}, nil
		}
	}

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

	err = sequenceMaps.CompleteNewStateExecution(request.StateId, int(request.StateIdSequence))
	if err != nil {
		return nil, fmt.Errorf("completing a non-existing state execution, maybe data is corrupted %v-%v, currentMap:%v, err:%w",
			request.StateId, request.StateIdSequence, sequenceMaps, err)
	}

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

		// finally update process execution row
		seqJ, err := sequenceMaps.ToBytes()
		if err != nil {
			return nil, err
		}
		prcRow.StateExecutionSequenceMaps = seqJ
		err = tx.UpdateProcessExecution(ctx, *prcRow)
		if err != nil {
			return nil, err
		}
		return &persistence.CompleteExecuteExecutionResponse{
			HasNewWorkerTask: hasNewWorkerTask,
		}, nil
	}

	// otherwise close the thread
	if threadDecision.GetCloseType() != xdbapi.FORCE_COMPLETE_PROCESS {
		return nil, fmt.Errorf("cannot support close type: %v", threadDecision.GetCloseType())
	}

	// also stop(abort) other running state executions
	for stateId, stateIdSeqMap := range sequenceMaps.PendingExecutionMap {
		for stateIdSeq := range stateIdSeqMap {
			stateRow, err := tx.SelectAsyncStateExecutionForUpdate(
				ctx, extensions.AsyncStateExecutionSelectFilter{
					ProcessExecutionId: request.ProcessExecutionId,
					StateId:            stateId,
					StateIdSequence:    int32(stateIdSeq),
				})
			if err != nil {
				return nil, err
			}
			if stateRow.WaitUntilStatus == persistence.StateExecutionStatusRunning {
				stateRow.WaitUntilStatus = persistence.StateExecutionStatusAborted
			}
			if stateRow.ExecuteStatus == persistence.StateExecutionStatusRunning {
				stateRow.ExecuteStatus = persistence.StateExecutionStatusAborted
			}
			err = tx.UpdateAsyncStateExecution(ctx, extensions.AsyncStateExecutionRowForUpdate{
				ProcessExecutionId: stateRow.ProcessExecutionId,
				StateId:            stateRow.StateId,
				StateIdSequence:    stateRow.StateIdSequence,
				WaitUntilStatus:    stateRow.WaitUntilStatus,
				ExecuteStatus:      stateRow.ExecuteStatus,
				PreviousVersion:    stateRow.PreviousVersion,
			})
			if err != nil {
				return nil, err
			}
			err = sequenceMaps.CompleteNewStateExecution(stateId, stateIdSeq)
			if err != nil {
				return nil, err
			}
		}
	}

	// update process execution row
	prcRow.Status = persistence.ProcessExecutionStatusCompleted
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
	stateInfo []byte) error {
	stateRow := extensions.AsyncStateExecutionRow{
		ProcessExecutionId: processExecutionId,
		StateId:            stateId,
		StateIdSequence:    int32(stateIdSeq),
		// the waitUntil/execute status will be set later

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
	shardId int32) error {
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
