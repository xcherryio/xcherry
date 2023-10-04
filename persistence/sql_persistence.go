package persistence

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
	"time"
)

type sqlPersistenceImpl struct {
	session extensions.SQLDBSession
	logger  log.Logger
}

func NewSQLPersistence(sqlConfig config.SQL, logger log.Logger) (ProcessStore, error) {
	session, err := extensions.NewSQLSession(&sqlConfig)
	return &sqlPersistenceImpl{
		session: session,
		logger:  logger,
	}, err
}

func (p sqlPersistenceImpl) Close() error {
	return p.session.Close()
}

func (p sqlPersistenceImpl) StartProcess(
	ctx context.Context, request StartProcessRequest,
) (*StartProcessResponse, error) {
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

func (p sqlPersistenceImpl) doStartProcessTx(
	ctx context.Context, tx extensions.SQLTransaction, request StartProcessRequest,
) (*StartProcessResponse, error) {
	req := request.Request
	prcExeId := uuid.MustNewUUID()

	err := tx.InsertCurrentProcessExecution(ctx, extensions.CurrentProcessExecutionRow{
		Namespace:          req.Namespace,
		ProcessId:          req.ProcessId,
		ProcessExecutionId: prcExeId,
	})

	if err != nil {
		if p.session.IsDupEntryError(err) {
			// TODO support other ProcessIdReusePolicy on this error
			return &StartProcessResponse{
				AlreadyStarted: true,
			}, nil
		}
		return nil, err
	}

	timeoutSeconds := int32(0)
	if sc, ok := req.GetProcessStartConfigOk(); ok {
		timeoutSeconds = sc.GetTimeoutSeconds()
	}

	processExeInfoBytes, err := FromStartRequestToProcessInfoBytes(req)
	if err != nil {
		return nil, err
	}

	sequenceMaps := NewStateExecutionSequenceMaps()
	if req.StartStateId != nil {
		stateIdSeq := sequenceMaps.StartNewStateExecution(req.GetStartStateId())

		stateInputBytes, err := FromEncodedObjectIntoBytes(req.StartStateInput)
		if err != nil {
			return nil, err
		}

		stateInfoBytes, err := FromStartRequestToStateInfoBytes(req)
		if err != nil {
			return nil, err
		}

		stateRow := extensions.AsyncStateExecutionRow{
			ProcessExecutionId: prcExeId,
			StateId:            req.GetStartStateId(),
			StateIdSequence:    int32(stateIdSeq),
			// the waitUntil/execute status will be set later

			PreviousVersion: 1,
			Input:           stateInputBytes,
			Info:            stateInfoBytes,
		}

		if req.StartStateConfig.GetSkipWaitUntil() {
			stateRow.WaitUntilStatus = StateExecutionStatusSkipped
			stateRow.ExecuteStatus = StateExecutionStatusRunning
		} else {
			stateRow.WaitUntilStatus = StateExecutionStatusRunning
			stateRow.ExecuteStatus = StateExecutionStatusUndefined
		}

		err = tx.InsertAsyncStateExecution(ctx, stateRow)
		if err != nil {
			return nil, err
		}

		workerTaskRow := extensions.WorkerTaskRowForInsert{
			ShardId:            DefaultShardId,
			ProcessExecutionId: prcExeId,
			StateId:            req.GetStartStateId(),
			StateIdSequence:    1,
		}
		if req.StartStateConfig.GetSkipWaitUntil() {
			workerTaskRow.TaskType = WorkerTaskTypeExecute
		} else {
			workerTaskRow.TaskType = WorkerTaskTypeWaitUntil
		}

		err = tx.InsertWorkerTask(ctx, workerTaskRow)
		if err != nil {
			return nil, err
		}
	}

	sequenceMapsBytes, err := sequenceMaps.ToBytes()
	if err != nil {
		return nil, err
	}

	row := extensions.ProcessExecutionRow{
		ProcessExecutionId: prcExeId,

		IsCurrent:                  true,
		Status:                     ProcessExecutionStatusRunning,
		HistoryEventIdSequence:     0,
		StateExecutionSequenceMaps: sequenceMapsBytes,
		Namespace:                  req.Namespace,
		ProcessId:                  req.ProcessId,

		StartTime:      time.Now(),
		TimeoutSeconds: timeoutSeconds,

		Info: processExeInfoBytes,
	}

	err = tx.InsertProcessExecution(ctx, row)
	return &StartProcessResponse{
		ProcessExecutionId: prcExeId.String(),
		AlreadyStarted:     false,
	}, err
}

func (p sqlPersistenceImpl) DescribeLatestProcess(
	ctx context.Context, request DescribeLatestProcessRequest,
) (*DescribeLatestProcessResponse, error) {
	row, err := p.session.SelectCurrentProcessExecution(ctx, request.Namespace, request.ProcessId)
	if err != nil {
		if p.session.IsNotFoundError(err) {
			return &DescribeLatestProcessResponse{
				NotExists: true,
			}, nil
		}
		return nil, err
	}

	info, err := BytesToProcessExecutionInfo(row.Info)

	return &DescribeLatestProcessResponse{
		Response: &xdbapi.ProcessExecutionDescribeResponse{
			ProcessExecutionId: ptr.Any(row.ProcessExecutionId.String()),
			ProcessType:        &info.ProcessType,
			WorkerUrl:          &info.WorkerURL,
			StartTimestamp:     ptr.Any(int32(row.StartTime.Unix())),
		},
	}, nil
}

func (p sqlPersistenceImpl) GetWorkerTasks(ctx context.Context, request GetWorkerTasksRequest) (*GetWorkerTasksResponse, error) {
	workerTasks, err := p.session.BatchSelectWorkerTasks(
		ctx, request.ShardId, request.StartSequenceInclusive, request.PageSize)
	if err != nil {
		return nil, err
	}
	var tasks []WorkerTask
	for _, t := range workerTasks {
		tasks = append(tasks, WorkerTask{
			ShardId:            request.ShardId,
			TaskSequence:       ptr.Any(t.TaskSequence),
			TaskType:           t.TaskType,
			ProcessExecutionId: t.ProcessExecutionId,
			StateExecutionId: StateExecutionId{
				StateId:         t.StateId,
				StateIdSequence: t.StateIdSequence,
			},
		})
	}
	resp := &GetWorkerTasksResponse{
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

func (p sqlPersistenceImpl) DeleteWorkerTasks(ctx context.Context, request DeleteWorkerTasksRequest) error {
	return p.session.BatchDeleteWorkerTask(ctx, extensions.WorkerTaskRangeDeleteFilter{
		ShardId:                  request.ShardId,
		MinTaskSequenceInclusive: request.MinTaskSequenceInclusive,
		MaxTaskSequenceInclusive: request.MaxTaskSequenceInclusive,
	})
}

func (p sqlPersistenceImpl) PrepareStateExecution(ctx context.Context, request PrepareStateExecutionRequest) (*PrepareStateExecutionResponse, error) {
	stateRow, err := p.session.SelectAsyncStateExecutionForUpdate(
		ctx, extensions.AsyncStateExecutionSelectFilter{
			ProcessExecutionId: request.ProcessExecutionId,
			StateId:            request.StateId,
			StateIdSequence:    request.StateIdSequence,
		})
	if err != nil {
		return nil, err
	}
	info, err := BytesToAsyncStateExecutionInfo(stateRow.Info)
	if err != nil {
		return nil, err
	}
	input, err := BytesToEncodedObject(stateRow.Input)
	if err != nil {
		return nil, err
	}
	return &PrepareStateExecutionResponse{
		WaitUntilStatus: stateRow.WaitUntilStatus,
		ExecuteStatus:   stateRow.ExecuteStatus,
		PreviousVersion: stateRow.PreviousVersion,
		Info:            info,
		Input:           input,
	}, nil
}

func (p sqlPersistenceImpl) CompleteWaitUntilExecution(
	ctx context.Context, request CompleteWaitUntilExecutionRequest,
) (*CompleteWaitUntilExecutionResponse, error) {
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

func (p sqlPersistenceImpl) doCompleteWaitUntilExecutionTx(
	ctx context.Context, tx extensions.SQLTransaction, request CompleteWaitUntilExecutionRequest,
) (*CompleteWaitUntilExecutionResponse, error) {
	stateRow := extensions.AsyncStateExecutionRowForUpdate{
		ProcessExecutionId: request.ProcessExecutionId,
		StateId:            request.StateId,
		StateIdSequence:    request.StateIdSequence,
		WaitUntilStatus:    StateExecutionStatusCompleted,
		ExecuteStatus:      StateExecutionStatusRunning,
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
		TaskType:           WorkerTaskTypeExecute,
		ProcessExecutionId: request.ProcessExecutionId,
		StateId:            request.StateId,
		StateIdSequence:    request.StateIdSequence,
	})
	if err != nil {
		return nil, err
	}
	return &CompleteWaitUntilExecutionResponse{
		HasNewWorkerTask: true,
	}, nil
}

func (p sqlPersistenceImpl) CompleteExecuteExecution(
	ctx context.Context, request CompleteExecuteExecutionRequest,
) (*CompleteExecuteExecutionResponse, error) {

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

func (p sqlPersistenceImpl) doCompleteExecuteExecutionTx(
	ctx context.Context, tx extensions.SQLTransaction, request CompleteExecuteExecutionRequest,
) (*CompleteExecuteExecutionResponse, error) {
	hasNewWorkerTask := false

	currStateRow := extensions.AsyncStateExecutionRowForUpdate{
		ProcessExecutionId: request.ProcessExecutionId,
		StateId:            request.StateId,
		StateIdSequence:    request.StateIdSequence,
		WaitUntilStatus:    request.Prepare.WaitUntilStatus,
		ExecuteStatus:      StateExecutionStatusCompleted,
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
			return nil, nil
		}
	}

	// at this point, it's either going to next states or closing the process
	// either will require to do transaction on process execution row
	prcRow, err := tx.SelectProcessExecutionForUpdate(ctx, request.ProcessExecutionId)
	if err != nil {
		return nil, err
	}

	sequenceMaps, err := NewStateExecutionSequenceMapsFromBytes(prcRow.StateExecutionSequenceMaps)
	if err != nil {
		return nil, err
	}

	if len(request.StateDecision.GetNextStates()) > 0 {
		hasNewWorkerTask = true

		// reuse the info from last state execution as it won't change
		stateInfo, err := FromAsyncStateExecutionInfoToBytes(request.Prepare.Info)
		if err != nil {
			return nil, err
		}

		for _, next := range request.StateDecision.GetNextStates() {
			stateIdSeq := sequenceMaps.StartNewStateExecution(next.StateId)

			stateInput, err := FromEncodedObjectIntoBytes(next.StateInput)
			if err != nil {
				return nil, err
			}

			newStateRow := extensions.AsyncStateExecutionRow{
				ProcessExecutionId: request.ProcessExecutionId,
				StateId:            next.StateId,
				StateIdSequence:    int32(stateIdSeq),
				PreviousVersion:    1,
				Input:              stateInput,
				Info:               stateInfo,
			}

			if next.StateConfig.GetSkipWaitUntil() {
				newStateRow.WaitUntilStatus = StateExecutionStatusSkipped
				newStateRow.ExecuteStatus = StateExecutionStatusRunning
			} else {
				newStateRow.WaitUntilStatus = StateExecutionStatusRunning
				newStateRow.ExecuteStatus = StateExecutionStatusUndefined
			}

			err = tx.InsertAsyncStateExecution(ctx, newStateRow)
			if err != nil {
				return nil, err
			}

			workerTaskRow := extensions.WorkerTaskRowForInsert{
				ShardId:            request.TaskShardId,
				ProcessExecutionId: request.ProcessExecutionId,
				StateId:            next.StateId,
				StateIdSequence:    int32(stateIdSeq),
			}
			if next.StateConfig.GetSkipWaitUntil() {
				workerTaskRow.TaskType = WorkerTaskTypeExecute
			} else {
				workerTaskRow.TaskType = WorkerTaskTypeWaitUntil
			}

			err = tx.InsertWorkerTask(ctx, workerTaskRow)
			if err != nil {
				return nil, err
			}
		}

		// finally update process execution row
		seqJ, err := sequenceMaps.ToBytes()
		if err != nil {
			return nil, err
		}
		err = tx.UpdateProcessExecution(ctx, *prcRow)
		if err != nil {
			return nil, err
		}
		prcRow.StateExecutionSequenceMaps = seqJ
		return &CompleteExecuteExecutionResponse{
			HasNewWorkerTask: hasNewWorkerTask,
		}, nil
	} else {
		// close the thread
		if threadDecision.GetCloseType() != xdbapi.FORCE_COMPLETE_PROCESS {
			return nil, fmt.Errorf("cannot support close type: %v", threadDecision.GetCloseType())
		}

		// also stop(abort) other running state executions
		for stateId, stateIdSeqMap := range sequenceMaps.PendingExecutionMap {
			for stateIdSeq := range stateIdSeqMap {
				stateRow, err := tx.SelectAsyncStateExecutionForUpdate(ctx, extensions.AsyncStateExecutionSelectFilter{
					ProcessExecutionId: request.ProcessExecutionId,
					StateId:            stateId,
					StateIdSequence:    int32(stateIdSeq),
				})
				if err != nil {
					return nil, err
				}
				if stateRow.WaitUntilStatus == StateExecutionStatusRunning {
					stateRow.WaitUntilStatus = StateExecutionStatusAborted
				}
				if stateRow.ExecuteStatus == StateExecutionStatusRunning {
					stateRow.ExecuteStatus = StateExecutionStatusAborted
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
		prcRow.Status = ProcessExecutionStatusCompleted
		prcRow.StateExecutionSequenceMaps, err = sequenceMaps.ToBytes()
		if err != nil {
			return nil, err
		}
		err = tx.UpdateProcessExecution(ctx, *prcRow)
		if err != nil {
			return nil, err
		}
		return &CompleteExecuteExecutionResponse{
			HasNewWorkerTask: hasNewWorkerTask,
		}, nil
	}
}
