package persistence

import (
	"context"
	"encoding/json"
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
	sqlDB  extensions.SQLDBSession
	logger log.Logger
}

func NewSQLPersistence(sqlConfig config.SQL, logger log.Logger) (Persistence, error) {
	session, err := extensions.NewSQLSession(&sqlConfig)
	return &sqlPersistenceImpl{
		sqlDB:  session,
		logger: logger,
	}, err
}

func (p sqlPersistenceImpl) StartProcess(
	ctx context.Context, request StartProcessRequest,
) (*StartProcessResponse, error) {
	tx, err := p.sqlDB.StartTransaction(ctx)
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
		if p.sqlDB.IsDupEntryError(err) {
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

	processExeInfoBytes, err := extensions.FromStartRequestToProcessInfoBytes(req)
	if err != nil {
		return nil, err
	}

	sequenceMap := map[string]int{}
	if req.StartStateId != nil {
		sequenceMap[req.GetStartStateId()] = 1

		stateInputBytes, err := json.Marshal(req.StartStateInput)
		if err != nil {
			return nil, err
		}

		stateInfoBytes, err := extensions.FromStartRequestToStateInfoBytes(req)
		if err != nil {
			return nil, err
		}

		stateRow := extensions.AsyncStateExecutionRow{
			ProcessExecutionId: prcExeId,
			StateId:            req.GetStartStateId(),
			StateIdSequence:    1,
			// the waitUntil/execute status will be set later

			PreviousVersion: 1,
			Input:           stateInputBytes,
			Info:            stateInfoBytes,
		}

		if req.StartStateConfig.GetSkipWaitUntil() {
			stateRow.WaitUntilStatus = extensions.StateExecutionStatusSkipped
			stateRow.ExecuteStatus = extensions.StateExecutionStatusRunning
		} else {
			stateRow.WaitUntilStatus = extensions.StateExecutionStatusRunning
			stateRow.ExecuteStatus = extensions.StateExecutionStatusUndefined
		}

		err = tx.InsertAsyncStateExecution(ctx, stateRow)
		if err != nil {
			return nil, err
		}

		workerTaskRow := extensions.WorkerTaskRowForInsert{
			ShardId:            extensions.DefaultShardId,
			ProcessExecutionId: prcExeId,
			StateId:            req.GetStartStateId(),
			StateIdSequence:    1,
		}
		if req.StartStateConfig.GetSkipWaitUntil() {
			workerTaskRow.TaskType = extensions.WorkerTaskTypeExecute
		} else {
			workerTaskRow.TaskType = extensions.WorkerTaskTypeWaitUntil
		}

		err = tx.InsertWorkerTask(ctx, workerTaskRow)
		if err != nil {
			return nil, err
		}
	}

	stateIdSequenceBytes, err := extensions.FromSequenceMapToBytes(sequenceMap)
	if err != nil {
		return nil, err
	}

	row := extensions.ProcessExecutionRow{
		ProcessExecutionId: prcExeId,

		IsCurrent:              true,
		Status:                 extensions.ProcessExecutionStatusRunning,
		HistoryEventIdSequence: 0,
		StateIdSequence:        stateIdSequenceBytes,
		Namespace:              req.Namespace,
		ProcessId:              req.ProcessId,

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
	row, err := p.sqlDB.SelectCurrentProcessExecution(ctx, request.Namespace, request.ProcessId)
	if err != nil {
		if p.sqlDB.IsNotFoundError(err) {
			return &DescribeLatestProcessResponse{
				NotExists: true,
			}, nil
		}
		return nil, err
	}

	info, err := extensions.BytesToProcessExecutionInfo(row.Info)

	return &DescribeLatestProcessResponse{
		Response: &xdbapi.ProcessExecutionDescribeResponse{
			ProcessExecutionId: ptr.Any(row.ProcessExecutionId.String()),
			ProcessType:        &info.ProcessType,
			WorkerUrl:          &info.WorkerURL,
			StartTimestamp:     ptr.Any(int32(row.StartTime.Unix())),
		},
	}, nil
}

func (p sqlPersistenceImpl) Close() error {
	return p.sqlDB.Close()
}
