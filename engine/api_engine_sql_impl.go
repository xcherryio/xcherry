package engine

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

type APIEngineSQLImpl struct {
	sqlDB  extensions.SQLDBSession
	logger log.Logger
}

func NewAPIEngineSQLImpl(sqlConfig config.SQL, logger log.Logger) (APIEngine, error) {
	session, err := extensions.NewSQLSession(&sqlConfig)
	return &APIEngineSQLImpl{
		sqlDB:  session,
		logger: logger,
	}, err
}

func (p APIEngineSQLImpl) StartProcess(
	ctx context.Context, request xdbapi.ProcessExecutionStartRequest,
) (resp *xdbapi.ProcessExecutionStartResponse, alreadyStarted bool, err error) {
	tx, err := p.sqlDB.StartTransaction(ctx)
	if err != nil {
		return nil, false, err
	}
	defer func() {
		if alreadyStarted || err != nil {
			err2 := tx.Rollback()
			if err2 != nil {
				p.logger.Error("error on rollback transaction", tag.Error(err2))
			}
		} else {
			// at here, err must be nil, so we can safely override it and return to caller
			err2 := tx.Commit()
			if err2 != nil {
				err = err2
				p.logger.Error("error on committing transaction", tag.Error(err))
			}
		}
	}()
	prcExeId := uuid.MustNewUUID()
	if err != nil {
		return nil, false, err
	}
	err = tx.InsertCurrentProcessExecution(ctx, extensions.CurrentProcessExecutionRow{
		Namespace:          request.Namespace,
		ProcessId:          request.ProcessId,
		ProcessExecutionId: prcExeId,
	})
	if err != nil {
		if p.sqlDB.IsDupEntryError(err) {
			// TODO support other ProcessIdReusePolicy on this error
			return nil, true, nil
		}
		return nil, false, err
	}

	timeoutSeconds := int32(0)
	if sc, ok := request.GetProcessStartConfigOk(); ok {
		timeoutSeconds = sc.GetTimeoutSeconds()
	}

	processExeInfo, err := json.Marshal(extensions.ProcessExecutionInfoJson{
		ProcessType: request.GetProcessType(),
		WorkerURL:   request.GetWorkerUrl(),
	})
	if err != nil {
		return nil, false, err
	}

	sequenceMap := map[string]int{}
	if request.StartStateId != nil {
		sequenceMap[request.GetStartStateId()] = 1

		stateInput, err := json.Marshal(request.StartStateInput)
		if err != nil {
			return nil, false, err
		}

		stateExeInfo, err := json.Marshal(extensions.AsyncStateExecutionInfoJson{
			ProcessType: request.GetProcessType(),
			WorkerURL:   request.GetWorkerUrl(),
		})
		if err != nil {
			return nil, false, err
		}

		stateRow := extensions.AsyncStateExecutionRow{
			ProcessExecutionId: prcExeId,
			StateId:            request.GetStartStateId(),
			StateIdSequence:    1,
			PreviousVersion:    1,
			Input:              stateInput,
			Info:               stateExeInfo,
		}
		if request.StartStateConfig.GetSkipWaitUntil() {
			stateRow.WaitUntilStatus = extensions.StateExecutionStatusSkipped
			stateRow.ExecuteStatus = extensions.StateExecutionStatusRunning
		} else {
			stateRow.WaitUntilStatus = extensions.StateExecutionStatusRunning
			stateRow.ExecuteStatus = extensions.StateExecutionStatusUndefined
		}

		err = tx.InsertAsyncStateExecution(ctx, stateRow)
		if err != nil {
			return nil, false, err
		}

		workerTaskRow := extensions.WorkerTaskRowForInsert{
			ShardId:            extensions.DefaultShardId,
			ProcessExecutionId: prcExeId,
			StateId:            request.GetStartStateId(),
			StateIdSequence:    1,
		}
		if request.StartStateConfig.GetSkipWaitUntil() {
			workerTaskRow.TaskType = extensions.WorkerTaskTypeExecute
		} else {
			workerTaskRow.TaskType = extensions.WorkerTaskTypeWaitUntil
		}
		
		err = tx.InsertWorkerTask(ctx, workerTaskRow)
		if err != nil {
			return nil, false, err
		}
	}

	stateIdSequence, err := json.Marshal(extensions.StateExecutionIdSequenceJson{
		SequenceMap: sequenceMap,
	})
	if err != nil {
		return nil, false, err
	}

	row := extensions.ProcessExecutionRow{
		ProcessExecutionId: prcExeId,

		IsCurrent:              true,
		Status:                 extensions.ProcessExecutionStatusRunning,
		HistoryEventIdSequence: 0,
		StateIdSequence:        stateIdSequence,
		Namespace:              request.Namespace,
		ProcessId:              request.ProcessId,

		StartTime:      time.Now(),
		TimeoutSeconds: timeoutSeconds,

		Info: processExeInfo,
	}
	err = tx.InsertProcessExecution(ctx, row)
	return &xdbapi.ProcessExecutionStartResponse{
		ProcessExecutionId: prcExeId.String(),
	}, false, err
}

func (p APIEngineSQLImpl) DescribeLatestProcess(
	ctx context.Context, request xdbapi.ProcessExecutionDescribeRequest,
) (*xdbapi.ProcessExecutionDescribeResponse, bool, error) {
	row, err := p.sqlDB.SelectCurrentProcessExecution(ctx, request.GetNamespace(), request.GetProcessId())
	if err != nil {
		if p.sqlDB.IsNotFoundError(err) {
			return nil, true, nil
		}
		return nil, false, err
	}

	var info extensions.ProcessExecutionInfoJson
	err = json.Unmarshal(row.Info, &info)
	if err != nil {
		return nil, false, err
	}

	return &xdbapi.ProcessExecutionDescribeResponse{
		ProcessExecutionId: ptr.Any(row.ProcessExecutionId.String()),
		ProcessType:        &info.ProcessType,
		WorkerUrl:          &info.WorkerURL,
		StartTimestamp:     ptr.Any(int32(row.StartTime.Unix())),
	}, false, nil
}

func (p APIEngineSQLImpl) Close() error {
	return p.sqlDB.Close()
}
