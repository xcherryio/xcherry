package persistence

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/xdblab/xdb-apis/goapi/xdbapi"
	"github.com/xdblab/xdb/common/log"
	"github.com/xdblab/xdb/common/log/tag"
	"github.com/xdblab/xdb/common/ptr"
	"github.com/xdblab/xdb/config"
	"github.com/xdblab/xdb/extensions"
	"github.com/xdblab/xdb/extensions/postgres"
	"time"
)

type ProcessORMSQLImpl struct {
	sqlDB  extensions.SQLDBSession
	logger log.Logger
}

func NewProcessORMSQLImpl(sqlConfig config.SQL, logger log.Logger) (ProcessORM, error) {
	// init() will not work because `postgres` package has not been imported before executing this function
	println(postgres.ExtensionName)

	session, err := extensions.NewSQLSession(&sqlConfig)
	return &ProcessORMSQLImpl{
		sqlDB:  session,
		logger: logger,
	}, err
}

func (p ProcessORMSQLImpl) StartProcess(
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
	exeUUID, err := uuid.NewUUID()
	if err != nil {
		return nil, false, err
	}
	eUUID := exeUUID.String()
	_, err = tx.InsertCurrentProcessExecution(ctx, request.ProcessId, eUUID)
	if err != nil {
		if p.sqlDB.IsDupEntryError(err) {
			// TODO support other ProcessIdReusePolicy on this error
			return nil, true, nil
		}
		return nil, false, err
	}

	timeoutSeconds := 0
	if sc, ok := request.GetProcessStartConfigOk(); ok {
		timeoutSeconds = int(sc.GetTimeoutSeconds())
	}

	info := extensions.ProcessExecutionInfoJson{
		ProcessType: request.GetProcessType(),
		WorkerURL:   request.GetWorkerUrl(),
	}
	infoJ, err := json.Marshal(info)
	if err != nil {
		return nil, false, err
	}

	row := extensions.ProcessExecutionRow{
		ProcessExecutionId:     eUUID,
		ProcessId:              request.ProcessId,
		IsCurrent:              true,
		Status:                 extensions.ExecutionStatusRunning.String(),
		StartTime:              time.Now(),
		TimeoutSeconds:         timeoutSeconds,
		HistoryEventIdSequence: 0,
		Info:                   infoJ,
	}
	_, err = tx.InsertProcessExecution(ctx, row)
	return &xdbapi.ProcessExecutionStartResponse{
		ProcessExecutionId: eUUID,
	}, false, err

}

func (p ProcessORMSQLImpl) DescribeLatestProcess(
	ctx context.Context, request xdbapi.ProcessExecutionDescribeRequest,
) (*xdbapi.ProcessExecutionDescribeResponse, bool, error) {
	rows, err := p.sqlDB.SelectCurrentProcessExecution(ctx, request.GetProcessId())
	if err != nil {
		if p.sqlDB.IsNotFoundError(err) {
			return nil, true, nil
		}
		return nil, false, err
	}
	if len(rows) == 0 {
		return nil, true, nil
	}
	if len(rows) != 1 {
		return nil, false, fmt.Errorf("internal data corruption, more than one row is not expected")
	}

	var info extensions.ProcessExecutionInfoJson
	err = json.Unmarshal(rows[0].Info, &info)
	if err != nil {
		return nil, false, err
	}

	return &xdbapi.ProcessExecutionDescribeResponse{
		ProcessExecutionId: &rows[0].ProcessExecutionId,
		ProcessType:        &info.ProcessType,
		WorkerUrl:          &info.WorkerURL,
		StartTimestamp:     ptr.Any(int32(rows[0].StartTime.Unix())),
	}, false, nil
}

func (p ProcessORMSQLImpl) Close() error {
	return p.sqlDB.Close()
}
