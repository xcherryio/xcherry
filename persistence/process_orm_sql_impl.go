package persistence

import (
	"context"
	"github.com/google/uuid"
	"github.com/xdblab/xdb-apis/goapi/xdbapi"
	"github.com/xdblab/xdb/common/ptr"
	"github.com/xdblab/xdb/config"
	"github.com/xdblab/xdb/extensions"
	"time"
)

type ProcessORMSQLImpl struct {
	sqlDB extensions.SQLDBSession
}

func NewProcessORMSQLImpl(sqlConfig config.SQL) (ProcessORM, error) {
	session, err := extensions.NewSQLSession(&sqlConfig)
	return &ProcessORMSQLImpl{
		sqlDB: session,
	}, err
}

func (p ProcessORMSQLImpl) StartProcess(ctx context.Context, request xdbapi.ProcessExecutionStartRequest) (*xdbapi.ProcessExecutionStartResponse, bool, error) {
	tx, err := p.sqlDB.StartTransaction(ctx)
	if err != nil {
		return nil, false, err
	}
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

	row := extensions.ProcessExecutionRow{
		Id:                     eUUID,
		ProcessId:              request.ProcessId,
		Status:                 extensions.ExecutionStatusRunning.String(),
		StartTime:              time.Now(),
		TimeoutSeconds:         timeoutSeconds,
		HistoryEventIdSequence: 0,
		Info: extensions.ProcessExecutionInfo{
			ProcessType: request.GetProcessType(),
			WorkerURL:   request.GetWorkerUrl(),
		},
	}
	_, err = tx.InsertProcessExecution(ctx, row)
	return &xdbapi.ProcessExecutionStartResponse{
		ProcessExecutionId: eUUID,
	}, false, err

}

func (p ProcessORMSQLImpl) DescribeLatestProcess(ctx context.Context, request xdbapi.ProcessExecutionDescribeRequest) (*xdbapi.ProcessExecutionDescribeResponse, bool, error) {
	row, err := p.sqlDB.SelectCurrentProcessExecution(ctx, request.GetProcessId())
	if err != nil {
		if p.sqlDB.IsNotFoundError(err) {
			return nil, true, nil
		}
		return nil, false, err
	}
	return &xdbapi.ProcessExecutionDescribeResponse{
		ProcessExecutionId: &row.Id,
		ProcessType:        &row.Info.ProcessType,
		WorkerUrl:          &row.Info.WorkerURL,
		StartTimestamp:     ptr.Any(int32(row.StartTime.Unix())),
	}, false, nil
}
