package extensions

import (
	"context"
	"database/sql"
	"github.com/xdblab/xdb/config"
)

type SQLDBExtension interface {
	// StartDBSession starts the session for regular business logic
	StartDBSession(cfg *config.SQL) (SQLDBSession, error)
	// StartAdminDBSession starts the session for admin operation like DDL
	StartAdminDBSession(cfg *config.SQL) (SQLAdminDBSession, error)
}

type SQLDBSession interface {
	processExecutionNonTxnCRUD

	ErrorChecker
	StartTransaction(ctx context.Context) (SQLTransaction, error)
	Close() error
}

type SQLTransaction interface {
	processExecutionTxnCRUD
	Commit() error
	Rollback() error
}

type SQLAdminDBSession interface {
	CreateDatabase(ctx context.Context, database string) error
	DropDatabase(ctx context.Context, database string) error
	ExecuteSchemaDDL(ctx context.Context, ddlQuery string) error
	Close() error
}

type processExecutionTxnCRUD interface {
	InsertCurrentProcessExecution(ctx context.Context, row CurrentProcessExecutionRow) (sql.Result, error)
	InsertProcessExecution(ctx context.Context, row ProcessExecutionRow) (sql.Result, error)
	SelectProcessExecutionForUpdate(ctx context.Context, processExecutionId string)
	InsertStateExecution(ctx context.Context, row AsyncStateExecutionRow) (sql.Result, error)
	InsertWorkerTask(ctx context.Context, row WorkerTaskRowForInsert) (sql.Result, error)
	DeleteWorkerTask(ctx context.Context, filter WorkerTaskRowDeleteFilter) (sql.Result, error)
}

type processExecutionNonTxnCRUD interface {
	SelectCurrentProcessExecution(ctx context.Context, namespace string, processId string) ([]ProcessExecutionRow, error)
}

type ErrorChecker interface {
	IsDupEntryError(err error) bool
	IsNotFoundError(err error) bool
	IsTimeoutError(err error) bool
	IsThrottlingError(err error) bool
}
