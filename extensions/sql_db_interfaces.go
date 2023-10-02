package extensions

import (
	"context"
	"github.com/xdblab/xdb/config"
	"github.com/xdblab/xdb/persistence/uuid"
)

type SQLDBExtension interface {
	// StartDBSession starts the session for regular business logic
	StartDBSession(cfg *config.SQL) (SQLDBSession, error)
	// StartAdminDBSession starts the session for admin operation like DDL
	StartAdminDBSession(cfg *config.SQL) (SQLAdminDBSession, error)
}

type SQLDBSession interface {
	nonTransactionalCRUD

	ErrorChecker
	StartTransaction(ctx context.Context) (SQLTransaction, error)
	Close() error
}

type SQLTransaction interface {
	transactionalCRUD
	Commit() error
	Rollback() error
}

type SQLAdminDBSession interface {
	CreateDatabase(ctx context.Context, database string) error
	DropDatabase(ctx context.Context, database string) error
	ExecuteSchemaDDL(ctx context.Context, ddlQuery string) error
	Close() error
}

type transactionalCRUD interface {
	InsertCurrentProcessExecution(ctx context.Context, row CurrentProcessExecutionRow) error

	InsertProcessExecution(ctx context.Context, row ProcessExecutionRow) error
	SelectProcessExecutionForUpdate(ctx context.Context, processExecutionId uuid.UUID) (*ProcessExecutionRowForUpdate, error)
	UpdateProcessExecution(ctx context.Context, row ProcessExecutionRowForUpdate) error

	InsertAsyncStateExecution(ctx context.Context, row AsyncStateExecutionRow) error
	UpdateAsyncStateExecution(ctx context.Context, row AsyncStateExecutionRowForUpdate) error

	InsertWorkerTask(ctx context.Context, row WorkerTaskRowForInsert) error
}

type nonTransactionalCRUD interface {
	SelectCurrentProcessExecution(ctx context.Context, namespace string, processId string) (*ProcessExecutionRow, error)
	SelectAsyncStateExecutionForUpdate(ctx context.Context, filter AsyncStateExecutionSelectFilter) (*AsyncStateExecutionRowForUpdate, error)

	BatchSelectWorkerTasksOfFirstPage(ctx context.Context, shardId, pageSize int32) ([]WorkerTaskRow, error)
	BatchDeleteWorkerTask(ctx context.Context, filter WorkerTaskRangeDeleteFilter) error
}

type ErrorChecker interface {
	IsDupEntryError(err error) bool
	IsNotFoundError(err error) bool
	IsTimeoutError(err error) bool
	IsThrottlingError(err error) bool
	IsConflictError(err error) bool
}
