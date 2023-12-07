// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: BUSL-1.1

package extensions

import (
	"context"
	"database/sql"
	"github.com/xcherryio/apis/goapi/xcapi"

	"github.com/xcherryio/xcherry/common/uuid"
	"github.com/xcherryio/xcherry/config"
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
	StartTransaction(ctx context.Context, opts *sql.TxOptions) (SQLTransaction, error)
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
	InsertLatestProcessExecution(ctx context.Context, row LatestProcessExecutionRow) error
	SelectLatestProcessExecutionForUpdate(
		ctx context.Context, namespace string, processId string,
	) (*LatestProcessExecutionRow, bool, error)
	UpdateLatestProcessExecution(ctx context.Context, row LatestProcessExecutionRow) error

	InsertProcessExecution(ctx context.Context, row ProcessExecutionRow) error
	SelectProcessExecutionForUpdate(
		ctx context.Context, processExecutionId uuid.UUID,
	) (*ProcessExecutionRowForUpdate, error)
	SelectProcessExecution(ctx context.Context, processExecutionId uuid.UUID) (*ProcessExecutionRow, error)
	UpdateProcessExecution(ctx context.Context, row ProcessExecutionRowForUpdate) error

	InsertAsyncStateExecution(ctx context.Context, row AsyncStateExecutionRow) error
	SelectAsyncStateExecutionForUpdate(
		ctx context.Context, filter AsyncStateExecutionSelectFilter,
	) (*AsyncStateExecutionRowForUpdate, error)
	UpdateAsyncStateExecution(ctx context.Context, row AsyncStateExecutionRowForUpdate) error
	UpdateAsyncStateExecutionWithoutCommands(
		ctx context.Context, row AsyncStateExecutionRowForUpdateWithoutCommands,
	) error
	BatchUpdateAsyncStateExecutionsToAbortRunning(ctx context.Context, processExecutionId uuid.UUID) error
	InsertImmediateTask(ctx context.Context, row ImmediateTaskRowForInsert) error
	InsertTimerTask(ctx context.Context, row TimerTaskRowForInsert) error

	DeleteImmediateTask(ctx context.Context, filter ImmediateTaskRowDeleteFilter) error
	DeleteTimerTask(ctx context.Context, filter TimerTaskRowDeleteFilter) error

	InsertLocalQueueMessage(ctx context.Context, row LocalQueueMessageRow) (bool, error)

	InsertAppDatabaseTable(ctx context.Context, row AppDatabaseTableRow, writeConfigMode xcapi.WriteConflictMode) error
	UpsertAppDatabaseTableByPK(ctx context.Context, row AppDatabaseTableRow) error

	InsertLocalAttribute(ctx context.Context, insert LocalAttributeRow) error
	UpsertLocalAttribute(ctx context.Context, row LocalAttributeRow) error
}

type nonTransactionalCRUD interface {
	SelectLatestProcessExecution(ctx context.Context, namespace string, processId string) (*ProcessExecutionRow, error)

	SelectAsyncStateExecution(
		ctx context.Context, filter AsyncStateExecutionSelectFilter,
	) (*AsyncStateExecutionRow, error)

	BatchSelectImmediateTasks(
		ctx context.Context, shardId int32, startSequenceInclusive int64, pageSize int32,
	) ([]ImmediateTaskRow, error)
	BatchDeleteImmediateTask(ctx context.Context, filter ImmediateTaskRangeDeleteFilter) error

	BatchSelectTimerTasks(ctx context.Context, filter TimerTaskRangeSelectFilter) ([]TimerTaskRow, error)
	SelectTimerTasksForTimestamps(ctx context.Context, filter TimerTaskSelectByTimestampsFilter) ([]TimerTaskRow, error)

	CleanUpTasksForTest(ctx context.Context, shardId int32) error

	SelectLocalQueueMessages(
		ctx context.Context, processExecutionId uuid.UUID, dedupIdStrings []string,
	) ([]LocalQueueMessageRow, error)

	SelectAppDatabaseTableByPK(
		ctx context.Context, tableName string, primaryKeys [][]xcapi.AppDatabaseColumnValue, columns []string,
	) ([]AppDatabaseTableRowSelect, error)

	SelectLocalAttributes(
		ctx context.Context, processExecutionId uuid.UUID, keys []string,
	) ([]LocalAttributeRow, error)
}

type ErrorChecker interface {
	IsDupEntryError(err error) bool
	IsNotFoundError(err error) bool
	IsTimeoutError(err error) bool
	IsThrottlingError(err error) bool
	IsConditionalUpdateFailure(err error) bool
}
