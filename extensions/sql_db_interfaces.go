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

package extensions

import (
	"context"

	"github.com/xdblab/xdb/common/uuid"
	"github.com/xdblab/xdb/config"
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
	InsertLatestProcessExecution(ctx context.Context, row LatestProcessExecutionRow) error
	SelectLatestProcessExecutionForUpdate(ctx context.Context, namespace string, processId string) (*LatestProcessExecutionRow, bool, error)
	UpdateLatestProcessExecution(ctx context.Context, row LatestProcessExecutionRow) error

	InsertProcessExecution(ctx context.Context, row ProcessExecutionRow) error
	SelectProcessExecutionForUpdate(ctx context.Context, processExecutionId uuid.UUID) (*ProcessExecutionRowForUpdate, error)
	SelectProcessExecution(ctx context.Context, processExecutionId uuid.UUID) (*ProcessExecutionRowForUpdate, error)
	UpdateProcessExecution(ctx context.Context, row ProcessExecutionRowForUpdate) error

	SelectAsyncStateExecutionForUpdate(ctx context.Context, filter AsyncStateExecutionSelectFilter) (*AsyncStateExecutionRow, error)
	InsertAsyncStateExecution(ctx context.Context, row AsyncStateExecutionRow) error
	UpdateAsyncStateExecution(ctx context.Context, row AsyncStateExecutionRowForUpdate) error
	BatchUpdateAsyncStateExecutionsToAbortRunning(ctx context.Context, processExecutionId uuid.UUID) error
	InsertWorkerTask(ctx context.Context, row WorkerTaskRowForInsert) error
	InsertTimerTask(ctx context.Context, row TimerTaskRowForInsert) error

	DeleteWorkerTask(ctx context.Context, filter WorkerTaskRowDeleteFilter) error
	DeleteTimerTask(ctx context.Context, filter TimerTaskRowDeleteFilter) error
}

type nonTransactionalCRUD interface {
	SelectLatestProcessExecution(ctx context.Context, namespace string, processId string) (*ProcessExecutionRow, error)

	SelectAsyncStateExecutionForUpdate(ctx context.Context, filter AsyncStateExecutionSelectFilter) (*AsyncStateExecutionRow, error)

	BatchSelectWorkerTasks(ctx context.Context, shardId int32, startSequenceInclusive int64, pageSize int32) ([]WorkerTaskRow, error)
	BatchDeleteWorkerTask(ctx context.Context, filter WorkerTaskRangeDeleteFilter) error

	BatchSelectTimerTasks(ctx context.Context, filter TimerTaskRangeSelectFilter) ([]TimerTaskRow, error)
	SelectTimerTasksForTimestamps(ctx context.Context, filter TimerTaskSelectByTimestampsFilter) ([]TimerTaskRow, error)

	CleanUpTasksForTest(ctx context.Context, shardId int32) error
}

type ErrorChecker interface {
	IsDupEntryError(err error) bool
	IsNotFoundError(err error) bool
	IsTimeoutError(err error) bool
	IsThrottlingError(err error) bool
	IsConditionalUpdateFailure(err error) bool
}
