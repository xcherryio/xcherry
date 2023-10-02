package postgres

import (
	"context"
	"database/sql"
	"github.com/xdblab/xdb/extensions"
	"github.com/xdblab/xdb/persistence"
)

const (
	insertCurrentExecutionQuery = `INSERT INTO xdb_sys_current_process_executions
	(namespace, process_id, process_execution_id) VALUES
	($1, $2, $3)`

	insertExecutionQuery = `INSERT INTO xdb_sys_process_executions
	(namespace, id, process_id, is_current, status, start_time, timeout_seconds, history_event_id_sequence, info) VALUES
	(:namespace, :process_execution_id, :process_id, :is_current, :status, :start_time, :timeout_seconds, :history_event_id_sequence, :info)`

	selectCurrentExecutionQuery = `SELECT
	ce.process_execution_id, e.process_id, e.is_current, e.status, e.start_time, e.timeout_seconds, e.history_event_id_sequence, e.info
	FROM xdb_sys_current_process_executions ce
	INNER JOIN xdb_sys_process_executions e ON e.process_id = ce.process_id
	WHERE ce.namespace = $1 AND ce.process_id = $2`
)

func (d dbTx) InsertCurrentProcessExecution(ctx context.Context, row extensions.CurrentProcessExecutionRow) (sql.Result, error) {
	return d.tx.ExecContext(ctx, insertCurrentExecutionQuery, row.ProcessId, row.ProcessId, row.ProcessExecutionId)
}

func (d dbTx) InsertProcessExecution(ctx context.Context, row extensions.ProcessExecutionRow) (sql.Result, error) {
	return d.tx.NamedExecContext(ctx, insertExecutionQuery, row)
}

func (d dbTx) SelectProcessExecutionForUpdate(ctx context.Context, processExecutionId persistence.UUID) (*extensions.ProcessExecutionRowForUpdate, error) {
	//TODO implement me
	panic("implement me")
}

func (d dbTx) InsertAsyncStateExecution(ctx context.Context, row extensions.AsyncStateExecutionRow) (sql.Result, error) {
	//TODO implement me
	panic("implement me")
}

func (d dbTx) SelectAsyncStateExecutionForUpdate(ctx context.Context, filter extensions.AsyncStateExecutionSelectFilter) (*extensions.AsyncStateExecutionRowForUpdate, error) {
	//TODO implement me
	panic("implement me")
}

func (d dbTx) InsertWorkerTask(ctx context.Context, row extensions.WorkerTaskRowForInsert) (sql.Result, error) {
	//TODO implement me
	panic("implement me")
}

func (d dbTx) DeleteWorkerTask(ctx context.Context, filter extensions.WorkerTaskRangeDeleteFilter) (sql.Result, error) {
	//TODO implement me
	panic("implement me")
}
