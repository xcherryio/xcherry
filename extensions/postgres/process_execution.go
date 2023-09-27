package postgres

import (
	"context"
	"database/sql"
	"github.com/xdblab/xdb/extensions"
)

const (
	insertCurrentExecutionQuery = `INSERT INTO xdb_sys_current_process_execution
	(process_id, process_execution_id) VALUES
	($1, $2)`

	insertExecutionQuery = `INSERT INTO xdb_sys_process_execution
	(id, process_id, is_current, status, start_time, timeout_seconds, history_event_id_sequence, info) VALUES
	(:process_id, :process_execution_id)`

	selectCurrentExecutionQuery = `SELECT
	ce.process_execution_id, e.process_id, e.is_current, e.status, e.start_time, e.timeout_seconds, e.history_event_id_sequence, e.info
	FROM xdb_sys_current_process_execution ce
	INNER JOIN xdb_sys_process_execution e ON e.process_id = ce.process_id
	WHERE ce.process_id = $1`
)

func (d dbTx) InsertCurrentProcessExecution(ctx context.Context, processId, processExecutionId string) (sql.Result, error) {
	return d.tx.ExecContext(ctx, insertCurrentExecutionQuery, processId, processExecutionId)
}

func (d dbTx) InsertProcessExecution(ctx context.Context, row extensions.ProcessExecutionRow) (sql.Result, error) {
	return d.tx.NamedExecContext(ctx, insertExecutionQuery, row)
}

func (d dbSession) SelectCurrentProcessExecution(ctx context.Context, processId string) (extensions.ProcessExecutionRow, error) {
	var row extensions.ProcessExecutionRow
	err := d.db.SelectContext(ctx, &row, selectCurrentExecutionQuery, processId)
	return row, err
}
