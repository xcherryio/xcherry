package postgres

import (
	"context"
	"github.com/xdblab/xdb/extensions"
	"github.com/xdblab/xdb/persistence/uuid"
)

const insertCurrentProcessExecutionQuery = `INSERT INTO xdb_sys_current_process_executions
	(namespace, process_id, process_execution_id) VALUES
	($1, $2, $3)`

func (d dbTx) InsertCurrentProcessExecution(ctx context.Context, row extensions.CurrentProcessExecutionRow) error {
	row.ProcessExecutionIdString = row.ProcessExecutionId.String()
	_, err := d.tx.ExecContext(ctx, insertCurrentProcessExecutionQuery, row.Namespace, row.ProcessId, row.ProcessExecutionIdString)
	return err
}

const insertProcessExecutionQuery = `INSERT INTO xdb_sys_process_executions
	(namespace, id, process_id, is_current, status, start_time, timeout_seconds, history_event_id_sequence, state_id_sequence, info) VALUES
	(:namespace, :process_execution_id_string, :process_id, :is_current, :status, :start_time, :timeout_seconds, :history_event_id_sequence, :state_id_sequence, :info)`

func (d dbTx) InsertProcessExecution(ctx context.Context, row extensions.ProcessExecutionRow) error {
	row.StartTime = ToPostgresDateTime(row.StartTime)
	row.ProcessExecutionIdString = row.ProcessExecutionId.String()
	_, err := d.tx.NamedExecContext(ctx, insertProcessExecutionQuery, row)
	return err
}

const updateProcessExecutionQuery = `UPDATE xdb_sys_process_executions set
is_current = :is_current, 
status = :status,
history_event_id_sequence= :history_event_id_sequence,
state_id_sequence= :state_id_sequence
WHERE process_execution_id=:process_execution_id_string
`

func (d dbTx) UpdateProcessExecution(ctx context.Context, row extensions.ProcessExecutionRowForUpdate) error {
	row.ProcessExecutionIdString = row.ProcessExecutionId.String()
	_, err := d.tx.NamedExecContext(ctx, updateProcessExecutionQuery, row)
	return err
}

const insertAsyncStateExecutionQuery = `INSERT INTO xdb_sys_async_state_executions 
	(process_execution_id, state_id, state_id_sequence, version, wait_until_status, info, input) VALUES
	(:process_execution_id_string, :state_id, :state_id_sequence, :previous_version, :wait_until_status, :info, :input)`

func (d dbTx) InsertAsyncStateExecution(ctx context.Context, row extensions.AsyncStateExecutionRow) error {
	row.ProcessExecutionIdString = row.ProcessExecutionId.String()
	_, err := d.tx.NamedExecContext(ctx, insertAsyncStateExecutionQuery, row)
	return err
}

const updateAsyncStateExecutionQuery = `UPDATE xdb_sys_async_state_executions set
version = :previous_version +1,
wait_until_status = :wait_until_status,
execute_status = :execute_status,
WHERE process_execution_id=:process_execution_id_string AND state_id=:state_id AND state_id_sequence=:state_id_sequence AND version = :previous_version
`

func (d dbTx) UpdateAsyncStateExecution(ctx context.Context, row extensions.AsyncStateExecutionRowForUpdate) error {
	row.ProcessExecutionIdString = row.ProcessExecutionId.String()
	_, err := d.tx.NamedExecContext(ctx, updateAsyncStateExecutionQuery, row)
	// TODO check conflict error
	return err
}

const insertWorkerTaskQuery = `INSERT INTO xdb_sys_worker_tasks
	(shard_id, process_execution_id, state_id, state_id_sequence, task_type) VALUES
	(:shard_id, :process_execution_id_string, :state_id, :state_id_sequence, :task_type)`

func (d dbTx) InsertWorkerTask(ctx context.Context, row extensions.WorkerTaskRowForInsert) error {
	row.ProcessExecutionIdString = row.ProcessExecutionId.String()
	_, err := d.tx.NamedExecContext(ctx, insertWorkerTaskQuery, row)
	return err
}

const selectProcessExecutionForUpdateQuery = `SELECT process_execution_id, is_current, status, history_event_id_sequence, state_id_sequence
	FROM xdb_sys_process_executions WHERE process_execution_id=$1`

func (d dbTx) SelectProcessExecutionForUpdate(ctx context.Context, processExecutionId uuid.UUID) (*extensions.ProcessExecutionRowForUpdate, error) {
	var row extensions.ProcessExecutionRowForUpdate
	err := d.tx.GetContext(ctx, &row, selectProcessExecutionForUpdateQuery, processExecutionId)
	return &row, err
}
