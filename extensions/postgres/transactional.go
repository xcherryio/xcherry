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

package postgres

import (
	"context"

	"github.com/xdblab/xdb/common/uuid"
	"github.com/xdblab/xdb/extensions"
)

const insertLatestProcessExecutionQuery = `INSERT INTO xdb_sys_latest_process_executions
	(namespace, process_id, process_execution_id) VALUES
	($1, $2, $3)`

func (d dbTx) InsertLatestProcessExecution(ctx context.Context, row extensions.LatestProcessExecutionRow) error {
	row.ProcessExecutionIdString = row.ProcessExecutionId.String()
	_, err := d.tx.ExecContext(ctx, insertLatestProcessExecutionQuery, row.Namespace, row.ProcessId, row.ProcessExecutionIdString)
	return err
}

const selectLatestProcessExecutionForUpdateQuery = `SELECT namespace, process_id, process_execution_id 
FROM xdb_sys_latest_process_executions 
WHERE namespace=$1 AND process_id=$2 FOR UPDATE`

func (d dbTx) SelectLatestProcessExecutionForUpdate(
	ctx context.Context, namespace string, processId string,
) ([]extensions.LatestProcessExecutionRow, error) {
	var rows []extensions.LatestProcessExecutionRow
	err := d.tx.SelectContext(ctx, &rows, selectLatestProcessExecutionForUpdateQuery, namespace, processId)
	return rows, err
}

const updateLatestProcessExecutionQuery = `UPDATE xdb_sys_latest_process_executions set process_execution_id=$3 WHERE namespace=$1 AND process_id=$2`

func (d dbTx) UpdateLatestProcessExecution(ctx context.Context, row extensions.LatestProcessExecutionRow) error {
	_, err := d.tx.ExecContext(ctx, updateLatestProcessExecutionQuery, row.Namespace, row.ProcessId, row.ProcessExecutionId.String())
	return err
}

const insertProcessExecutionQuery = `INSERT INTO xdb_sys_process_executions
	(namespace, id, process_id, is_current, status, start_time, timeout_seconds, history_event_id_sequence, state_execution_sequence_maps, info) VALUES
	(:namespace, :process_execution_id_string, :process_id, :is_current, :status, :start_time, :timeout_seconds, :history_event_id_sequence, 
	 :state_execution_sequence_maps, :info)`

func (d dbTx) InsertProcessExecution(ctx context.Context, row extensions.ProcessExecutionRow) error {
	row.StartTime = ToPostgresDateTime(row.StartTime)
	row.ProcessExecutionIdString = row.ProcessExecutionId.String()
	_, err := d.tx.NamedExecContext(ctx, insertProcessExecutionQuery, row)
	return err
}

const updateProcessExecutionQuery = `UPDATE xdb_sys_process_executions SET
is_current = :is_current, 
status = :status,
history_event_id_sequence= :history_event_id_sequence,
state_execution_sequence_maps= :state_execution_sequence_maps,
wait_to_complete = :wait_to_complete
WHERE id=:process_execution_id_string
`

func (d dbTx) UpdateProcessExecution(ctx context.Context, row extensions.ProcessExecutionRowForUpdate) error {
	row.ProcessExecutionIdString = row.ProcessExecutionId.String()
	_, err := d.tx.NamedExecContext(ctx, updateProcessExecutionQuery, row)
	return err
}

func (d dbTx) SelectAsyncStateExecutionForUpdate(
	ctx context.Context, filter extensions.AsyncStateExecutionSelectFilter,
) (*extensions.AsyncStateExecutionRow, error) {
	var row extensions.AsyncStateExecutionRow
	filter.ProcessExecutionIdString = filter.ProcessExecutionId.String()
	err := d.tx.GetContext(
		ctx, &row, selectAsyncStateExecutionForUpdateQuery, filter.ProcessExecutionIdString,
		filter.StateId, filter.StateIdSequence)
	row.ProcessExecutionId = filter.ProcessExecutionId
	row.StateId = filter.StateId
	row.StateIdSequence = filter.StateIdSequence
	return &row, err
}

const insertAsyncStateExecutionQuery = `INSERT INTO xdb_sys_async_state_executions 
	(process_execution_id, state_id, state_id_sequence, version, wait_until_status, execute_status, info, input) VALUES
	(:process_execution_id_string, :state_id, :state_id_sequence, :previous_version, :wait_until_status, :execute_status, :info, :input)`

func (d dbTx) InsertAsyncStateExecution(ctx context.Context, row extensions.AsyncStateExecutionRow) error {
	row.ProcessExecutionIdString = row.ProcessExecutionId.String()
	_, err := d.tx.NamedExecContext(ctx, insertAsyncStateExecutionQuery, row)
	return err
}

const updateAsyncStateExecutionQuery = `UPDATE xdb_sys_async_state_executions set
version = :previous_version +1,
wait_until_status = :wait_until_status,
execute_status = :execute_status
WHERE process_execution_id=:process_execution_id_string AND state_id=:state_id 
  AND state_id_sequence=:state_id_sequence AND version = :previous_version`

func (d dbTx) UpdateAsyncStateExecution(
	ctx context.Context, row extensions.AsyncStateExecutionRowForUpdate,
) error {
	// ignore static info because they are not changing
	// TODO how to make that clear? maybe rename the method?
	row.ProcessExecutionIdString = row.ProcessExecutionId.String()
	result, err := d.tx.NamedExecContext(ctx, updateAsyncStateExecutionQuery, row)
	if err != nil {
		return err
	}
	effected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if effected != 1 {
		return conditionalUpdateFailure
	}
	return nil
}

const batchUpdateAsyncStateExecutionsToAbortRunningQuery = `UPDATE xdb_sys_async_state_executions SET
version = CASE WHEN wait_until_status=1 OR execute_status=1 THEN version+1 ELSE version END,
wait_until_status = CASE WHEN wait_until_status=1 THEN 5 ELSE wait_until_status END,
execute_status = CASE WHEN execute_status=1 THEN 5 ELSE execute_status END
WHERE process_execution_id=$1
`

func (d dbTx) BatchUpdateAsyncStateExecutionsToAbortRunning(
	ctx context.Context, processExecutionId uuid.UUID,
) error {
	_, err := d.tx.ExecContext(ctx, batchUpdateAsyncStateExecutionsToAbortRunningQuery, processExecutionId.String())
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

const selectProcessExecutionForUpdateQuery = `SELECT 
    id as process_execution_id, is_current, status, history_event_id_sequence, state_execution_sequence_maps, wait_to_complete
	FROM xdb_sys_process_executions WHERE id=$1 FOR UPDATE`

func (d dbTx) SelectProcessExecutionForUpdate(
	ctx context.Context, processExecutionId uuid.UUID,
) (*extensions.ProcessExecutionRowForUpdate, error) {
	var row extensions.ProcessExecutionRowForUpdate
	err := d.tx.GetContext(ctx, &row, selectProcessExecutionForUpdateQuery, processExecutionId.String())
	return &row, err
}
