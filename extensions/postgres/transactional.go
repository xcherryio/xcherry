// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"fmt"
	"github.com/xcherryio/apis/goapi/xcapi"
	"strings"

	"github.com/xcherryio/xcherry/common/uuid"
	"github.com/xcherryio/xcherry/extensions"
)

const insertLatestProcessExecutionQuery = `INSERT INTO xcherry_sys_latest_process_executions
	(namespace, process_id, process_execution_id) VALUES
	($1, $2, $3)`

func (d dbTx) InsertLatestProcessExecution(ctx context.Context, row extensions.LatestProcessExecutionRow) error {
	row.ProcessExecutionIdString = row.ProcessExecutionId.String()
	_, err := d.tx.ExecContext(ctx, insertLatestProcessExecutionQuery, row.Namespace, row.ProcessId, row.ProcessExecutionIdString)
	return err
}

const selectLatestProcessExecutionForUpdateQuery = `SELECT namespace, process_id, process_execution_id 
FROM xcherry_sys_latest_process_executions 
WHERE namespace=$1 AND process_id=$2 FOR UPDATE`

func (d dbTx) SelectLatestProcessExecutionForUpdate(
	ctx context.Context, namespace string, processId string,
) (*extensions.LatestProcessExecutionRow, bool, error) {
	var rows []extensions.LatestProcessExecutionRow
	err := d.tx.SelectContext(ctx, &rows, selectLatestProcessExecutionForUpdateQuery, namespace, processId)

	if err != nil {
		return nil, false, err
	}

	if len(rows) > 1 {
		return nil, false, fmt.Errorf("more than one row found for namespace %s and processId %s", namespace, processId)
	}

	if len(rows) == 0 {
		return &extensions.LatestProcessExecutionRow{}, false, err
	}

	return &rows[0], true, err
}

const updateLatestProcessExecutionQuery = `UPDATE xcherry_sys_latest_process_executions set process_execution_id=$3 WHERE namespace=$1 AND process_id=$2`

func (d dbTx) UpdateLatestProcessExecution(ctx context.Context, row extensions.LatestProcessExecutionRow) error {
	_, err := d.tx.ExecContext(ctx, updateLatestProcessExecutionQuery, row.Namespace, row.ProcessId, row.ProcessExecutionId.String())
	return err
}

const insertProcessExecutionQuery = `INSERT INTO xcherry_sys_process_executions
	(namespace, id, process_id, shard_id, status, start_time, timeout_seconds, history_event_id_sequence, state_execution_sequence_maps, 
	 state_execution_local_queues, info) VALUES
	(:namespace, :process_execution_id_string, :process_id, :shard_id, :status, :start_time, :timeout_seconds, :history_event_id_sequence, 
	 :state_execution_sequence_maps, :state_execution_local_queues, :info)`

func (d dbTx) InsertProcessExecution(ctx context.Context, row extensions.ProcessExecutionRow) error {
	row.StartTime = ToPostgresDateTime(row.StartTime)
	row.ProcessExecutionIdString = row.ProcessExecutionId.String()
	_, err := d.tx.NamedExecContext(ctx, insertProcessExecutionQuery, row)
	return err
}

const updateProcessExecutionQuery = `UPDATE xcherry_sys_process_executions SET
status = :status,
history_event_id_sequence = :history_event_id_sequence,
state_execution_sequence_maps = :state_execution_sequence_maps,
state_execution_local_queues = :state_execution_local_queues,
graceful_complete_requested = :graceful_complete_requested
WHERE id=:process_execution_id_string
`

func (d dbTx) UpdateProcessExecution(ctx context.Context, row extensions.ProcessExecutionRowForUpdate) error {
	row.ProcessExecutionIdString = row.ProcessExecutionId.String()
	_, err := d.tx.NamedExecContext(ctx, updateProcessExecutionQuery, row)
	return err
}

const insertAsyncStateExecutionQuery = `INSERT INTO xcherry_sys_async_state_executions 
	(process_execution_id, state_id, state_id_sequence, version, status, wait_until_commands, wait_until_command_results, info, input) VALUES
	(:process_execution_id_string, :state_id, :state_id_sequence, :previous_version, :status, :wait_until_commands, :wait_until_command_results, :info, :input)`

func (d dbTx) InsertAsyncStateExecution(ctx context.Context, row extensions.AsyncStateExecutionRow) error {
	row.ProcessExecutionIdString = row.ProcessExecutionId.String()
	_, err := d.tx.NamedExecContext(ctx, insertAsyncStateExecutionQuery, row)
	return err
}

const selectAsyncStateExecutionForUpdateQuery = `SELECT 
    status, version as previous_version, wait_until_commands, wait_until_command_results, last_failure
	FROM xcherry_sys_async_state_executions WHERE process_execution_id=$1 AND state_id=$2 AND state_id_sequence=$3 FOR UPDATE
`

func (d dbTx) SelectAsyncStateExecutionForUpdate(
	ctx context.Context,
	filter extensions.AsyncStateExecutionSelectFilter,
) (*extensions.AsyncStateExecutionRowForUpdate, error) {
	var row extensions.AsyncStateExecutionRowForUpdate
	filter.ProcessExecutionIdString = filter.ProcessExecutionId.String()
	err := d.tx.GetContext(ctx, &row, selectAsyncStateExecutionForUpdateQuery, filter.ProcessExecutionIdString, filter.StateId, filter.StateIdSequence)
	row.ProcessExecutionId = filter.ProcessExecutionId
	row.StateId = filter.StateId
	row.StateIdSequence = filter.StateIdSequence
	return &row, err
}

const updateAsyncStateExecutionQuery = `UPDATE xcherry_sys_async_state_executions set
version = :previous_version + 1,
status = :status,
wait_until_commands = :wait_until_commands,
wait_until_command_results = :wait_until_command_results,
last_failure = :last_failure     
WHERE process_execution_id=:process_execution_id_string AND state_id=:state_id 
  AND state_id_sequence=:state_id_sequence AND version = :previous_version`

func (d dbTx) UpdateAsyncStateExecution(
	ctx context.Context, row extensions.AsyncStateExecutionRowForUpdate,
) error {
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

const updateAsyncStateExecutionWithoutCommandsQuery = `UPDATE xcherry_sys_async_state_executions set
version = :previous_version + 1,
status = :status,
last_failure = :last_failure     
WHERE process_execution_id=:process_execution_id_string AND state_id=:state_id 
  AND state_id_sequence=:state_id_sequence AND version = :previous_version`

func (d dbTx) UpdateAsyncStateExecutionWithoutCommands(
	ctx context.Context, row extensions.AsyncStateExecutionRowForUpdateWithoutCommands,
) error {
	// ignore static info because they are not changing
	// TODO how to make that clear? maybe rename the method?
	row.ProcessExecutionIdString = row.ProcessExecutionId.String()
	result, err := d.tx.NamedExecContext(ctx, updateAsyncStateExecutionWithoutCommandsQuery, row)
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

const batchUpdateAsyncStateExecutionsToAbortRunningQuery = `UPDATE xcherry_sys_async_state_executions SET
version = CASE WHEN status<4 THEN version+1 ELSE version END,
status = CASE WHEN status<4 THEN 7 ELSE status END
WHERE process_execution_id=$1
`

func (d dbTx) BatchUpdateAsyncStateExecutionsToAbortRunning(
	ctx context.Context, processExecutionId uuid.UUID,
) error {
	_, err := d.tx.ExecContext(ctx, batchUpdateAsyncStateExecutionsToAbortRunningQuery, processExecutionId.String())
	return err
}

const insertImmediateTaskQuery = `INSERT INTO xcherry_sys_immediate_tasks
	(shard_id, process_execution_id, state_id, state_id_sequence, task_type, info) VALUES
	(:shard_id, :process_execution_id_string, :state_id, :state_id_sequence, :task_type, :info)`

func (d dbTx) InsertImmediateTask(ctx context.Context, row extensions.ImmediateTaskRowForInsert) error {
	row.ProcessExecutionIdString = row.ProcessExecutionId.String()
	_, err := d.tx.NamedExecContext(ctx, insertImmediateTaskQuery, row)
	return err
}

const selectProcessExecutionForUpdateQuery = `SELECT 
    id as process_execution_id, shard_id, status, history_event_id_sequence, state_execution_sequence_maps, 
    state_execution_local_queues, graceful_complete_requested
	FROM xcherry_sys_process_executions WHERE id=$1 FOR UPDATE`

func (d dbTx) SelectProcessExecutionForUpdate(
	ctx context.Context, processExecutionId uuid.UUID,
) (*extensions.ProcessExecutionRowForUpdate, error) {
	var row extensions.ProcessExecutionRowForUpdate
	err := d.tx.GetContext(ctx, &row, selectProcessExecutionForUpdateQuery, processExecutionId.String())
	return &row, err
}

const selectProcessExecutionQuery = `SELECT 
    id as process_execution_id, status, history_event_id_sequence, state_execution_sequence_maps, state_execution_local_queues, graceful_complete_requested,
	namespace, process_id, start_time, timeout_seconds, info
	FROM xcherry_sys_process_executions WHERE id=$1 `

func (d dbTx) SelectProcessExecution(
	ctx context.Context, processExecutionId uuid.UUID,
) (*extensions.ProcessExecutionRow, error) {
	var row extensions.ProcessExecutionRow
	err := d.tx.GetContext(ctx, &row, selectProcessExecutionQuery, processExecutionId.String())
	return &row, err
}

const insertTimerTaskQuery = `INSERT INTO xcherry_sys_timer_tasks
	(shard_id, fire_time_unix_seconds, process_execution_id, state_id, state_id_sequence, task_type, info) VALUES
	(:shard_id, :fire_time_unix_seconds, :process_execution_id_string, :state_id, :state_id_sequence, :task_type, :info)`

func (d dbTx) InsertTimerTask(ctx context.Context, row extensions.TimerTaskRowForInsert) error {
	row.ProcessExecutionIdString = row.ProcessExecutionId.String()
	_, err := d.tx.NamedExecContext(ctx, insertTimerTaskQuery, row)
	return err
}

const deleteSingleImmediateTaskQuery = `DELETE 
	FROM xcherry_sys_immediate_tasks WHERE shard_id = $1 AND task_sequence= $2`

func (d dbTx) DeleteImmediateTask(ctx context.Context, filter extensions.ImmediateTaskRowDeleteFilter) error {
	_, err := d.tx.ExecContext(ctx, deleteSingleImmediateTaskQuery, filter.ShardId, filter.TaskSequence)
	return err
}

const deleteSingleTimerTaskQuery = `DELETE 
	FROM xcherry_sys_timer_tasks WHERE shard_id = $1 AND fire_time_unix_seconds = $2 AND task_sequence= $3`

func (d dbTx) DeleteTimerTask(ctx context.Context, filter extensions.TimerTaskRowDeleteFilter) error {
	_, err := d.tx.ExecContext(ctx, deleteSingleTimerTaskQuery, filter.ShardId, filter.FireTimeUnixSeconds, filter.TaskSequence)
	return err
}

const insertLocalQueueMessageQuery = `INSERT INTO xcherry_sys_local_queue_messages
	(process_execution_id, queue_name, dedup_id, payload) VALUES 
   	(:process_execution_id_string, :queue_name, :dedup_id_string, :payload)
	ON CONFLICT (process_execution_id, dedup_id) DO NOTHING
`

// InsertLocalQueueMessage returns (insertSuccessfully, error)
func (d dbTx) InsertLocalQueueMessage(ctx context.Context, row extensions.LocalQueueMessageRow) (bool, error) {
	row.ProcessExecutionIdString = row.ProcessExecutionId.String()
	row.DedupIdString = row.DedupId.String()
	result, err := d.tx.NamedExecContext(ctx, insertLocalQueueMessageQuery, row)
	if err != nil {
		return false, err
	}

	effected, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	return effected == 1, err
}

func (d dbTx) InsertAppDatabaseTable(
	ctx context.Context, row extensions.AppDatabaseTableRow, writeConfigMode xcapi.WriteConflictMode,
) error {
	var pkCols []string
	var pkVals []string
	var otherCols []string
	var otherVals []string

	for k, v := range row.PrimaryKeyColumnToValue {
		pkCols = append(pkCols, k)
		pkVals = append(pkVals, "'"+v+"'")
	}

	for k, v := range row.OtherColumnToValue {
		otherCols = append(otherCols, k)
		otherVals = append(otherVals, "'"+v+"'")
	}

	var onConflictClause string

	switch writeConfigMode {
	case xcapi.RETURN_ERROR_ON_CONFLICT:
		onConflictClause = ""
	case xcapi.IGNORE_CONFLICT:
		onConflictClause = `ON CONFLICT DO NOTHING`
	case xcapi.OVERRIDE_ON_CONFLICT:
		var setClauses []string
		for col, val := range row.OtherColumnToValue {
			setClauses = append(setClauses, col+" = '"+val+"'")
		}
		updateClause := "UPDATE SET " + strings.Join(setClauses, ", ")

		onConflictClause = `ON CONFLICT (` + strings.Join(pkCols, ", ") + `) DO ` + updateClause
	default:
		panic("unknown write mode " + string(writeConfigMode))
	}
	_, err := d.tx.ExecContext(ctx,
		`INSERT INTO `+row.TableName+` (`+strings.Join(pkCols, ", ")+`, `+strings.Join(otherCols, ", ")+`)
    	VALUES (`+strings.Join(pkVals, ", ")+`, `+strings.Join(otherVals, ", ")+`) `+onConflictClause)
	return err
}

func (d dbTx) UpsertAppDatabaseTableByPK(ctx context.Context, row extensions.AppDatabaseTableRow) error {
	var pkCols []string
	var pkVals []string
	var otherCols []string
	var otherVals []string

	for k, v := range row.PrimaryKeyColumnToValue {
		pkCols = append(pkCols, k)
		pkVals = append(pkVals, "'"+v+"'")
	}

	for k, v := range row.OtherColumnToValue {
		otherCols = append(otherCols, k)
		otherVals = append(otherVals, "'"+v+"'")
	}

	var setClauses []string
	for col, val := range row.OtherColumnToValue {
		setClauses = append(setClauses, col+" = '"+val+"'")
	}
	updateClause := "UPDATE SET " + strings.Join(setClauses, ", ")

	// TODO get additonal conflict targets from request
	// support from https://github.com/xcherryio/sdk-go/issues/30
	// ??or maybe put all the columns in the conflict target??
	_, err := d.tx.ExecContext(ctx,
		`INSERT INTO `+row.TableName+` (`+strings.Join(pkCols, ", ")+`, `+strings.Join(otherCols, ", ")+`)
		VALUES (`+strings.Join(pkVals, ", ")+`, `+strings.Join(otherVals, ", ")+`)
		ON CONFLICT (`+strings.Join(pkCols, ", ")+`) DO `+updateClause)
	return err
}

const insertLocalAttributeQuery = `INSERT INTO xcherry_sys_local_attributes
	(process_execution_id, key, value)
	VALUES (:process_execution_id_string, :key, :value)
`

func (d dbTx) InsertLocalAttribute(ctx context.Context, row extensions.LocalAttributeRow) error {
	row.ProcessExecutionIdString = row.ProcessExecutionId.String()
	_, err := d.tx.NamedExecContext(ctx, insertLocalAttributeQuery, row)
	return err
}

const upsertLocalAttributeQuery = `INSERT INTO xcherry_sys_local_attributes 
(process_execution_id, key, value)
VALUES (:process_execution_id_string, :key, :value)
ON CONFLICT (process_execution_id, key) DO UPDATE SET value = :value
`

func (d dbTx) UpsertLocalAttribute(ctx context.Context, row extensions.LocalAttributeRow) error {
	row.ProcessExecutionIdString = row.ProcessExecutionId.String()
	_, err := d.tx.NamedExecContext(ctx, upsertLocalAttributeQuery, row)
	return err
}
