// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"fmt"
	"github.com/xcherryio/apis/goapi/xcapi"
	"github.com/xcherryio/xcherry/common/uuid"
	"github.com/xcherryio/xcherry/persistence/data_models"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/xcherryio/xcherry/extensions"
)

const selectLatestExecutionQuery = `SELECT
	le.process_execution_id, e.shard_id, e.status, e.start_time, e.timeout_seconds, e.history_event_id_sequence, e.state_execution_sequence_maps, e.info
	FROM xcherry_sys_latest_process_executions le
	INNER JOIN xcherry_sys_process_executions e ON e.process_id = le.process_id AND e.id = le.process_execution_id
	WHERE le.namespace = $1 AND le.process_id = $2`

func (d dbSession) SelectLatestProcessExecution(
	ctx context.Context, namespace, processId string,
) (*extensions.ProcessExecutionRow, error) {
	var row extensions.ProcessExecutionRow
	err := d.db.GetContext(ctx, &row, selectLatestExecutionQuery, namespace, processId)
	row.Namespace = namespace
	row.ProcessId = processId
	row.StartTime = FromPostgresDateTime(row.StartTime)
	return &row, err
}

const selectAsyncStateExecutionQuery = `SELECT 
    status, wait_until_commands, wait_until_command_results, version as previous_version, info, input, last_failure
	FROM xcherry_sys_async_state_executions WHERE process_execution_id=$1 AND state_id=$2 AND state_id_sequence=$3`

func (d dbSession) SelectAsyncStateExecution(
	ctx context.Context, filter extensions.AsyncStateExecutionSelectFilter,
) (*extensions.AsyncStateExecutionRow, error) {
	var row extensions.AsyncStateExecutionRow
	filter.ProcessExecutionIdString = filter.ProcessExecutionId.String()
	err := d.db.GetContext(ctx, &row, selectAsyncStateExecutionQuery, filter.ProcessExecutionIdString, filter.StateId, filter.StateIdSequence)
	row.ProcessExecutionId = filter.ProcessExecutionId
	row.StateId = filter.StateId
	row.StateIdSequence = filter.StateIdSequence
	return &row, err
}

const batchSelectImmediateTasksQuery = `SELECT 
    shard_id, task_sequence, process_execution_id, state_id, state_id_sequence, task_type, info
	FROM xcherry_sys_immediate_tasks WHERE shard_id = $1 AND task_sequence>= $2 ORDER BY task_sequence ASC LIMIT $3`

func (d dbSession) BatchSelectImmediateTasks(
	ctx context.Context, shardId int32, startSequenceInclusive int64, pageSize int32,
) ([]extensions.ImmediateTaskRow, error) {
	var rows []extensions.ImmediateTaskRow
	err := d.db.SelectContext(ctx, &rows, batchSelectImmediateTasksQuery, shardId, startSequenceInclusive, pageSize)
	return rows, err
}

const batchDeleteImmediateTaskQuery = `DELETE 
	FROM xcherry_sys_immediate_tasks WHERE shard_id = $1 AND task_sequence>= $2 AND task_sequence <= $3`

func (d dbSession) BatchDeleteImmediateTask(
	ctx context.Context, filter extensions.ImmediateTaskRangeDeleteFilter,
) error {
	_, err := d.db.ExecContext(ctx, batchDeleteImmediateTaskQuery, filter.ShardId, filter.MinTaskSequenceInclusive, filter.MaxTaskSequenceInclusive)
	return err
}

const batchSelectTimerTasksOfFirstPageQuery = `SELECT 
    shard_id, fire_time_unix_seconds, task_sequence, process_execution_id, state_id, state_id_sequence, task_type, info
	FROM xcherry_sys_timer_tasks WHERE shard_id = $1 AND fire_time_unix_seconds <= $2 
	ORDER BY fire_time_unix_seconds, task_sequence ASC LIMIT $3`

func (d dbSession) BatchSelectTimerTasks(
	ctx context.Context, filter extensions.TimerTaskRangeSelectFilter,
) ([]extensions.TimerTaskRow, error) {
	var rows []extensions.TimerTaskRow
	err := d.db.SelectContext(ctx, &rows, batchSelectTimerTasksOfFirstPageQuery,
		filter.ShardId, filter.MaxFireTimeUnixSecondsInclusive, filter.PageSize)
	return rows, err
}

const selectTimerTasksForTimestampsQuery = `SELECT 
    shard_id, fire_time_unix_seconds, task_sequence, process_execution_id, state_id, state_id_sequence, task_type, info
	FROM xcherry_sys_timer_tasks WHERE shard_id = ? AND fire_time_unix_seconds IN (?) AND task_sequence >= ? 
	ORDER BY fire_time_unix_seconds, task_sequence ASC`

func (d dbSession) SelectTimerTasksForTimestamps(
	ctx context.Context, filter extensions.TimerTaskSelectByTimestampsFilter,
) ([]extensions.TimerTaskRow, error) {
	var rows []extensions.TimerTaskRow
	query, args, err := sqlx.In(selectTimerTasksForTimestampsQuery, filter.ShardId, filter.FireTimeUnixSeconds, filter.MinTaskSequenceInclusive)
	if err != nil {
		return nil, err
	}
	query = d.db.Rebind(query)
	err = d.db.SelectContext(ctx, &rows, query, args...)
	return rows, err
}

func (d dbSession) CleanUpTasksForTest(ctx context.Context, shardId int32) error {
	_, err := d.db.ExecContext(ctx, `DELETE FROM xcherry_sys_immediate_tasks WHERE shard_id = $1`, shardId)
	if err != nil {
		return err
	}
	_, err = d.db.ExecContext(ctx, `DELETE FROM xcherry_sys_timer_tasks WHERE shard_id = $1`, shardId)
	return err
}

const selectLocalQueueMessagesQuery = `SELECT
	process_execution_id, queue_name, dedup_id, payload
	FROM xcherry_sys_local_queue_messages WHERE process_execution_id = ? AND dedup_id IN (?)
`

func (d dbSession) SelectLocalQueueMessages(
	ctx context.Context, processExecutionId uuid.UUID, dedupIdStrings []string,
) (
	[]extensions.LocalQueueMessageRow, error) {
	var rows []extensions.LocalQueueMessageRow
	query, args, err := sqlx.In(selectLocalQueueMessagesQuery, processExecutionId.String(), dedupIdStrings)
	if err != nil {
		return nil, err
	}
	query = d.db.Rebind(query)
	err = d.db.SelectContext(ctx, &rows, query, args...)
	return rows, err
}

func (d dbSession) SelectAppDatabaseTableByPK(
	ctx context.Context, tableName string, primaryKeys [][]xcapi.AppDatabaseColumnValue, columns []string,
) ([]extensions.AppDatabaseTableRowSelect, error) {
	var rows []extensions.AppDatabaseTableRowSelect

	for _, pksPerRow := range primaryKeys {
		var whereClause string

		for _, pk := range pksPerRow {
			if len(whereClause) > 0 {
				whereClause += " AND "
			}

			whereClause += pk.GetColumn() + " = '" + pk.GetQueryValue() + "'"
		}

		row := d.db.QueryRowContext(ctx, `SELECT `+
			strings.Join(columns, ", ")+` FROM `+
			tableName+` WHERE `+whereClause)

		if row.Err() != nil {
			if d.IsNotFoundError(row.Err()) {
				continue
			}
			return nil, row.Err()
		}

		colsToScan := make([]any, len(columns))
		for i := range colsToScan {
			colsToScan[i] = new(string)
		}

		err := row.Scan(colsToScan...)
		if err != nil {
			return nil, err
		}

		columnToValue := map[string]string{}
		for i := range colsToScan {
			ele := colsToScan[i]
			strPtr, ok := ele.(*string)
			if ok {
				columnToValue[columns[i]] = *strPtr
			} else {
				return nil, fmt.Errorf("unexpected type %v", ele)
			}
		}

		rows = append(rows, extensions.AppDatabaseTableRowSelect{
			ColumnToValue: columnToValue,
		})
	}

	return rows, nil
}

const selectLocalAttributesQuery = `SELECT
process_execution_id, key, value
FROM xcherry_sys_local_attributes WHERE process_execution_id = ? AND key IN (?)
`

func (d dbSession) SelectLocalAttributes(
	ctx context.Context, processExecutionId uuid.UUID, keys []string,
) ([]extensions.LocalAttributeRow, error) {
	var rows []extensions.LocalAttributeRow
	query, args, err := sqlx.In(selectLocalAttributesQuery, processExecutionId.String(), keys)
	if err != nil {
		return nil, err
	}
	query = d.db.Rebind(query)
	err = d.db.SelectContext(ctx, &rows, query, args...)
	return rows, err
}

const insertProcessExecutionStartQuery = `INSERT INTO xcherry_sys_executions_visibility
	(namespace, process_id, process_execution_id, process_type_name, status, start_time)
	VALUES (:namespace, :process_id, :process_execution_id_string, :process_type_name, :status, :start_time)`

func (d dbSession) InsertProcessExecutionStartForVisibility(
	ctx context.Context, row extensions.ExecutionVisibilityRow,
) error {
	row.StartTime = ToPostgresDateTime(row.StartTime)
	row.ProcessExecutionIdString = row.ProcessExecutionId.String()
	_, err := d.db.NamedExecContext(ctx, insertProcessExecutionStartQuery, row)
	return err
}

const updateProcessExecutionStatusQuery = `UPDATE xcherry_sys_executions_visibility
	SET status = :status, close_time = :close_time
	WHERE namespace = :namespace AND process_execution_id = :process_execution_id_string
`

func (d dbSession) UpdateProcessExecutionStatusForVisibility(
	ctx context.Context, row extensions.ExecutionVisibilityRow,
) error {
	row.CloseTime = ToPostgresDateTime(row.CloseTime)
	row.ProcessExecutionIdString = row.ProcessExecutionId.String()
	_, err := d.db.NamedExecContext(ctx, updateProcessExecutionStatusQuery, row)
	return err
}

const selectProcessExecutionsQuery = `SELECT * 
FROM xcherry_sys_executions_visibility
WHERE namespace = $1 
AND start_time >= $2 AND start_time <= $3 
AND (process_execution_id > $4 OR start_time < $5)
ORDER BY start_time DESC, process_execution_id
LIMIT $6
`

func (d dbSession) SelectProcessExecutions(
	ctx context.Context,
	namespace string,
	startTimeMinInclusive, startTimeMaxInclusive int64,
	lastProcessExecutionIdString string,
	lastStartTime int64,
	pageSize int32,
) ([]extensions.ExecutionVisibilityRow, error) {
	var rows []extensions.ExecutionVisibilityRow
	err := d.db.SelectContext(ctx, &rows, selectProcessExecutionsQuery,
		namespace,
		time.Unix(startTimeMinInclusive, 0),
		time.Unix(startTimeMaxInclusive, 0),
		lastProcessExecutionIdString,
		time.Unix(lastStartTime, 0),
		pageSize)
	if err != nil {
		return nil, err
	}

	return rows, nil
}

const selectProcessExecutionsByStatusQuery = `SELECT * 
FROM xcherry_sys_executions_visibility
WHERE namespace = $1 
AND status = $2 
AND start_time >= $3 AND start_time <= $4 
AND (process_execution_id > $5 OR start_time < $6)
ORDER BY start_time DESC, process_execution_id
LIMIT $7
`

func (d dbSession) SelectProcessExecutionsByStatus(
	ctx context.Context,
	namespace string,
	status data_models.ProcessExecutionStatus,
	startTimeMinInclusive, startTimeMaxInclusive int64,
	lastProcessExecutionIdString string,
	lastStartTime int64,
	pageSize int32,
) ([]extensions.ExecutionVisibilityRow, error) {
	var rows []extensions.ExecutionVisibilityRow
	err := d.db.SelectContext(ctx, &rows, selectProcessExecutionsByStatusQuery,
		namespace,
		status,
		time.Unix(startTimeMinInclusive, 0),
		time.Unix(startTimeMaxInclusive, 0),
		lastProcessExecutionIdString,
		time.Unix(lastStartTime, 0),
		pageSize)
	if err != nil {
		return nil, err
	}

	return rows, nil
}

const selectProcessExecutionsByTypeQuery = `SELECT * 
FROM xcherry_sys_executions_visibility
WHERE namespace = $1 
AND process_type_name = $2 
AND start_time >= $3 AND start_time <= $4 
AND (process_execution_id > $5 OR start_time < $6)
ORDER BY start_time DESC, process_execution_id
LIMIT $7
`

func (d dbSession) SelectProcessExecutionsByTypeQuery(
	ctx context.Context,
	namespace string,
	processTypeName string,
	startTimeMinInclusive, startTimeMaxInclusive int64,
	lastProcessExecutionIdString string,
	lastStartTime int64,
	pageSize int32,
) ([]extensions.ExecutionVisibilityRow, error) {
	var rows []extensions.ExecutionVisibilityRow
	err := d.db.SelectContext(ctx, &rows, selectProcessExecutionsByTypeQuery,
		namespace,
		processTypeName,
		time.Unix(startTimeMinInclusive, 0),
		time.Unix(startTimeMaxInclusive, 0),
		lastProcessExecutionIdString,
		time.Unix(lastStartTime, 0),
		pageSize)
	if err != nil {
		return nil, err
	}

	return rows, nil
}

const selectProcessExecutionsByIdQuery = `SELECT *
FROM xcherry_sys_executions_visibility
WHERE namespace = $1 
AND process_id = $2 
AND start_time >= $3 AND start_time <= $4 
AND (process_execution_id > $5 OR start_time < $6)
ORDER BY start_time DESC, process_execution_id
LIMIT $7`

func (d dbSession) SelectProcessExecutionsById(
	ctx context.Context,
	namespace string,
	processId string,
	startTimeMinInclusive, startTimeMaxInclusive int64,
	lastProcessExecutionIdString string,
	lastStartTime int64,
	pageSize int32,
) ([]extensions.ExecutionVisibilityRow, error) {
	var rows []extensions.ExecutionVisibilityRow
	err := d.db.SelectContext(ctx, &rows, selectProcessExecutionsByIdQuery,
		namespace,
		processId,
		time.Unix(startTimeMinInclusive, 0),
		time.Unix(startTimeMaxInclusive, 0),
		lastProcessExecutionIdString,
		time.Unix(lastStartTime, 0),
		pageSize)
	if err != nil {
		return nil, err
	}

	return rows, nil
}

const selectProcessExecutionsByStatusAndType = `SELECT *
FROM xcherry_sys_executions_visibility
WHERE namespace = $1 
AND status = $2 
AND process_type_name = $3 
AND start_time >= $4 AND start_time <= $5 
AND (process_execution_id > $6 OR start_time < $7)
ORDER BY start_time DESC, process_execution_id
LIMIT $8
`

func (d dbSession) SelectProcessExecutionsByStatusAndType(
	ctx context.Context,
	namespace string,
	status data_models.ProcessExecutionStatus,
	processTypeName string,
	startTimeMinInclusive, startTimeMaxInclusive int64,
	lastProcessExecutionIdString string,
	lastStartTime int64,
	pageSize int32,
) ([]extensions.ExecutionVisibilityRow, error) {
	var rows []extensions.ExecutionVisibilityRow
	err := d.db.SelectContext(ctx, &rows, selectProcessExecutionsByStatusAndType,
		namespace,
		status,
		processTypeName,
		time.Unix(startTimeMinInclusive, 0),
		time.Unix(startTimeMaxInclusive, 0),
		lastProcessExecutionIdString,
		time.Unix(lastStartTime, 0),
		pageSize)
	if err != nil {
		return nil, err
	}

	return rows, nil
}

const selectProcessExecutionsByStatusAndIdQuery = `SELECT *
FROM xcherry_sys_executions_visibility
WHERE namespace = $1 
AND status = $2 
AND process_id = $3 
AND start_time >= $4 AND start_time <= $5 
AND (process_execution_id > $6 OR start_time < $7)
ORDER BY start_time DESC, process_execution_id
LIMIT $8`

func (d dbSession) SelectProcessExecutionsByStatusAndId(
	ctx context.Context,
	namespace string,
	status data_models.ProcessExecutionStatus,
	processId string,
	startTimeMinInclusive, startTimeMaxInclusive int64,
	lastProcessExecutionIdString string,
	lastStartTime int64,
	pageSize int32,
) ([]extensions.ExecutionVisibilityRow, error) {
	var rows []extensions.ExecutionVisibilityRow
	err := d.db.SelectContext(ctx, &rows, selectProcessExecutionsByStatusAndIdQuery,
		namespace,
		status,
		processId,
		time.Unix(startTimeMinInclusive, 0),
		time.Unix(startTimeMaxInclusive, 0),
		lastProcessExecutionIdString,
		time.Unix(lastStartTime, 0),
		pageSize)
	if err != nil {
		return nil, err
	}

	return rows, nil
}
