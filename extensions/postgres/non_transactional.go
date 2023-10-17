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

	"github.com/xdblab/xdb/extensions"
)

const selectLatestExecutionQuery = `SELECT
	ce.process_execution_id, e.is_current, e.status, e.start_time, e.timeout_seconds, e.history_event_id_sequence, e.state_execution_sequence_maps, e.info
	FROM xdb_sys_latest_process_executions ce
	INNER JOIN xdb_sys_process_executions e ON e.process_id = ce.process_id
	WHERE ce.namespace = $1 AND ce.process_id = $2`

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

const selectAsyncStateExecutionForUpdateQuery = `SELECT 
    wait_until_status, execute_status, version as previous_version, info, input
	FROM xdb_sys_async_state_executions WHERE process_execution_id=$1 AND state_id=$2 AND state_id_sequence=$3`

func (d dbSession) SelectAsyncStateExecutionForUpdate(
	ctx context.Context, filter extensions.AsyncStateExecutionSelectFilter,
) (*extensions.AsyncStateExecutionRow, error) {
	var row extensions.AsyncStateExecutionRow
	filter.ProcessExecutionIdString = filter.ProcessExecutionId.String()
	err := d.db.GetContext(ctx, &row, selectAsyncStateExecutionForUpdateQuery, filter.ProcessExecutionIdString, filter.StateId, filter.StateIdSequence)
	row.ProcessExecutionId = filter.ProcessExecutionId
	row.StateId = filter.StateId
	row.StateIdSequence = filter.StateIdSequence
	return &row, err
}

const batchSelectWorkerTasksOfFirstPageQuery = `SELECT 
    shard_id, task_sequence, process_execution_id, state_id, state_id_sequence, task_type
	FROM xdb_sys_worker_tasks WHERE shard_id = $1 AND task_sequence>= $2 ORDER BY task_sequence ASC LIMIT $3`

func (d dbSession) BatchSelectWorkerTasks(
	ctx context.Context, shardId int32, startSequenceInclusive int64, pageSize int32,
) ([]extensions.WorkerTaskRow, error) {
	var rows []extensions.WorkerTaskRow
	err := d.db.SelectContext(ctx, &rows, batchSelectWorkerTasksOfFirstPageQuery, shardId, startSequenceInclusive, pageSize)
	return rows, err
}

const batchDeleteWorkerTaskQuery = `DELETE 
	FROM xdb_sys_worker_tasks WHERE shard_id = $1 AND task_sequence>= $2 AND task_sequence <= $3`

func (d dbSession) BatchDeleteWorkerTask(
	ctx context.Context, filter extensions.WorkerTaskRangeDeleteFilter,
) error {
	_, err := d.db.ExecContext(ctx, batchDeleteWorkerTaskQuery, filter.ShardId, filter.MinTaskSequenceInclusive, filter.MaxTaskSequenceInclusive)
	return err
}
