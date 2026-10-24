// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use dexzd_keyspace::row_keys::{
    HARD_DELETE_PENDING_OBJECT, active_work_index_key, flow_list_index_key,
    hard_delete_pending_key, prefix_range, retention_index_key, run_item_index_key,
    tenant_run_index_key, timer_index_key,
};
use dexzd_keyspace::{ColumnFamily, IndexKind, RunId, RunTable, run_prefix, run_table_prefix};
use dexzd_protocol::dex::ErrorSubStatus;
use dexzd_protocol::dexzd::v1::{
    HardDeleteObjectRecord, HardDeleteObjectsDeletedCommand, OutboxRecord, RetentionTimerId,
    RunPurgeCommand, TimerId, TimerRecord, timer_id,
};
use prost::Message;

use crate::ApplyError;
use crate::command_handlers::command_replies::{error_reply, grpc_code, ok_reply};
use crate::command_handlers::run_cursor::{HistoryKind, RunCursor};
use crate::command_handlers::{CommandScope, HandlerOutput, HandlerResult};
use crate::state_access::{FlowTransaction, RunRows, read_flow_directory, write_flow_directory};
use crate::steps::arm_timer;

const RETENTION_RETRY_MICROS: i64 = 60 * 60 * 1_000_000;

/// Removes acknowledged hard-delete pending rows after object deletion.
pub(crate) fn apply_hard_delete_objects_deleted(
    transaction: &mut FlowTransaction<'_>,
    cursor: RunCursor,
    scope: &CommandScope<'_>,
    command: &HardDeleteObjectsDeletedCommand,
) -> Result<HandlerResult, ApplyError> {
    let mut changed = false;
    for object_id in &command.object_ids {
        let key = hard_delete_pending_key(cursor.rows.flow_hash, cursor.run_id(), object_id);
        if transaction.get(ColumnFamily::Flow, &key)?.is_none() {
            continue;
        }
        transaction.delete(ColumnFamily::Flow, key);
        transaction.delete(
            ColumnFamily::PartitionIndex,
            run_item_index_key(
                transaction.context().partition,
                IndexKind::HardDeletePending,
                cursor.rows.flow_hash,
                cursor.run_id(),
                object_id.as_bytes(),
            ),
        );
        changed = true;
    }
    if !changed {
        return Ok(HandlerResult::Rejected(ok_reply()));
    }
    Ok(HandlerResult::Applied(cursor.finish(
        transaction,
        scope,
        ok_reply(),
        HistoryKind::BOOKKEEPING,
    )?))
}

/// Purges all run rows and every derived partition index computable from those rows.
pub(crate) fn apply_run_purge(
    transaction: &mut FlowTransaction<'_>,
    scope: &CommandScope<'_>,
    _command: &RunPurgeCommand,
) -> Result<HandlerResult, ApplyError> {
    let Ok(run_id) = RunId::from_slice(&scope.command.run_id) else {
        return Ok(HandlerResult::Rejected(error_reply(
            grpc_code::INVALID_ARGUMENT,
            ErrorSubStatus::Uncategorized,
            "run purge requires a 16-byte run ID",
        )));
    };
    let flow_hash = transaction.context().flow_hash;
    let partition = transaction.context().partition;
    let rows = RunRows::new(flow_hash, run_id);
    let Some(record) = rows.run_record(transaction)? else {
        return Ok(HandlerResult::Rejected(ok_reply()));
    };
    if record.tenant_id != scope.tenant_id.0 || record.flow_id != scope.flow_id() {
        return Ok(HandlerResult::Rejected(ok_reply()));
    }
    if !record.is_purging {
        return Ok(HandlerResult::Rejected(error_reply(
            grpc_code::FAILED_PRECONDITION,
            ErrorSubStatus::Uncategorized,
            "run retention has not requested purge",
        )));
    }
    if has_unacknowledged_outbox(transaction, rows)? {
        let previous_deadline = record.retention_deadline_micros;
        let mut cursor = RunCursor::for_new_run(rows, record);
        cursor.record.is_purging = false;
        let retry_deadline = scope.now_micros.saturating_add(RETENTION_RETRY_MICROS);
        cursor.record.retention_deadline_micros = retry_deadline;
        transaction.delete(
            ColumnFamily::PartitionIndex,
            retention_index_key(partition, previous_deadline, flow_hash, run_id),
        );
        arm_timer(
            transaction,
            &mut cursor,
            &TimerId {
                run_attempt: 0,
                kind: Some(timer_id::Kind::Retention(RetentionTimerId {})),
            },
            retry_deadline,
        );
        transaction.put(
            ColumnFamily::PartitionIndex,
            retention_index_key(partition, retry_deadline, flow_hash, run_id),
            Vec::new(),
        );
        return Ok(HandlerResult::Applied(cursor.finish(
            transaction,
            scope,
            ok_reply(),
            HistoryKind::BOOKKEEPING,
        )?));
    }
    remove_timer_indexes(transaction, rows)?;
    remove_outbox_indexes(transaction, rows)?;
    remove_hard_delete_indexes(transaction, rows)?;
    transaction.delete(
        ColumnFamily::PartitionIndex,
        tenant_run_index_key(
            partition,
            IndexKind::TenantRuns,
            scope.tenant_id,
            flow_hash,
            run_id,
        ),
    );
    transaction.delete(
        ColumnFamily::PartitionIndex,
        tenant_run_index_key(
            partition,
            IndexKind::SearchDocument,
            scope.tenant_id,
            flow_hash,
            run_id,
        ),
    );
    transaction.delete(
        ColumnFamily::PartitionIndex,
        flow_list_index_key(
            partition,
            scope.tenant_id,
            &record.flow_type,
            u8::try_from(record.status).unwrap_or(u8::MAX),
            flow_hash,
            run_id,
        ),
    );
    transaction.delete(
        ColumnFamily::PartitionIndex,
        active_work_index_key(partition, flow_hash, run_id),
    );
    if record.retention_deadline_micros != 0 {
        transaction.delete(
            ColumnFamily::PartitionIndex,
            retention_index_key(
                partition,
                record.retention_deadline_micros,
                flow_hash,
                run_id,
            ),
        );
    }
    let mut directory = read_flow_directory(transaction, scope.tenant_id, scope.flow_id())?;
    directory
        .runs
        .retain(|entry| entry.run_id != scope.command.run_id);
    write_flow_directory(transaction, scope.tenant_id, scope.flow_id(), &directory);
    let (start, end) = prefix_range(&run_prefix(flow_hash, run_id));
    for column_family in [
        ColumnFamily::Flow,
        ColumnFamily::History,
        ColumnFamily::Value,
    ] {
        transaction.delete_range(column_family, start.clone(), end.clone());
    }
    Ok(HandlerResult::Applied(HandlerOutput {
        reply: ok_reply(),
        effects: Vec::new(),
        run_id: Some(run_id),
        replayed: None,
    }))
}

fn has_unacknowledged_outbox(
    transaction: &FlowTransaction<'_>,
    rows: RunRows,
) -> Result<bool, ApplyError> {
    let prefix = run_table_prefix(rows.flow_hash, rows.run_id, RunTable::Outbox);
    let (start, end) = prefix_range(&prefix);
    let messages: Vec<(Vec<u8>, OutboxRecord)> =
        transaction.scan_messages(ColumnFamily::Flow, &start, &end, usize::MAX, "outbox")?;
    Ok(messages
        .iter()
        .any(|(_, message)| message.acknowledged_at_micros == 0))
}

fn remove_timer_indexes(
    transaction: &mut FlowTransaction<'_>,
    rows: RunRows,
) -> Result<(), ApplyError> {
    let prefix = run_table_prefix(rows.flow_hash, rows.run_id, RunTable::Timer);
    let (start, end) = prefix_range(&prefix);
    let timers: Vec<(Vec<u8>, TimerRecord)> =
        transaction.scan_messages(ColumnFamily::Flow, &start, &end, usize::MAX, "timer")?;
    for (_, timer) in timers {
        let timer_id = timer
            .timer_id
            .ok_or_else(|| ApplyError::InvariantViolation {
                detail: "stored timer has no ID during run purge".to_owned(),
            })?;
        transaction.delete(
            ColumnFamily::PartitionIndex,
            timer_index_key(
                transaction.context().partition,
                timer.fire_at_micros,
                rows.flow_hash,
                rows.run_id,
                &timer_id.encode_to_vec(),
            ),
        );
    }
    Ok(())
}

fn remove_outbox_indexes(
    transaction: &mut FlowTransaction<'_>,
    rows: RunRows,
) -> Result<(), ApplyError> {
    let prefix = run_table_prefix(rows.flow_hash, rows.run_id, RunTable::Outbox);
    let (start, end) = prefix_range(&prefix);
    let messages: Vec<(Vec<u8>, OutboxRecord)> =
        transaction.scan_messages(ColumnFamily::Flow, &start, &end, usize::MAX, "outbox")?;
    for (_, message) in messages {
        if message.acknowledged_at_micros == 0 {
            transaction.delete(
                ColumnFamily::PartitionIndex,
                run_item_index_key(
                    transaction.context().partition,
                    IndexKind::OutboxPending,
                    rows.flow_hash,
                    rows.run_id,
                    &message.message_id,
                ),
            );
        }
    }
    Ok(())
}

fn remove_hard_delete_indexes(
    transaction: &mut FlowTransaction<'_>,
    rows: RunRows,
) -> Result<(), ApplyError> {
    let mut prefix = run_table_prefix(rows.flow_hash, rows.run_id, RunTable::HardDeleteObject);
    prefix.push(HARD_DELETE_PENDING_OBJECT);
    let (start, end) = prefix_range(&prefix);
    let objects: Vec<(Vec<u8>, HardDeleteObjectRecord)> = transaction.scan_messages(
        ColumnFamily::Flow,
        &start,
        &end,
        usize::MAX,
        "hard-delete pending",
    )?;
    for (_, object) in objects {
        let reference = object
            .object
            .ok_or_else(|| ApplyError::InvariantViolation {
                detail: "pending hard-delete row has no object during run purge".to_owned(),
            })?;
        transaction.delete(
            ColumnFamily::PartitionIndex,
            run_item_index_key(
                transaction.context().partition,
                IndexKind::HardDeletePending,
                rows.flow_hash,
                rows.run_id,
                reference.object_id.as_bytes(),
            ),
        );
    }
    Ok(())
}
