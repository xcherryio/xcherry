// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use dexzd_keyspace::row_keys::{outbox_key, run_item_index_key};
use dexzd_keyspace::{ColumnFamily, IndexKind};
use dexzd_protocol::dex::{FlowConfig, FlowResult, StepCompletionOutput, SubFlowCondition, Value};
use dexzd_protocol::dexzd::v1::{
    ChildFlowCompletedMessage, ChildFlowStartMessage, CrossFlowEndpoint, OutboxAckCommand,
    OutboxRecord, OutboxRetentionTimerId, ParentFlowReference, StepExecutionRecord, StoredValue,
    TimerId, outbox_record, timer_id,
};

use crate::ApplyEffect;
use crate::ApplyError;
use crate::command_handlers::command_replies::ok_reply;
use crate::command_handlers::run_cursor::{HistoryKind, RunCursor};
use crate::command_handlers::{CommandScope, HandlerResult};
use crate::state_access::FlowTransaction;
use crate::steps::arm_timer;
use crate::values::{
    ValueHolder, ValueRejection, load_stored_value, release_stored_value, store_value,
};

const OUTBOX_RETENTION_MICROS: i64 = 24 * 60 * 60 * 1_000_000;

/// Writes one complete child-start message per SubFlow condition.
pub(crate) fn start_child_flows(
    transaction: &mut FlowTransaction<'_>,
    cursor: &mut RunCursor,
    scope: &CommandScope<'_>,
    step: &mut StepExecutionRecord,
    sub_flow_conditions: &[SubFlowCondition],
) -> Result<Result<(), ValueRejection>, ApplyError> {
    let mut child_ids = Vec::with_capacity(sub_flow_conditions.len());
    for (condition_index, condition) in sub_flow_conditions.iter().enumerate() {
        let sub_flow_index = i32::try_from(condition_index).unwrap_or(i32::MAX);
        let child_flow_id = format!(
            "SubFlow:{}-{}-{sub_flow_index}",
            scope.flow_id(),
            step.step_execution_id
        );
        let message_id = message_id(
            cursor.seq,
            u64::try_from(condition_index).unwrap_or(u64::MAX),
        );
        let mut complete_condition = condition.clone();
        complete_condition.sub_flow_index = sub_flow_index;
        let mut held_values = Vec::new();
        if let Some(value) = condition.step_input.as_ref() {
            let complete = match pin_complete_value(
                transaction,
                cursor,
                value,
                &message_id,
                &mut held_values,
            )? {
                Ok(complete) => complete,
                Err(rejection) => return Ok(Err(rejection)),
            };
            complete_condition.step_input = Some(complete);
        }
        if let Some(options) = complete_condition.options.as_mut() {
            for write in &mut options.attributes {
                if let Some(value) = write.value.as_ref() {
                    let complete = match pin_complete_value(
                        transaction,
                        cursor,
                        value,
                        &message_id,
                        &mut held_values,
                    )? {
                        Ok(complete) => complete,
                        Err(rejection) => return Ok(Err(rejection)),
                    };
                    write.value = Some(complete);
                }
            }
        }
        let parent = ParentFlowReference {
            parent: Some(CrossFlowEndpoint {
                tenant_id: scope.tenant_id.0,
                flow_id: scope.flow_id().to_owned(),
                run_id: cursor.run_id().as_bytes().to_vec(),
                run_attempt: cursor.attempt(),
            }),
            step_execution_id: step.step_execution_id.clone(),
            sub_flow_index,
            condition_id: condition.condition_id.clone(),
        };
        let request_id = format!(
            "{}:{}:{}:{sub_flow_index}",
            cursor.run_id(),
            cursor.attempt(),
            step.step_execution_id
        );
        let flow_config = effective_flow_config(
            cursor.record.flow_config.as_ref(),
            condition
                .options
                .as_ref()
                .and_then(|options| options.flow_config_override.as_ref()),
        );
        write_outbox(
            transaction,
            cursor,
            message_id,
            CrossFlowEndpoint {
                tenant_id: scope.tenant_id.0,
                flow_id: child_flow_id.clone(),
                run_id: Vec::new(),
                run_attempt: 0,
            },
            outbox_record::Payload::ChildStart(ChildFlowStartMessage {
                child_flow_id: child_flow_id.clone(),
                condition: Some(complete_condition),
                flow_config: Some(flow_config),
                request_id,
                parent: Some(parent),
            }),
            held_values,
        );
        child_ids.push(child_flow_id);
    }
    if let Some(wait) = step.wait.as_mut() {
        wait.sub_flow_child_ids = child_ids;
    }
    Ok(Ok(()))
}

fn pin_complete_value(
    transaction: &mut FlowTransaction<'_>,
    cursor: &mut RunCursor,
    value: &Value,
    message_id: &[u8],
    held_values: &mut Vec<StoredValue>,
) -> Result<Result<Value, ValueRejection>, ApplyError> {
    let holder = ValueHolder::OutboxMessage {
        message_id: message_id.to_vec(),
        value_index: u32::try_from(held_values.len()).unwrap_or(u32::MAX),
    };
    let stored = match store_value(
        transaction,
        cursor.rows,
        cursor.value_limits(),
        &mut cursor.values,
        value,
        &holder,
    )? {
        Ok(stored) => stored,
        Err(rejection) => return Ok(Err(rejection)),
    };
    let complete = load_stored_value(transaction, cursor.rows, &stored)?.ok_or_else(|| {
        ApplyError::InvariantViolation {
            detail: "newly stored SubFlow value was unavailable".to_owned(),
        }
    })?;
    held_values.push(stored);
    Ok(Ok(complete))
}

fn effective_flow_config(
    parent: Option<&FlowConfig>,
    override_config: Option<&FlowConfig>,
) -> FlowConfig {
    let mut effective = parent.cloned().unwrap_or_default();
    if let Some(overrides) = override_config {
        if overrides.active_step_search_mode.is_some() {
            effective.active_step_search_mode = overrides.active_step_search_mode;
        }
        if overrides.continue_as_new_threshold.is_some() {
            effective.continue_as_new_threshold = overrides.continue_as_new_threshold;
        }
        if overrides.continue_as_new_page_size_in_bytes.is_some() {
            effective.continue_as_new_page_size_in_bytes =
                overrides.continue_as_new_page_size_in_bytes;
        }
        if overrides.step_durability.is_some() {
            effective.step_durability = overrides.step_durability;
        }
        if overrides.worker_target.is_some() {
            effective.worker_target.clone_from(&overrides.worker_target);
        }
        if overrides.attribute_store_names.is_some() {
            effective
                .attribute_store_names
                .clone_from(&overrides.attribute_store_names);
        }
    }
    effective
}

pub(crate) fn write_outbox(
    transaction: &mut FlowTransaction<'_>,
    cursor: &mut RunCursor,
    message_id: Vec<u8>,
    target: CrossFlowEndpoint,
    payload: outbox_record::Payload,
    held_values: Vec<StoredValue>,
) {
    let flow_hash = cursor.rows.flow_hash;
    let run_id = cursor.run_id();
    transaction.put_message(
        ColumnFamily::Flow,
        outbox_key(flow_hash, run_id, &message_id),
        &OutboxRecord {
            message_id: message_id.clone(),
            target: Some(target),
            payload: Some(payload),
            written_seq: cursor.seq,
            acknowledged_at_micros: 0,
            held_values,
        },
    );
    transaction.put(
        ColumnFamily::PartitionIndex,
        run_item_index_key(
            transaction.context().partition,
            IndexKind::OutboxPending,
            flow_hash,
            run_id,
            &message_id,
        ),
        Vec::new(),
    );
    cursor.add_effect(ApplyEffect::OutboxMessagesWritten {
        run_id,
        message_ids: vec![message_id],
    });
}

pub(crate) fn message_id(run_seq: u64, index: u64) -> Vec<u8> {
    let mut bytes = run_seq.to_be_bytes().to_vec();
    bytes.extend_from_slice(&index.to_be_bytes());
    bytes
}

/// Reports every terminal status, including termination, to each attached parent.
pub(crate) fn report_child_closed(
    transaction: &mut FlowTransaction<'_>,
    cursor: &mut RunCursor,
    _scope: &CommandScope<'_>,
) -> Result<(), ApplyError> {
    let Some(original_parent) = cursor.record.parent.clone() else {
        return Ok(());
    };
    if cursor
        .record
        .close
        .as_ref()
        .is_some_and(|close| close.is_reported_to_parent)
    {
        return Ok(());
    }
    let mut parents = vec![original_parent];
    for parent in &cursor.record.attached_parents {
        if !parents.contains(parent) {
            parents.push(parent.clone());
        }
    }
    for (index, parent) in parents.into_iter().enumerate() {
        report_terminal_to_parent(
            transaction,
            cursor,
            parent,
            u64::try_from(index).unwrap_or(u64::MAX),
        )?;
    }
    if let Some(close) = cursor.record.close.as_mut() {
        close.is_reported_to_parent = true;
    }
    Ok(())
}

pub(crate) fn report_terminal_to_parent(
    transaction: &mut FlowTransaction<'_>,
    cursor: &mut RunCursor,
    parent: ParentFlowReference,
    index: u64,
) -> Result<(), ApplyError> {
    let Some(target) = parent.parent.clone() else {
        return Err(ApplyError::InvariantViolation {
            detail: "child parent reference has no endpoint".to_owned(),
        });
    };
    let close = cursor.record.close.clone().unwrap_or_default();
    let mut results = Vec::with_capacity(close.results.len());
    for output in &close.results {
        let complete = output
            .completed_step_output
            .as_ref()
            .map(|stored| load_stored_value(transaction, cursor.rows, stored))
            .transpose()?
            .flatten();
        results.push(StepCompletionOutput {
            completed_step_type: output.completed_step_type.clone(),
            completed_step_execution_id: output.completed_step_execution_id.clone(),
            completed_step_output: complete,
        });
    }
    write_outbox(
        transaction,
        cursor,
        message_id(cursor.seq, index),
        target,
        outbox_record::Payload::ChildCompleted(ChildFlowCompletedMessage {
            child_flow_id: cursor.record.flow_id.clone(),
            child_run_id: cursor.run_id().as_bytes().to_vec(),
            child_run_attempt: cursor.attempt(),
            result: Some(FlowResult {
                flow_status: cursor.record.status,
                results,
                error_type: close.error_type,
                error_message: close.error_message,
            }),
            parent: Some(parent),
        }),
        Vec::new(),
    );
    Ok(())
}

pub(crate) fn apply_outbox_ack(
    transaction: &mut FlowTransaction<'_>,
    mut cursor: RunCursor,
    scope: &CommandScope<'_>,
    command: &OutboxAckCommand,
) -> Result<HandlerResult, ApplyError> {
    let mut changed = false;
    for message_id in &command.message_ids {
        let key = outbox_key(cursor.rows.flow_hash, cursor.run_id(), message_id);
        let Some(mut record): Option<OutboxRecord> =
            transaction.get_message(ColumnFamily::Flow, &key, "outbox")?
        else {
            continue;
        };
        if record.acknowledged_at_micros != 0 {
            continue;
        }
        record.acknowledged_at_micros = scope.now_micros;
        transaction.put_message(ColumnFamily::Flow, key, &record);
        transaction.delete(
            ColumnFamily::PartitionIndex,
            run_item_index_key(
                transaction.context().partition,
                IndexKind::OutboxPending,
                cursor.rows.flow_hash,
                cursor.run_id(),
                message_id,
            ),
        );
        arm_timer(
            transaction,
            &mut cursor,
            &TimerId {
                run_attempt: 0,
                kind: Some(timer_id::Kind::OutboxRetention(OutboxRetentionTimerId {
                    message_id: message_id.clone(),
                })),
            },
            scope.now_micros.saturating_add(OUTBOX_RETENTION_MICROS),
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

pub(crate) fn on_outbox_retention_fired(
    transaction: &mut FlowTransaction<'_>,
    cursor: &mut RunCursor,
    timer: &OutboxRetentionTimerId,
) -> Result<(), ApplyError> {
    let key = outbox_key(cursor.rows.flow_hash, cursor.run_id(), &timer.message_id);
    let Some(record): Option<OutboxRecord> =
        transaction.get_message(ColumnFamily::Flow, &key, "outbox")?
    else {
        return Ok(());
    };
    if record.acknowledged_at_micros == 0 {
        return Ok(());
    }
    for (index, stored) in record.held_values.iter().enumerate() {
        release_stored_value(
            transaction,
            cursor.rows,
            stored,
            &ValueHolder::OutboxMessage {
                message_id: timer.message_id.clone(),
                value_index: u32::try_from(index).unwrap_or(u32::MAX),
            },
            cursor.seq,
        )?;
    }
    transaction.delete(ColumnFamily::Flow, key);
    Ok(())
}
