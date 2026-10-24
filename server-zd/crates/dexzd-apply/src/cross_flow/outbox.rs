// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use dexzd_keyspace::row_keys::{outbox_key, run_item_index_key};
use dexzd_keyspace::{ColumnFamily, IndexKind};
use dexzd_protocol::dex::{
    ActiveStepSearchMode, ExecuteMethodFailurePolicy, FlowConfig, FlowResult, FlowTimeoutPolicy,
    StepCompletionOutput, StepDurability, SubFlowCondition, SubFlowReusePolicy,
    WaitForMethodFailurePolicy,
};
use dexzd_protocol::dexzd::v1::{
    ChildFlowCompletedMessage, ChildFlowStartMessage, CrossFlowEndpoint, OutboxAckCommand,
    OutboxRecord, OutboxRetentionTimerId, ParentFlowReference, StoredValue, TimerId, outbox_record,
    timer_id,
};

use crate::ApplyEffect;
use crate::ApplyError;
use crate::command_handlers::command_replies::ok_reply;
use crate::command_handlers::run_cursor::{HistoryKind, RunCursor};
use crate::command_handlers::{CommandScope, HandlerResult};
use crate::state_access::FlowTransaction;
use crate::steps::arm_timer;
use crate::values::{
    ValueHolder, ValueRejection, complete_value_for_outbox, load_transaction_stored_value,
    release_stored_value,
};

const OUTBOX_RETENTION_MICROS: i64 = 24 * 60 * 60 * 1_000_000;
const CHILD_START_MESSAGE_KIND: u8 = 1;
const CHILD_COMPLETED_MESSAGE_KIND: u8 = 2;

/// Writes one complete child-start message per SubFlow condition.
pub(crate) fn start_child_flows(
    transaction: &mut FlowTransaction<'_>,
    cursor: &mut RunCursor,
    scope: &CommandScope<'_>,
    step_execution_id: &str,
    sub_flow_conditions: &[SubFlowCondition],
    child_ids: &[String],
) -> Result<Result<(), ValueRejection>, ApplyError> {
    if child_ids.len() != sub_flow_conditions.len() {
        return Err(ApplyError::InvariantViolation {
            detail: "SubFlow condition and child ID counts differ".to_owned(),
        });
    }
    validate_child_start_conditions(cursor.record.flow_config.as_ref(), sub_flow_conditions)
        .map_err(|detail| ApplyError::InvariantViolation { detail })?;
    for (condition_index, condition) in sub_flow_conditions.iter().enumerate() {
        let sub_flow_index =
            i32::try_from(condition_index).expect("SubFlow condition count fits a signed index");
        let child_flow_id = child_ids[condition_index].clone();
        let message_id = message_id(
            CHILD_START_MESSAGE_KIND,
            cursor.seq,
            u64::try_from(condition_index).expect("SubFlow condition count fits a u64 index"),
        );
        let mut complete_condition = condition.clone();
        if let Some(value) = condition.step_input.as_ref() {
            let complete = match complete_value_for_outbox(
                transaction,
                cursor.rows,
                cursor.value_limits(),
                value,
            )? {
                Ok(complete) => complete,
                Err(rejection) => return Ok(Err(rejection)),
            };
            complete_condition.step_input = Some(complete);
        }
        if let Some(options) = complete_condition.options.as_mut() {
            for write in &mut options.attributes {
                if let Some(value) = write.value.as_ref() {
                    let complete = match complete_value_for_outbox(
                        transaction,
                        cursor.rows,
                        cursor.value_limits(),
                        value,
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
            step_execution_id: step_execution_id.to_owned(),
            sub_flow_index,
            condition_id: condition.condition_id.clone(),
        };
        let request_id = format!(
            "{}:{}:{}:{sub_flow_index}",
            cursor.run_id(),
            cursor.attempt(),
            step_execution_id
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
            Vec::new(),
        );
    }
    Ok(Ok(()))
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

pub(crate) fn validate_child_start_conditions(
    parent_config: Option<&FlowConfig>,
    conditions: &[SubFlowCondition],
) -> Result<(), String> {
    let parent_config = parent_config.ok_or("SubFlow start requires the parent FlowConfig")?;
    for (index, condition) in conditions.iter().enumerate() {
        let expected_index =
            i32::try_from(index).expect("SubFlow condition count fits a signed index");
        let flow_config = effective_flow_config(
            Some(parent_config),
            condition
                .options
                .as_ref()
                .and_then(|options| options.flow_config_override.as_ref()),
        );
        validate_child_start_parameters(condition, &flow_config, expected_index)
            .map_err(|detail| format!("SubFlow condition at index {index}: {detail}"))?;
    }
    Ok(())
}

pub(crate) fn validate_child_start_parameters(
    condition: &SubFlowCondition,
    flow_config: &FlowConfig,
    expected_index: i32,
) -> Result<(), String> {
    if condition.sub_flow_index != expected_index {
        return Err(format!(
            "unstable index {} (expected {expected_index})",
            condition.sub_flow_index
        ));
    }
    if condition.sub_flow_type.is_empty() || condition.start_step_type.is_empty() {
        return Err("Flow and starting Step types are required".to_owned());
    }
    if ['/', '$', ':']
        .iter()
        .any(|character| condition.start_step_type.contains(*character))
    {
        return Err("starting Step type contains a reserved character".to_owned());
    }
    if let Some(step_options) = condition.step_options.as_ref()
        && (step_options.wait_for_failure_policy
            == WaitForMethodFailurePolicy::ProceedOnFailure as i32
            || step_options.execute_failure_policy
                == ExecuteMethodFailurePolicy::ProceedToConfiguredStep as i32
            || !step_options.execute_failure_proceed_step_type.is_empty()
            || step_options.execute_failure_proceed_step_options.is_some())
    {
        return Err("SubFlow Step failure proceed policy is unsupported".to_owned());
    }
    let options = condition.options.as_ref().cloned().unwrap_or_default();
    if options.flow_start_delay_seconds < 0 {
        return Err("SubFlow start delay must be non-negative".to_owned());
    }
    if options.flow_start_delay_seconds > 0 {
        return Err("SubFlow start delay is unsupported".to_owned());
    }
    if options.retry_policy.is_some() {
        return Err("SubFlow Flow retry policy is unsupported".to_owned());
    }
    if SubFlowReusePolicy::try_from(options.reuse_policy).is_err() {
        return Err("unknown SubFlow reuse policy".to_owned());
    }
    let timeout_policy =
        resolved_timeout_policy(options.flow_timeout_seconds, options.flow_timeout_policy)?;
    if let Some(handler) = options.timeout_handler_options.as_ref() {
        if options.flow_timeout_seconds <= 0 || timeout_policy != FlowTimeoutPolicy::Handler {
            return Err(
                "Flow timeout handler options require the Handler policy and a positive timeout"
                    .to_owned(),
            );
        }
        if handler.method_timeout_seconds < 0 {
            return Err("Flow timeout handler method timeout must be non-negative".to_owned());
        }
        if handler.heartbeat_timeout_seconds < 0 {
            return Err("Flow timeout handler heartbeat timeout must be non-negative".to_owned());
        }
        if handler.failure_policy == ExecuteMethodFailurePolicy::ProceedToConfiguredStep as i32 {
            return Err("Flow timeout handler failure proceed policy is unsupported".to_owned());
        }
        if ExecuteMethodFailurePolicy::try_from(handler.failure_policy).is_err() {
            return Err("unknown Flow timeout handler failure policy".to_owned());
        }
        if !handler.failure_proceed_step_type.is_empty()
            || handler.failure_proceed_step_options.is_some()
        {
            return Err(
                "Flow timeout handler failure target requires the proceed policy".to_owned(),
            );
        }
    }
    validate_effective_flow_config(flow_config)
}

fn resolved_timeout_policy(timeout_seconds: i32, policy: i32) -> Result<FlowTimeoutPolicy, String> {
    if timeout_seconds < 0 {
        return Err("flow timeout must be non-negative".to_owned());
    }
    let policy = FlowTimeoutPolicy::try_from(policy)
        .map_err(|_| "unknown flow timeout policy".to_owned())?;
    if timeout_seconds == 0 && policy != FlowTimeoutPolicy::Unspecified {
        return Err("flow timeout policy requires a positive timeout".to_owned());
    }
    if timeout_seconds > 0 && policy == FlowTimeoutPolicy::Unspecified {
        return Ok(FlowTimeoutPolicy::Fail);
    }
    Ok(policy)
}

fn validate_effective_flow_config(flow_config: &FlowConfig) -> Result<(), String> {
    if flow_config
        .continue_as_new_threshold
        .is_some_and(|value| value < 0)
        || flow_config
            .continue_as_new_page_size_in_bytes
            .is_some_and(|value| value < 0)
    {
        return Err("FlowConfig thresholds must be non-negative".to_owned());
    }
    if flow_config
        .active_step_search_mode
        .is_some_and(|value| ActiveStepSearchMode::try_from(value).is_err())
        || flow_config
            .step_durability
            .is_some_and(|value| StepDurability::try_from(value).is_err())
    {
        return Err("FlowConfig has an unknown enum value".to_owned());
    }
    let Some(target) = flow_config.worker_target.as_ref() else {
        return Err("SubFlow requires a pull Worker target".to_owned());
    };
    if !target.is_pull_mode || !target.address.is_empty() || target.is_headless_address {
        return Err("SubFlow requires a pull Worker target".to_owned());
    }
    Ok(())
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

pub(crate) fn message_id(kind: u8, run_seq: u64, index: u64) -> Vec<u8> {
    let index_bytes = index.to_be_bytes();
    assert_eq!(index_bytes[0], 0, "outbox index fits 56 bits");
    let mut bytes = Vec::with_capacity(16);
    bytes.push(kind);
    bytes.extend_from_slice(&run_seq.to_be_bytes());
    bytes.extend_from_slice(&index_bytes[1..]);
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
            u64::try_from(index).expect("attached parent count fits a u64 index"),
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
            .map(|stored| {
                load_transaction_stored_value(transaction, cursor.rows, stored)?.ok_or_else(|| {
                    ApplyError::InvariantViolation {
                        detail: "stored child output has no value row".to_owned(),
                    }
                })
            })
            .transpose()?;
        results.push(StepCompletionOutput {
            completed_step_type: output.completed_step_type.clone(),
            completed_step_execution_id: output.completed_step_execution_id.clone(),
            completed_step_output: complete,
        });
    }
    write_outbox(
        transaction,
        cursor,
        message_id(CHILD_COMPLETED_MESSAGE_KIND, cursor.seq, index),
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
                value_index: u32::try_from(index)
                    .expect("retained outbox value count fits a u32 index"),
            },
            cursor.seq,
        )?;
    }
    transaction.delete(ColumnFamily::Flow, key);
    Ok(())
}
