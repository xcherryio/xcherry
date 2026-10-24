// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Attribute writes, Channel publications, and Channel deletions shared by every command that
//! carries them (plan sections 9.2 and 9.3). Effects apply in the order: Attribute writes,
//! deletions, publications.

use dexzd_keyspace::row_keys::{
    hard_delete_current_key, hard_delete_pending_key, run_item_index_key,
};
use dexzd_keyspace::{ColumnFamily, IndexKind};
use dexzd_protocol::dex::{
    ActionPermissionMappings, AttributeWrite, ChannelMessage, ChannelMessageDeletion, Value,
};
use dexzd_protocol::dexzd::v1::{
    AttributeRecord, ChannelMessageRecord, HardDeleteObjectRecord, HardDeleteObjectReference,
    StoredValue, stored_value,
};

use crate::command_handlers::run_cursor::RunCursor;
use crate::state_access::FlowTransaction;
use crate::values::{
    ValueHolder, ValueRejection, is_null_value, release_stored_value, store_value,
};
use crate::{ApplyEffect, ApplyError, WaitableChange};

/// Upper bound of messages one command may publish; keeps enqueue seqs unique per command.
const MAX_MESSAGES_PER_COMMAND: u64 = 1_000_000;

/// Applies Attribute writes in order (later writes win) and the permission projection.
///
/// Returns the changed keys, or the first value the caller must reject.
pub(crate) fn apply_attribute_writes(
    transaction: &mut FlowTransaction<'_>,
    cursor: &mut RunCursor,
    writes: &[AttributeWrite],
    hard_delete_objects: &[HardDeleteObjectReference],
    permission_mappings: Option<&ActionPermissionMappings>,
) -> Result<Result<Vec<String>, ValueRejection>, ApplyError> {
    let mut changed_keys = Vec::with_capacity(writes.len());
    for (write_index, write) in writes.iter().enumerate() {
        let hard_delete_object = hard_delete_objects
            .iter()
            .find(|object| usize::try_from(object.write_index).ok() == Some(write_index));
        let outcome = if write.is_hard_delete_on_null {
            apply_hard_delete_write(transaction, cursor, write, hard_delete_object)?;
            Ok(())
        } else {
            apply_plain_write(transaction, cursor, write)?
        };
        if let Err(rejection) = outcome {
            return Ok(Err(rejection));
        }
        changed_keys.push(write.key.clone());
    }
    if let Some(mappings) = permission_mappings {
        apply_permission_mappings(transaction, cursor, writes, mappings)?;
    }
    if !changed_keys.is_empty() {
        let mut keys = changed_keys.clone();
        keys.sort();
        keys.dedup();
        cursor.wake(WaitableChange::AttributesChanged { keys });
    }
    Ok(Ok(changed_keys))
}

fn apply_plain_write(
    transaction: &mut FlowTransaction<'_>,
    cursor: &mut RunCursor,
    write: &AttributeWrite,
) -> Result<Result<(), ValueRejection>, ApplyError> {
    // Release first: a write may name the value the Attribute already holds.
    release_previous_attribute(transaction, cursor, &write.key)?;
    if is_null_value(write.value.as_ref()) {
        cursor.rows.delete_attribute(transaction, &write.key);
        return Ok(Ok(()));
    }
    let holder = ValueHolder::Attribute {
        attribute_key: write.key.clone(),
    };
    let value = write.value.clone().unwrap_or_default();
    let limits = cursor.value_limits();
    let stored = match store_value(
        transaction,
        cursor.rows,
        limits,
        &mut cursor.values,
        &value,
        &holder,
    )? {
        Ok(stored) => stored,
        Err(rejection) => return Ok(Err(rejection)),
    };
    cursor.rows.put_attribute(
        transaction,
        &write.key,
        &AttributeRecord {
            value: Some(stored),
            index_config: write.index_config.clone(),
            sync_config: write.sync_config,
            written_seq: cursor.seq,
            is_hard_delete_on_null: false,
        },
    );
    Ok(Ok(()))
}

/// Hard-delete Attributes live in their own objects; writing or clearing one schedules the old
/// object for deletion (plan section 10.3).
fn apply_hard_delete_write(
    transaction: &mut FlowTransaction<'_>,
    cursor: &mut RunCursor,
    write: &AttributeWrite,
    hard_delete_object: Option<&HardDeleteObjectReference>,
) -> Result<(), ApplyError> {
    release_previous_attribute(transaction, cursor, &write.key)?;
    // The owner replaced the value with an uploaded object; its reference means "set" even
    // though the value in the command is now empty.
    match (hard_delete_object, is_null_value(write.value.as_ref())) {
        (Some(object), _) => {
            let current_key =
                hard_delete_current_key(cursor.rows.flow_hash, cursor.run_id(), &write.key);
            transaction.put_message(
                ColumnFamily::Flow,
                current_key,
                &HardDeleteObjectRecord {
                    object: Some(object.clone()),
                    written_seq: cursor.seq,
                },
            );
            cursor.rows.put_attribute(
                transaction,
                &write.key,
                &AttributeRecord {
                    value: Some(StoredValue {
                        kind: Some(stored_value::Kind::HardDeleteObject(object.clone())),
                    }),
                    index_config: None,
                    sync_config: None,
                    written_seq: cursor.seq,
                    is_hard_delete_on_null: true,
                },
            );
        }
        (None, true) => cursor.rows.delete_attribute(transaction, &write.key),
        (None, false) => {
            return Err(ApplyError::InvariantViolation {
                detail: format!(
                    "hard-delete write of {:?} has no uploaded object",
                    write.key
                ),
            });
        }
    }
    Ok(())
}

/// Releases what an Attribute holds before a write replaces or deletes it: the holder of a
/// stored value, or a hard-delete object, which is scheduled for deletion.
fn release_previous_attribute(
    transaction: &mut FlowTransaction<'_>,
    cursor: &mut RunCursor,
    attribute_key: &str,
) -> Result<(), ApplyError> {
    let Some(previous) = cursor.rows.attribute(transaction, attribute_key)? else {
        return Ok(());
    };
    match previous.value {
        Some(StoredValue {
            kind: Some(stored_value::Kind::HardDeleteObject(object)),
        }) => {
            transaction.delete(
                ColumnFamily::Flow,
                hard_delete_current_key(cursor.rows.flow_hash, cursor.run_id(), attribute_key),
            );
            schedule_hard_delete(transaction, cursor, object);
        }
        Some(stored) => {
            let holder = ValueHolder::Attribute {
                attribute_key: attribute_key.to_owned(),
            };
            release_stored_value(transaction, cursor.rows, &stored, &holder, cursor.seq)?;
        }
        None => {}
    }
    Ok(())
}

pub(crate) fn schedule_hard_delete(
    transaction: &mut FlowTransaction<'_>,
    cursor: &mut RunCursor,
    object: HardDeleteObjectReference,
) {
    let flow_hash = cursor.rows.flow_hash;
    let run_id = cursor.run_id();
    let partition = transaction.context().partition;
    transaction.put_message(
        ColumnFamily::Flow,
        hard_delete_pending_key(flow_hash, run_id, &object.object_id),
        &HardDeleteObjectRecord {
            object: Some(object.clone()),
            written_seq: cursor.seq,
        },
    );
    transaction.put(
        ColumnFamily::PartitionIndex,
        run_item_index_key(
            partition,
            IndexKind::HardDeletePending,
            flow_hash,
            run_id,
            object.object_id.as_bytes(),
        ),
        Vec::new(),
    );
    cursor.add_effect(ApplyEffect::HardDeleteObjectsPending {
        run_id,
        object_ids: vec![object.object_id],
    });
}

/// Adds work queue permissions for written Attributes whose new value equals a mapped value.
/// The set only grows (plan section 9.3).
pub(crate) fn apply_permission_mappings(
    transaction: &mut FlowTransaction<'_>,
    cursor: &mut RunCursor,
    writes: &[AttributeWrite],
    mappings: &ActionPermissionMappings,
) -> Result<(), ApplyError> {
    let mut permission_set = cursor.rows.permission_set(transaction)?;
    let mut is_changed = false;
    for mapping in &mappings.mappings {
        let Some(write) = writes
            .iter()
            .rev()
            .find(|write| write.key == mapping.attribute_key)
        else {
            continue;
        };
        let Some(value) = &write.value else { continue };
        let is_match = mapping
            .equal_values
            .iter()
            .any(|candidate| values_equal(candidate, value));
        if is_match
            && !permission_set
                .permissions
                .contains(&mapping.required_permission)
        {
            permission_set
                .permissions
                .push(mapping.required_permission.clone());
            is_changed = true;
        }
    }
    if is_changed {
        permission_set.permissions.sort();
        cursor.rows.put_permission_set(transaction, &permission_set);
    }
    Ok(())
}

fn values_equal(left: &Value, right: &Value) -> bool {
    left == right
}

/// Deletes Channel messages by ID. Best effort: missing messages are skipped. Returns the
/// deletions that matched a pending message.
pub(crate) fn apply_channel_deletions(
    transaction: &mut FlowTransaction<'_>,
    cursor: &mut RunCursor,
    deletions: &[ChannelMessageDeletion],
) -> Result<Vec<ChannelMessageDeletion>, ApplyError> {
    let mut deleted = Vec::new();
    for deletion in deletions {
        if let Some(record) = find_pending_message(
            transaction,
            cursor,
            &deletion.channel_name,
            &deletion.message_id,
        )? {
            remove_pending_message(transaction, cursor, &record)?;
            deleted.push(deletion.clone());
        }
    }
    Ok(deleted)
}

/// Returns the pending message with this ID, if any.
pub(crate) fn find_pending_message(
    transaction: &FlowTransaction<'_>,
    cursor: &RunCursor,
    channel_name: &str,
    message_id: &str,
) -> Result<Option<ChannelMessageRecord>, ApplyError> {
    Ok(cursor
        .rows
        .channel_messages(transaction, channel_name, usize::MAX)?
        .into_iter()
        .find(|record| record.message_id == message_id))
}

fn remove_pending_message(
    transaction: &mut FlowTransaction<'_>,
    cursor: &mut RunCursor,
    record: &ChannelMessageRecord,
) -> Result<(), ApplyError> {
    cursor.rows.delete_channel_message(transaction, record);
    if let Some(stored) = &record.value {
        let holder = ValueHolder::ChannelMessage {
            channel_name: record.channel_name.clone(),
            enqueued_seq: record.enqueued_seq,
        };
        release_stored_value(transaction, cursor.rows, stored, &holder, cursor.seq)?;
    }
    let mut meta = cursor
        .rows
        .channel_meta(transaction, &record.channel_name)?;
    meta.pending_count = meta.pending_count.saturating_sub(1);
    cursor
        .rows
        .put_channel_meta(transaction, &record.channel_name, &meta);
    Ok(())
}

/// Appends messages to their channels in list order. Returns the channels that changed, or the
/// first value the caller must reject.
pub(crate) fn apply_channel_publications(
    transaction: &mut FlowTransaction<'_>,
    cursor: &mut RunCursor,
    messages: &[ChannelMessage],
) -> Result<Result<Vec<String>, ValueRejection>, ApplyError> {
    let mut changed_channels: Vec<String> = Vec::new();
    for (index, message) in messages.iter().enumerate() {
        let enqueued_seq =
            cursor.seq * MAX_MESSAGES_PER_COMMAND + u64::try_from(index).unwrap_or(u64::MAX);
        let holder = ValueHolder::ChannelMessage {
            channel_name: message.channel_name.clone(),
            enqueued_seq,
        };
        let value = message.value.clone().unwrap_or_default();
        let limits = cursor.value_limits();
        let stored = match store_value(
            transaction,
            cursor.rows,
            limits,
            &mut cursor.values,
            &value,
            &holder,
        )? {
            Ok(stored) => stored,
            Err(rejection) => return Ok(Err(rejection)),
        };
        cursor.rows.put_channel_message(
            transaction,
            &ChannelMessageRecord {
                channel_name: message.channel_name.clone(),
                message_id: message.message_id.clone(),
                value: Some(stored),
                enqueued_seq,
            },
        );
        let mut meta = cursor
            .rows
            .channel_meta(transaction, &message.channel_name)?;
        meta.pending_count += 1;
        cursor
            .rows
            .put_channel_meta(transaction, &message.channel_name, &meta);
        if !changed_channels.contains(&message.channel_name) {
            changed_channels.push(message.channel_name.clone());
        }
    }
    Ok(Ok(changed_channels))
}

/// The data effects of a Worker method result, applied in the Go Server's order: Attribute
/// writes, deletions, publications, then locals.
pub(crate) struct MethodEffects<'response> {
    pub(crate) writes: &'response [AttributeWrite],
    pub(crate) deletions: &'response [ChannelMessageDeletion],
    pub(crate) publications: &'response [ChannelMessage],
    pub(crate) locals: &'response [dexzd_protocol::dex::Kv],
    pub(crate) permission_mappings: Option<&'response ActionPermissionMappings>,
    pub(crate) hard_delete_objects: &'response [HardDeleteObjectReference],
}

/// Applies a method result's data effects to the run and the Step's locals, then lets waiting
/// Steps react to the changed channels.
pub(crate) fn apply_method_effects(
    transaction: &mut FlowTransaction<'_>,
    cursor: &mut RunCursor,
    step: &mut dexzd_protocol::dexzd::v1::StepExecutionRecord,
    effects: &MethodEffects<'_>,
    now_micros: i64,
) -> Result<Result<(), ValueRejection>, ApplyError> {
    if let Err(rejection) = apply_attribute_writes(
        transaction,
        cursor,
        effects.writes,
        effects.hard_delete_objects,
        effects.permission_mappings,
    )? {
        return Ok(Err(rejection));
    }
    apply_channel_deletions(transaction, cursor, effects.deletions)?;
    let changed_channels =
        match apply_channel_publications(transaction, cursor, effects.publications)? {
            Ok(channels) => channels,
            Err(rejection) => return Ok(Err(rejection)),
        };
    for local in effects.locals {
        let holder = ValueHolder::StepLocal {
            step_execution_id: step.step_execution_id.clone(),
            local_key: local.key.clone(),
        };
        if let Some(position) = step
            .locals
            .iter()
            .position(|existing| existing.key == local.key)
        {
            let previous = step.locals.remove(position);
            if let Some(stored) = &previous.value {
                release_stored_value(transaction, cursor.rows, stored, &holder, cursor.seq)?;
            }
        }
        if is_null_value(local.value.as_ref()) {
            continue;
        }
        let value = local.value.clone().unwrap_or_default();
        let limits = cursor.value_limits();
        let stored = match store_value(
            transaction,
            cursor.rows,
            limits,
            &mut cursor.values,
            &value,
            &holder,
        )? {
            Ok(stored) => stored,
            Err(rejection) => return Ok(Err(rejection)),
        };
        step.locals.push(dexzd_protocol::dexzd::v1::StoredKeyValue {
            key: local.key.clone(),
            value: Some(stored),
        });
    }
    step.locals.sort_by(|left, right| left.key.cmp(&right.key));
    crate::steps::wait_conditions::on_channels_changed(
        transaction,
        cursor,
        now_micros,
        &changed_channels,
    )?;
    Ok(Ok(()))
}
