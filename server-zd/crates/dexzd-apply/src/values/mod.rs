// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Value storage and holder sets (plan section 10.2).
//!
//! Small values stay inline in state rows. Larger values get a [`ValueId`] and a row in the
//! value table, and every slot or pinned call that uses one is a holder. A value whose holder
//! set becomes empty moves to table G and is deleted when the time travel window passes it.

mod garbage;
mod storage_accounting;
mod time_travel;
mod value_holder;

pub(crate) use garbage::enforce_superseded_byte_cap;
#[expect(unused_imports, reason = "Available to window maintenance handlers.")]
pub(crate) use garbage::{prune_garbage, superseded_bytes_over_cap};
pub(crate) use storage_accounting::{
    active_run_storage_bytes, projected_superseded_value_bytes, reconcile_run_storage,
    total_run_storage_bytes,
};
pub(crate) use time_travel::rewrite_holders_for_time_travel;
pub(crate) use value_holder::ValueHolder;

use dexzd_keyspace::row_keys::{prefix_range, value_garbage_key, value_holder_key, value_row_key};
use dexzd_keyspace::{ColumnFamily, FlowHash, RunId, RunTable, ValueId, ValueReference};
use dexzd_protocol::dex::Value;
use dexzd_protocol::dex::value::Kind;
use dexzd_protocol::dexzd::v1::{
    StoredValue, StoredValueId, StoredValueReference, ValueBirthRecord, ValueGarbageRecord,
    ValueRecord, stored_value,
};
use prost::Message;

use crate::ApplyError;
use crate::FlowStateReader;
use crate::state_access::{FlowTransaction, RunRows};

/// Why a value from a client or Worker cannot be stored.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ValueRejection {
    /// The value is larger than the run's maximum value size.
    TooLarge {
        size_bytes: u64,
        max_value_bytes: u64,
    },
    /// The value is a reference that does not name a live value of this run.
    UnknownReference { reference: String },
}

impl ValueRejection {
    pub(crate) fn message(&self) -> String {
        match self {
            Self::TooLarge {
                size_bytes,
                max_value_bytes,
            } => format!(
                "value of {size_bytes} bytes exceeds the maximum of {max_value_bytes} bytes"
            ),
            Self::UnknownReference { reference } => {
                format!("value reference {reference:?} does not name a value of this run")
            }
        }
    }
}

/// Assigns value IDs for the values one command stores.
#[derive(Clone)]
pub(crate) struct ValueIdAllocator {
    run_seq: u64,
    next_index: u32,
}

impl ValueIdAllocator {
    pub(crate) fn new(run_seq: u64) -> Self {
        Self {
            run_seq,
            next_index: 0,
        }
    }

    fn allocate(&mut self) -> ValueId {
        let value_id = ValueId {
            run_seq: self.run_seq,
            index_in_command: self.next_index,
        };
        self.next_index += 1;
        value_id
    }
}

/// Limits that decide how a value is stored.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ValueLimits {
    pub(crate) wire_inline_threshold_bytes: u32,
    pub(crate) max_value_bytes: u64,
}

/// Stores `value` for `holder` and returns how state rows keep it.
///
/// References that a Worker echoed back must name a live value of this run (plan R1').
/// Returns `Ok(Err(..))` for values the caller must reject without failing the partition.
pub(crate) fn store_value(
    transaction: &mut FlowTransaction<'_>,
    rows: RunRows,
    limits: ValueLimits,
    allocator: &mut ValueIdAllocator,
    value: &Value,
    holder: &ValueHolder,
) -> Result<Result<StoredValue, ValueRejection>, ApplyError> {
    if let Some(reference_text) = reference_text(value) {
        let Some(reference) = parse_own_reference(reference_text, rows) else {
            return Ok(Err(ValueRejection::UnknownReference {
                reference: reference_text.to_owned(),
            }));
        };
        let Some(birth) = value_birth(transaction, rows, reference.value_id)? else {
            return Ok(Err(ValueRejection::UnknownReference {
                reference: reference_text.to_owned(),
            }));
        };
        let size_bytes = birth.size_bytes;
        add_holder(transaction, rows, reference.value_id, birth, holder);
        return Ok(Ok(table_value(
            reference.value_id,
            size_bytes,
            is_string_reference(value),
        )));
    }
    let size_bytes = u64::try_from(value.encoded_len()).unwrap_or(u64::MAX);
    if size_bytes > limits.max_value_bytes {
        return Ok(Err(ValueRejection::TooLarge {
            size_bytes,
            max_value_bytes: limits.max_value_bytes,
        }));
    }
    if size_bytes <= u64::from(limits.wire_inline_threshold_bytes) || !is_referenceable(value) {
        return Ok(Ok(StoredValue {
            kind: Some(stored_value::Kind::InlineValue(value.clone())),
        }));
    }
    let value_id = allocator.allocate();
    transaction.put_message(
        ColumnFamily::Value,
        value_row_key(rows.flow_hash, rows.run_id, RunTable::Value, value_id),
        &ValueRecord {
            value: Some(value.clone()),
        },
    );
    transaction.put_message(
        ColumnFamily::Flow,
        value_row_key(rows.flow_hash, rows.run_id, RunTable::ValueBirth, value_id),
        &ValueBirthRecord {
            size_bytes,
            written_seq: value_id.run_seq,
            garbage_seq: 0,
        },
    );
    put_holder(transaction, rows, value_id, holder);
    Ok(Ok(table_value(
        value_id,
        size_bytes,
        matches!(value.kind, Some(Kind::StringValue(_))),
    )))
}

/// Adds `holder` to a value another slot already holds (for example a Step input copied into a
/// history display or a recovery Step). Inline values need no bookkeeping.
pub(crate) fn hold_stored_value(
    transaction: &mut FlowTransaction<'_>,
    rows: RunRows,
    stored: &StoredValue,
    holder: &ValueHolder,
) -> Result<(), ApplyError> {
    if let Some((value_id, _)) = table_value_id(stored) {
        let Some(birth) = value_birth(transaction, rows, value_id)? else {
            return Err(ApplyError::InvariantViolation {
                detail: format!("stored value {value_id:?} has no live value row"),
            });
        };
        add_holder(transaction, rows, value_id, birth, holder);
    }
    Ok(())
}

/// Removes `holder` from the value; when no holder remains, the value moves to table G at
/// `current_seq` (plan R2', R3').
pub(crate) fn release_stored_value(
    transaction: &mut FlowTransaction<'_>,
    rows: RunRows,
    stored: &StoredValue,
    holder: &ValueHolder,
    current_seq: u64,
) -> Result<(), ApplyError> {
    let Some((value_id, _)) = table_value_id(stored) else {
        return Ok(());
    };
    // Removing a holder that does not exist does nothing (plan section 10.2).
    let holder_key = value_holder_key(rows.flow_hash, rows.run_id, value_id, &holder.encode());
    if transaction.get(ColumnFamily::Flow, &holder_key)?.is_none() {
        return Ok(());
    }
    transaction.delete(ColumnFamily::Flow, holder_key);
    let holder_prefix = value_holder_key(rows.flow_hash, rows.run_id, value_id, &[]);
    let (start, end) = prefix_range(&holder_prefix);
    if !transaction
        .scan(ColumnFamily::Flow, &start, &end, 1)?
        .is_empty()
    {
        return Ok(());
    }
    let birth_key = value_row_key(rows.flow_hash, rows.run_id, RunTable::ValueBirth, value_id);
    let Some(mut birth): Option<ValueBirthRecord> =
        transaction.get_message(ColumnFamily::Flow, &birth_key, "value birth")?
    else {
        return Ok(());
    };
    birth.garbage_seq = current_seq;
    transaction.put_message(ColumnFamily::Flow, birth_key, &birth);
    transaction.put_message(
        ColumnFamily::Flow,
        value_garbage_key(rows.flow_hash, rows.run_id, current_seq, value_id),
        &ValueGarbageRecord {
            size_bytes: stored_value_size(stored),
        },
    );
    Ok(())
}

/// Returns the value as Workers and clients receive it: inline, or a `zd1|...` reference.
pub(crate) fn wire_value(rows: RunRows, stored: &StoredValue) -> Value {
    match &stored.kind {
        Some(stored_value::Kind::InlineValue(value)) => value.clone(),
        Some(stored_value::Kind::TableValue(reference)) => {
            let value_id = reference.value_id.unwrap_or_default();
            let text = ValueReference {
                run_id: rows.run_id,
                value_id: ValueId {
                    run_seq: value_id.run_seq,
                    index_in_command: value_id.index_in_command,
                },
            }
            .to_string();
            let kind = if reference.is_string_value {
                Kind::InternalBlobIdForStringValue(text)
            } else {
                Kind::InternalBlobIdForObjValue(text)
            };
            Value { kind: Some(kind) }
        }
        Some(stored_value::Kind::HardDeleteObject(_)) | None => Value { kind: None },
    }
}

/// Reads the complete value, or `None` if a table value was already collected.
pub(crate) fn load_stored_value(
    state: &dyn FlowStateReader,
    rows: RunRows,
    stored: &StoredValue,
) -> Result<Option<Value>, ApplyError> {
    match &stored.kind {
        Some(stored_value::Kind::InlineValue(value)) => Ok(Some(value.clone())),
        Some(stored_value::Kind::TableValue(_)) => {
            let Some((value_id, _)) = table_value_id(stored) else {
                return Ok(None);
            };
            let key = value_row_key(rows.flow_hash, rows.run_id, RunTable::Value, value_id);
            let record = state.get(ColumnFamily::Value, &key)?;
            let record = record
                .map(|bytes| {
                    ValueRecord::decode(bytes.as_slice()).map_err(|error| {
                        ApplyError::CorruptState {
                            table: "value",
                            key: key.iter().map(|byte| format!("{byte:02x}")).collect(),
                            detail: error.to_string(),
                        }
                    })
                })
                .transpose()?;
            Ok(record.and_then(|record| record.value))
        }
        Some(stored_value::Kind::HardDeleteObject(_)) | None => Ok(None),
    }
}

/// Size accounted for a stored value.
pub(crate) fn stored_value_size(stored: &StoredValue) -> u64 {
    match &stored.kind {
        Some(stored_value::Kind::InlineValue(value)) => {
            u64::try_from(value.encoded_len()).unwrap_or(u64::MAX)
        }
        Some(stored_value::Kind::TableValue(reference)) => reference.size_bytes,
        Some(stored_value::Kind::HardDeleteObject(object)) => object.size_bytes,
        None => 0,
    }
}

/// Loads a wire reference; invalid, foreign, hard-delete, or collected references return `None`; state failures return errors.
pub fn load_for_wire(
    state: &dyn FlowStateReader,
    flow_hash: FlowHash,
    run_id: RunId,
    reference: &str,
) -> Result<Option<Value>, ApplyError> {
    let Ok(parsed): Result<ValueReference, _> = reference.parse() else {
        return Ok(None);
    };
    if parsed.run_id != run_id {
        return Ok(None);
    }
    load_stored_value(
        state,
        RunRows::new(flow_hash, run_id),
        &table_value(parsed.value_id, 0, false),
    )
}

/// True when the value is a JSON null or absent, which deletes an Attribute.
pub(crate) fn is_null_value(value: Option<&Value>) -> bool {
    match value {
        None => true,
        Some(value) => matches!(value.kind, None | Some(Kind::NullValue(_))),
    }
}

fn table_value(value_id: ValueId, size_bytes: u64, is_string_value: bool) -> StoredValue {
    StoredValue {
        kind: Some(stored_value::Kind::TableValue(StoredValueReference {
            value_id: Some(StoredValueId {
                run_seq: value_id.run_seq,
                index_in_command: value_id.index_in_command,
            }),
            size_bytes,
            is_string_value,
        })),
    }
}

fn table_value_id(stored: &StoredValue) -> Option<(ValueId, u64)> {
    match &stored.kind {
        Some(stored_value::Kind::TableValue(reference)) => {
            let value_id = reference.value_id?;
            Some((
                ValueId {
                    run_seq: value_id.run_seq,
                    index_in_command: value_id.index_in_command,
                },
                reference.size_bytes,
            ))
        }
        _ => None,
    }
}

fn reference_text(value: &Value) -> Option<&str> {
    match &value.kind {
        Some(Kind::InternalBlobIdForStringValue(text) | Kind::InternalBlobIdForObjValue(text)) => {
            Some(text.as_str())
        }
        _ => None,
    }
}

fn is_string_reference(value: &Value) -> bool {
    matches!(value.kind, Some(Kind::InternalBlobIdForStringValue(_)))
}

/// Only strings and objects become table values; scalars always stay inline.
fn is_referenceable(value: &Value) -> bool {
    matches!(value.kind, Some(Kind::StringValue(_) | Kind::ObjValue(_)))
}

fn parse_own_reference(text: &str, rows: RunRows) -> Option<ValueReference> {
    let reference: ValueReference = text.parse().ok()?;
    (reference.run_id == rows.run_id).then_some(reference)
}

fn value_birth(
    transaction: &FlowTransaction<'_>,
    rows: RunRows,
    value_id: ValueId,
) -> Result<Option<ValueBirthRecord>, ApplyError> {
    let birth: Option<ValueBirthRecord> = transaction.get_message(
        ColumnFamily::Flow,
        &value_row_key(rows.flow_hash, rows.run_id, RunTable::ValueBirth, value_id),
        "value birth",
    )?;
    // A value already deleted by window pruning has no value row, even if B was not yet cleared.
    let is_live = transaction
        .get(
            ColumnFamily::Value,
            &value_row_key(rows.flow_hash, rows.run_id, RunTable::Value, value_id),
        )?
        .is_some();
    Ok(birth.filter(|_| is_live))
}

fn add_holder(
    transaction: &mut FlowTransaction<'_>,
    rows: RunRows,
    value_id: ValueId,
    mut birth: ValueBirthRecord,
    holder: &ValueHolder,
) {
    // A value in G is revived: drop its garbage row so pruning keeps it (plan R1').
    if birth.garbage_seq != 0 {
        transaction.delete(
            ColumnFamily::Flow,
            value_garbage_key(rows.flow_hash, rows.run_id, birth.garbage_seq, value_id),
        );
        birth.garbage_seq = 0;
        transaction.put_message(
            ColumnFamily::Flow,
            value_row_key(rows.flow_hash, rows.run_id, RunTable::ValueBirth, value_id),
            &birth,
        );
    }
    put_holder(transaction, rows, value_id, holder);
}

fn put_holder(
    transaction: &mut FlowTransaction<'_>,
    rows: RunRows,
    value_id: ValueId,
    holder: &ValueHolder,
) {
    transaction.put(
        ColumnFamily::Flow,
        value_holder_key(rows.flow_hash, rows.run_id, value_id, &holder.encode()),
        Vec::new(),
    );
}
