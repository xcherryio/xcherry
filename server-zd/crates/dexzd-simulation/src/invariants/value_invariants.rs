// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Value holder sets and garbage rows (plan section 10.2, invariant I1).
//!
//! Expected holders derive from state slots and pins, using apply's permanent holder key encoding.

use std::collections::{BTreeMap, BTreeSet};

use dexzd_keyspace::row_keys::{length_prefixed_name, value_row_key};
use dexzd_keyspace::{ColumnFamily, FlowHash, RunTable, ValueId};
use dexzd_protocol::dexzd::v1::{
    StepExecutionPhase, StoredStepCompletionOutput, StoredValue, ValueRecord, command_reply,
    stored_value,
};
use prost::Message;

use crate::invariants::{CheckerMemory, InvariantViolation, RunContext, WindowPointState};
use crate::memory_state::MemoryFlowState;
use crate::row_decoding::hex;
use crate::state_view::{FlowView, RunView};

const HOLDERS: &str = "I1-value-holders";
const GARBAGE: &str = "I1-value-garbage";
const REFERENCE: &str = "I1-value-reference";
const WINDOW_REFERENCE: &str = "I1-window-value-reference";
const HARD_DELETE: &str = "hard-delete-object";

/// RequestKind::InvokeRpc in `dexzd-keyspace`.
const INVOKE_RPC_REQUEST_KIND: u8 = 0x04;

pub(crate) fn check_values(
    context: &RunContext<'_>,
    run: &RunView,
    genesis: Option<&BTreeSet<ValueId>>,
    window_points: Option<&BTreeMap<u64, WindowPointState>>,
) -> Result<(), InvariantViolation> {
    let mut slots = HolderCollector::default();
    collect_slots(run, &mut slots);
    if let Some(genesis) = genesis {
        for value_id in genesis {
            slots
                .expected
                .entry(*value_id)
                .or_default()
                .insert(vec![0x01]);
        }
    }
    for (value_id, size_bytes) in &slots.references {
        let birth = run.value_births.get(value_id);
        let is_live = run.value_rows.contains(value_id)
            && birth.is_some_and(|birth| birth.size_bytes == *size_bytes);
        if !is_live {
            return Err(InvariantViolation::new(
                REFERENCE,
                format!(
                    "{context} state references value {value_id:?} of {size_bytes} bytes, \
                     which is not live (value row: {}, birth: {birth:?})",
                    run.value_rows.contains(value_id)
                ),
            ));
        }
    }
    let empty = BTreeSet::new();
    let value_ids: BTreeSet<&ValueId> = slots
        .expected
        .keys()
        .chain(run.value_holders.keys())
        .collect();
    for value_id in value_ids {
        let expected = slots.expected.get(value_id).unwrap_or(&empty);
        let actual = run.value_holders.get(value_id).unwrap_or(&empty);
        if expected != actual {
            let missing: Vec<String> = expected.difference(actual).map(|h| hex(h)).collect();
            let extra: Vec<String> = actual.difference(expected).map(|h| hex(h)).collect();
            return Err(InvariantViolation::new(
                HOLDERS,
                format!(
                    "{context} value {value_id:?} holder rows differ from state: missing \
                     {missing:?}, stale {extra:?}"
                ),
            ));
        }
    }
    check_garbage(context, run)?;
    check_window_references(context, run, window_points)?;
    check_hard_delete_objects(context, run)
}

pub(crate) fn remember_window_points(view: &FlowView, memory: &mut CheckerMemory) {
    for (run_id, run) in view.runs.iter().filter(|(_, run)| run.is_complete) {
        let window_start = run
            .record
            .as_ref()
            .map_or(0, |record| record.window_start_seq);
        let points = memory.window_points.entry(*run_id).or_default();
        points.retain(|seq, _| *seq >= window_start);
        let current_references = collect_references(run);
        let current_attribute_values: BTreeSet<ValueId> = run
            .attributes
            .values()
            .filter_map(|attribute| table_value_id(attribute.value.as_ref()))
            .collect();
        for (seq, history) in &run.history {
            if history.is_semantic && *seq >= window_start {
                points.entry(*seq).or_insert_with(|| WindowPointState {
                    references: current_references.clone(),
                    attribute_values: current_attribute_values.clone(),
                });
            }
        }
    }
}

fn collect_references(run: &RunView) -> BTreeSet<(ValueId, u64)> {
    let mut slots = HolderCollector::default();
    collect_slots(run, &mut slots);
    slots.references
}

fn table_value_id(stored: Option<&StoredValue>) -> Option<ValueId> {
    let stored_value::Kind::TableValue(reference) = stored?.kind.as_ref()? else {
        return None;
    };
    let value_id = reference.value_id?;
    Some(ValueId {
        run_seq: value_id.run_seq,
        index_in_command: value_id.index_in_command,
    })
}

fn check_window_references(
    context: &RunContext<'_>,
    run: &RunView,
    window_points: Option<&BTreeMap<u64, WindowPointState>>,
) -> Result<(), InvariantViolation> {
    let window_start = run
        .record
        .as_ref()
        .map_or(0, |record| record.window_start_seq);
    for (seq, history) in &run.history {
        if !history.is_semantic || *seq < window_start {
            continue;
        }
        let point = window_points
            .and_then(|points| points.get(seq))
            .ok_or_else(|| {
                InvariantViolation::new(
                    WINDOW_REFERENCE,
                    format!("{context} semantic point {seq} has no recorded state"),
                )
            })?;
        for (value_id, size_bytes) in &point.references {
            let birth = run.value_births.get(value_id);
            if !run.value_rows.contains(value_id)
                || !birth.is_some_and(|birth| birth.size_bytes == *size_bytes)
            {
                return Err(InvariantViolation::new(
                    WINDOW_REFERENCE,
                    format!(
                        "{context} semantic point {seq} references unreadable value {value_id:?} \
                         of {size_bytes} bytes (value row: {}, birth: {birth:?})",
                        run.value_rows.contains(value_id)
                    ),
                ));
            }
        }
    }
    Ok(())
}

pub(crate) fn check_window_value_content(
    context: &RunContext<'_>,
    run: &RunView,
    window_points: Option<&BTreeMap<u64, WindowPointState>>,
    state: &MemoryFlowState,
    flow_hash: FlowHash,
) -> Result<(), InvariantViolation> {
    let window_start = run
        .record
        .as_ref()
        .map_or(0, |record| record.window_start_seq);
    for (seq, history) in &run.history {
        if !history.is_semantic || *seq < window_start {
            continue;
        }
        let point = window_points
            .and_then(|points| points.get(seq))
            .ok_or_else(|| {
                InvariantViolation::new(
                    WINDOW_REFERENCE,
                    format!("{context} semantic point {seq} has no recorded state"),
                )
            })?;
        for (value_id, size_bytes) in &point.references {
            let key = value_row_key(flow_hash, *context.run_id, RunTable::Value, *value_id);
            let content_size = match state.row(ColumnFamily::Value, &key) {
                Some(bytes) => ValueRecord::decode(bytes)
                    .map_err(|error| {
                        InvariantViolation::new(
                            WINDOW_REFERENCE,
                            format!(
                                "{context} semantic point {seq} value {value_id:?} cannot decode: {error}"
                            ),
                        )
                    })?
                    .value
                    .map(|value| u64::try_from(value.encoded_len()).unwrap_or(u64::MAX)),
                None => None,
            };
            let birth_size = run.value_births.get(value_id).map(|birth| birth.size_bytes);
            if content_size != Some(*size_bytes) || birth_size != Some(*size_bytes) {
                return Err(InvariantViolation::new(
                    WINDOW_REFERENCE,
                    format!(
                        "{context} semantic point {seq} references unreadable value {value_id:?} \
                         of {size_bytes} bytes (content size: {content_size:?}, birth size: {birth_size:?})"
                    ),
                ));
            }
        }
    }
    Ok(())
}

/// Every hard-delete Attribute has exactly one current object row naming its object.
fn check_hard_delete_objects(
    context: &RunContext<'_>,
    run: &RunView,
) -> Result<(), InvariantViolation> {
    for (key, record) in &run.attributes {
        let attribute_object = match record.value.as_ref().and_then(|value| value.kind.as_ref()) {
            Some(stored_value::Kind::HardDeleteObject(object)) => Some(object),
            _ => None,
        };
        let current_object = run
            .hard_delete_current
            .get(key)
            .and_then(|current| current.object.as_ref());
        if attribute_object != current_object {
            return Err(InvariantViolation::new(
                HARD_DELETE,
                format!(
                    "{context} Attribute {key} stores hard-delete object {attribute_object:?} \
                     but the current object row names {current_object:?}"
                ),
            ));
        }
    }
    if let Some(key) = run
        .hard_delete_current
        .keys()
        .find(|key| !run.attributes.contains_key(*key))
    {
        return Err(InvariantViolation::new(
            HARD_DELETE,
            format!("{context} hard-delete object row of Attribute {key} has no Attribute"),
        ));
    }
    Ok(())
}

fn check_garbage(context: &RunContext<'_>, run: &RunView) -> Result<(), InvariantViolation> {
    let mut garbage_by_value: BTreeMap<ValueId, Vec<u64>> = BTreeMap::new();
    for (seq, value_id, record) in &run.value_garbage {
        garbage_by_value.entry(*value_id).or_default().push(*seq);
        let birth = run.value_births.get(value_id);
        let is_consistent = birth.is_some_and(|birth| {
            birth.garbage_seq == *seq && birth.size_bytes == record.size_bytes
        }) && !run.value_holders.contains_key(value_id);
        if !is_consistent {
            return Err(InvariantViolation::new(
                GARBAGE,
                format!(
                    "{context} garbage row ({seq}, {value_id:?}) disagrees with birth {birth:?} \
                     or the value still has holders"
                ),
            ));
        }
    }
    for (value_id, birth) in &run.value_births {
        if !run.value_rows.contains(value_id) {
            return Err(InvariantViolation::new(
                GARBAGE,
                format!("{context} value {value_id:?} has a birth row but no value row"),
            ));
        }
        let has_holders = run.value_holders.contains_key(value_id);
        let garbage_seqs = garbage_by_value
            .get(value_id)
            .map_or(&[][..], Vec::as_slice);
        let is_consistent = if has_holders {
            birth.garbage_seq == 0 && garbage_seqs.is_empty()
        } else {
            birth.garbage_seq != 0 && garbage_seqs == [birth.garbage_seq]
        };
        if !is_consistent {
            return Err(InvariantViolation::new(
                GARBAGE,
                format!(
                    "{context} value {value_id:?} has holders: {has_holders}, birth garbage seq \
                     {}, garbage rows at {garbage_seqs:?}",
                    birth.garbage_seq
                ),
            ));
        }
    }
    for value_id in run.value_rows.iter().chain(run.value_holders.keys()) {
        if !run.value_births.contains_key(value_id) {
            return Err(InvariantViolation::new(
                GARBAGE,
                format!("{context} value {value_id:?} has rows but no birth row"),
            ));
        }
    }
    Ok(())
}

#[derive(Default)]
struct HolderCollector {
    expected: BTreeMap<ValueId, BTreeSet<Vec<u8>>>,
    /// Every table value reference reachable from state: value and the size it records.
    references: BTreeSet<(ValueId, u64)>,
}

impl HolderCollector {
    fn reach(&mut self, stored: Option<&StoredValue>) -> Option<ValueId> {
        let Some(stored_value::Kind::TableValue(reference)) = stored.and_then(|s| s.kind.as_ref())
        else {
            return None;
        };
        let id = reference.value_id.unwrap_or_default();
        let value_id = ValueId {
            run_seq: id.run_seq,
            index_in_command: id.index_in_command,
        };
        self.references.insert((value_id, reference.size_bytes));
        Some(value_id)
    }

    fn hold(&mut self, stored: Option<&StoredValue>, holder: Vec<u8>) {
        if let Some(value_id) = self.reach(stored) {
            self.expected.entry(value_id).or_default().insert(holder);
        }
    }

    fn hold_outputs(&mut self, outputs: &[StoredStepCompletionOutput], holder: Option<Vec<u8>>) {
        for (index, output) in outputs.iter().enumerate() {
            let stored = output.completed_step_output.as_ref();
            match &holder {
                Some(holder) => self.hold(stored, holder.clone()),
                None => {
                    let index = u32::try_from(index).unwrap_or(u32::MAX);
                    self.hold(stored, holder_key(0x09, &[&index.to_be_bytes()]));
                }
            }
        }
    }
}

fn collect_slots(run: &RunView, slots: &mut HolderCollector) {
    for (key, record) in &run.attributes {
        let holder = holder_key(0x02, &[&length_prefixed_name(key)]);
        slots.hold(record.value.as_ref(), holder);
    }
    for message in &run.channel_messages {
        let holder = holder_key(
            0x03,
            &[
                &length_prefixed_name(&message.channel_name),
                &message.enqueued_seq.to_be_bytes(),
            ],
        );
        slots.hold(message.value.as_ref(), holder);
    }
    for step in run.step_executions.values() {
        let id = length_prefixed_name(&step.step_execution_id);
        slots.hold(step.input.as_ref(), holder_key(0x04, &[&id]));
        for local in &step.locals {
            let holder = holder_key(0x05, &[&id, &length_prefixed_name(&local.key)]);
            slots.hold(local.value.as_ref(), holder);
        }
        let is_executing = step.phase == StepExecutionPhase::Execute as i32;
        for channel in step
            .condition_results
            .iter()
            .flat_map(|results| &results.channel_results)
        {
            for message in &channel.messages {
                let holder = holder_key(
                    0x06,
                    &[
                        &id,
                        &length_prefixed_name(&message.channel_name),
                        &message.enqueued_seq.to_be_bytes(),
                    ],
                );
                // Finished Steps keep condition results for display only.
                if is_executing {
                    slots.hold(message.value.as_ref(), holder);
                } else {
                    slots.reach(message.value.as_ref());
                }
            }
        }
        // Apply has no holder kind for SubFlow condition results yet; check only liveness and size.
        for output in step
            .condition_results
            .iter()
            .flat_map(|results| &results.sub_flow_results)
            .flat_map(|result| &result.results)
        {
            slots.reach(output.completed_step_output.as_ref());
        }
        let Some(invocation) = &step.invocation else {
            continue;
        };
        let method = invocation.method.to_be_bytes();
        let checkpoint_holder = holder_key(0x07, &[&id, &method]);
        slots.hold(invocation.heartbeat_checkpoint.as_ref(), checkpoint_holder);
        let Some(snapshot) = &invocation.snapshot else {
            continue;
        };
        let pin = holder_key(0x08, &[&id, &method, &invocation.attempt.to_be_bytes()]);
        for attribute in &snapshot.attributes {
            slots.hold(attribute.value.as_ref(), pin.clone());
        }
        for channel in &snapshot.loaded_channel_messages {
            for message in &channel.messages {
                slots.hold(message.value.as_ref(), pin.clone());
            }
        }
    }
    if let Some(close) = run.record.as_ref().and_then(|record| record.close.as_ref()) {
        slots.hold_outputs(&close.results, None);
    }
    for (child_flow_id, record) in &run.sub_flow_results {
        let holder = holder_key(0x0a, &[&length_prefixed_name(child_flow_id)]);
        if let Some(result) = &record.result {
            slots.hold_outputs(&result.results, Some(holder));
        }
    }
    for (kind, request_id, record) in &run.request_dedups {
        if *kind != INVOKE_RPC_REQUEST_KIND {
            continue;
        }
        if let Some(command_reply::Reply::Rpc(reply)) =
            record.reply.as_ref().and_then(|reply| reply.reply.as_ref())
        {
            let holder = holder_key(0x0b, &[&length_prefixed_name(request_id)]);
            slots.hold(reply.output.as_ref(), holder);
        }
    }
}

#[cfg(test)]
mod retained_point_tests {
    use super::*;
    use dexzd_apply::StateWrite;
    use dexzd_keyspace::RunId;
    use dexzd_protocol::dex::{Value, value};
    use dexzd_protocol::dexzd::v1::{HistoryRecord, RunRecord, ValueBirthRecord};

    #[test]
    fn retained_point_requires_decodable_value_content_of_the_referenced_size() {
        let flow_hash = FlowHash(7);
        let run_id = RunId([1; 16]);
        let value_id = ValueId {
            run_seq: 1,
            index_in_command: 0,
        };
        let value = Value {
            kind: Some(value::Kind::StringValue("retained".to_owned())),
        };
        let size_bytes = u64::try_from(value.encoded_len()).expect("test value size fits u64");
        let run = RunView {
            record: Some(RunRecord {
                window_start_seq: 1,
                ..RunRecord::default()
            }),
            value_births: BTreeMap::from([(
                value_id,
                ValueBirthRecord {
                    size_bytes,
                    ..ValueBirthRecord::default()
                },
            )]),
            value_rows: BTreeSet::from([value_id]),
            history: vec![(
                1,
                HistoryRecord {
                    is_semantic: true,
                    ..HistoryRecord::default()
                },
            )],
            ..RunView::default()
        };
        let points = BTreeMap::from([(
            1,
            WindowPointState {
                references: BTreeSet::from([(value_id, size_bytes)]),
                attribute_values: BTreeSet::new(),
            },
        )]);
        let context = RunContext {
            flow_id: "flow",
            run_id: &run_id,
        };
        let key = value_row_key(flow_hash, run_id, RunTable::Value, value_id);
        let mut state = MemoryFlowState::default();
        let missing_violation =
            check_window_value_content(&context, &run, Some(&points), &state, flow_hash)
                .expect_err("retained point must reject a missing value row");
        assert_eq!(missing_violation.invariant, WINDOW_REFERENCE);
        for invalid_record in [
            ValueRecord { value: None },
            ValueRecord {
                value: Some(Value {
                    kind: Some(value::Kind::StringValue("short".to_owned())),
                }),
            },
        ] {
            state.commit(&[StateWrite::Put {
                column_family: ColumnFamily::Value,
                key: key.clone(),
                value: invalid_record.encode_to_vec(),
            }]);
            let violation =
                check_window_value_content(&context, &run, Some(&points), &state, flow_hash)
                    .expect_err("retained point must have readable value bytes");
            assert_eq!(violation.invariant, WINDOW_REFERENCE);
        }
        state.commit(&[StateWrite::Put {
            column_family: ColumnFamily::Value,
            key,
            value: ValueRecord { value: Some(value) }.encode_to_vec(),
        }]);
        check_window_value_content(&context, &run, Some(&points), &state, flow_hash)
            .expect("matching value content is readable");
    }
}

fn holder_key(kind: u8, parts: &[&[u8]]) -> Vec<u8> {
    let mut encoded = vec![kind];
    for part in parts {
        encoded.extend_from_slice(part);
    }
    encoded
}

#[cfg(test)]
mod tests {
    use dexzd_keyspace::RunId;
    use dexzd_protocol::dexzd::v1::{
        AttributeRecord, StoredValueId, StoredValueReference, ValueBirthRecord,
    };

    use super::*;

    fn table_value(size_bytes: u64) -> StoredValue {
        StoredValue {
            kind: Some(stored_value::Kind::TableValue(StoredValueReference {
                value_id: Some(StoredValueId {
                    run_seq: 3,
                    index_in_command: 0,
                }),
                size_bytes,
                is_string_value: true,
            })),
        }
    }

    /// `notes` and `status` share one value; only `notes`, visited first, records a wrong size.
    fn run_sharing_one_value(notes_size_bytes: u64) -> RunView {
        let value_id = ValueId {
            run_seq: 3,
            index_in_command: 0,
        };
        let mut run = RunView {
            is_complete: true,
            ..RunView::default()
        };
        for (key, size_bytes) in [("notes", notes_size_bytes), ("status", 500)] {
            run.attributes.insert(
                key.to_owned(),
                AttributeRecord {
                    value: Some(table_value(size_bytes)),
                    ..AttributeRecord::default()
                },
            );
            run.value_holders
                .entry(value_id)
                .or_default()
                .insert(holder_key(0x02, &[&length_prefixed_name(key)]));
        }
        run.value_births.insert(
            value_id,
            ValueBirthRecord {
                size_bytes: 500,
                written_seq: 3,
                garbage_seq: 0,
            },
        );
        run.value_rows.insert(value_id);
        run
    }

    fn check(run: &RunView) -> Result<(), InvariantViolation> {
        let run_id = RunId([1; 16]);
        let context = RunContext {
            flow_id: "order-1",
            run_id: &run_id,
        };
        check_values(&context, run, None, None)
    }

    #[test]
    fn one_wrong_reference_size_is_not_hidden_by_another_holder() {
        check(&run_sharing_one_value(500)).expect("both references record the birth size");
        let violation = check(&run_sharing_one_value(499))
            .expect_err("the notes reference records a wrong size");
        assert_eq!(violation.invariant, REFERENCE, "{violation}");
    }
}
