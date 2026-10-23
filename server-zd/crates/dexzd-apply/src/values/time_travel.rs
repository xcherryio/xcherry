// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::collections::BTreeMap;

use dexzd_keyspace::{RunTable, ValueId};
use dexzd_protocol::dexzd::v1::{
    AttributeRecord, ChannelMessageRecord, CheckpointRow, RunRecord, StepExecutionRecord,
    StepInvocationPhase, StoredValue, SubFlowResultRecord,
};
use prost::Message;

use super::{ValueHolder, hold_stored_value, release_stored_value, table_value_id};
use crate::ApplyError;
use crate::command_handlers::run_cursor::RunCursor;
use crate::state_access::FlowTransaction;

type HolderAssignments = BTreeMap<(ValueId, Vec<u8>), (StoredValue, ValueHolder)>;

/// Replaces attempt holders; additions precede removals so shared values remain live.
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "Called by the time travel handler when task A4 lands."
    )
)]
pub(crate) fn rewrite_holders_for_time_travel(
    transaction: &mut FlowTransaction<'_>,
    cursor: &RunCursor,
    old_attempt_rows: &[CheckpointRow],
    new_attempt_rows: &[CheckpointRow],
    seq: u64,
) -> Result<(), ApplyError> {
    let old_assignments = collect_holder_assignments(old_attempt_rows)?;
    let new_assignments = collect_holder_assignments(new_attempt_rows)?;
    for (assignment_key, (stored, holder)) in &new_assignments {
        if !old_assignments.contains_key(assignment_key) {
            hold_stored_value(transaction, cursor.rows, stored, holder)?;
        }
    }
    for (assignment_key, (stored, holder)) in &old_assignments {
        if !new_assignments.contains_key(assignment_key) {
            release_stored_value(transaction, cursor.rows, stored, holder, seq)?;
        }
    }
    Ok(())
}

fn collect_holder_assignments(rows: &[CheckpointRow]) -> Result<HolderAssignments, ApplyError> {
    let mut assignments = HolderAssignments::new();
    for row in rows {
        let table_byte = u8::try_from(row.table).map_err(|_| ApplyError::InvariantViolation {
            detail: format!("checkpoint table {} is not a run table", row.table),
        })?;
        let table =
            RunTable::from_byte(table_byte).map_err(|error| ApplyError::InvariantViolation {
                detail: format!("checkpoint has an unknown run table: {error}"),
            })?;
        match table {
            RunTable::RunRecord => {
                let record: RunRecord = decode_row(row, "run record")?;
                if let Some(close) = &record.close {
                    for (index, output) in close.results.iter().enumerate() {
                        insert_assignment(
                            &mut assignments,
                            output.completed_step_output.as_ref(),
                            ValueHolder::FlowResult {
                                index: u32::try_from(index).unwrap_or(u32::MAX),
                            },
                        );
                    }
                }
            }
            RunTable::Attribute => {
                let record: AttributeRecord = decode_row(row, "attribute")?;
                let attribute_key = String::from_utf8(row.key_suffix.clone()).map_err(|error| {
                    ApplyError::InvariantViolation {
                        detail: format!("checkpoint Attribute key is invalid UTF-8: {error}"),
                    }
                })?;
                insert_assignment(
                    &mut assignments,
                    record.value.as_ref(),
                    ValueHolder::Attribute { attribute_key },
                );
            }
            RunTable::ChannelMessage => {
                let record: ChannelMessageRecord = decode_row(row, "channel message")?;
                let holder = ValueHolder::ChannelMessage {
                    channel_name: record.channel_name,
                    enqueued_seq: record.enqueued_seq,
                };
                insert_assignment(&mut assignments, record.value.as_ref(), holder);
            }
            RunTable::StepExecution => {
                let record: StepExecutionRecord = decode_row(row, "step execution")?;
                collect_step_holders(&mut assignments, &record);
            }
            RunTable::SubFlowResult => {
                let record: SubFlowResultRecord = decode_row(row, "SubFlow result")?;
                let child_flow_id = String::from_utf8(row.key_suffix.clone()).map_err(|error| {
                    ApplyError::InvariantViolation {
                        detail: format!("checkpoint SubFlow key is invalid UTF-8: {error}"),
                    }
                })?;
                if let Some(result) = record.result {
                    for output in result.results {
                        insert_assignment(
                            &mut assignments,
                            output.completed_step_output.as_ref(),
                            ValueHolder::SubFlowResult {
                                child_flow_id: child_flow_id.clone(),
                            },
                        );
                    }
                }
            }
            _ => {}
        }
    }
    Ok(assignments)
}

fn collect_step_holders(assignments: &mut HolderAssignments, step: &StepExecutionRecord) {
    let step_execution_id = step.step_execution_id.clone();
    insert_assignment(
        assignments,
        step.input.as_ref(),
        ValueHolder::StepInput {
            step_execution_id: step_execution_id.clone(),
        },
    );
    for local in &step.locals {
        insert_assignment(
            assignments,
            local.value.as_ref(),
            ValueHolder::StepLocal {
                step_execution_id: step_execution_id.clone(),
                local_key: local.key.clone(),
            },
        );
    }
    if let Some(results) = &step.condition_results {
        for channel in &results.channel_results {
            for message in &channel.messages {
                insert_assignment(
                    assignments,
                    message.value.as_ref(),
                    ValueHolder::ConditionResult {
                        step_execution_id: step_execution_id.clone(),
                        channel_name: message.channel_name.clone(),
                        enqueued_seq: message.enqueued_seq,
                    },
                );
            }
        }
    }
    if let Some(invocation) = &step.invocation {
        insert_assignment(
            assignments,
            invocation.heartbeat_checkpoint.as_ref(),
            ValueHolder::HeartbeatCheckpoint {
                step_execution_id: step_execution_id.clone(),
                method: invocation.method,
            },
        );
        if invocation.phase == StepInvocationPhase::Running as i32
            && let Some(snapshot) = &invocation.snapshot
        {
            let pin = ValueHolder::AttemptSnapshot {
                step_execution_id,
                method: invocation.method,
                attempt: invocation.attempt,
            };
            for attribute in &snapshot.attributes {
                insert_assignment(assignments, attribute.value.as_ref(), pin.clone());
            }
            for channel in &snapshot.loaded_channel_messages {
                for message in &channel.messages {
                    insert_assignment(assignments, message.value.as_ref(), pin.clone());
                }
            }
        }
    }
}

fn insert_assignment(
    assignments: &mut HolderAssignments,
    stored: Option<&StoredValue>,
    holder: ValueHolder,
) {
    if let Some(stored) = stored
        && let Some((value_id, _)) = table_value_id(stored)
    {
        assignments.insert((value_id, holder.encode()), (stored.clone(), holder));
    }
}

fn decode_row<RowMessage: Message + Default>(
    row: &CheckpointRow,
    table: &'static str,
) -> Result<RowMessage, ApplyError> {
    RowMessage::decode(row.value.as_slice()).map_err(|error| ApplyError::InvariantViolation {
        detail: format!("checkpoint {table} row is corrupt: {error}"),
    })
}

#[cfg(test)]
mod tests {
    use dexzd_keyspace::row_keys::{value_garbage_key, value_row_key};
    use dexzd_keyspace::{ColumnFamily, FlowHash, PartitionId, RunId, RunTable, ValueId};
    use dexzd_protocol::dex::{Value, value};
    use dexzd_protocol::dexzd::v1::{
        CheckpointRow, MethodSnapshotRecord, RunRecord, StepExecutionRecord, StepInvocationPhase,
        StepInvocationRecord, StepMethod, StoredKeyValue,
    };
    use prost::Message;

    use super::rewrite_holders_for_time_travel;
    use crate::state_access::{FlowTransaction, RunRows, StateRow};
    use crate::values::garbage::prune_garbage;
    use crate::values::{
        ValueHolder, ValueIdAllocator, ValueLimits, release_stored_value, store_value,
    };
    use crate::{ApplyContext, FlowStateReader, StateReadError};

    #[derive(Default)]
    struct EmptyState;

    impl FlowStateReader for EmptyState {
        fn get(
            &self,
            _column_family: ColumnFamily,
            _key: &[u8],
        ) -> Result<Option<Vec<u8>>, StateReadError> {
            Ok(None)
        }

        fn scan(
            &self,
            _column_family: ColumnFamily,
            _start: &[u8],
            _end: &[u8],
            _limit: usize,
        ) -> Result<Vec<StateRow>, StateReadError> {
            Ok(Vec::new())
        }
    }

    #[test]
    fn time_travel_revives_garbage_for_an_in_flight_snapshot_then_collects_it() {
        let rows = RunRows::new(FlowHash(7), RunId([1; 16]));
        let reader = EmptyState;
        let mut transaction = FlowTransaction::new(
            &reader,
            ApplyContext {
                partition: PartitionId(0),
                flow_hash: rows.flow_hash,
            },
        );
        let old_holder = ValueHolder::Attribute {
            attribute_key: "report".to_owned(),
        };
        let stored = store_value(
            &mut transaction,
            rows,
            ValueLimits {
                wire_inline_threshold_bytes: 100,
                max_value_bytes: 16 << 20,
            },
            &mut ValueIdAllocator::new(2),
            &Value {
                kind: Some(value::Kind::StringValue("x".repeat(500))),
            },
            &old_holder,
        )
        .expect("storage succeeds")
        .expect("value is accepted");
        release_stored_value(&mut transaction, rows, &stored, &old_holder, 3)
            .expect("holder release succeeds");
        let value_id = ValueId {
            run_seq: 2,
            index_in_command: 0,
        };
        let garbage_at_three = value_garbage_key(rows.flow_hash, rows.run_id, 3, value_id);
        assert!(
            transaction
                .get(ColumnFamily::Flow, &garbage_at_three)
                .unwrap()
                .is_some()
        );

        let snapshot = MethodSnapshotRecord {
            attributes: vec![StoredKeyValue {
                key: "report".to_owned(),
                value: Some(stored.clone()),
            }],
            ..MethodSnapshotRecord::default()
        };
        let step = StepExecutionRecord {
            step_execution_id: "ReportStep-1".to_owned(),
            invocation: Some(StepInvocationRecord {
                method: StepMethod::Execute as i32,
                phase: StepInvocationPhase::Running as i32,
                attempt: 1,
                snapshot: Some(snapshot),
                ..StepInvocationRecord::default()
            }),
            ..StepExecutionRecord::default()
        };
        let restored_rows = vec![CheckpointRow {
            table: RunTable::StepExecution as u32,
            key_suffix: b"ReportStep-1".to_vec(),
            value: step.encode_to_vec(),
        }];
        let cursor = crate::command_handlers::run_cursor::RunCursor::for_new_run(
            rows,
            RunRecord {
                next_run_seq: 4,
                ..RunRecord::default()
            },
        );
        rewrite_holders_for_time_travel(&mut transaction, &cursor, &[], &restored_rows, 4)
            .expect("time travel holder rewrite succeeds");
        assert!(
            transaction
                .get(ColumnFamily::Flow, &garbage_at_three)
                .unwrap()
                .is_none()
        );
        assert_eq!(prune_garbage(&mut transaction, &cursor, 5).unwrap(), 0);
        rewrite_holders_for_time_travel(&mut transaction, &cursor, &restored_rows, &[], 5)
            .expect("ending the attempt releases the pin");
        assert_eq!(prune_garbage(&mut transaction, &cursor, 5).unwrap(), 0);
        assert_eq!(prune_garbage(&mut transaction, &cursor, 6).unwrap(), 1);
        assert_eq!(prune_garbage(&mut transaction, &cursor, 6).unwrap(), 0);
        assert!(
            transaction
                .get(
                    ColumnFamily::Value,
                    &value_row_key(rows.flow_hash, rows.run_id, RunTable::Value, value_id),
                )
                .unwrap()
                .is_none()
        );
    }
}
