// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Durable cross-Flow delivery, child Flow reuse, and run deletion.

mod inbox;
mod outbox;
mod run_purge;

use dexzd_keyspace::RunTable;
use dexzd_keyspace::row_keys::run_named_key;

use crate::ApplyError;
use crate::command_handlers::CommandScope;
use crate::command_handlers::run_cursor::RunCursor;
use crate::state_access::{FlowTransaction, RunRows};

pub(crate) use inbox::apply_inbox_message;
pub(crate) use outbox::{
    apply_outbox_ack, on_outbox_retention_fired, report_child_closed, start_child_flows,
    validate_child_start_conditions,
};
pub(crate) use run_purge::{apply_hard_delete_objects_deleted, apply_run_purge};

pub(crate) fn sub_flow_result_key(
    rows: RunRows,
    child_flow_id: &str,
    registration_attempt: u32,
) -> Vec<u8> {
    let name = format!("{registration_attempt:010}:{child_flow_id}");
    run_named_key(rows.flow_hash, rows.run_id, RunTable::SubFlowResult, &name)
}

/// Re-evaluates restored waits using each condition's original registration attempt.
pub(crate) fn complete_restored_sub_flow_waits(
    transaction: &mut FlowTransaction<'_>,
    cursor: &mut RunCursor,
    scope: &CommandScope<'_>,
) -> Result<(), ApplyError> {
    crate::steps::wait_conditions::complete_restored_sub_flow_waits(transaction, cursor, scope)
}

#[cfg(test)]
mod tests {
    use dexzd_keyspace::{ColumnFamily, PartitionHasher, RunId, TenantId};
    use dexzd_protocol::dex::{
        FlowStatus, SubFlowCondition, WaitingCondition, WaitingConditionType,
    };
    use dexzd_protocol::dexzd::v1::{
        FlowCommand, RunRecord, StepExecutionPhase, StepExecutionRecord, StoredFlowResult,
        SubFlowResultRecord, WaitStateRecord,
    };

    use crate::command_handlers::CommandScope;
    use crate::command_handlers::run_cursor::RunCursor;
    use crate::state_access::{FlowTransaction, RunRows};
    use crate::{ApplyContext, BehaviorVersion, FlowStateReader, StateReadError, StateRow};

    use super::{complete_restored_sub_flow_waits, sub_flow_result_key};

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
    fn restored_wait_consumes_result_addressed_to_original_attempt() {
        let flow_id = "parent";
        let child_flow_id = "SubFlow:parent-StartStep-1-0";
        let run_id = RunId([1; 16]);
        let hasher = PartitionHasher::new(&[7; 16], 1024).expect("valid hasher");
        let flow_hash = hasher.flow_hash(TenantId::DEFAULT, flow_id);
        let context = ApplyContext {
            partition: hasher.partition_of(flow_hash),
            flow_hash,
        };
        let mut transaction = FlowTransaction::new(&EmptyState, context);
        let rows = RunRows::new(flow_hash, run_id);
        let waiting_step = StepExecutionRecord {
            step_execution_id: "StartStep-1".to_owned(),
            step_type: "StartStep".to_owned(),
            phase: StepExecutionPhase::Waiting as i32,
            wait: Some(WaitStateRecord {
                condition: Some(WaitingCondition {
                    waiting_condition_type: WaitingConditionType::AllCompleted as i32,
                    sub_flow_conditions: vec![SubFlowCondition {
                        sub_flow_type: "ChildFlow".to_owned(),
                        start_step_type: "ChildStep".to_owned(),
                        ..SubFlowCondition::default()
                    }],
                    ..WaitingCondition::default()
                }),
                sub_flow_child_ids: vec![child_flow_id.to_owned()],
                sub_flow_registration_attempt: 1,
                started_waiting_seq: 2,
                ..WaitStateRecord::default()
            }),
            ..StepExecutionRecord::default()
        };
        rows.put_step_execution(&mut transaction, &waiting_step);
        transaction.put_message(
            ColumnFamily::Flow,
            sub_flow_result_key(rows, child_flow_id, 1),
            &SubFlowResultRecord {
                parent_registration_attempt: 1,
                result: Some(StoredFlowResult {
                    flow_status: FlowStatus::Completed as i32,
                    ..StoredFlowResult::default()
                }),
                ..SubFlowResultRecord::default()
            },
        );
        let record = RunRecord {
            flow_id: flow_id.to_owned(),
            run_id: run_id.as_bytes().to_vec(),
            status: FlowStatus::Running as i32,
            current_attempt: 2,
            next_run_seq: 10,
            ..RunRecord::default()
        };
        let mut cursor = RunCursor::for_new_run(rows, record);
        let command = FlowCommand {
            flow_id: flow_id.to_owned(),
            run_id: run_id.as_bytes().to_vec(),
            behavior_version: 1,
            proposed_at_micros: 1_800_000_000_000_000,
            ..FlowCommand::default()
        };
        let scope = CommandScope::new(
            &command,
            BehaviorVersion::from_command(1).expect("known behavior version"),
        );
        complete_restored_sub_flow_waits(&mut transaction, &mut cursor, &scope)
            .expect("restored wait completes");
        let step = rows
            .step_execution(&transaction, "StartStep-1")
            .expect("Step reads")
            .expect("Step exists");
        assert_eq!(step.phase, StepExecutionPhase::Execute as i32);
        assert_eq!(
            step.condition_results
                .expect("condition results")
                .sub_flow_results[0]
                .flow_status,
            FlowStatus::Completed as i32
        );
    }
}
