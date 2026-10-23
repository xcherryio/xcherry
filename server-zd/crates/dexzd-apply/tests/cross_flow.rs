// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::collections::BTreeMap;

use dexzd_apply::{
    ApplyContext, ApplyOutcome, FlowStateReader, StateReadError, StateRow, StateWrite,
    apply_flow_command,
};
use dexzd_keyspace::row_keys::{
    hard_delete_pending_key, outbox_key, prefix_range, retention_index_key, run_item_index_key,
    run_named_key, run_singleton_key, timer_index_key, timer_key,
};
use dexzd_keyspace::{
    ColumnFamily, IndexKind, PartitionHasher, RunId, RunTable, TenantId, run_prefix,
    run_table_prefix,
};
use dexzd_protocol::dex::{
    CloseDecision, CloseDecisionType, FlowConfig, FlowStatus, FlowTimeoutPolicy,
    InvokeExecuteMethodResponse, StartFlowRequest, StepDecision, StepOptions, StopFlowRequest,
    StopType, SubFlowCondition, SubFlowOptions, SubFlowReusePolicy, Value, WorkerTarget, value,
};
use dexzd_protocol::dexzd::v1::{
    ChildFlowStartMessage, CrossFlowEndpoint, FlowCommand, FlowStartCommand, FlowStopCommand,
    FlowTimeoutTimerId, HardDeleteObjectRecord, HardDeleteObjectReference,
    HardDeleteObjectsDeletedCommand, InboxMessageCommand, OutboxAckCommand, OutboxRecord,
    OutboxRetentionTimerId, ParentFlowReference, RunPurgeCommand, RunRecord, RunSettings,
    StepAttemptFailedCommand, StepAttemptStartedCommand, StepInvocationKey, StepMethod,
    StepMethodCompletedCommand, SubFlowResultRecord, TimerId, TimersFiredCommand, command_reply,
    flow_command::Command, inbox_message_command, outbox_record, step_method_completed_command,
    timer_id,
};
use prost::Message;

const SECOND: i64 = 1_000_000;
const PARENT_RUN: [u8; 16] = [1; 16];
const FIRST_CHILD_RUN: [u8; 16] = [2; 16];
const SECOND_CHILD_RUN: [u8; 16] = [3; 16];
const START_MESSAGE: [u8; 16] = [4; 16];
const CHILD_ID: &str = "SubFlow:parent-StartStep-1-0";

#[derive(Default)]
struct MemoryState {
    rows: BTreeMap<(ColumnFamily, Vec<u8>), Vec<u8>>,
}

impl FlowStateReader for MemoryState {
    fn get(
        &self,
        column_family: ColumnFamily,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, StateReadError> {
        Ok(self.rows.get(&(column_family, key.to_vec())).cloned())
    }

    fn scan(
        &self,
        column_family: ColumnFamily,
        start: &[u8],
        end: &[u8],
        limit: usize,
    ) -> Result<Vec<StateRow>, StateReadError> {
        Ok(self
            .rows
            .range((column_family, start.to_vec())..(column_family, end.to_vec()))
            .take(limit)
            .map(|((_, key), value)| (key.clone(), value.clone()))
            .collect())
    }
}

impl MemoryState {
    fn commit(&mut self, outcome: &ApplyOutcome) {
        for write in &outcome.writes {
            match write {
                StateWrite::Put {
                    column_family,
                    key,
                    value,
                } => {
                    self.rows
                        .insert((*column_family, key.clone()), value.clone());
                }
                StateWrite::Delete { column_family, key } => {
                    self.rows.remove(&(*column_family, key.clone()));
                }
                StateWrite::DeleteRange {
                    column_family,
                    start,
                    end,
                } => {
                    let doomed: Vec<_> = self
                        .rows
                        .range((*column_family, start.clone())..(*column_family, end.clone()))
                        .map(|(key, _)| key.clone())
                        .collect();
                    for key in doomed {
                        self.rows.remove(&key);
                    }
                }
            }
        }
    }
}

struct Harness {
    state: MemoryState,
    hasher: PartitionHasher,
    now_micros: i64,
}

impl Harness {
    fn new() -> Self {
        Self {
            state: MemoryState::default(),
            hasher: PartitionHasher::new(&[7; 16], 1024).expect("valid hasher"),
            now_micros: 1_800_000_000 * SECOND,
        }
    }

    fn apply(&mut self, flow_id: &str, run_id: &[u8], body: Command) -> ApplyOutcome {
        self.now_micros += 1000;
        let flow_hash = self.hasher.flow_hash(TenantId::DEFAULT, flow_id);
        let context = ApplyContext {
            partition: self.hasher.partition_of(flow_hash),
            flow_hash,
        };
        let command = FlowCommand {
            flow_id: flow_id.to_owned(),
            run_id: run_id.to_vec(),
            expected_run_attempt: 0,
            epoch: 1,
            behavior_version: 1,
            proposed_at_micros: self.now_micros,
            command_id: [0; 16].to_vec(),
            tenant_id: 0,
            command: Some(body),
        };
        let outcome = apply_flow_command(&context, &self.state, &command).expect("apply succeeds");
        let repeated = apply_flow_command(&context, &self.state, &command).expect("apply repeats");
        assert_eq!(outcome.writes, repeated.writes, "writes are deterministic");
        assert_eq!(
            outcome.effects, repeated.effects,
            "effects are deterministic"
        );
        self.state.commit(&outcome);
        outcome
    }

    fn start_parent(&mut self) {
        self.start_parent_with_timeout(0);
    }

    fn start_parent_with_timeout(&mut self, timeout_seconds: i32) {
        let outcome = self.apply(
            "parent",
            &[],
            Command::FlowStart(FlowStartCommand {
                new_run_id: PARENT_RUN.to_vec(),
                request: Some(StartFlowRequest {
                    flow_id: "parent".to_owned(),
                    flow_type: "ParentFlow".to_owned(),
                    start_step_type: "StartStep".to_owned(),
                    flow_timeout_seconds: timeout_seconds,
                    request_id: "parent-request".to_owned(),
                    ..StartFlowRequest::default()
                }),
                settings: Some(settings()),
                resolved_timeout_policy: FlowTimeoutPolicy::Fail as i32,
                worker_target: Some(pull_worker()),
                ..FlowStartCommand::default()
            }),
        );
        assert!(matches!(
            outcome.reply.reply,
            Some(command_reply::Reply::StartFlow(_))
        ));
    }

    fn deliver_child_start(
        &mut self,
        message_id: [u8; 16],
        new_run_id: [u8; 16],
        request_id: &str,
        reuse_policy: SubFlowReusePolicy,
    ) -> ApplyOutcome {
        let parent = parent_reference();
        self.apply(
            CHILD_ID,
            &[],
            Command::InboxMessage(InboxMessageCommand {
                message_id: message_id.to_vec(),
                source: parent.parent.clone(),
                payload: Some(inbox_message_command::Payload::ChildStart(
                    ChildFlowStartMessage {
                        child_flow_id: CHILD_ID.to_owned(),
                        condition: Some(SubFlowCondition {
                            sub_flow_type: "ChildFlow".to_owned(),
                            start_step_type: "ChildStep".to_owned(),
                            step_options: Some(StepOptions {
                                skip_wait_for: true,
                                ..StepOptions::default()
                            }),
                            options: Some(SubFlowOptions {
                                reuse_policy: reuse_policy as i32,
                                ..SubFlowOptions::default()
                            }),
                            ..SubFlowCondition::default()
                        }),
                        flow_config: Some(FlowConfig {
                            worker_target: Some(pull_worker()),
                            ..FlowConfig::default()
                        }),
                        request_id: request_id.to_owned(),
                        parent: Some(parent),
                    },
                )),
                new_run_id: new_run_id.to_vec(),
                settings: Some(settings()),
            }),
        )
    }

    fn record(&self, flow_id: &str, run_id: [u8; 16]) -> RunRecord {
        let flow_hash = self.hasher.flow_hash(TenantId::DEFAULT, flow_id);
        let key = run_singleton_key(flow_hash, RunId(run_id), RunTable::RunRecord);
        let bytes = self
            .state
            .get(ColumnFamily::Flow, &key)
            .expect("memory reads")
            .expect("run exists");
        RunRecord::decode(bytes.as_slice()).expect("run decodes")
    }

    fn outbox(&self, flow_id: &str, run_id: [u8; 16]) -> Vec<OutboxRecord> {
        let flow_hash = self.hasher.flow_hash(TenantId::DEFAULT, flow_id);
        let prefix = run_table_prefix(flow_hash, RunId(run_id), RunTable::Outbox);
        let (start, end) = prefix_range(&prefix);
        self.state
            .scan(ColumnFamily::Flow, &start, &end, usize::MAX)
            .expect("memory reads")
            .into_iter()
            .map(|(_, bytes)| OutboxRecord::decode(bytes.as_slice()).expect("outbox decodes"))
            .collect()
    }

    fn deliver_completion(&mut self, outbox: &OutboxRecord) -> ApplyOutcome {
        let outbox_record::Payload::ChildCompleted(completed) =
            outbox.payload.clone().expect("payload")
        else {
            panic!("expected completion")
        };
        self.apply(
            "parent",
            &PARENT_RUN,
            Command::InboxMessage(InboxMessageCommand {
                message_id: outbox.message_id.clone(),
                source: Some(CrossFlowEndpoint {
                    tenant_id: 0,
                    flow_id: CHILD_ID.to_owned(),
                    run_id: completed.child_run_id.clone(),
                    run_attempt: completed.child_run_attempt,
                }),
                payload: Some(inbox_message_command::Payload::ChildCompleted(completed)),
                ..InboxMessageCommand::default()
            }),
        )
    }
}

fn pull_worker() -> WorkerTarget {
    WorkerTarget {
        is_pull_mode: true,
        ..WorkerTarget::default()
    }
}

fn settings() -> RunSettings {
    RunSettings {
        retention_after_close_micros: 7 * 24 * 3600 * SECOND,
        wire_inline_threshold_bytes: 100,
        max_value_bytes: 16 << 20,
        request_dedup_capacity: 1000,
        ..RunSettings::default()
    }
}

fn parent_reference() -> ParentFlowReference {
    ParentFlowReference {
        parent: Some(CrossFlowEndpoint {
            tenant_id: 0,
            flow_id: "parent".to_owned(),
            run_id: PARENT_RUN.to_vec(),
            run_attempt: 1,
        }),
        step_execution_id: "StartStep-1".to_owned(),
        sub_flow_index: 0,
        condition_id: "child".to_owned(),
    }
}

fn assert_ok(outcome: &ApplyOutcome) {
    assert!(matches!(
        outcome.reply.reply,
        Some(command_reply::Reply::Ok(_))
    ));
}

#[test]
fn child_start_completion_dedup_ack_retention_and_purge() {
    let mut harness = Harness::new();
    harness.start_parent();
    let started = harness.deliver_child_start(
        START_MESSAGE,
        FIRST_CHILD_RUN,
        "child-request",
        SubFlowReusePolicy::Unspecified,
    );
    assert!(matches!(
        started.reply.reply,
        Some(command_reply::Reply::StartFlow(_))
    ));
    assert_eq!(
        harness.record(CHILD_ID, FIRST_CHILD_RUN).parent,
        Some(parent_reference())
    );
    let redelivery = harness.deliver_child_start(
        START_MESSAGE,
        SECOND_CHILD_RUN,
        "child-request",
        SubFlowReusePolicy::Unspecified,
    );
    assert_ok(&redelivery);
    assert!(redelivery.writes.is_empty());
    harness.apply(
        CHILD_ID,
        &FIRST_CHILD_RUN,
        Command::FlowStop(FlowStopCommand {
            request: Some(StopFlowRequest {
                stop_type: StopType::Terminate as i32,
                ..StopFlowRequest::default()
            }),
        }),
    );
    let closed_record = harness.record(CHILD_ID, FIRST_CHILD_RUN);
    assert_eq!(closed_record.status, FlowStatus::Terminated as i32);
    let child_hash = harness.hasher.flow_hash(TenantId::DEFAULT, CHILD_ID);
    let retention_key = retention_index_key(
        harness.hasher.partition_of(child_hash),
        closed_record.retention_deadline_micros,
        child_hash,
        RunId(FIRST_CHILD_RUN),
    );
    assert!(
        harness
            .state
            .rows
            .contains_key(&(ColumnFamily::PartitionIndex, retention_key.clone()))
    );
    let outbox = harness
        .outbox(CHILD_ID, FIRST_CHILD_RUN)
        .pop()
        .expect("completion message");
    let completed = harness.deliver_completion(&outbox);
    assert_ok(&completed);
    let parent_hash = harness.hasher.flow_hash(TenantId::DEFAULT, "parent");
    let result_key = run_named_key(
        parent_hash,
        RunId(PARENT_RUN),
        RunTable::SubFlowResult,
        CHILD_ID,
    );
    let bytes = harness
        .state
        .get(ColumnFamily::Flow, &result_key)
        .unwrap()
        .expect("result row");
    let result = SubFlowResultRecord::decode(bytes.as_slice()).expect("result decodes");
    assert_eq!(
        result.result.expect("result").flow_status,
        FlowStatus::Terminated as i32
    );
    let repeated = harness.deliver_completion(&outbox);
    assert_ok(&repeated);
    assert!(repeated.writes.is_empty());
    let ack = harness.apply(
        CHILD_ID,
        &FIRST_CHILD_RUN,
        Command::OutboxAck(OutboxAckCommand {
            message_ids: vec![outbox.message_id.clone()],
        }),
    );
    assert_ok(&ack);
    let row_key = outbox_key(child_hash, RunId(FIRST_CHILD_RUN), &outbox.message_id);
    let retained = harness
        .state
        .get(ColumnFamily::Flow, &row_key)
        .unwrap()
        .expect("retained outbox");
    assert!(
        OutboxRecord::decode(retained.as_slice())
            .unwrap()
            .acknowledged_at_micros
            > 0
    );
    harness.apply(
        CHILD_ID,
        &FIRST_CHILD_RUN,
        Command::TimersFired(TimersFiredCommand {
            timers: vec![TimerId {
                run_attempt: 0,
                kind: Some(timer_id::Kind::OutboxRetention(OutboxRetentionTimerId {
                    message_id: outbox.message_id,
                })),
            }],
        }),
    );
    assert!(
        harness
            .state
            .get(ColumnFamily::Flow, &row_key)
            .unwrap()
            .is_none()
    );
    harness.apply(
        CHILD_ID,
        &FIRST_CHILD_RUN,
        Command::TimersFired(TimersFiredCommand {
            timers: vec![TimerId {
                run_attempt: 0,
                kind: Some(timer_id::Kind::Retention(
                    dexzd_protocol::dexzd::v1::RetentionTimerId {},
                )),
            }],
        }),
    );
    let purged = harness.apply(
        CHILD_ID,
        &FIRST_CHILD_RUN,
        Command::RunPurge(RunPurgeCommand {}),
    );
    assert_ok(&purged);
    assert!(
        !harness
            .state
            .rows
            .contains_key(&(ColumnFamily::PartitionIndex, retention_key))
    );
    let run_key_prefix = run_prefix(child_hash, RunId(FIRST_CHILD_RUN));
    assert!(
        harness
            .state
            .rows
            .keys()
            .all(|(_, key)| !key.starts_with(&run_key_prefix))
    );
    let mut signature = child_hash.to_be_bytes().to_vec();
    signature.extend_from_slice(&FIRST_CHILD_RUN);
    assert!(harness.state.rows.keys().all(|(family, key)| {
        *family != ColumnFamily::PartitionIndex
            || !key
                .windows(signature.len())
                .any(|window| window == signature)
    }));
}

#[test]
fn closed_parent_discards_completion_and_abnormal_child_restarts() {
    let mut harness = Harness::new();
    harness.start_parent();
    harness.deliver_child_start(
        START_MESSAGE,
        FIRST_CHILD_RUN,
        "first",
        SubFlowReusePolicy::Unspecified,
    );
    harness.apply(
        "parent",
        &PARENT_RUN,
        Command::FlowStop(FlowStopCommand {
            request: Some(StopFlowRequest {
                stop_type: StopType::Terminate as i32,
                ..StopFlowRequest::default()
            }),
        }),
    );
    harness.apply(
        CHILD_ID,
        &FIRST_CHILD_RUN,
        Command::FlowStop(FlowStopCommand {
            request: Some(StopFlowRequest {
                stop_type: StopType::Terminate as i32,
                ..StopFlowRequest::default()
            }),
        }),
    );
    let report = harness.outbox(CHILD_ID, FIRST_CHILD_RUN).pop().unwrap();
    let ignored = harness.deliver_completion(&report);
    assert_ok(&ignored);
    assert!(ignored.writes.is_empty());
    let restarted = harness.deliver_child_start(
        [5; 16],
        SECOND_CHILD_RUN,
        "second",
        SubFlowReusePolicy::Unspecified,
    );
    assert!(matches!(
        restarted.reply.reply,
        Some(command_reply::Reply::StartFlow(_))
    ));
    assert_eq!(
        harness.record(CHILD_ID, SECOND_CHILD_RUN).status,
        FlowStatus::Running as i32
    );
}

#[test]
fn attach_reuses_running_run_and_always_restart_terminates_it() {
    let mut harness = Harness::new();
    harness.start_parent();
    harness.deliver_child_start(
        START_MESSAGE,
        FIRST_CHILD_RUN,
        "first",
        SubFlowReusePolicy::Unspecified,
    );
    let attached = harness.deliver_child_start(
        [5; 16],
        SECOND_CHILD_RUN,
        "second",
        SubFlowReusePolicy::Attach,
    );
    assert_ok(&attached);
    assert!(
        harness
            .state
            .rows
            .keys()
            .all(|(_, key)| !key.starts_with(&run_prefix(
                harness.hasher.flow_hash(TenantId::DEFAULT, CHILD_ID),
                RunId(SECOND_CHILD_RUN)
            )))
    );
    let restarted = harness.deliver_child_start(
        [6; 16],
        SECOND_CHILD_RUN,
        "third",
        SubFlowReusePolicy::AlwaysRestart,
    );
    assert!(matches!(
        restarted.reply.reply,
        Some(command_reply::Reply::StartFlow(_))
    ));
    assert_eq!(
        harness.record(CHILD_ID, FIRST_CHILD_RUN).status,
        FlowStatus::Terminated as i32
    );
    assert_eq!(
        harness.record(CHILD_ID, SECOND_CHILD_RUN).status,
        FlowStatus::Running as i32
    );
}

#[test]
fn every_terminal_child_status_reports_to_parent() {
    for expected_status in [
        FlowStatus::Completed,
        FlowStatus::Failed,
        FlowStatus::Canceled,
        FlowStatus::Terminated,
    ] {
        let mut harness = Harness::new();
        harness.start_parent();
        harness.deliver_child_start(
            START_MESSAGE,
            FIRST_CHILD_RUN,
            "child-request",
            SubFlowReusePolicy::Unspecified,
        );
        match expected_status {
            FlowStatus::Completed | FlowStatus::Failed => {
                let invocation = StepInvocationKey {
                    step_execution_id: "ChildStep-1".to_owned(),
                    method: StepMethod::Execute as i32,
                };
                harness.apply(
                    CHILD_ID,
                    &FIRST_CHILD_RUN,
                    Command::StepAttemptStarted(StepAttemptStartedCommand {
                        invocation: Some(invocation.clone()),
                        attempt: 1,
                        task_id: [8; 16].to_vec(),
                    }),
                );
                if expected_status == FlowStatus::Completed {
                    harness.apply(
                        CHILD_ID,
                        &FIRST_CHILD_RUN,
                        Command::StepMethodCompleted(StepMethodCompletedCommand {
                            invocation: Some(invocation),
                            attempt: 1,
                            response: Some(step_method_completed_command::Response::Execute(
                                InvokeExecuteMethodResponse {
                                    step_decision: Some(StepDecision {
                                        close_decision: Some(CloseDecision {
                                            close_decision_type: CloseDecisionType::ForceComplete
                                                as i32,
                                            close_input: Some(Value {
                                                kind: Some(value::Kind::StringValue(
                                                    "x".repeat(500),
                                                )),
                                            }),
                                            ..CloseDecision::default()
                                        }),
                                        ..StepDecision::default()
                                    }),
                                    ..InvokeExecuteMethodResponse::default()
                                },
                            )),
                            ..StepMethodCompletedCommand::default()
                        }),
                    );
                } else {
                    harness.apply(
                        CHILD_ID,
                        &FIRST_CHILD_RUN,
                        Command::StepAttemptFailed(StepAttemptFailedCommand {
                            invocation: Some(invocation),
                            attempt: 1,
                            kind: 1,
                            is_non_retryable: true,
                            server_detail: "failed".to_owned(),
                            ..StepAttemptFailedCommand::default()
                        }),
                    );
                }
            }
            FlowStatus::Canceled | FlowStatus::Terminated => {
                let stop_type = if expected_status == FlowStatus::Canceled {
                    StopType::Cancel
                } else {
                    StopType::Terminate
                };
                harness.apply(
                    CHILD_ID,
                    &FIRST_CHILD_RUN,
                    Command::FlowStop(FlowStopCommand {
                        request: Some(StopFlowRequest {
                            stop_type: stop_type as i32,
                            ..StopFlowRequest::default()
                        }),
                    }),
                );
            }
            _ => unreachable!("test names only terminal statuses"),
        }
        let outbox = harness.outbox(CHILD_ID, FIRST_CHILD_RUN);
        assert_eq!(outbox.len(), 1, "one report for {expected_status:?}");
        let outbox_record::Payload::ChildCompleted(report) = outbox[0].payload.as_ref().unwrap()
        else {
            panic!("expected completion report")
        };
        assert_eq!(
            report.result.as_ref().unwrap().flow_status,
            expected_status as i32
        );
        assert_ok(&harness.deliver_completion(&outbox[0]));
        if expected_status == FlowStatus::Completed {
            let parent_hash = harness.hasher.flow_hash(TenantId::DEFAULT, "parent");
            let prefix = run_table_prefix(parent_hash, RunId(PARENT_RUN), RunTable::Value);
            assert!(harness.state.rows.keys().any(|(family, key)| {
                *family == ColumnFamily::Value && key.starts_with(&prefix)
            }));
        }
    }
}

#[test]
fn hard_delete_ack_removes_pending_row_and_index_idempotently() {
    let mut harness = Harness::new();
    harness.start_parent();
    let flow_hash = harness.hasher.flow_hash(TenantId::DEFAULT, "parent");
    let partition = harness.hasher.partition_of(flow_hash);
    let row_key = hard_delete_pending_key(flow_hash, RunId(PARENT_RUN), "object-1");
    let index_key = run_item_index_key(
        partition,
        IndexKind::HardDeletePending,
        flow_hash,
        RunId(PARENT_RUN),
        b"object-1",
    );
    harness.state.rows.insert(
        (ColumnFamily::Flow, row_key.clone()),
        HardDeleteObjectRecord {
            object: Some(HardDeleteObjectReference {
                object_id: "object-1".to_owned(),
                ..HardDeleteObjectReference::default()
            }),
            written_seq: 1,
        }
        .encode_to_vec(),
    );
    harness.state.rows.insert(
        (ColumnFamily::PartitionIndex, index_key.clone()),
        Vec::new(),
    );
    let command = Command::HardDeleteObjectsDeleted(HardDeleteObjectsDeletedCommand {
        object_ids: vec!["object-1".to_owned()],
    });
    assert_ok(&harness.apply("parent", &PARENT_RUN, command.clone()));
    assert!(
        !harness
            .state
            .rows
            .contains_key(&(ColumnFamily::Flow, row_key))
    );
    assert!(
        !harness
            .state
            .rows
            .contains_key(&(ColumnFamily::PartitionIndex, index_key))
    );
    let repeated = harness.apply("parent", &PARENT_RUN, command);
    assert_ok(&repeated);
    assert!(repeated.writes.is_empty());
}

#[test]
fn terminal_attach_reports_immediately_and_same_request_never_restarts() {
    let mut harness = Harness::new();
    harness.start_parent();
    harness.deliver_child_start(
        START_MESSAGE,
        FIRST_CHILD_RUN,
        "first",
        SubFlowReusePolicy::Unspecified,
    );
    let same_request = harness.deliver_child_start(
        [5; 16],
        SECOND_CHILD_RUN,
        "first",
        SubFlowReusePolicy::AlwaysRestart,
    );
    assert_ok(&same_request);
    assert_eq!(
        harness.record(CHILD_ID, FIRST_CHILD_RUN).status,
        FlowStatus::Running as i32
    );
    harness.apply(
        CHILD_ID,
        &FIRST_CHILD_RUN,
        Command::FlowStop(FlowStopCommand {
            request: Some(StopFlowRequest {
                stop_type: StopType::Terminate as i32,
                ..StopFlowRequest::default()
            }),
        }),
    );
    let attached = harness.deliver_child_start(
        [6; 16],
        SECOND_CHILD_RUN,
        "different",
        SubFlowReusePolicy::Attach,
    );
    assert_ok(&attached);
    let reports = harness.outbox(CHILD_ID, FIRST_CHILD_RUN);
    assert_eq!(reports.len(), 2);
    for report in reports {
        let outbox_record::Payload::ChildCompleted(completed) = report.payload.unwrap() else {
            panic!("terminal attach writes a completion report")
        };
        assert_eq!(
            completed.result.unwrap().flow_status,
            FlowStatus::Terminated as i32
        );
    }
}

#[test]
fn invalid_child_parameters_reject_without_writes() {
    let mut harness = Harness::new();
    harness.start_parent();
    let parent = parent_reference();
    let invalid = ChildFlowStartMessage {
        child_flow_id: CHILD_ID.to_owned(),
        condition: Some(SubFlowCondition {
            sub_flow_type: "ChildFlow".to_owned(),
            start_step_type: "ChildStep".to_owned(),
            options: Some(SubFlowOptions {
                flow_start_delay_seconds: 1,
                ..SubFlowOptions::default()
            }),
            ..SubFlowCondition::default()
        }),
        flow_config: Some(FlowConfig {
            worker_target: Some(pull_worker()),
            ..FlowConfig::default()
        }),
        request_id: "invalid".to_owned(),
        parent: Some(parent.clone()),
    };
    let rejected = harness.apply(
        CHILD_ID,
        &[],
        Command::InboxMessage(InboxMessageCommand {
            message_id: START_MESSAGE.to_vec(),
            source: parent.parent,
            payload: Some(inbox_message_command::Payload::ChildStart(invalid)),
            new_run_id: FIRST_CHILD_RUN.to_vec(),
            settings: Some(settings()),
        }),
    );
    assert!(matches!(
        rejected.reply.reply,
        Some(command_reply::Reply::Error(error)) if error.grpc_code == 3
    ));
    assert!(rejected.writes.is_empty());
    let child_hash = harness.hasher.flow_hash(TenantId::DEFAULT, CHILD_ID);
    assert!(
        harness
            .state
            .rows
            .keys()
            .all(|(_, key)| { !key.starts_with(&run_prefix(child_hash, RunId(FIRST_CHILD_RUN))) })
    );
}

#[test]
fn stale_attempt_timer_is_disarmed_without_closing_the_run() {
    let mut harness = Harness::new();
    harness.start_parent_with_timeout(5);
    let flow_hash = harness.hasher.flow_hash(TenantId::DEFAULT, "parent");
    let partition = harness.hasher.partition_of(flow_hash);
    let timer = TimerId {
        run_attempt: 1,
        kind: Some(timer_id::Kind::FlowTimeout(FlowTimeoutTimerId {})),
    };
    let encoded_timer = timer.encode_to_vec();
    let deadline = harness.record("parent", PARENT_RUN).timeout_deadline_micros;
    let row_key = timer_key(flow_hash, RunId(PARENT_RUN), &encoded_timer);
    let index_key = timer_index_key(
        partition,
        deadline,
        flow_hash,
        RunId(PARENT_RUN),
        &encoded_timer,
    );
    assert!(
        harness
            .state
            .rows
            .contains_key(&(ColumnFamily::Flow, row_key.clone()))
    );
    assert!(
        harness
            .state
            .rows
            .contains_key(&(ColumnFamily::PartitionIndex, index_key.clone()))
    );
    let record_key = run_singleton_key(flow_hash, RunId(PARENT_RUN), RunTable::RunRecord);
    let mut record = harness.record("parent", PARENT_RUN);
    record.current_attempt = 2;
    harness
        .state
        .rows
        .insert((ColumnFamily::Flow, record_key), record.encode_to_vec());
    assert_ok(&harness.apply(
        "parent",
        &PARENT_RUN,
        Command::TimersFired(TimersFiredCommand {
            timers: vec![timer],
        }),
    ));
    assert!(
        !harness
            .state
            .rows
            .contains_key(&(ColumnFamily::Flow, row_key))
    );
    assert!(
        !harness
            .state
            .rows
            .contains_key(&(ColumnFamily::PartitionIndex, index_key))
    );
    assert_eq!(
        harness.record("parent", PARENT_RUN).status,
        FlowStatus::Running as i32
    );
}
