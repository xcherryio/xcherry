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
    flow_list_index_key, hard_delete_pending_key, outbox_key, prefix_range, retention_index_key,
    run_item_index_key, run_named_key, run_singleton_key, tenant_run_index_key, timer_index_key,
    timer_key,
};
use dexzd_keyspace::{
    ColumnFamily, IndexKind, PartitionHasher, RunId, RunTable, TenantId, flow_directory_key,
    run_prefix, run_table_prefix,
};
use dexzd_protocol::dex::{
    AttributeWrite, CloseDecision, CloseDecisionType, FlowConfig, FlowErrorType, FlowStatus,
    FlowTimeoutHandlerOptions, FlowTimeoutPolicy, InvokeExecuteMethodResponse,
    InvokeWaitForMethodResponse, StartFlowRequest, StepDecision, StepOptions, StopFlowRequest,
    StopType, SubFlowCondition, SubFlowOptions, SubFlowReusePolicy, Value, WaitingCondition,
    WaitingConditionType, WorkerTarget, value,
};
use dexzd_protocol::dexzd::v1::{
    ChildFlowStartMessage, CrossFlowEndpoint, FlowCommand, FlowStartCommand, FlowStopCommand,
    FlowTimeoutTimerId, HardDeleteObjectRecord, HardDeleteObjectReference,
    HardDeleteObjectsDeletedCommand, InboxMessageCommand, OutboxAckCommand, OutboxRecord,
    OutboxRetentionTimerId, ParentFlowReference, RetentionTimerId, RunPurgeCommand, RunRecord,
    RunSettings, StepAttemptFailedCommand, StepAttemptStartedCommand, StepExecutionPhase,
    StepExecutionRecord, StepInvocationKey, StepMethod, StepMethodCompletedCommand,
    SubFlowResultRecord, TimerId, TimersFiredCommand, command_reply, flow_command::Command,
    inbox_message_command, outbox_record, step_method_completed_command, timer_id,
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

    fn register_sub_flow_wait(&mut self, reuse_policy: SubFlowReusePolicy) -> OutboxRecord {
        assert_ok(&self.submit_sub_flow_wait(SubFlowCondition {
            condition_id: "child".to_owned(),
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
        }));
        self.outbox("parent", PARENT_RUN)
            .pop()
            .expect("child start outbox")
    }

    fn submit_sub_flow_wait(&mut self, condition: SubFlowCondition) -> ApplyOutcome {
        let invocation = StepInvocationKey {
            step_execution_id: "StartStep-1".to_owned(),
            method: StepMethod::WaitFor as i32,
        };
        assert_ok(&self.apply(
            "parent",
            &PARENT_RUN,
            Command::StepAttemptStarted(StepAttemptStartedCommand {
                invocation: Some(invocation.clone()),
                attempt: 1,
                task_id: vec![9; 16],
            }),
        ));
        self.apply(
            "parent",
            &PARENT_RUN,
            Command::StepMethodCompleted(StepMethodCompletedCommand {
                invocation: Some(invocation),
                attempt: 1,
                response: Some(step_method_completed_command::Response::WaitFor(
                    InvokeWaitForMethodResponse {
                        waiting_condition: Some(WaitingCondition {
                            waiting_condition_type: WaitingConditionType::AllCompleted as i32,
                            sub_flow_conditions: vec![condition],
                            ..WaitingCondition::default()
                        }),
                        ..InvokeWaitForMethodResponse::default()
                    },
                )),
                ..StepMethodCompletedCommand::default()
            }),
        )
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

    fn deliver_start_outbox(
        &mut self,
        outbox: &OutboxRecord,
        new_run_id: [u8; 16],
    ) -> ApplyOutcome {
        let outbox_record::Payload::ChildStart(start) =
            outbox.payload.clone().expect("child start payload")
        else {
            panic!("expected child start")
        };
        let source = start
            .parent
            .as_ref()
            .and_then(|parent| parent.parent.clone());
        self.apply(
            CHILD_ID,
            &[],
            Command::InboxMessage(InboxMessageCommand {
                message_id: outbox.message_id.clone(),
                source,
                payload: Some(inbox_message_command::Payload::ChildStart(start)),
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

    fn step(
        &self,
        flow_id: &str,
        run_id: [u8; 16],
        step_execution_id: &str,
    ) -> StepExecutionRecord {
        let flow_hash = self.hasher.flow_hash(TenantId::DEFAULT, flow_id);
        let key = run_named_key(
            flow_hash,
            RunId(run_id),
            RunTable::StepExecution,
            step_execution_id,
        );
        let bytes = self
            .state
            .get(ColumnFamily::Flow, &key)
            .expect("memory reads")
            .expect("Step exists");
        StepExecutionRecord::decode(bytes.as_slice()).expect("Step decodes")
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
fn wait_for_starts_child_and_completion_wakes_parent() {
    let mut harness = Harness::new();
    harness.start_parent();
    let step_execution_id = "StartStep-1";
    let invocation = StepInvocationKey {
        step_execution_id: step_execution_id.to_owned(),
        method: StepMethod::WaitFor as i32,
    };
    assert_ok(&harness.apply(
        "parent",
        &PARENT_RUN,
        Command::StepAttemptStarted(StepAttemptStartedCommand {
            invocation: Some(invocation.clone()),
            attempt: 1,
            task_id: vec![9; 16],
        }),
    ));
    let input_text = "child input".repeat(100);
    let wait = harness.apply(
        "parent",
        &PARENT_RUN,
        Command::StepMethodCompleted(StepMethodCompletedCommand {
            invocation: Some(invocation),
            attempt: 1,
            response: Some(step_method_completed_command::Response::WaitFor(
                InvokeWaitForMethodResponse {
                    waiting_condition: Some(WaitingCondition {
                        waiting_condition_type: WaitingConditionType::AllCompleted as i32,
                        sub_flow_conditions: vec![SubFlowCondition {
                            condition_id: "child".to_owned(),
                            sub_flow_type: "ChildFlow".to_owned(),
                            start_step_type: "ChildStep".to_owned(),
                            step_input: Some(Value {
                                kind: Some(value::Kind::StringValue(input_text.clone())),
                            }),
                            step_options: Some(StepOptions {
                                skip_wait_for: true,
                                ..StepOptions::default()
                            }),
                            ..SubFlowCondition::default()
                        }],
                        ..WaitingCondition::default()
                    }),
                    ..InvokeWaitForMethodResponse::default()
                },
            )),
            ..StepMethodCompletedCommand::default()
        }),
    );
    assert_ok(&wait);
    let mut starts = harness.outbox("parent", PARENT_RUN);
    assert_eq!(starts.len(), 1);
    let start = starts.remove(0);
    assert_eq!(start.message_id.len(), 16);
    let outbox_record::Payload::ChildStart(start_message) = start.payload.clone().expect("payload")
    else {
        panic!("WaitFor writes a child start")
    };
    assert_eq!(start_message.child_flow_id, CHILD_ID);
    assert_eq!(
        start_message.request_id,
        format!("{}:1:StartStep-1:0", RunId(PARENT_RUN))
    );
    assert_eq!(start_message.parent, Some(parent_reference()));
    assert_eq!(
        start_message
            .flow_config
            .as_ref()
            .and_then(|config| config.worker_target.as_ref()),
        Some(&pull_worker())
    );
    assert_eq!(
        start_message
            .condition
            .as_ref()
            .and_then(|condition| condition.step_input.as_ref()),
        Some(&Value {
            kind: Some(value::Kind::StringValue(input_text)),
        })
    );
    assert!(start.held_values.is_empty());
    let delivered = harness.apply(
        CHILD_ID,
        &[],
        Command::InboxMessage(InboxMessageCommand {
            message_id: start.message_id.clone(),
            source: start_message
                .parent
                .as_ref()
                .and_then(|parent| parent.parent.clone()),
            payload: Some(inbox_message_command::Payload::ChildStart(start_message)),
            new_run_id: FIRST_CHILD_RUN.to_vec(),
            settings: Some(settings()),
        }),
    );
    assert_ok(&delivered);
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
    let completed = harness.outbox(CHILD_ID, FIRST_CHILD_RUN);
    assert_eq!(completed.len(), 1);
    let prior_semantic_events = harness.record("parent", PARENT_RUN).semantic_event_count;
    assert_ok(&harness.deliver_completion(&completed[0]));
    assert_eq!(
        harness.record("parent", PARENT_RUN).semantic_event_count,
        prior_semantic_events + 1
    );
    let step = harness.step("parent", PARENT_RUN, step_execution_id);
    assert_eq!(step.phase, StepExecutionPhase::Execute as i32);
    let results = step.condition_results.expect("wait completed");
    assert_eq!(results.sub_flow_results.len(), 1);
    assert_eq!(
        results.sub_flow_results[0].flow_status,
        FlowStatus::Terminated as i32
    );
    assert_ok(&harness.apply(
        "parent",
        &PARENT_RUN,
        Command::OutboxAck(OutboxAckCommand {
            message_ids: vec![start.message_id.clone()],
        }),
    ));
    harness.now_micros += 24 * 3600 * SECOND;
    assert_ok(&harness.apply(
        "parent",
        &PARENT_RUN,
        Command::TimersFired(TimersFiredCommand {
            timers: vec![TimerId {
                run_attempt: 0,
                kind: Some(timer_id::Kind::OutboxRetention(OutboxRetentionTimerId {
                    message_id: start.message_id,
                })),
            }],
        }),
    ));
    assert!(harness.outbox("parent", PARENT_RUN).is_empty());
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
    assert_ok(&started);
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
        &format!("{:010}:{CHILD_ID}", 1),
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
fn pending_outbox_defers_purge_and_purging_child_start_retries() {
    let mut harness = Harness::new();
    harness.start_parent();
    assert_ok(&harness.deliver_child_start(
        START_MESSAGE,
        FIRST_CHILD_RUN,
        "first",
        SubFlowReusePolicy::Unspecified,
    ));
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
    let child_hash = harness.hasher.flow_hash(TenantId::DEFAULT, CHILD_ID);
    let partition = harness.hasher.partition_of(child_hash);
    let original_deadline = harness
        .record(CHILD_ID, FIRST_CHILD_RUN)
        .retention_deadline_micros;
    harness.now_micros = original_deadline;
    let retention_timer = TimerId {
        run_attempt: 0,
        kind: Some(timer_id::Kind::Retention(RetentionTimerId {})),
    };
    assert_ok(&harness.apply(
        CHILD_ID,
        &FIRST_CHILD_RUN,
        Command::TimersFired(TimersFiredCommand {
            timers: vec![retention_timer.clone()],
        }),
    ));
    assert!(harness.record(CHILD_ID, FIRST_CHILD_RUN).is_purging);
    let unavailable = harness.deliver_child_start(
        [8; 16],
        SECOND_CHILD_RUN,
        "second",
        SubFlowReusePolicy::Attach,
    );
    assert!(matches!(
        unavailable.reply.reply,
        Some(command_reply::Reply::Error(error)) if error.grpc_code == 14
    ));
    assert!(unavailable.writes.is_empty());
    assert_ok(&harness.apply(
        CHILD_ID,
        &FIRST_CHILD_RUN,
        Command::RunPurge(RunPurgeCommand {}),
    ));
    let deferred = harness.record(CHILD_ID, FIRST_CHILD_RUN);
    assert!(!deferred.is_purging);
    assert!(deferred.retention_deadline_micros > original_deadline);
    let report = harness
        .outbox(CHILD_ID, FIRST_CHILD_RUN)
        .pop()
        .expect("pending completion report");
    let pending_index = run_item_index_key(
        partition,
        IndexKind::OutboxPending,
        child_hash,
        RunId(FIRST_CHILD_RUN),
        &report.message_id,
    );
    assert!(
        harness
            .state
            .rows
            .contains_key(&(ColumnFamily::PartitionIndex, pending_index.clone()))
    );
    let object_key = hard_delete_pending_key(child_hash, RunId(FIRST_CHILD_RUN), "object-1");
    let object_index = run_item_index_key(
        partition,
        IndexKind::HardDeletePending,
        child_hash,
        RunId(FIRST_CHILD_RUN),
        b"object-1",
    );
    harness.state.rows.insert(
        (ColumnFamily::Flow, object_key),
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
        (ColumnFamily::PartitionIndex, object_index.clone()),
        Vec::new(),
    );
    let search_index = tenant_run_index_key(
        partition,
        IndexKind::SearchDocument,
        TenantId::DEFAULT,
        child_hash,
        RunId(FIRST_CHILD_RUN),
    );
    harness.state.rows.insert(
        (ColumnFamily::PartitionIndex, search_index.clone()),
        Vec::new(),
    );
    let flow_list_index = flow_list_index_key(
        partition,
        TenantId::DEFAULT,
        "ChildFlow",
        u8::try_from(FlowStatus::Terminated as i32).expect("known Flow status fits u8"),
        child_hash,
        RunId(FIRST_CHILD_RUN),
    );
    harness.state.rows.insert(
        (ColumnFamily::PartitionIndex, flow_list_index.clone()),
        Vec::new(),
    );
    assert!(
        harness
            .state
            .rows
            .contains_key(&(ColumnFamily::PartitionIndex, flow_list_index.clone()))
    );
    assert_ok(&harness.apply(
        CHILD_ID,
        &FIRST_CHILD_RUN,
        Command::OutboxAck(OutboxAckCommand {
            message_ids: vec![report.message_id],
        }),
    ));
    harness.now_micros = deferred.retention_deadline_micros;
    assert_ok(&harness.apply(
        CHILD_ID,
        &FIRST_CHILD_RUN,
        Command::TimersFired(TimersFiredCommand {
            timers: vec![retention_timer],
        }),
    ));
    assert_ok(&harness.apply(
        CHILD_ID,
        &FIRST_CHILD_RUN,
        Command::RunPurge(RunPurgeCommand {}),
    ));
    let directory_key = flow_directory_key(child_hash, TenantId::DEFAULT, CHILD_ID);
    for key in [pending_index, object_index, search_index, flow_list_index] {
        assert!(
            !harness
                .state
                .rows
                .contains_key(&(ColumnFamily::PartitionIndex, key))
        );
    }
    assert!(
        !harness
            .state
            .rows
            .contains_key(&(ColumnFamily::Flow, directory_key))
    );
    let run_key_prefix = run_prefix(child_hash, RunId(FIRST_CHILD_RUN));
    assert!(
        harness
            .state
            .rows
            .keys()
            .all(|(_, key)| !key.starts_with(&run_key_prefix))
    );
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
    assert_ok(&restarted);
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
    assert_ok(&restarted);
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
fn running_attach_reports_to_both_parent_attempts() {
    let mut harness = Harness::new();
    harness.start_parent();
    assert_ok(&harness.deliver_child_start(
        START_MESSAGE,
        FIRST_CHILD_RUN,
        "first",
        SubFlowReusePolicy::Unspecified,
    ));
    let mut attached_parent = parent_reference();
    attached_parent
        .parent
        .as_mut()
        .expect("parent endpoint")
        .run_attempt = 2;
    let source = attached_parent.parent.clone();
    assert_ok(&harness.apply(
        CHILD_ID,
        &[],
        Command::InboxMessage(InboxMessageCommand {
            message_id: vec![55; 16],
            source,
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
                            reuse_policy: SubFlowReusePolicy::Attach as i32,
                            ..SubFlowOptions::default()
                        }),
                        ..SubFlowCondition::default()
                    }),
                    flow_config: Some(FlowConfig {
                        worker_target: Some(pull_worker()),
                        ..FlowConfig::default()
                    }),
                    request_id: "second".to_owned(),
                    parent: Some(attached_parent),
                },
            )),
            new_run_id: SECOND_CHILD_RUN.to_vec(),
            settings: Some(settings()),
        }),
    ));
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
    let reports = harness.outbox(CHILD_ID, FIRST_CHILD_RUN);
    assert_eq!(reports.len(), 2);
    let mut attempts = Vec::new();
    for report in reports {
        let outbox_record::Payload::ChildCompleted(completed) = report.payload.expect("report")
        else {
            panic!("expected child completion")
        };
        attempts.push(
            completed
                .parent
                .and_then(|parent| parent.parent)
                .expect("parent endpoint")
                .run_attempt,
        );
    }
    attempts.sort_unstable();
    assert_eq!(attempts, vec![1, 2]);
}

#[test]
fn default_reuse_returns_completed_child_without_new_run() {
    let mut harness = Harness::new();
    harness.start_parent();
    assert_ok(&harness.deliver_child_start(
        START_MESSAGE,
        FIRST_CHILD_RUN,
        "first",
        SubFlowReusePolicy::Unspecified,
    ));
    let invocation = StepInvocationKey {
        step_execution_id: "ChildStep-1".to_owned(),
        method: StepMethod::Execute as i32,
    };
    assert_ok(&harness.apply(
        CHILD_ID,
        &FIRST_CHILD_RUN,
        Command::StepAttemptStarted(StepAttemptStartedCommand {
            invocation: Some(invocation.clone()),
            attempt: 1,
            task_id: vec![8; 16],
        }),
    ));
    assert_ok(&harness.apply(
        CHILD_ID,
        &FIRST_CHILD_RUN,
        Command::StepMethodCompleted(StepMethodCompletedCommand {
            invocation: Some(invocation),
            attempt: 1,
            response: Some(step_method_completed_command::Response::Execute(
                InvokeExecuteMethodResponse {
                    step_decision: Some(StepDecision {
                        close_decision: Some(CloseDecision {
                            close_decision_type: CloseDecisionType::ForceComplete as i32,
                            ..CloseDecision::default()
                        }),
                        ..StepDecision::default()
                    }),
                    ..InvokeExecuteMethodResponse::default()
                },
            )),
            ..StepMethodCompletedCommand::default()
        }),
    ));
    assert_ok(&harness.deliver_child_start(
        [56; 16],
        SECOND_CHILD_RUN,
        "second",
        SubFlowReusePolicy::Unspecified,
    ));
    assert_eq!(
        harness.record(CHILD_ID, FIRST_CHILD_RUN).status,
        FlowStatus::Completed as i32
    );
    let child_hash = harness.hasher.flow_hash(TenantId::DEFAULT, CHILD_ID);
    assert!(
        harness
            .state
            .rows
            .keys()
            .all(|(_, key)| { !key.starts_with(&run_prefix(child_hash, RunId(SECOND_CHILD_RUN))) })
    );
    assert_eq!(harness.outbox(CHILD_ID, FIRST_CHILD_RUN).len(), 2);
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
        if expected_status == FlowStatus::Completed {
            let parent_hash = harness.hasher.flow_hash(TenantId::DEFAULT, "parent");
            let key = run_singleton_key(parent_hash, RunId(PARENT_RUN), RunTable::RunRecord);
            let mut record = harness.record("parent", PARENT_RUN);
            record
                .settings
                .as_mut()
                .expect("run settings")
                .max_value_bytes = 100;
            harness
                .state
                .rows
                .insert((ColumnFamily::Flow, key), record.encode_to_vec());
        }
        assert_ok(&harness.deliver_completion(&outbox[0]));
        if expected_status == FlowStatus::Completed {
            let parent_hash = harness.hasher.flow_hash(TenantId::DEFAULT, "parent");
            let prefix = run_table_prefix(parent_hash, RunId(PARENT_RUN), RunTable::Value);
            assert!(harness.state.rows.keys().any(|(family, key)| {
                *family == ColumnFamily::Value && key.starts_with(&prefix)
            }));
            let key = run_named_key(
                parent_hash,
                RunId(PARENT_RUN),
                RunTable::SubFlowResult,
                &format!("{:010}:{CHILD_ID}", 1),
            );
            let result = SubFlowResultRecord::decode(
                harness
                    .state
                    .get(ColumnFamily::Flow, &key)
                    .expect("memory reads")
                    .expect("SubFlow result")
                    .as_slice(),
            )
            .expect("SubFlow result decodes");
            let output = result.result.expect("Flow result").results.remove(0);
            let stored = output.completed_step_output.expect("child output");
            let dexzd_protocol::dexzd::v1::stored_value::Kind::TableValue(reference) =
                stored.kind.expect("stored value")
            else {
                panic!("large child output remains in the value table")
            };
            assert!(reference.size_bytes > 100);
        }
    }
}

#[test]
fn child_flow_timeout_reports_failure_to_parent() {
    let mut harness = Harness::new();
    harness.start_parent();
    let mut start = harness.register_sub_flow_wait(SubFlowReusePolicy::Unspecified);
    let outbox_record::Payload::ChildStart(message) = start.payload.as_mut().expect("start") else {
        panic!("expected child start")
    };
    let options = message
        .condition
        .as_mut()
        .expect("condition")
        .options
        .as_mut()
        .expect("options");
    options.flow_timeout_seconds = 5;
    options.flow_timeout_policy = FlowTimeoutPolicy::Fail as i32;
    assert_ok(&harness.deliver_start_outbox(&start, FIRST_CHILD_RUN));
    let deadline = harness
        .record(CHILD_ID, FIRST_CHILD_RUN)
        .timeout_deadline_micros;
    harness.now_micros = deadline;
    assert_ok(&harness.apply(
        CHILD_ID,
        &FIRST_CHILD_RUN,
        Command::TimersFired(TimersFiredCommand {
            timers: vec![TimerId {
                run_attempt: 1,
                kind: Some(timer_id::Kind::FlowTimeout(FlowTimeoutTimerId {})),
            }],
        }),
    ));
    let report = harness
        .outbox(CHILD_ID, FIRST_CHILD_RUN)
        .pop()
        .expect("timeout report");
    let outbox_record::Payload::ChildCompleted(completed) =
        report.payload.as_ref().expect("report")
    else {
        panic!("expected completion report")
    };
    assert_eq!(
        completed.result.as_ref().expect("result").flow_status,
        FlowStatus::Failed as i32
    );
    assert_ok(&harness.deliver_completion(&report));
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
fn always_restart_old_child_result_does_not_complete_new_attempt_wait() {
    let mut harness = Harness::new();
    harness.start_parent();
    let first_start = harness.register_sub_flow_wait(SubFlowReusePolicy::AlwaysRestart);
    assert_ok(&harness.deliver_start_outbox(&first_start, FIRST_CHILD_RUN));
    let parent_hash = harness.hasher.flow_hash(TenantId::DEFAULT, "parent");
    let record_key = run_singleton_key(parent_hash, RunId(PARENT_RUN), RunTable::RunRecord);
    let mut parent_record = harness.record("parent", PARENT_RUN);
    parent_record.current_attempt = 2;
    harness.state.rows.insert(
        (ColumnFamily::Flow, record_key),
        parent_record.encode_to_vec(),
    );
    let step_key = run_named_key(
        parent_hash,
        RunId(PARENT_RUN),
        RunTable::StepExecution,
        "StartStep-1",
    );
    let mut step = harness.step("parent", PARENT_RUN, "StartStep-1");
    step.wait
        .as_mut()
        .expect("waiting Step")
        .sub_flow_registration_attempt = 2;
    harness
        .state
        .rows
        .insert((ColumnFamily::Flow, step_key), step.encode_to_vec());
    let outbox_record::Payload::ChildStart(mut restarted_start) =
        first_start.payload.expect("start payload")
    else {
        panic!("expected child start")
    };
    let restarted_parent = restarted_start.parent.as_mut().expect("parent reference");
    restarted_parent
        .parent
        .as_mut()
        .expect("parent endpoint")
        .run_attempt = 2;
    restarted_start.request_id = format!("{}:2:StartStep-1:0", RunId(PARENT_RUN));
    restarted_start
        .condition
        .as_mut()
        .expect("SubFlow condition")
        .options
        .as_mut()
        .expect("SubFlow options")
        .reuse_policy = SubFlowReusePolicy::AlwaysRestart as i32;
    let source = restarted_parent.parent.clone();
    assert_ok(&harness.apply(
        CHILD_ID,
        &[],
        Command::InboxMessage(InboxMessageCommand {
            message_id: vec![77; 16],
            source,
            payload: Some(inbox_message_command::Payload::ChildStart(restarted_start)),
            new_run_id: SECOND_CHILD_RUN.to_vec(),
            settings: Some(settings()),
        }),
    ));
    assert_eq!(
        harness.record(CHILD_ID, FIRST_CHILD_RUN).status,
        FlowStatus::Terminated as i32
    );
    let old_report = harness.outbox(CHILD_ID, FIRST_CHILD_RUN);
    assert_eq!(old_report.len(), 1);
    assert_ok(&harness.deliver_completion(&old_report[0]));
    assert_eq!(
        harness.step("parent", PARENT_RUN, "StartStep-1").phase,
        StepExecutionPhase::Waiting as i32
    );
    harness.apply(
        CHILD_ID,
        &SECOND_CHILD_RUN,
        Command::FlowStop(FlowStopCommand {
            request: Some(StopFlowRequest {
                stop_type: StopType::Terminate as i32,
                ..StopFlowRequest::default()
            }),
        }),
    );
    let new_report = harness.outbox(CHILD_ID, SECOND_CHILD_RUN);
    assert_eq!(new_report.len(), 1);
    assert_ok(&harness.deliver_completion(&new_report[0]));
    assert_eq!(
        harness.step("parent", PARENT_RUN, "StartStep-1").phase,
        StepExecutionPhase::Execute as i32
    );
}

#[test]
fn invalid_child_parameters_fail_parent_without_wait_effects() {
    let mut harness = Harness::new();
    harness.start_parent();
    let invocation = StepInvocationKey {
        step_execution_id: "StartStep-1".to_owned(),
        method: StepMethod::WaitFor as i32,
    };
    assert_ok(&harness.apply(
        "parent",
        &PARENT_RUN,
        Command::StepAttemptStarted(StepAttemptStartedCommand {
            invocation: Some(invocation.clone()),
            attempt: 1,
            task_id: vec![9; 16],
        }),
    ));
    assert_ok(&harness.apply(
        "parent",
        &PARENT_RUN,
        Command::StepMethodCompleted(StepMethodCompletedCommand {
            invocation: Some(invocation),
            attempt: 1,
            response: Some(step_method_completed_command::Response::WaitFor(
                InvokeWaitForMethodResponse {
                    waiting_condition: Some(WaitingCondition {
                        waiting_condition_type: WaitingConditionType::AllCompleted as i32,
                        sub_flow_conditions: vec![SubFlowCondition {
                            sub_flow_type: "ChildFlow".to_owned(),
                            start_step_type: "ChildStep".to_owned(),
                            options: Some(SubFlowOptions {
                                flow_start_delay_seconds: 1,
                                ..SubFlowOptions::default()
                            }),
                            ..SubFlowCondition::default()
                        }],
                        ..WaitingCondition::default()
                    }),
                    upsert_attributes: vec![AttributeWrite {
                        key: "should-not-write".to_owned(),
                        value: Some(Value {
                            kind: Some(value::Kind::StringValue("value".to_owned())),
                        }),
                        ..AttributeWrite::default()
                    }],
                    ..InvokeWaitForMethodResponse::default()
                },
            )),
            ..StepMethodCompletedCommand::default()
        }),
    ));
    let parent = harness.record("parent", PARENT_RUN);
    assert_eq!(parent.status, FlowStatus::Failed as i32);
    let close = parent.close.expect("parent close");
    assert_eq!(close.error_type, FlowErrorType::InvalidUserFlowCode as i32);
    assert!(close.error_message.contains("delay"));
    assert!(harness.outbox("parent", PARENT_RUN).is_empty());
    let parent_hash = harness.hasher.flow_hash(TenantId::DEFAULT, "parent");
    assert!(!harness.state.rows.contains_key(&(
        ColumnFamily::Flow,
        run_named_key(
            parent_hash,
            RunId(PARENT_RUN),
            RunTable::Attribute,
            "should-not-write",
        ),
    )));
}

#[test]
fn sub_flow_start_validation_fails_parent_before_outbox_write() {
    let base = SubFlowCondition {
        condition_id: "child".to_owned(),
        sub_flow_type: "ChildFlow".to_owned(),
        start_step_type: "ChildStep".to_owned(),
        options: Some(SubFlowOptions::default()),
        ..SubFlowCondition::default()
    };
    let mut unstable_index = base.clone();
    unstable_index.sub_flow_index = 1;
    let mut invalid_timeout_policy = base.clone();
    invalid_timeout_policy
        .options
        .as_mut()
        .expect("options")
        .flow_timeout_policy = FlowTimeoutPolicy::Fail as i32;
    let mut invalid_handler_options = base.clone();
    invalid_handler_options
        .options
        .as_mut()
        .expect("options")
        .timeout_handler_options = Some(FlowTimeoutHandlerOptions::default());
    let mut invalid_config = base;
    invalid_config
        .options
        .as_mut()
        .expect("options")
        .flow_config_override = Some(FlowConfig {
        continue_as_new_threshold: Some(-1),
        ..FlowConfig::default()
    });
    for (condition, expected_error) in [
        (unstable_index, "unstable index"),
        (invalid_timeout_policy, "positive timeout"),
        (invalid_handler_options, "Handler policy"),
        (invalid_config, "thresholds"),
    ] {
        let mut harness = Harness::new();
        harness.start_parent();
        assert_ok(&harness.submit_sub_flow_wait(condition));
        let record = harness.record("parent", PARENT_RUN);
        assert_eq!(record.status, FlowStatus::Failed as i32);
        let close = record.close.expect("parent close");
        assert_eq!(close.error_type, FlowErrorType::InvalidUserFlowCode as i32);
        assert!(close.error_message.contains(expected_error));
        assert!(harness.outbox("parent", PARENT_RUN).is_empty());
    }
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
