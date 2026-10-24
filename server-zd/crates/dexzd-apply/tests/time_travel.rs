// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! History, checkpoints, the window, and time travel over an in-memory state.

use std::collections::BTreeMap;

use dexzd_apply::diagnostics::{current_semantic_point_hash, time_travel_target_hash};
use dexzd_apply::read_model::get_search_document;
use dexzd_apply::{
    ApplyContext, ApplyEffect, ApplyError, ApplyOutcome, FlowStateReader, StateReadError, StateRow,
    StateWrite, apply_flow_command,
};
use dexzd_keyspace::row_keys::{
    attempt_record_key, checkpoint_key, hard_delete_current_key, hard_delete_pending_key,
    history_record_key, run_singleton_key, value_row_key,
};
use dexzd_keyspace::{
    ColumnFamily, IndexKind, PartitionHasher, RunId, RunTable, TenantId, ValueId,
    flow_directory_key, parse_run_key,
};
use dexzd_protocol::dex::{
    AttributeWrite, ChannelMessage, ChannelMessageDeletion, CloseDecision, CloseDecisionType,
    DeleteChannelMessageRequest, FlowResetStepMethod, FlowResetType, FlowStatus, FlowTimeoutPolicy,
    IndexConfig, InvokeExecuteMethodResponse, InvokeRpcRequest, InvokeWaitForMethodResponse,
    InvokeWorkerRpcResponse, PublishToChannelRequest, ResetFlowRequest, SetAttributesRequest,
    SkipTimerRequest, StartFlowRequest, StepDecision, StepMovement, StepOptions, TimerCondition,
    Value, WaitingCondition, WaitingConditionType, WorkerErrorResponse, WorkerTarget,
    flow_history_event, value,
};
use dexzd_protocol::dexzd::v1::{
    AttemptRecord, AttributeRecord, AttributesSetCommand, ChannelMessageDeleteCommand,
    ChannelMetaRecord, ChannelPublishCommand, CheckpointRecord, CommandReply, FlowCommand,
    FlowDirectoryRecord, FlowStartCommand, HardDeleteObjectRecord, HardDeleteObjectReference,
    HistoryRecord, RetryBackoffTimerId, RpcCommittedCommand, RunRecord, RunSettings,
    StepAttemptFailedCommand, StepAttemptStartedCommand, StepExecutionPhase, StepExecutionRecord,
    StepInvocationKey, StepInvocationPhase, StepMethod, StepMethodCompletedCommand,
    TimeTravelCommand, TimerId, TimerRecord, TimerSkipCommand, TimersFiredCommand, ValueRecord,
    command_reply, flow_command::Command, step_method_completed_command, stored_value, timer_id,
};
use prost::Message;

#[derive(Default, Clone)]
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

const SECOND: i64 = 1_000_000;

/// Attempt rows without timer run attempts and dispatch IDs, so attempts compare.
type AttemptRows = BTreeMap<(u8, Vec<u8>), Vec<u8>>;

struct Harness {
    state: MemoryState,
    context: ApplyContext,
    now_micros: i64,
    flow_id: String,
    run_id: Vec<u8>,
    settings: RunSettings,
}

impl Harness {
    fn new() -> Self {
        Self::with_settings(settings())
    }

    fn with_settings(settings: RunSettings) -> Self {
        let flow_id = "order-1";
        let hasher = PartitionHasher::new(&[7; 16], 1024).expect("valid partition hasher");
        let flow_hash = hasher.flow_hash(TenantId::DEFAULT, flow_id);
        Self {
            state: MemoryState::default(),
            context: ApplyContext {
                partition: hasher.partition_of(flow_hash),
                flow_hash,
            },
            now_micros: 1_800_000_000 * SECOND,
            flow_id: flow_id.to_owned(),
            run_id: Vec::new(),
            settings,
        }
    }

    fn command(&mut self, body: Command) -> FlowCommand {
        self.now_micros += 1000;
        FlowCommand {
            flow_id: self.flow_id.clone(),
            run_id: if matches!(body, Command::FlowStart(_)) {
                Vec::new()
            } else {
                self.run_id.clone()
            },
            expected_run_attempt: 0,
            epoch: 1,
            behavior_version: 1,
            proposed_at_micros: self.now_micros,
            command_id: (self.now_micros as u128).to_be_bytes().to_vec(),
            tenant_id: 0,
            command: Some(body),
        }
    }

    fn apply(&mut self, body: Command) -> ApplyOutcome {
        let command = self.command(body);
        let outcome =
            apply_flow_command(&self.context, &self.state, &command).expect("apply succeeds");
        let again =
            apply_flow_command(&self.context, &self.state, &command).expect("apply succeeds");
        assert_eq!(outcome.writes, again.writes, "apply must be deterministic");
        assert_eq!(
            outcome.effects, again.effects,
            "effects must be deterministic"
        );
        self.state.commit(&outcome);
        outcome
    }

    fn start(&mut self, request: StartFlowRequest) -> ApplyOutcome {
        let outcome = self.apply(Command::FlowStart(FlowStartCommand {
            new_run_id: vec![9; 16],
            request: Some(request),
            settings: Some(self.settings),
            resolved_timeout_policy: FlowTimeoutPolicy::Fail as i32,
            worker_target: Some(WorkerTarget {
                is_pull_mode: true,
                ..WorkerTarget::default()
            }),
            hard_delete_objects: Vec::new(),
            request_fingerprint: Vec::new(),
        }));
        if let Some(command_reply::Reply::StartFlow(reply)) = &outcome.reply.reply {
            self.run_id = reply.run_id.clone();
        }
        outcome
    }

    fn run(&self) -> RunId {
        RunId::from_slice(&self.run_id).expect("the harness has a run")
    }

    fn get<RowMessage: Message + Default>(
        &self,
        column_family: ColumnFamily,
        key: &[u8],
    ) -> Option<RowMessage> {
        self.state
            .rows
            .get(&(column_family, key.to_vec()))
            .map(|bytes| RowMessage::decode(bytes.as_slice()).expect("row decodes"))
    }

    fn run_record(&self) -> RunRecord {
        let key = run_singleton_key(self.context.flow_hash, self.run(), RunTable::RunRecord);
        self.get(ColumnFamily::Flow, &key)
            .expect("the run record exists")
    }

    fn attempt_record(&self, attempt: u32) -> AttemptRecord {
        let key = attempt_record_key(self.context.flow_hash, self.run(), attempt);
        self.get(ColumnFamily::Flow, &key)
            .expect("the attempt record exists")
    }

    fn history_record(&self, seq: u64) -> Option<HistoryRecord> {
        let key = history_record_key(self.context.flow_hash, self.run(), seq);
        self.get(ColumnFamily::History, &key)
    }

    fn checkpoint(&self, attempt: u32, seq: u64) -> Option<CheckpointRecord> {
        let key = checkpoint_key(self.context.flow_hash, self.run(), attempt, seq);
        self.get(ColumnFamily::History, &key)
    }

    fn last_seq(&self) -> u64 {
        self.run_record().next_run_seq - 1
    }

    fn directory_status(&self) -> i32 {
        let key = flow_directory_key(self.context.flow_hash, TenantId::DEFAULT, &self.flow_id);
        let directory: FlowDirectoryRecord = self
            .get(ColumnFamily::Flow, &key)
            .expect("the directory exists");
        directory.runs.last().expect("one run").status
    }

    fn has_index_entry(&self, kind: IndexKind) -> bool {
        self.state.rows.keys().any(|(column_family, key)| {
            *column_family == ColumnFamily::PartitionIndex && key.get(2) == Some(&(kind as u8))
        })
    }

    fn step(&self, step_execution_id: &str) -> StepExecutionRecord {
        let key = dexzd_keyspace::row_keys::run_named_key(
            self.context.flow_hash,
            self.run(),
            RunTable::StepExecution,
            step_execution_id,
        );
        self.get(ColumnFamily::Flow, &key)
            .expect("the Step execution exists")
    }

    fn attribute_keys(&self) -> Vec<String> {
        self.attempt_rows()
            .keys()
            .filter(|(table, _)| *table == RunTable::Attribute as u8)
            .map(|(_, suffix)| String::from_utf8(suffix.clone()).expect("Attribute keys are UTF-8"))
            .collect()
    }

    fn channel_message_count(&self) -> usize {
        self.attempt_rows()
            .keys()
            .filter(|(table, _)| *table == RunTable::ChannelMessage as u8)
            .count()
    }

    fn attempt_rows(&self) -> AttemptRows {
        let attempt_tables = [
            RunTable::Attribute,
            RunTable::ChannelMessage,
            RunTable::ChannelMeta,
            RunTable::StepExecution,
            RunTable::StepCounter,
            RunTable::LockTable,
            RunTable::Waiter,
            RunTable::Timer,
        ];
        let mut rows = BTreeMap::new();
        for ((column_family, key), value) in &self.state.rows {
            if *column_family != ColumnFamily::Flow {
                continue;
            }
            let Ok(parsed) = parse_run_key(key) else {
                continue;
            };
            if parsed.run_id != self.run() || !attempt_tables.contains(&parsed.table) {
                continue;
            }
            let (suffix, value) = match parsed.table {
                RunTable::Timer => {
                    let mut timer =
                        TimerRecord::decode(value.as_slice()).expect("timer rows decode");
                    let mut timer_id = timer.timer_id.clone().expect("timer rows name their timer");
                    if timer_id.run_attempt == 0 {
                        continue;
                    }
                    timer_id.run_attempt = 0;
                    timer.timer_id = Some(timer_id.clone());
                    (timer_id.encode_to_vec(), timer.encode_to_vec())
                }
                RunTable::StepExecution => {
                    let mut step =
                        StepExecutionRecord::decode(value.as_slice()).expect("Step rows decode");
                    if let Some(invocation) = step.invocation.as_mut() {
                        invocation.task_id.clear();
                    }
                    (parsed.suffix.to_vec(), step.encode_to_vec())
                }
                _ => (parsed.suffix.to_vec(), value.clone()),
            };
            rows.insert((parsed.table as u8, suffix), value);
        }
        rows
    }

    fn start_attempt(&mut self, step_execution_id: &str, method: StepMethod, attempt: u32) {
        let outcome = self.apply(Command::StepAttemptStarted(StepAttemptStartedCommand {
            invocation: Some(key(step_execution_id, method)),
            attempt,
            task_id: vec![attempt as u8; 16],
            pull_routing: None,
        }));
        assert!(
            !outcome.writes.is_empty(),
            "{step_execution_id} attempt {attempt} starts"
        );
    }

    fn complete_wait_for(&mut self, step_execution_id: &str) {
        let outcome = self.apply(Command::StepMethodCompleted(StepMethodCompletedCommand {
            invocation: Some(key(step_execution_id, StepMethod::WaitFor)),
            attempt: 1,
            response: Some(step_method_completed_command::Response::WaitFor(
                InvokeWaitForMethodResponse::default(),
            )),
            app_worker_id: "worker-a".to_owned(),
            ..StepMethodCompletedCommand::default()
        }));
        assert!(
            !outcome.writes.is_empty(),
            "{step_execution_id} WaitFor completes"
        );
    }

    fn complete_execute(&mut self, step_execution_id: &str, attempt: u32, decision: StepDecision) {
        let outcome = self.apply(Command::StepMethodCompleted(StepMethodCompletedCommand {
            invocation: Some(key(step_execution_id, StepMethod::Execute)),
            attempt,
            response: Some(step_method_completed_command::Response::Execute(
                InvokeExecuteMethodResponse {
                    step_decision: Some(decision),
                    ..InvokeExecuteMethodResponse::default()
                },
            )),
            app_worker_id: "worker-a".to_owned(),
            ..StepMethodCompletedCommand::default()
        }));
        assert!(
            !outcome.writes.is_empty(),
            "{step_execution_id} Execute completes"
        );
    }

    fn set_attribute(&mut self, attribute_key: &str, text: &str, request_id: &str) {
        let outcome = self.apply(Command::AttributesSet(AttributesSetCommand {
            request: Some(SetAttributesRequest {
                attributes: vec![AttributeWrite {
                    key: attribute_key.to_owned(),
                    value: Some(string_value(text)),
                    ..AttributeWrite::default()
                }],
                request_id: request_id.to_owned(),
                ..SetAttributesRequest::default()
            }),
            hard_delete_objects: Vec::new(),
            request_fingerprint: request_id.as_bytes().to_vec(),
        }));
        assert!(is_ok(&outcome.reply), "SetAttributes applies");
    }

    fn publish(&mut self, channel_name: &str, message_id: &str) {
        let outcome = self.apply(Command::ChannelPublish(ChannelPublishCommand {
            request: Some(PublishToChannelRequest {
                messages: vec![ChannelMessage {
                    channel_name: channel_name.to_owned(),
                    value: Some(string_value(message_id)),
                    message_id: message_id.to_owned(),
                }],
                ..PublishToChannelRequest::default()
            }),
        }));
        assert!(is_ok(&outcome.reply), "PublishToChannel applies");
    }

    fn time_travel(&mut self, request: ResetFlowRequest) -> ApplyOutcome {
        self.apply(Command::TimeTravel(TimeTravelCommand {
            request: Some(request),
        }))
    }

    /// Time travels and returns the new run attempt; panics on rejection.
    fn travel_to(&mut self, request: ResetFlowRequest) -> (u32, ApplyOutcome) {
        let outcome = self.time_travel(request);
        match &outcome.reply.reply {
            Some(command_reply::Reply::TimeTravel(reply)) => (reply.run_attempt, outcome),
            other => panic!("time travel was rejected: {other:?}"),
        }
    }
}

fn settings() -> RunSettings {
    RunSettings {
        history_window_semantic_events: 10_000,
        history_prune_chunk_semantic_events: 1000,
        max_retained_superseded_bytes: 256 << 20,
        retention_after_close_micros: 7 * 24 * 3600 * SECOND,
        wire_inline_threshold_bytes: 100,
        max_value_bytes: 16 << 20,
        checkpoint_min_written_bytes: 1 << 20,
        checkpoint_max_commands: 10_000,
        request_dedup_capacity: 1000,
        max_run_state_bytes: 1 << 30,
        max_active_step_executions: 10_000,
    }
}

fn key(step_execution_id: &str, method: StepMethod) -> StepInvocationKey {
    StepInvocationKey {
        step_execution_id: step_execution_id.to_owned(),
        method: method as i32,
    }
}

fn start_request() -> StartFlowRequest {
    StartFlowRequest {
        flow_id: "order-1".to_owned(),
        flow_type: "OrderFlow".to_owned(),
        start_step_type: "ChargeStep".to_owned(),
        step_input: Some(string_value("small input")),
        request_id: "start-1".to_owned(),
        ..StartFlowRequest::default()
    }
}

fn skip_wait_for() -> Option<StepOptions> {
    Some(StepOptions {
        skip_wait_for: true,
        ..StepOptions::default()
    })
}

fn string_value(text: &str) -> Value {
    Value {
        kind: Some(value::Kind::StringValue(text.to_owned())),
    }
}

fn next_steps(step_types: &[&str]) -> StepDecision {
    StepDecision {
        next_steps: step_types
            .iter()
            .map(|step_type| StepMovement {
                step_type: (*step_type).to_owned(),
                step_options: skip_wait_for(),
                ..StepMovement::default()
            })
            .collect(),
        ..StepDecision::default()
    }
}

fn force_complete() -> StepDecision {
    StepDecision {
        close_decision: Some(CloseDecision {
            close_decision_type: CloseDecisionType::ForceComplete as i32,
            close_input: Some(string_value("done")),
            ..CloseDecision::default()
        }),
        ..StepDecision::default()
    }
}

fn reset(reset_type: FlowResetType) -> ResetFlowRequest {
    ResetFlowRequest {
        flow_id: "order-1".to_owned(),
        reset_type: reset_type as i32,
        reason: "test".to_owned(),
        skip_writes_reapply: true,
        ..ResetFlowRequest::default()
    }
}

fn reset_to_time(timestamp_micros: i64) -> ResetFlowRequest {
    ResetFlowRequest {
        history_event_time: (i128::from(timestamp_micros) * 1000).to_string(),
        ..reset(FlowResetType::HistoryEventTime)
    }
}

fn is_ok(reply: &CommandReply) -> bool {
    matches!(reply.reply, Some(command_reply::Reply::Ok(_)))
}

fn error_code(reply: &CommandReply) -> Option<i32> {
    match &reply.reply {
        Some(command_reply::Reply::Error(error)) => Some(error.grpc_code),
        _ => None,
    }
}

fn error_message(reply: &CommandReply) -> String {
    match &reply.reply {
        Some(command_reply::Reply::Error(error)) => error.message.clone(),
        other => panic!("expected an error reply, got {other:?}"),
    }
}

fn pending_effects(outcome: &ApplyOutcome) -> Vec<(String, i32, u32, u32)> {
    outcome
        .effects
        .iter()
        .filter_map(|effect| match effect {
            ApplyEffect::InvocationPending {
                invocation,
                attempt,
                run_attempt,
                ..
            } => Some((
                invocation.step_execution_id.clone(),
                invocation.method,
                *attempt,
                *run_attempt,
            )),
            _ => None,
        })
        .collect()
}

fn started_effects(outcome: &ApplyOutcome) -> Vec<(String, u32, u32, Vec<u8>)> {
    outcome
        .effects
        .iter()
        .filter_map(|effect| match effect {
            ApplyEffect::InvocationStarted {
                invocation,
                attempt,
                run_attempt,
                task_id,
                ..
            } => Some((
                invocation.step_execution_id.clone(),
                *attempt,
                *run_attempt,
                task_id.clone(),
            )),
            _ => None,
        })
        .collect()
}

fn started_timeouts(outcome: &ApplyOutcome) -> Vec<(String, i32, i32)> {
    outcome
        .effects
        .iter()
        .filter_map(|effect| match effect {
            ApplyEffect::InvocationStarted {
                invocation,
                method_timeout_seconds,
                ..
            } => Some((
                invocation.step_execution_id.clone(),
                invocation.method,
                *method_timeout_seconds,
            )),
            _ => None,
        })
        .collect()
}

fn canceled_task_ids(outcome: &ApplyOutcome) -> Vec<Vec<u8>> {
    outcome
        .effects
        .iter()
        .filter_map(|effect| match effect {
            ApplyEffect::InvocationCanceled { task_id, .. } => Some(task_id.clone()),
            _ => None,
        })
        .collect()
}

const FAILED_PRECONDITION: i32 = 9;
const INVALID_ARGUMENT: i32 = 3;

/// Runs ChargeStep-1, then creates two ShipSteps. Returns seqs: Execute pending, ShipStep created.
fn run_charge_then_ship(harness: &mut Harness) -> (u64, u64) {
    harness.start(start_request());
    harness.start_attempt("ChargeStep-1", StepMethod::WaitFor, 1);
    harness.complete_wait_for("ChargeStep-1");
    let execute_pending_seq = harness.last_seq();
    harness.start_attempt("ChargeStep-1", StepMethod::Execute, 1);
    harness.complete_execute("ChargeStep-1", 1, next_steps(&["ShipStep", "ShipStep"]));
    (execute_pending_seq, harness.last_seq())
}

#[test]
fn flow_start_writes_the_genesis_checkpoint_and_records_pending_methods() {
    let mut harness = Harness::new();
    harness.start(start_request());
    let record = harness.run_record();
    assert_eq!(record.last_checkpoint_seq, 1);
    assert_eq!(harness.attempt_record(1).checkpoint_seqs, vec![1]);
    let checkpoint = harness
        .checkpoint(1, 1)
        .expect("the genesis checkpoint exists");
    assert_eq!(
        checkpoint.run_status.unwrap().status,
        FlowStatus::Running as i32
    );
    assert!(!checkpoint.rows.is_empty());
    let genesis = harness.history_record(1).unwrap();
    assert_eq!(genesis.pending_step_methods.len(), 1);
    let pending = &genesis.pending_step_methods[0];
    assert_eq!(pending.step_type, "ChargeStep");
    assert!(pending.is_step_created);
    assert_eq!(genesis.events[0].run_attempt, 1);
}

#[test]
fn checkpoints_follow_the_command_count_and_written_bytes_rules() {
    let mut harness = Harness::with_settings(RunSettings {
        checkpoint_max_commands: 3,
        ..settings()
    });
    harness.start(start_request());
    for index in 0..7 {
        harness.set_attribute("status", &format!("v{index}"), "");
    }
    assert_eq!(harness.attempt_record(1).checkpoint_seqs, vec![1, 4, 7]);
    let mut by_bytes = Harness::with_settings(RunSettings {
        checkpoint_min_written_bytes: 1,
        ..settings()
    });
    by_bytes.start(start_request());
    let state_bytes = by_bytes.run_record().last_checkpoint_state_bytes;
    assert!(state_bytes > 0);
    by_bytes.set_attribute("status", "x", "");
    let record = by_bytes.run_record();
    assert_eq!(
        by_bytes.attempt_record(1).checkpoint_seqs,
        vec![1],
        "small writes stay below the current state size"
    );
    assert!(record.written_bytes_since_checkpoint > 0);
    let mut checkpoint_seq = None;
    for index in 0..100 {
        by_bytes.set_attribute(&format!("key-{index}"), "value", "");
        if by_bytes.attempt_record(1).checkpoint_seqs.len() == 2 {
            checkpoint_seq = Some(by_bytes.last_seq());
            break;
        }
    }
    let checkpoint_seq = checkpoint_seq.expect("written bytes eventually reach the state size");
    assert!(checkpoint_seq > 3);
    let record = by_bytes.run_record();
    assert_eq!(record.written_bytes_since_checkpoint, 0);
    assert!(record.last_checkpoint_state_bytes > state_bytes);
}

#[test]
fn beginning_reopens_a_completed_flow_with_the_start_state() {
    let mut harness = Harness::new();
    harness.start(start_request());
    let genesis_rows = harness.attempt_rows();
    harness.start_attempt("ChargeStep-1", StepMethod::WaitFor, 1);
    harness.complete_wait_for("ChargeStep-1");
    harness.start_attempt("ChargeStep-1", StepMethod::Execute, 1);
    harness.complete_execute("ChargeStep-1", 1, force_complete());
    assert_eq!(harness.run_record().status, FlowStatus::Completed as i32);
    assert!(!harness.has_index_entry(IndexKind::ActiveWork));
    let (attempt, outcome) = harness.travel_to(reset(FlowResetType::Beginning));
    assert_eq!(attempt, 2);
    let record = harness.run_record();
    assert_eq!(record.current_attempt, 2);
    assert_eq!(record.status, FlowStatus::Running as i32);
    assert!(record.close.is_none());
    assert_eq!(record.retention_deadline_micros, 0);
    assert_eq!(harness.directory_status(), FlowStatus::Running as i32);
    assert!(harness.has_index_entry(IndexKind::ActiveWork));
    assert_eq!(harness.attempt_rows(), genesis_rows);
    assert_eq!(
        pending_effects(&outcome),
        vec![("ChargeStep-1".to_owned(), StepMethod::WaitFor as i32, 1, 2)]
    );
    let attempt_record = harness.attempt_record(2);
    assert_eq!(
        (attempt_record.parent_attempt, attempt_record.parent_seq),
        (1, 1)
    );
    assert_eq!(
        attempt_record.checkpoint_seqs,
        vec![attempt_record.start_seq]
    );
    let fork = harness.history_record(attempt_record.start_seq).unwrap();
    assert_eq!(fork.attempt, 2);
    assert!(fork.is_semantic);
    let Some(flow_history_event::Payload::TimeTravelFork(event)) = &fork.events[0].payload else {
        panic!("the fork records a time travel event");
    };
    assert_eq!(event.previous_run_attempt, 1);
    assert_eq!(event.target_run_attempt, 1);
    assert_eq!(event.target_event_id, 1);
    // The reopened attempt runs again and a stale result of attempt 1 never applies.
    harness.start_attempt("ChargeStep-1", StepMethod::WaitFor, 1);
    harness.complete_wait_for("ChargeStep-1");
    assert_eq!(
        harness.step("ChargeStep-1").phase,
        dexzd_protocol::dexzd::v1::StepExecutionPhase::Execute as i32
    );
}

#[test]
fn each_reset_type_selects_its_point() {
    let mut harness = Harness::new();
    let (execute_pending_seq, ship_created_seq) = run_charge_then_ship(&mut harness);
    let execute_pending_time = harness
        .history_record(execute_pending_seq)
        .unwrap()
        .timestamp_micros;
    let cases = [
        (reset(FlowResetType::Beginning), 1),
        (
            ResetFlowRequest {
                step_type: "ShipStep".to_owned(),
                ..reset(FlowResetType::StepType)
            },
            ship_created_seq,
        ),
        (
            ResetFlowRequest {
                step_execution_id: "ChargeStep-1".to_owned(),
                step_method: FlowResetStepMethod::WaitFor as i32,
                ..reset(FlowResetType::StepExecutionId)
            },
            1,
        ),
        (
            ResetFlowRequest {
                step_execution_id: "ChargeStep-1".to_owned(),
                step_method: FlowResetStepMethod::Execute as i32,
                ..reset(FlowResetType::StepExecutionId)
            },
            execute_pending_seq,
        ),
        (
            ResetFlowRequest {
                step_execution_id: "ShipStep-2".to_owned(),
                step_method: FlowResetStepMethod::Execute as i32,
                ..reset(FlowResetType::StepExecutionId)
            },
            ship_created_seq,
        ),
        (reset_to_time(execute_pending_time - 1), execute_pending_seq),
        (reset_to_time(execute_pending_time), execute_pending_seq),
    ];
    for (index, (request, expected_seq)) in cases.into_iter().enumerate() {
        // Every case searches attempt 1, which earlier cases abandoned.
        let request = ResetFlowRequest {
            run_attempt: 1,
            ..request
        };
        let (attempt, _) = harness.travel_to(request);
        assert_eq!(attempt, 2 + index as u32);
        let record = harness.attempt_record(attempt);
        assert_eq!(
            (record.parent_attempt, record.parent_seq),
            (1, expected_seq),
            "case {index}"
        );
    }
    let rfc3339 = ResetFlowRequest {
        history_event_time: "2026-01-01T00:00:00Z".to_owned(),
        run_attempt: 1,
        ..reset(FlowResetType::HistoryEventTime)
    };
    let (attempt, _) = harness.travel_to(rfc3339);
    assert_eq!(harness.attempt_record(attempt).parent_seq, 1);
}

#[test]
fn invalid_targets_are_rejected_without_writes() {
    let mut harness = Harness::new();
    run_charge_then_ship(&mut harness);
    let rejections = [
        (
            ResetFlowRequest {
                step_type: "MissingStep".to_owned(),
                ..reset(FlowResetType::StepType)
            },
            FAILED_PRECONDITION,
        ),
        (
            ResetFlowRequest {
                step_execution_id: "ChargeStep-1".to_owned(),
                ..reset(FlowResetType::StepExecutionId)
            },
            INVALID_ARGUMENT,
        ),
        (
            ResetFlowRequest {
                run_attempt: 2,
                ..reset(FlowResetType::Beginning)
            },
            INVALID_ARGUMENT,
        ),
        (
            ResetFlowRequest {
                history_event_time: "yesterday".to_owned(),
                ..reset(FlowResetType::HistoryEventTime)
            },
            INVALID_ARGUMENT,
        ),
        (
            reset_to_time(harness.now_micros + SECOND),
            FAILED_PRECONDITION,
        ),
        (reset(FlowResetType::Unspecified), INVALID_ARGUMENT),
    ];
    for (request, code) in rejections {
        let outcome = harness.time_travel(request.clone());
        assert_eq!(error_code(&outcome.reply), Some(code), "{request:?}");
        assert!(outcome.writes.is_empty());
    }
    assert_eq!(harness.run_record().current_attempt, 1);
}

#[test]
fn parallel_in_flight_steps_are_redispatched_with_their_snapshots_and_attempts() {
    let mut harness = Harness::new();
    run_charge_then_ship(&mut harness);
    harness.set_attribute("status", "shipping", "");
    harness.start_attempt("ShipStep-1", StepMethod::Execute, 1);
    harness.apply(Command::StepAttemptFailed(StepAttemptFailedCommand {
        invocation: Some(key("ShipStep-1", StepMethod::Execute)),
        attempt: 1,
        kind: 1,
        worker_error: Some(WorkerErrorResponse {
            detail: "boom".to_owned(),
            ..WorkerErrorResponse::default()
        }),
        ..StepAttemptFailedCommand::default()
    }));
    harness.apply(Command::TimersFired(TimersFiredCommand {
        timers: vec![TimerId {
            run_attempt: 1,
            kind: Some(timer_id::Kind::RetryBackoff(RetryBackoffTimerId {
                invocation: Some(key("ShipStep-1", StepMethod::Execute)),
                next_attempt: 2,
            })),
        }],
    }));
    harness.start_attempt("ShipStep-1", StepMethod::Execute, 2);
    harness.start_attempt("ShipStep-2", StepMethod::Execute, 1);
    // A semantic point while both attempts run.
    harness.set_attribute("status", "both running", "");
    let point_seq = harness.last_seq();
    let point_time = harness.history_record(point_seq).unwrap().timestamp_micros;
    let rows_at_point = harness.attempt_rows();
    let snapshot_at_point = harness.step("ShipStep-1").invocation.unwrap().snapshot;
    harness.complete_execute("ShipStep-2", 1, StepDecision::default());
    harness.set_attribute("status", "after", "");
    let (attempt, outcome) = harness.travel_to(reset_to_time(point_time));
    assert_eq!(harness.attempt_record(attempt).parent_seq, point_seq);
    assert_eq!(harness.attempt_rows(), rows_at_point);
    let started = started_effects(&outcome);
    assert_eq!(started.len(), 2);
    assert_eq!(
        (started[0].0.as_str(), started[0].1, started[0].2),
        ("ShipStep-1", 2, attempt)
    );
    assert_eq!(
        (started[1].0.as_str(), started[1].1, started[1].2),
        ("ShipStep-2", 1, attempt)
    );
    assert_ne!(
        started[0].3, started[1].3,
        "restored dispatches get distinct IDs"
    );
    assert_eq!(
        canceled_task_ids(&outcome),
        vec![vec![2; 16]],
        "only ShipStep-1 attempt 2 was still running"
    );
    let restored = harness.step("ShipStep-1").invocation.unwrap();
    assert_eq!(restored.phase, StepInvocationPhase::Running as i32);
    assert_eq!(restored.snapshot, snapshot_at_point);
    assert_eq!(restored.task_id, started[0].3);
    // The restored attempt completes under the new run attempt.
    let completion = StepMethodCompletedCommand {
        invocation: Some(key("ShipStep-1", StepMethod::Execute)),
        attempt: 2,
        response: Some(step_method_completed_command::Response::Execute(
            InvokeExecuteMethodResponse::default(),
        )),
        ..StepMethodCompletedCommand::default()
    };
    let completed = harness.apply(Command::StepMethodCompleted(completion));
    assert!(is_ok(&completed.reply));
}

#[test]
fn restored_running_attempts_carry_their_configured_method_timeouts() {
    let mut harness = Harness::new();
    let mut request = start_request();
    request.step_options = Some(StepOptions {
        wait_for_timeout_seconds: 7,
        execute_timeout_seconds: 30,
        ..StepOptions::default()
    });
    harness.start(request);
    harness.start_attempt("ChargeStep-1", StepMethod::WaitFor, 1);
    harness.set_attribute("status", "waiting", "");
    let wait_for_point = harness.history_record(harness.last_seq()).unwrap();
    harness.complete_wait_for("ChargeStep-1");
    harness.start_attempt("ChargeStep-1", StepMethod::Execute, 1);
    harness.set_attribute("status", "executing", "");
    let execute_point = harness.history_record(harness.last_seq()).unwrap();
    harness.complete_execute("ChargeStep-1", 1, force_complete());

    let (_, outcome) = harness.travel_to(reset_to_time(execute_point.timestamp_micros));
    assert_eq!(
        started_timeouts(&outcome),
        vec![("ChargeStep-1".to_owned(), StepMethod::Execute as i32, 30)]
    );
    let (_, outcome) = harness.travel_to(ResetFlowRequest {
        run_attempt: 1,
        ..reset_to_time(wait_for_point.timestamp_micros)
    });
    assert_eq!(
        started_timeouts(&outcome),
        vec![("ChargeStep-1".to_owned(), StepMethod::WaitFor as i32, 7)]
    );
}

#[test]
fn timers_move_to_the_new_attempt_and_stale_timers_never_fire() {
    let mut harness = Harness::new();
    let mut request = start_request();
    request.flow_timeout_seconds = 30;
    harness.start(request);
    harness.set_attribute("status", "a", "");
    let (attempt, outcome) = harness.travel_to(reset(FlowResetType::Beginning));
    assert!(
        outcome
            .effects
            .iter()
            .any(|effect| matches!(effect, ApplyEffect::TimerArmed { .. }))
    );
    let stale = harness.apply(Command::TimersFired(TimersFiredCommand {
        timers: vec![TimerId {
            run_attempt: 1,
            kind: Some(timer_id::Kind::FlowTimeout(
                dexzd_protocol::dexzd::v1::FlowTimeoutTimerId {},
            )),
        }],
    }));
    assert!(stale.writes.is_empty());
    harness.apply(Command::TimersFired(TimersFiredCommand {
        timers: vec![TimerId {
            run_attempt: attempt,
            kind: Some(timer_id::Kind::FlowTimeout(
                dexzd_protocol::dexzd::v1::FlowTimeoutTimerId {},
            )),
        }],
    }));
    assert_eq!(harness.run_record().status, FlowStatus::Failed as i32);
}

#[test]
fn reapply_restores_later_external_input_unless_skipped() {
    let mut skipped = Harness::new();
    let mut reapplied = Harness::new();
    for harness in [&mut skipped, &mut reapplied] {
        harness.start(start_request());
        harness.set_attribute("a", "1", "set-a");
        harness.publish("orders", "m1");
        harness.publish("orders", "m2");
        harness.apply(Command::ChannelMessageDelete(ChannelMessageDeleteCommand {
            request: Some(DeleteChannelMessageRequest {
                channel_name: "orders".to_owned(),
                message_id: "m1".to_owned(),
                request_id: "delete-m1".to_owned(),
                ..DeleteChannelMessageRequest::default()
            }),
            request_fingerprint: vec![1],
        }));
        harness.set_attribute("b", "2", "set-b");
    }
    skipped.travel_to(reset(FlowResetType::Beginning));
    assert!(skipped.attribute_keys().is_empty());
    assert_eq!(skipped.channel_message_count(), 0);
    let before_travel = reapplied.last_seq();
    let (attempt, outcome) = reapplied.travel_to(ResetFlowRequest {
        skip_writes_reapply: false,
        ..reset(FlowResetType::Beginning)
    });
    assert_eq!(
        reapplied.attribute_keys(),
        vec!["a".to_owned(), "b".to_owned()]
    );
    assert_eq!(reapplied.channel_message_count(), 1);
    // Reapply is part of the TimeTravel command, and its start checkpoint holds the result.
    let fork_seq = before_travel + 1;
    assert_eq!(reapplied.last_seq(), fork_seq);
    let fork = reapplied.history_record(fork_seq).unwrap();
    assert!(fork.attempt == attempt && fork.is_semantic && fork.is_external_input);
    assert_eq!(fork.reapplied_inputs.len(), 5);
    assert!(matches!(
        fork.events.first().and_then(|event| event.payload.as_ref()),
        Some(flow_history_event::Payload::TimeTravelFork(_))
    ));
    assert_eq!(
        reapplied.attempt_record(attempt).checkpoint_seqs,
        vec![fork_seq]
    );
    let start_checkpoint = reapplied.checkpoint(attempt, fork_seq).unwrap();
    assert!(start_checkpoint.rows.iter().any(|row| {
        row.table == u32::from(RunTable::Attribute as u8) && row.key_suffix == b"b"
    }));
    assert!(
        outcome
            .effects
            .iter()
            .any(|effect| matches!(effect, ApplyEffect::WakeWaiters { .. }))
    );
    // Reapplied input belongs to the new lineage, so a second travel reapplies it again.
    reapplied.travel_to(ResetFlowRequest {
        skip_writes_reapply: false,
        ..reset(FlowResetType::Beginning)
    });
    assert_eq!(
        reapplied.attribute_keys(),
        vec!["a".to_owned(), "b".to_owned()]
    );
    assert_eq!(reapplied.channel_message_count(), 1);
    // The run-scoped deduplication table still answers the original request IDs.
    let repeated = reapplied.apply(Command::AttributesSet(AttributesSetCommand {
        request: Some(SetAttributesRequest {
            request_id: "set-a".to_owned(),
            ..SetAttributesRequest::default()
        }),
        hard_delete_objects: Vec::new(),
        request_fingerprint: b"set-a".to_vec(),
    }));
    assert!(is_ok(&repeated.reply));
    assert!(repeated.writes.is_empty());
}

#[test]
fn reapply_skips_stop_and_input_of_abandoned_branches() {
    let mut harness = Harness::new();
    harness.start(start_request());
    harness.set_attribute("a", "1", "");
    let point_time = harness.history_record(2).unwrap().timestamp_micros;
    harness.set_attribute("abandoned", "x", "");
    harness.apply(Command::FlowStop(
        dexzd_protocol::dexzd::v1::FlowStopCommand {
            request: Some(dexzd_protocol::dex::StopFlowRequest::default()),
        },
    ));
    assert_eq!(harness.run_record().status, FlowStatus::Canceled as i32);
    // Attempt 2 forks after "a" without reapply; its own input is "b".
    harness.travel_to(reset_to_time(point_time));
    assert_eq!(harness.run_record().status, FlowStatus::Running as i32);
    harness.set_attribute("b", "2", "");
    // Back to the beginning of attempt 2's lineage: "a" and "b", never "abandoned" or Stop.
    harness.travel_to(ResetFlowRequest {
        skip_writes_reapply: false,
        ..reset(FlowResetType::Beginning)
    });
    assert_eq!(
        harness.attribute_keys(),
        vec!["a".to_owned(), "b".to_owned()]
    );
    assert_eq!(harness.run_record().status, FlowStatus::Running as i32);
}

#[test]
fn a_point_in_an_abandoned_attempt_can_be_restored() {
    let mut harness = Harness::new();
    harness.start(start_request());
    harness.set_attribute("a", "1", "");
    harness.set_attribute("b", "2", "");
    let point_time = harness.history_record(3).unwrap().timestamp_micros;
    let rows_at_point = harness.attempt_rows();
    harness.travel_to(reset(FlowResetType::Beginning));
    harness.set_attribute("c", "3", "");
    let (attempt, _) = harness.travel_to(ResetFlowRequest {
        run_attempt: 1,
        ..reset_to_time(point_time)
    });
    assert_eq!(attempt, 3);
    assert_eq!(harness.attempt_record(3).parent_attempt, 1);
    assert_eq!(harness.attempt_rows(), rows_at_point);
    // Attempt 2's input is not part of attempt 1's lineage.
    let (_, _) = harness.travel_to(ResetFlowRequest {
        run_attempt: 2,
        ..reset(FlowResetType::Beginning)
    });
    assert!(harness.attribute_keys().is_empty());
}

#[test]
fn rebuilt_states_equal_the_recorded_states_at_every_point() {
    let mut harness = Harness::with_settings(RunSettings {
        checkpoint_max_commands: 3,
        ..settings()
    });
    let mut recorded: Vec<(u64, i64, AttemptRows, i32, [u8; 16])> = Vec::new();
    let mut record_point = |harness: &Harness| {
        let seq = harness.last_seq();
        let history = harness.history_record(seq).unwrap();
        if history.is_semantic {
            recorded.push((
                seq,
                history.timestamp_micros,
                harness.attempt_rows(),
                harness.run_record().status,
                current_semantic_point_hash(
                    &harness.context,
                    &harness.state,
                    harness.run(),
                    &history
                        .command
                        .as_ref()
                        .expect("history command")
                        .command_id,
                )
                .expect("hash current semantic point")
                .expect("semantic history point")
                .state_hash,
            ));
        }
    };
    harness.start(start_request());
    record_point(&harness);
    harness.start_attempt("ChargeStep-1", StepMethod::WaitFor, 1);
    harness.complete_wait_for("ChargeStep-1");
    record_point(&harness);
    harness.publish("orders", "m1");
    record_point(&harness);
    harness.start_attempt("ChargeStep-1", StepMethod::Execute, 1);
    harness.set_attribute("large", &"z".repeat(300), "");
    record_point(&harness);
    harness.complete_execute("ChargeStep-1", 1, next_steps(&["ShipStep", "PackStep"]));
    record_point(&harness);
    harness.start_attempt("ShipStep-1", StepMethod::Execute, 1);
    harness.publish("orders", "m2");
    record_point(&harness);
    harness.complete_execute("ShipStep-1", 1, StepDecision::default());
    record_point(&harness);
    harness.start_attempt("PackStep-1", StepMethod::Execute, 1);
    harness.complete_execute("PackStep-1", 1, force_complete());
    record_point(&harness);
    assert!(harness.attempt_record(1).checkpoint_seqs.len() > 2);
    for (seq, timestamp, rows, status, expected_hash) in recorded {
        let diagnostic_command = harness.command(Command::TimeTravel(TimeTravelCommand {
            request: Some(ResetFlowRequest {
                run_attempt: 1,
                ..reset_to_time(timestamp)
            }),
        }));
        let reconstructed_hash =
            time_travel_target_hash(&harness.context, &harness.state, &diagnostic_command)
                .expect("hash reconstructed semantic point")
                .expect("target exists");
        assert_eq!(
            reconstructed_hash.state_hash, expected_hash,
            "state hash at seq {seq}"
        );
        let (attempt, _) = harness.travel_to(ResetFlowRequest {
            run_attempt: 1,
            ..reset_to_time(timestamp)
        });
        assert_eq!(harness.attempt_record(attempt).parent_seq, seq);
        assert_eq!(harness.attempt_rows(), rows, "state at seq {seq}");
        assert_eq!(harness.run_record().status, status, "status at seq {seq}");
    }
}

#[test]
fn diagnostic_hash_detects_a_broken_time_travel_rebuild() {
    let mut harness = Harness::new();
    harness.start(start_request());
    let history = harness
        .history_record(harness.last_seq())
        .expect("start history");
    let expected = current_semantic_point_hash(
        &harness.context,
        &harness.state,
        harness.run(),
        &history
            .command
            .as_ref()
            .expect("history command")
            .command_id,
    )
    .expect("hash current state")
    .expect("start is semantic");
    let command = harness.command(Command::TimeTravel(TimeTravelCommand {
        request: Some(reset(FlowResetType::Beginning)),
    }));
    let original = time_travel_target_hash(&harness.context, &harness.state, &command)
        .expect("rebuild target")
        .expect("beginning exists");
    assert_eq!(original, expected);

    let checkpoint_key = checkpoint_key(
        harness.context.flow_hash,
        harness.run(),
        1,
        expected.run_seq,
    );
    let checkpoint_bytes = harness
        .state
        .rows
        .get(&(ColumnFamily::History, checkpoint_key.clone()))
        .expect("checkpoint exists");
    let mut checkpoint =
        CheckpointRecord::decode(checkpoint_bytes.as_slice()).expect("checkpoint decodes");
    checkpoint
        .run_status
        .as_mut()
        .expect("checkpoint has status")
        .closed_at_micros += 1;
    harness.state.rows.insert(
        (ColumnFamily::History, checkpoint_key),
        checkpoint.encode_to_vec(),
    );
    let broken = time_travel_target_hash(&harness.context, &harness.state, &command)
        .expect("rebuild corrupted target")
        .expect("beginning exists");
    assert_eq!(
        (broken.run_id, broken.run_attempt, broken.run_seq),
        (expected.run_id, expected.run_attempt, expected.run_seq)
    );
    assert_ne!(
        broken.state_hash, expected.state_hash,
        "a corrupted rebuild must fail the hash check"
    );
}

#[test]
fn the_window_prunes_history_and_rejects_points_outside_it() {
    let mut harness = Harness::with_settings(RunSettings {
        history_window_semantic_events: 4,
        history_prune_chunk_semantic_events: 2,
        checkpoint_max_commands: 3,
        ..settings()
    });
    harness.start(start_request());
    harness.start_attempt("ChargeStep-1", StepMethod::WaitFor, 1);
    harness.complete_wait_for("ChargeStep-1");
    for index in 0..5 {
        harness.set_attribute("status", &format!("v{index}"), "");
    }
    // Seven semantic points (seqs 1, 3, 4..8) exceed N + M = 6: W skips seqs 1 and 3.
    let record = harness.run_record();
    assert_eq!(record.window_start_seq, 4);
    assert_eq!(record.pruned_semantic_event_count, 2);
    assert_eq!(record.max_pruned_external_input_seq, 0);
    assert!(harness.history_record(1).is_some(), "FlowStart is kept");
    assert!(
        harness.checkpoint(1, 1).is_some(),
        "the genesis checkpoint is kept"
    );
    assert!(
        harness.history_record(4).is_some(),
        "the rebuild checkpoint's records stay"
    );
    let execute = ResetFlowRequest {
        step_execution_id: "ChargeStep-1".to_owned(),
        step_method: FlowResetStepMethod::Execute as i32,
        ..reset(FlowResetType::StepExecutionId)
    };
    let outside = harness.time_travel(execute.clone());
    assert_eq!(error_code(&outside.reply), Some(FAILED_PRECONDITION));
    for index in 5..9 {
        harness.set_attribute("status", &format!("v{index}"), "");
    }
    let record = harness.run_record();
    assert_eq!(record.window_start_seq, 8);
    assert_eq!(record.max_pruned_external_input_seq, 7);
    assert_eq!(harness.attempt_record(1).checkpoint_seqs, vec![1, 7, 10]);
    assert!(harness.history_record(2).is_none());
    assert!(harness.history_record(6).is_none());
    assert!(harness.history_record(7).is_some());
    // Every point in the window still rebuilds.
    let in_window = harness.history_record(8).unwrap().timestamp_micros;
    let (attempt, _) = harness.travel_to(reset_to_time(in_window));
    assert_eq!(harness.attempt_record(attempt).parent_seq, 8);
    let rows = harness.attempt_rows();
    let status = rows
        .iter()
        .find(|((table, suffix), _)| *table == RunTable::Attribute as u8 && suffix == b"status")
        .map(|(_, value)| AttributeRecord::decode(value.as_slice()).unwrap())
        .unwrap();
    assert_eq!(
        status.value.unwrap().kind,
        Some(dexzd_protocol::dexzd::v1::stored_value::Kind::InlineValue(
            string_value("v4")
        ))
    );
    // Reapplying from the beginning would miss pruned input.
    let beginning = harness.time_travel(ResetFlowRequest {
        skip_writes_reapply: false,
        run_attempt: 1,
        ..reset(FlowResetType::Beginning)
    });
    assert_eq!(error_code(&beginning.reply), Some(FAILED_PRECONDITION));
    assert!(error_message(&beginning.reply).contains("skip_writes_reapply"));
    let (_, _) = harness.travel_to(ResetFlowRequest {
        run_attempt: 1,
        ..reset(FlowResetType::Beginning)
    });
    assert!(harness.attribute_keys().is_empty());
}

#[test]
fn a_result_applied_as_failure_is_recorded_and_replayed_as_that_failure() {
    let mut harness = Harness::new();
    let mut request = start_request();
    request.step_options = skip_wait_for();
    harness.start(request);
    harness.start_attempt("ChargeStep-1", StepMethod::Execute, 1);
    let unknown_reference = Value {
        kind: Some(value::Kind::InternalBlobIdForStringValue(
            "zd1|not-a-reference".to_owned(),
        )),
    };
    harness.apply(Command::StepMethodCompleted(StepMethodCompletedCommand {
        invocation: Some(key("ChargeStep-1", StepMethod::Execute)),
        attempt: 1,
        response: Some(step_method_completed_command::Response::Execute(
            InvokeExecuteMethodResponse {
                upsert_attributes: vec![AttributeWrite {
                    key: "report".to_owned(),
                    value: Some(unknown_reference),
                    ..AttributeWrite::default()
                }],
                ..InvokeExecuteMethodResponse::default()
            },
        )),
        ..StepMethodCompletedCommand::default()
    }));
    let failure_seq = harness.last_seq();
    let recorded = harness
        .history_record(failure_seq)
        .unwrap()
        .command
        .unwrap();
    assert!(matches!(
        recorded.command,
        Some(Command::StepAttemptFailed(_))
    ));
    // The non-retryable failure failed the Flow; rebuilding that point replays the failure.
    assert_eq!(harness.run_record().status, FlowStatus::Failed as i32);
    let point_time = harness
        .history_record(failure_seq)
        .unwrap()
        .timestamp_micros;
    let rows_at_point = harness.attempt_rows();
    harness.travel_to(reset(FlowResetType::Beginning));
    harness.travel_to(ResetFlowRequest {
        run_attempt: 1,
        ..reset_to_time(point_time)
    });
    assert_eq!(harness.run_record().status, FlowStatus::Failed as i32);
    assert_eq!(harness.attempt_rows(), rows_at_point);
}

#[test]
fn results_stamped_with_an_abandoned_run_attempt_are_rejected() {
    let mut harness = Harness::new();
    harness.start(start_request());
    harness.start_attempt("ChargeStep-1", StepMethod::WaitFor, 1);
    harness.set_attribute("status", "running", "");
    let point_time = harness
        .history_record(harness.last_seq())
        .unwrap()
        .timestamp_micros;
    harness.travel_to(reset_to_time(point_time));
    // The owner stamps every Worker result with the run attempt it dispatched.
    let mut stale = harness.command(Command::StepMethodCompleted(StepMethodCompletedCommand {
        invocation: Some(key("ChargeStep-1", StepMethod::WaitFor)),
        attempt: 1,
        response: Some(step_method_completed_command::Response::WaitFor(
            InvokeWaitForMethodResponse::default(),
        )),
        ..StepMethodCompletedCommand::default()
    }));
    stale.expected_run_attempt = 1;
    let outcome = apply_flow_command(&harness.context, &harness.state, &stale).unwrap();
    assert_eq!(error_code(&outcome.reply), Some(FAILED_PRECONDITION));
    assert!(outcome.writes.is_empty());
    stale.expected_run_attempt = 2;
    let outcome = apply_flow_command(&harness.context, &harness.state, &stale).unwrap();
    assert!(
        is_ok(&outcome.reply),
        "the restored attempt accepts its own result"
    );
}

#[test]
fn only_the_latest_run_of_a_flow_id_can_time_travel() {
    let mut harness = Harness::new();
    harness.start(start_request());
    harness.set_attribute("status", "first run", "");
    let first_run_id = harness.run_id.clone();
    let mut terminate = start_request();
    terminate.request_id = "start-2".to_owned();
    terminate.flow_start_options = Some(dexzd_protocol::dex::FlowStartOptions {
        id_reuse_policy: dexzd_protocol::dex::IdReusePolicy::AllowTerminateIfRunning as i32,
        ..dexzd_protocol::dex::FlowStartOptions::default()
    });
    let outcome = harness.apply(Command::FlowStart(FlowStartCommand {
        new_run_id: vec![8; 16],
        request: Some(terminate),
        settings: Some(settings()),
        resolved_timeout_policy: FlowTimeoutPolicy::Fail as i32,
        worker_target: Some(WorkerTarget::default()),
        hard_delete_objects: Vec::new(),
        request_fingerprint: Vec::new(),
    }));
    assert!(matches!(
        outcome.reply.reply,
        Some(command_reply::Reply::StartFlow(_))
    ));
    harness.run_id = first_run_id;
    assert_eq!(harness.run_record().status, FlowStatus::Terminated as i32);
    let rejected = harness.time_travel(reset(FlowResetType::Beginning));
    assert_eq!(error_code(&rejected.reply), Some(FAILED_PRECONDITION));
    assert!(rejected.writes.is_empty());
    harness.run_id = vec![8; 16];
    harness.travel_to(reset(FlowResetType::Beginning));
}

/// Completes ChargeStep-1's WaitFor with a wait on timer `t0`.
fn wait_on_timer(harness: &mut Harness) {
    harness.start_attempt("ChargeStep-1", StepMethod::WaitFor, 1);
    let outcome = harness.apply(Command::StepMethodCompleted(StepMethodCompletedCommand {
        invocation: Some(key("ChargeStep-1", StepMethod::WaitFor)),
        attempt: 1,
        response: Some(step_method_completed_command::Response::WaitFor(
            InvokeWaitForMethodResponse {
                waiting_condition: Some(WaitingCondition {
                    waiting_condition_type: WaitingConditionType::AnyCompleted as i32,
                    timer_conditions: vec![TimerCondition {
                        condition_id: "t0".to_owned(),
                        duration_seconds: 60,
                        firing_unix_timestamp_seconds: 0,
                    }],
                    ..WaitingCondition::default()
                }),
                ..InvokeWaitForMethodResponse::default()
            },
        )),
        app_worker_id: "worker-a".to_owned(),
        ..StepMethodCompletedCommand::default()
    }));
    assert!(
        is_ok(&outcome.reply),
        "WaitFor completes: {:?}",
        outcome.reply
    );
}

fn skip_timer(harness: &mut Harness) -> ApplyOutcome {
    harness.apply(Command::TimerSkip(TimerSkipCommand {
        request: Some(SkipTimerRequest {
            flow_id: "order-1".to_owned(),
            step_execution_id: "ChargeStep-1".to_owned(),
            timer_condition_id: "t0".to_owned(),
            ..SkipTimerRequest::default()
        }),
    }))
}

fn reapplying(request: ResetFlowRequest) -> ResetFlowRequest {
    ResetFlowRequest {
        skip_writes_reapply: false,
        ..request
    }
}

fn attribute(harness: &Harness, attribute_key: &str) -> AttributeRecord {
    harness
        .attempt_rows()
        .iter()
        .find(|((table, suffix), _)| {
            *table == RunTable::Attribute as u8 && suffix == attribute_key.as_bytes()
        })
        .map(|(_, value)| AttributeRecord::decode(value.as_slice()).expect("Attribute rows decode"))
        .expect("the Attribute exists")
}

fn value_row(harness: &Harness, run_seq: u64) -> Option<ValueRecord> {
    let value_id = ValueId {
        run_seq,
        index_in_command: 0,
    };
    let key = value_row_key(
        harness.context.flow_hash,
        harness.run(),
        RunTable::Value,
        value_id,
    );
    harness.get(ColumnFamily::Value, &key)
}

#[test]
fn the_fork_point_includes_reapplied_input() {
    let mut harness = Harness::new();
    harness.start(start_request());
    harness.set_attribute("a", "1", "set-a");
    let (first, _) = harness.travel_to(reapplying(reset(FlowResetType::Beginning)));
    assert_eq!(harness.attribute_keys(), vec!["a".to_owned()]);
    let after_first = harness.attempt_rows();
    let fork_seq = harness.last_seq();
    let fork_time = harness.history_record(fork_seq).unwrap().timestamp_micros;
    let (second, _) = harness.travel_to(ResetFlowRequest {
        run_attempt: first as i32,
        ..reset_to_time(fork_time)
    });
    assert_eq!(harness.attempt_record(second).parent_seq, fork_seq);
    assert_eq!(harness.attempt_rows(), after_first);
}

#[test]
fn only_beginning_targets_genesis_outside_the_window() {
    let mut harness = Harness::with_settings(RunSettings {
        history_window_semantic_events: 4,
        history_prune_chunk_semantic_events: 2,
        checkpoint_max_commands: 3,
        ..settings()
    });
    harness.start(start_request());
    let genesis_time = harness.history_record(1).unwrap().timestamp_micros;
    harness.start_attempt("ChargeStep-1", StepMethod::WaitFor, 1);
    harness.complete_wait_for("ChargeStep-1");
    let last_pruned_time = harness.history_record(3).unwrap().timestamp_micros;
    for index in 0..5 {
        harness.set_attribute("status", &format!("v{index}"), "");
    }
    assert_eq!(harness.run_record().window_start_seq, 4);
    for request in [
        reset_to_time(genesis_time),
        reset_to_time(last_pruned_time),
        ResetFlowRequest {
            step_type: "ChargeStep".to_owned(),
            ..reset(FlowResetType::StepType)
        },
        ResetFlowRequest {
            step_execution_id: "ChargeStep-1".to_owned(),
            step_method: FlowResetStepMethod::WaitFor as i32,
            ..reset(FlowResetType::StepExecutionId)
        },
        ResetFlowRequest {
            step_execution_id: "ChargeStep-1".to_owned(),
            step_method: FlowResetStepMethod::Execute as i32,
            ..reset(FlowResetType::StepExecutionId)
        },
    ] {
        let outcome = harness.time_travel(request.clone());
        assert_eq!(
            error_code(&outcome.reply),
            Some(FAILED_PRECONDITION),
            "{request:?}"
        );
        assert!(outcome.writes.is_empty());
    }
    // A time after the last pruned point targets W, the first point at or after it.
    let (attempt, _) = harness.travel_to(reset_to_time(last_pruned_time + 1));
    assert_eq!(harness.attempt_record(attempt).parent_seq, 4);
    let (attempt, _) = harness.travel_to(ResetFlowRequest {
        run_attempt: 1,
        ..reset(FlowResetType::Beginning)
    });
    assert_eq!(harness.attempt_record(attempt).parent_seq, 1);
}

#[test]
fn reapplied_skip_timer_completes_the_restored_wait_or_is_skipped() {
    let mut harness = Harness::new();
    harness.start(start_request());
    wait_on_timer(&mut harness);
    let waiting_time = harness
        .history_record(harness.last_seq())
        .unwrap()
        .timestamp_micros;
    assert!(is_ok(&skip_timer(&mut harness).reply));
    let (attempt, outcome) = harness.travel_to(reapplying(ResetFlowRequest {
        run_attempt: 1,
        ..reset_to_time(waiting_time)
    }));
    assert_eq!(
        harness.step("ChargeStep-1").phase,
        StepExecutionPhase::Execute as i32
    );
    assert!(
        !harness
            .attempt_rows()
            .keys()
            .any(|(table, _)| *table == RunTable::Timer as u8),
        "the reapplied skip disarmed the timer"
    );
    assert_eq!(
        pending_effects(&outcome),
        vec![(
            "ChargeStep-1".to_owned(),
            StepMethod::Execute as i32,
            1,
            attempt
        )]
    );
    // The reapplied skip made Execute pending, so the fork point is its target.
    let fork_seq = harness.last_seq();
    harness.travel_to(ResetFlowRequest {
        run_attempt: attempt as i32,
        step_execution_id: "ChargeStep-1".to_owned(),
        step_method: FlowResetStepMethod::Execute as i32,
        ..reset(FlowResetType::StepExecutionId)
    });
    let current_attempt = harness.run_record().current_attempt;
    assert_eq!(harness.attempt_record(current_attempt).parent_seq, fork_seq);
    // At the beginning no timer is pending, so the recorded skip is skipped.
    let (beginning, outcome) = harness.travel_to(reapplying(reset(FlowResetType::Beginning)));
    let fork = harness.history_record(harness.last_seq()).unwrap();
    assert_eq!(fork.reapplied_inputs.len(), 1);
    assert_eq!(
        pending_effects(&outcome),
        vec![(
            "ChargeStep-1".to_owned(),
            StepMethod::WaitFor as i32,
            1,
            beginning
        )]
    );
}

#[test]
fn an_unexpected_reapply_rejection_is_an_invariant_violation() {
    let mut harness = Harness::new();
    harness.start(start_request());
    harness.set_attribute("a", "1", "set-a");
    let seq = harness.last_seq();
    let mut record = harness.history_record(seq).unwrap();
    record.command.as_mut().unwrap().command =
        Some(Command::RpcCommitted(RpcCommittedCommand::default()));
    let key = history_record_key(harness.context.flow_hash, harness.run(), seq);
    harness
        .state
        .rows
        .insert((ColumnFamily::History, key), record.encode_to_vec());
    let command = harness.command(Command::TimeTravel(TimeTravelCommand {
        request: Some(reapplying(reset(FlowResetType::Beginning))),
    }));
    let error = apply_flow_command(&harness.context, &harness.state, &command)
        .expect_err("an invalid reapplied input fails apply");
    assert!(
        matches!(error, ApplyError::InvariantViolation { .. }),
        "{error:?}"
    );
}

#[test]
fn a_restored_large_value_survives_window_pruning() {
    let mut harness = Harness::with_settings(RunSettings {
        history_window_semantic_events: 4,
        history_prune_chunk_semantic_events: 2,
        checkpoint_max_commands: 3,
        ..settings()
    });
    harness.start(start_request());
    harness.set_attribute("report", &"a".repeat(300), "");
    let restored_seq = harness.last_seq();
    let restored_time = harness
        .history_record(restored_seq)
        .unwrap()
        .timestamp_micros;
    harness.set_attribute("report", &"b".repeat(300), "");
    let overwritten_seq = harness.last_seq();
    harness.travel_to(reset_to_time(restored_time));
    let fork_seq = harness.last_seq();
    for index in 0..8 {
        harness.set_attribute("status", &format!("v{index}"), "");
    }
    assert!(harness.run_record().window_start_seq > fork_seq);
    let report = attribute(&harness, "report");
    let Some(stored_value::Kind::TableValue(reference)) = report.value.and_then(|value| value.kind)
    else {
        panic!("the restored report is a table value");
    };
    assert_eq!(reference.value_id.unwrap().run_seq, restored_seq);
    let restored = value_row(&harness, restored_seq).expect("the restored value stays live");
    assert_eq!(restored.value, Some(string_value(&"a".repeat(300))));
    assert!(
        value_row(&harness, overwritten_seq).is_none(),
        "the abandoned attempt's value was released and pruned"
    );
}

fn set_hard_delete_attribute(harness: &mut Harness, attribute_key: &str, object_id: &str) {
    let outcome = harness.apply(Command::AttributesSet(AttributesSetCommand {
        request: Some(SetAttributesRequest {
            attributes: vec![AttributeWrite {
                key: attribute_key.to_owned(),
                is_hard_delete_on_null: true,
                ..AttributeWrite::default()
            }],
            ..SetAttributesRequest::default()
        }),
        hard_delete_objects: vec![HardDeleteObjectReference {
            write_index: 0,
            object_id: object_id.to_owned(),
            size_bytes: 500,
            is_string_value: true,
        }],
        request_fingerprint: Vec::new(),
    }));
    assert!(
        is_ok(&outcome.reply),
        "hard-delete write: {:?}",
        outcome.reply
    );
}

fn hard_delete_object_id(record: &AttributeRecord) -> Option<String> {
    match record.value.as_ref().and_then(|value| value.kind.as_ref()) {
        Some(stored_value::Kind::HardDeleteObject(object)) => Some(object.object_id.clone()),
        _ => None,
    }
}

#[test]
fn time_travel_keeps_hard_delete_attributes_current() {
    let mut harness = Harness::new();
    harness.start(start_request());
    set_hard_delete_attribute(&mut harness, "secret", "object-1");
    harness.set_attribute("plain", "1", "set-plain");
    set_hard_delete_attribute(&mut harness, "secret", "object-2");
    let flow_hash = harness.context.flow_hash;
    let run = harness.run();
    let pending = |harness: &Harness, object_id: &str| {
        harness
            .get::<HardDeleteObjectRecord>(
                ColumnFamily::Flow,
                &hard_delete_pending_key(flow_hash, run, object_id),
            )
            .is_some()
    };
    assert!(pending(&harness, "object-1"));
    harness.travel_to(reapplying(reset(FlowResetType::Beginning)));
    assert_eq!(
        hard_delete_object_id(&attribute(&harness, "secret")),
        Some("object-2".to_owned())
    );
    let current: HardDeleteObjectRecord = harness
        .get(
            ColumnFamily::Flow,
            &hard_delete_current_key(flow_hash, run, "secret"),
        )
        .expect("the current object row stays");
    assert_eq!(
        current.object.map(|object| object.object_id),
        Some("object-2".to_owned())
    );
    assert!(pending(&harness, "object-1"));
    assert!(!pending(&harness, "object-2"));
    assert!(
        attribute(&harness, "plain").value.is_some(),
        "plain writes still reapply"
    );
    let referenced: Vec<String> = harness
        .attempt_rows()
        .iter()
        .filter(|((table, _), _)| *table == RunTable::Attribute as u8)
        .filter_map(|(_, value)| {
            hard_delete_object_id(&AttributeRecord::decode(value.as_slice()).unwrap())
        })
        .collect();
    assert_eq!(referenced, vec!["object-2".to_owned()]);
}

fn publish_large(harness: &mut Harness, channel_name: &str, message_id: &str) {
    let outcome = harness.apply(Command::ChannelPublish(ChannelPublishCommand {
        request: Some(PublishToChannelRequest {
            messages: vec![ChannelMessage {
                channel_name: channel_name.to_owned(),
                value: Some(string_value(&message_id.repeat(200))),
                message_id: message_id.to_owned(),
            }],
            ..PublishToChannelRequest::default()
        }),
    }));
    assert!(is_ok(&outcome.reply), "PublishToChannel applies");
}

fn pending_count(harness: &Harness, channel_name: &str) -> u64 {
    harness
        .attempt_rows()
        .iter()
        .find(|((table, suffix), _)| {
            *table == RunTable::ChannelMeta as u8 && suffix == channel_name.as_bytes()
        })
        .map(|(_, value)| {
            ChannelMetaRecord::decode(value.as_slice())
                .expect("Channel meta decodes")
                .pending_count
        })
        .unwrap_or(0)
}

fn value_holder_count(harness: &Harness) -> usize {
    let run = harness.run();
    harness
        .state
        .rows
        .keys()
        .filter(|(column_family, key)| {
            *column_family == ColumnFamily::Flow
                && parse_run_key(key).is_ok_and(|parsed| {
                    parsed.run_id == run && parsed.table == RunTable::ValueHolder
                })
        })
        .count()
}

#[test]
fn reapplied_publications_to_one_channel_stay_distinct() {
    let mut harness = Harness::new();
    harness.start(start_request());
    let genesis_holders = value_holder_count(&harness);
    publish_large(&mut harness, "orders", "m1");
    publish_large(&mut harness, "orders", "m2");
    let (attempt, _) = harness.travel_to(reapplying(reset(FlowResetType::Beginning)));
    assert_eq!(harness.channel_message_count(), 2);
    assert_eq!(pending_count(&harness, "orders"), 2);
    assert_eq!(value_holder_count(&harness), genesis_holders + 2);
    let after_reapply = harness.attempt_rows();
    let fork_time = harness
        .history_record(harness.last_seq())
        .unwrap()
        .timestamp_micros;
    harness.travel_to(ResetFlowRequest {
        run_attempt: attempt as i32,
        ..reset_to_time(fork_time)
    });
    assert_eq!(harness.attempt_rows(), after_reapply);
    assert_eq!(value_holder_count(&harness), genesis_holders + 2);
    let deleted = harness.apply(Command::ChannelMessageDelete(ChannelMessageDeleteCommand {
        request: Some(DeleteChannelMessageRequest {
            channel_name: "orders".to_owned(),
            message_id: "m1".to_owned(),
            request_id: "delete-m1".to_owned(),
            ..DeleteChannelMessageRequest::default()
        }),
        request_fingerprint: vec![1],
    }));
    assert!(is_ok(&deleted.reply), "{:?}", deleted.reply);
    assert_eq!(harness.channel_message_count(), 1);
    assert_eq!(pending_count(&harness, "orders"), 1);
    assert_eq!(value_holder_count(&harness), genesis_holders + 1);
}

#[test]
fn a_shrinking_command_checkpoints_against_the_size_after_it() {
    let mut harness = Harness::with_settings(RunSettings {
        wire_inline_threshold_bytes: 1 << 20,
        checkpoint_min_written_bytes: 200,
        ..settings()
    });
    harness.start(start_request());
    harness.set_attribute("big", &"x".repeat(20_000), "");
    let big_seq = harness.last_seq();
    assert_eq!(harness.run_record().last_checkpoint_seq, big_seq);
    // Overwrites add written bytes without growing the state.
    for index in 0..5 {
        harness.set_attribute("small", &format!("{index}").repeat(1000), "");
    }
    assert_eq!(harness.run_record().last_checkpoint_seq, big_seq);
    let shrinking = harness.apply(Command::AttributesSet(AttributesSetCommand {
        request: Some(SetAttributesRequest {
            attributes: vec![
                AttributeWrite {
                    key: "big".to_owned(),
                    value: None,
                    ..AttributeWrite::default()
                },
                AttributeWrite {
                    key: "small".to_owned(),
                    value: Some(string_value(&"z".repeat(1000))),
                    ..AttributeWrite::default()
                },
            ],
            ..SetAttributesRequest::default()
        }),
        hard_delete_objects: Vec::new(),
        request_fingerprint: Vec::new(),
    }));
    assert!(is_ok(&shrinking.reply));
    // Against the 20 KB state before the command, 6 KB of writes would not checkpoint.
    assert_eq!(harness.run_record().last_checkpoint_seq, harness.last_seq());
}

fn commit_rpc(
    harness: &mut Harness,
    request_id: &str,
    response: InvokeWorkerRpcResponse,
    is_transactional: bool,
) -> CommandReply {
    let outcome = harness.apply(Command::RpcCommitted(RpcCommittedCommand {
        request: Some(InvokeRpcRequest {
            flow_id: "order-1".to_owned(),
            rpc_name: "updateOrder".to_owned(),
            request_id: request_id.to_owned(),
            ..InvokeRpcRequest::default()
        }),
        response: Some(response),
        app_worker_id: "worker-rpc".to_owned(),
        request_fingerprint: request_id.as_bytes().to_vec(),
        is_transactional,
        ..RpcCommittedCommand::default()
    }));
    assert!(
        matches!(outcome.reply.reply, Some(command_reply::Reply::Rpc(_))),
        "RPC commit: {:?}",
        outcome.reply
    );
    outcome.reply
}

fn request_dedup_rows(harness: &Harness) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let run = harness.run();
    harness
        .state
        .rows
        .iter()
        .filter(|((column_family, key), _)| {
            *column_family == ColumnFamily::Flow
                && parse_run_key(key).is_ok_and(|parsed| {
                    parsed.run_id == run
                        && matches!(
                            parsed.table,
                            RunTable::RequestDedup | RunTable::RequestDedupOrder
                        )
                })
        })
        .map(|((_, key), value)| (key.clone(), value.clone()))
        .collect()
}

fn rpc_attribute_write(attribute_key: &str) -> InvokeWorkerRpcResponse {
    InvokeWorkerRpcResponse {
        upsert_attributes: vec![AttributeWrite {
            key: attribute_key.to_owned(),
            value: Some(string_value("from RPC")),
            ..AttributeWrite::default()
        }],
        ..InvokeWorkerRpcResponse::default()
    }
}

#[test]
fn reapply_restores_committed_rpc_effects() {
    let mut harness = Harness::new();
    harness.start(start_request());
    commit_rpc(&mut harness, "rpc-1", rpc_attribute_write("from-rpc"), true);
    let dedup_rows = request_dedup_rows(&harness);
    harness.travel_to(reapplying(reset(FlowResetType::Beginning)));
    assert_eq!(harness.attribute_keys(), vec!["from-rpc".to_owned()]);
    let fork = harness.history_record(harness.last_seq()).unwrap();
    assert_eq!(fork.reapplied_inputs.len(), 1);
    // The reapplied input carries its original request ID, so a later travel reapplies it again.
    harness.travel_to(reapplying(reset(FlowResetType::Beginning)));
    assert_eq!(harness.attribute_keys(), vec!["from-rpc".to_owned()]);
    assert_eq!(request_dedup_rows(&harness), dedup_rows);
}

#[test]
fn reapplied_rpcs_bypass_request_deduplication() {
    let mut harness = Harness::new();
    harness.start(start_request());
    let genesis_holders = value_holder_count(&harness);
    let with_large_output = |attribute_key: &str| InvokeWorkerRpcResponse {
        output: Some(string_value(&attribute_key.repeat(300))),
        ..rpc_attribute_write(attribute_key)
    };
    commit_rpc(&mut harness, "foo", with_large_output("a"), true);
    // A client may choose the ID a synthesized reapply request ID would take.
    let second_reply = commit_rpc(
        &mut harness,
        "foo:time-travel-2",
        with_large_output("b"),
        true,
    );
    assert_eq!(value_holder_count(&harness), genesis_holders + 2);
    let dedup_rows = request_dedup_rows(&harness);
    let (attempt, _) = harness.travel_to(reapplying(reset(FlowResetType::Beginning)));
    assert_eq!(attempt, 2);
    assert_eq!(
        harness.attribute_keys(),
        vec!["a".to_owned(), "b".to_owned()]
    );
    assert_eq!(request_dedup_rows(&harness), dedup_rows);
    assert_eq!(value_holder_count(&harness), genesis_holders + 2);
    let retried = commit_rpc(
        &mut harness,
        "foo:time-travel-2",
        with_large_output("b"),
        true,
    );
    assert_eq!(retried, second_reply);
    let (attempt, _) = harness.travel_to(reapplying(reset(FlowResetType::Beginning)));
    assert_eq!(attempt, 3);
    assert_eq!(
        harness.attribute_keys(),
        vec!["a".to_owned(), "b".to_owned()]
    );
    assert_eq!(request_dedup_rows(&harness), dedup_rows);
    assert_eq!(value_holder_count(&harness), genesis_holders + 2);
}

#[test]
fn reapply_skips_a_transactional_rpc_whose_deletions_no_longer_hold() {
    let mut harness = Harness::new();
    harness.start(start_request());
    // A Worker result publishes m1; Worker results are not external input.
    harness.start_attempt("ChargeStep-1", StepMethod::WaitFor, 1);
    let published = harness.apply(Command::StepMethodCompleted(StepMethodCompletedCommand {
        invocation: Some(key("ChargeStep-1", StepMethod::WaitFor)),
        attempt: 1,
        response: Some(step_method_completed_command::Response::WaitFor(
            InvokeWaitForMethodResponse {
                publish_to_channel: vec![ChannelMessage {
                    channel_name: "orders".to_owned(),
                    value: Some(string_value("m1")),
                    message_id: "m1".to_owned(),
                }],
                ..InvokeWaitForMethodResponse::default()
            },
        )),
        app_worker_id: "worker-a".to_owned(),
        ..StepMethodCompletedCommand::default()
    }));
    assert!(is_ok(&published.reply), "{:?}", published.reply);
    let deleting = InvokeWorkerRpcResponse {
        delete_from_channel: vec![ChannelMessageDeletion {
            channel_name: "orders".to_owned(),
            message_id: "m1".to_owned(),
        }],
        ..rpc_attribute_write("from-rpc")
    };
    commit_rpc(&mut harness, "rpc-1", deleting, true);
    assert_eq!(harness.channel_message_count(), 0);
    let (attempt, _) = harness.travel_to(reapplying(reset(FlowResetType::Beginning)));
    assert_eq!(harness.run_record().current_attempt, attempt);
    assert!(
        harness.attribute_keys().is_empty(),
        "the RPC is skipped as a whole"
    );
    let fork = harness.history_record(harness.last_seq()).unwrap();
    assert_eq!(fork.reapplied_inputs.len(), 1);
}

fn set_indexed_attribute(harness: &mut Harness, attribute_key: &str, text: &str) {
    let outcome = harness.apply(Command::AttributesSet(AttributesSetCommand {
        request: Some(SetAttributesRequest {
            attributes: vec![AttributeWrite {
                key: attribute_key.to_owned(),
                value: Some(string_value(text)),
                index_config: Some(IndexConfig {
                    enable: true,
                    index_key: "ReviewState".to_owned(),
                    ..IndexConfig::default()
                }),
                ..AttributeWrite::default()
            }],
            ..SetAttributesRequest::default()
        }),
        hard_delete_objects: Vec::new(),
        request_fingerprint: Vec::new(),
    }));
    assert!(is_ok(&outcome.reply), "SetAttributes applies");
}

fn indexed_values(harness: &Harness) -> Vec<(String, Option<Value>)> {
    get_search_document(&harness.state, harness.context.flow_hash, harness.run())
        .expect("the search document reads")
        .expect("the run has a search document")
        .attributes
        .into_iter()
        .map(|attribute| (attribute.index_key, attribute.value))
        .collect()
}

#[test]
fn time_travel_rebuilds_indexed_attributes_from_the_restored_state() {
    let mut harness = Harness::new();
    harness.start(start_request());
    set_indexed_attribute(&mut harness, "state", "new");
    let new_time = harness
        .history_record(harness.last_seq())
        .unwrap()
        .timestamp_micros;
    set_indexed_attribute(&mut harness, "state", "ready");
    let ready = vec![("ReviewState".to_owned(), Some(string_value("ready")))];
    assert_eq!(indexed_values(&harness), ready);
    harness.travel_to(reset_to_time(new_time));
    assert_eq!(
        indexed_values(&harness),
        vec![("ReviewState".to_owned(), Some(string_value("new")))]
    );
    harness.travel_to(reapplying(ResetFlowRequest {
        run_attempt: 1,
        ..reset_to_time(new_time)
    }));
    assert_eq!(indexed_values(&harness), ready);
    harness.travel_to(reset(FlowResetType::Beginning));
    assert!(indexed_values(&harness).is_empty());
}
