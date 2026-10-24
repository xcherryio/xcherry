// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Wait condition engine and SkipTimer tests over an in-memory state.

use std::collections::BTreeMap;

use dexzd_apply::{
    ApplyContext, ApplyOutcome, FlowStateReader, StateReadError, StateRow, StateWrite,
    apply_flow_command,
};
use dexzd_keyspace::row_keys::{decode_timer_index_key, prefix_range, run_named_key};
use dexzd_keyspace::{
    ColumnFamily, PartitionHasher, RunId, RunTable, TenantId, partition_index_prefix,
    run_table_prefix,
};
use dexzd_protocol::dex::{
    ChannelCondition, ChannelMessage, CloseDecision, CloseDecisionType, ConditionCombination,
    ConditionResults, ConditionStatus, FlowErrorType, FlowStatus, FlowTimeoutPolicy,
    InvokeExecuteMethodResponse, InvokeWaitForMethodResponse, PublishToChannelRequest, RetryPolicy,
    SkipTimerRequest, StartFlowRequest, StepDecision, StepMethodEventInput, StepMovement,
    StepOptions, StopFlowRequest, StopType, TimerCondition, Value, WaitingCondition,
    WaitingConditionType, WorkerErrorResponse, WorkerTarget, flow_history_event, value,
};
use dexzd_protocol::dexzd::v1::{
    ChannelMetaRecord, ChannelPublishCommand, CommandReply, ConditionResultsRecord, FlowCommand,
    FlowStartCommand, FlowStopCommand, HistoryRecord, RunRecord, RunSettings,
    StepAttemptFailedCommand, StepAttemptStartedCommand, StepExecutionPhase, StepExecutionRecord,
    StepInvocationKey, StepMethod, StepMethodCompletedCommand, TimerId, TimerRecord,
    TimerSkipCommand, TimersFiredCommand, WaitConditionTimerId, command_reply,
    flow_command::Command, step_method_completed_command, timer_id,
};
use prost::Message;

const SECOND: i64 = 1_000_000;
const INVALID_ARGUMENT: i32 = 3;

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
    context: ApplyContext,
    now_micros: i64,
    flow_id: String,
    run_id: Vec<u8>,
}

impl Harness {
    fn new() -> Self {
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
        }
    }

    /// Applies a command twice against the same state, checks both results match, commits.
    fn apply(&mut self, body: Command) -> ApplyOutcome {
        self.now_micros += 1000;
        let command = FlowCommand {
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
            command_id: vec![0; 16],
            tenant_id: 0,
            command: Some(body),
        };
        let outcome =
            apply_flow_command(&self.context, &self.state, &command).expect("apply succeeds");
        let again =
            apply_flow_command(&self.context, &self.state, &command).expect("apply succeeds");
        assert_eq!(outcome.writes, again.writes, "apply must be deterministic");
        assert_eq!(
            outcome.effects, again.effects,
            "apply must be deterministic"
        );
        self.state.commit(&outcome);
        outcome
    }

    fn start(&mut self, request: StartFlowRequest) {
        let resolved_timeout_policy = match FlowTimeoutPolicy::try_from(request.flow_timeout_policy)
        {
            Ok(FlowTimeoutPolicy::Unspecified) | Err(_) => FlowTimeoutPolicy::Fail,
            Ok(policy) => policy,
        };
        let outcome = self.apply(Command::FlowStart(FlowStartCommand {
            new_run_id: uuid::Uuid::now_v7().as_bytes().to_vec(),
            request: Some(request),
            settings: Some(settings()),
            resolved_timeout_policy: resolved_timeout_policy as i32,
            worker_target: Some(WorkerTarget {
                is_pull_mode: true,
                ..WorkerTarget::default()
            }),
            hard_delete_objects: Vec::new(),
            request_fingerprint: Vec::new(),
        }));
        let Some(command_reply::Reply::StartFlow(reply)) = &outcome.reply.reply else {
            panic!("unexpected start reply {:?}", outcome.reply);
        };
        self.run_id = reply.run_id.clone();
    }

    /// Starts a Flow whose first Step `WaitStep-1` has a pending WaitFor.
    fn start_waiting_flow(&mut self, step_options: Option<StepOptions>) {
        let mut request = start_request();
        request.step_options = step_options;
        self.start(request);
    }

    /// Starts a Flow whose first Step creates `WaitStep-1` and `WaitStep-2`, both with a
    /// pending WaitFor, and returns the outcome of that first Step's Execute completion.
    fn start_two_waiting_steps(&mut self) -> ApplyOutcome {
        let mut request = start_request();
        request.start_step_type = "PlanStep".to_owned();
        request.step_options = Some(StepOptions {
            skip_wait_for: true,
            ..StepOptions::default()
        });
        self.start(request);
        self.start_attempt("PlanStep-1", StepMethod::Execute, 1);
        let movement = StepMovement {
            step_type: "WaitStep".to_owned(),
            ..StepMovement::default()
        };
        self.complete_execute(
            "PlanStep-1",
            StepDecision {
                next_steps: vec![movement.clone(), movement],
                ..StepDecision::default()
            },
        )
    }

    fn start_attempt(&mut self, step_execution_id: &str, method: StepMethod, attempt: u32) {
        let outcome = self.apply(Command::StepAttemptStarted(StepAttemptStartedCommand {
            invocation: Some(invocation_key(step_execution_id, method)),
            attempt,
            task_id: vec![attempt as u8; 16],
        }));
        assert!(is_ok(&outcome.reply), "attempt start: {:?}", outcome.reply);
    }

    /// Runs the Step's WaitFor and returns the given condition (and publications).
    fn wait_on(
        &mut self,
        step_execution_id: &str,
        condition: WaitingCondition,
        publications: Vec<ChannelMessage>,
    ) -> ApplyOutcome {
        self.start_attempt(step_execution_id, StepMethod::WaitFor, 1);
        let outcome = self.apply(Command::StepMethodCompleted(StepMethodCompletedCommand {
            invocation: Some(invocation_key(step_execution_id, StepMethod::WaitFor)),
            attempt: 1,
            response: Some(step_method_completed_command::Response::WaitFor(
                InvokeWaitForMethodResponse {
                    waiting_condition: Some(condition),
                    publish_to_channel: publications,
                    ..InvokeWaitForMethodResponse::default()
                },
            )),
            ..StepMethodCompletedCommand::default()
        }));
        assert!(
            is_ok(&outcome.reply),
            "WaitFor completion: {:?}",
            outcome.reply
        );
        outcome
    }

    fn complete_execute(
        &mut self,
        step_execution_id: &str,
        decision: StepDecision,
    ) -> ApplyOutcome {
        let outcome = self.apply(Command::StepMethodCompleted(StepMethodCompletedCommand {
            invocation: Some(invocation_key(step_execution_id, StepMethod::Execute)),
            attempt: 1,
            response: Some(step_method_completed_command::Response::Execute(
                InvokeExecuteMethodResponse {
                    step_decision: Some(decision),
                    ..InvokeExecuteMethodResponse::default()
                },
            )),
            ..StepMethodCompletedCommand::default()
        }));
        assert!(
            is_ok(&outcome.reply),
            "Execute completion: {:?}",
            outcome.reply
        );
        outcome
    }

    fn publish(&mut self, channel_name: &str, texts: &[&str]) {
        let messages = texts
            .iter()
            .map(|text| message(channel_name, text))
            .collect();
        let outcome = self.apply(Command::ChannelPublish(ChannelPublishCommand {
            request: Some(PublishToChannelRequest {
                messages,
                ..PublishToChannelRequest::default()
            }),
        }));
        assert!(is_ok(&outcome.reply), "publish: {:?}", outcome.reply);
    }

    fn fire_wait_timer(
        &mut self,
        step_execution_id: &str,
        index: u32,
        run_attempt: u32,
    ) -> ApplyOutcome {
        self.apply(Command::TimersFired(TimersFiredCommand {
            timers: vec![wait_timer(step_execution_id, index, run_attempt)],
        }))
    }

    fn skip(
        &mut self,
        step_execution_id: &str,
        condition_id: &str,
        index: Option<i32>,
    ) -> ApplyOutcome {
        self.apply(Command::TimerSkip(TimerSkipCommand {
            request: Some(SkipTimerRequest {
                flow_id: self.flow_id.clone(),
                step_execution_id: step_execution_id.to_owned(),
                timer_condition_id: condition_id.to_owned(),
                timer_condition_index: index,
                ..SkipTimerRequest::default()
            }),
        }))
    }

    fn run_id(&self) -> RunId {
        RunId::from_slice(&self.run_id).expect("the harness has a run")
    }

    fn get<RowMessage: Message + Default>(
        &self,
        table: RunTable,
        name: &str,
    ) -> Option<RowMessage> {
        let key = run_named_key(self.context.flow_hash, self.run_id(), table, name);
        self.state
            .rows
            .get(&(table.column_family(), key))
            .map(|bytes| RowMessage::decode(bytes.as_slice()).expect("the row decodes"))
    }

    fn run_record(&self) -> RunRecord {
        let key = dexzd_keyspace::row_keys::run_singleton_key(
            self.context.flow_hash,
            self.run_id(),
            RunTable::RunRecord,
        );
        let bytes = &self.state.rows[&(ColumnFamily::Flow, key)];
        RunRecord::decode(bytes.as_slice()).expect("the run record decodes")
    }

    fn step(&self, step_execution_id: &str) -> StepExecutionRecord {
        self.get(RunTable::StepExecution, step_execution_id)
            .expect("the Step exists")
    }

    fn channel_meta(&self, channel_name: &str) -> ChannelMetaRecord {
        self.get(RunTable::ChannelMeta, channel_name)
            .unwrap_or_default()
    }

    /// Key suffixes and values of every row of one run table.
    fn table_rows(&self, table: RunTable) -> Vec<(Vec<u8>, Vec<u8>)> {
        let prefix = run_table_prefix(self.context.flow_hash, self.run_id(), table);
        let (start, end) = prefix_range(&prefix);
        self.state
            .rows
            .range((table.column_family(), start)..(table.column_family(), end))
            .map(|((_, key), value)| (key[prefix.len()..].to_vec(), value.clone()))
            .collect()
    }

    fn wait_timers(&self) -> Vec<TimerRecord> {
        self.table_rows(RunTable::Timer)
            .into_iter()
            .map(|(_, bytes)| TimerRecord::decode(bytes.as_slice()).expect("timer decodes"))
            .filter(|record| {
                matches!(
                    record
                        .timer_id
                        .as_ref()
                        .and_then(|timer| timer.kind.as_ref()),
                    Some(timer_id::Kind::WaitCondition(_))
                )
            })
            .collect()
    }

    fn wait_timer_index_entries(&self) -> usize {
        let prefix =
            partition_index_prefix(self.context.partition, dexzd_keyspace::IndexKind::Timer);
        let (start, end) = prefix_range(&prefix);
        self.state
            .rows
            .range((ColumnFamily::PartitionIndex, start)..(ColumnFamily::PartitionIndex, end))
            .filter_map(|((_, key), _)| decode_timer_index_key(key))
            .filter(|entry| {
                let timer = TimerId::decode(entry.encoded_timer_id.as_slice()).expect("decodes");
                matches!(timer.kind, Some(timer_id::Kind::WaitCondition(_)))
            })
            .count()
    }

    /// Holder kind bytes (see ValueHolder::encode) of every holder row.
    fn holder_kinds(&self) -> Vec<u8> {
        self.table_rows(RunTable::ValueHolder)
            .into_iter()
            .map(|(suffix, _)| suffix[12])
            .collect()
    }

    /// Channel result values of a Step as text, by channel condition.
    fn consumed_texts(&self, step_execution_id: &str) -> Vec<(i32, Vec<String>)> {
        let results = self
            .step(step_execution_id)
            .condition_results
            .expect("the wait completed");
        results
            .channel_results
            .iter()
            .map(|channel| {
                let texts = channel
                    .messages
                    .iter()
                    .map(
                        |message| match message.value.as_ref().and_then(|v| v.kind.as_ref()) {
                            Some(dexzd_protocol::dexzd::v1::stored_value::Kind::InlineValue(
                                Value {
                                    kind: Some(value::Kind::StringValue(text)),
                                },
                            )) => text.clone(),
                            other => panic!("unexpected stored value {other:?}"),
                        },
                    )
                    .collect();
                (channel.condition_status, texts)
            })
            .collect()
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

fn start_request() -> StartFlowRequest {
    StartFlowRequest {
        flow_id: "order-1".to_owned(),
        flow_type: "OrderFlow".to_owned(),
        start_step_type: "WaitStep".to_owned(),
        request_id: "request-1".to_owned(),
        ..StartFlowRequest::default()
    }
}

fn invocation_key(step_execution_id: &str, method: StepMethod) -> StepInvocationKey {
    StepInvocationKey {
        step_execution_id: step_execution_id.to_owned(),
        method: method as i32,
    }
}

fn wait_timer(step_execution_id: &str, index: u32, run_attempt: u32) -> TimerId {
    TimerId {
        run_attempt,
        kind: Some(timer_id::Kind::WaitCondition(WaitConditionTimerId {
            step_execution_id: step_execution_id.to_owned(),
            timer_index: index,
        })),
    }
}

fn message(channel_name: &str, text: &str) -> ChannelMessage {
    ChannelMessage {
        channel_name: channel_name.to_owned(),
        value: Some(Value {
            kind: Some(value::Kind::StringValue(text.to_owned())),
        }),
        message_id: format!("id-{text}"),
    }
}

fn timer(condition_id: &str, duration_seconds: i64) -> TimerCondition {
    TimerCondition {
        condition_id: condition_id.to_owned(),
        duration_seconds,
        firing_unix_timestamp_seconds: 0,
    }
}

fn channel(
    condition_id: &str,
    channel_name: &str,
    at_least: Option<i32>,
    at_most: Option<i32>,
) -> ChannelCondition {
    ChannelCondition {
        condition_id: condition_id.to_owned(),
        channel_name: channel_name.to_owned(),
        at_least,
        at_most,
    }
}

fn condition(
    condition_type: WaitingConditionType,
    timers: Vec<TimerCondition>,
    channels: Vec<ChannelCondition>,
) -> WaitingCondition {
    WaitingCondition {
        waiting_condition_type: condition_type as i32,
        timer_conditions: timers,
        channel_conditions: channels,
        ..WaitingCondition::default()
    }
}

fn is_ok(reply: &CommandReply) -> bool {
    matches!(reply.reply, Some(command_reply::Reply::Ok(_)))
}

fn is_error(reply: &CommandReply, grpc_code: i32) -> bool {
    matches!(&reply.reply, Some(command_reply::Reply::Error(error)) if error.grpc_code == grpc_code)
}

fn phase(step: &StepExecutionRecord) -> StepExecutionPhase {
    StepExecutionPhase::try_from(step.phase).expect("known phase")
}

const WAITING: i32 = ConditionStatus::Waiting as i32;
const COMPLETED: i32 = ConditionStatus::Completed as i32;

fn texts(values: &[&str]) -> Vec<String> {
    values.iter().map(|text| (*text).to_owned()).collect()
}

/// Waits on one channel condition with these bounds after publishing `present` messages.
fn consume_with_bounds(
    at_least: Option<i32>,
    at_most: Option<i32>,
    present: &[&str],
) -> (Harness, StepExecutionRecord) {
    let mut harness = Harness::new();
    harness.start_waiting_flow(None);
    if !present.is_empty() {
        harness.publish("orders", present);
    }
    harness.wait_on(
        "WaitStep-1",
        condition(
            WaitingConditionType::AnyCompleted,
            Vec::new(),
            vec![channel("c", "orders", at_least, at_most)],
        ),
        Vec::new(),
    );
    let step = harness.step("WaitStep-1");
    (harness, step)
}

#[test]
fn unset_bounds_take_exactly_one_message() {
    let (harness, step) = consume_with_bounds(None, None, &["m1", "m2", "m3"]);
    assert_eq!(phase(&step), StepExecutionPhase::Execute);
    assert_eq!(
        harness.consumed_texts("WaitStep-1"),
        vec![(COMPLETED, texts(&["m1"]))]
    );
    let meta = harness.channel_meta("orders");
    assert_eq!((meta.pending_count, meta.claimed_count), (2, 1));
}

#[test]
fn unset_bounds_wait_on_an_empty_channel() {
    let (harness, step) = consume_with_bounds(None, None, &[]);
    assert_eq!(phase(&step), StepExecutionPhase::Waiting);
    assert!(step.condition_results.is_none());
    assert_eq!(harness.table_rows(RunTable::Waiter).len(), 1);
}

#[test]
fn at_most_only_is_satisfied_by_an_empty_channel() {
    let (harness, step) = consume_with_bounds(None, Some(2), &[]);
    assert_eq!(phase(&step), StepExecutionPhase::Execute);
    assert_eq!(
        harness.consumed_texts("WaitStep-1"),
        vec![(COMPLETED, Vec::new())]
    );
    assert!(harness.table_rows(RunTable::Waiter).is_empty());
}

#[test]
fn at_most_only_takes_up_to_the_maximum() {
    let (harness, _) = consume_with_bounds(None, Some(2), &["m1", "m2", "m3"]);
    assert_eq!(
        harness.consumed_texts("WaitStep-1"),
        vec![(COMPLETED, texts(&["m1", "m2"]))]
    );
}

#[test]
fn at_least_only_waits_for_the_minimum_then_takes_everything() {
    let (mut harness, step) = consume_with_bounds(Some(2), None, &["m1"]);
    assert_eq!(phase(&step), StepExecutionPhase::Waiting);
    harness.publish("orders", &["m2", "m3"]);
    assert_eq!(
        phase(&harness.step("WaitStep-1")),
        StepExecutionPhase::Execute
    );
    assert_eq!(
        harness.consumed_texts("WaitStep-1"),
        vec![(COMPLETED, texts(&["m1", "m2", "m3"]))]
    );
    let meta = harness.channel_meta("orders");
    assert_eq!((meta.pending_count, meta.claimed_count), (0, 3));
}

#[test]
fn both_bounds_take_a_range() {
    let (_, step) = consume_with_bounds(Some(2), Some(3), &["m1"]);
    assert_eq!(phase(&step), StepExecutionPhase::Waiting);
    let (harness, _) = consume_with_bounds(Some(2), Some(3), &["m1", "m2", "m3", "m4"]);
    assert_eq!(
        harness.consumed_texts("WaitStep-1"),
        vec![(COMPLETED, texts(&["m1", "m2", "m3"]))]
    );
}

#[test]
fn all_reserves_every_minimum_before_filling_maximums_on_a_shared_channel() {
    let mut harness = Harness::new();
    harness.start_waiting_flow(None);
    harness.publish("orders", &["m1", "m2"]);
    let shared = condition(
        WaitingConditionType::AllCompleted,
        Vec::new(),
        vec![
            channel("a", "orders", Some(1), None),
            channel("b", "orders", Some(1), Some(1)),
        ],
    );
    harness.wait_on("WaitStep-1", shared, Vec::new());
    assert_eq!(
        harness.consumed_texts("WaitStep-1"),
        vec![(COMPLETED, texts(&["m1"])), (COMPLETED, texts(&["m2"]))],
        "the unbounded condition must not take the other condition's minimum"
    );
    let mut harness = Harness::new();
    harness.start_waiting_flow(None);
    harness.publish("orders", &["m1", "m2", "m3", "m4"]);
    let shared = condition(
        WaitingConditionType::AllCompleted,
        Vec::new(),
        vec![
            channel("a", "orders", Some(1), None),
            channel("b", "orders", Some(1), Some(1)),
        ],
    );
    harness.wait_on("WaitStep-1", shared, Vec::new());
    assert_eq!(
        harness.consumed_texts("WaitStep-1"),
        vec![
            (COMPLETED, texts(&["m1", "m2", "m3"])),
            (COMPLETED, texts(&["m4"]))
        ],
        "extras fill in declaration order, messages leave FIFO"
    );
}

#[test]
fn all_waits_for_every_member() {
    let mut harness = Harness::new();
    harness.start_waiting_flow(None);
    let all = condition(
        WaitingConditionType::AllCompleted,
        vec![timer("t", 60)],
        vec![channel("c", "orders", None, None)],
    );
    harness.wait_on("WaitStep-1", all, Vec::new());
    harness.publish("orders", &["m1"]);
    assert_eq!(
        phase(&harness.step("WaitStep-1")),
        StepExecutionPhase::Waiting
    );
    assert_eq!(
        harness.channel_meta("orders").pending_count,
        1,
        "nothing is reserved"
    );
    harness.fire_wait_timer("WaitStep-1", 0, 1);
    let step = harness.step("WaitStep-1");
    assert_eq!(phase(&step), StepExecutionPhase::Execute);
    let results = step.condition_results.expect("completed");
    assert_eq!(results.timer_results[0].condition_status, COMPLETED);
    assert!(!results.wait_for_failed);
    assert_eq!(
        harness.consumed_texts("WaitStep-1"),
        vec![(COMPLETED, texts(&["m1"]))]
    );
}

#[test]
fn any_of_reports_only_the_winning_candidate() {
    let mut harness = Harness::new();
    harness.start_waiting_flow(None);
    harness.publish("second", &["s1"]);
    let any = condition(
        WaitingConditionType::AnyCompleted,
        vec![timer("t", 60)],
        vec![
            channel("first", "first", None, None),
            channel("second", "second", None, None),
        ],
    );
    harness.wait_on("WaitStep-1", any, Vec::new());
    let step = harness.step("WaitStep-1");
    assert_eq!(phase(&step), StepExecutionPhase::Execute);
    let results = step.condition_results.clone().expect("completed");
    assert_eq!(results.timer_results[0].condition_status, WAITING);
    assert_eq!(
        harness.consumed_texts("WaitStep-1"),
        vec![(WAITING, Vec::new()), (COMPLETED, texts(&["s1"]))]
    );
    assert!(
        harness.wait_timers().is_empty(),
        "the pending timer is disarmed"
    );
    assert_eq!(harness.wait_timer_index_entries(), 0);
    assert!(harness.table_rows(RunTable::Waiter).is_empty());
}

#[test]
fn any_of_completes_on_the_timer_and_leaves_messages_alone() {
    let mut harness = Harness::new();
    harness.start_waiting_flow(None);
    let any = condition(
        WaitingConditionType::AnyCompleted,
        vec![timer("t", 60)],
        vec![channel("c", "orders", Some(2), None)],
    );
    let waited = harness.wait_on("WaitStep-1", any, Vec::new());
    let fire_at = harness.wait_timers()[0].fire_at_micros;
    assert_eq!(
        fire_at,
        harness.now_micros + 60 * SECOND,
        "exact microseconds"
    );
    let stored = harness.step("WaitStep-1").wait.expect("waiting");
    assert_eq!(
        stored.condition.expect("stored").timer_conditions[0].firing_unix_timestamp_seconds,
        fire_at / SECOND + 1,
        "seconds are rounded up"
    );
    assert!(waited
        .effects
        .iter()
        .any(|effect| matches!(effect, dexzd_apply::ApplyEffect::TimerArmed { fire_at_micros } if *fire_at_micros == fire_at)));
    harness.publish("orders", &["m1"]);
    harness.fire_wait_timer("WaitStep-1", 0, 1);
    assert_eq!(
        phase(&harness.step("WaitStep-1")),
        StepExecutionPhase::Execute
    );
    assert_eq!(
        harness.consumed_texts("WaitStep-1"),
        vec![(WAITING, Vec::new())]
    );
    assert_eq!(harness.channel_meta("orders").pending_count, 1);
    assert!(harness.table_rows(RunTable::Waiter).is_empty());
}

#[test]
fn any_combination_uses_combination_declaration_order() {
    let mut harness = Harness::new();
    harness.start_waiting_flow(None);
    harness.publish("a", &["a1"]);
    harness.publish("b", &["b1"]);
    let mut combination = condition(
        WaitingConditionType::AnyCombinationCompleted,
        vec![timer("t", 60)],
        vec![channel("a", "a", None, None), channel("b", "b", None, None)],
    );
    combination.condition_combinations = vec![
        ConditionCombination {
            condition_ids: vec!["t".to_owned(), "a".to_owned()],
        },
        ConditionCombination {
            condition_ids: vec!["b".to_owned(), "a".to_owned()],
        },
        ConditionCombination {
            condition_ids: vec!["a".to_owned()],
        },
    ];
    harness.wait_on("WaitStep-1", combination, Vec::new());
    assert_eq!(
        harness.consumed_texts("WaitStep-1"),
        vec![(COMPLETED, texts(&["a1"])), (COMPLETED, texts(&["b1"]))],
        "the first satisfied combination wins even when a later one is smaller"
    );
}

#[test]
fn any_combination_waits_until_a_whole_combination_is_satisfied() {
    let mut harness = Harness::new();
    harness.start_waiting_flow(None);
    let mut combination = condition(
        WaitingConditionType::AnyCombinationCompleted,
        vec![timer("t", 60)],
        vec![channel("a", "a", None, None), channel("b", "b", None, None)],
    );
    combination.condition_combinations = vec![
        ConditionCombination {
            condition_ids: vec!["t".to_owned(), "a".to_owned()],
        },
        ConditionCombination {
            condition_ids: vec!["b".to_owned()],
        },
    ];
    harness.wait_on("WaitStep-1", combination, Vec::new());
    harness.publish("a", &["a1"]);
    assert_eq!(
        phase(&harness.step("WaitStep-1")),
        StepExecutionPhase::Waiting
    );
    harness.skip("WaitStep-1", "t", None);
    let step = harness.step("WaitStep-1");
    assert_eq!(phase(&step), StepExecutionPhase::Execute);
    assert_eq!(
        harness.consumed_texts("WaitStep-1"),
        vec![(COMPLETED, texts(&["a1"])), (WAITING, Vec::new())]
    );
    assert_eq!(
        step.condition_results.expect("completed").timer_results[0].condition_status,
        COMPLETED,
        "a skipped timer reports COMPLETED"
    );
}

#[test]
fn waiters_on_one_channel_are_served_in_wait_start_order() {
    let mut harness = Harness::new();
    harness.start_two_waiting_steps();
    let exactly_one = || {
        condition(
            WaitingConditionType::AnyCompleted,
            Vec::new(),
            vec![channel("c", "orders", None, None)],
        )
    };
    harness.wait_on("WaitStep-2", exactly_one(), Vec::new());
    harness.wait_on("WaitStep-1", exactly_one(), Vec::new());
    harness.publish("orders", &["m1"]);
    assert_eq!(
        phase(&harness.step("WaitStep-2")),
        StepExecutionPhase::Execute
    );
    assert_eq!(
        phase(&harness.step("WaitStep-1")),
        StepExecutionPhase::Waiting
    );
    assert_eq!(
        harness.consumed_texts("WaitStep-2"),
        vec![(COMPLETED, texts(&["m1"]))]
    );
    harness.publish("orders", &["m2"]);
    assert_eq!(
        harness.consumed_texts("WaitStep-1"),
        vec![(COMPLETED, texts(&["m2"]))]
    );
}

#[test]
fn a_consumption_by_one_waiter_is_visible_to_the_next_in_the_same_command() {
    let mut harness = Harness::new();
    harness.start_two_waiting_steps();
    harness.wait_on(
        "WaitStep-1",
        condition(
            WaitingConditionType::AnyCompleted,
            Vec::new(),
            vec![channel("c", "orders", Some(2), None)],
        ),
        Vec::new(),
    );
    harness.wait_on(
        "WaitStep-2",
        condition(
            WaitingConditionType::AnyCompleted,
            Vec::new(),
            vec![channel("c", "orders", None, None)],
        ),
        Vec::new(),
    );
    harness.publish("orders", &["m1", "m2", "m3"]);
    assert_eq!(
        harness.consumed_texts("WaitStep-1"),
        vec![(COMPLETED, texts(&["m1", "m2", "m3"]))]
    );
    assert_eq!(
        phase(&harness.step("WaitStep-2")),
        StepExecutionPhase::Waiting,
        "the earlier waiter took every message"
    );
    harness.publish("orders", &["m4", "m5"]);
    assert_eq!(
        harness.consumed_texts("WaitStep-2"),
        vec![(COMPLETED, texts(&["m4"]))]
    );
    let meta = harness.channel_meta("orders");
    assert_eq!((meta.pending_count, meta.claimed_count), (1, 4));
}

#[test]
fn a_wait_for_publication_reaches_earlier_waiters_first() {
    let mut harness = Harness::new();
    harness.start_two_waiting_steps();
    let exactly_one = || {
        condition(
            WaitingConditionType::AnyCompleted,
            Vec::new(),
            vec![channel("c", "orders", None, None)],
        )
    };
    harness.wait_on("WaitStep-1", exactly_one(), Vec::new());
    harness.wait_on("WaitStep-2", exactly_one(), vec![message("orders", "m1")]);
    assert_eq!(
        harness.consumed_texts("WaitStep-1"),
        vec![(COMPLETED, texts(&["m1"]))]
    );
    assert_eq!(
        phase(&harness.step("WaitStep-2")),
        StepExecutionPhase::Waiting
    );
}

/// Waits on ALL of timers `t0` and `t1` (and the Flow timeout of 30 seconds).
fn waiting_on_two_timers() -> Harness {
    let mut harness = Harness::new();
    let mut request = start_request();
    request.flow_timeout_seconds = 30;
    harness.start(request);
    harness.wait_on(
        "WaitStep-1",
        condition(
            WaitingConditionType::AllCompleted,
            vec![timer("t0", 60), timer("t1", 60)],
            Vec::new(),
        ),
        Vec::new(),
    );
    harness
}

#[test]
fn skip_selects_by_condition_id_then_by_index() {
    let mut harness = waiting_on_two_timers();
    let skipped = harness.skip("WaitStep-1", "t1", Some(0));
    assert!(is_ok(&skipped.reply));
    let wait = harness.step("WaitStep-1").wait.expect("still waiting");
    assert_eq!(
        wait.timer_statuses,
        vec![WAITING, COMPLETED],
        "the ID wins over the index"
    );
    assert_eq!(harness.wait_timers().len(), 1);
    assert_eq!(harness.wait_timer_index_entries(), 1);
    harness.skip("WaitStep-1", "", Some(0));
    let step = harness.step("WaitStep-1");
    assert_eq!(phase(&step), StepExecutionPhase::Execute);
    assert!(harness.wait_timers().is_empty());
    assert_eq!(harness.wait_timer_index_entries(), 0);
}

#[test]
fn invalid_skips_are_rejected_without_writes() {
    let mut harness = waiting_on_two_timers();
    assert!(is_ok(&harness.skip("WaitStep-1", "t0", None).reply));
    for (step_execution_id, condition_id, index) in [
        ("WaitStep-1", "t0", None),
        ("WaitStep-1", "", Some(0)),
        ("WaitStep-1", "missing", Some(1)),
        ("WaitStep-1", "", Some(2)),
        ("WaitStep-1", "", Some(-1)),
        ("WaitStep-1", "", None),
        ("OtherStep-1", "", Some(0)),
        ("sys:timeout_handler-1", "", Some(1)),
        ("sys:timeout_handler-1", "t0", Some(0)),
    ] {
        let outcome = harness.skip(step_execution_id, condition_id, index);
        assert!(
            is_error(&outcome.reply, INVALID_ARGUMENT),
            "skip {step_execution_id} {condition_id:?} {index:?}: {:?}",
            outcome.reply
        );
        assert!(outcome.writes.is_empty());
    }
    let fired = harness.fire_wait_timer("WaitStep-1", 0, 1);
    assert!(fired.writes.is_empty(), "a skipped timer's row is gone");
}

#[test]
fn skipping_a_timer_of_a_step_that_left_waiting_is_rejected() {
    let mut harness = Harness::new();
    harness.start_waiting_flow(None);
    harness.publish("orders", &["m1"]);
    harness.wait_on(
        "WaitStep-1",
        condition(
            WaitingConditionType::AnyCompleted,
            vec![timer("t", 60)],
            vec![channel("c", "orders", None, None)],
        ),
        Vec::new(),
    );
    let outcome = harness.skip("WaitStep-1", "t", None);
    assert!(is_error(&outcome.reply, INVALID_ARGUMENT));
}

#[test]
fn skipping_the_timeout_handler_timer_fires_the_flow_timeout() {
    let mut harness = waiting_on_two_timers();
    let outcome = harness.skip("sys:timeout_handler-1", "", Some(0));
    assert!(is_ok(&outcome.reply));
    let record = harness.run_record();
    assert_eq!(record.status, FlowStatus::Failed as i32);
    assert_eq!(
        record.close.expect("closed").error_type,
        FlowErrorType::FlowTimeout as i32
    );
    assert!(
        harness.wait_timers().is_empty(),
        "closing released the waiting Step"
    );
    assert!(harness.table_rows(RunTable::Waiter).is_empty());
    let again = harness.skip("sys:timeout_handler-1", "", Some(0));
    assert!(is_error(&again.reply, INVALID_ARGUMENT));
}

#[test]
fn skipping_the_timeout_handler_timer_starts_the_handler() {
    let mut harness = Harness::new();
    let mut request = start_request();
    request.flow_timeout_seconds = 30;
    request.flow_timeout_policy = FlowTimeoutPolicy::Handler as i32;
    harness.start(request);
    assert!(is_ok(
        &harness.skip("sys:timeout_handler-1", "", Some(0)).reply
    ));
    let handler = harness.step("sys:timeout_handler-1");
    assert_eq!(phase(&handler), StepExecutionPhase::Execute);
    let again = harness.skip("sys:timeout_handler-1", "", Some(0));
    assert!(
        is_error(&again.reply, INVALID_ARGUMENT),
        "the timeout already fired"
    );
}

#[test]
fn timers_of_an_old_run_attempt_are_ignored() {
    let mut harness = Harness::new();
    harness.start_waiting_flow(None);
    harness.wait_on(
        "WaitStep-1",
        condition(
            WaitingConditionType::AnyCompleted,
            vec![timer("t", 60)],
            Vec::new(),
        ),
        Vec::new(),
    );
    let stale = harness.fire_wait_timer("WaitStep-1", 0, 2);
    assert!(stale.writes.is_empty());
    assert_eq!(
        phase(&harness.step("WaitStep-1")),
        StepExecutionPhase::Waiting
    );
    let unknown = harness.fire_wait_timer("WaitStep-1", 5, 1);
    assert!(
        unknown.writes.is_empty(),
        "no timer row exists for that index"
    );
    harness.fire_wait_timer("WaitStep-1", 0, 1);
    assert_eq!(
        phase(&harness.step("WaitStep-1")),
        StepExecutionPhase::Execute
    );
}

#[test]
fn canceling_a_waiting_step_releases_its_timers_and_waiters() {
    let mut harness = Harness::new();
    harness.start_waiting_flow(None);
    harness.wait_on(
        "WaitStep-1",
        condition(
            WaitingConditionType::AllCompleted,
            vec![timer("t", 60)],
            vec![
                channel("a", "orders", None, None),
                channel("b", "orders", Some(3), None),
                channel("c", "refunds", None, None),
            ],
        ),
        Vec::new(),
    );
    assert_eq!(
        harness.table_rows(RunTable::Waiter).len(),
        2,
        "one row per channel"
    );
    harness.publish("orders", &["m1"]);
    let stopped = harness.apply(Command::FlowStop(FlowStopCommand {
        request: Some(StopFlowRequest {
            stop_type: StopType::Cancel as i32,
            ..StopFlowRequest::default()
        }),
    }));
    assert!(is_ok(&stopped.reply));
    let step = harness.step("WaitStep-1");
    assert_eq!(phase(&step), StepExecutionPhase::Canceled);
    assert!(step.wait.is_none() && step.condition_results.is_none());
    assert!(harness.wait_timers().is_empty());
    assert_eq!(harness.wait_timer_index_entries(), 0);
    assert!(harness.table_rows(RunTable::Waiter).is_empty());
    assert_eq!(
        harness.channel_meta("orders").pending_count,
        1,
        "a canceled wait consumes nothing"
    );
}

#[test]
fn canceling_a_waiting_sibling_by_step_type_releases_its_registration() {
    let mut harness = Harness::new();
    harness.start_two_waiting_steps();
    harness.wait_on(
        "WaitStep-1",
        condition(
            WaitingConditionType::AnyCompleted,
            vec![timer("t", 60)],
            vec![channel("c", "orders", None, None)],
        ),
        Vec::new(),
    );
    harness.wait_on(
        "WaitStep-2",
        condition(
            WaitingConditionType::AnyCompleted,
            vec![timer("t", 0)],
            Vec::new(),
        ),
        Vec::new(),
    );
    harness.fire_wait_timer("WaitStep-2", 0, 1);
    harness.start_attempt("WaitStep-2", StepMethod::Execute, 1);
    harness.complete_execute(
        "WaitStep-2",
        StepDecision {
            cancel_step_types: vec!["WaitStep".to_owned()],
            ..StepDecision::default()
        },
    );
    assert_eq!(
        phase(&harness.step("WaitStep-1")),
        StepExecutionPhase::Canceled
    );
    assert!(harness.wait_timers().is_empty());
    assert!(harness.table_rows(RunTable::Waiter).is_empty());
}

#[test]
fn consumed_values_change_holders_and_become_garbage_when_execute_ends() {
    const CHANNEL_MESSAGE_HOLDER: u8 = 0x03;
    const CONDITION_RESULT_HOLDER: u8 = 0x06;
    let mut harness = Harness::new();
    harness.start_waiting_flow(None);
    let large = "x".repeat(500);
    harness.publish("orders", &[large.as_str()]);
    assert_eq!(harness.holder_kinds(), vec![CHANNEL_MESSAGE_HOLDER]);
    harness.wait_on(
        "WaitStep-1",
        condition(
            WaitingConditionType::AnyCompleted,
            Vec::new(),
            vec![channel("c", "orders", None, None)],
        ),
        Vec::new(),
    );
    assert_eq!(harness.holder_kinds(), vec![CONDITION_RESULT_HOLDER]);
    assert!(harness.table_rows(RunTable::ValueGarbage).is_empty());
    harness.start_attempt("WaitStep-1", StepMethod::Execute, 1);
    let snapshot = harness
        .step("WaitStep-1")
        .invocation
        .and_then(|invocation| invocation.snapshot)
        .expect("a running attempt has a snapshot");
    let snapshot_results = snapshot
        .condition_results
        .expect("Execute snapshots results");
    assert_eq!(snapshot_results.channel_results[0].messages.len(), 1);
    harness.complete_execute("WaitStep-1", StepDecision::default());
    assert!(harness.holder_kinds().is_empty());
    assert_eq!(harness.table_rows(RunTable::ValueGarbage).len(), 1);
    assert_eq!(harness.channel_meta("orders"), ChannelMetaRecord::default());
}

#[test]
fn an_execute_that_exhausts_its_retries_releases_its_claimed_messages() {
    let mut harness = Harness::new();
    harness.start_waiting_flow(Some(StepOptions {
        execute_retry_policy: Some(RetryPolicy {
            maximum_attempts: 1,
            ..RetryPolicy::default()
        }),
        ..StepOptions::default()
    }));
    let large = "y".repeat(500);
    harness.publish("orders", &[large.as_str()]);
    harness.wait_on(
        "WaitStep-1",
        condition(
            WaitingConditionType::AnyCompleted,
            Vec::new(),
            vec![channel("c", "orders", None, None)],
        ),
        Vec::new(),
    );
    assert_eq!(harness.channel_meta("orders").claimed_count, 1);
    harness.start_attempt("WaitStep-1", StepMethod::Execute, 1);
    harness.apply(Command::StepAttemptFailed(StepAttemptFailedCommand {
        invocation: Some(invocation_key("WaitStep-1", StepMethod::Execute)),
        attempt: 1,
        kind: 1,
        worker_error: Some(WorkerErrorResponse {
            detail: "boom".to_owned(),
            ..WorkerErrorResponse::default()
        }),
        ..StepAttemptFailedCommand::default()
    }));
    assert_eq!(harness.run_record().status, FlowStatus::Failed as i32);
    assert_eq!(harness.channel_meta("orders"), ChannelMetaRecord::default());
    assert!(harness.holder_kinds().is_empty());
    assert!(harness.step("WaitStep-1").condition_results.is_none());
}

#[test]
fn graceful_completion_waits_for_a_waiting_step() {
    let mut harness = Harness::new();
    harness.start_two_waiting_steps();
    harness.wait_on(
        "WaitStep-1",
        condition(
            WaitingConditionType::AnyCompleted,
            vec![timer("t", 60)],
            Vec::new(),
        ),
        Vec::new(),
    );
    harness.wait_on("WaitStep-2", WaitingCondition::default(), Vec::new());
    harness.start_attempt("WaitStep-2", StepMethod::Execute, 1);
    harness.complete_execute(
        "WaitStep-2",
        StepDecision {
            close_decision: Some(CloseDecision {
                close_decision_type: CloseDecisionType::GracefulComplete as i32,
                ..CloseDecision::default()
            }),
            ..StepDecision::default()
        },
    );
    assert_eq!(harness.run_record().status, FlowStatus::Running as i32);
    harness.skip("WaitStep-1", "t", None);
    assert_eq!(harness.run_record().status, FlowStatus::Running as i32);
    harness.start_attempt("WaitStep-1", StepMethod::Execute, 1);
    harness.complete_execute("WaitStep-1", StepDecision::default());
    assert_eq!(harness.run_record().status, FlowStatus::Completed as i32);
}

#[test]
fn an_empty_wait_presents_empty_condition_results_but_skip_wait_for_omits_them() {
    let mut harness = Harness::new();
    let plan_completed = harness.start_two_waiting_steps();
    assert_eq!(execute_inputs(&plan_completed)[0].condition_results, None);
    harness.wait_on("WaitStep-1", WaitingCondition::default(), Vec::new());
    harness.start_attempt("WaitStep-1", StepMethod::Execute, 1);
    let snapshot = harness
        .step("WaitStep-1")
        .invocation
        .and_then(|invocation| invocation.snapshot)
        .expect("a running attempt has a snapshot");
    assert_eq!(
        snapshot.condition_results,
        Some(ConditionResultsRecord::default())
    );
    let completed = harness.complete_execute("WaitStep-1", StepDecision::default());
    assert_eq!(
        execute_inputs(&completed)[0].condition_results,
        Some(ConditionResults::default())
    );
}

fn execute_inputs(outcome: &ApplyOutcome) -> Vec<StepMethodEventInput> {
    history_events(outcome)
        .into_iter()
        .filter_map(|payload| match payload {
            flow_history_event::Payload::StepExecuteCompleted(event) => event.input,
            _ => None,
        })
        .collect()
}

fn history_events(outcome: &ApplyOutcome) -> Vec<flow_history_event::Payload> {
    outcome
        .writes
        .iter()
        .filter_map(|write| match write {
            StateWrite::Put {
                column_family: ColumnFamily::History,
                value,
                ..
            } => HistoryRecord::decode(value.as_slice()).ok(),
            _ => None,
        })
        .flat_map(|record| record.events)
        .filter_map(|event| event.payload)
        .collect()
}
