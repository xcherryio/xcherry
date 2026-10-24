// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! End-to-end tests of the apply backbone over an in-memory state.

use std::collections::BTreeMap;

use dexzd_apply::{
    ApplyContext, ApplyEffect, ApplyOutcome, FlowStateReader, StateReadError, StateRow, StateWrite,
    apply_flow_command,
};
use dexzd_keyspace::row_keys::{
    hard_delete_current_key, hard_delete_pending_key, prefix_range, run_named_key,
    run_singleton_key, value_holder_key, value_row_key,
};
use dexzd_keyspace::{
    ColumnFamily, PartitionHasher, RunId, RunTable, TenantId, ValueId, ValueReference, run_prefix,
};
use dexzd_protocol::dex::{
    AttributeWrite, CloseDecision, CloseDecisionType, FlowAlreadyStartedOptions, FlowStartOptions,
    FlowStatus, FlowTimeoutHandlerOptions, FlowTimeoutPolicy, IdReusePolicy,
    InvokeExecuteMethodResponse, InvokeWaitForMethodResponse, Kv, RetryPolicy,
    SetAttributesRequest, StartFlowRequest, StepDecision, StepMovement, StepOptions,
    StopFlowRequest, StopType, Value, WorkerErrorResponse, WorkerTarget, flow_history_event, value,
};
use dexzd_protocol::dexzd::v1::{
    AttributeRecord, AttributesSetCommand, CommandReply, FlowCommand, FlowStartCommand,
    FlowStopCommand, FlowTimeoutTimerId, HardDeleteObjectRecord, HardDeleteObjectReference,
    HistoryRecord, RetryBackoffTimerId, RunRecord, RunSettings, StepAttemptFailedCommand,
    StepAttemptHeartbeatCommand, StepAttemptStartedCommand, StepExecutionPhase,
    StepExecutionRecord, StepInvocationKey, StepMethod, StepMethodCompletedCommand, StoredValue,
    TimerId, TimersFiredCommand, ValueBirthRecord, command_reply, flow_command::Command,
    step_attempt_heartbeat_command, step_method_completed_command, stored_value, timer_id,
};
use prost::Message;

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

const SECOND: i64 = 1_000_000;

impl Harness {
    fn new(flow_id: &str) -> Self {
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
            command_id: vec![0; 16],
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
        self.state.commit(&outcome);
        outcome
    }

    fn start(&mut self, request: StartFlowRequest) -> ApplyOutcome {
        self.start_with_timeout_policy(request, FlowTimeoutPolicy::Fail)
    }

    fn start_with_timeout_policy(
        &mut self,
        request: StartFlowRequest,
        resolved_timeout_policy: FlowTimeoutPolicy,
    ) -> ApplyOutcome {
        let new_run_id = uuid::Uuid::now_v7().as_bytes().to_vec();
        let outcome = self.apply(Command::FlowStart(FlowStartCommand {
            new_run_id: new_run_id.clone(),
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
        if let Some(command_reply::Reply::StartFlow(reply)) = &outcome.reply.reply
            && !reply.is_existing_run
        {
            self.run_id = reply.run_id.clone();
        }
        outcome
    }

    fn run_record(&self) -> RunRecord {
        let run_id = RunId::from_slice(&self.run_id).expect("the harness has a run");
        let key = run_singleton_key(self.context.flow_hash, run_id, RunTable::RunRecord);
        let bytes = self
            .state
            .get(ColumnFamily::Flow, &key)
            .expect("memory reads never fail")
            .expect("the run record exists");
        RunRecord::decode(bytes.as_slice()).expect("the run record decodes")
    }

    fn start_attempt(
        &mut self,
        step_execution_id: &str,
        method: StepMethod,
        attempt: u32,
    ) -> ApplyOutcome {
        self.apply(Command::StepAttemptStarted(StepAttemptStartedCommand {
            invocation: Some(key(step_execution_id, method)),
            attempt,
            task_id: vec![attempt as u8; 16],
        }))
    }

    fn complete_wait_for(&mut self, step_execution_id: &str, attempt: u32) -> ApplyOutcome {
        self.apply(Command::StepMethodCompleted(StepMethodCompletedCommand {
            invocation: Some(key(step_execution_id, StepMethod::WaitFor)),
            attempt,
            response: Some(step_method_completed_command::Response::WaitFor(
                InvokeWaitForMethodResponse::default(),
            )),
            app_worker_id: "worker-a".to_owned(),
            ..StepMethodCompletedCommand::default()
        }))
    }

    fn complete_execute(
        &mut self,
        step_execution_id: &str,
        attempt: u32,
        decision: StepDecision,
    ) -> ApplyOutcome {
        self.apply(Command::StepMethodCompleted(StepMethodCompletedCommand {
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
        }))
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

fn start_request(request_id: &str) -> StartFlowRequest {
    StartFlowRequest {
        flow_id: "order-1".to_owned(),
        flow_type: "OrderFlow".to_owned(),
        start_step_type: "ChargeStep".to_owned(),
        step_input: Some(string_value("small input")),
        request_id: request_id.to_owned(),
        ..StartFlowRequest::default()
    }
}

fn string_value(text: &str) -> Value {
    Value {
        kind: Some(value::Kind::StringValue(text.to_owned())),
    }
}

fn pending_invocations(outcome: &ApplyOutcome) -> Vec<(String, i32, u32)> {
    outcome
        .effects
        .iter()
        .filter_map(|effect| match effect {
            ApplyEffect::InvocationPending {
                invocation,
                attempt,
                ..
            } => Some((
                invocation.step_execution_id.clone(),
                invocation.method,
                *attempt,
            )),
            _ => None,
        })
        .collect()
}

fn is_error(reply: &CommandReply, grpc_code: i32) -> bool {
    matches!(&reply.reply, Some(command_reply::Reply::Error(error)) if error.grpc_code == grpc_code)
}

#[test]
fn start_flow_creates_the_run_and_a_pending_wait_for() {
    let mut harness = Harness::new("order-1");
    let outcome = harness.start(start_request("request-1"));
    assert!(matches!(
        outcome.reply.reply,
        Some(command_reply::Reply::StartFlow(_))
    ));
    assert_eq!(
        pending_invocations(&outcome),
        vec![("ChargeStep-1".to_owned(), StepMethod::WaitFor as i32, 1)]
    );
    let record = harness.run_record();
    assert_eq!(record.status, FlowStatus::Running as i32);
    assert_eq!(record.current_attempt, 1);
    assert_eq!(record.next_run_seq, 2);
}

#[test]
fn wait_for_then_execute_then_force_complete_closes_the_flow() {
    let mut harness = Harness::new("order-1");
    harness.start(start_request("request-1"));
    let started = harness.start_attempt("ChargeStep-1", StepMethod::WaitFor, 1);
    assert!(
        started
            .effects
            .iter()
            .any(|effect| matches!(effect, ApplyEffect::InvocationStarted { .. }))
    );
    let waited = harness.complete_wait_for("ChargeStep-1", 1);
    assert_eq!(
        pending_invocations(&waited),
        vec![("ChargeStep-1".to_owned(), StepMethod::Execute as i32, 1)]
    );
    harness.start_attempt("ChargeStep-1", StepMethod::Execute, 1);
    let decision = StepDecision {
        close_decision: Some(CloseDecision {
            close_decision_type: CloseDecisionType::ForceComplete as i32,
            close_input: Some(string_value("done")),
            ..CloseDecision::default()
        }),
        ..StepDecision::default()
    };
    harness.complete_execute("ChargeStep-1", 1, decision);
    let record = harness.run_record();
    assert_eq!(record.status, FlowStatus::Completed as i32);
    assert_eq!(record.sticky_app_worker_id, "worker-a");
    assert_eq!(record.close.unwrap().results.len(), 1);
}

#[test]
fn next_steps_get_increasing_step_numbers_and_graceful_complete_waits_for_them() {
    let mut harness = Harness::new("order-1");
    let mut request = start_request("request-1");
    request.step_options = Some(StepOptions {
        skip_wait_for: true,
        ..StepOptions::default()
    });
    let started = harness.start(request);
    assert_eq!(
        pending_invocations(&started),
        vec![("ChargeStep-1".to_owned(), StepMethod::Execute as i32, 1)]
    );
    harness.start_attempt("ChargeStep-1", StepMethod::Execute, 1);
    let skip = Some(StepOptions {
        skip_wait_for: true,
        ..StepOptions::default()
    });
    let decision = StepDecision {
        next_steps: vec![
            StepMovement {
                step_type: "ShipStep".to_owned(),
                step_options: skip.clone(),
                ..StepMovement::default()
            },
            StepMovement {
                step_type: "ShipStep".to_owned(),
                step_options: skip,
                ..StepMovement::default()
            },
        ],
        ..StepDecision::default()
    };
    let outcome = harness.complete_execute("ChargeStep-1", 1, decision);
    let pending: Vec<String> = pending_invocations(&outcome)
        .into_iter()
        .map(|(id, _, _)| id)
        .collect();
    assert_eq!(
        pending,
        vec!["ShipStep-1".to_owned(), "ShipStep-2".to_owned()]
    );
    let graceful = StepDecision {
        close_decision: Some(CloseDecision {
            close_decision_type: CloseDecisionType::GracefulComplete as i32,
            ..CloseDecision::default()
        }),
        ..StepDecision::default()
    };
    harness.start_attempt("ShipStep-1", StepMethod::Execute, 1);
    harness.complete_execute("ShipStep-1", 1, graceful.clone());
    assert_eq!(harness.run_record().status, FlowStatus::Running as i32);
    harness.start_attempt("ShipStep-2", StepMethod::Execute, 1);
    harness.complete_execute("ShipStep-2", 1, StepDecision::default());
    assert_eq!(harness.run_record().status, FlowStatus::Completed as i32);
}

#[test]
fn failed_attempts_back_off_then_exhaust_and_fail_the_flow() {
    let mut harness = Harness::new("order-1");
    let mut request = start_request("request-1");
    request.step_options = Some(StepOptions {
        wait_for_retry_policy: Some(RetryPolicy {
            maximum_attempts: 2,
            ..RetryPolicy::default()
        }),
        ..StepOptions::default()
    });
    harness.start(request);
    harness.start_attempt("ChargeStep-1", StepMethod::WaitFor, 1);
    let failure = |attempt| StepAttemptFailedCommand {
        invocation: Some(key("ChargeStep-1", StepMethod::WaitFor)),
        attempt,
        kind: 1,
        worker_error: Some(WorkerErrorResponse {
            detail: "boom".to_owned(),
            ..WorkerErrorResponse::default()
        }),
        ..StepAttemptFailedCommand::default()
    };
    let failed = harness.apply(Command::StepAttemptFailed(failure(1)));
    assert!(
        failed
            .effects
            .iter()
            .any(|effect| matches!(effect, ApplyEffect::TimerArmed { .. }))
    );
    let fired = harness.apply(Command::TimersFired(TimersFiredCommand {
        timers: vec![TimerId {
            run_attempt: 1,
            kind: Some(timer_id::Kind::RetryBackoff(RetryBackoffTimerId {
                invocation: Some(key("ChargeStep-1", StepMethod::WaitFor)),
                next_attempt: 2,
            })),
        }],
    }));
    assert_eq!(
        pending_invocations(&fired),
        vec![("ChargeStep-1".to_owned(), StepMethod::WaitFor as i32, 2)]
    );
    harness.start_attempt("ChargeStep-1", StepMethod::WaitFor, 2);
    harness.apply(Command::StepAttemptFailed(failure(2)));
    let record = harness.run_record();
    assert_eq!(record.status, FlowStatus::Failed as i32);
    assert!(record.close.unwrap().error_message.contains("boom"));
}

#[test]
fn stale_attempt_results_are_ignored_without_writes() {
    let mut harness = Harness::new("order-1");
    harness.start(start_request("request-1"));
    let stale = harness.complete_wait_for("ChargeStep-1", 1);
    assert!(
        stale.writes.is_empty(),
        "a result for an attempt that never started changes nothing"
    );
}

#[test]
fn id_reuse_policies_follow_the_go_server() {
    let mut harness = Harness::new("order-1");
    harness.start(start_request("request-1"));
    let duplicate = harness.start(start_request("request-2"));
    assert!(
        is_error(&duplicate.reply, 6),
        "DISALLOW_REUSE rejects a second run"
    );
    let mut same_request = start_request("request-1");
    same_request.flow_start_options = Some(FlowStartOptions {
        flow_already_started_options: Some(FlowAlreadyStartedOptions {
            ignore_already_started_error: true,
        }),
        ..FlowStartOptions::default()
    });
    let existing = harness.start(same_request);
    match existing.reply.reply {
        Some(command_reply::Reply::StartFlow(reply)) => {
            assert!(reply.is_existing_run);
            assert_eq!(reply.run_id, harness.run_id);
        }
        other => panic!("unexpected reply {other:?}"),
    }
    let first_run_id = harness.run_id.clone();
    let mut terminate = start_request("request-3");
    terminate.flow_start_options = Some(FlowStartOptions {
        id_reuse_policy: IdReusePolicy::AllowTerminateIfRunning as i32,
        ..FlowStartOptions::default()
    });
    harness.start(terminate);
    assert_ne!(harness.run_id, first_run_id);
    let new_run = harness.run_record();
    assert_eq!(new_run.status, FlowStatus::Running as i32);
    harness.run_id = first_run_id;
    assert_eq!(harness.run_record().status, FlowStatus::Terminated as i32);
}

#[test]
fn stop_cancels_running_attempts() {
    let mut harness = Harness::new("order-1");
    harness.start(start_request("request-1"));
    harness.start_attempt("ChargeStep-1", StepMethod::WaitFor, 1);
    let stopped = harness.apply(Command::FlowStop(FlowStopCommand {
        request: Some(StopFlowRequest {
            stop_type: StopType::Cancel as i32,
            ..StopFlowRequest::default()
        }),
    }));
    assert!(
        stopped
            .effects
            .iter()
            .any(|effect| matches!(effect, ApplyEffect::InvocationCanceled { .. }))
    );
    assert_eq!(harness.run_record().status, FlowStatus::Canceled as i32);
}

#[test]
fn flow_timeout_fails_with_the_default_policy() {
    let mut harness = Harness::new("order-1");
    let mut request = start_request("request-1");
    request.flow_timeout_seconds = 30;
    harness.start(request);
    harness.apply(Command::TimersFired(TimersFiredCommand {
        timers: vec![TimerId {
            run_attempt: 1,
            kind: Some(timer_id::Kind::FlowTimeout(FlowTimeoutTimerId {})),
        }],
    }));
    let record = harness.run_record();
    assert_eq!(record.status, FlowStatus::Failed as i32);
    assert_eq!(
        record.close.unwrap().error_type,
        dexzd_protocol::dex::FlowErrorType::FlowTimeout as i32
    );
}

#[test]
fn large_attribute_values_go_to_the_value_table_and_reach_the_snapshot_as_references() {
    let mut harness = Harness::new("order-1");
    harness.start(start_request("request-1"));
    let large = "x".repeat(500);
    harness.apply(Command::AttributesSet(AttributesSetCommand {
        request: Some(SetAttributesRequest {
            attributes: vec![AttributeWrite {
                key: "report".to_owned(),
                value: Some(string_value(&large)),
                ..AttributeWrite::default()
            }],
            request_id: "set-1".to_owned(),
            ..SetAttributesRequest::default()
        }),
        hard_delete_objects: Vec::new(),
        request_fingerprint: vec![1],
    }));
    let value_rows = harness
        .state
        .rows
        .keys()
        .filter(|(column_family, _)| *column_family == ColumnFamily::Value)
        .count();
    assert_eq!(value_rows, 1);
    let repeated = harness.apply(Command::AttributesSet(AttributesSetCommand {
        request: Some(SetAttributesRequest {
            request_id: "set-1".to_owned(),
            ..SetAttributesRequest::default()
        }),
        hard_delete_objects: Vec::new(),
        request_fingerprint: vec![2],
    }));
    assert!(
        is_error(&repeated.reply, 3),
        "a reused request ID with different content is rejected"
    );
}

fn locking_options(lock_key: &str) -> StepOptions {
    StepOptions {
        skip_wait_for: true,
        execute_lock_attribute_keys: vec![lock_key.to_owned()],
        ..StepOptions::default()
    }
}

fn is_aborted(outcome: &ApplyOutcome) -> bool {
    is_error(&outcome.reply, 10) && outcome.writes.is_empty()
}

/// Starts a Flow whose start Step fans out into three Steps that all lock `counter`.
fn start_three_locking_steps(harness: &mut Harness) {
    let mut request = start_request("request-1");
    request.step_options = Some(StepOptions {
        skip_wait_for: true,
        ..StepOptions::default()
    });
    harness.start(request);
    harness.start_attempt("ChargeStep-1", StepMethod::Execute, 1);
    let movement = || StepMovement {
        step_type: "LockStep".to_owned(),
        step_options: Some(locking_options("counter")),
        ..StepMovement::default()
    };
    harness.complete_execute(
        "ChargeStep-1",
        1,
        StepDecision {
            next_steps: vec![movement(), movement(), movement()],
            ..StepDecision::default()
        },
    );
}

#[test]
fn a_held_lock_blocks_other_attempts_and_waiters_start_in_fifo_order() {
    let mut harness = Harness::new("order-1");
    start_three_locking_steps(&mut harness);
    let first = harness.start_attempt("LockStep-1", StepMethod::Execute, 1);
    assert!(
        first
            .effects
            .iter()
            .any(|effect| matches!(effect, ApplyEffect::InvocationStarted { .. }))
    );
    assert!(
        is_aborted(&harness.start_attempt("LockStep-2", StepMethod::Execute, 1)),
        "the lock is held"
    );
    let released = harness.complete_execute("LockStep-1", 1, StepDecision::default());
    assert!(released
        .effects
        .iter()
        .any(|effect| matches!(effect, ApplyEffect::LocksReleased { lock_keys, .. } if lock_keys == &vec!["counter".to_owned()])));
    assert!(
        is_aborted(&harness.start_attempt("LockStep-3", StepMethod::Execute, 1)),
        "LockStep-2 queued first"
    );
    let second = harness.start_attempt("LockStep-2", StepMethod::Execute, 1);
    assert!(
        second
            .effects
            .iter()
            .any(|effect| matches!(effect, ApplyEffect::InvocationStarted { .. }))
    );
}

#[test]
fn a_failed_attempt_releases_its_lock_during_backoff_and_requeues_behind_waiters() {
    let mut harness = Harness::new("order-1");
    start_three_locking_steps(&mut harness);
    harness.start_attempt("LockStep-1", StepMethod::Execute, 1);
    let failed = harness.apply(Command::StepAttemptFailed(StepAttemptFailedCommand {
        invocation: Some(key("LockStep-1", StepMethod::Execute)),
        attempt: 1,
        kind: 1,
        worker_error: Some(WorkerErrorResponse {
            detail: "retry me".to_owned(),
            ..WorkerErrorResponse::default()
        }),
        ..StepAttemptFailedCommand::default()
    }));
    assert!(
        failed
            .effects
            .iter()
            .any(|effect| matches!(effect, ApplyEffect::LocksReleased { .. }))
    );
    let second = harness.start_attempt("LockStep-2", StepMethod::Execute, 1);
    assert!(
        second
            .effects
            .iter()
            .any(|effect| matches!(effect, ApplyEffect::InvocationStarted { .. })),
        "another Step takes the lock while LockStep-1 backs off"
    );
    harness.apply(Command::TimersFired(TimersFiredCommand {
        timers: vec![TimerId {
            run_attempt: 1,
            kind: Some(timer_id::Kind::RetryBackoff(RetryBackoffTimerId {
                invocation: Some(key("LockStep-1", StepMethod::Execute)),
                next_attempt: 2,
            })),
        }],
    }));
    harness.complete_execute("LockStep-2", 1, StepDecision::default());
    assert!(
        is_aborted(&harness.start_attempt("LockStep-1", StepMethod::Execute, 2)),
        "the retry queued behind LockStep-3, which became pending earlier"
    );
    let third = harness.start_attempt("LockStep-3", StepMethod::Execute, 1);
    assert!(
        third
            .effects
            .iter()
            .any(|effect| matches!(effect, ApplyEffect::InvocationStarted { .. }))
    );
}

#[test]
fn stopping_a_flow_releases_held_locks_and_waiters() {
    let mut harness = Harness::new("order-1");
    start_three_locking_steps(&mut harness);
    harness.start_attempt("LockStep-1", StepMethod::Execute, 1);
    harness.apply(Command::FlowStop(FlowStopCommand {
        request: Some(StopFlowRequest {
            stop_type: StopType::Terminate as i32,
            ..StopFlowRequest::default()
        }),
    }));
    let lock_rows = harness
        .state
        .rows
        .keys()
        .filter(|(column_family, key)| {
            *column_family == ColumnFamily::Flow
                && dexzd_keyspace::parse_run_key(key)
                    .is_ok_and(|parsed| parsed.table == RunTable::LockTable)
        })
        .count();
    assert_eq!(lock_rows, 0, "no lock row survives a closed Flow");
}

/// Value-table bookkeeping read straight from the harness state.
impl Harness {
    fn set_attribute(&mut self, attribute_key: &str, value: Value, request_id: &str) {
        self.set_attributes(
            AttributeWrite {
                key: attribute_key.to_owned(),
                value: Some(value),
                ..AttributeWrite::default()
            },
            Vec::new(),
            request_id,
        );
    }

    fn set_hard_delete_attribute(
        &mut self,
        attribute_key: &str,
        object_id: &str,
        request_id: &str,
    ) -> ApplyOutcome {
        self.set_attributes(
            AttributeWrite {
                key: attribute_key.to_owned(),
                is_hard_delete_on_null: true,
                ..AttributeWrite::default()
            },
            vec![HardDeleteObjectReference {
                write_index: 0,
                object_id: object_id.to_owned(),
                size_bytes: 500,
                is_string_value: true,
            }],
            request_id,
        )
    }

    fn set_attributes(
        &mut self,
        write: AttributeWrite,
        hard_delete_objects: Vec<HardDeleteObjectReference>,
        request_id: &str,
    ) -> ApplyOutcome {
        let outcome = self.apply(Command::AttributesSet(AttributesSetCommand {
            request: Some(SetAttributesRequest {
                attributes: vec![write],
                request_id: request_id.to_owned(),
                ..SetAttributesRequest::default()
            }),
            hard_delete_objects,
            request_fingerprint: request_id.as_bytes().to_vec(),
        }));
        assert!(
            !matches!(outcome.reply.reply, Some(command_reply::Reply::Error(_))),
            "setting Attributes succeeds: {:?}",
            outcome.reply
        );
        outcome
    }

    fn harness_run_id(&self) -> RunId {
        RunId::from_slice(&self.run_id).expect("the harness has a run")
    }

    fn decoded_row<RowMessage: Message + Default>(&self, key: &[u8]) -> Option<RowMessage> {
        self.state
            .rows
            .get(&(ColumnFamily::Flow, key.to_vec()))
            .map(|bytes| RowMessage::decode(bytes.as_slice()).expect("the row decodes"))
    }

    fn attribute(&self, attribute_key: &str) -> Option<AttributeRecord> {
        self.decoded_row(&run_named_key(
            self.context.flow_hash,
            self.harness_run_id(),
            RunTable::Attribute,
            attribute_key,
        ))
    }

    fn step_execution(&self, step_execution_id: &str) -> StepExecutionRecord {
        self.decoded_row(&run_named_key(
            self.context.flow_hash,
            self.harness_run_id(),
            RunTable::StepExecution,
            step_execution_id,
        ))
        .expect("the Step execution exists")
    }

    fn hard_delete_current_object(&self, attribute_key: &str) -> Option<HardDeleteObjectRecord> {
        self.decoded_row(&hard_delete_current_key(
            self.context.flow_hash,
            self.harness_run_id(),
            attribute_key,
        ))
    }

    fn hard_delete_pending_object(&self, object_id: &str) -> Option<HardDeleteObjectRecord> {
        self.decoded_row(&hard_delete_pending_key(
            self.context.flow_hash,
            self.harness_run_id(),
            object_id,
        ))
    }

    fn value_birth(&self, value_id: ValueId) -> ValueBirthRecord {
        self.decoded_row(&value_row_key(
            self.context.flow_hash,
            self.harness_run_id(),
            RunTable::ValueBirth,
            value_id,
        ))
        .expect("the value has a birth row")
    }

    fn holder_count(&self, value_id: ValueId) -> usize {
        let prefix = value_holder_key(self.context.flow_hash, self.harness_run_id(), value_id, &[]);
        let (start, end) = prefix_range(&prefix);
        self.state
            .rows
            .range((ColumnFamily::Flow, start)..(ColumnFamily::Flow, end))
            .count()
    }

    /// Seqs of the table G rows of the value, in key order.
    fn garbage_seqs(&self, value_id: ValueId) -> Vec<u64> {
        let prefix = run_singleton_key(
            self.context.flow_hash,
            self.harness_run_id(),
            RunTable::ValueGarbage,
        );
        let (start, end) = prefix_range(&prefix);
        let value_suffix = value_id.to_key_suffix();
        self.state
            .rows
            .range((ColumnFamily::Flow, start)..(ColumnFamily::Flow, end))
            .filter_map(|((_, key), _)| {
                let suffix = &key[prefix.len()..];
                let (seq, id) = suffix.split_at(8);
                (id == value_suffix).then(|| u64::from_be_bytes(seq.try_into().expect("8 bytes")))
            })
            .collect()
    }
}

fn table_value_id(stored: Option<&StoredValue>) -> ValueId {
    let Some(StoredValue {
        kind: Some(stored_value::Kind::TableValue(reference)),
    }) = stored
    else {
        panic!("expected a table value, got {stored:?}");
    };
    let value_id = reference.value_id.expect("table values have an ID");
    ValueId {
        run_seq: value_id.run_seq,
        index_in_command: value_id.index_in_command,
    }
}

#[test]
fn a_plain_write_over_a_hard_delete_attribute_schedules_the_object_for_deletion() {
    let mut harness = Harness::new("order-1");
    harness.start(start_request("request-1"));
    harness.set_hard_delete_attribute("document", "object-1", "set-1");
    assert!(harness.hard_delete_current_object("document").is_some());

    let outcome = harness.set_attributes(
        AttributeWrite {
            key: "document".to_owned(),
            value: Some(string_value(&"x".repeat(500))),
            ..AttributeWrite::default()
        },
        Vec::new(),
        "set-2",
    );

    assert!(outcome.effects.iter().any(|effect| matches!(
        effect,
        ApplyEffect::HardDeleteObjectsPending { object_ids, .. } if object_ids == &["object-1"]
    )));
    assert!(harness.hard_delete_current_object("document").is_none());
    assert!(harness.hard_delete_pending_object("object-1").is_some());
    let attribute = harness.attribute("document").expect("the Attribute exists");
    assert!(!attribute.is_hard_delete_on_null);
    assert_eq!(
        harness.holder_count(table_value_id(attribute.value.as_ref())),
        1
    );
}

#[test]
fn a_hard_delete_write_over_a_plain_attribute_releases_the_value() {
    let mut harness = Harness::new("order-1");
    harness.start(start_request("request-1"));
    harness.set_attribute("document", string_value(&"x".repeat(500)), "set-1");
    let value_id = table_value_id(
        harness
            .attribute("document")
            .expect("the Attribute exists")
            .value
            .as_ref(),
    );
    assert_eq!(harness.holder_count(value_id), 1);

    harness.set_hard_delete_attribute("document", "object-1", "set-2");

    assert_eq!(harness.holder_count(value_id), 0);
    let garbage_seqs = harness.garbage_seqs(value_id);
    assert_eq!(garbage_seqs.len(), 1);
    assert_eq!(harness.value_birth(value_id).garbage_seq, garbage_seqs[0]);
    assert!(harness.hard_delete_current_object("document").is_some());
}

#[test]
fn rewriting_an_attribute_with_its_own_reference_keeps_the_value_held() {
    let mut harness = Harness::new("order-1");
    harness.start(start_request("request-1"));
    harness.set_attribute("report", string_value(&"x".repeat(500)), "set-1");
    let attribute = harness.attribute("report").expect("the Attribute exists");
    let value_id = table_value_id(attribute.value.as_ref());
    let reference = ValueReference {
        run_id: harness.harness_run_id(),
        value_id,
    }
    .to_string();

    harness.set_attribute(
        "report",
        Value {
            kind: Some(value::Kind::InternalBlobIdForStringValue(reference)),
        },
        "set-2",
    );

    let rewritten = harness.attribute("report").expect("the Attribute exists");
    assert!(rewritten.written_seq > attribute.written_seq);
    assert_eq!(table_value_id(rewritten.value.as_ref()), value_id);
    assert_eq!(harness.holder_count(value_id), 1);
    assert!(harness.garbage_seqs(value_id).is_empty());
    assert_eq!(harness.value_birth(value_id).garbage_seq, 0);

    // The value enters table G exactly once when the Attribute finally lets it go.
    harness.set_attribute("report", string_value("small"), "set-3");
    assert_eq!(harness.holder_count(value_id), 0);
    let garbage_seqs = harness.garbage_seqs(value_id);
    assert_eq!(garbage_seqs.len(), 1);
    assert_eq!(harness.value_birth(value_id).garbage_seq, garbage_seqs[0]);
}

#[test]
fn exhausted_retries_release_the_heartbeat_checkpoint() {
    let mut harness = Harness::new("order-1");
    let mut request = start_request("request-1");
    request.step_options = Some(StepOptions {
        wait_for_retry_policy: Some(RetryPolicy {
            maximum_attempts: 1,
            ..RetryPolicy::default()
        }),
        ..StepOptions::default()
    });
    harness.start(request);
    harness.start_attempt("ChargeStep-1", StepMethod::WaitFor, 1);
    harness.apply(Command::StepAttemptHeartbeat(StepAttemptHeartbeatCommand {
        invocation: Some(key("ChargeStep-1", StepMethod::WaitFor)),
        attempt: 1,
        checkpoint: Some(step_attempt_heartbeat_command::Checkpoint::Value(
            string_value(&"c".repeat(500)),
        )),
    }));
    let invocation = harness
        .step_execution("ChargeStep-1")
        .invocation
        .expect("the attempt is running");
    let value_id = table_value_id(invocation.heartbeat_checkpoint.as_ref());
    assert_eq!(harness.holder_count(value_id), 1);

    harness.apply(Command::StepAttemptFailed(StepAttemptFailedCommand {
        invocation: Some(key("ChargeStep-1", StepMethod::WaitFor)),
        attempt: 1,
        kind: 1,
        worker_error: Some(WorkerErrorResponse {
            detail: "boom".to_owned(),
            ..WorkerErrorResponse::default()
        }),
        ..StepAttemptFailedCommand::default()
    }));

    assert_eq!(harness.run_record().status, FlowStatus::Failed as i32);
    assert_eq!(harness.holder_count(value_id), 0);
    assert_eq!(harness.garbage_seqs(value_id).len(), 1);
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

#[test]
fn a_result_without_a_matching_response_fails_the_step_without_retry() {
    let wrong_method =
        step_method_completed_command::Response::Execute(InvokeExecuteMethodResponse::default());
    for response in [None, Some(wrong_method)] {
        let mut harness = Harness::new("order-1");
        harness.start(start_request("request-1"));
        harness.start_attempt("ChargeStep-1", StepMethod::WaitFor, 1);

        let outcome = harness.apply(Command::StepMethodCompleted(StepMethodCompletedCommand {
            invocation: Some(key("ChargeStep-1", StepMethod::WaitFor)),
            attempt: 1,
            response,
            app_worker_id: "worker-a".to_owned(),
            ..StepMethodCompletedCommand::default()
        }));

        assert!(!is_error(&outcome.reply, 9), "{:?}", outcome.reply);
        let record = harness.run_record();
        assert_eq!(record.status, FlowStatus::Failed as i32);
        assert!(
            record
                .close
                .expect("the run is closed")
                .error_message
                .contains("does not match its running method")
        );
        let step = harness.step_execution("ChargeStep-1");
        assert_eq!(step.phase, StepExecutionPhase::Failed as i32);
        assert!(step.invocation.is_none());
        assert!(
            harness
                .complete_wait_for("ChargeStep-1", 1)
                .writes
                .is_empty(),
            "a repeated result is stale"
        );
    }
}

#[test]
fn the_timeout_handler_waits_for_its_declared_locks() {
    let mut harness = Harness::new("order-1");
    let mut request = start_request("request-1");
    request.flow_timeout_seconds = 30;
    request.step_options = Some(locking_options("counter"));
    request.flow_start_options = Some(FlowStartOptions {
        timeout_handler_options: Some(FlowTimeoutHandlerOptions {
            lock_attribute_keys: vec!["counter".to_owned()],
            ..FlowTimeoutHandlerOptions::default()
        }),
        ..FlowStartOptions::default()
    });
    harness.start_with_timeout_policy(request, FlowTimeoutPolicy::Handler);
    harness.start_attempt("ChargeStep-1", StepMethod::Execute, 1);

    let fired = harness.apply(Command::TimersFired(TimersFiredCommand {
        timers: vec![TimerId {
            run_attempt: 1,
            kind: Some(timer_id::Kind::FlowTimeout(FlowTimeoutTimerId {})),
        }],
    }));
    assert_eq!(
        pending_invocations(&fired),
        vec![(
            "sys:timeout_handler-1".to_owned(),
            StepMethod::Execute as i32,
            1
        )]
    );
    assert!(
        is_aborted(&harness.start_attempt("sys:timeout_handler-1", StepMethod::Execute, 1)),
        "ChargeStep-1 holds counter"
    );

    harness.complete_execute("ChargeStep-1", 1, StepDecision::default());
    let handler = harness.start_attempt("sys:timeout_handler-1", StepMethod::Execute, 1);
    assert!(
        !matches!(handler.reply.reply, Some(command_reply::Reply::Error(_))),
        "{:?}",
        handler.reply
    );
}

#[test]
fn an_exhausted_step_releases_its_values_and_its_failure_event_keeps_the_snapshot() {
    let mut harness = Harness::new("order-1");
    let mut request = start_request("request-1");
    request.step_input = Some(string_value(&"i".repeat(500)));
    request.flow_start_options = Some(FlowStartOptions {
        attributes: vec![AttributeWrite {
            key: "limit".to_owned(),
            value: Some(string_value("ten")),
            ..AttributeWrite::default()
        }],
        ..FlowStartOptions::default()
    });
    request.step_options = Some(StepOptions {
        execute_retry_policy: Some(RetryPolicy {
            maximum_attempts: 1,
            ..RetryPolicy::default()
        }),
        ..StepOptions::default()
    });
    harness.start(request);
    harness.start_attempt("ChargeStep-1", StepMethod::WaitFor, 1);
    harness.apply(Command::StepMethodCompleted(StepMethodCompletedCommand {
        invocation: Some(key("ChargeStep-1", StepMethod::WaitFor)),
        attempt: 1,
        response: Some(step_method_completed_command::Response::WaitFor(
            InvokeWaitForMethodResponse {
                upsert_step_exe_locals: vec![Kv {
                    key: "draft".to_owned(),
                    value: Some(string_value(&"d".repeat(500))),
                }],
                ..InvokeWaitForMethodResponse::default()
            },
        )),
        app_worker_id: "worker-a".to_owned(),
        ..StepMethodCompletedCommand::default()
    }));
    let step = harness.step_execution("ChargeStep-1");
    let input_value_id = table_value_id(step.input.as_ref());
    let local_value_id = table_value_id(step.locals[0].value.as_ref());
    // The start Step's input is also held for the whole run (plan D12).
    assert_eq!(harness.holder_count(input_value_id), 2);
    assert_eq!(harness.holder_count(local_value_id), 1);
    harness.start_attempt("ChargeStep-1", StepMethod::Execute, 1);

    let failed = harness.apply(Command::StepAttemptFailed(StepAttemptFailedCommand {
        invocation: Some(key("ChargeStep-1", StepMethod::Execute)),
        attempt: 1,
        kind: 1,
        worker_error: Some(WorkerErrorResponse {
            detail: "boom".to_owned(),
            ..WorkerErrorResponse::default()
        }),
        ..StepAttemptFailedCommand::default()
    }));

    let failure_inputs: Vec<_> = history_events(&failed)
        .into_iter()
        .filter_map(|payload| match payload {
            flow_history_event::Payload::StepExecuteFailed(event) => event.input,
            _ => None,
        })
        .collect();
    assert_eq!(failure_inputs.len(), 1);
    assert_eq!(
        failure_inputs[0].attributes,
        vec![Kv {
            key: "limit".to_owned(),
            value: Some(string_value("ten")),
        }]
    );
    let step = harness.step_execution("ChargeStep-1");
    assert_eq!(step.phase, StepExecutionPhase::Failed as i32);
    assert!(step.input.is_none() && step.locals.is_empty());
    assert_eq!(harness.holder_count(input_value_id), 1);
    assert_eq!(harness.holder_count(local_value_id), 0);
    assert_eq!(harness.run_record().status, FlowStatus::Failed as i32);
}

#[test]
fn id_reuse_records_the_previous_runs_termination_as_a_replayable_stop() {
    let mut harness = Harness::new("order-1");
    harness.start(start_request("request-1"));
    let previous_run_id = harness.harness_run_id();
    let state_before_reuse = harness.state.rows.clone();
    let mut request = start_request("request-2");
    request.flow_start_options = Some(FlowStartOptions {
        id_reuse_policy: IdReusePolicy::AllowTerminateIfRunning as i32,
        ..FlowStartOptions::default()
    });

    harness.start(request);

    let previous_run_prefix = run_prefix(harness.context.flow_hash, previous_run_id);
    let previous_run_rows = |rows: &BTreeMap<(ColumnFamily, Vec<u8>), Vec<u8>>| {
        rows.iter()
            .filter(|((_, key), _)| key.starts_with(&previous_run_prefix))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<Vec<_>>()
    };
    let history_prefix = run_singleton_key(
        harness.context.flow_hash,
        previous_run_id,
        RunTable::HistoryRecord,
    );
    let last_history_record = harness
        .state
        .rows
        .iter()
        .rfind(|((column_family, key), _)| {
            *column_family == ColumnFamily::History && key.starts_with(&history_prefix)
        })
        .map(|(_, value)| HistoryRecord::decode(value.as_slice()).expect("the record decodes"))
        .expect("the previous run has history");
    let recorded = last_history_record
        .command
        .expect("the record keeps its command");
    assert_eq!(recorded.run_id, previous_run_id.as_bytes().to_vec());
    let Some(Command::FlowStop(stop)) = &recorded.command else {
        panic!("expected a recorded stop, got {:?}", recorded.command);
    };
    assert_eq!(
        stop.request.as_ref().map(|request| request.stop_type),
        Some(StopType::Terminate as i32)
    );

    let mut replayed = MemoryState {
        rows: state_before_reuse,
    };
    let outcome =
        apply_flow_command(&harness.context, &replayed, &recorded).expect("apply succeeds");
    replayed.commit(&outcome);
    assert_eq!(
        previous_run_rows(&replayed.rows),
        previous_run_rows(&harness.state.rows),
        "replaying the recorded stop rebuilds the previous run exactly"
    );
}

#[test]
fn a_flow_without_a_start_step_type_starts_without_any_step() {
    let mut harness = Harness::new("order-1");
    let mut request = start_request("request-1");
    request.start_step_type = String::new();

    let outcome = harness.start(request);

    assert!(
        matches!(&outcome.reply.reply, Some(command_reply::Reply::StartFlow(reply)) if !reply.is_existing_run),
        "{:?}",
        outcome.reply
    );
    assert!(pending_invocations(&outcome).is_empty());
    let step_prefix = run_singleton_key(
        harness.context.flow_hash,
        harness.harness_run_id(),
        RunTable::StepExecution,
    );
    assert!(
        !harness
            .state
            .rows
            .keys()
            .any(|(_, key)| key.starts_with(&step_prefix)),
        "no Step execution exists"
    );
    assert_eq!(harness.run_record().status, FlowStatus::Running as i32);
    harness.set_attribute("status", string_value("ready"), "set-1");
}

#[test]
fn an_oversized_input_without_a_start_step_type_is_rejected() {
    let mut harness = Harness::new("order-1");
    let mut request = start_request("request-1");
    request.start_step_type = String::new();
    request.step_input = Some(string_value(&"i".repeat(17 << 20)));

    let outcome = harness.start(request);

    assert!(is_error(&outcome.reply, 3), "{:?}", outcome.reply);
    assert!(outcome.writes.is_empty());
}
