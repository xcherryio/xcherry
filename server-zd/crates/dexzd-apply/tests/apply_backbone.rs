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
use dexzd_keyspace::row_keys::run_singleton_key;
use dexzd_keyspace::{ColumnFamily, PartitionHasher, RunId, RunTable, TenantId};
use dexzd_protocol::dex::{
    AttributeWrite, CloseDecision, CloseDecisionType, FlowAlreadyStartedOptions, FlowStartOptions,
    FlowStatus, FlowTimeoutPolicy, IdReusePolicy, InvokeExecuteMethodResponse,
    InvokeWaitForMethodResponse, RetryPolicy, SetAttributesRequest, StartFlowRequest, StepDecision,
    StepMovement, StepOptions, StopFlowRequest, StopType, Value, WorkerErrorResponse, WorkerTarget,
    value,
};
use dexzd_protocol::dexzd::v1::{
    AttributesSetCommand, CommandReply, FlowCommand, FlowStartCommand, FlowStopCommand,
    FlowTimeoutTimerId, RetryBackoffTimerId, RunRecord, RunSettings, StepAttemptFailedCommand,
    StepAttemptStartedCommand, StepInvocationKey, StepMethod, StepMethodCompletedCommand,
    TimerId, TimersFiredCommand, command_reply, flow_command::Command,
    step_method_completed_command, timer_id,
};
use prost::Message;

#[derive(Default)]
struct MemoryState {
    rows: BTreeMap<(ColumnFamily, Vec<u8>), Vec<u8>>,
}

impl FlowStateReader for MemoryState {
    fn get(&self, column_family: ColumnFamily, key: &[u8]) -> Result<Option<Vec<u8>>, StateReadError> {
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
                StateWrite::Put { column_family, key, value } => {
                    self.rows.insert((*column_family, key.clone()), value.clone());
                }
                StateWrite::Delete { column_family, key } => {
                    self.rows.remove(&(*column_family, key.clone()));
                }
                StateWrite::DeleteRange { column_family, start, end } => {
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
        let hasher = PartitionHasher::new(&[7; 16], 1024).unwrap();
        let flow_hash = hasher.flow_hash(TenantId::DEFAULT, flow_id);
        Self {
            state: MemoryState::default(),
            context: ApplyContext { partition: hasher.partition_of(flow_hash), flow_hash },
            now_micros: 1_800_000_000 * SECOND,
            flow_id: flow_id.to_owned(),
            run_id: Vec::new(),
        }
    }

    fn command(&mut self, body: Command) -> FlowCommand {
        self.now_micros += 1000;
        FlowCommand {
            flow_id: self.flow_id.clone(),
            run_id: if matches!(body, Command::FlowStart(_)) { Vec::new() } else { self.run_id.clone() },
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
        let outcome = apply_flow_command(&self.context, &self.state, &command).unwrap();
        let again = apply_flow_command(&self.context, &self.state, &command).unwrap();
        assert_eq!(outcome.writes, again.writes, "apply must be deterministic");
        self.state.commit(&outcome);
        outcome
    }

    fn start(&mut self, request: StartFlowRequest) -> ApplyOutcome {
        let new_run_id = uuid::Uuid::now_v7().as_bytes().to_vec();
        let outcome = self.apply(Command::FlowStart(FlowStartCommand {
            new_run_id: new_run_id.clone(),
            request: Some(request),
            settings: Some(settings()),
            resolved_timeout_policy: FlowTimeoutPolicy::Fail as i32,
            worker_target: Some(WorkerTarget { is_pull_mode: true, ..WorkerTarget::default() }),
            hard_delete_objects: Vec::new(),
            request_fingerprint: Vec::new(),
        }));
        if let Some(command_reply::Reply::StartFlow(reply)) = &outcome.reply.reply {
            if !reply.is_existing_run {
                self.run_id = reply.run_id.clone();
            }
        }
        outcome
    }

    fn run_record(&self) -> RunRecord {
        let run_id = RunId::from_slice(&self.run_id).unwrap();
        let key = run_singleton_key(self.context.flow_hash, run_id, RunTable::RunRecord);
        RunRecord::decode(self.state.get(ColumnFamily::Flow, &key).unwrap().unwrap().as_slice()).unwrap()
    }

    fn start_attempt(&mut self, step_execution_id: &str, method: StepMethod, attempt: u32) -> ApplyOutcome {
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
            response: Some(step_method_completed_command::Response::WaitFor(InvokeWaitForMethodResponse::default())),
            app_worker_id: "worker-a".to_owned(),
            ..StepMethodCompletedCommand::default()
        }))
    }

    fn complete_execute(&mut self, step_execution_id: &str, attempt: u32, decision: StepDecision) -> ApplyOutcome {
        self.apply(Command::StepMethodCompleted(StepMethodCompletedCommand {
            invocation: Some(key(step_execution_id, StepMethod::Execute)),
            attempt,
            response: Some(step_method_completed_command::Response::Execute(InvokeExecuteMethodResponse {
                step_decision: Some(decision),
                ..InvokeExecuteMethodResponse::default()
            })),
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
    StepInvocationKey { step_execution_id: step_execution_id.to_owned(), method: method as i32 }
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
    Value { kind: Some(value::Kind::StringValue(text.to_owned())) }
}

fn pending_invocations(outcome: &ApplyOutcome) -> Vec<(String, i32, u32)> {
    outcome
        .effects
        .iter()
        .filter_map(|effect| match effect {
            ApplyEffect::InvocationPending { invocation, attempt, .. } => {
                Some((invocation.step_execution_id.clone(), invocation.method, *attempt))
            }
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
    assert!(matches!(outcome.reply.reply, Some(command_reply::Reply::StartFlow(_))));
    assert_eq!(pending_invocations(&outcome), vec![("ChargeStep-1".to_owned(), StepMethod::WaitFor as i32, 1)]);
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
    assert!(started.effects.iter().any(|effect| matches!(effect, ApplyEffect::InvocationStarted { .. })));
    let waited = harness.complete_wait_for("ChargeStep-1", 1);
    assert_eq!(pending_invocations(&waited), vec![("ChargeStep-1".to_owned(), StepMethod::Execute as i32, 1)]);
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
    request.step_options = Some(StepOptions { skip_wait_for: true, ..StepOptions::default() });
    let started = harness.start(request);
    assert_eq!(pending_invocations(&started), vec![("ChargeStep-1".to_owned(), StepMethod::Execute as i32, 1)]);
    harness.start_attempt("ChargeStep-1", StepMethod::Execute, 1);
    let skip = Some(StepOptions { skip_wait_for: true, ..StepOptions::default() });
    let decision = StepDecision {
        next_steps: vec![
            StepMovement { step_type: "ShipStep".to_owned(), step_options: skip.clone(), ..StepMovement::default() },
            StepMovement { step_type: "ShipStep".to_owned(), step_options: skip, ..StepMovement::default() },
        ],
        ..StepDecision::default()
    };
    let outcome = harness.complete_execute("ChargeStep-1", 1, decision);
    let pending: Vec<String> = pending_invocations(&outcome).into_iter().map(|(id, _, _)| id).collect();
    assert_eq!(pending, vec!["ShipStep-1".to_owned(), "ShipStep-2".to_owned()]);
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
        wait_for_retry_policy: Some(RetryPolicy { maximum_attempts: 2, ..RetryPolicy::default() }),
        ..StepOptions::default()
    });
    harness.start(request);
    harness.start_attempt("ChargeStep-1", StepMethod::WaitFor, 1);
    let failure = |attempt| StepAttemptFailedCommand {
        invocation: Some(key("ChargeStep-1", StepMethod::WaitFor)),
        attempt,
        kind: 1,
        worker_error: Some(WorkerErrorResponse { detail: "boom".to_owned(), ..WorkerErrorResponse::default() }),
        ..StepAttemptFailedCommand::default()
    };
    let failed = harness.apply(Command::StepAttemptFailed(failure(1)));
    assert!(failed.effects.iter().any(|effect| matches!(effect, ApplyEffect::TimerArmed { .. })));
    let fired = harness.apply(Command::TimersFired(TimersFiredCommand {
        timers: vec![TimerId {
            run_attempt: 1,
            kind: Some(timer_id::Kind::RetryBackoff(RetryBackoffTimerId {
                invocation: Some(key("ChargeStep-1", StepMethod::WaitFor)),
                next_attempt: 2,
            })),
        }],
    }));
    assert_eq!(pending_invocations(&fired), vec![("ChargeStep-1".to_owned(), StepMethod::WaitFor as i32, 2)]);
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
    assert!(stale.writes.is_empty(), "a result for an attempt that never started changes nothing");
}

#[test]
fn id_reuse_policies_follow_the_go_server() {
    let mut harness = Harness::new("order-1");
    harness.start(start_request("request-1"));
    let duplicate = harness.start(start_request("request-2"));
    assert!(is_error(&duplicate.reply, 6), "DISALLOW_REUSE rejects a second run");
    let mut same_request = start_request("request-1");
    same_request.flow_start_options = Some(FlowStartOptions {
        flow_already_started_options: Some(FlowAlreadyStartedOptions { ignore_already_started_error: true }),
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
        request: Some(StopFlowRequest { stop_type: StopType::Cancel as i32, ..StopFlowRequest::default() }),
    }));
    assert!(stopped.effects.iter().any(|effect| matches!(effect, ApplyEffect::InvocationCanceled { .. })));
    assert_eq!(harness.run_record().status, FlowStatus::Canceled as i32);
}

#[test]
fn flow_timeout_fails_with_the_default_policy() {
    let mut harness = Harness::new("order-1");
    let mut request = start_request("request-1");
    request.flow_timeout_seconds = 30;
    harness.start(request);
    harness.apply(Command::TimersFired(TimersFiredCommand {
        timers: vec![TimerId { run_attempt: 1, kind: Some(timer_id::Kind::FlowTimeout(FlowTimeoutTimerId {})) }],
    }));
    let record = harness.run_record();
    assert_eq!(record.status, FlowStatus::Failed as i32);
    assert_eq!(record.close.unwrap().error_type, dexzd_protocol::dex::FlowErrorType::FlowTimeout as i32);
}

#[test]
fn large_attribute_values_go_to_the_value_table_and_reach_the_snapshot_as_references() {
    let mut harness = Harness::new("order-1");
    harness.start(start_request("request-1"));
    let large = "x".repeat(500);
    harness.apply(Command::AttributesSet(AttributesSetCommand {
        request: Some(SetAttributesRequest {
            attributes: vec![AttributeWrite { key: "report".to_owned(), value: Some(string_value(&large)), ..AttributeWrite::default() }],
            request_id: "set-1".to_owned(),
            ..SetAttributesRequest::default()
        }),
        hard_delete_objects: Vec::new(),
        request_fingerprint: vec![1],
    }));
    let value_rows = harness.state.rows.keys().filter(|(column_family, _)| *column_family == ColumnFamily::Value).count();
    assert_eq!(value_rows, 1);
    let repeated = harness.apply(Command::AttributesSet(AttributesSetCommand {
        request: Some(SetAttributesRequest { request_id: "set-1".to_owned(), ..SetAttributesRequest::default() }),
        hard_delete_objects: Vec::new(),
        request_fingerprint: vec![2],
    }));
    assert!(is_error(&repeated.reply, 3), "a reused request ID with different content is rejected");
}
