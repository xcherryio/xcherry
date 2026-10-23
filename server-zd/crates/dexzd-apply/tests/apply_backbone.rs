// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! End-to-end tests of the apply backbone over an in-memory state.

use std::collections::BTreeMap;

use dexzd_apply::values::load_for_wire;
use dexzd_apply::{
    ApplyContext, ApplyEffect, ApplyOutcome, FlowStateReader, StateReadError, StateRow, StateWrite,
    apply_flow_command,
};
use dexzd_keyspace::row_keys::{history_record_key, run_singleton_key};
use dexzd_keyspace::{
    ColumnFamily, PartitionHasher, RunId, RunTable, TenantId, ValueId, ValueReference,
    parse_run_key, run_prefix,
};
use dexzd_protocol::dex::{
    AttributeWrite, CloseDecision, CloseDecisionType, FlowAlreadyStartedOptions, FlowStartOptions,
    FlowStatus, FlowTimeoutPolicy, IdReusePolicy, InvokeExecuteMethodResponse,
    InvokeWaitForMethodResponse, RetryPolicy, SetAttributesRequest, StartFlowRequest, StepDecision,
    StepMovement, StepOptions, StopFlowRequest, StopType, Value, WorkerErrorResponse, WorkerTarget,
    value,
};
use dexzd_protocol::dexzd::v1::{
    AttributesSetCommand, CommandReply, FlowCommand, FlowStartCommand, FlowStopCommand,
    FlowTimeoutTimerId, RetryBackoffTimerId, RunRecord, RunSettings, RunStorageUsage,
    StepAttemptFailedCommand, StepAttemptStartedCommand, StepInvocationKey, StepMethod,
    StepMethodCompletedCommand, TimerId, TimersFiredCommand, ValueBirthRecord, ValueGarbageRecord,
    command_reply, flow_command::Command, step_method_completed_command, timer_id,
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
        if !self.run_id.is_empty() && !outcome.writes.is_empty() {
            assert_eq!(
                self.run_record().storage.expect("run storage is accounted"),
                self.recount_storage()
            );
            self.assert_value_table_invariant();
        }
        outcome
    }

    fn start(&mut self, request: StartFlowRequest) -> ApplyOutcome {
        self.start_with_settings(request, settings())
    }

    fn start_with_settings(
        &mut self,
        request: StartFlowRequest,
        run_settings: RunSettings,
    ) -> ApplyOutcome {
        let new_run_id = uuid::Uuid::now_v7().as_bytes().to_vec();
        let outcome = self.apply(Command::FlowStart(FlowStartCommand {
            new_run_id: new_run_id.clone(),
            request: Some(request),
            settings: Some(run_settings),
            resolved_timeout_policy: FlowTimeoutPolicy::Fail as i32,
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
        if !self.run_id.is_empty() && !outcome.writes.is_empty() {
            assert_eq!(
                self.run_record().storage.expect("run storage is accounted"),
                self.recount_storage()
            );
            self.assert_value_table_invariant();
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

    fn set_max_run_state_bytes(&mut self, max_run_state_bytes: u64) {
        let run_id = RunId::from_slice(&self.run_id).expect("started run");
        let key = run_singleton_key(self.context.flow_hash, run_id, RunTable::RunRecord);
        let original = self
            .state
            .rows
            .get(&(ColumnFamily::Flow, key.clone()))
            .expect("the run record exists")
            .clone();
        let mut record = RunRecord::decode(original.as_slice()).expect("valid run record");
        record
            .settings
            .as_mut()
            .expect("run settings exist")
            .max_run_state_bytes = max_run_state_bytes;
        let original_state_bytes = record
            .storage
            .expect("storage is accounted")
            .state_row_bytes;
        loop {
            let new_state_bytes =
                original_state_bytes - original.len() as u64 + record.encoded_len() as u64;
            let storage = record.storage.as_mut().expect("storage is accounted");
            if storage.state_row_bytes == new_state_bytes {
                break;
            }
            storage.state_row_bytes = new_state_bytes;
        }
        self.state
            .rows
            .insert((ColumnFamily::Flow, key), record.encode_to_vec());
        assert_eq!(
            record.storage.expect("storage is accounted"),
            self.recount_storage()
        );
    }

    fn set_attribute(
        &mut self,
        attribute_key: &str,
        value: Value,
        request_id: &str,
    ) -> ApplyOutcome {
        self.apply(Command::AttributesSet(AttributesSetCommand {
            request: Some(SetAttributesRequest {
                attributes: vec![AttributeWrite {
                    key: attribute_key.to_owned(),
                    value: Some(value),
                    ..AttributeWrite::default()
                }],
                request_id: request_id.to_owned(),
                ..SetAttributesRequest::default()
            }),
            hard_delete_objects: Vec::new(),
            request_fingerprint: request_id.as_bytes().to_vec(),
        }))
    }

    fn value_reference(&self, run_seq: u64, index_in_command: u32) -> String {
        ValueReference {
            run_id: RunId::from_slice(&self.run_id).expect("started run"),
            value_id: ValueId {
                run_seq,
                index_in_command,
            },
        }
        .to_string()
    }

    fn recount_storage(&self) -> RunStorageUsage {
        let run_id = RunId::from_slice(&self.run_id).expect("started run");
        let prefix = run_prefix(self.context.flow_hash, run_id);
        let mut usage = RunStorageUsage::default();
        let mut birth_bytes = 0;
        for ((column_family, key), value) in &self.state.rows {
            if !key.starts_with(&prefix) {
                continue;
            }
            let table = parse_run_key(key).expect("valid run key").table;
            match column_family {
                ColumnFamily::Flow => {
                    usage.state_row_bytes += (key.len() + value.len()) as u64;
                    if table == RunTable::ValueBirth {
                        birth_bytes += ValueBirthRecord::decode(value.as_slice())
                            .expect("valid birth row")
                            .size_bytes;
                    } else if table == RunTable::ValueGarbage {
                        usage.superseded_value_bytes +=
                            ValueGarbageRecord::decode(value.as_slice())
                                .expect("valid garbage row")
                                .size_bytes;
                    }
                }
                ColumnFamily::History => {
                    usage.history_bytes += (key.len() + value.len()) as u64;
                }
                _ => {}
            }
        }
        usage.live_value_bytes = birth_bytes - usage.superseded_value_bytes;
        usage
    }

    fn assert_value_table_invariant(&self) {
        let run_id = RunId::from_slice(&self.run_id).expect("started run");
        let prefix = run_prefix(self.context.flow_hash, run_id);
        let mut births = BTreeMap::new();
        let mut holders = BTreeMap::<(u64, u32), usize>::new();
        let mut garbage = BTreeMap::new();
        let mut value_rows = BTreeMap::new();
        for ((family, key), bytes) in &self.state.rows {
            if !key.starts_with(&prefix) {
                continue;
            }
            let parsed = parse_run_key(key).expect("valid run key");
            match (family, parsed.table) {
                (ColumnFamily::Flow, RunTable::ValueBirth) => {
                    let id = value_id_from_suffix(parsed.suffix);
                    births.insert(
                        id,
                        ValueBirthRecord::decode(bytes.as_slice()).expect("valid birth row"),
                    );
                }
                (ColumnFamily::Flow, RunTable::ValueHolder) => {
                    *holders
                        .entry(value_id_from_suffix(parsed.suffix))
                        .or_default() += 1;
                }
                (ColumnFamily::Flow, RunTable::ValueGarbage) => {
                    let seq = u64::from_be_bytes(parsed.suffix[..8].try_into().expect("G seq"));
                    let id = value_id_from_suffix(&parsed.suffix[8..]);
                    assert!(garbage.insert(id, seq).is_none(), "one G row per value");
                }
                (ColumnFamily::Value, RunTable::Value) => {
                    value_rows.insert(value_id_from_suffix(parsed.suffix), ());
                }
                _ => {}
            }
        }
        for (id, birth) in &births {
            assert!(value_rows.contains_key(id), "B row needs a value row");
            let holder_count = holders.remove(id).unwrap_or_default();
            let garbage_seq = garbage.remove(id);
            assert!(
                (holder_count > 0 && garbage_seq.is_none() && birth.garbage_seq == 0)
                    || (holder_count == 0
                        && garbage_seq == Some(birth.garbage_seq)
                        && birth.garbage_seq > 0),
                "each value has holders or exactly one matching G row"
            );
        }
        assert!(holders.is_empty(), "holder without B row");
        assert!(garbage.is_empty(), "G row without B row");
        assert_eq!(value_rows.len(), births.len(), "value row without B row");
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

fn value_id_from_suffix(suffix: &[u8]) -> (u64, u32) {
    (
        u64::from_be_bytes(suffix[..8].try_into().expect("value seq")),
        u32::from_be_bytes(suffix[8..12].try_into().expect("value index")),
    )
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

#[test]
fn load_blobs_reads_live_values_and_omits_invalid_or_foreign_references() {
    let mut harness = Harness::new("order-1");
    harness.start(start_request("request-1"));
    let value = string_value(&"v".repeat(500));
    harness.set_attribute("report", value.clone(), "set-1");
    let run_id = RunId::from_slice(&harness.run_id).expect("started run");
    let reference = ValueReference {
        run_id,
        value_id: ValueId {
            run_seq: 2,
            index_in_command: 0,
        },
    }
    .to_string();
    assert_eq!(
        load_for_wire(
            &harness.state,
            harness.context.flow_hash,
            run_id,
            &reference
        )
        .expect("value lookup succeeds"),
        Some(value)
    );
    assert!(
        load_for_wire(
            &harness.state,
            harness.context.flow_hash,
            run_id,
            "hard-delete/object"
        )
        .expect("invalid reference is omitted")
        .is_none()
    );
    let other_run_id = RunId::from_slice(&[8; 16]).expect("valid run ID");
    assert!(
        load_for_wire(
            &harness.state,
            harness.context.flow_hash,
            other_run_id,
            &reference,
        )
        .expect("foreign reference is omitted")
        .is_none()
    );
}

#[test]
fn superseded_values_revive_and_repeated_holder_writes_remain_live() {
    let mut harness = Harness::new("order-1");
    harness.start(start_request("request-1"));
    let first_value = string_value(&"a".repeat(500));
    let first_reference = harness.value_reference(2, 0);
    harness.set_attribute("report", first_value.clone(), "set-1");
    harness.set_attribute("report", string_value(&"b".repeat(500)), "set-2");
    assert_eq!(
        harness.run_record().storage.unwrap().superseded_value_bytes,
        first_value.encoded_len() as u64
    );
    harness.set_attribute(
        "report",
        Value {
            kind: Some(value::Kind::InternalBlobIdForStringValue(
                first_reference.clone(),
            )),
        },
        "set-3",
    );
    assert_eq!(
        load_for_wire(
            &harness.state,
            harness.context.flow_hash,
            RunId::from_slice(&harness.run_id).expect("started run"),
            &first_reference,
        )
        .expect("value lookup succeeds"),
        Some(first_value)
    );
    let superseded_after_revival = harness.run_record().storage.unwrap().superseded_value_bytes;
    harness.set_attribute(
        "report",
        Value {
            kind: Some(value::Kind::InternalBlobIdForStringValue(first_reference)),
        },
        "set-4",
    );
    assert_eq!(
        harness.run_record().storage.unwrap().superseded_value_bytes,
        superseded_after_revival
    );
    assert_eq!(
        harness.run_record().storage.unwrap(),
        harness.recount_storage()
    );
}

#[test]
fn byte_cap_advances_the_window_and_collects_old_values() {
    let mut harness = Harness::new("order-1");
    let mut run_settings = settings();
    run_settings.max_retained_superseded_bytes = 0;
    harness.start_with_settings(start_request("request-1"), run_settings);
    let first_reference = harness.value_reference(2, 0);
    harness.set_attribute("report", string_value(&"a".repeat(500)), "set-1");
    harness.set_attribute("report", string_value(&"b".repeat(500)), "set-2");
    let record = harness.run_record();
    assert!(record.window_start_seq <= 3);
    let run_id = RunId::from_slice(&harness.run_id).expect("started run");
    assert!(
        harness
            .state
            .get(
                ColumnFamily::History,
                &history_record_key(harness.context.flow_hash, run_id, 3)
            )
            .expect("memory reads never fail")
            .is_some(),
        "the current command's history remains readable"
    );
    assert!(record.storage.unwrap().superseded_value_bytes > 0);
    assert_eq!(record.storage.unwrap(), harness.recount_storage());
    assert!(
        load_for_wire(
            &harness.state,
            harness.context.flow_hash,
            run_id,
            &first_reference
        )
        .expect("window value remains readable")
        .is_some()
    );
    harness.set_attribute("report", string_value(&"c".repeat(500)), "set-3");
    let record = harness.run_record();
    assert_eq!(record.window_start_seq, 4);
    assert!(
        load_for_wire(
            &harness.state,
            harness.context.flow_hash,
            run_id,
            &first_reference,
        )
        .expect("collected reference is omitted")
        .is_none()
    );
    assert_eq!(
        harness
            .state
            .rows
            .keys()
            .filter(|(family, key)| {
                *family == ColumnFamily::Flow
                    && parse_run_key(key).is_ok_and(|parsed| parsed.table == RunTable::ValueGarbage)
            })
            .count(),
        1
    );
}

#[test]
fn repeated_overwrites_retain_values_at_the_window_boundary() {
    let mut harness = Harness::new("order-1");
    let mut run_settings = settings();
    run_settings.max_retained_superseded_bytes = 600;
    harness.start_with_settings(start_request("request-1"), run_settings);
    let first_reference = harness.value_reference(2, 0);
    let second_reference = harness.value_reference(3, 0);
    let run_id = RunId::from_slice(&harness.run_id).expect("started run");
    harness.set_attribute("report", string_value(&"a".repeat(500)), "set-1");
    harness.set_attribute("report", string_value(&"b".repeat(500)), "set-2");
    assert!(
        load_for_wire(
            &harness.state,
            harness.context.flow_hash,
            run_id,
            &first_reference,
        )
        .expect("window value remains readable")
        .is_some()
    );
    harness.set_attribute("report", string_value(&"c".repeat(500)), "set-3");
    assert_eq!(harness.run_record().window_start_seq, 4);
    assert!(
        load_for_wire(
            &harness.state,
            harness.context.flow_hash,
            run_id,
            &first_reference,
        )
        .expect("old value lookup succeeds")
        .is_none()
    );
    assert!(
        load_for_wire(
            &harness.state,
            harness.context.flow_hash,
            run_id,
            &second_reference,
        )
        .expect("boundary value remains readable")
        .is_some()
    );
    assert_eq!(
        harness
            .run_record()
            .storage
            .expect("run storage is accounted"),
        harness.recount_storage()
    );
}

#[test]
fn garbage_collection_is_bounded_and_resumes_on_the_next_command() {
    let mut harness = Harness::new("order-1");
    let mut run_settings = settings();
    run_settings.max_retained_superseded_bytes = 0;
    harness.start_with_settings(start_request("request-1"), run_settings);
    let attributes = (0..150)
        .map(|index| AttributeWrite {
            key: format!("value-{index}"),
            value: Some(string_value(&"x".repeat(150))),
            ..AttributeWrite::default()
        })
        .collect();
    harness.apply(Command::AttributesSet(AttributesSetCommand {
        request: Some(SetAttributesRequest {
            attributes,
            request_id: "set-many".to_owned(),
            ..SetAttributesRequest::default()
        }),
        request_fingerprint: vec![1],
        ..AttributesSetCommand::default()
    }));
    let attributes = (0..150)
        .map(|index| AttributeWrite {
            key: format!("value-{index}"),
            value: Some(Value { kind: None }),
            ..AttributeWrite::default()
        })
        .collect();
    harness.apply(Command::AttributesSet(AttributesSetCommand {
        request: Some(SetAttributesRequest {
            attributes,
            request_id: "delete-many".to_owned(),
            ..SetAttributesRequest::default()
        }),
        request_fingerprint: vec![2],
        ..AttributesSetCommand::default()
    }));
    assert_eq!(garbage_row_count(&harness), 150);
    harness.set_attribute("trigger", string_value("small"), "trigger-1");
    assert_eq!(garbage_row_count(&harness), 22);
    harness.set_attribute("trigger", string_value("new"), "trigger-2");
    assert_eq!(garbage_row_count(&harness), 0);
}

fn garbage_row_count(harness: &Harness) -> usize {
    harness
        .state
        .rows
        .keys()
        .filter(|(family, key)| {
            *family == ColumnFamily::Flow
                && parse_run_key(key).is_ok_and(|parsed| parsed.table == RunTable::ValueGarbage)
        })
        .count()
}

#[test]
fn running_snapshot_holds_a_superseded_value_until_the_attempt_ends() {
    let mut harness = Harness::new("order-1");
    harness.start(start_request("request-1"));
    let first_reference = harness.value_reference(2, 0);
    harness.set_attribute("report", string_value(&"a".repeat(500)), "set-1");
    harness.start_attempt("ChargeStep-1", StepMethod::WaitFor, 1);
    harness.set_attribute("report", string_value(&"b".repeat(500)), "set-2");
    assert_eq!(
        harness.run_record().storage.unwrap().superseded_value_bytes,
        0
    );
    harness.complete_wait_for("ChargeStep-1", 1);
    assert!(harness.run_record().storage.unwrap().superseded_value_bytes > 0);
    assert!(
        load_for_wire(
            &harness.state,
            harness.context.flow_hash,
            RunId::from_slice(&harness.run_id).expect("started run"),
            &first_reference,
        )
        .expect("window value remains readable")
        .is_some()
    );
    assert_eq!(
        harness.run_record().storage.unwrap(),
        harness.recount_storage()
    );
}

#[test]
fn snapshot_pin_survives_window_advance_and_collects_after_attempt_completion() {
    let mut harness = Harness::new("order-1");
    let mut run_settings = settings();
    run_settings.max_retained_superseded_bytes = 0;
    harness.start_with_settings(start_request("request-1"), run_settings);
    let first_value = string_value(&"a".repeat(500));
    let first_reference = harness.value_reference(2, 0);
    let run_id = RunId::from_slice(&harness.run_id).expect("started run");
    harness.set_attribute("report", first_value.clone(), "set-1");
    harness.start_attempt("ChargeStep-1", StepMethod::WaitFor, 1);
    harness.set_attribute("report", string_value(&"b".repeat(500)), "set-2");
    harness.set_attribute("report", string_value(&"c".repeat(500)), "set-3");
    harness.set_attribute("report", string_value(&"d".repeat(500)), "set-4");
    assert!(harness.run_record().window_start_seq > 2);
    assert_eq!(
        load_for_wire(
            &harness.state,
            harness.context.flow_hash,
            run_id,
            &first_reference
        )
        .expect("snapshot value remains readable"),
        Some(first_value)
    );
    assert_eq!(
        harness
            .state
            .rows
            .iter()
            .filter(|((family, key), _)| {
                *family == ColumnFamily::Flow
                    && parse_run_key(key).is_ok_and(|parsed| {
                        parsed.table == RunTable::ValueGarbage
                            && value_id_from_suffix(&parsed.suffix[8..]) == (2, 0)
                    })
            })
            .count(),
        0
    );
    harness.complete_wait_for("ChargeStep-1", 1);
    let end_seq = harness.run_record().next_run_seq - 1;
    assert!(harness.state.rows.keys().any(|(family, key)| {
        *family == ColumnFamily::Flow
            && parse_run_key(key).is_ok_and(|parsed| {
                parsed.table == RunTable::ValueGarbage
                    && u64::from_be_bytes(parsed.suffix[..8].try_into().expect("G seq")) == end_seq
                    && value_id_from_suffix(&parsed.suffix[8..]) == (2, 0)
            })
    }));
    assert!(
        load_for_wire(
            &harness.state,
            harness.context.flow_hash,
            run_id,
            &first_reference
        )
        .expect("newly superseded value remains readable")
        .is_some()
    );
    harness.set_attribute("report", string_value(&"e".repeat(500)), "set-5");
    assert!(harness.run_record().window_start_seq > end_seq);
    assert!(
        load_for_wire(
            &harness.state,
            harness.context.flow_hash,
            run_id,
            &first_reference
        )
        .expect("collected value is omitted")
        .is_none()
    );
}

#[test]
fn client_state_limit_rejects_writes_without_changes() {
    let mut harness = Harness::new("order-1");
    harness.start(start_request("request-1"));
    harness.set_max_run_state_bytes(1);
    let before = harness.state.rows.clone();
    let rejected = harness.set_attribute("report", string_value(&"x".repeat(9000)), "set-1");
    assert!(is_error(&rejected.reply, 8));
    assert!(rejected.writes.is_empty());
    assert_eq!(harness.state.rows, before);
}

#[test]
fn worker_state_limit_becomes_a_non_retryable_attempt_failure() {
    let mut harness = Harness::new("order-1");
    harness.start(start_request("request-1"));
    harness.start_attempt("ChargeStep-1", StepMethod::WaitFor, 1);
    harness.set_max_run_state_bytes(1);
    let outcome = harness.apply(Command::StepMethodCompleted(StepMethodCompletedCommand {
        invocation: Some(key("ChargeStep-1", StepMethod::WaitFor)),
        attempt: 1,
        response: Some(step_method_completed_command::Response::WaitFor(
            InvokeWaitForMethodResponse {
                upsert_attributes: vec![AttributeWrite {
                    key: "report".to_owned(),
                    value: Some(string_value(&"x".repeat(9000))),
                    ..AttributeWrite::default()
                }],
                ..InvokeWaitForMethodResponse::default()
            },
        )),
        ..StepMethodCompletedCommand::default()
    }));
    assert!(!is_error(&outcome.reply, 8));
    let record = harness.run_record();
    assert_eq!(record.status, FlowStatus::Failed as i32);
    assert!(record.close.unwrap().error_message.contains("run state"));
    assert_eq!(record.storage.unwrap(), harness.recount_storage());
}

#[test]
fn state_limit_does_not_drop_an_attempt_start() {
    let mut harness = Harness::new("order-1");
    harness.start(start_request("request-1"));
    harness.set_max_run_state_bytes(1);
    let started = harness.start_attempt("ChargeStep-1", StepMethod::WaitFor, 1);
    assert!(!started.writes.is_empty());
    assert!(
        started
            .effects
            .iter()
            .any(|effect| { matches!(effect, ApplyEffect::InvocationStarted { .. }) })
    );
}

#[test]
fn state_limit_allows_timers_and_stop_to_settle_a_full_run() {
    let mut timed = Harness::new("order-1");
    let mut request = start_request("request-1");
    request.flow_timeout_seconds = 30;
    timed.start(request);
    timed.set_max_run_state_bytes(1);
    let fired = timed.apply(Command::TimersFired(TimersFiredCommand {
        timers: vec![TimerId {
            run_attempt: 1,
            kind: Some(timer_id::Kind::FlowTimeout(FlowTimeoutTimerId {})),
        }],
    }));
    assert!(!fired.writes.is_empty());
    assert_eq!(timed.run_record().status, FlowStatus::Failed as i32);

    let mut stopped = Harness::new("order-1");
    stopped.start(start_request("request-1"));
    stopped.set_max_run_state_bytes(1);
    let outcome = stopped.apply(Command::FlowStop(FlowStopCommand {
        request: Some(StopFlowRequest {
            stop_type: StopType::Terminate as i32,
            ..StopFlowRequest::default()
        }),
    }));
    assert!(!outcome.writes.is_empty());
    assert_ne!(stopped.run_record().status, FlowStatus::Running as i32);
}

#[test]
fn state_limit_allows_attribute_deletion_to_release_a_value() {
    let mut harness = Harness::new("order-1");
    harness.start(start_request("request-1"));
    harness.set_attribute("report", string_value(&"x".repeat(500)), "set-1");
    harness.set_max_run_state_bytes(1);
    let deleted = harness.set_attribute("report", Value { kind: None }, "delete-1");
    assert!(!deleted.writes.is_empty());
    assert!(!is_error(&deleted.reply, 8));
}

#[test]
fn value_size_limit_rejects_client_writes_and_fails_worker_attempts() {
    let mut client_harness = Harness::new("order-1");
    let mut run_settings = settings();
    run_settings.max_value_bytes = 100;
    client_harness.start_with_settings(start_request("request-1"), run_settings);
    let rejected = client_harness.set_attribute("report", string_value(&"x".repeat(500)), "set-1");
    assert!(is_error(&rejected.reply, 3));
    assert!(rejected.writes.is_empty());

    let mut worker_harness = Harness::new("order-1");
    worker_harness.start_with_settings(start_request("request-1"), run_settings);
    worker_harness.start_attempt("ChargeStep-1", StepMethod::WaitFor, 1);
    worker_harness.apply(Command::StepMethodCompleted(StepMethodCompletedCommand {
        invocation: Some(key("ChargeStep-1", StepMethod::WaitFor)),
        attempt: 1,
        response: Some(step_method_completed_command::Response::WaitFor(
            InvokeWaitForMethodResponse {
                upsert_attributes: vec![AttributeWrite {
                    key: "report".to_owned(),
                    value: Some(string_value(&"x".repeat(500))),
                    ..AttributeWrite::default()
                }],
                ..InvokeWaitForMethodResponse::default()
            },
        )),
        ..StepMethodCompletedCommand::default()
    }));
    let record = worker_harness.run_record();
    assert_eq!(record.status, FlowStatus::Failed as i32);
    assert!(
        record
            .close
            .expect("failure has close details")
            .error_message
            .contains("value")
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
