// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Effect runner tests on paused tokio time. A fake runtime applies commands with the real
//! apply on an in-memory state and feeds the committed effects back to the runner.

mod dispatch_tests;
mod hard_delete_tests;
mod outbox_and_purge_tests;
mod rebuild_tests;
mod timer_tests;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use dexzd_apply::{
    ApplyContext, ApplyOutcome, FlowStateReader, StateReadError, StateRow, StateWrite,
    apply_flow_command,
};
use dexzd_clock::WallClock;
use dexzd_keyspace::row_keys::run_singleton_key;
use dexzd_keyspace::{ColumnFamily, PartitionHasher, PartitionId, RunId, RunTable, TenantId};
use dexzd_object_store::{FaultAction, FaultInjector, InMemoryObjectStore, ObjectStoreOperation};
use dexzd_protocol::dex::{
    FlowTimeoutPolicy, InvokeExecuteMethodOutput, InvokeExecuteMethodResponse,
    InvokeWaitForMethodOutput, InvokeWaitForMethodResponse, PollWorkerTaskFrame, StartFlowRequest,
    StepMethodHeartbeat, Value, WorkerErrorResponse, WorkerTarget, invoke_execute_method_output,
    invoke_wait_for_method_output, poll_worker_task_frame, value,
};
use dexzd_protocol::dexzd::v1::{
    CommandErrorReply, CommandReply, EmptyReply, FlowCommand, FlowStartCommand, RunRecord,
    RunSettings, StepAttemptStartedCommand, StepExecutionRecord, StepInvocationKey,
    StepInvocationPhase, command_reply, flow_command,
};
use dexzd_request_validation::ValidationConfig;
use dexzd_storage::InMemoryLocalStore;
use dexzd_worker_dispatch::{DispatchEvent, DispatchOutcome};
use futures::future::BoxFuture;
use prost::Message;
use tokio::sync::mpsc;

use crate::effects::effect_runtime::{EffectRuntime, EffectRuntimeError, InvocationStartOutcome};
use crate::effects::state_reads::{RunLocation, read_step_execution};
use crate::effects::{
    EffectDependencies, EffectError, EffectRunner, EffectTasksHandle, EffectsConfig,
    HardDeleteObjects, HardDeleteUploadLedger, MethodDispatchRequest, MethodDispatchSession,
    MethodDispatchStartError, MethodDispatcher, MethodRequest, NodeMetaUploadLedger,
};
use crate::runtime::{CommitUrgency, FlowAddress, RuntimeEffectSink};

pub(super) const SECOND_MICROS: i64 = 1_000_000;
pub(super) const START_MICROS: i64 = 1_800_000_000 * SECOND_MICROS;

/// Wall clock that follows paused tokio time.
pub(super) struct TokioWallClock {
    base_micros: i64,
    start: tokio::time::Instant,
}

impl TokioWallClock {
    pub(super) fn new() -> Self {
        Self {
            base_micros: START_MICROS,
            start: tokio::time::Instant::now(),
        }
    }
}

impl WallClock for TokioWallClock {
    fn now_unix_micros(&self) -> i64 {
        self.base_micros + i64::try_from(self.start.elapsed().as_micros()).expect("fits")
    }
}

/// Rows of committed state.
#[derive(Debug, Default, Clone)]
pub(super) struct MemoryState {
    pub(super) rows: BTreeMap<(ColumnFamily, Vec<u8>), Vec<u8>>,
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
    pub(super) fn commit(&mut self, writes: &[StateWrite]) {
        for write in writes {
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

    pub(super) fn put_message(
        &mut self,
        column_family: ColumnFamily,
        key: Vec<u8>,
        row: &impl Message,
    ) {
        self.rows.insert((column_family, key), row.encode_to_vec());
    }
}

/// Replaces apply for one command: returns the reply after changing the state, or `None` to
/// let the real apply run.
pub(super) type CommandOverride =
    Box<dyn Fn(&FlowCommand, &mut MemoryState) -> Option<CommandReply> + Send + Sync>;

/// A recorded proposal.
#[derive(Debug, Clone)]
pub(super) struct RecordedCommand {
    pub(super) command: FlowCommand,
    pub(super) urgency: CommitUrgency,
    pub(super) reply: CommandReply,
}

/// Fake Flow runtime: stamps, applies with the real apply, and publishes committed effects.
pub(super) struct ApplyRuntime {
    pub(super) state: Arc<Mutex<MemoryState>>,
    hasher: PartitionHasher,
    clock: Arc<TokioWallClock>,
    sink: EffectRunner,
    last_proposed_micros: Mutex<i64>,
    pub(super) commands: Mutex<Vec<RecordedCommand>>,
    pub(super) busy_lock_keys: Mutex<BTreeSet<String>>,
    pub(super) unavailable_proposals: AtomicUsize,
    overrides: Mutex<Vec<CommandOverride>>,
}

impl ApplyRuntime {
    /// Proposes and commits one command, then publishes its effects.
    pub(super) fn commit(
        &self,
        flow: &FlowAddress,
        mut command: FlowCommand,
        urgency: CommitUrgency,
    ) -> Result<CommandReply, EffectRuntimeError> {
        if self
            .unavailable_proposals
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |budget| {
                budget.checked_sub(1)
            })
            .is_ok()
        {
            return Err(EffectRuntimeError::Unavailable {
                detail: "injected outage".to_owned(),
            });
        }
        let effects;
        let reply;
        {
            let mut last = self.last_proposed_micros.lock().expect("lock");
            *last = (*last + 1).max(self.clock.now_unix_micros());
            command.epoch = 1;
            command.behavior_version = 1;
            command.proposed_at_micros = *last;
            command.command_id = uuid::Uuid::now_v7().as_bytes().to_vec();
            let mut state = self.state.lock().expect("lock");
            let overridden = self
                .overrides
                .lock()
                .expect("lock")
                .iter()
                .find_map(|handler| handler(&command, &mut state));
            if let Some(override_reply) = overridden {
                effects = Vec::new();
                reply = override_reply;
            } else {
                let flow_hash = self.hasher.flow_hash(flow.tenant_id, &flow.flow_id);
                let context = ApplyContext {
                    partition: self.hasher.partition_of(flow_hash),
                    flow_hash,
                };
                let outcome: ApplyOutcome =
                    apply_flow_command(&context, &*state, &command).expect("apply succeeds");
                state.commit(&outcome.writes);
                effects = outcome.effects;
                reply = outcome.reply;
            }
            self.commands.lock().expect("lock").push(RecordedCommand {
                command,
                urgency,
                reply: reply.clone(),
            });
        }
        self.sink.handle(flow, &effects, true);
        Ok(reply)
    }

    pub(super) fn add_override(&self, handler: CommandOverride) {
        self.overrides.lock().expect("lock").push(handler);
    }

    /// Returns recorded commands whose body matches `predicate`.
    pub(super) fn commands_matching(
        &self,
        predicate: impl Fn(&flow_command::Command) -> bool,
    ) -> Vec<RecordedCommand> {
        self.commands
            .lock()
            .expect("lock")
            .iter()
            .filter(|recorded| recorded.command.command.as_ref().is_some_and(&predicate))
            .cloned()
            .collect()
    }

    pub(super) fn snapshot(&self) -> MemoryState {
        self.state.lock().expect("lock").clone()
    }
}

impl EffectRuntime for ApplyRuntime {
    fn propose_committed<'a>(
        &'a self,
        flow: &'a FlowAddress,
        command: FlowCommand,
        urgency: CommitUrgency,
    ) -> BoxFuture<'a, Result<CommandReply, EffectRuntimeError>> {
        Box::pin(async move { self.commit(flow, command, urgency) })
    }

    fn propose_speculative<'a>(
        &'a self,
        flow: &'a FlowAddress,
        command: FlowCommand,
        urgency: CommitUrgency,
    ) -> BoxFuture<'a, Result<CommandReply, EffectRuntimeError>> {
        Box::pin(async move { self.commit(flow, command, urgency) })
    }

    fn try_start_invocation<'a>(
        &'a self,
        flow: &'a FlowAddress,
        run_id: RunId,
        run_attempt: u32,
        invocation: &'a StepInvocationKey,
        attempt: u32,
        task_id: Vec<u8>,
    ) -> BoxFuture<'a, Result<InvocationStartOutcome, EffectRuntimeError>> {
        Box::pin(async move {
            let run = RunLocation {
                flow_hash: self.hasher.flow_hash(flow.tenant_id, &flow.flow_id),
                run_id,
            };
            let step = read_step_execution(&self.snapshot(), run, &invocation.step_execution_id)
                .expect("state reads succeed");
            let Some(record) = step.and_then(|step| step.invocation) else {
                return Ok(InvocationStartOutcome::Stale);
            };
            if record.phase != StepInvocationPhase::Pending as i32 || record.attempt != attempt {
                return Ok(InvocationStartOutcome::Stale);
            }
            let busy = self.busy_lock_keys.lock().expect("lock");
            if record.lock_keys.iter().any(|key| busy.contains(key)) {
                return Ok(InvocationStartOutcome::LocksBusy);
            }
            drop(busy);
            let command = FlowCommand {
                flow_id: flow.flow_id.clone(),
                run_id: run_id.as_bytes().to_vec(),
                expected_run_attempt: run_attempt,
                tenant_id: flow.tenant_id.0,
                command: Some(flow_command::Command::StepAttemptStarted(
                    StepAttemptStartedCommand {
                        invocation: Some(invocation.clone()),
                        attempt,
                        task_id,
                    },
                )),
                ..FlowCommand::default()
            };
            let reply = self.commit(flow, command, CommitUrgency::Prompt)?;
            Ok(if is_error_reply(&reply) {
                InvocationStartOutcome::Stale
            } else {
                InvocationStartOutcome::Started
            })
        })
    }

    fn flow_state_reader(
        &self,
        _flow: &FlowAddress,
        _include_speculative: bool,
    ) -> Result<Box<dyn FlowStateReader + Send>, EffectRuntimeError> {
        Ok(Box::new(self.snapshot()))
    }

    fn committed_state_reader(
        &self,
    ) -> Result<Box<dyn FlowStateReader + Send>, EffectRuntimeError> {
        Ok(Box::new(self.snapshot()))
    }
}

pub(super) fn is_error_reply(reply: &CommandReply) -> bool {
    matches!(reply.reply, Some(command_reply::Reply::Error(_)))
}

pub(super) fn ok_reply() -> CommandReply {
    CommandReply {
        reply: Some(command_reply::Reply::Ok(EmptyReply {})),
    }
}

pub(super) fn error_reply(message: &str) -> CommandReply {
    CommandReply {
        reply: Some(command_reply::Reply::Error(CommandErrorReply {
            grpc_code: 9,
            message: message.to_owned(),
            ..CommandErrorReply::default()
        })),
    }
}

/// One dispatch seen by the fake dispatcher.
pub(super) struct FakeDispatch {
    pub(super) request: MethodDispatchRequest,
    events: mpsc::UnboundedSender<DispatchEvent>,
    pub(super) is_cancelled: Arc<AtomicBool>,
}

impl FakeDispatch {
    pub(super) fn method_request(&self) -> &MethodRequest {
        &self.request.request
    }

    pub(super) fn context(&self) -> dexzd_protocol::dex::Context {
        match self.method_request() {
            MethodRequest::WaitFor(request) => request.context.clone(),
            MethodRequest::Execute(request) => request.context.clone(),
        }
        .expect("requests carry a Context")
    }

    pub(super) fn send_frame(&self, frame: PollWorkerTaskFrame) {
        self.events
            .send(DispatchEvent::Output(frame))
            .expect("the attempt listens");
    }

    pub(super) fn complete(&self, outcome: DispatchOutcome) {
        // The attempt may already have stopped listening (timeouts, cancellation).
        if self.events.send(DispatchEvent::Completed(outcome)).is_err() {
            tracing::debug!("attempt stopped listening");
        }
    }

    pub(super) fn complete_wait_for(&self, response: InvokeWaitForMethodResponse) {
        self.complete(DispatchOutcome::WorkerResult {
            frame: PollWorkerTaskFrame {
                task_id: "task".to_owned(),
                output: Some(poll_worker_task_frame::Output::WaitFor(
                    InvokeWaitForMethodOutput {
                        output: Some(invoke_wait_for_method_output::Output::Result(response)),
                    },
                )),
            },
            app_worker_id: "worker-a".to_owned(),
        });
    }

    pub(super) fn complete_execute(&self, response: InvokeExecuteMethodResponse) {
        self.complete(DispatchOutcome::WorkerResult {
            frame: PollWorkerTaskFrame {
                task_id: "task".to_owned(),
                output: Some(poll_worker_task_frame::Output::Execute(
                    InvokeExecuteMethodOutput {
                        output: Some(invoke_execute_method_output::Output::Result(response)),
                    },
                )),
            },
            app_worker_id: "worker-a".to_owned(),
        });
    }

    pub(super) fn fail_with_worker_error(&self, detail: &str) {
        self.complete(DispatchOutcome::WorkerError {
            frame: PollWorkerTaskFrame {
                task_id: "task".to_owned(),
                output: Some(poll_worker_task_frame::Output::WorkerError(
                    WorkerErrorResponse {
                        detail: detail.to_owned(),
                        error_type: "TestError".to_owned(),
                        ..WorkerErrorResponse::default()
                    },
                )),
            },
            app_worker_id: "worker-a".to_owned(),
        });
    }

    pub(super) fn send_wait_for_heartbeat(&self, value: Option<Value>) {
        self.send_frame(PollWorkerTaskFrame {
            task_id: "task".to_owned(),
            output: Some(poll_worker_task_frame::Output::WaitFor(
                InvokeWaitForMethodOutput {
                    output: Some(invoke_wait_for_method_output::Output::Heartbeat(
                        StepMethodHeartbeat { value },
                    )),
                },
            )),
        });
    }
}

/// Method dispatcher that hands every dispatch to the test.
pub(super) struct FakeDispatcher {
    dispatches: Mutex<mpsc::UnboundedSender<FakeDispatch>>,
    receiver: tokio::sync::Mutex<mpsc::UnboundedReceiver<FakeDispatch>>,
    pub(super) dispatch_count: AtomicUsize,
    pub(super) refuse_dispatch: AtomicBool,
}

impl FakeDispatcher {
    fn new() -> Self {
        let (sender, receiver) = mpsc::unbounded_channel();
        Self {
            dispatches: Mutex::new(sender),
            receiver: tokio::sync::Mutex::new(receiver),
            dispatch_count: AtomicUsize::new(0),
            refuse_dispatch: AtomicBool::new(false),
        }
    }

    /// Waits for the next dispatch.
    pub(super) async fn next_dispatch(&self) -> FakeDispatch {
        tokio::time::timeout(Duration::from_secs(3600), self.receiver.lock().await.recv())
            .await
            .expect("a dispatch arrives")
            .expect("the dispatcher is alive")
    }

    /// Returns a dispatch if one is waiting.
    pub(super) async fn try_next_dispatch(&self) -> Option<FakeDispatch> {
        self.receiver.lock().await.try_recv().ok()
    }
}

impl MethodDispatcher for FakeDispatcher {
    fn dispatch(
        &self,
        request: MethodDispatchRequest,
    ) -> Result<Box<dyn MethodDispatchSession>, MethodDispatchStartError> {
        self.dispatch_count.fetch_add(1, Ordering::SeqCst);
        if self.refuse_dispatch.load(Ordering::SeqCst) {
            return Err(MethodDispatchStartError {
                detail: "matching server is closing".to_owned(),
            });
        }
        let (events, receiver) = mpsc::unbounded_channel();
        let is_cancelled = Arc::new(AtomicBool::new(false));
        let dispatch = FakeDispatch {
            request,
            events,
            is_cancelled: Arc::clone(&is_cancelled),
        };
        self.dispatches
            .lock()
            .expect("lock")
            .send(dispatch)
            .expect("the test holds the receiver");
        Ok(Box::new(FakeSession {
            receiver,
            is_cancelled,
        }))
    }
}

struct FakeSession {
    receiver: mpsc::UnboundedReceiver<DispatchEvent>,
    is_cancelled: Arc<AtomicBool>,
}

impl MethodDispatchSession for FakeSession {
    fn next_event(&mut self) -> BoxFuture<'_, Option<DispatchEvent>> {
        Box::pin(self.receiver.recv())
    }

    fn cancel(&self) {
        self.is_cancelled.store(true, Ordering::SeqCst);
    }
}

/// Everything a node keeps across a restart: committed state, objects, and the local store.
#[derive(Clone)]
pub(super) struct DurableState {
    pub(super) state: Arc<Mutex<MemoryState>>,
    pub(super) objects: Arc<InMemoryObjectStore>,
    pub(super) local_store: Arc<InMemoryLocalStore>,
    pub(super) clock: Arc<TokioWallClock>,
}

impl DurableState {
    pub(super) fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(MemoryState::default())),
            objects: Arc::new(InMemoryObjectStore::new()),
            local_store: Arc::new(InMemoryLocalStore::new()),
            clock: Arc::new(TokioWallClock::new()),
        }
    }
}

/// One running node: fake runtime, effect runner, and fake Worker dispatch.
pub(super) struct EffectHarness {
    pub(super) durable: DurableState,
    pub(super) runtime: Arc<ApplyRuntime>,
    pub(super) dispatcher: Arc<FakeDispatcher>,
    pub(super) runner: EffectRunner,
    pub(super) hard_delete_objects: HardDeleteObjects,
    pub(super) hasher: PartitionHasher,
    pub(super) clock: Arc<TokioWallClock>,
    tasks: EffectTasksHandle,
}

impl EffectHarness {
    pub(super) fn new() -> Self {
        Self::with_config(DurableState::new(), EffectsConfig::default())
    }

    pub(super) fn with_config(durable: DurableState, config: EffectsConfig) -> Self {
        let ledger = Arc::new(NodeMetaUploadLedger::new(
            Arc::clone(&durable.local_store) as _,
            Arc::new(Mutex::new(())),
        ));
        Self::with_ledger(durable, config, ledger)
    }

    pub(super) fn with_ledger(
        durable: DurableState,
        config: EffectsConfig,
        ledger: Arc<dyn HardDeleteUploadLedger>,
    ) -> Self {
        let hasher = PartitionHasher::new(&[7; 16], 16).expect("valid partition hasher");
        let clock = Arc::clone(&durable.clock);
        let hard_delete_objects = HardDeleteObjects::new(
            Arc::clone(&durable.objects) as _,
            ledger,
            Arc::clone(&clock) as _,
            hasher.clone(),
        );
        let dispatcher = Arc::new(FakeDispatcher::new());
        let (runner, tasks) = EffectRunner::new(
            &config,
            EffectDependencies {
                dispatcher: Arc::clone(&dispatcher) as _,
                hard_delete_objects: hard_delete_objects.clone(),
                partition_hasher: hasher.clone(),
                clock: Arc::clone(&clock) as _,
                validation_config: ValidationConfig {
                    has_blob_store: true,
                    ..ValidationConfig::default()
                },
            },
        );
        let runtime = Arc::new(ApplyRuntime {
            state: Arc::clone(&durable.state),
            hasher: hasher.clone(),
            clock: Arc::clone(&clock),
            sink: runner.clone(),
            last_proposed_micros: Mutex::new(0),
            commands: Mutex::new(Vec::new()),
            busy_lock_keys: Mutex::new(BTreeSet::new()),
            unavailable_proposals: AtomicUsize::new(0),
            overrides: Mutex::new(Vec::new()),
        });
        let tasks = tasks.start(Arc::clone(&runtime) as _);
        Self {
            durable,
            runtime,
            dispatcher,
            runner,
            hard_delete_objects,
            hasher,
            clock,
            tasks,
        }
    }

    pub(super) fn flow(flow_id: &str) -> FlowAddress {
        FlowAddress::new(TenantId::DEFAULT, flow_id)
    }

    pub(super) fn partition_of(&self, flow_id: &str) -> PartitionId {
        self.hasher.partition_of_flow(TenantId::DEFAULT, flow_id)
    }

    pub(super) fn run_location(&self, flow_id: &str, run_id: RunId) -> RunLocation {
        RunLocation {
            flow_hash: self.hasher.flow_hash(TenantId::DEFAULT, flow_id),
            run_id,
        }
    }

    /// Starts every partition's timer service and rebuilds its work.
    pub(super) async fn rebuild_all_partitions(&self) {
        self.try_rebuild_all_partitions()
            .await
            .expect("rebuild succeeds");
    }

    /// Rebuilds every partition and returns the first failure.
    pub(super) async fn try_rebuild_all_partitions(&self) -> Result<(), EffectError> {
        for partition in 0..self.hasher.partition_count() {
            let partition = PartitionId(u16::try_from(partition).expect("fits"));
            self.runner.rebuild_partition(partition).await?;
        }
        Ok(())
    }

    /// Starts a Flow and returns its run ID.
    pub(super) fn start_flow(&self, request: StartFlowRequest, settings: RunSettings) -> RunId {
        let flow = Self::flow(&request.flow_id);
        let new_run_id = uuid::Uuid::now_v7().as_bytes().to_vec();
        let command = FlowCommand {
            flow_id: request.flow_id.clone(),
            tenant_id: 0,
            command: Some(flow_command::Command::FlowStart(FlowStartCommand {
                new_run_id: new_run_id.clone(),
                request: Some(request),
                settings: Some(settings),
                resolved_timeout_policy: FlowTimeoutPolicy::Fail as i32,
                worker_target: Some(WorkerTarget {
                    is_pull_mode: true,
                    ..WorkerTarget::default()
                }),
                ..FlowStartCommand::default()
            })),
            ..FlowCommand::default()
        };
        let reply = self
            .runtime
            .commit(&flow, command, CommitUrgency::Prompt)
            .expect("start commits");
        assert!(!is_error_reply(&reply), "start failed: {reply:?}");
        RunId::from_slice(&new_run_id).expect("16 bytes")
    }

    pub(super) fn run_record(&self, flow_id: &str, run_id: RunId) -> RunRecord {
        let run = self.run_location(flow_id, run_id);
        let key = run_singleton_key(run.flow_hash, run.run_id, RunTable::RunRecord);
        let bytes = self
            .runtime
            .snapshot()
            .get(ColumnFamily::Flow, &key)
            .expect("memory reads never fail")
            .expect("the run record exists");
        RunRecord::decode(bytes.as_slice()).expect("the run record decodes")
    }

    pub(super) fn step_execution(
        &self,
        flow_id: &str,
        run_id: RunId,
        step_execution_id: &str,
    ) -> StepExecutionRecord {
        read_step_execution(
            &self.runtime.snapshot(),
            self.run_location(flow_id, run_id),
            step_execution_id,
        )
        .expect("state reads succeed")
        .expect("the Step execution exists")
    }

    /// Lets every runnable task proceed without moving time forward much.
    pub(super) async fn settle(&self) {
        for _ in 0..50 {
            tokio::task::yield_now().await;
        }
    }

    pub(super) async fn shutdown(self) {
        self.tasks.shutdown().await;
    }
}

pub(super) fn settings() -> RunSettings {
    RunSettings {
        history_window_semantic_events: 10_000,
        history_prune_chunk_semantic_events: 1000,
        max_retained_superseded_bytes: 256 << 20,
        retention_after_close_micros: 7 * 24 * 3600 * SECOND_MICROS,
        wire_inline_threshold_bytes: 100,
        max_value_bytes: 16 << 20,
        checkpoint_min_written_bytes: 1 << 20,
        checkpoint_max_commands: 10_000,
        request_dedup_capacity: 1000,
        max_run_state_bytes: 1 << 30,
        max_active_step_executions: 10_000,
    }
}

pub(super) fn start_request(flow_id: &str) -> StartFlowRequest {
    StartFlowRequest {
        flow_id: flow_id.to_owned(),
        flow_type: "OrderFlow".to_owned(),
        start_step_type: "ChargeStep".to_owned(),
        step_input: Some(string_value("small input")),
        request_id: format!("request-{flow_id}"),
        ..StartFlowRequest::default()
    }
}

pub(super) fn string_value(text: &str) -> Value {
    Value {
        kind: Some(value::Kind::StringValue(text.to_owned())),
    }
}

pub(super) fn is_attempt_failed(command: &flow_command::Command) -> bool {
    matches!(command, flow_command::Command::StepAttemptFailed(_))
}

pub(super) fn is_method_completed(command: &flow_command::Command) -> bool {
    matches!(command, flow_command::Command::StepMethodCompleted(_))
}

pub(super) fn is_timers_fired(command: &flow_command::Command) -> bool {
    matches!(command, flow_command::Command::TimersFired(_))
}

pub(super) fn ok_override(
    predicate: impl Fn(&flow_command::Command) -> bool + Send + Sync + 'static,
) -> CommandOverride {
    Box::new(move |command, _| {
        command
            .command
            .as_ref()
            .is_some_and(&predicate)
            .then(ok_reply)
    })
}

/// Applies `action` to `operation` calls on keys under `key_prefix` while enabled.
pub(super) struct SwitchableFault {
    pub(super) is_enabled: AtomicBool,
    pub(super) operations: Vec<ObjectStoreOperation>,
    pub(super) key_prefix: &'static str,
    pub(super) action: FaultAction,
}

impl SwitchableFault {
    /// Returns a durable state whose object store consults a disabled fault.
    pub(super) fn install(
        operations: Vec<ObjectStoreOperation>,
        action: FaultAction,
    ) -> (DurableState, Arc<Self>) {
        let fault = Arc::new(Self {
            is_enabled: AtomicBool::new(false),
            operations,
            key_prefix: "hard-delete/",
            action,
        });
        let mut durable = DurableState::new();
        durable.objects = Arc::new(InMemoryObjectStore::with_injector(
            Arc::clone(&fault) as Arc<dyn FaultInjector>
        ));
        (durable, fault)
    }

    pub(super) fn set_enabled(&self, is_enabled: bool) {
        self.is_enabled.store(is_enabled, Ordering::SeqCst);
    }
}

impl FaultInjector for SwitchableFault {
    fn action(&self, operation: ObjectStoreOperation, key: &str) -> FaultAction {
        if self.is_enabled.load(Ordering::SeqCst)
            && self.operations.contains(&operation)
            && key.starts_with(self.key_prefix)
        {
            return self.action;
        }
        FaultAction::Proceed
    }
}
