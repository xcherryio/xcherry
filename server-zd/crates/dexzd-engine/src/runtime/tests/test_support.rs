// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Test doubles: an in-memory committed store, a fake commit pipeline, and recording sinks.

use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use dexzd_apply::{
    ApplyContext, ApplyEffect, ApplyError, ApplyOutcome, FlowStateReader, StateReadError, StateRow,
    StateWrite,
};
use dexzd_keyspace::{ColumnFamily, PartitionHasher, PartitionId, RunId};
use dexzd_protocol::dex::Value;
use dexzd_protocol::dexzd::v1::{FlowCommand, HardDeleteObjectReference, LogPosition};
use futures::future::BoxFuture;
use tokio::sync::Notify;

use crate::CommandApplier;
use crate::runtime::tests::model_applier::ModelApplier;
use crate::runtime::{
    CommitError, CommitTicket, CommitTicketSender, CommitUrgency, CommittedCommand,
    CommittedFlowCommand, FlowAddress, FlowCommandCommitter, FlowRuntimeDependencies,
    FlowRuntimeRegistry, HardDeleteReadError, HardDeleteValueReader, ReplayMismatch,
    RpcInvokeError, RpcWorkerInvoker, RuntimeConfig, RuntimeEffectSink, WorkerRpcInvocation,
    WorkerRpcResult,
};

type Rows = BTreeMap<(ColumnFamily, Vec<u8>), Vec<u8>>;

/// Committed rows shared by the fake pipeline and its views.
#[derive(Clone, Default)]
pub(crate) struct MemoryStore {
    rows: Arc<Mutex<Rows>>,
}

/// A snapshot of [`MemoryStore`].
pub(crate) struct MemoryView {
    rows: Rows,
}

impl MemoryStore {
    pub(crate) fn view(&self) -> MemoryView {
        MemoryView {
            rows: self.rows.lock().unwrap().clone(),
        }
    }

    pub(crate) fn apply_writes(&self, writes: &[StateWrite]) {
        let mut rows = self.rows.lock().unwrap();
        apply_writes_to(&mut rows, writes);
    }

    pub(crate) fn put(&self, column_family: ColumnFamily, key: Vec<u8>, value: Vec<u8>) {
        self.rows
            .lock()
            .unwrap()
            .insert((column_family, key), value);
    }
}

pub(crate) fn apply_writes_to(rows: &mut Rows, writes: &[StateWrite]) {
    for write in writes {
        match write {
            StateWrite::Put {
                column_family,
                key,
                value,
            } => {
                rows.insert((*column_family, key.clone()), value.clone());
            }
            StateWrite::Delete { column_family, key } => {
                rows.remove(&(*column_family, key.clone()));
            }
            StateWrite::DeleteRange {
                column_family,
                start,
                end,
            } => {
                rows.retain(|(family, key), _| {
                    !(family == column_family && key >= start && key < end)
                });
            }
        }
    }
}

impl MemoryView {
    pub(crate) fn from_rows(rows: Rows) -> Self {
        Self { rows }
    }
}

impl FlowStateReader for MemoryView {
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

/// A submitted command waiting for the test to commit it.
pub(crate) struct PendingCommit {
    pub(crate) context: ApplyContext,
    pub(crate) command: FlowCommand,
    pub(crate) sender: CommitTicketSender,
}

/// Fake commit pipeline: stamps commands, queues them, and commits when the test says so.
pub(crate) struct FakeCommitter {
    pub(crate) store: MemoryStore,
    pending: Mutex<VecDeque<PendingCommit>>,
    pub(crate) submitted: Notify,
    next_command_number: AtomicU64,
    pub(crate) submit_errors: Mutex<VecDeque<CommitError>>,
    pub(crate) submitted_commands: Mutex<Vec<FlowCommand>>,
    pub(crate) replay_mismatch: Mutex<Option<ReplayMismatch>>,
}

impl FakeCommitter {
    fn new(store: MemoryStore) -> Self {
        Self {
            store,
            pending: Mutex::new(VecDeque::new()),
            submitted: Notify::new(),
            next_command_number: AtomicU64::new(0),
            submit_errors: Mutex::new(VecDeque::new()),
            submitted_commands: Mutex::new(Vec::new()),
            replay_mismatch: Mutex::new(None),
        }
    }

    pub(crate) fn take_next(&self) -> Option<PendingCommit> {
        self.pending.lock().unwrap().pop_front()
    }

    pub(crate) fn pending_count(&self) -> usize {
        self.pending.lock().unwrap().len()
    }
}

impl FlowCommandCommitter for FakeCommitter {
    fn submit(
        &self,
        context: ApplyContext,
        mut command: FlowCommand,
        _urgency: CommitUrgency,
    ) -> Result<(FlowCommand, CommitTicket), CommitError> {
        if let Some(error) = self.submit_errors.lock().unwrap().pop_front() {
            return Err(error);
        }
        let number = self.next_command_number.fetch_add(1, Ordering::SeqCst) + 1;
        command.epoch = 1;
        command.behavior_version = 1;
        command.proposed_at_micros = i64::try_from(number).unwrap() * 1_000_000;
        command.command_id = u128::from(number).to_be_bytes().to_vec();
        let (sender, ticket) = CommitTicket::new_pair();
        self.submitted_commands
            .lock()
            .unwrap()
            .push(command.clone());
        self.pending.lock().unwrap().push_back(PendingCommit {
            context,
            command: command.clone(),
            sender,
        });
        self.submitted.notify_one();
        Ok((command, ticket))
    }

    fn committed_view(&self) -> Result<Box<dyn FlowStateReader + Send>, CommitError> {
        Ok(Box::new(self.store.view()))
    }

    fn check_speculative_outcome(
        &self,
        _partition: PartitionId,
        _speculative: &ApplyOutcome,
        _committed: &CommittedCommand,
    ) -> Result<(), ReplayMismatch> {
        self.replay_mismatch
            .lock()
            .unwrap()
            .clone()
            .map_or(Ok(()), Err)
    }
}

/// Records every effect the runtime forwards.
#[derive(Default)]
pub(crate) struct RecordingEffectSink {
    pub(crate) calls: Mutex<Vec<(FlowAddress, Vec<ApplyEffect>, bool)>>,
}

impl RuntimeEffectSink for RecordingEffectSink {
    fn handle(&self, flow: &FlowAddress, effects: &[ApplyEffect], is_committed: bool) {
        self.calls
            .lock()
            .unwrap()
            .push((flow.clone(), effects.to_vec(), is_committed));
    }
}

/// Answers Worker RPCs from a script; optionally waits for a release signal first.
#[derive(Default)]
pub(crate) struct FakeRpcInvoker {
    pub(crate) results: Mutex<VecDeque<Result<WorkerRpcResult, RpcInvokeError>>>,
    pub(crate) invocations: Mutex<Vec<WorkerRpcInvocation>>,
    pub(crate) entered: Notify,
    pub(crate) gate: Mutex<Option<Arc<Notify>>>,
}

impl RpcWorkerInvoker for FakeRpcInvoker {
    fn invoke_worker_rpc(
        &self,
        invocation: WorkerRpcInvocation,
    ) -> BoxFuture<'_, Result<WorkerRpcResult, RpcInvokeError>> {
        Box::pin(async move {
            self.invocations.lock().unwrap().push(invocation);
            self.entered.notify_one();
            let gate = self.gate.lock().unwrap().clone();
            if let Some(gate) = gate {
                gate.notified().await;
            }
            self.results.lock().unwrap().pop_front().unwrap_or(Err(
                RpcInvokeError::NoWorkerAvailable {
                    detail: "no scripted result".to_owned(),
                },
            ))
        })
    }
}

/// Serves hard-delete values from a map keyed by object ID.
#[derive(Default)]
pub(crate) struct FakeHardDeleteReader {
    pub(crate) values: Mutex<BTreeMap<String, Value>>,
}

impl HardDeleteValueReader for FakeHardDeleteReader {
    fn read_hard_delete_value<'a>(
        &'a self,
        _flow: &'a FlowAddress,
        _run_id: RunId,
        object: &'a HardDeleteObjectReference,
    ) -> BoxFuture<'a, Result<Option<Value>, HardDeleteReadError>> {
        let value = self.values.lock().unwrap().get(&object.object_id).cloned();
        Box::pin(async move { Ok(value) })
    }
}

/// Rewrites a committed outcome, to simulate nondeterministic apply.
pub(crate) type OutcomeTamper = Box<dyn Fn(&mut ApplyOutcome) + Send + Sync>;

/// A registry wired to fakes, plus the handles tests drive.
pub(crate) struct RuntimeHarness {
    pub(crate) registry: FlowRuntimeRegistry,
    pub(crate) committer: Arc<FakeCommitter>,
    pub(crate) applier: Arc<ModelApplier>,
    pub(crate) store: MemoryStore,
    pub(crate) effects: Arc<RecordingEffectSink>,
    pub(crate) rpc_invoker: Arc<FakeRpcInvoker>,
    pub(crate) hard_delete_reader: Arc<FakeHardDeleteReader>,
    pub(crate) tamper: Mutex<Option<OutcomeTamper>>,
}

impl RuntimeHarness {
    pub(crate) fn new() -> Self {
        Self::with_config(&RuntimeConfig::default())
    }

    pub(crate) fn with_config(config: &RuntimeConfig) -> Self {
        Self::with_store(config, MemoryStore::default())
    }

    /// Builds a registry over existing committed state, as a restarted owner would.
    pub(crate) fn with_store(config: &RuntimeConfig, store: MemoryStore) -> Self {
        let committer = Arc::new(FakeCommitter::new(store.clone()));
        let applier = Arc::new(ModelApplier::default());
        let effects = Arc::new(RecordingEffectSink::default());
        let rpc_invoker = Arc::new(FakeRpcInvoker::default());
        let hard_delete_reader = Arc::new(FakeHardDeleteReader::default());
        let registry = FlowRuntimeRegistry::new(
            config,
            FlowRuntimeDependencies {
                hasher: PartitionHasher::new(&[7; 16], 4).unwrap(),
                committer: committer.clone(),
                applier: applier.clone(),
                effects: effects.clone(),
                rpc_invoker: rpc_invoker.clone(),
                hard_delete_reader: hard_delete_reader.clone(),
            },
        );
        Self {
            registry,
            committer,
            applier,
            store,
            effects,
            rpc_invoker,
            hard_delete_reader,
            tamper: Mutex::new(None),
        }
    }

    /// Commits the oldest pending command the way the pipeline does: apply on committed
    /// state, write the store, resolve the ticket, then publish the outcome.
    pub(crate) fn commit_next(&self) -> bool {
        let Some(pending) = self.committer.take_next() else {
            return false;
        };
        let view = self.store.view();
        let mut outcome = self
            .applier
            .apply(&pending.context, &view, &pending.command)
            .expect("committed apply succeeds");
        if let Some(tamper) = self.tamper.lock().unwrap().as_ref() {
            tamper(&mut outcome);
        }
        self.store.apply_writes(&outcome.writes);
        let committed = CommittedCommand {
            outcome,
            position: LogPosition {
                node_id: "node-1".to_owned(),
                incarnation: 1,
                seq: 1,
            },
        };
        pending.sender.resolve(Ok(committed.clone()));
        self.registry.apply_committed(CommittedFlowCommand {
            command: pending.command,
            context: pending.context,
            committed,
        });
        true
    }

    pub(crate) fn commit_all(&self) -> usize {
        let mut count = 0;
        while self.commit_next() {
            count += 1;
        }
        count
    }

    /// Fails the oldest pending command's ticket without committing it.
    pub(crate) fn fail_next(&self, error: CommitError) -> bool {
        let Some(pending) = self.committer.take_next() else {
            return false;
        };
        pending.sender.resolve(Err(error));
        true
    }

    /// Runs `operation` while committing every command it submits.
    pub(crate) async fn drive<T>(&self, operation: impl Future<Output = T>) -> T {
        tokio::pin!(operation);
        loop {
            tokio::select! {
                biased;
                output = &mut operation => return output,
                () = self.committer.submitted.notified() => {
                    self.commit_all();
                }
            }
        }
    }

    pub(crate) fn effect_calls(&self) -> Vec<(FlowAddress, Vec<ApplyEffect>, bool)> {
        self.effects.calls.lock().unwrap().clone()
    }

    pub(crate) fn submitted(&self) -> Vec<FlowCommand> {
        self.committer.submitted_commands.lock().unwrap().clone()
    }
}

impl Drop for RuntimeHarness {
    fn drop(&mut self) {
        // Pending tickets resolve as dropped; nothing else runs in the background.
        self.registry.close_long_polls();
    }
}

/// Builds an `ApplyError` for scripted failures.
pub(crate) fn invariant_error(detail: &str) -> ApplyError {
    ApplyError::InvariantViolation {
        detail: detail.to_owned(),
    }
}
