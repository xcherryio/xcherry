// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Test doubles: an in-memory committed store, a fake commit pipeline, and recording sinks.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use dexzd_apply::{
    ApplyContext, ApplyEffect, ApplyError, ApplyOutcome, FlowStateReader, StateReadError, StateRow,
    StateWrite,
};
use dexzd_keyspace::{ColumnFamily, PartitionHasher, PartitionId, RunId};
use dexzd_protocol::dex::{AttributeWrite, Value, value};
use dexzd_protocol::dexzd::v1::{FlowCommand, HardDeleteObjectReference, LogPosition};
use futures::future::BoxFuture;
use prost::Message;
use tokio::sync::Notify;

use crate::CommandApplier;
use crate::runtime::tests::model_applier::ModelApplier;
use crate::runtime::{
    CommitError, CommitTicket, CommitTicketSender, CommitUrgency, CommittedCommand,
    CommittedFlowCommand, FlowAddress, FlowCommandCommitter, FlowRuntimeDependencies,
    FlowRuntimeRegistry, HardDeleteReadError, HardDeleteUploadError, HardDeleteUploadOutcome,
    HardDeleteUploads, HardDeleteValueReader, HardDeleteValueUploader, ReplayMismatch,
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
    /// Runs once, right after the next committed view captured its snapshot.
    pub(crate) view_hook: Mutex<Option<Box<dyn FnOnce() + Send>>>,
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
            view_hook: Mutex::new(None),
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
        let view = self.store.view();
        let hook = self.view_hook.lock().unwrap().take();
        if let Some(hook) = hook {
            hook();
        }
        Ok(Box::new(view))
    }

    fn check_speculative_outcome(
        &self,
        _partition: PartitionId,
        _command: &FlowCommand,
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

/// One recorded upload call: the Flow, the run, and the writes as the runtime passed them.
pub(crate) type RecordedUpload = (FlowAddress, RunId, Vec<AttributeWrite>);

/// Uploads hard-delete values into memory, replaces each with an empty value, and records every
/// finish; optionally fails, skips a write, or waits for a release signal first.
#[derive(Default)]
pub(crate) struct FakeHardDeleteUploader {
    pub(crate) upload_errors: Mutex<VecDeque<HardDeleteUploadError>>,
    pub(crate) uploads: Mutex<Vec<RecordedUpload>>,
    pub(crate) finished: Mutex<Vec<(HardDeleteUploads, HardDeleteUploadOutcome)>>,
    /// Leaves the write at this index without an object, as a faulty uploader would.
    pub(crate) skipped_write_index: Mutex<Option<usize>>,
    pub(crate) entered: Notify,
    pub(crate) gate: Mutex<Option<Arc<Notify>>>,
    next_object_number: AtomicU64,
}

impl FakeHardDeleteUploader {
    pub(crate) fn finished(&self) -> Vec<(HardDeleteUploads, HardDeleteUploadOutcome)> {
        self.finished.lock().unwrap().clone()
    }
}

impl HardDeleteValueUploader for FakeHardDeleteUploader {
    fn upload_hard_delete_values<'a>(
        &'a self,
        flow: &'a FlowAddress,
        run_id: RunId,
        writes: &'a mut [AttributeWrite],
    ) -> BoxFuture<'a, Result<HardDeleteUploads, HardDeleteUploadError>> {
        Box::pin(async move {
            self.uploads
                .lock()
                .unwrap()
                .push((flow.clone(), run_id, writes.to_vec()));
            self.entered.notify_one();
            let gate = self.gate.lock().unwrap().clone();
            if let Some(gate) = gate {
                gate.notified().await;
            }
            if let Some(error) = self.upload_errors.lock().unwrap().pop_front() {
                return Err(error);
            }
            let skipped_write_index = *self.skipped_write_index.lock().unwrap();
            let mut uploads = HardDeleteUploads::default();
            for (write_index, write) in writes.iter_mut().enumerate() {
                if !write.is_hard_delete_on_null || skipped_write_index == Some(write_index) {
                    continue;
                }
                let is_string_value =
                    match write.value.as_ref().and_then(|value| value.kind.as_ref()) {
                        Some(value::Kind::StringValue(_)) => true,
                        Some(value::Kind::ObjValue(_)) => false,
                        _ => continue,
                    };
                let number = self.next_object_number.fetch_add(1, Ordering::SeqCst) + 1;
                let object_id = format!("object-{number}");
                let size_bytes =
                    u64::try_from(write.value.as_ref().map_or(0, Message::encoded_len)).unwrap();
                uploads.object_keys.push(format!("{run_id}/{object_id}"));
                uploads.references.push(HardDeleteObjectReference {
                    write_index: u32::try_from(write_index).unwrap(),
                    object_id,
                    size_bytes,
                    is_string_value,
                });
                write.value = Some(Value { kind: None });
            }
            Ok(uploads)
        })
    }

    fn finish_hard_delete_uploads(
        &self,
        uploads: HardDeleteUploads,
        outcome: HardDeleteUploadOutcome,
    ) -> BoxFuture<'_, Result<(), HardDeleteUploadError>> {
        self.finished.lock().unwrap().push((uploads, outcome));
        Box::pin(async { Ok(()) })
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
    pub(crate) hard_delete_uploader: Arc<FakeHardDeleteUploader>,
    pub(crate) tamper: Mutex<Option<OutcomeTamper>>,
    /// Partitions whose commit failed; like the real pipeline, they persist nothing afterwards.
    failed_partitions: Mutex<BTreeSet<PartitionId>>,
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
        let hard_delete_uploader = Arc::new(FakeHardDeleteUploader::default());
        let registry = FlowRuntimeRegistry::new(
            config,
            FlowRuntimeDependencies {
                hasher: PartitionHasher::new(&[7; 16], 4).unwrap(),
                committer: committer.clone(),
                applier: applier.clone(),
                effects: effects.clone(),
                rpc_invoker: rpc_invoker.clone(),
                hard_delete_reader: hard_delete_reader.clone(),
                hard_delete_uploader: hard_delete_uploader.clone(),
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
            hard_delete_uploader,
            tamper: Mutex::new(None),
            failed_partitions: Mutex::new(BTreeSet::new()),
        }
    }

    /// Commits the oldest pending command the way the pipeline does: apply on committed
    /// state, write the store, resolve the ticket, then publish the outcome.
    pub(crate) fn commit_next(&self) -> bool {
        let Some(pending) = self.committer.take_next() else {
            return false;
        };
        if self
            .failed_partitions
            .lock()
            .unwrap()
            .contains(&pending.context.partition)
        {
            pending.sender.resolve(Err(CommitError::NodeStopping {
                detail: "partition stopped after a commit failure".to_owned(),
            }));
            return true;
        }
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

    /// Fails the oldest pending command's ticket without committing it; its partition stops.
    pub(crate) fn fail_next(&self, error: CommitError) -> bool {
        let Some(pending) = self.committer.take_next() else {
            return false;
        };
        self.stop_partition(pending.context.partition);
        pending.sender.resolve(Err(error));
        true
    }

    /// Drops the oldest pending command's ticket unresolved; its partition stops.
    pub(crate) fn drop_next(&self) -> bool {
        let Some(pending) = self.committer.take_next() else {
            return false;
        };
        self.stop_partition(pending.context.partition);
        drop(pending);
        true
    }

    fn stop_partition(&self, partition: PartitionId) {
        self.failed_partitions.lock().unwrap().insert(partition);
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
