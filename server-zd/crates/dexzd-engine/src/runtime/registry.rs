// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! The node's Flow runtimes: proposals through the speculative overlay, committed outcomes,
//! the SYNC barrier, and lock checks.

mod effect_runtime_adapter;

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use dexzd_apply::{ApplyContext, ApplyEffect, ApplyOutcome, FlowStateReader};
use dexzd_keyspace::{PartitionHasher, RunId, TenantId};
use dexzd_protocol::dexzd::v1::{
    FlowCommand, StepInvocationKey, flow_command, inbox_message_command,
};
use prost::Message;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use crate::CommandApplier;
use crate::engine_id_source::EngineIdSource;
use crate::long_poll::WaiterRegistry;
use crate::runtime::flow_runtime::{FlowRuntime, FlowRuntimeState};
use crate::runtime::rpc_locks::{
    LockBlocker, RpcLockTable, find_invocation_lock_blocker, find_rpc_lock_blocker,
};
use crate::runtime::speculative_overlay::{CommittedMatch, OverlayEntry, OwnedOverlayView};
use crate::runtime::{
    CommitTicket, CommitUrgency, CommittedFlowCommand, FlowAddress, FlowCommandCommitter,
    HardDeleteValueReader, HardDeleteValueUploader, RpcWorkerInvoker, RuntimeConfig,
    RuntimeEffectSink, RuntimeError, grpc_code,
};

/// Components the runtime calls.
pub struct FlowRuntimeDependencies {
    /// Maps Flows to hashes and partitions; built from the cluster object.
    pub hasher: PartitionHasher,
    /// The commit pipeline.
    pub committer: Arc<dyn FlowCommandCommitter>,
    /// Apply, used for the speculative overlay.
    pub applier: Arc<dyn CommandApplier>,
    /// Receives effects of committed and speculative ASYNC outcomes.
    pub effects: Arc<dyn RuntimeEffectSink>,
    /// Calls RPC handlers on Workers.
    pub rpc_invoker: Arc<dyn RpcWorkerInvoker>,
    /// Reads hard-delete Attribute values.
    pub hard_delete_reader: Arc<dyn HardDeleteValueReader>,
    /// Uploads hard-delete Attribute values before their commands are proposed.
    pub hard_delete_uploader: Arc<dyn HardDeleteValueUploader>,
    /// Creates run IDs and Channel message IDs.
    pub id_source: Arc<dyn EngineIdSource>,
}

/// A pending method call that waits for its locks (plan 8.3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingInvocation {
    /// Run of the call.
    pub run_id: RunId,
    /// The call.
    pub invocation: StepInvocationKey,
    /// Lock keys the call needs.
    pub lock_keys: Vec<String>,
    /// Seq at which the attempt became pending; orders the per-key FIFO.
    pub pending_since_seq: u64,
}

/// Outcome of a proposal.
#[derive(Debug)]
pub struct ProposalResult {
    /// The outcome on the speculative overlay; equal to the committed one unless the node halts.
    pub speculative: ApplyOutcome,
    /// Resolves once the command is committed and applied to the local store. Wait for it with
    /// [`FlowRuntimeRegistry::wait_for_commit`], which halts the registry when the commit fails.
    pub ticket: CommitTicket,
}

/// Every Flow runtime of the node. Runtimes are created on demand and reclaimed when idle.
///
/// The registry spawns no tasks: proposals, waits, and long polls run in their callers'
/// futures, and committed outcomes run in the commit pipeline's call.
#[derive(Clone)]
pub struct FlowRuntimeRegistry {
    pub(crate) inner: Arc<RegistryInner>,
}

pub(crate) struct RegistryInner {
    pub(crate) config: RuntimeConfig,
    pub(crate) dependencies: FlowRuntimeDependencies,
    pub(crate) waiters: WaiterRegistry,
    runtimes: Mutex<BTreeMap<FlowAddress, Arc<FlowRuntime>>>,
    uncommitted_bytes: AtomicUsize,
    capacity_released: Notify,
    halted: CancellationToken,
    halt_reason: Mutex<Option<String>>,
}

/// Keeps a Flow runtime alive; the runtime is reclaimed when the last lease drops while idle.
pub(crate) struct FlowRuntimeLease {
    inner: Arc<RegistryInner>,
    runtime: Option<Arc<FlowRuntime>>,
}

impl FlowRuntimeRegistry {
    /// Creates a registry with no Flow runtimes.
    pub fn new(config: &RuntimeConfig, dependencies: FlowRuntimeDependencies) -> Self {
        Self {
            inner: Arc::new(RegistryInner {
                config: config.clone(),
                waiters: WaiterRegistry::new(
                    config.max_long_poll_waiters_per_node,
                    config.max_long_poll_waiters_per_flow,
                ),
                dependencies,
                runtimes: Mutex::new(BTreeMap::new()),
                uncommitted_bytes: AtomicUsize::new(0),
                capacity_released: Notify::new(),
                halted: CancellationToken::new(),
                halt_reason: Mutex::new(None),
            }),
        }
    }

    /// Serializes per Flow: applies the stamped command to the speculative overlay, submits
    /// it, and returns the speculative outcome plus the commit ticket.
    ///
    /// Waits up to `speculation_capacity_wait` while the Flow or the node is at its speculation
    /// limit. Returns `Unavailable` when capacity does not free up or the pipeline pushes back,
    /// and `NodeStopping` after the registry halted.
    pub async fn propose(
        &self,
        flow: FlowAddress,
        command: FlowCommand,
        urgency: CommitUrgency,
    ) -> Result<ProposalResult, RuntimeError> {
        let proposal = self
            .propose_with(&flow, urgency, |_| Ok(Some((command.clone(), ()))))
            .await?;
        Ok(proposal.expect("the builder always proposes").0)
    }

    /// Waits until every command this Flow proposed before the call has settled (committed; a
    /// failed commit halts the registry instead). Dispatch calls it before a SYNC invocation
    /// (plan 8.2, invariant A4).
    ///
    /// Returns `NodeStopping` when the registry halts while waiting.
    pub async fn sync_barrier(&self, flow: &FlowAddress) -> Result<(), RuntimeError> {
        self.check_running()?;
        let lease = self.inner.lease_existing(flow);
        let Some(runtime) = lease.as_ref().map(FlowRuntimeLease::runtime) else {
            return Ok(());
        };
        let target = runtime.lock_state().proposed_count;
        let mut settled = runtime.subscribe_settled();
        // Biased for reproducible simulation; the halt fires once, so the barrier cannot starve.
        tokio::select! {
            biased;
            () = self.inner.halted.cancelled() => Err(self.inner.halted_error()),
            result = settled.wait_for(|settled_count| *settled_count >= target) => {
                result.map(|_| ()).map_err(|_| RuntimeError::NodeStopping {
                    detail: "Flow runtime dropped while waiting for the SYNC barrier".to_owned(),
                })
            }
        }
    }

    /// Reports whether a pending invocation may start now: every lock key is free in the
    /// speculative lock table and in the RPC locks, and no earlier waiter queues on any of its
    /// keys (plan 8.3). Prefer [`Self::propose_invocation_start`], which checks and proposes
    /// atomically.
    ///
    /// Returns an error when state cannot be read.
    pub fn can_start_invocation(
        &self,
        flow: &FlowAddress,
        pending: &PendingInvocation,
    ) -> Result<bool, RuntimeError> {
        self.check_running()?;
        let lease = self.inner.lease(flow);
        let runtime = lease.runtime();
        let state = runtime.lock_state();
        // Snapshot under the Flow lock: a commit cannot drop its overlay entry in between.
        let committed = self.inner.dependencies.committer.committed_view()?;
        let reader = state.overlay.reader(committed.as_ref());
        let blocker = find_invocation_lock_blocker(
            &reader,
            &state.rpc_locks,
            runtime.context.flow_hash,
            pending.run_id,
            &pending.invocation,
            &pending.lock_keys,
            pending.pending_since_seq,
        )?;
        Ok(blocker.is_none())
    }

    /// Proposes `command` (a `StepAttemptStarted`) only when the invocation's locks are free,
    /// checking and proposing under the Flow's lock so no RPC can take a key in between.
    ///
    /// Returns `None` when a lock key is held or an earlier waiter queues on it.
    pub async fn propose_invocation_start(
        &self,
        flow: &FlowAddress,
        pending: &PendingInvocation,
        command: FlowCommand,
        urgency: CommitUrgency,
    ) -> Result<Option<ProposalResult>, RuntimeError> {
        let proposal = self
            .propose_with(flow, urgency, |scope| {
                let blocker = find_invocation_lock_blocker(
                    scope.state,
                    scope.rpc_locks,
                    scope.context.flow_hash,
                    pending.run_id,
                    &pending.invocation,
                    &pending.lock_keys,
                    pending.pending_since_seq,
                )?;
                if blocker.is_some() {
                    return Ok(None);
                }
                Ok(Some((command.clone(), ())))
            })
            .await?;
        Ok(proposal.map(|(result, ())| result))
    }

    /// Receives one committed command from the commit pipeline, in log order per partition.
    ///
    /// Drops the command's overlay entry and compares its speculative outcome with the
    /// committed one byte for byte; a mismatch halts the registry. Then forwards the committed
    /// effects and wakes long-poll waiters. Replayed commands, which have no overlay entry, only
    /// wake waiters: partition rebuild owns their effects. Never blocks on I/O.
    pub fn apply_committed(&self, committed: CommittedFlowCommand) {
        if self.inner.halted.is_cancelled() {
            return;
        }
        let flow = FlowAddress::new(
            TenantId(committed.command.tenant_id),
            committed.command.flow_id.clone(),
        );
        let outcome = &committed.committed.outcome;
        let mut speculative = None;
        if let Some(lease) = self.inner.lease_existing(&flow) {
            let runtime = lease.runtime();
            let mut state = runtime.lock_state();
            let mut is_out_of_order = false;
            match state.overlay.take_committed(&committed.command.command_id) {
                CommittedMatch::Head(entry) => {
                    self.inner.release_bytes(entry.size_bytes);
                    state.record_settled(1);
                    speculative = Some(entry.outcome);
                }
                CommittedMatch::Foreign => {}
                CommittedMatch::OutOfOrder => is_out_of_order = true,
            }
            runtime.publish_settled(&state);
            drop(state);
            if is_out_of_order {
                self.halt(format!(
                    "commands of Flow {} committed out of proposal order",
                    flow.flow_id
                ));
                return;
            }
        }
        // Commands this registry never proposed are replays; partition rebuild owns their effects.
        if let Some(speculative) = speculative {
            if !is_same_outcome(&speculative, outcome) {
                self.halt(format!(
                    "speculative and committed outcomes differ for Flow {}",
                    flow.flow_id
                ));
                return;
            }
            if let Err(mismatch) = self.inner.dependencies.committer.check_speculative_outcome(
                committed.context.partition,
                &speculative,
                &committed.committed,
            ) {
                self.halt(mismatch.to_string());
                return;
            }
            self.inner
                .dependencies
                .effects
                .handle(&flow, &outcome.effects, true);
        }
        for effect in &outcome.effects {
            if let ApplyEffect::WakeWaiters { change, .. } = effect {
                self.inner.waiters.wake(&flow, change);
            }
        }
    }

    /// Stops serving: every pending and future operation fails with `NodeStopping`, and every
    /// long poll ends. The node owner watches [`Self::halted`] and exits.
    pub fn halt(&self, reason: String) {
        self.inner.halt(reason);
    }

    /// Ends every long poll with `Unavailable` and rejects new ones, for a graceful stop.
    pub fn close_long_polls(&self) {
        self.inner.waiters.close();
    }

    /// Returns a token that is cancelled when the registry halts.
    pub fn halted(&self) -> CancellationToken {
        self.inner.halted.clone()
    }

    /// Returns the number of live Flow runtimes.
    pub fn runtime_count(&self) -> usize {
        self.inner.lock_runtimes().len()
    }

    /// Returns the uncommitted bytes held by every overlay of the node.
    pub fn uncommitted_bytes(&self) -> usize {
        self.inner.uncommitted_bytes.load(Ordering::Acquire)
    }

    /// Returns the number of registered long-poll waiters.
    pub fn long_poll_waiter_count(&self) -> usize {
        self.inner.waiters.waiter_count()
    }

    /// Builds, submits, and speculatively applies a command in one critical section of the Flow's lock.
    ///
    /// `build` runs again after each wait for capacity, against the then-current state.
    pub(crate) async fn propose_with<T>(
        &self,
        flow: &FlowAddress,
        urgency: CommitUrgency,
        mut build: impl FnMut(&mut ProposalScope<'_>) -> Result<Option<(FlowCommand, T)>, RuntimeError>,
    ) -> Result<Option<(ProposalResult, T)>, RuntimeError> {
        let lease = self.inner.lease(flow);
        let runtime = lease.runtime();
        let deadline = tokio::time::Instant::now() + self.inner.config.speculation_capacity_wait;
        loop {
            self.check_running()?;
            let capacity_released = self.inner.capacity_released.notified();
            tokio::pin!(capacity_released);
            capacity_released.as_mut().enable();
            let admission = {
                let mut state = runtime.lock_state();
                self.propose_locked(runtime, &mut state, urgency, &mut build)
            };
            let proposal = match admission {
                Ok(Admission::AtCapacity) => None,
                Ok(Admission::Decided(decided)) => Some(Ok(decided)),
                Err(error) => Some(Err(error)),
            };
            if let Some(proposal) = proposal {
                if let Ok(Some((result, _))) = &proposal {
                    self.observe_commit_failure(flow, &result.ticket);
                }
                if let Ok(Some((result, _))) = &proposal
                    && urgency == CommitUrgency::Batched
                {
                    self.inner.dependencies.effects.handle(
                        flow,
                        &result.speculative.effects,
                        false,
                    );
                }
                return proposal;
            }
            // Biased for reproducible simulation; the halt and the release fire once, so the
            // deadline cannot starve.
            tokio::select! {
                biased;
                () = self.inner.halted.cancelled() => return Err(self.inner.halted_error()),
                () = &mut capacity_released => {}
                () = tokio::time::sleep_until(deadline) => {
                    return Err(RuntimeError::Unavailable {
                        detail: format!(
                            "speculation limit reached for Flow {}; commits are not catching up",
                            flow.flow_id
                        ),
                    });
                }
            }
        }
    }

    /// Halts the registry when the ticket's commit fails, whether or not a caller waits for it.
    fn observe_commit_failure(&self, flow: &FlowAddress, ticket: &CommitTicket) {
        let inner = Arc::downgrade(&self.inner);
        let flow_id = flow.flow_id.clone();
        ticket.observe_failure(Box::new(move |error| {
            // A registry that is already gone serves nothing.
            if let Some(inner) = inner.upgrade() {
                inner.halt(format!(
                    "commit of a command of Flow {flow_id} failed: {error}"
                ));
            }
        }));
    }

    /// Returns [`Admission::AtCapacity`] when the Flow or the node is at its speculation limit;
    /// nothing is reserved or submitted then.
    fn propose_locked<T>(
        &self,
        runtime: &FlowRuntime,
        state: &mut FlowRuntimeState,
        urgency: CommitUrgency,
        build: &mut impl FnMut(&mut ProposalScope<'_>) -> Result<Option<(FlowCommand, T)>, RuntimeError>,
    ) -> Result<Admission<T>, RuntimeError> {
        if state.overlay.len() >= self.inner.config.max_speculative_commands_per_flow {
            return Ok(Admission::AtCapacity);
        }
        let dependencies = &self.inner.dependencies;
        let committed = dependencies.committer.committed_view()?;
        let built = {
            let reader = state.overlay.reader(committed.as_ref());
            build(&mut ProposalScope {
                state: &reader,
                rpc_locks: &mut state.rpc_locks,
                context: runtime.context,
            })?
        };
        let Some((command, extra)) = built else {
            return Ok(Admission::Decided(None));
        };
        let command = self.resolve_child_start(command);
        self.check_running()?;
        // Reserve before submit, which cannot be undone; the outcome's size is added after apply.
        let reserved_bytes = command.encoded_len();
        if !self.inner.try_reserve_bytes(reserved_bytes) {
            return Ok(Admission::AtCapacity);
        }
        let submitted = dependencies
            .committer
            .submit(runtime.context, command, urgency);
        let (stamped, ticket) = match submitted {
            Ok(submitted) => submitted,
            Err(error) => {
                self.inner.release_bytes(reserved_bytes);
                return Err(error.into());
            }
        };
        let applied = {
            let reader = state.overlay.reader(committed.as_ref());
            dependencies
                .applier
                .apply(&runtime.context, &reader, &stamped)
        };
        let speculative = match applied {
            Ok(outcome) => outcome,
            Err(error) => {
                self.inner.release_bytes(reserved_bytes);
                let error = RuntimeError::from(error);
                self.halt(error.to_string());
                return Err(error);
            }
        };
        let entry = OverlayEntry::new(&stamped, speculative.clone());
        self.inner
            .settle_reservation(reserved_bytes, entry.size_bytes);
        state.overlay.push(entry);
        state.proposed_count += 1;
        Ok(Admission::Decided(Some((
            ProposalResult {
                speculative,
                ticket,
            },
            extra,
        ))))
    }

    /// Gives a child Flow start its new run ID and run settings, as FlowStart gets them.
    fn resolve_child_start(&self, mut command: FlowCommand) -> FlowCommand {
        if let Some(flow_command::Command::InboxMessage(inbox)) = command.command.as_mut()
            && matches!(
                inbox.payload,
                Some(inbox_message_command::Payload::ChildStart(_))
            )
        {
            if inbox.new_run_id.is_empty() {
                inbox.new_run_id = self
                    .inner
                    .dependencies
                    .id_source
                    .run_id()
                    .as_bytes()
                    .to_vec();
            }
            if inbox.settings.is_none() {
                inbox.settings = Some(self.inner.config.run_settings);
            }
        }
        command
    }

    /// Returns a read view of a Flow's committed state, plus its uncommitted commands when
    /// `include_speculative` is true. The view is a snapshot and holds no lock.
    ///
    /// Returns an error when the committed view cannot be opened.
    pub fn flow_state_reader(
        &self,
        flow: &FlowAddress,
        include_speculative: bool,
    ) -> Result<Box<dyn FlowStateReader + Send>, RuntimeError> {
        self.check_running()?;
        let lease = include_speculative
            .then(|| self.inner.lease_existing(flow))
            .flatten();
        let Some(lease) = lease else {
            return Ok(self.inner.dependencies.committer.committed_view()?);
        };
        let state = lease.runtime().lock_state();
        // Snapshot under the Flow lock: a commit cannot drop its overlay entry in between.
        let committed = self.inner.dependencies.committer.committed_view()?;
        let overlay = state.overlay.clone();
        drop(state);
        Ok(Box::new(OwnedOverlayView { committed, overlay }))
    }

    /// Runs `inspect` under the Flow's lock with the speculative view and the RPC locks.
    pub(crate) fn inspect_flow<T>(
        &self,
        lease: &FlowRuntimeLease,
        inspect: impl FnOnce(&mut ProposalScope<'_>) -> Result<T, RuntimeError>,
    ) -> Result<T, RuntimeError> {
        self.check_running()?;
        let runtime = lease.runtime();
        let mut state = runtime.lock_state();
        // Snapshot under the Flow lock: a commit cannot drop its overlay entry in between.
        let committed = self.inner.dependencies.committer.committed_view()?;
        let state = &mut *state;
        let reader = state.overlay.reader(committed.as_ref());
        inspect(&mut ProposalScope {
            state: &reader,
            rpc_locks: &mut state.rpc_locks,
            context: runtime.context,
        })
    }

    /// Takes RPC locks, or fails with `ABORTED` when an RPC or a running Step holds a key.
    pub(crate) fn acquire_rpc_locks(
        &self,
        lease: FlowRuntimeLease,
        run_id: RunId,
        lock_keys: &[String],
    ) -> Result<RpcLockGuard, RuntimeError> {
        let lock_id = self.inspect_flow(&lease, |scope| {
            if let Some((lock_key, blocker)) = find_rpc_lock_blocker(
                scope.state,
                scope.rpc_locks,
                scope.context.flow_hash,
                run_id,
                lock_keys,
            )? {
                let holder = match blocker {
                    LockBlocker::Rpc => "another RPC",
                    LockBlocker::Step | LockBlocker::EarlierWaiter => "a running Step",
                };
                // The Go SDK maps ABORTED with WORKER_API_ERROR to RPCLockConflictError.
                return Err(RuntimeError::rejected(
                    grpc_code::ABORTED,
                    dexzd_protocol::dex::ErrorSubStatus::WorkerApiError,
                    format!("RPC lock conflict: attribute key {lock_key} is locked by {holder}"),
                ));
            }
            Ok(scope.rpc_locks.acquire(run_id, lock_keys))
        })?;
        Ok(RpcLockGuard { lease, lock_id })
    }

    /// Returns the apply context of a Flow.
    pub(crate) fn flow_context(&self, flow: &FlowAddress) -> ApplyContext {
        self.inner.flow_context(flow)
    }

    pub(crate) fn lease(&self, flow: &FlowAddress) -> FlowRuntimeLease {
        self.inner.lease(flow)
    }

    pub(crate) fn check_running(&self) -> Result<(), RuntimeError> {
        if self.inner.halted.is_cancelled() {
            return Err(self.inner.halted_error());
        }
        Ok(())
    }
}

/// Result of one proposal attempt under the Flow's lock.
enum Admission<T> {
    /// The Flow or the node is at its speculation limit.
    AtCapacity,
    /// The builder declined (`None`) or the command was proposed.
    Decided(Option<(ProposalResult, T)>),
}

/// What a proposal builder sees while it holds the Flow's lock.
pub(crate) struct ProposalScope<'a> {
    /// Committed state plus every uncommitted command of the Flow.
    pub(crate) state: &'a dyn FlowStateReader,
    pub(crate) rpc_locks: &'a mut RpcLockTable,
    pub(crate) context: ApplyContext,
}

/// RPC locks held for one RPC; released on drop.
pub(crate) struct RpcLockGuard {
    lease: FlowRuntimeLease,
    lock_id: u64,
}

impl Drop for RpcLockGuard {
    fn drop(&mut self) {
        self.lease
            .runtime()
            .lock_state()
            .rpc_locks
            .release(self.lock_id);
    }
}

impl RegistryInner {
    fn lock_runtimes(&self) -> std::sync::MutexGuard<'_, BTreeMap<FlowAddress, Arc<FlowRuntime>>> {
        self.runtimes
            .lock()
            .expect("runtime map lock is poisoned only if a holder panicked")
    }

    fn lease(self: &Arc<Self>, flow: &FlowAddress) -> FlowRuntimeLease {
        let runtime = {
            let mut runtimes = self.lock_runtimes();
            Arc::clone(runtimes.entry(flow.clone()).or_insert_with(|| {
                Arc::new(FlowRuntime::new(flow.clone(), self.flow_context(flow)))
            }))
        };
        FlowRuntimeLease {
            inner: Arc::clone(self),
            runtime: Some(runtime),
        }
    }

    fn lease_existing(self: &Arc<Self>, flow: &FlowAddress) -> Option<FlowRuntimeLease> {
        let runtime = Arc::clone(self.lock_runtimes().get(flow)?);
        Some(FlowRuntimeLease {
            inner: Arc::clone(self),
            runtime: Some(runtime),
        })
    }

    fn flow_context(&self, flow: &FlowAddress) -> ApplyContext {
        let flow_hash = self
            .dependencies
            .hasher
            .flow_hash(flow.tenant_id, &flow.flow_id);
        ApplyContext {
            partition: self.dependencies.hasher.partition_of(flow_hash),
            flow_hash,
        }
    }

    /// Reserves `bytes` of the node's speculation budget in one atomic step.
    ///
    /// Returns false while retained bytes are at or over the limit, or when the reservation would
    /// exceed it while other bytes are held.
    fn try_reserve_bytes(&self, bytes: usize) -> bool {
        let limit = self.config.max_speculative_bytes_per_node;
        self.uncommitted_bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                let is_within_limit = current < limit && current.saturating_add(bytes) <= limit;
                (current == 0 || is_within_limit).then(|| current + bytes)
            })
            .is_ok()
    }

    /// Replaces a pre-submit reservation with the entry's actual retained size.
    fn settle_reservation(&self, reserved_bytes: usize, retained_bytes: usize) {
        if retained_bytes >= reserved_bytes {
            self.uncommitted_bytes
                .fetch_add(retained_bytes - reserved_bytes, Ordering::AcqRel);
        } else {
            self.release_bytes(reserved_bytes - retained_bytes);
        }
    }

    fn release_bytes(&self, bytes: usize) {
        self.uncommitted_bytes.fetch_sub(bytes, Ordering::AcqRel);
        self.capacity_released.notify_waiters();
    }

    fn halt(&self, reason: String) {
        {
            let mut halt_reason = self
                .halt_reason
                .lock()
                .expect("halt reason lock is poisoned only if a holder panicked");
            if halt_reason.is_none() {
                tracing::error!(reason = %reason, "flow runtime registry halted");
                *halt_reason = Some(reason);
            }
        }
        self.halted.cancel();
        self.waiters.close();
        self.capacity_released.notify_waiters();
    }

    fn halted_error(&self) -> RuntimeError {
        let reason = self
            .halt_reason
            .lock()
            .expect("halt reason lock is poisoned only if a holder panicked")
            .clone()
            .unwrap_or_default();
        RuntimeError::NodeStopping {
            detail: format!("Flow runtime halted: {reason}"),
        }
    }
}

impl FlowRuntimeLease {
    pub(crate) fn runtime(&self) -> &FlowRuntime {
        self.runtime.as_ref().expect("present until drop")
    }
}

impl Drop for FlowRuntimeLease {
    fn drop(&mut self) {
        let runtime = self.runtime.take().expect("present until drop");
        let address = runtime.address.clone();
        let mut runtimes = self.inner.lock_runtimes();
        drop(runtime);
        let is_reclaimable = runtimes.get(&address).is_some_and(|registered| {
            // Leases clone only under the map lock, so one reference means unused.
            Arc::strong_count(registered) == 1
                && registered
                    .try_lock_state()
                    .is_some_and(|state| state.is_idle())
        });
        if is_reclaimable {
            runtimes.remove(&address);
        }
    }
}

/// Compares outcomes byte for byte: writes, effects, run ID, and the encoded reply.
fn is_same_outcome(speculative: &ApplyOutcome, committed: &ApplyOutcome) -> bool {
    speculative.writes == committed.writes
        && speculative.effects == committed.effects
        && speculative.run_id == committed.run_id
        && speculative.reply.encode_to_vec() == committed.reply.encode_to_vec()
}
