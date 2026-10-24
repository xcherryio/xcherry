// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Runs the work that committed commands imply: method dispatch, timers, cross-Flow deliveries,
//! hard-delete object deletion, and retention purges.
//!
//! [`EffectRunner::handle`] only enqueues. One router task owns the scheduling state and
//! spawns tracked tasks for the work. Everything here can be rebuilt from committed state with
//! [`EffectRunner::rebuild_partition`], so a lost effect delays work but never loses it.

mod dispatch;
mod effect_runtime;
mod hard_delete_objects;
mod method_request;
mod object_deletions;
mod outbox_delivery;
mod partition_rebuild;
mod state_reads;
mod timer_service;

#[cfg(test)]
mod tests;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use dexzd_apply::{ApplyEffect, StateReadError};
use dexzd_clock::WallClock;
use dexzd_keyspace::{PartitionHasher, PartitionId, RunId};
use dexzd_object_store::ObjectStoreError;
use dexzd_protocol::dexzd::v1::{CommandReply, FlowCommand, PendingHardDeleteUpload, RunSettings};
use dexzd_request_validation::ValidationConfig;
use dexzd_storage::StorageError;
use dexzd_stream_store::StreamStore;
use futures::future::BoxFuture;
use tokio::sync::{Semaphore, mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

pub use dispatch::{
    MethodDispatchRequest, MethodDispatchSession, MethodDispatchStartError, MethodDispatcher,
};
pub use effect_runtime::{EffectRuntime, EffectRuntimeError, InvocationStartOutcome};
pub use hard_delete_objects::{
    HARD_DELETE_OBJECT_KEY_PREFIX, HardDeleteObjectOwner, HardDeleteObjects,
    HardDeleteUploadLedger, HardDeleteUploads, NodeMetaUploadLedger, decode_hard_delete_value,
    encode_hard_delete_value, hard_delete_placeholder_value,
};
pub use method_request::MethodRequest;
pub(crate) use method_request::resolve_method_durability;
pub(crate) use state_reads::{RunLocation, read_run_record, read_step_execution};

use crate::blocking_work::BlockingWorkPlacement;
use crate::engine_id_source::EngineIdSource;
use crate::recovery::{PartitionRuntimeRebuilder, RuntimeRebuildError};
use crate::runtime::{CommitUrgency, FlowAddress, RuntimeEffectSink};
use dispatch::{DispatchScheduler, InvocationStartResult};
use method_request::MethodAttemptKey;
use timer_service::TimerServices;

/// Settings of the effect runner.
#[derive(Debug, Clone)]
pub struct EffectsConfig {
    /// Default 20 milliseconds, the commit batch window. Heartbeats that change an attempt's
    /// checkpoint within this window are proposed once, with the last value.
    pub heartbeat_batch_window: Duration,
    /// Default 32 MiB, `api.grpcMaxMessageBytes`. An attempt whose Worker request is larger
    /// fails before dispatch with `REQUEST_TOO_LARGE`.
    pub max_worker_request_bytes: usize,
    /// Default 16 MiB, `values.maxValueBytes`. Used only for runs whose settings carry no limit.
    pub default_max_value_bytes: u64,
    /// Default false, `values.hydrateBeforeDispatch`. When true, method requests carry complete
    /// values: the step input, Attributes, Step locals, condition results, loaded Channel messages,
    /// and the last heartbeat value are read from the value table before dispatch instead of being
    /// sent as value references. The request size limit applies to the complete request.
    pub hydrate_before_dispatch: bool,
    /// Default 60 seconds. The timer service keeps timers firing within this horizon in memory
    /// and rereads the Timer index at least this often.
    pub timer_lookahead: Duration,
    /// Default 1024. Number of timer index rows read per page of a partition. A pass keeps
    /// paging past rows in retry backoff until it has read this many other rows.
    pub timer_load_limit: usize,
    /// Default 1 second. A timer still in the index after its TimersFired command committed
    /// is retried after this delay instead of in a tight loop.
    pub fired_timer_retry_delay: Duration,
    /// Default 1 second. Invocations waiting for locks are retried at least this often,
    /// because in-memory RPC locks release without an effect.
    pub locked_invocation_retry_interval: Duration,
    /// Default 100 milliseconds. First delay before retrying a transient failure.
    pub retry_initial_backoff: Duration,
    /// Default 30 seconds. Longest delay between retries of a transient failure; deliveries
    /// and deletions retry forever.
    pub retry_max_backoff: Duration,
    /// Default 64. Most local store reads of method attempts that run on blocking threads at
    /// once. A read keeps its slot until its store call returns, also after its attempt timed
    /// out, so a stalled store cannot take every blocking thread. An attempt waits for a free
    /// slot under its method timeout. Values below 1 count as 1; values above
    /// `tokio::sync::Semaphore::MAX_PERMITS` count as that maximum.
    pub max_blocking_state_reads: usize,
    /// Settings recorded into the child run that a delivered SubFlow start creates; apply never
    /// reads configuration. Set it to the runtime's run settings so that client and SubFlow
    /// starts record the same values. Defaults follow the `RunSettings` field documentation.
    pub child_run_settings: RunSettings,
    /// Default [`BlockingWorkPlacement::BlockingThreadPool`]. Where method attempts run their
    /// local store reads; see [`BlockingWorkPlacement`].
    pub blocking_work: BlockingWorkPlacement,
}

impl Default for EffectsConfig {
    fn default() -> Self {
        Self {
            heartbeat_batch_window: Duration::from_millis(20),
            max_worker_request_bytes: 32 * 1024 * 1024,
            default_max_value_bytes: 16 * 1024 * 1024,
            hydrate_before_dispatch: false,
            timer_lookahead: Duration::from_secs(60),
            timer_load_limit: 1024,
            fired_timer_retry_delay: Duration::from_secs(1),
            locked_invocation_retry_interval: Duration::from_secs(1),
            retry_initial_backoff: Duration::from_millis(100),
            retry_max_backoff: Duration::from_secs(30),
            max_blocking_state_reads: 64,
            child_run_settings: RunSettings {
                history_window_semantic_events: 10_000,
                history_prune_chunk_semantic_events: 1_000,
                max_retained_superseded_bytes: 256 * 1024 * 1024,
                retention_after_close_micros: 7 * 24 * 60 * 60 * 1_000_000,
                wire_inline_threshold_bytes: 100,
                max_value_bytes: 16 * 1024 * 1024,
                checkpoint_min_written_bytes: 1024 * 1024,
                checkpoint_max_commands: 10_000,
                request_dedup_capacity: 1_000,
                max_run_state_bytes: 1024 * 1024 * 1024,
                max_active_step_executions: 10_000,
            },
            blocking_work: BlockingWorkPlacement::BlockingThreadPool,
        }
    }
}

/// A failure of effect work.
#[derive(Debug, thiserror::Error)]
pub enum EffectError {
    /// Reading committed or speculative state failed.
    #[error("reading state failed: {0}")]
    StateRead(#[from] StateReadError),
    /// A stored row or index key could not be decoded.
    #[error("stored {table} row {key} is corrupt: {detail}")]
    CorruptState {
        /// Table or index of the row.
        table: &'static str,
        /// Identifies the row, usually as hexadecimal.
        key: String,
        /// Decoder message.
        detail: String,
    },
    /// A trusted invariant does not hold.
    #[error("effect invariant violated: {detail}")]
    InvariantViolation {
        /// What was violated.
        detail: String,
    },
    /// An object storage call failed.
    #[error(transparent)]
    ObjectStore(#[from] ObjectStoreError),
    /// A local store call failed.
    #[error("local store operation failed: {0}")]
    Storage(#[from] StorageError),
    /// The runtime could not serve a proposal or read.
    #[error(transparent)]
    Runtime(#[from] EffectRuntimeError),
    /// The hard-delete upload ledger could not be read when the effect runner started, so
    /// recovery cannot tell which uploads predate the start. Restart the node to read it again.
    #[error("reading pending hard-delete uploads at startup failed: {detail}")]
    StartupUploadsUnreadable {
        /// The ledger's error.
        detail: String,
    },
    /// The effect runner stopped before the work finished.
    #[error("effect runner stopped")]
    Stopped,
}

impl EffectError {
    /// Returns true when the same operation may succeed if retried.
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::ObjectStore(error) => error.is_retryable(),
            Self::Runtime(EffectRuntimeError::Unavailable { .. }) | Self::StateRead(_) => true,
            _ => false,
        }
    }
}

/// Messages to the router task.
pub(crate) enum RouterMessage {
    Effects {
        flow: FlowAddress,
        effects: Vec<ApplyEffect>,
        is_committed: bool,
        scheduled_at: Option<std::time::Instant>,
    },
    RebuildPartition {
        partition: PartitionId,
        reply: oneshot::Sender<Result<(), EffectError>>,
    },
    InvocationStartFinished {
        flow: FlowAddress,
        key: MethodAttemptKey,
        result: InvocationStartResult,
    },
    AttemptFinished {
        task_id: Vec<u8>,
        key: MethodAttemptKey,
    },
    WorkFinished {
        flow: FlowAddress,
        work: TrackedWork,
    },
}

/// Deduplication keys of work that runs at most once at a time.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum TrackedWork {
    OutboxMessage { run_id: RunId, message_id: Vec<u8> },
    HardDeleteObjects { run_id: RunId },
    RunPurge { run_id: RunId },
}

/// Dependencies of the effect runner, passed once to [`EffectRunner::new`].
pub struct EffectDependencies {
    /// Process-local metrics for timer lag.
    pub metrics: Arc<dexzd_metrics::NodeMetrics>,
    /// Worker transport for method attempts.
    pub dispatcher: Arc<dyn MethodDispatcher>,
    /// Hard-delete object operations.
    pub hard_delete_objects: HardDeleteObjects,
    /// The cluster's partition hasher.
    pub partition_hasher: PartitionHasher,
    /// Owner wall clock; timers never fire before it reaches their fire time.
    pub clock: Arc<dyn WallClock>,
    /// Limits for validating Worker responses.
    pub validation_config: ValidationConfig,
    /// Receives Step Stream writes from Worker frames, in frame order, on a best-effort basis.
    pub stream_store: Arc<StreamStore>,
    /// Creates child run IDs, dispatch task IDs, and Worker Channel message IDs.
    pub id_source: Arc<dyn EngineIdSource>,
}

/// Shared state of every effect task.
pub(crate) struct EffectContext {
    pub(crate) metrics: Arc<dexzd_metrics::NodeMetrics>,
    pub(crate) config: EffectsConfig,
    pub(crate) runtime: Arc<dyn EffectRuntime>,
    pub(crate) dispatcher: Arc<dyn MethodDispatcher>,
    pub(crate) hard_delete_objects: HardDeleteObjects,
    pub(crate) hasher: PartitionHasher,
    pub(crate) clock: Arc<dyn WallClock>,
    pub(crate) validation_config: ValidationConfig,
    pub(crate) stream_store: Arc<StreamStore>,
    pub(crate) id_source: Arc<dyn EngineIdSource>,
    /// Hard-delete uploads recorded before this node started; recovery checks only these, so
    /// uploads of commands proposed after startup are never mistaken for orphans.
    /// An error fails every partition rebuild, because a later read could include new uploads.
    pub(crate) startup_pending_uploads: Result<Vec<PendingHardDeleteUpload>, String>,
    pub(crate) shutdown: CancellationToken,
    pub(crate) tracker: TaskTracker,
    /// Permits for method attempt store reads on blocking threads; see
    /// [`EffectsConfig::max_blocking_state_reads`].
    pub(crate) blocking_read_slots: Arc<Semaphore>,
    router: mpsc::UnboundedSender<RouterMessage>,
}

impl EffectContext {
    /// Sends a message to the router; ignored once the router stopped.
    pub(crate) fn report(&self, message: RouterMessage) {
        if self.router.send(message).is_err() {
            tracing::debug!("effect router stopped before a task report");
        }
    }

    pub(crate) fn run_location(&self, flow: &FlowAddress, run_id: RunId) -> RunLocation {
        RunLocation {
            flow_hash: self.hasher.flow_hash(flow.tenant_id, &flow.flow_id),
            run_id,
        }
    }

    pub(crate) fn retry_backoff(&self) -> RetryBackoff {
        RetryBackoff {
            next: self.config.retry_initial_backoff,
            max: self.config.retry_max_backoff,
        }
    }

    /// Proposes `command` and waits for its committed reply, retrying while the runtime is
    /// unavailable. Returns `None` when the node is stopping.
    pub(crate) async fn propose_until_committed(
        &self,
        flow: &FlowAddress,
        command: FlowCommand,
        urgency: CommitUrgency,
    ) -> Option<CommandReply> {
        let mut backoff = self.retry_backoff();
        loop {
            let proposal = self
                .runtime
                .propose_committed(flow, command.clone(), urgency);
            // Biased for reproducible simulation; shutdown fires once, so the proposal cannot
            // starve.
            let outcome = tokio::select! {
                biased;
                () = self.shutdown.cancelled() => return None,
                outcome = proposal => outcome,
            };
            match outcome {
                Ok(reply) => return Some(reply),
                Err(EffectRuntimeError::NodeStopping { .. }) => return None,
                Err(error @ EffectRuntimeError::Unavailable { .. }) => {
                    tracing::debug!(%error, flow_id = %flow.flow_id, "retrying an effect proposal");
                    if !backoff.wait(&self.shutdown).await {
                        return None;
                    }
                }
            }
        }
    }
}

/// Exponential backoff for transient failures.
pub(crate) struct RetryBackoff {
    next: Duration,
    max: Duration,
}

impl RetryBackoff {
    /// Sleeps for the next delay; returns false when `shutdown` fired first.
    pub(crate) async fn wait(&mut self, shutdown: &CancellationToken) -> bool {
        let delay = self.next;
        self.next = (self.next * 2).min(self.max);
        // Biased for reproducible simulation; shutdown fires once, so the sleep cannot starve.
        tokio::select! {
            biased;
            () = shutdown.cancelled() => false,
            () = tokio::time::sleep(delay) => true,
        }
    }
}

/// Handle that the Flow runtime uses to submit effects. Cheap to clone.
#[derive(Clone)]
pub struct EffectRunner {
    router: mpsc::UnboundedSender<RouterMessage>,
}

/// The effect runner's tasks, started once the runtime exists.
pub struct EffectTasks {
    config: EffectsConfig,
    dependencies: EffectDependencies,
    router_sender: mpsc::UnboundedSender<RouterMessage>,
    router_receiver: mpsc::UnboundedReceiver<RouterMessage>,
}

/// Owner of running effect tasks.
pub struct EffectTasksHandle {
    shutdown: CancellationToken,
    tracker: TaskTracker,
}

impl EffectRunner {
    /// Creates the runner handle and its not yet started tasks.
    pub fn new(config: &EffectsConfig, dependencies: EffectDependencies) -> (Self, EffectTasks) {
        let (router_sender, router_receiver) = mpsc::unbounded_channel();
        let runner = Self {
            router: router_sender.clone(),
        };
        let tasks = EffectTasks {
            config: config.clone(),
            dependencies,
            router_sender,
            router_receiver,
        };
        (runner, tasks)
    }

    /// Rebuilds pending work of every run of `partition` from committed state: pending and
    /// running invocations, timers, outbox messages, hard-delete deletions, retention purges,
    /// and hard-delete uploads that never committed.
    pub async fn rebuild_partition(&self, partition: PartitionId) -> Result<(), EffectError> {
        let (reply, response) = oneshot::channel();
        self.router
            .send(RouterMessage::RebuildPartition { partition, reply })
            .map_err(|_| EffectError::Stopped)?;
        response.await.map_err(|_| EffectError::Stopped)?
    }
}

impl RuntimeEffectSink for EffectRunner {
    /// Committed effects run all work. Speculative effects (ASYNC Steps) only start and run
    /// method attempts; the committed copy of the same effect is then ignored.
    fn handle(&self, flow: &FlowAddress, effects: &[ApplyEffect], is_committed: bool) {
        if effects.is_empty() {
            return;
        }
        let message = RouterMessage::Effects {
            flow: flow.clone(),
            effects: effects.to_vec(),
            is_committed,
            scheduled_at: is_committed.then(std::time::Instant::now),
        };
        if self.router.send(message).is_err() {
            tracing::debug!("effect runner stopped; effects are rebuilt after restart");
        }
    }
}

impl PartitionRuntimeRebuilder for EffectRunner {
    fn rebuild_partition(
        &self,
        partition: PartitionId,
    ) -> BoxFuture<'_, Result<(), RuntimeRebuildError>> {
        Box::pin(async move {
            EffectRunner::rebuild_partition(self, partition)
                .await
                .map_err(|error| RuntimeRebuildError {
                    partition,
                    detail: error.to_string(),
                })
        })
    }
}

impl EffectTasks {
    /// Starts the router task with the runtime it proposes through.
    pub fn start(self, runtime: Arc<dyn EffectRuntime>) -> EffectTasksHandle {
        let shutdown = CancellationToken::new();
        let tracker = TaskTracker::new();
        let startup_pending_uploads = self
            .dependencies
            .hard_delete_objects
            .pending_uploads()
            .map_err(|error| {
                tracing::error!(%error, "reading pending hard-delete uploads failed");
                error.to_string()
            });
        let blocking_read_slots = Arc::new(Semaphore::new(
            self.config
                .max_blocking_state_reads
                .clamp(1, Semaphore::MAX_PERMITS),
        ));
        let context = Arc::new(EffectContext {
            metrics: self.dependencies.metrics,
            config: self.config,
            runtime,
            dispatcher: self.dependencies.dispatcher,
            hard_delete_objects: self.dependencies.hard_delete_objects,
            hasher: self.dependencies.partition_hasher,
            clock: self.dependencies.clock,
            validation_config: ValidationConfig {
                // Hard-delete writes always have an object store on server-zd.
                has_blob_store: true,
                ..self.dependencies.validation_config
            },
            stream_store: self.dependencies.stream_store,
            id_source: self.dependencies.id_source,
            startup_pending_uploads,
            shutdown: shutdown.clone(),
            tracker: tracker.clone(),
            blocking_read_slots,
            router: self.router_sender,
        });
        let router = EffectRouter {
            dispatch: DispatchScheduler::new(Arc::clone(&context)),
            timers: TimerServices::new(Arc::clone(&context)),
            tracked_work: BTreeMap::new(),
            context,
        };
        tracker.spawn(router.run(self.router_receiver));
        EffectTasksHandle { shutdown, tracker }
    }
}

impl EffectTasksHandle {
    /// Cancels every effect task and waits until all of them ended.
    pub async fn shutdown(self) {
        self.shutdown.cancel();
        self.tracker.close();
        self.tracker.wait().await;
    }
}

/// Owns scheduling state and turns effects into tracked tasks.
struct EffectRouter {
    context: Arc<EffectContext>,
    dispatch: DispatchScheduler,
    timers: TimerServices,
    /// Running work, and whether a request arrived while it ran (it then runs again).
    tracked_work: BTreeMap<TrackedWork, bool>,
}

impl EffectRouter {
    async fn run(mut self, mut receiver: mpsc::UnboundedReceiver<RouterMessage>) {
        let mut parked_retry =
            tokio::time::interval(self.context.config.locked_invocation_retry_interval);
        parked_retry.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            // Biased for reproducible simulation; the tick fires once per interval, so messages
            // cannot starve.
            tokio::select! {
                biased;
                () = self.context.shutdown.cancelled() => return,
                _ = parked_retry.tick(), if self.dispatch.has_parked_invocations() => {
                    self.dispatch.retry_parked_invocations();
                }
                message = receiver.recv() => {
                    let Some(message) = message else { return };
                    self.route(message);
                }
            }
        }
    }

    fn route(&mut self, message: RouterMessage) {
        match message {
            RouterMessage::Effects {
                flow,
                effects,
                is_committed,
                scheduled_at,
            } => {
                for effect in effects {
                    self.route_effect(&flow, effect, is_committed, scheduled_at);
                }
            }
            RouterMessage::RebuildPartition { partition, reply } => {
                let context = Arc::clone(&self.context);
                self.timers.start_partition(partition);
                self.context.tracker.spawn(async move {
                    let result = partition_rebuild::rebuild_partition(&context, partition).await;
                    if reply.send(result).is_err() {
                        tracing::debug!(%partition, "rebuild caller stopped waiting");
                    }
                });
            }
            RouterMessage::InvocationStartFinished { flow, key, result } => {
                self.dispatch.finish_start(flow, key, result);
            }
            RouterMessage::AttemptFinished { task_id, key } => {
                self.dispatch.finish_attempt(task_id, &key);
            }
            RouterMessage::WorkFinished { flow, work } => {
                if self.tracked_work.remove(&work) == Some(true) {
                    self.spawn_tracked(&flow, work);
                }
            }
        }
    }

    fn route_effect(
        &mut self,
        flow: &FlowAddress,
        effect: ApplyEffect,
        is_committed: bool,
        scheduled_at: Option<std::time::Instant>,
    ) {
        match effect {
            ApplyEffect::InvocationPending {
                run_id,
                run_attempt,
                invocation,
                attempt,
            } => {
                let key = MethodAttemptKey::new(run_id, run_attempt, &invocation, attempt);
                self.dispatch.start_pending_invocation(flow.clone(), key);
            }
            ApplyEffect::InvocationStarted {
                run_id,
                run_attempt,
                invocation,
                attempt,
                task_id,
                method_timeout_seconds,
            } => {
                let key = MethodAttemptKey::new(run_id, run_attempt, &invocation, attempt);
                self.dispatch.run_started_attempt(
                    flow.clone(),
                    key,
                    task_id,
                    method_timeout_seconds,
                    scheduled_at,
                );
            }
            ApplyEffect::InvocationCanceled { task_id, .. } => {
                self.dispatch.cancel_attempt(&task_id);
            }
            ApplyEffect::LocksReleased { run_id, .. } => self.dispatch.release_locks(run_id),
            ApplyEffect::WakeWaiters { .. } => {}
            effect if !is_committed => {
                tracing::debug!(?effect, "ignoring a speculative external effect");
            }
            ApplyEffect::TimerArmed { fire_at_micros } => {
                let partition = self
                    .context
                    .hasher
                    .partition_of_flow(flow.tenant_id, &flow.flow_id);
                self.timers.notify_armed(partition, fire_at_micros);
            }
            ApplyEffect::OutboxMessagesWritten {
                run_id,
                message_ids,
            } => {
                for message_id in message_ids {
                    let work = TrackedWork::OutboxMessage { run_id, message_id };
                    self.spawn_tracked(flow, work);
                }
            }
            ApplyEffect::HardDeleteObjectsPending { run_id, .. } => {
                self.spawn_tracked(flow, TrackedWork::HardDeleteObjects { run_id });
            }
            ApplyEffect::RunPurgeRequested { run_id } => {
                self.spawn_tracked(flow, TrackedWork::RunPurge { run_id });
            }
        }
    }

    /// Spawns `work`, or marks running work to run again because state may have changed
    /// after its last read.
    fn spawn_tracked(&mut self, flow: &FlowAddress, work: TrackedWork) {
        if let Some(is_rerun_requested) = self.tracked_work.get_mut(&work) {
            *is_rerun_requested = true;
            return;
        }
        self.tracked_work.insert(work.clone(), false);
        let context = Arc::clone(&self.context);
        let flow = flow.clone();
        self.context.tracker.spawn(async move {
            let result = match &work {
                TrackedWork::OutboxMessage { run_id, message_id } => {
                    outbox_delivery::deliver_outbox_message(&context, &flow, *run_id, message_id)
                        .await
                }
                TrackedWork::HardDeleteObjects { run_id } => {
                    object_deletions::delete_pending_hard_delete_objects(&context, &flow, *run_id)
                        .await
                }
                TrackedWork::RunPurge { run_id } => {
                    object_deletions::purge_run(&context, &flow, *run_id).await
                }
            };
            if let Err(error) = result {
                tracing::warn!(%error, ?work, "effect work stopped with an error");
            }
            context.report(RouterMessage::WorkFinished { flow, work });
        });
    }
}
