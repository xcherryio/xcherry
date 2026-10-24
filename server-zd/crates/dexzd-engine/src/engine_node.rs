// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! The engine node: startup and recovery, serving, and shutdown.

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use dexzd_clock::WallClock;
use dexzd_cluster_metadata::{
    ClusterConfig, LeaseConfig, LeaseHandle, LeaseReleaseError, LeaseStatus, NodeConfig,
};
use dexzd_keyspace::PartitionId;
use dexzd_log::{CommitPipeline, StreamIdentity};
use dexzd_object_store::ObjectStore;
use dexzd_protocol::dexzd::v1::SnapshotReference;
use dexzd_storage::LocalReadView;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::blocking_work::BlockingWorkPlacement;
use crate::command_applier::CommandApplier;
use crate::commit::{
    AppliedPositionAdvancer, CommandCommitter, CommandCommitterParts, CommitConfig, CommitProgress,
    CommitState, CommittedBatchRouter, EngineLocalStore, FrameApplier, NodeStopReason,
    PartitionApplierPool, run_log_proposer,
};
use crate::engine_id_source::EngineIdSource;
use crate::recovery::{
    ClusterMetadata, EngineRuntimeHooks, EngineServices, EngineStartError, LocalStoreConfig,
    LocalStoreRequirements, LogReplay, open_local_store,
};
use crate::snapshots::{
    LogCleanup, OwnershipFence, SnapshotCatalog, SnapshotConfig, SnapshotError, SnapshotRetention,
    SnapshotService,
};

/// Longest wait for engine tasks and applier lanes to stop before the lease is released anyway.
/// It includes [`FINAL_ADVANCE_BUDGET`].
const TASK_STOP_BUDGET: Duration = Duration::from_secs(3);

/// Longest wait at shutdown for advancing applied positions, so the next start replays little.
const FINAL_ADVANCE_BUDGET: Duration = Duration::from_secs(1);

/// Configuration sections of an engine node.
#[derive(Debug, Clone)]
pub struct EngineNodeConfig {
    /// This node's identity in the manifest and its log stream.
    pub node: NodeConfig,
    /// Immutable cluster settings, checked against the cluster object.
    pub cluster: ClusterConfig,
    /// Node lease timing.
    pub lease: LeaseConfig,
    /// Local state store.
    pub local_store: LocalStoreConfig,
    /// Commit path.
    pub commit: CommitConfig,
    /// Snapshots and log cleanup.
    pub snapshots: SnapshotConfig,
    /// Log replay read-ahead and compaction after a bulk replay.
    pub recovery: crate::recovery::RecoveryConfig,
    /// Where frame applies, applied-position writes, and snapshot uploads run.
    /// [`BlockingWorkPlacement::BlockingThreadPool`] in production.
    pub blocking_work: BlockingWorkPlacement,
}

/// Shared services the node runs on.
pub struct EngineNodeDependencies {
    /// Process-local metric handles for this node.
    pub metrics: Arc<dexzd_metrics::NodeMetrics>,
    /// Object storage that holds the cluster metadata, logs, and snapshots.
    pub object_store: Arc<dyn ObjectStore>,
    /// Wall clock for command timestamps and lease expiry.
    pub clock: Arc<dyn WallClock>,
    /// Applies committed commands.
    pub applier: Arc<dyn CommandApplier>,
    /// Creates the command, lineage, and EpochStart writer IDs.
    pub id_source: Arc<dyn EngineIdSource>,
}

/// A node serving its log stream. After a fatal failure it stops serving; then exit non-zero.
pub struct EngineNode {
    metrics: Arc<dexzd_metrics::NodeMetrics>,
    committer: CommandCommitter,
    state: Arc<CommitState>,
    appliers: Arc<PartitionApplierPool>,
    applied_positions: Arc<AppliedPositionAdvancer>,
    snapshots: Arc<SnapshotService>,
    lease: LeaseHandle,
    tasks: RunningTasks,
}

impl EngineNode {
    /// Longest [`EngineNode::shutdown`] runs: the task stop budget plus the lease release budget.
    pub const SHUTDOWN_BUDGET: Duration =
        TASK_STOP_BUDGET.saturating_add(LeaseHandle::RELEASE_BUDGET);

    /// Restores state, replays the log, starts serving, and rebuilds every running partition's runtime.
    pub async fn start<F>(
        config: &EngineNodeConfig,
        dependencies: EngineNodeDependencies,
        runtime_factory: F,
    ) -> Result<Self, EngineStartError>
    where
        F: FnOnce(EngineServices) -> EngineRuntimeHooks,
    {
        let (_cancellation_sender, mut cancellation) = watch::channel(false);
        let started =
            Self::start_until_cancelled(config, dependencies, runtime_factory, &mut cancellation)
                .await?;
        Ok(started.expect("startup without cancellation must return an engine node"))
    }

    /// Starts the node, or returns `None` when cancellation arrives while waiting for its lease.
    pub async fn start_until_cancelled<F>(
        config: &EngineNodeConfig,
        dependencies: EngineNodeDependencies,
        runtime_factory: F,
        cancellation: &mut watch::Receiver<bool>,
    ) -> Result<Option<Self>, EngineStartError>
    where
        F: FnOnce(EngineServices) -> EngineRuntimeHooks,
    {
        validate_config(config)?;
        let EngineNodeDependencies {
            metrics,
            object_store,
            clock,
            applier,
            id_source,
        } = dependencies;
        // Fails fast on a contradicting manifest; the predecessor may still change it until the lease.
        let unleased_metadata = ClusterMetadata::bootstrap_or_validate(
            object_store.as_ref(),
            clock.as_ref(),
            config.cluster,
            &config.node,
        )
        .await?;
        let partition_count = unleased_metadata.partition_count();
        metrics
            .behavior_version
            .set(i64::from(unleased_metadata.behavior_version));
        metrics.set_role(dexzd_metrics::NodeRole::Standby);
        let cluster_id = unleased_metadata.cluster_id()?;
        let Some(lease) = config
            .lease
            .acquire_until_cancelled_with_metrics(
                object_store.clone(),
                clock.clone(),
                &config.node,
                cancellation,
                id_source.lease_holder_process_id(),
                metrics.as_ref().clone(),
            )
            .await?
        else {
            return Ok(None);
        };
        let epoch = lease.epoch();
        metrics
            .lease_epoch
            .set(i64::try_from(epoch).unwrap_or(i64::MAX));
        metrics.set_role(dexzd_metrics::NodeRole::Recovering);
        let recovery_started = std::time::Instant::now();
        tracing::info!(node_id = %config.node.node_id, epoch, partition_count, "engine node recovering");
        let state = Arc::new(CommitState::new(partition_count));
        let mut tasks = RunningTasks::default();
        tasks.spawn(watch_lease(
            lease.subscribe(),
            state.clone(),
            metrics.clone(),
            tasks.cancellation.clone(),
        ));
        // Every failure from here on must release the lease, or a successor waits for it to expire.
        let mut starting = StartingNode {
            parts: Some(LeasedParts {
                state: state.clone(),
                tasks,
                appliers: None,
                lease,
            }),
        };
        let serving = async {
            let parts = starting.parts_mut();
            let metadata = unleased_metadata
                .read_under_lease(object_store.as_ref(), &config.node, epoch)
                .await?;
            metrics
                .behavior_version
                .set(i64::from(metadata.behavior_version));
            let stream = StreamIdentity {
                cluster_id,
                node_id: config.node.node_id.clone(),
                incarnation: metadata.streams.serving_incarnation,
            };
            let store = open_local_store(
                &config.local_store,
                object_store.as_ref(),
                &LocalStoreRequirements {
                    snapshot: metadata.node_snapshot(&config.node.node_id),
                    streams: &metadata.streams,
                    partition_count,
                    new_store_lineage_id: id_source.store_lineage_id(),
                },
            )
            .await?;
            let node_meta_lock = Arc::new(std::sync::Mutex::new(()));
            let (proposal_sender, proposal_receiver) = mpsc::unbounded_channel();
            let committer = CommandCommitter::new(CommandCommitterParts {
                state: state.clone(),
                store: store.clone(),
                clock: clock.clone(),
                id_source: id_source.clone(),
                epoch,
                behavior_version: metadata.behavior_version,
                max_unapplied_bytes: config.commit.max_unapplied_bytes,
                is_checking_replay: config.commit.debug_replay_checks,
                proposals: proposal_sender,
            });
            let runtime = runtime_factory(EngineServices {
                committer: committer.clone(),
                partition_hasher: metadata.cluster.partition_hasher.clone(),
                node_meta_lock: node_meta_lock.clone(),
                local_store: store.clone(),
                id_source: id_source.clone(),
            });
            let pre_replay_semantic_points = if config.commit.debug_replay_checks {
                FrameApplier::pre_replay_semantic_points(store.as_ref())?
            } else {
                std::collections::BTreeSet::new()
            };
            tracing::debug!(
                unverifiable_semantic_points = pre_replay_semantic_points.len(),
                "semantic points present before log replay lack independent hashes"
            );
            let appliers = Arc::new(PartitionApplierPool::start(
                FrameApplier {
                    metrics: metrics.clone(),
                    metrics_enabled: std::sync::atomic::AtomicBool::new(false),
                    store: store.clone(),
                    applier,
                    hasher: metadata.cluster.partition_hasher.clone(),
                    state: state.clone(),
                    committed_commands: runtime.committed_commands.clone(),
                    is_checking_replay: config.commit.debug_replay_checks,
                    blocking_work: config.blocking_work,
                    semantic_point_hashes: std::sync::Mutex::new(std::collections::BTreeMap::new()),
                    pre_replay_semantic_points,
                    verified_time_travels: AtomicU64::new(0),
                    unverifiable_time_travels: AtomicU64::new(0),
                },
                config.commit.applier_lanes,
                config.commit.applier_queue_frames,
            ));
            parts.appliers = Some(appliers.clone());
            let replayed = LogReplay {
                config: &config.recovery,
                object_store: object_store.as_ref(),
                clock: clock.as_ref(),
                id_source: id_source.as_ref(),
                cluster_id,
                streams: &metadata.streams,
                store: &store,
                appliers: &appliers,
                state: &state,
                epoch,
            }
            .run()
            .await?;
            // Recovery uses these applier lanes; only later frames are live commits.
            appliers.enable_metrics();
            committer.seed_stamp_watermarks(&applied_command_timestamps(&store)?);
            let (pipeline, committed_batches) = CommitPipeline::start_with_metrics(
                object_store.clone(),
                clock.clone(),
                stream.clone(),
                epoch,
                replayed.next_seq,
                config.commit.pipeline_config(),
                metrics.as_ref().clone(),
            )?;
            let progress = Arc::new(CommitProgress {
                committed_log_bytes: AtomicU64::new(0),
                last_routed_seq: AtomicU64::new(replayed.epoch_start_seq),
            });
            let tasks = &mut parts.tasks;
            tasks.spawn(run_log_proposer(
                pipeline,
                proposal_receiver,
                state.clone(),
                tasks.cancellation.clone(),
            ));
            tasks.spawn(
                CommittedBatchRouter {
                    appliers: appliers.clone(),
                    state: state.clone(),
                    node_id: stream.node_id.clone(),
                    incarnation: stream.incarnation,
                    progress: progress.clone(),
                }
                .run(committed_batches, tasks.cancellation.clone()),
            );
            let applied_positions = Arc::new(AppliedPositionAdvancer::new(
                appliers.clone(),
                progress.clone(),
                stream.node_id.clone(),
                stream.incarnation,
                epoch,
                partition_count,
            ));
            tasks.spawn(applied_positions.clone().run(tasks.cancellation.clone()));
            // Rebuilt work may submit at once, so commands are accepted before rebuilding.
            if !state.start_serving() {
                return Err(EngineStartError::Stopped {
                    reason: state.stop_reason().unwrap_or(NodeStopReason::Shutdown),
                });
            }
            for partition in 0..partition_count {
                let partition = PartitionId(
                    u16::try_from(partition).expect("the manifest limits partitions to u16"),
                );
                if !state.is_partition_halted(partition) {
                    runtime.rebuilder.rebuild_partition(partition).await?;
                }
            }
            if let Some(reason) = state.stop_reason() {
                return Err(EngineStartError::Stopped { reason });
            }
            let snapshots = Arc::new(SnapshotService {
                store,
                object_store: object_store.clone(),
                clock: clock.clone(),
                appliers: appliers.clone(),
                progress,
                retention: SnapshotRetention {
                    object_store: object_store.clone(),
                    node_id: stream.node_id.clone(),
                    retained_count: config.snapshots.retained_count,
                    deletion_delay: config.snapshots.deletion_delay,
                },
                fence: OwnershipFence {
                    object_store: object_store.clone(),
                    clock: clock.clone(),
                    state: state.clone(),
                    node_id: stream.node_id.clone(),
                    epoch,
                },
                log_cleanup: LogCleanup {
                    object_store,
                    clock,
                    stream,
                    partition_count,
                    deletion_delay: config.snapshots.deletion_delay,
                },
                cluster_id,
                epoch,
                id_source,
                node_meta_lock,
                config: config.snapshots.clone(),
                catalog: tokio::sync::Mutex::new(SnapshotCatalog::default()),
                last_snapshot_routed_seq: AtomicU64::new(0),
                blocking_work: config.blocking_work,
                metrics: metrics.clone(),
            });
            tasks.spawn(snapshots.clone().run(tasks.cancellation.clone()));
            metrics
                .recovery_last_duration
                .set(recovery_started.elapsed().as_secs_f64());
            metrics
                .recovery_last_replayed_bytes
                .set(i64::try_from(replayed.replayed_bytes).unwrap_or(i64::MAX));
            tracing::info!(epoch, next_seq = replayed.next_seq, "engine node serving");
            metrics.set_role(dexzd_metrics::NodeRole::Serving);
            Ok((committer, applied_positions, snapshots))
        }
        .await;
        match serving {
            Ok((committer, applied_positions, snapshots)) => {
                let LeasedParts {
                    state,
                    tasks,
                    appliers,
                    lease,
                } = starting.take_parts();
                Ok(Some(Self {
                    metrics,
                    committer,
                    state,
                    appliers: appliers.expect("the applier pool starts before the node serves"),
                    applied_positions,
                    snapshots,
                    lease,
                    tasks,
                }))
            }
            Err(error) => {
                starting.release_after_failure().await;
                metrics.lease_epoch.set(0);
                metrics.set_role(dexzd_metrics::NodeRole::Standby);
                Err(error)
            }
        }
    }

    /// Returns the committer that Flow runtimes submit commands through.
    pub fn committer(&self) -> &CommandCommitter {
        &self.committer
    }

    /// Waits until the node stops serving and returns why.
    pub fn wait_for_stop(&self) -> impl Future<Output = NodeStopReason> + Send + 'static {
        let state = self.state.clone();
        async move { state.wait_stopped().await }
    }

    /// Returns why the node stopped, or `None` while it serves.
    pub fn stop_reason(&self) -> Option<NodeStopReason> {
        self.state.stop_reason()
    }

    /// Returns partitions that stopped applying commands after an apply failure.
    pub fn halted_partitions(&self) -> Vec<PartitionId> {
        self.state.halted_partitions()
    }

    /// Returns verified and snapshot-only time-travel target counts, in that order.
    pub fn time_travel_verification_counts(&self) -> (u64, u64) {
        self.appliers.time_travel_verification_counts()
    }

    /// Takes a snapshot now, records it in the manifest, and deletes covered objects.
    pub async fn take_snapshot(&self) -> Result<SnapshotReference, SnapshotError> {
        self.snapshots.take_snapshot().await
    }

    /// Stops serving, fails waiting tickets, stops every task, and releases the lease.
    ///
    /// It first records the last routed batch as every running partition's position, for at most
    /// 1 second.
    ///
    /// Tasks and applier lanes that do not stop within 3 seconds are abandoned, so the lease
    /// release is always attempted. The 3 seconds include that first second. Finishes within
    /// [`EngineNode::SHUTDOWN_BUDGET`].
    ///
    /// Returns `Ok` once this process no longer holds the lease, and [`LeaseReleaseError`] when the
    /// release was rejected or timed out; the lease then stays held until it expires.
    pub async fn shutdown(self) -> Result<(), LeaseReleaseError> {
        self.metrics.set_role(dexzd_metrics::NodeRole::Draining);
        let metrics = self.metrics.clone();
        let Self {
            state,
            appliers,
            applied_positions,
            lease,
            tasks,
            ..
        } = self;
        let released = LeasedParts {
            state,
            tasks,
            appliers: Some(appliers),
            lease,
        }
        .stop_and_release_lease(Some(&applied_positions))
        .await;
        if released.is_ok() {
            metrics.lease_epoch.set(0);
        }
        tracing::info!("engine node stopped");
        released
    }
}

/// Everything that runs under the node lease, stopped before the lease is released.
struct LeasedParts {
    state: Arc<CommitState>,
    tasks: RunningTasks,
    appliers: Option<Arc<PartitionApplierPool>>,
    lease: LeaseHandle,
}

impl LeasedParts {
    /// Stops serving, gives tasks and applier lanes [`TASK_STOP_BUDGET`] to stop, then releases the
    /// lease.
    ///
    /// A serving node passes `final_advance` to advance applied positions before it stops.
    ///
    /// Returns the outcome of the lease release.
    async fn stop_and_release_lease(
        self,
        final_advance: Option<&AppliedPositionAdvancer>,
    ) -> Result<(), LeaseReleaseError> {
        let Self {
            state,
            tasks,
            appliers,
            lease,
        } = self;
        let stop_deadline = tokio::time::Instant::now() + TASK_STOP_BUDGET;
        if let Some(advancer) = final_advance
            && state.stop_reason().is_none()
        {
            // The stop blocks every later store write, so the advance must finish first.
            match tokio::time::timeout(FINAL_ADVANCE_BUDGET, advancer.advance_to_last_routed())
                .await
            {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    tracing::warn!(%error, "applied positions were not advanced before shutdown");
                }
                Err(_) => tracing::warn!(
                    budget_seconds = FINAL_ADVANCE_BUDGET.as_secs(),
                    "applied positions were not advanced in time; the next start replays more of \
                     the log"
                ),
            }
        }
        state.stop(NodeStopReason::Shutdown);
        let stopping = async {
            tasks.shutdown().await;
            if let Some(appliers) = &appliers {
                appliers.shutdown().await;
            }
        };
        if tokio::time::timeout_at(stop_deadline, stopping)
            .await
            .is_err()
        {
            // Dropping the tasks aborts them; a lane blocked in apply ends with the process.
            tracing::error!(
                budget_seconds = TASK_STOP_BUDGET.as_secs(),
                "engine tasks did not stop in time; releasing the lease anyway"
            );
        }
        lease.shutdown().await
    }
}

/// Owns the leased parts while [`EngineNode::start`] runs, so a failed or cancelled start releases
/// the lease.
struct StartingNode {
    parts: Option<LeasedParts>,
}

impl StartingNode {
    fn parts_mut(&mut self) -> &mut LeasedParts {
        self.parts
            .as_mut()
            .expect("the parts stay until the start finishes")
    }

    /// Disarms the guard and returns the parts.
    fn take_parts(&mut self) -> LeasedParts {
        self.parts
            .take()
            .expect("the parts stay until the start finishes")
    }

    /// Stops the parts and releases the lease; the release continues if the caller is cancelled.
    async fn release_after_failure(mut self) {
        let release_task = tokio::spawn(release_after_unfinished_start(self.take_parts()));
        if let Err(error) = release_task.await {
            tracing::error!(%error, "the lease release task failed after an unfinished start");
        }
    }
}

impl Drop for StartingNode {
    fn drop(&mut self) {
        let Some(parts) = self.parts.take() else {
            return;
        };
        match tokio::runtime::Handle::try_current() {
            Ok(runtime) => {
                // Detached on purpose: the release must outlive the cancelled start.
                let release_task = runtime.spawn(release_after_unfinished_start(parts));
                drop(release_task);
            }
            Err(_) => tracing::error!(
                "no async runtime can release the node lease after a cancelled start; it stays \
                 held until it expires"
            ),
        }
    }
}

/// Stops the parts of a failed or cancelled start, releases the lease, and logs the outcome.
async fn release_after_unfinished_start(parts: LeasedParts) {
    match parts.stop_and_release_lease(None).await {
        Ok(()) => tracing::info!("released the node lease after an unfinished start"),
        Err(error) => {
            tracing::error!(%error, "the node lease stays held after an unfinished start")
        }
    }
}

fn validate_config(config: &EngineNodeConfig) -> Result<(), EngineStartError> {
    let commit = &config.commit;
    let snapshots = &config.snapshots;
    let problem = if commit.applier_lanes == 0 {
        Some("commit.applier_lanes must be positive")
    } else if commit.applier_queue_frames == 0 {
        Some("commit.applier_queue_frames must be positive")
    } else if commit.max_unapplied_bytes == 0 {
        Some("commit.max_unapplied_bytes must be positive")
    } else if snapshots.retained_count == 0 {
        Some("snapshots.retained_count must be positive")
    } else if snapshots.check_interval.is_zero() {
        Some("snapshots.check_interval must be positive")
    } else if config.recovery.log_read_ahead.max_read_ahead_batches == 0 {
        Some("recovery.log_read_ahead.max_read_ahead_batches must be positive")
    } else if config.recovery.log_read_ahead.max_buffered_bytes == 0 {
        Some("recovery.log_read_ahead.max_buffered_bytes must be positive")
    } else {
        None
    };
    match problem {
        Some(detail) => Err(EngineStartError::InvalidConfig {
            detail: detail.to_owned(),
        }),
        None => Ok(()),
    }
}

fn applied_command_timestamps(
    store: &EngineLocalStore,
) -> Result<Vec<(PartitionId, i64)>, EngineStartError> {
    store
        .partition_metas()?
        .into_iter()
        .map(|meta| {
            let partition =
                u16::try_from(meta.partition).map_err(|_| EngineStartError::InvalidMetadata {
                    detail: format!("local store names partition {}", meta.partition),
                })?;
            Ok((PartitionId(partition), meta.last_command_timestamp_micros))
        })
        .collect()
}

/// Stops the node when the lease is lost or its local validity deadline passes.
async fn watch_lease(
    mut status: watch::Receiver<LeaseStatus>,
    state: Arc<CommitState>,
    metrics: Arc<dexzd_metrics::NodeMetrics>,
    cancellation: CancellationToken,
) {
    loop {
        let current = status.borrow_and_update().clone();
        let deadline = match current {
            LeaseStatus::Lost(detail) => {
                metrics.lease_epoch.set(0);
                state.stop(NodeStopReason::LeaseLost { detail });
                return;
            }
            LeaseStatus::ValidUntil(deadline) => deadline,
        };
        tokio::select! {
            biased;
            () = cancellation.cancelled() => return,
            changed = status.changed() => {
                if changed.is_err() {
                    metrics.lease_epoch.set(0);
                    state.stop(NodeStopReason::LeaseLost {
                        detail: "lease renewal stopped".to_owned(),
                    });
                    return;
                }
            }
            () = tokio::time::sleep_until(deadline) => {
                metrics.lease_epoch.set(0);
                state.stop(NodeStopReason::LeaseLost {
                    detail: "lease validity expired before renewal".to_owned(),
                });
                return;
            }
        }
    }
}

/// Background tasks of a node; dropping it cancels and aborts every task.
#[derive(Default)]
struct RunningTasks {
    cancellation: CancellationToken,
    handles: Vec<JoinHandle<()>>,
}

impl RunningTasks {
    fn spawn(&mut self, task: impl Future<Output = ()> + Send + 'static) {
        let handle = tokio::spawn(task);
        self.handles.push(handle);
    }

    async fn shutdown(mut self) {
        self.cancellation.cancel();
        for handle in std::mem::take(&mut self.handles) {
            if let Err(error) = handle.await
                && !error.is_cancelled()
            {
                tracing::error!(error = %error, "engine task failed during shutdown");
            }
        }
    }
}

impl Drop for RunningTasks {
    fn drop(&mut self) {
        self.cancellation.cancel();
        for handle in &self.handles {
            handle.abort();
        }
    }
}
