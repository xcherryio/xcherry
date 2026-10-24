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

use dexzd_clock::WallClock;
use dexzd_cluster_metadata::{
    ClusterConfig, LeaseConfig, LeaseHandle, LeaseStatus, NodeConfig, acquire_lease,
};
use dexzd_keyspace::PartitionId;
use dexzd_log::{CommitPipeline, StreamIdentity};
use dexzd_object_store::ObjectStore;
use dexzd_protocol::dexzd::v1::SnapshotReference;
use dexzd_storage::LocalReadView;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::command_applier::CommandApplier;
use crate::commit::{
    CommandCommitter, CommandCommitterParts, CommitConfig, CommitProgress, CommitState,
    CommittedBatchRouter, EngineLocalStore, FrameApplier, NodeStopReason, PartitionApplierPool,
    run_log_proposer,
};
use crate::recovery::{
    ClusterMetadata, EngineRuntimeHooks, EngineServices, EngineStartError, LocalStoreConfig,
    LocalStoreRequirements, LogReplay, open_local_store,
};
use crate::snapshots::{
    LogCleanup, OwnershipFence, SnapshotCatalog, SnapshotConfig, SnapshotError, SnapshotRetention,
    SnapshotService,
};

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
}

/// Shared services the node runs on.
pub struct EngineNodeDependencies {
    /// Object storage that holds the cluster metadata, logs, and snapshots.
    pub object_store: Arc<dyn ObjectStore>,
    /// Wall clock for command timestamps and lease expiry.
    pub clock: Arc<dyn WallClock>,
    /// Applies committed commands.
    pub applier: Arc<dyn CommandApplier>,
}

/// A node serving its log stream. After a fatal failure it stops serving; then exit non-zero.
pub struct EngineNode {
    committer: CommandCommitter,
    state: Arc<CommitState>,
    appliers: Arc<PartitionApplierPool>,
    snapshots: Arc<SnapshotService>,
    lease: LeaseHandle,
    tasks: RunningTasks,
}

impl EngineNode {
    /// Restores state, replays the log, starts serving, and rebuilds every running partition's runtime.
    pub async fn start<F>(
        config: &EngineNodeConfig,
        dependencies: EngineNodeDependencies,
        runtime_factory: F,
    ) -> Result<Self, EngineStartError>
    where
        F: FnOnce(EngineServices) -> EngineRuntimeHooks,
    {
        validate_config(config)?;
        let EngineNodeDependencies {
            object_store,
            clock,
            applier,
        } = dependencies;
        let metadata = ClusterMetadata::bootstrap_or_validate(
            object_store.as_ref(),
            clock.as_ref(),
            config.cluster,
            &config.node,
        )
        .await?;
        let partition_count = metadata.partition_count();
        let cluster_id = metadata.cluster_id()?;
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
            },
        )
        .await?;
        let lease = acquire_lease(
            object_store.clone(),
            clock.clone(),
            &config.node,
            &config.lease,
        )
        .await?;
        let epoch = lease.epoch();
        tracing::info!(node_id = %config.node.node_id, epoch, partition_count, "engine node recovering");
        let state = Arc::new(CommitState::new(partition_count));
        let mut tasks = RunningTasks::default();
        tasks.spawn(watch_lease(
            lease.subscribe(),
            state.clone(),
            tasks.cancellation.clone(),
        ));
        let node_meta_lock = Arc::new(std::sync::Mutex::new(()));
        let (proposal_sender, proposal_receiver) = mpsc::unbounded_channel();
        let committer = CommandCommitter::new(CommandCommitterParts {
            state: state.clone(),
            store: store.clone(),
            clock: clock.clone(),
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
        });
        let appliers = Arc::new(PartitionApplierPool::start(
            FrameApplier {
                store: store.clone(),
                applier,
                hasher: metadata.cluster.partition_hasher.clone(),
                state: state.clone(),
                committed_commands: runtime.committed_commands.clone(),
                is_checking_replay: config.commit.debug_replay_checks,
            },
            config.commit.applier_lanes,
            config.commit.applier_queue_frames,
        ));
        let replayed = LogReplay {
            object_store: object_store.as_ref(),
            clock: clock.as_ref(),
            cluster_id,
            streams: &metadata.streams,
            store: &store,
            appliers: &appliers,
            state: &state,
            epoch,
        }
        .run()
        .await?;
        committer.seed_stamp_watermarks(&applied_command_timestamps(&store)?);
        let (pipeline, committed_batches) = CommitPipeline::start(
            object_store.clone(),
            clock.clone(),
            stream.clone(),
            epoch,
            replayed.next_seq,
            config.commit.pipeline_config(),
        )?;
        let progress = Arc::new(CommitProgress {
            committed_log_bytes: AtomicU64::new(0),
            last_routed_seq: AtomicU64::new(replayed.epoch_start_seq),
        });
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
            node_meta_lock,
            config: config.snapshots.clone(),
            catalog: tokio::sync::Mutex::new(SnapshotCatalog::default()),
            last_snapshot_routed_seq: AtomicU64::new(0),
        });
        tasks.spawn(snapshots.clone().run(tasks.cancellation.clone()));
        tracing::info!(epoch, next_seq = replayed.next_seq, "engine node serving");
        Ok(Self {
            committer,
            state,
            appliers,
            snapshots,
            lease,
            tasks,
        })
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

    /// Takes a snapshot now, records it in the manifest, and deletes covered objects.
    pub async fn take_snapshot(&self) -> Result<SnapshotReference, SnapshotError> {
        self.snapshots.take_snapshot().await
    }

    /// Stops serving, fails waiting tickets, stops every task, and releases the lease.
    pub async fn shutdown(self) {
        self.state.stop(NodeStopReason::Shutdown);
        self.tasks.shutdown().await;
        self.appliers.shutdown().await;
        self.lease.shutdown().await;
        tracing::info!("engine node stopped");
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
    cancellation: CancellationToken,
) {
    loop {
        let current = status.borrow_and_update().clone();
        let deadline = match current {
            LeaseStatus::Lost(detail) => {
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
                    state.stop(NodeStopReason::LeaseLost {
                        detail: "lease renewal stopped".to_owned(),
                    });
                    return;
                }
            }
            () = tokio::time::sleep_until(deadline) => {
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
