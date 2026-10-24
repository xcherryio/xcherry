// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! One engine node process: the engine, its Flow runtime and effects, and how it crashes.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use dexzd_clock::WallClock;
use dexzd_cluster_metadata::{ClusterConfig, LeaseConfig, METADATA_FORMAT_VERSION, NodeConfig};
use dexzd_engine::{
    BlockingWorkPlacement, CommandApplier, CommitConfig, DeterministicCommandApplier,
    EffectDependencies, EffectRunner, EffectTasksHandle, EffectsConfig, EngineIdSource,
    EngineLocalStore, EngineNode, EngineNodeConfig, EngineNodeDependencies, EngineRuntimeHooks,
    EngineServices, EngineStartError, FlowRuntimeDependencies, FlowRuntimeRegistry,
    HardDeleteObjects, LocalStoreConfig, NodeMetaUploadLedger, NodeStopReason, RecoveryConfig,
    RuntimeConfig, SnapshotConfig, default_run_settings,
};
use dexzd_keyspace::{ColumnFamily, KEY_LAYOUT_VERSION};
use dexzd_object_store::{ObjectStore, WriteCondition};
use dexzd_protocol::dexzd::v1::{ClusterObject, PartitionHashAlgorithm, RunSettings};
use dexzd_request_validation::ValidationConfig;
use dexzd_storage::{InMemoryLocalStore, LocalStore, ScanDirection, WriteBatch};
use dexzd_stream_store::{StreamStore, StreamStoreConfig};
use prost::Message;
use tokio::task::JoinHandle;

use crate::engine_simulation::committed_log::CommittedLog;
use crate::engine_simulation::event_trace::EventTrace;
use crate::engine_simulation::simulated_clock::{
    START_UNIX_MICROS, SimulatedWallClock, duration_micros,
};
use crate::engine_simulation::simulated_object_store::{NodeObjectStore, SharedObjectStorage};
use crate::engine_simulation::simulated_workers::SimulatedWorkers;

/// The single node of the simulated cluster.
pub(crate) const NODE_ID: &str = "node-a";
/// Partitioning of [`crate::ApplyChecker::new`], so the shadow places Flows like the engine.
const PARTITION_HASH_KEY: [u8; 16] = [7; 16];
pub(crate) const PARTITION_COUNT: u32 = 64;
/// Object key of the cluster object, a persistent format.
const CLUSTER_OBJECT_KEY: &str = "cluster";
pub(crate) const LEASE_DURATION: Duration = Duration::from_secs(10);
pub(crate) const LEASE_TAKEOVER_GRACE: Duration = Duration::from_secs(1);

/// What every node process of one seed shares.
pub(crate) struct ClusterEnvironment {
    /// The shared object store, its observations, and writes still on their way to it.
    pub(crate) storage: Arc<SharedObjectStorage>,
    pub(crate) workers: Arc<SimulatedWorkers>,
    pub(crate) committed_log: Arc<CommittedLog>,
    pub(crate) trace: Arc<EventTrace>,
    /// The seeded source of every ID the engine persists, shared by all processes.
    pub(crate) id_source: Arc<dyn EngineIdSource>,
    /// How long closed runs stay before their purge; some seeds keep them past the end.
    pub(crate) retention_after_close: Duration,
    pub(crate) origin: tokio::time::Instant,
}

impl ClusterEnvironment {
    /// Returns a new process's view of the shared store, with its own fault draws.
    pub(crate) fn node_object_store(
        &self,
        process_label: &str,
        start_kind: &str,
        fault_seed: u64,
        fault_probability: f64,
    ) -> Arc<NodeObjectStore> {
        let object_store = Arc::new(NodeObjectStore::new(
            Arc::clone(&self.storage),
            process_label.to_owned(),
            start_kind.to_owned(),
            fault_seed,
        ));
        object_store.set_fault_probability(fault_probability);
        object_store
    }

    /// Writes the cluster object before any node starts, so its hash key is not random.
    pub(crate) async fn bootstrap_cluster_object(&self, random_bytes: [u8; 16]) {
        let mut cluster_id = random_bytes;
        // UUID version 7 and the RFC 4122 variant, which cluster validation requires.
        cluster_id[6] = (cluster_id[6] & 0x0F) | 0x70;
        cluster_id[8] = (cluster_id[8] & 0x3F) | 0x80;
        let cluster = ClusterObject {
            format_version: METADATA_FORMAT_VERSION,
            cluster_id: cluster_id.to_vec(),
            partition_count: PARTITION_COUNT,
            partition_hash_algorithm: PartitionHashAlgorithm::Siphash24 as i32,
            partition_hash_key: PARTITION_HASH_KEY.to_vec(),
            key_layout_version: KEY_LAYOUT_VERSION,
            created_at_unix_micros: START_UNIX_MICROS,
        };
        self.storage
            .store
            .put(
                CLUSTER_OBJECT_KEY,
                Bytes::from(cluster.encode_to_vec()),
                WriteCondition::CreateOnly,
            )
            .await
            .expect("the empty in-memory store accepts the cluster object");
    }
}

/// How a restarted process finds its local store.
pub(crate) enum LocalStoreStart {
    /// Restored from the newest snapshot, or empty: a new machine or a lost disk.
    Lost,
    /// A store the process continues with: the store at the crash, or an earlier copy of it.
    Existing(Arc<EngineLocalStore>),
}

/// Settings of one process start.
pub(crate) struct ProcessStart {
    pub(crate) process_label: String,
    pub(crate) local_store: LocalStoreStart,
    pub(crate) clock_offset_micros: i64,
    pub(crate) object_store: Arc<NodeObjectStore>,
}

/// A running node process.
pub(crate) struct NodeProcess {
    pub(crate) process_label: String,
    engine: EngineNode,
    pub(crate) registry: FlowRuntimeRegistry,
    effect_tasks: EffectTasksHandle,
    pub(crate) object_store: Arc<NodeObjectStore>,
    pub(crate) local_store: Arc<EngineLocalStore>,
    pub(crate) clock: Arc<SimulatedWallClock>,
}

/// Runtime parts the engine's factory builds, captured for the process.
struct RuntimeParts {
    registry: FlowRuntimeRegistry,
    effect_tasks: EffectTasksHandle,
    local_store: Arc<EngineLocalStore>,
}

impl NodeProcess {
    /// Starts a process: recovery, log replay, and runtime rebuild, as the server does.
    pub(crate) async fn start(
        environment: &ClusterEnvironment,
        start: ProcessStart,
    ) -> Result<Self, EngineStartError> {
        let ProcessStart {
            process_label,
            local_store,
            clock_offset_micros,
            object_store,
        } = start;
        let clock = Arc::new(SimulatedWallClock::new(
            environment.origin,
            clock_offset_micros,
        ));
        let local_store_config = match local_store {
            LocalStoreStart::Lost => LocalStoreConfig::InMemory,
            LocalStoreStart::Existing(store) => LocalStoreConfig::ExistingForTest(store),
        };
        let settings = run_settings(environment.retention_after_close);
        let applier: Arc<dyn CommandApplier> = Arc::new(DeterministicCommandApplier);
        let stream_store = Arc::new(
            StreamStore::new(&StreamStoreConfig::default())
                .expect("the disabled default Stream Store always starts"),
        );
        let worker_port = environment.workers.dispatcher_for(&process_label);
        let committed_log = Arc::clone(&environment.committed_log);
        let runtime_parts: Arc<Mutex<Option<RuntimeParts>>> = Arc::new(Mutex::new(None));
        let wall_clock: Arc<dyn WallClock> = clock.clone();
        let object_store_port: Arc<dyn ObjectStore> = object_store.clone();
        let factory_label = process_label.clone();
        let factory_parts = Arc::clone(&runtime_parts);
        let factory_clock = Arc::clone(&wall_clock);
        let factory_store = Arc::clone(&object_store_port);
        let factory_applier = Arc::clone(&applier);
        let started = EngineNode::start(
            &engine_node_config(local_store_config),
            EngineNodeDependencies {
                metrics: Arc::new(dexzd_metrics::NodeMetrics::unregistered()),
                object_store: object_store_port,
                clock: wall_clock,
                applier,
                id_source: Arc::clone(&environment.id_source),
            },
            move |services: EngineServices| {
                let partition_hasher = services.partition_hasher.clone();
                let hard_delete_objects = HardDeleteObjects::new(
                    factory_store,
                    Arc::new(NodeMetaUploadLedger::new(
                        services.local_store.clone(),
                        services.node_meta_lock.clone(),
                    )),
                    factory_clock.clone(),
                    partition_hasher.clone(),
                    services.id_source.clone(),
                );
                let (effect_runner, effect_tasks) = EffectRunner::new(
                    &effects_config(settings),
                    EffectDependencies {
                        metrics: Arc::new(dexzd_metrics::NodeMetrics::unregistered()),
                        dispatcher: worker_port.clone(),
                        hard_delete_objects: hard_delete_objects.clone(),
                        partition_hasher: partition_hasher.clone(),
                        clock: factory_clock,
                        validation_config: ValidationConfig::default(),
                        stream_store,
                        id_source: services.id_source.clone(),
                    },
                );
                let registry = FlowRuntimeRegistry::new(
                    &runtime_config(settings),
                    FlowRuntimeDependencies {
                        hasher: partition_hasher,
                        committer: Arc::new(services.committer),
                        applier: factory_applier,
                        effects: Arc::new(effect_runner.clone()),
                        rpc_invoker: worker_port,
                        hard_delete_reader: Arc::new(hard_delete_objects.clone()),
                        hard_delete_uploader: Arc::new(hard_delete_objects),
                        id_source: services.id_source,
                    },
                );
                let effect_tasks = effect_tasks.start(Arc::new(registry.clone()));
                let hooks = EngineRuntimeHooks {
                    committed_commands: committed_log.sink_for(&factory_label, registry.clone()),
                    rebuilder: Arc::new(effect_runner),
                };
                *factory_parts
                    .lock()
                    .expect("runtime parts lock is not poisoned") = Some(RuntimeParts {
                    registry,
                    effect_tasks,
                    local_store: services.local_store,
                });
                hooks
            },
        )
        .await;
        let parts = runtime_parts
            .lock()
            .expect("runtime parts lock is not poisoned")
            .take();
        let engine = match started {
            Ok(engine) => engine,
            Err(error) => {
                if let Some(parts) = parts {
                    parts.registry.close_long_polls();
                    parts.effect_tasks.shutdown().await;
                }
                return Err(error);
            }
        };
        let parts = parts.expect("the engine calls the runtime factory before it serves");
        Ok(Self {
            process_label,
            engine,
            registry: parts.registry,
            effect_tasks: parts.effect_tasks,
            object_store,
            local_store: parts.local_store,
            clock,
        })
    }

    /// Returns why the engine stopped, or `None` while it serves.
    pub(crate) fn stop_reason(&self) -> Option<NodeStopReason> {
        self.engine.stop_reason()
    }

    /// Partitions that stopped applying commands after an apply failure.
    pub(crate) fn halted_partitions(&self) -> Vec<dexzd_keyspace::PartitionId> {
        self.engine.halted_partitions()
    }

    pub(crate) fn is_registry_halted(&self) -> bool {
        self.registry.halted().is_cancelled()
    }

    /// Kills the process, cutting off its storage first; `tasks` gets its effect shutdown task.
    pub(crate) fn crash(self, tasks: &mut Vec<JoinHandle<()>>) {
        let Self {
            engine,
            registry,
            effect_tasks,
            object_store,
            ..
        } = self;
        object_store.cut_off();
        registry.close_long_polls();
        drop(engine);
        let effect_shutdown = tokio::spawn(effect_tasks.shutdown());
        tasks.push(effect_shutdown);
    }
}

/// Copies every row of `store` into a new in-memory store: a disk image at this instant.
pub(crate) fn copy_local_store(store: &EngineLocalStore) -> Arc<EngineLocalStore> {
    let view = store
        .consistent_view()
        .expect("an in-memory store always opens a consistent view");
    let mut batch = WriteBatch::new();
    for column_family in ColumnFamily::ALL {
        let rows = view
            .scan(
                column_family,
                None,
                None,
                ScanDirection::Forward,
                usize::MAX,
            )
            .expect("an in-memory view always scans");
        for (key, value) in rows {
            batch.put(column_family, key, value);
        }
    }
    let copy = InMemoryLocalStore::new();
    copy.write(batch)
        .expect("an in-memory store accepts puts of valid rows");
    Arc::new(EngineLocalStore::Memory(copy))
}

fn engine_node_config(local_store: LocalStoreConfig) -> EngineNodeConfig {
    EngineNodeConfig {
        node: NodeConfig {
            node_id: NODE_ID.to_owned(),
            incarnation: 1,
            internal_address: String::new(),
        },
        cluster: ClusterConfig {
            partition_count: PARTITION_COUNT,
        },
        lease: LeaseConfig {
            duration: LEASE_DURATION,
            takeover_grace: LEASE_TAKEOVER_GRACE,
            // The wait loop draws jitter from the thread RNG; the simulator schedules retries.
            fail_fast: true,
        },
        local_store,
        commit: CommitConfig {
            applier_lanes: 2,
            debug_replay_checks: true,
            ..CommitConfig::default()
        },
        snapshots: SnapshotConfig {
            log_threshold_bytes: 64 * 1024,
            interval: Duration::from_secs(20),
            retry_delay: Duration::from_secs(5),
            deletion_delay: Duration::from_secs(30),
            ..SnapshotConfig::default()
        },
        recovery: RecoveryConfig::default(),
        blocking_work: BlockingWorkPlacement::CallingTaskForTest,
    }
}

fn effects_config(settings: RunSettings) -> EffectsConfig {
    EffectsConfig {
        retry_max_backoff: Duration::from_secs(5),
        child_run_settings: settings,
        blocking_work: BlockingWorkPlacement::CallingTaskForTest,
        ..EffectsConfig::default()
    }
}

fn runtime_config(settings: RunSettings) -> RuntimeConfig {
    RuntimeConfig {
        // As on the server, hard-delete values have an object store.
        validation: ValidationConfig {
            has_blob_store: true,
            ..ValidationConfig::default()
        },
        long_poll_default_wait: Duration::from_secs(5),
        long_poll_max_wait: Duration::from_secs(10),
        rpc_default_timeout: Duration::from_secs(5),
        run_settings: settings,
        ..RuntimeConfig::default()
    }
}

fn run_settings(retention_after_close: Duration) -> RunSettings {
    RunSettings {
        retention_after_close_micros: duration_micros(retention_after_close),
        request_dedup_capacity: 64,
        ..default_run_settings()
    }
}
