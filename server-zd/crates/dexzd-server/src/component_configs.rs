// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::time::Duration;

use dexzd_api::FlowServiceConfig;
use dexzd_cluster_metadata::NodeConfig;
use dexzd_engine::{
    CommitConfig, EffectsConfig, EngineNodeConfig, LocalStoreConfig, RecoveryConfig, RuntimeConfig,
    SnapshotConfig, default_run_settings,
};
use dexzd_log::LogReadAheadConfig;
use dexzd_request_validation::ValidationConfig;
use dexzd_storage::RocksDbStoreOptions;
use dexzd_stream_store::StreamStoreConfig;
use dexzd_worker_dispatch::MatchingConfig;

use crate::ServerError;
use crate::config::{Config, StreamStoreBackend};

/// The incarnation of this node's log stream; one node writes one stream until multi-node support.
const LOG_STREAM_INCARNATION: u32 = 1;

/// Typed configurations of every component, derived from one validated [`Config`].
pub(crate) struct ComponentConfigs {
    pub(crate) engine_node: EngineNodeConfig,
    pub(crate) runtime: RuntimeConfig,
    pub(crate) effects: EffectsConfig,
    pub(crate) matching: MatchingConfig,
    pub(crate) stream_store: StreamStoreConfig,
    pub(crate) flow_service: FlowServiceConfig,
    pub(crate) grpc_max_message_bytes: usize,
}

impl ComponentConfigs {
    /// Converts the configuration sections; fails when a value does not fit its component.
    pub(crate) fn from_config(config: &Config) -> Result<Self, ServerError> {
        let grpc_max_message_bytes =
            to_usize("api.grpcMaxMessageBytes", config.api.grpc_max_message_bytes)?;
        let max_value_bytes = config.values.max_value_bytes;
        let max_wait = Duration::from_secs(config.api.max_wait_seconds);
        let validation = ValidationConfig {
            maximum_flow_id_bytes: to_usize(
                "limits.maxFlowIdBytes",
                u64::from(config.limits.max_flow_id_bytes),
            )?,
            has_blob_store: true,
            ..ValidationConfig::default()
        };
        let mut run_settings = default_run_settings();
        run_settings.history_window_semantic_events = config.history.window_semantic_events;
        run_settings.history_prune_chunk_semantic_events = config.history.prune_chunk_size;
        run_settings.max_retained_superseded_bytes = config.history.max_retained_superseded_bytes;
        run_settings.retention_after_close_micros = duration_micros(
            "retention.closedFlowDuration",
            config.retention.closed_flow_duration,
        )?;
        run_settings.wire_inline_threshold_bytes =
            u32::try_from(config.values.wire_inline_threshold_bytes).map_err(|_| {
                ServerError::ConfigValueOutOfRange {
                    field: "values.wireInlineThresholdBytes",
                }
            })?;
        run_settings.max_value_bytes = max_value_bytes;
        run_settings.checkpoint_min_written_bytes = config.history.checkpoint_min_written_bytes;
        run_settings.checkpoint_max_commands = config.history.checkpoint_command_interval;
        run_settings.request_dedup_capacity = config.limits.max_request_ids_per_run;
        run_settings.max_run_state_bytes = config.limits.max_flow_state_bytes;
        run_settings.max_active_step_executions = config.limits.max_concurrent_steps_per_flow;
        let engine_node = EngineNodeConfig {
            node: NodeConfig {
                node_id: config.cluster.node_id.clone(),
                incarnation: LOG_STREAM_INCARNATION,
                internal_address: config.api.listen.to_string(),
            },
            cluster: dexzd_cluster_metadata::ClusterConfig {
                partition_count: config.cluster.partition_count,
            },
            lease: dexzd_cluster_metadata::LeaseConfig {
                duration: config.lease.duration,
                fail_fast: config.lease.fail_if_held,
                ..dexzd_cluster_metadata::LeaseConfig::default()
            },
            local_store: LocalStoreConfig::RocksDb {
                directory: config.local_storage.directory.clone(),
                options: RocksDbStoreOptions {
                    block_cache_bytes: to_usize(
                        "localStorage.blockCacheBytes",
                        config.local_storage.block_cache_bytes,
                    )?,
                    max_total_wal_bytes: config.local_storage.max_wal_bytes,
                },
            },
            commit: CommitConfig {
                debug_replay_checks: config
                    .commit
                    .debug_replay_checks
                    .unwrap_or(cfg!(debug_assertions)),
                batch_window: config.commit.batch_window,
                seal_at_bytes: to_usize("commit.sealAtBytes", config.commit.seal_at_bytes)?,
                max_in_flight_batches: to_usize(
                    "commit.maxInFlight",
                    u64::from(config.commit.max_in_flight),
                )?,
                ..CommitConfig::default()
            },
            snapshots: SnapshotConfig {
                log_threshold_bytes: config.snapshot.log_threshold_bytes,
                interval: config.snapshot.interval,
                retained_count: to_usize(
                    "snapshot.retainedCount",
                    u64::from(config.snapshot.retained_count),
                )?,
                deletion_delay: config.snapshot.log_deletion_delay,
                ..SnapshotConfig::default()
            },
            recovery: RecoveryConfig {
                log_read_ahead: LogReadAheadConfig {
                    max_read_ahead_batches: to_usize(
                        "recovery.readAheadBatches",
                        u64::from(config.recovery.read_ahead_batches),
                    )?,
                    max_buffered_bytes: to_usize(
                        "recovery.readAheadBytes",
                        config.recovery.read_ahead_bytes,
                    )?,
                },
                compact_after_replay_bytes: config.local_storage.compact_after_replay_bytes,
            },
        };
        let runtime = RuntimeConfig {
            max_speculative_commands_per_flow: to_usize(
                "limits.maxSpeculativeCommandsPerFlow",
                u64::from(config.limits.max_speculative_commands_per_flow),
            )?,
            max_speculative_bytes_per_node: to_usize(
                "limits.maxSpeculativeBytesPerNode",
                config.limits.max_speculative_bytes_per_node,
            )?,
            max_long_poll_waiters_per_node: to_usize(
                "limits.maxLongPollWaitersPerNode",
                u64::from(config.limits.max_long_poll_waiters_per_node),
            )?,
            max_long_poll_waiters_per_flow: to_usize(
                "limits.maxLongPollWaitersPerFlow",
                u64::from(config.limits.max_long_poll_waiters_per_flow),
            )?,
            long_poll_default_wait: max_wait,
            long_poll_max_wait: max_wait,
            rpc_default_timeout: config.api.default_rpc_timeout,
            max_worker_rpc_request_bytes: grpc_max_message_bytes,
            hydrate_value_references: config.values.hydrate_before_dispatch,
            run_settings,
            validation: validation.clone(),
            ..RuntimeConfig::default()
        };
        let effects = EffectsConfig {
            heartbeat_batch_window: config.commit.batch_window,
            max_worker_request_bytes: grpc_max_message_bytes,
            default_max_value_bytes: max_value_bytes,
            hydrate_before_dispatch: config.values.hydrate_before_dispatch,
            max_blocking_state_reads: to_usize(
                "limits.maxBlockingStateReads",
                u64::from(config.limits.max_blocking_state_reads),
            )?,
            ..EffectsConfig::default()
        };
        let matching = MatchingConfig {
            dispatch_wait: config.matching.dispatch_wait,
            sticky_wait: config.matching.sticky_wait,
            ping_interval: config.matching.ping_interval,
            max_streams: to_usize(
                "matching.maxStreams",
                u64::from(config.matching.max_streams),
            )?,
            max_concurrent_tasks_cap: to_usize(
                "matching.maxConcurrentTasksCap",
                u64::from(config.matching.max_concurrent_tasks_cap),
            )?,
            max_queue_length: to_usize(
                "matching.maxQueueLength",
                u64::from(config.matching.max_queue_length),
            )?,
            max_flow_types: to_usize(
                "matching.maxFlowTypes",
                u64::from(config.matching.max_flow_types),
            )?,
            max_flow_type_length: to_usize(
                "matching.maxFlowTypeLength",
                u64::from(config.matching.max_flow_type_length),
            )?,
            ..MatchingConfig::default()
        };
        let stream_store = StreamStoreConfig {
            enabled: config.stream_store.backend == StreamStoreBackend::Memory,
            max_message_bytes: to_usize(
                "streamStore.maxMessageBytes",
                config.stream_store.max_message_bytes,
            )?,
            max_read_messages: to_usize(
                "streamStore.maxReadMessages",
                u64::from(config.stream_store.max_read_messages),
            )?,
            trim_trigger_percent: config.stream_store.trim_trigger_percent,
            trim_target_percent: config.stream_store.trim_target_percent,
            ..StreamStoreConfig::default()
        };
        let flow_service = FlowServiceConfig {
            node_name: config.cluster.node_id.clone(),
            max_wait,
            ..FlowServiceConfig::default()
        };
        Ok(Self {
            engine_node,
            runtime,
            effects,
            matching,
            stream_store,
            flow_service,
            grpc_max_message_bytes,
        })
    }

    /// Returns the validation limits shared by the runtime and Worker response checks.
    pub(crate) fn validation(&self) -> &ValidationConfig {
        &self.runtime.validation
    }
}

fn to_usize(field: &'static str, value: u64) -> Result<usize, ServerError> {
    usize::try_from(value).map_err(|_| ServerError::ConfigValueOutOfRange { field })
}

fn duration_micros(field: &'static str, duration: Duration) -> Result<i64, ServerError> {
    i64::try_from(duration.as_micros()).map_err(|_| ServerError::ConfigValueOutOfRange { field })
}
