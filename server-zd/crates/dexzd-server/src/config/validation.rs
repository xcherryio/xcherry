// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::time::Duration;

use dexzd_keyspace::validate_node_identifier;

use super::{Config, ConfigError, CredentialSource, SearchBackend};

impl Config {
    /// Checks cross-section limits and reports the first failing camelCase YAML field path.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if !self.metrics.listen.is_empty() {
            self.metrics
                .listen
                .parse::<std::net::SocketAddr>()
                .map_err(|_| ConfigError::Invalid {
                    field: "metrics.listen",
                    message: "must be empty or a socket bind address such as 127.0.0.1:9090"
                        .to_owned(),
                })?;
        }
        self.validate_cluster_and_storage()?;
        self.validate_commit_and_recovery()?;
        self.validate_values_and_api()?;
        self.validate_matching_and_streams()?;
        self.validate_search_and_limits()?;
        Ok(())
    }

    fn validate_cluster_and_storage(&self) -> Result<(), ConfigError> {
        validate_node_identifier(&self.cluster.node_id).map_err(|error| ConfigError::Invalid {
            field: "cluster.nodeId",
            message: error.to_string(),
        })?;
        ensure(
            self.cluster.partition_count.is_power_of_two()
                && self.cluster.partition_count <= 65_536,
            "cluster.partitionCount",
            "must be a power of two between 1 and 65536",
        )?;
        ensure(
            !self.object_store.region.trim().is_empty(),
            "objectStore.region",
            "must be non-empty",
        )?;
        ensure(
            !self.object_store.bucket.trim().is_empty(),
            "objectStore.bucket",
            "must be non-empty",
        )?;
        validate_endpoint("objectStore.endpoint", &self.object_store.endpoint)?;
        validate_prefix("objectStore.prefix", &self.object_store.prefix)?;
        if self.object_store.credentials_source == CredentialSource::Environment {
            ensure(
                self.object_store.credentials.is_some(),
                "objectStore.credentialsSource",
                "environment credentials must be resolved before serving",
            )?;
        }
        if let Some(hard_delete) = &self.object_store.hard_delete_store {
            ensure(
                !hard_delete.region.trim().is_empty(),
                "objectStore.hardDeleteStore.region",
                "must be non-empty",
            )?;
            ensure(
                !hard_delete.bucket.trim().is_empty(),
                "objectStore.hardDeleteStore.bucket",
                "must be non-empty",
            )?;
            validate_endpoint(
                "objectStore.hardDeleteStore.endpoint",
                &hard_delete.endpoint,
            )?;
            validate_prefix("objectStore.hardDeleteStore.prefix", &hard_delete.prefix)?;
            if hard_delete.credentials_source == CredentialSource::Environment {
                ensure(
                    hard_delete.credentials.is_some(),
                    "objectStore.hardDeleteStore.credentialsSource",
                    "environment credentials must be resolved before serving",
                )?;
            }
        }
        ensure(
            !self.local_storage.directory.as_os_str().is_empty(),
            "localStorage.directory",
            "must be non-empty",
        )?;
        ensure(
            self.local_storage.block_cache_bytes > 0,
            "localStorage.blockCacheBytes",
            "must be positive",
        )?;
        ensure(
            self.local_storage.max_wal_bytes >= 64 * 1024 * 1024,
            "localStorage.maxWalBytes",
            "must be at least 64 MiB, the memtable size of one column family",
        )?;
        if !self.dev.data_directory.as_os_str().is_empty() {
            ensure(
                !self.dev.s3_data_directory.as_os_str().is_empty(),
                "dev.s3DataDirectory",
                "must be set when dev.dataDirectory is set",
            )?;
        }
        Ok(())
    }

    fn validate_commit_and_recovery(&self) -> Result<(), ConfigError> {
        ensure(
            self.commit.batch_window > Duration::ZERO,
            "commit.batchWindow",
            "must be positive",
        )?;
        ensure(
            self.commit.max_in_flight > 0,
            "commit.maxInFlight",
            "must be positive",
        )?;
        ensure(
            self.commit.seal_at_bytes > 0,
            "commit.sealAtBytes",
            "must be positive",
        )?;
        ensure(
            self.commit.multipart_threshold_bytes >= 5 * 1024 * 1024,
            "commit.multipartThresholdBytes",
            "must be at least the 5 MiB S3 multipart part minimum",
        )?;
        ensure(
            self.commit.multipart_threshold_bytes >= self.commit.seal_at_bytes,
            "commit.multipartThresholdBytes",
            "must be no smaller than commit.sealAtBytes",
        )?;
        ensure(
            self.lease.duration > Duration::ZERO,
            "lease.duration",
            "must be positive",
        )?;
        ensure(
            self.snapshot.log_threshold_bytes > 0,
            "snapshot.logThresholdBytes",
            "must be positive",
        )?;
        ensure(
            self.snapshot.interval > Duration::ZERO,
            "snapshot.interval",
            "must be positive",
        )?;
        ensure(
            self.snapshot.retained_count > 0,
            "snapshot.retainedCount",
            "must be positive",
        )?;
        ensure(
            self.snapshot.log_deletion_delay > Duration::ZERO,
            "snapshot.logDeletionDelay",
            "must be positive",
        )?;
        ensure(
            (1..=1024).contains(&self.recovery.read_ahead_batches),
            "recovery.readAheadBatches",
            "must be between 1 and 1024",
        )?;
        ensure(
            self.recovery.read_ahead_bytes > 0,
            "recovery.readAheadBytes",
            "must be positive",
        )?;
        ensure(
            self.history.window_semantic_events > 0,
            "history.windowSemanticEvents",
            "must be positive",
        )?;
        ensure(
            self.history.prune_chunk_size > 0
                && self.history.prune_chunk_size <= self.history.window_semantic_events,
            "history.pruneChunkSize",
            "must be positive and no larger than history.windowSemanticEvents",
        )?;
        ensure(
            self.history.checkpoint_min_written_bytes > 0,
            "history.checkpointMinWrittenBytes",
            "must be positive",
        )?;
        ensure(
            self.history.checkpoint_command_interval > 0,
            "history.checkpointCommandInterval",
            "must be positive",
        )?;
        ensure(
            self.history.max_retained_superseded_bytes >= self.values.max_value_bytes,
            "history.maxRetainedSupersededBytes",
            "must hold at least one maximum-sized value",
        )?;
        ensure(
            self.retention.closed_flow_duration > Duration::ZERO,
            "retention.closedFlowDuration",
            "must be positive",
        )?;
        Ok(())
    }

    fn validate_values_and_api(&self) -> Result<(), ConfigError> {
        ensure(
            self.values.max_value_bytes > 0,
            "values.maxValueBytes",
            "must be positive",
        )?;
        ensure(
            self.values.wire_inline_threshold_bytes < self.values.max_value_bytes,
            "values.wireInlineThresholdBytes",
            "must be smaller than values.maxValueBytes",
        )?;
        ensure(
            self.values.blob_db_min_value_bytes > self.values.wire_inline_threshold_bytes
                && self.values.blob_db_min_value_bytes <= self.values.max_value_bytes,
            "values.blobDbMinValueBytes",
            "must exceed the wire inline threshold and not exceed maxValueBytes",
        )?;
        ensure(
            self.api.listen.port() != 0,
            "api.listen",
            "must bind a nonzero TCP port",
        )?;
        ensure(
            self.api.grpc_max_message_bytes > self.values.max_value_bytes,
            "api.grpcMaxMessageBytes",
            "must exceed values.maxValueBytes for the request envelope",
        )?;
        ensure(
            self.api.max_wait_seconds > 0,
            "api.maxWaitSeconds",
            "must be positive",
        )?;
        ensure(
            self.api.default_rpc_timeout > Duration::ZERO,
            "api.defaultRpcTimeout",
            "must be positive",
        )?;
        Ok(())
    }

    fn validate_matching_and_streams(&self) -> Result<(), ConfigError> {
        ensure(
            self.matching.dispatch_wait > Duration::ZERO,
            "matching.dispatchWait",
            "must be positive",
        )?;
        ensure(
            self.matching.sticky_wait > Duration::ZERO
                && self.matching.sticky_wait <= self.matching.dispatch_wait,
            "matching.stickyWait",
            "must be positive and no greater than matching.dispatchWait",
        )?;
        ensure(
            self.matching.max_concurrent_tasks_cap > 0,
            "matching.maxConcurrentTasksCap",
            "must be positive",
        )?;
        ensure(
            self.matching.max_streams >= self.matching.max_concurrent_tasks_cap,
            "matching.maxStreams",
            "must be at least matching.maxConcurrentTasksCap",
        )?;
        ensure(
            self.matching.max_queue_length > 0,
            "matching.maxQueueLength",
            "must be positive",
        )?;
        ensure(
            self.matching.max_flow_types > 0,
            "matching.maxFlowTypes",
            "must be positive",
        )?;
        ensure(
            self.matching.max_flow_type_length > 0,
            "matching.maxFlowTypeLength",
            "must be positive",
        )?;
        ensure(
            self.matching.ping_interval > Duration::ZERO,
            "matching.pingInterval",
            "must be positive",
        )?;
        ensure(
            self.stream_store.max_message_bytes > 0,
            "streamStore.maxMessageBytes",
            "must be positive",
        )?;
        ensure(
            (1..=1_000).contains(&self.stream_store.max_read_messages),
            "streamStore.maxReadMessages",
            "must be between 1 and 1000",
        )?;
        ensure(
            (1..=99).contains(&self.stream_store.trim_trigger_percent),
            "streamStore.trimTriggerPercent",
            "must be between 1 and 99",
        )?;
        ensure(
            self.stream_store.trim_target_percent < self.stream_store.trim_trigger_percent,
            "streamStore.trimTargetPercent",
            "must be below streamStore.trimTriggerPercent",
        )?;
        ensure(
            self.stream_store.background_trim_batch_size > 0,
            "streamStore.backgroundTrimBatchSize",
            "must be positive",
        )?;
        ensure(
            self.stream_store.trim_batch_yield_time > Duration::ZERO,
            "streamStore.trimBatchYieldTime",
            "must be positive",
        )?;
        ensure(
            self.stream_store.trim_workers > 0,
            "streamStore.trimWorkers",
            "must be positive",
        )?;
        Ok(())
    }

    fn validate_search_and_limits(&self) -> Result<(), ConfigError> {
        ensure(
            self.search.builtin_scan_budget_documents > 0,
            "search.builtinScanBudgetDocuments",
            "must be positive",
        )?;
        if self.search.backend == SearchBackend::Paradedb {
            ensure(
                !self.search.parade_db_connection_string.trim().is_empty(),
                "search.paradeDbConnectionString",
                "is required for the paradedb backend",
            )?;
        }
        ensure(
            self.search.parade_db_connection_pool_size > 0,
            "search.paradeDbConnectionPoolSize",
            "must be positive",
        )?;
        ensure(
            !self.search.parade_db_schema.trim().is_empty(),
            "search.paradeDbSchema",
            "must be non-empty",
        )?;
        ensure(
            is_unquoted_identifier(&self.search.parade_db_schema),
            "search.paradeDbSchema",
            "must be an unquoted PostgreSQL identifier of letters, digits, and underscores",
        )?;
        ensure(
            self.search.read_your_writes_wait > Duration::ZERO,
            "search.readYourWritesWait",
            "must be positive",
        )?;
        ensure(
            self.limits.max_flow_state_bytes >= self.values.max_value_bytes,
            "limits.maxFlowStateBytes",
            "must hold at least one maximum-sized value",
        )?;
        ensure(
            self.limits.max_channel_messages > 0,
            "limits.maxChannelMessages",
            "must be positive",
        )?;
        ensure(
            self.limits.max_concurrent_steps_per_flow > 0,
            "limits.maxConcurrentStepsPerFlow",
            "must be positive",
        )?;
        ensure(
            self.limits.max_speculative_commands_per_flow > 0,
            "limits.maxSpeculativeCommandsPerFlow",
            "must be positive",
        )?;
        ensure(
            self.limits.max_speculative_bytes_per_node >= self.values.max_value_bytes,
            "limits.maxSpeculativeBytesPerNode",
            "must hold at least one maximum-sized value",
        )?;
        ensure(
            self.limits.max_blocking_state_reads > 0,
            "limits.maxBlockingStateReads",
            "must be positive",
        )?;
        ensure(
            self.limits.max_long_poll_waiters_per_node > 0,
            "limits.maxLongPollWaitersPerNode",
            "must be positive",
        )?;
        ensure(
            self.limits.max_long_poll_waiters_per_flow > 0
                && self.limits.max_long_poll_waiters_per_flow
                    <= self.limits.max_long_poll_waiters_per_node,
            "limits.maxLongPollWaitersPerFlow",
            "must be positive and no greater than the node limit",
        )?;
        ensure(
            self.limits.max_request_ids_per_run > 0,
            "limits.maxRequestIdsPerRun",
            "must be positive",
        )?;
        ensure(
            self.limits.max_flow_id_bytes > 0,
            "limits.maxFlowIdBytes",
            "must be positive",
        )?;
        Ok(())
    }
}

fn ensure(condition: bool, field: &'static str, message: &'static str) -> Result<(), ConfigError> {
    if condition {
        Ok(())
    } else {
        Err(ConfigError::Invalid {
            field,
            message: message.to_owned(),
        })
    }
}

fn validate_endpoint(field: &'static str, endpoint: &str) -> Result<(), ConfigError> {
    ensure(
        endpoint.is_empty()
            || ((endpoint.starts_with("http://") || endpoint.starts_with("https://"))
                && !endpoint.chars().any(char::is_whitespace)),
        field,
        "must be an HTTP(S) URL without whitespace",
    )
}

fn validate_prefix(field: &'static str, prefix: &str) -> Result<(), ConfigError> {
    ensure(!prefix.starts_with('/'), field, "must not begin with '/'")
}

/// The schema goes into a `search_path` connection option, which splits on commas and spaces.
fn is_unquoted_identifier(name: &str) -> bool {
    let mut characters = name.chars();
    characters
        .next()
        .is_some_and(|first| first.is_ascii_alphabetic() || first == '_')
        && characters.all(|character| character.is_ascii_alphanumeric() || character == '_')
}
