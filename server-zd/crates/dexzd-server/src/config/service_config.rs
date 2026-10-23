// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::fmt;
use std::net::{Ipv4Addr, SocketAddr};
use std::time::Duration;

use serde::{Deserialize, Serialize};

/// Configures the public FlowService and WorkerTaskService gRPC listener.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default, rename_all = "camelCase", deny_unknown_fields)]
pub struct ApiConfig {
    /// Defaults to 0.0.0.0:8801; bind address for the plaintext public gRPC services.
    pub listen: SocketAddr,
    /// Defaults to 32 MiB; maximum incoming and outgoing gRPC message size, above maxValueBytes for envelope headroom.
    pub grpc_max_message_bytes: u64,
    /// Defaults to 60 s; upper bound on one transport long poll, reusing the Go maxWaitSeconds key.
    #[serde(with = "humantime_serde")]
    pub max_wait_seconds: Duration,
    /// Defaults to 60 s; timeout used for RPC calls that do not specify one.
    #[serde(with = "humantime_serde")]
    pub default_rpc_timeout: Duration,
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            listen: SocketAddr::from((Ipv4Addr::UNSPECIFIED, 8801)),
            grpc_max_message_bytes: 32 * 1024 * 1024,
            max_wait_seconds: Duration::from_secs(60),
            default_rpc_timeout: Duration::from_secs(60),
        }
    }
}

/// Controls pull Worker dispatch waits, registration capacity, and idle pings.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default, rename_all = "camelCase", deny_unknown_fields)]
pub struct MatchingConfig {
    /// Defaults to 10 s; total time to find a Worker for a regular attempt.
    #[serde(with = "humantime_serde")]
    pub dispatch_wait: Duration,
    /// Defaults to 5 s; time to try a connected sticky Worker before fallback, no greater than dispatchWait.
    #[serde(with = "humantime_serde")]
    pub sticky_wait: Duration,
    /// Defaults to 256; maximum registered streams for one appWorkerID on this node.
    pub max_concurrent_tasks_cap: u32,
    /// Defaults to 100000; maximum registered pull Worker streams on this node.
    pub max_streams: u32,
    /// Defaults to 256 characters; maximum queue and fallback queue name length.
    pub max_queue_length: u32,
    /// Defaults to 256; maximum Flow types in one Worker registration.
    pub max_flow_types: u32,
    /// Defaults to 256 characters; maximum length of each registered Flow type.
    pub max_flow_type_length: u32,
    /// Defaults to 20 s; interval between pings on idle Worker streams.
    #[serde(with = "humantime_serde")]
    pub ping_interval: Duration,
}

impl Default for MatchingConfig {
    fn default() -> Self {
        Self {
            dispatch_wait: Duration::from_secs(10),
            sticky_wait: Duration::from_secs(5),
            max_concurrent_tasks_cap: 256,
            max_streams: 100_000,
            max_queue_length: 256,
            max_flow_types: 256,
            max_flow_type_length: 256,
            ping_interval: Duration::from_secs(20),
        }
    }
}

/// Selects the supported Stream Store backend.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum StreamStoreBackend {
    /// Default; Stream operations are unavailable.
    #[default]
    Disabled,
    /// Process-local Stream storage for development or a single node.
    Memory,
}

/// Bounds Stream message size, page size, accounting, and trimming.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default, rename_all = "camelCase", deny_unknown_fields)]
pub struct StreamStoreConfig {
    /// Defaults to disabled; dev mode overrides this to memory storage.
    pub backend: StreamStoreBackend,
    /// Defaults to 100 KiB; maximum serialized Value in one Stream message.
    pub max_message_bytes: u64,
    /// Defaults to 1000; maximum messages in one ListStreamMessages page.
    pub max_read_messages: u32,
    /// Defaults to 512 bytes; accounting added to each message beyond value, Flow ID, and source.
    pub estimated_message_overhead_bytes: u64,
    /// Defaults to 90%; asynchronous trimming starts at this capacity usage.
    pub trim_trigger_percent: u8,
    /// Defaults to 80%; asynchronous trimming stops below this capacity usage, less than trimTriggerPercent.
    pub trim_target_percent: u8,
    /// Defaults to 256; maximum messages removed in one trim batch.
    pub background_trim_batch_size: u32,
    /// Defaults to 4; maximum concurrent background trim jobs in one process.
    pub trim_workers: u32,
}

impl Default for StreamStoreConfig {
    fn default() -> Self {
        Self {
            backend: StreamStoreBackend::Disabled,
            max_message_bytes: 100 * 1024,
            max_read_messages: 1_000,
            estimated_message_overhead_bytes: 512,
            trim_trigger_percent: 90,
            trim_target_percent: 80,
            background_trim_batch_size: 256,
            trim_workers: 4,
        }
    }
}

/// Selects the search implementation used by SearchFlows.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum SearchBackend {
    /// Default; queries committed local search documents with a scan budget.
    #[default]
    Builtin,
    /// Queries the external ParadeDB projection using the configured connection string.
    Paradedb,
}

/// Configures builtin search or the optional ParadeDB projection.
#[derive(Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default, rename_all = "camelCase", deny_unknown_fields)]
pub struct SearchConfig {
    /// Defaults to builtin; production deployments may select paradedb for indexed queries.
    pub backend: SearchBackend,
    /// Defaults to 100000 documents; maximum builtin documents inspected by one SearchFlows query.
    pub builtin_scan_budget_documents: u32,
    /// Defaults to empty; required when backend is paradedb and identifies the PostgreSQL-compatible endpoint.
    pub parade_db_connection_string: String,
    /// Defaults to 10; maximum ParadeDB connections held by one node.
    pub parade_db_connection_pool_size: u32,
    /// Defaults to public; schema containing the external search projection.
    pub parade_db_schema: String,
    /// Defaults to 2 s; maximum wait for ParadeDB projection to catch up to the node's committed position.
    #[serde(with = "humantime_serde")]
    pub read_your_writes_wait: Duration,
}

impl fmt::Debug for SearchConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SearchConfig")
            .field("backend", &self.backend)
            .field(
                "builtin_scan_budget_documents",
                &self.builtin_scan_budget_documents,
            )
            .field("parade_db_connection_string", &"[redacted]")
            .field(
                "parade_db_connection_pool_size",
                &self.parade_db_connection_pool_size,
            )
            .field("parade_db_schema", &self.parade_db_schema)
            .field("read_your_writes_wait", &self.read_your_writes_wait)
            .finish()
    }
}

impl Default for SearchConfig {
    fn default() -> Self {
        Self {
            backend: SearchBackend::Builtin,
            builtin_scan_budget_documents: 100_000,
            parade_db_connection_string: String::new(),
            parade_db_connection_pool_size: 10,
            parade_db_schema: "public".to_owned(),
            read_your_writes_wait: Duration::from_secs(2),
        }
    }
}
