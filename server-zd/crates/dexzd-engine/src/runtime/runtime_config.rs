// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::time::Duration;

use dexzd_protocol::dexzd::v1::RunSettings;
use dexzd_request_validation::ValidationConfig;

/// Configuration section of the Flow runtime: speculation limits, long polls, RPC calls, and
/// the settings recorded into new runs.
#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    /// Largest number of uncommitted commands one Flow may have in its speculative overlay.
    /// Default 256. A proposal beyond it waits for commits to catch up, at most
    /// `speculation_capacity_wait`, and then fails with `Unavailable`.
    pub max_speculative_commands_per_flow: usize,
    /// Largest number of uncommitted write bytes (keys, values, and replies) across every
    /// overlay of the node. Default 64 MiB. Enforced like `max_speculative_commands_per_flow`;
    /// a single command larger than the limit is admitted only when no other bytes are
    /// uncommitted.
    pub max_speculative_bytes_per_node: usize,
    /// How long a proposal waits for speculation capacity before it fails with `Unavailable`.
    /// Default 5 s.
    pub speculation_capacity_wait: Duration,
    /// Largest number of long-poll waiters on the node. Default 100,000. Registering beyond it
    /// fails with `ResourceExhausted`.
    pub max_long_poll_waiters_per_node: usize,
    /// Largest number of long-poll waiters on one Flow. Default 1,000. Registering beyond it
    /// fails with `ResourceExhausted`.
    pub max_long_poll_waiters_per_flow: usize,
    /// Wait time of a WaitForFlow request that sets `wait_time_seconds` to 0, and the longest
    /// time one WaitForStepCompletion or WaitForAttribute call waits before it returns
    /// `LONG_POLL_TIME_OUT` so the client polls again. Default 60 s.
    pub long_poll_default_wait: Duration,
    /// Upper bound of any requested long-poll wait time. Default 60 s.
    pub long_poll_max_wait: Duration,
    /// Worker call timeout of an RPC that sets `timeout_seconds` to 0. Default 60 s.
    pub rpc_default_timeout: Duration,
    /// Settings recorded into every new run at FlowStart; apply never reads configuration.
    /// Defaults follow the `RunSettings` field documentation.
    pub run_settings: RunSettings,
    /// Limits and capabilities used to validate client requests. The default Flow config in
    /// it is applied before a StartFlow override.
    pub validation: ValidationConfig,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            max_speculative_commands_per_flow: 256,
            max_speculative_bytes_per_node: 64 * 1024 * 1024,
            speculation_capacity_wait: Duration::from_secs(5),
            max_long_poll_waiters_per_node: 100_000,
            max_long_poll_waiters_per_flow: 1_000,
            long_poll_default_wait: Duration::from_secs(60),
            long_poll_max_wait: Duration::from_secs(60),
            rpc_default_timeout: Duration::from_secs(60),
            run_settings: default_run_settings(),
            validation: ValidationConfig::default(),
        }
    }
}

/// Returns the documented defaults of every `RunSettings` field.
pub fn default_run_settings() -> RunSettings {
    RunSettings {
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
    }
}
