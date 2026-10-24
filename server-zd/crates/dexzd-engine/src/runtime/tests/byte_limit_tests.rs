// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! The node's soft limit on uncommitted bytes, with small commands and large outcomes.

use std::time::Duration;

use dexzd_keyspace::RunId;
use prost::Message;

use crate::runtime::tests::fixtures::{
    attributes_set_command, flow, int_value, large_outcome, start_flow,
};
use crate::runtime::tests::test_support::RuntimeHarness;
use crate::runtime::{CommitUrgency, ProposalResult, RuntimeConfig, RuntimeError};

/// Starts `order-1` and `order-2`, then scripts `outcome_count` large outcomes for order-1.
async fn harness_with_two_flows(
    config: &RuntimeConfig,
    outcome_count: usize,
) -> (RuntimeHarness, RunId, RunId) {
    let harness = RuntimeHarness::with_config(config);
    let first_run = start_flow(&harness, "order-1").await.unwrap();
    let second_run = start_flow(&harness, "order-2").await.unwrap();
    let mut scripted = harness.applier.scripted.lock().unwrap();
    for _ in 0..outcome_count {
        scripted.push_back(Ok(large_outcome(first_run)));
    }
    drop(scripted);
    (harness, first_run, second_run)
}

async fn propose_small_command(
    harness: &RuntimeHarness,
    flow_id: &str,
    run_id: RunId,
) -> Result<ProposalResult, RuntimeError> {
    harness
        .registry
        .propose(
            flow(flow_id),
            attributes_set_command(flow_id, run_id, "count", int_value(1)),
            CommitUrgency::Batched,
        )
        .await
}

#[tokio::test(start_paused = true)]
async fn retained_bytes_at_the_limit_reject_a_small_command() {
    let (probe, probe_run, _) = harness_with_two_flows(&RuntimeConfig::default(), 1).await;
    propose_small_command(&probe, "order-1", probe_run)
        .await
        .unwrap();
    let entry_bytes = probe.registry.uncommitted_bytes();

    let config = RuntimeConfig {
        max_speculative_bytes_per_node: entry_bytes,
        speculation_capacity_wait: Duration::from_millis(100),
        ..RuntimeConfig::default()
    };
    let (harness, first_run, second_run) = harness_with_two_flows(&config, 2).await;
    propose_small_command(&harness, "order-1", first_run)
        .await
        .unwrap();
    assert_eq!(harness.registry.uncommitted_bytes(), entry_bytes);
    let error = propose_small_command(&harness, "order-2", second_run)
        .await
        .unwrap_err();
    assert!(
        matches!(error, RuntimeError::Unavailable { .. }),
        "{error:?}"
    );
    assert_eq!(harness.committer.pending_count(), 1);

    assert_eq!(harness.commit_all(), 1);
    assert_eq!(harness.registry.uncommitted_bytes(), 0);
    propose_small_command(&harness, "order-2", second_run)
        .await
        .unwrap();
}

#[tokio::test(start_paused = true)]
async fn large_outcome_overshoots_the_limit_by_one_outcome_and_blocks_admission() {
    let config = RuntimeConfig {
        max_speculative_bytes_per_node: 4096,
        speculation_capacity_wait: Duration::from_millis(100),
        ..RuntimeConfig::default()
    };
    let (harness, first_run, second_run) = harness_with_two_flows(&config, 2).await;
    propose_small_command(&harness, "order-1", first_run)
        .await
        .unwrap();
    let command_bytes = harness.submitted().last().unwrap().encoded_len();
    assert!(command_bytes < config.max_speculative_bytes_per_node);
    let retained = harness.registry.uncommitted_bytes();
    assert!(
        retained > config.max_speculative_bytes_per_node,
        "the small command's outcome overshoots the soft limit: {retained}"
    );

    let error = propose_small_command(&harness, "order-2", second_run)
        .await
        .unwrap_err();
    assert!(
        matches!(error, RuntimeError::Unavailable { .. }),
        "{error:?}"
    );
    assert_eq!(harness.commit_all(), 1);
    propose_small_command(&harness, "order-2", second_run)
        .await
        .unwrap();
}
