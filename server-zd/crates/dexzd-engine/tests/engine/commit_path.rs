// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::sync::Arc;
use std::time::Duration;

use dexzd_engine::{
    CommitError, CommitUrgency, FlowCommandCommitter, LocalStoreConfig, NodeStopReason,
    ReplayCheck, SpeculativeOutcomeMismatch,
};
use dexzd_storage::RocksDbStoreOptions;

use crate::harness::{
    BLOCKING_FLOW, CountingApplier, FAILING_FLOW, NONDETERMINISTIC_FLOW, PANICKING_FLOW,
    RecordingRuntime, TestCluster, is_partition_stopped,
};

#[tokio::test(start_paused = true)]
async fn committed_commands_are_stamped_applied_and_published_in_order() {
    let mut cluster = TestCluster::new();
    let test = cluster.start_in_memory().await;
    let mut stamped = Vec::new();
    let mut tickets = Vec::new();
    for _ in 0..3 {
        let (command, ticket) = test
            .submit("order-flow", CommitUrgency::Prompt)
            .expect("submit succeeds");
        stamped.push(command);
        tickets.push(ticket);
    }
    let mut positions = Vec::new();
    for ticket in tickets {
        let committed = ticket.wait().await.expect("command commits");
        positions.push(committed.position.seq);
    }
    assert!(positions.windows(2).all(|pair| pair[0] <= pair[1]));
    for command in &stamped {
        assert_eq!(command.epoch, 1);
        assert_eq!(command.behavior_version, 1);
        assert_eq!(command.command_id.len(), 16);
    }
    assert!(
        stamped
            .windows(2)
            .all(|pair| pair[0].proposed_at_micros < pair[1].proposed_at_micros)
    );
    assert_eq!(test.committed_count("order-flow").await, 3);
    let published = test.runtime.committed_command_ids("order-flow");
    let submitted: Vec<Vec<u8>> = stamped
        .into_iter()
        .map(|command| command.command_id)
        .collect();
    assert_eq!(published, submitted);
    test.node.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn batched_commands_share_a_frame_and_see_earlier_writes() {
    let mut cluster = TestCluster::new();
    let test = cluster.start_in_memory().await;
    let tickets: Vec<_> = (0..5)
        .map(|_| {
            test.submit("batched-flow", CommitUrgency::Batched)
                .expect("submit succeeds")
                .1
        })
        .collect();
    let mut seqs = Vec::new();
    for ticket in tickets {
        seqs.push(ticket.wait().await.expect("command commits").position.seq);
    }
    seqs.dedup();
    assert_eq!(seqs.len(), 1, "one batch holds every batched command");
    assert_eq!(test.committed_count("batched-flow").await, 5);
    test.node.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn stamps_stay_monotonic_when_the_wall_clock_moves_backwards() {
    let mut cluster = TestCluster::new();
    let test = cluster.start_in_memory().await;
    let (first, ticket) = test
        .submit("clock-flow", CommitUrgency::Prompt)
        .expect("submit succeeds");
    ticket.wait().await.expect("command commits");
    cluster.clock.shift(-60_000_000);
    let (second, ticket) = test
        .submit("clock-flow", CommitUrgency::Prompt)
        .expect("submit succeeds");
    ticket.wait().await.expect("command commits");
    assert_eq!(second.proposed_at_micros, first.proposed_at_micros + 1);
    test.node.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn submissions_beyond_the_unapplied_budget_are_rejected() {
    let mut cluster = TestCluster::new();
    let mut config = cluster.config(LocalStoreConfig::InMemory);
    config.commit.max_unapplied_bytes = 8;
    let test = cluster
        .start(
            &config,
            Arc::new(CountingApplier::default()),
            Arc::new(RecordingRuntime::default()),
        )
        .await
        .expect("node starts");
    let rejected = test.submit("large-flow", CommitUrgency::Prompt);
    assert!(
        matches!(&rejected, Err(CommitError::Unavailable { detail }) if detail.contains("backlog")),
        "{rejected:?}"
    );
    test.node.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn apply_failure_stops_only_its_partition() {
    for failing_flow in [FAILING_FLOW, PANICKING_FLOW] {
        let mut cluster = TestCluster::new();
        let test = cluster.start_in_memory().await;
        let healthy_flow = test.flow_in_other_partition(&[failing_flow]);
        let failing_partition = test.context(failing_flow).partition;
        let (_, ticket) = test
            .submit(failing_flow, CommitUrgency::Prompt)
            .expect("submit succeeds");
        let error = ticket.wait().await.expect_err("apply fails");
        assert!(is_partition_stopped(&error), "{error:?}");
        assert_eq!(test.node.halted_partitions(), vec![failing_partition]);
        let error = test
            .submit(failing_flow, CommitUrgency::Prompt)
            .expect_err("the partition stopped");
        assert!(is_partition_stopped(&error), "{error:?}");
        test.commit_prompt(&healthy_flow, 2).await;
        assert_eq!(test.committed_count(&healthy_flow).await, 2);
        assert_eq!(test.node.stop_reason(), None);
        test.node.shutdown().await;
    }
}

#[tokio::test(start_paused = true)]
async fn repeated_apply_check_stops_a_nondeterministic_partition() {
    let mut cluster = TestCluster::new();
    let mut config = cluster.config(LocalStoreConfig::InMemory);
    config.commit.debug_replay_checks = true;
    let test = cluster
        .start(
            &config,
            Arc::new(CountingApplier::default()),
            Arc::new(RecordingRuntime::default()),
        )
        .await
        .expect("node starts");
    test.commit_prompt("deterministic-flow", 2).await;
    let (_, ticket) = test
        .submit(NONDETERMINISTIC_FLOW, CommitUrgency::Prompt)
        .expect("submit succeeds");
    let error = ticket.wait().await.expect_err("apply differs");
    assert!(is_partition_stopped(&error), "{error:?}");
    assert_eq!(test.applier.applied_commands().len(), 6);
    test.node.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn speculative_outcome_check_compares_only_when_enabled() {
    for is_checking in [false, true] {
        let mut cluster = TestCluster::new();
        let mut config = cluster.config(LocalStoreConfig::InMemory);
        config.commit.debug_replay_checks = is_checking;
        let test = cluster
            .start(
                &config,
                Arc::new(CountingApplier::default()),
                Arc::new(RecordingRuntime::default()),
            )
            .await
            .expect("node starts");
        let partition = test.context("speculative-flow").partition;
        let (_, ticket) = test
            .submit("speculative-flow", CommitUrgency::Prompt)
            .expect("submit succeeds");
        let committed = ticket.wait().await.expect("command commits");
        let committer = test.node.committer();
        committer
            .check_speculative_outcome(partition, &committed.outcome, &committed)
            .expect("identical outcomes match");
        let mut diverged = committed.outcome.clone();
        diverged.writes.pop();
        let result = committer.check_speculative_outcome(partition, &diverged, &committed);
        if is_checking {
            let mismatch = result.expect_err("different outcomes mismatch");
            assert_eq!(mismatch.check, ReplayCheck::SpeculativeVersusCommitted);
            assert_eq!(test.node.halted_partitions(), vec![partition]);
        } else {
            result.expect("checks are disabled");
            assert!(test.node.halted_partitions().is_empty());
        }
        test.node.shutdown().await;
    }
}

#[tokio::test(start_paused = true)]
async fn a_speculative_mismatch_reported_by_the_runtime_stops_the_partition() {
    let mut cluster = TestCluster::new();
    let mut config = cluster.config(LocalStoreConfig::InMemory);
    config.commit.debug_replay_checks = true;
    let test = cluster
        .start(
            &config,
            Arc::new(CountingApplier::default()),
            Arc::new(RecordingRuntime::default()),
        )
        .await
        .expect("node starts");
    let other_flow = test.flow_in_other_partition(&["seam-flow"]);
    let partition = test.context("seam-flow").partition;
    let (_, ticket) = test
        .submit("seam-flow", CommitUrgency::Prompt)
        .expect("submit succeeds");
    let committed = ticket.wait().await.expect("command commits");
    let committer: &dyn FlowCommandCommitter = test.node.committer();
    committer
        .check_speculative_outcome(partition, &committed.outcome, &committed)
        .expect("identical outcomes match");
    let mut diverged = committed.outcome.clone();
    diverged.writes.pop();
    let mismatch: SpeculativeOutcomeMismatch = committer
        .check_speculative_outcome(partition, &diverged, &committed)
        .expect_err("different outcomes mismatch");
    assert!(mismatch.detail.contains("speculative versus committed"));
    assert_eq!(test.node.halted_partitions(), vec![partition]);
    let rejected = test
        .submit("seam-flow", CommitUrgency::Prompt)
        .expect_err("the partition stopped");
    assert!(is_partition_stopped(&rejected), "{rejected:?}");
    test.commit_prompt(&other_flow, 1).await;
    test.node.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn a_frame_still_applying_when_the_node_stops_is_not_written() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let mut cluster = TestCluster::new();
    let config = cluster.config(LocalStoreConfig::RocksDb {
        directory: directory.path().join("store"),
        options: RocksDbStoreOptions::default(),
    });
    let first = cluster
        .start(
            &config,
            Arc::new(CountingApplier::default()),
            Arc::new(RecordingRuntime::default()),
        )
        .await
        .expect("node starts");
    let (_, ticket) = first
        .submit(BLOCKING_FLOW, CommitUrgency::Prompt)
        .expect("submit succeeds");
    first.applier.gate.wait_until_entered().await;
    let stopped = first.node.wait_for_stop();
    let shutdown = tokio::spawn(first.node.shutdown());
    assert_eq!(stopped.await, NodeStopReason::Shutdown);
    first.applier.gate.open();
    shutdown.await.expect("shutdown completes");
    assert!(matches!(
        ticket.wait().await,
        Err(CommitError::NodeStopping { .. })
    ));
    assert!(first.runtime.committed_commands().is_empty());

    let applier = Arc::new(CountingApplier::default());
    applier.gate.open();
    let second = cluster
        .start(&config, applier, Arc::new(RecordingRuntime::default()))
        .await
        .expect("node restarts");
    let replayed = second
        .applier
        .applied_commands()
        .into_iter()
        .filter(|command| command.flow_id == BLOCKING_FLOW)
        .count();
    assert_eq!(replayed, 1);
    assert_eq!(second.committed_count(BLOCKING_FLOW).await, 1);
    second.node.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn a_committed_view_keeps_the_state_it_was_opened_with() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let local_stores = [
        LocalStoreConfig::InMemory,
        LocalStoreConfig::RocksDb {
            directory: directory.path().join("store"),
            options: RocksDbStoreOptions::default(),
        },
    ];
    for local_store in local_stores {
        let mut cluster = TestCluster::new();
        let test = cluster
            .start(
                &cluster.config(local_store),
                Arc::new(CountingApplier::default()),
                Arc::new(RecordingRuntime::default()),
            )
            .await
            .expect("node starts");
        test.commit_prompt("view-flow", 1).await;
        let earlier_view = test
            .node
            .committer()
            .committed_view()
            .expect("committed view opens");
        test.commit_prompt("view-flow", 1).await;
        assert_eq!(test.count_in_view(earlier_view, "view-flow").await, 1);
        assert_eq!(test.committed_count("view-flow").await, 2);
        test.node.shutdown().await;
    }
}

#[tokio::test(start_paused = true)]
async fn object_storage_outage_stops_the_node() {
    let mut cluster = TestCluster::new();
    let test = cluster.start_in_memory().await;
    test.commit_prompt("outage-flow", 1).await;
    cluster.faults.fail_puts_under("log/");
    let (_, ticket) = test
        .submit("outage-flow", CommitUrgency::Prompt)
        .expect("submit succeeds");
    assert!(matches!(
        ticket.wait().await,
        Err(CommitError::NodeStopping { .. })
    ));
    assert!(matches!(
        test.node.wait_for_stop().await,
        NodeStopReason::CommitPipelineFailed { .. }
    ));
    assert!(matches!(
        test.submit("outage-flow", CommitUrgency::Prompt),
        Err(CommitError::NodeStopping { .. })
    ));
    test.node.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn lost_lease_stops_serving() {
    let mut cluster = TestCluster::new();
    let test = cluster.start_in_memory().await;
    cluster.faults.fail_puts_under("lease/");
    let reason = tokio::time::timeout(Duration::from_secs(60), test.node.wait_for_stop())
        .await
        .expect("the node stops before the lease could be renewed");
    assert!(matches!(reason, NodeStopReason::LeaseLost { .. }));
    assert!(matches!(
        test.submit("lease-flow", CommitUrgency::Prompt),
        Err(CommitError::NodeStopping { .. })
    ));
    test.node.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn shutdown_fails_waiting_tickets() {
    let mut cluster = TestCluster::new();
    let test = cluster.start_in_memory().await;
    let (_, ticket) = test
        .submit("shutdown-flow", CommitUrgency::Batched)
        .expect("submit succeeds");
    test.node.shutdown().await;
    let result = ticket.wait().await;
    assert!(
        matches!(result, Ok(_) | Err(CommitError::NodeStopping { .. })),
        "{result:?}"
    );
}
