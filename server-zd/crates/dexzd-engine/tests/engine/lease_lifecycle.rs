// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::time::Duration;

use dexzd_cluster_metadata::{LeaseHandle, LeaseReleaseError, MetadataError};
use dexzd_engine::{EngineNode, EngineStartError, LocalStoreConfig};
use dexzd_object_store::ObjectStore;
use dexzd_protocol::dexzd::v1::NodeLease;
use prost::Message;
use tokio::time::Instant;

use crate::harness::TestCluster;
use crate::standby_takeover::start_node;

const RECOVERED_FLOW: &str = "recovered-flow";
const LEASE_KEY: &str = "lease/node-a";
const LOG_PREFIX: &str = "log/node-a/";

#[tokio::test(start_paused = true)]
async fn a_start_cancelled_during_recovery_releases_the_lease() {
    let mut cluster = TestCluster::new();
    let config = cluster.config(LocalStoreConfig::InMemory);
    let first = cluster.start_with(&config).await.expect("node starts");
    first.commit_prompt(RECOVERED_FLOW, 2).await;
    first
        .node
        .shutdown()
        .await
        .expect("the node releases its lease");
    // Only recovery reads the log, and it runs after the lease is taken.
    cluster
        .faults
        .delay_next_get_under(LOG_PREFIX, Duration::from_secs(60));
    let start = tokio::spawn(start_node(&cluster, config.clone()));
    cluster.faults.wait_until_delayed_operation_entered().await;
    assert!(
        is_lease_held(&cluster).await,
        "recovery runs under the lease"
    );
    start.abort();
    assert!(matches!(start.await, Err(error) if error.is_cancelled()));
    // Far below the ten-second lease, so only the release can free it.
    tokio::time::timeout(Duration::from_secs(1), async {
        while is_lease_held(&cluster).await {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the cancelled start releases its lease");
    let mut successor_config = config;
    successor_config.lease.fail_fast = true;
    let successor = cluster
        .start_with(&successor_config)
        .await
        .expect("a fail-fast successor takes the released lease");
    assert_eq!(successor.committed_count(RECOVERED_FLOW).await, 2);
    successor
        .node
        .shutdown()
        .await
        .expect("the successor releases its lease");
}

#[tokio::test(start_paused = true)]
async fn a_rejected_release_fails_shutdown_and_the_lease_stays_held() {
    let mut cluster = TestCluster::new();
    let mut config = cluster.config(LocalStoreConfig::InMemory);
    let node = cluster.start_with(&config).await.expect("node starts");
    cluster.faults.fail_puts_under("lease/");
    let released = node.node.shutdown().await;
    assert!(
        matches!(
            released,
            Err(LeaseReleaseError::Rejected {
                source: MetadataError::Store(_),
                ..
            })
        ),
        "{released:?}"
    );
    cluster.faults.clear();
    assert!(is_lease_held(&cluster).await);
    config.lease.fail_fast = true;
    assert!(matches!(
        cluster.start_with(&config).await,
        Err(EngineStartError::Metadata(MetadataError::LeaseHeld { .. }))
    ));
}

#[tokio::test(start_paused = true)]
async fn a_release_slower_than_its_budget_fails_shutdown_in_time() {
    let mut cluster = TestCluster::new();
    let config = cluster.config(LocalStoreConfig::InMemory);
    let node = cluster.start_with(&config).await.expect("node starts");
    cluster
        .faults
        .delay_next_put_under("lease/", Duration::from_secs(60));
    let started_at = Instant::now();
    let released = node.node.shutdown().await;
    assert!(
        matches!(
            released,
            Err(LeaseReleaseError::TimedOut { budget, .. }) if budget == LeaseHandle::RELEASE_BUDGET
        ),
        "{released:?}"
    );
    assert!(started_at.elapsed() <= EngineNode::SHUTDOWN_BUDGET);
}

/// Returns whether the stored lease names a holder; a release sets its expiry to `i64::MIN`.
async fn is_lease_held(cluster: &TestCluster) -> bool {
    let content = cluster
        .object_store
        .get(LEASE_KEY)
        .await
        .expect("read lease")
        .expect("lease exists");
    NodeLease::decode(content.bytes)
        .expect("decode lease")
        .expires_at_unix_micros
        != i64::MIN
}
