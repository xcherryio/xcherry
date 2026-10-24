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

const LEASE_KEY: &str = "lease/node-a";

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
