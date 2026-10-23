// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Lifecycle clock and pass controls exposed through the admin interface.

use aws_sdk_s3::types::{
    BucketLifecycleConfiguration, ExpirationStatus, LifecycleExpiration, LifecycleRule,
    LifecycleRuleFilter,
};
use serde_json::{Value, json};

use crate::support::RunningServer;

const DAY_MS: i64 = 86_400_000;

#[tokio::test]
async fn advancing_the_lifecycle_clock_and_running_a_pass_expires_objects() {
    let server = RunningServer::start(&[]).await;
    server
        .client
        .put_bucket_lifecycle_configuration()
        .bucket("testing")
        .lifecycle_configuration(
            BucketLifecycleConfiguration::builder()
                .rules(
                    LifecycleRule::builder()
                        .status(ExpirationStatus::Enabled)
                        .filter(LifecycleRuleFilter::builder().prefix("logs/").build())
                        .expiration(LifecycleExpiration::builder().days(1).build())
                        .build()
                        .unwrap(),
                )
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    server.put("logs/old", "x").await;
    server.put("keep/old", "x").await;

    let (status, run) = server.admin("POST", "lifecycle/run", Value::Null).await;
    assert_eq!(status, 200);
    assert_eq!(run["expired_current_versions"], 0);

    let (status, clock) = server
        .admin("POST", "lifecycle/advance", json!({"milliseconds": DAY_MS}))
        .await;
    assert_eq!(status, 200);
    assert_eq!(clock["offset_ms"], DAY_MS);
    let (_, read_back) = server.admin("GET", "lifecycle/clock", Value::Null).await;
    assert_eq!(read_back["offset_ms"], DAY_MS);

    let (status, run) = server.admin("POST", "lifecycle/run", Value::Null).await;
    assert_eq!(status, 200);
    assert_eq!(run["expired_current_versions"], 1);
    let expired = server
        .client
        .head_object()
        .bucket("testing")
        .key("logs/old")
        .send()
        .await;
    assert!(expired.is_err(), "the prefixed object expired");
    assert_eq!(server.get("keep/old").await, b"x");
    server.server.shutdown().await;
}

#[tokio::test]
async fn the_lifecycle_clock_never_moves_backward() {
    let server = RunningServer::start(&[]).await;
    let (status, _) = server
        .admin("POST", "lifecycle/advance", json!({"milliseconds": -1}))
        .await;
    assert_eq!(status, 400);
    let (status, _) = server
        .admin("POST", "lifecycle/advance", json!({"days": 1}))
        .await;
    assert_eq!(status, 400, "unknown fields are rejected");
    let (_, clock) = server.admin("GET", "lifecycle/clock", Value::Null).await;
    assert_eq!(clock["offset_ms"], 0);
    server.server.shutdown().await;
}
