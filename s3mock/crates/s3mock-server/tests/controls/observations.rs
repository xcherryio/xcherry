// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Admin HTTP schemas, byte accounting, bounded logs, and opaque ETags.

use super::support::RunningServer;
use serde_json::json;

#[tokio::test]
async fn admin_reports_and_resets_counters_and_bounds_log() {
    let server =
        RunningServer::start(&["--counter-prefix-depth", "2", "--request-log-capacity", "3"]).await;
    server.put("prefix/group/one", "abc").await;
    server.put("prefix/group/two", "abcde").await;
    assert_eq!(server.get("prefix/group/one").await, b"abc");
    server.get("prefix/group/two").await;
    let log = server.requests(3).await;
    assert_eq!(log["capacity"], 3);
    assert_eq!(
        log["requests"]
            .as_array()
            .expect("the controls integration fixture succeeds")
            .len(),
        3
    );
    assert_eq!(log["requests"][0]["operation"], "PutObject");
    assert_eq!(log["requests"][0]["key"], "prefix/group/two");
    let (_, counters) = server.admin("GET", "counters", json!(null)).await;
    assert_eq!(
        counters["counters"]["by_operation"]["PutObject"]["requests"],
        2
    );
    assert_eq!(
        counters["counters"]["by_operation"]["PutObject"]["bytes_received"],
        8
    );
    assert_eq!(
        counters["counters"]["by_operation"]["GetObject"]["bytes_sent"],
        8
    );
    assert_eq!(
        counters["counters"]["by_prefix"]["testing"]["prefix/group"]["requests"],
        4
    );
    assert_eq!(server.admin("DELETE", "counters", json!(null)).await.0, 200);
    let (_, counters) = server.admin("GET", "counters", json!(null)).await;
    assert_eq!(counters["counters"]["by_operation"], json!({}));
    assert_eq!(server.admin("POST", "gc", json!(null)).await.0, 200);
    assert_eq!(server.admin("GET", "missing", json!(null)).await.0, 404);
    server.server.shutdown().await;
}

#[tokio::test]
async fn invalid_fault_replacement_preserves_installed_rules() {
    let server = RunningServer::start(&[]).await;
    server
        .rules(json!([{"action":{"type":"error","status":503}}]))
        .await;
    for rule in [
        json!({"probability":1.1,"action":{"type":"delay","milliseconds":0}}),
        json!({"action":{"type":"error","status":404}}),
        json!({"action":{"type":"corrupt_body"}}),
        json!({"operation":"GetObject","action":{"type":"commit_then_drop"}}),
        json!({"action":{"type":"unknown"}}),
    ] {
        assert_eq!(
            server
                .admin("PUT", "faults", json!({"rules":[rule]}))
                .await
                .0,
            400
        );
    }
    let (_, installed) = server.admin("GET", "faults", json!(null)).await;
    assert_eq!(installed["rules"][0]["action"]["status"], 503);
    server.admin("DELETE", "faults", json!(null)).await;
    assert_eq!(
        server.admin("GET", "faults", json!(null)).await.1["rules"],
        json!([])
    );
    server.server.shutdown().await;
}

#[tokio::test]
async fn opaque_etags_are_stable_per_version_and_support_conditions() {
    let server = RunningServer::start(&["--etag-mode", "opaque"]).await;
    let first = server
        .client
        .put_object()
        .bucket("testing")
        .key("opaque")
        .body("abc".as_bytes().to_vec().into())
        .send()
        .await
        .expect("the controls integration fixture succeeds");
    let first_etag = first
        .e_tag()
        .expect("the controls integration fixture succeeds");
    assert_ne!(first_etag, "\"900150983cd24fb0d6963f7d28e17f72\"");
    let head = server
        .client
        .head_object()
        .bucket("testing")
        .key("opaque")
        .send()
        .await
        .expect("the controls integration fixture succeeds");
    assert_eq!(head.e_tag(), Some(first_etag));
    let read = server
        .client
        .get_object()
        .bucket("testing")
        .key("opaque")
        .if_match(first_etag)
        .send()
        .await
        .expect("the controls integration fixture succeeds");
    assert_eq!(read.e_tag(), Some(first_etag));
    assert_eq!(
        read.body
            .collect()
            .await
            .expect("the controls integration fixture succeeds")
            .into_bytes()
            .as_ref(),
        b"abc"
    );
    let second = server
        .client
        .put_object()
        .bucket("testing")
        .key("opaque")
        .if_match(first_etag)
        .body("abc".as_bytes().to_vec().into())
        .send()
        .await
        .expect("the controls integration fixture succeeds");
    assert_ne!(second.e_tag(), Some(first_etag));
    assert_eq!(
        server
            .client
            .put_object()
            .bucket("testing")
            .key("opaque")
            .if_match(first_etag)
            .body("abc".as_bytes().to_vec().into())
            .send()
            .await
            .unwrap_err()
            .raw_response()
            .expect("the controls integration fixture succeeds")
            .status()
            .as_u16(),
        412
    );
    server.server.shutdown().await;
}

#[tokio::test]
async fn opaque_etags_survive_a_store_restart() {
    let server = RunningServer::start(&["--etag-mode", "opaque"]).await;
    server.put("persistent", "abc").await;
    let etag = server
        .client
        .head_object()
        .bucket("testing")
        .key("persistent")
        .send()
        .await
        .expect("HEAD succeeds")
        .e_tag()
        .expect("HEAD has an ETag")
        .to_owned();
    server.server.shutdown().await;
    let store = s3mock::Store::open(&s3mock::StoreConfig {
        root_directory: server.directory.path().to_path_buf(),
        durability: s3mock::Durability::Relaxed,
        etag_mode: s3mock::EtagMode::Opaque,
    })
    .expect("the store reopens");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("loopback binding succeeds");
    let restarted =
        s3mock_server::start(std::sync::Arc::new(store), listener).expect("the server restarts");
    let client = super::support::client(&restarted, "s3mock", "s3mock");
    let read = client
        .get_object()
        .bucket("testing")
        .key("persistent")
        .if_match(&etag)
        .send()
        .await
        .expect("the persisted ETag matches");
    assert_eq!(read.e_tag(), Some(etag.as_str()));
    assert_eq!(
        read.body
            .collect()
            .await
            .expect("the object streams")
            .into_bytes()
            .as_ref(),
        b"abc"
    );
    restarted.shutdown().await;
}
