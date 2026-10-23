// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Fault outcomes observed by the real S3 SDK with automatic retries disabled.

use super::support::RunningServer;
use serde_json::json;
use std::time::{Duration, Instant};

#[tokio::test]
async fn dropped_commit_is_visible_and_conditional_retry_fails() {
    let server = RunningServer::start(&[]).await;
    server.rules(json!([{"operation":"PutObject","bucket":"testing","key_prefix":"uncertain/","has_conditional_header":true,"action":{"type":"commit_then_drop"}}])).await;
    let error = server
        .client
        .put_object()
        .bucket("testing")
        .key("uncertain/object")
        .if_none_match("*")
        .body("committed".as_bytes().to_vec().into())
        .send()
        .await
        .unwrap_err();
    assert!(
        error.as_service_error().is_none(),
        "the connection must close without an S3 response"
    );
    let error = server
        .client
        .put_object()
        .bucket("testing")
        .key("uncertain/object")
        .if_none_match("*")
        .body("committed".as_bytes().to_vec().into())
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        error
            .raw_response()
            .expect("the controls integration fixture succeeds")
            .status()
            .as_u16(),
        412
    );
    assert_eq!(server.get("uncertain/object").await, b"committed");
    let log = server.requests(3).await;
    assert_eq!(log["requests"][0]["injected_fault"], "commit_then_drop");
    assert_eq!(log["requests"][0]["status"], 0);
    server.server.shutdown().await;
}

#[tokio::test]
async fn errors_and_fail_before_commit_leave_no_object() {
    let server = RunningServer::start(&[]).await;
    for (action, expected_status, expected_code) in [
        (json!({"type":"fail_before_commit"}), 500, "InternalError"),
        (json!({"type":"error","status":500}), 500, "InternalError"),
        (json!({"type":"error","status":503}), 503, "SlowDown"),
        (
            json!({"type":"error","status":409}),
            409,
            "ConditionalRequestConflict",
        ),
    ] {
        server
            .rules(json!([{"operation":"PutObject","action":action}]))
            .await;
        let error = server
            .client
            .put_object()
            .bucket("testing")
            .key("absent")
            .body("not stored".as_bytes().to_vec().into())
            .send()
            .await
            .unwrap_err();
        assert_eq!(
            error
                .raw_response()
                .expect("the controls integration fixture succeeds")
                .status()
                .as_u16(),
            expected_status
        );
        use aws_sdk_s3::error::ProvideErrorMetadata;
        assert_eq!(
            error
                .as_service_error()
                .expect("the controls integration fixture succeeds")
                .code(),
            Some(expected_code)
        );
        assert_eq!(
            server
                .client
                .head_object()
                .bucket("testing")
                .key("absent")
                .send()
                .await
                .unwrap_err()
                .raw_response()
                .expect("the controls integration fixture succeeds")
                .status()
                .as_u16(),
            404
        );
    }
    server.server.shutdown().await;
}

#[tokio::test]
async fn delay_corruption_and_truncation_affect_sdk_requests() {
    let server = RunningServer::start(&[]).await;
    server.put("object", "original object body").await;
    server
        .rules(json!([{"operation":"GetObject","has_conditional_header":true,"action":{"type":"delay","milliseconds":40}}]))
        .await;
    let started = Instant::now();
    let conditional = server
        .client
        .get_object()
        .bucket("testing")
        .key("object")
        .if_modified_since(aws_sdk_s3::primitives::DateTime::from_secs(0))
        .send()
        .await
        .expect("the conditional read succeeds");
    assert_eq!(
        conditional
            .body
            .collect()
            .await
            .expect("the conditional body streams")
            .into_bytes()
            .as_ref(),
        b"original object body"
    );
    assert!(started.elapsed() >= Duration::from_millis(40));
    let original = server
        .client
        .head_object()
        .bucket("testing")
        .key("object")
        .send()
        .await
        .expect("the controls integration fixture succeeds");
    server
        .rules(json!([{"operation":"GetObject","action":{"type":"corrupt_body"}}]))
        .await;
    let corrupted = server
        .client
        .get_object()
        .bucket("testing")
        .key("object")
        .send()
        .await
        .expect("the controls integration fixture succeeds");
    assert_eq!(corrupted.e_tag(), original.e_tag());
    assert_eq!(corrupted.content_length(), original.content_length());
    let bytes = corrupted
        .body
        .collect()
        .await
        .expect("the controls integration fixture succeeds")
        .into_bytes();
    assert_ne!(bytes.as_ref(), b"original object body");
    assert_eq!(&bytes[1..], b"riginal object body");
    server
        .rules(json!([{"operation":"GetObject","action":{"type":"truncate_response"}}]))
        .await;
    match server
        .client
        .get_object()
        .bucket("testing")
        .key("object")
        .send()
        .await
    {
        Ok(output) => assert!(output.body.collect().await.is_err()),
        Err(error) => assert!(error.as_service_error().is_none()),
    }
    server.admin("DELETE", "faults", json!(null)).await;
    assert_eq!(server.get("object").await, b"original object body");
    let log = server.requests(6).await;
    for fault in ["delay", "corrupt_body", "truncate_response"] {
        assert!(
            log["requests"]
                .as_array()
                .expect("the controls integration fixture succeeds")
                .iter()
                .any(|request| request["injected_fault"] == fault)
        );
    }
    server.server.shutdown().await;
}

#[tokio::test]
async fn replacing_rules_replays_seeded_selection_and_filters_requests() {
    let server = RunningServer::start(&["--fault-seed", "1234"]).await;
    let rules = json!([{"operation":"PutObject","bucket":"testing","key_prefix":"selected/","has_conditional_header":true,"probability":0.5,"action":{"type":"error","status":409}}]);
    let mut sequences = Vec::new();
    for _ in 0..2 {
        server.rules(rules.clone()).await;
        server.put("selected/unconditional", "bypass").await;
        let mut sequence = Vec::new();
        for index in 0..24 {
            let output = server
                .client
                .put_object()
                .bucket("testing")
                .key(format!("selected/{index}"))
                .if_match("nonmatching")
                .body("data".as_bytes().to_vec().into())
                .send()
                .await
                .unwrap_err();
            sequence.push(
                output
                    .raw_response()
                    .expect("the controls integration fixture succeeds")
                    .status()
                    .as_u16(),
            );
        }
        sequences.push(sequence);
    }
    assert_eq!(sequences[0], sequences[1]);
    assert!(sequences[0].contains(&409));
    assert!(sequences[0].contains(&404));
    server.server.shutdown().await;
}

#[tokio::test]
async fn truncated_response_sends_headers_and_a_partial_body() {
    let server = RunningServer::start(&[]).await;
    server.put("object", "original object body").await;
    server
        .rules(json!([{"operation":"GetObject","action":{"type":"truncate_response"}}]))
        .await;
    let response = server.raw("GET", "/testing/object", "", b"").await;
    let boundary = response
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .expect("truncation must send headers");
    assert!(response.starts_with(b"HTTP/1.1 200"));
    assert!(
        response.len() > boundary + 4,
        "truncation must send a body prefix"
    );
    assert!(response.len() < boundary + 4 + b"original object body".len());
    server.server.shutdown().await;
}

#[tokio::test]
async fn corruption_preserves_error_responses_and_reset_releases_delays() {
    use aws_sdk_s3::error::ProvideErrorMetadata;
    let server = RunningServer::start(&[]).await;
    server
        .rules(json!([{"operation":"GetObject","action":{"type":"corrupt_body"}}]))
        .await;
    let error = server
        .client
        .get_object()
        .bucket("testing")
        .key("missing")
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        error
            .as_service_error()
            .expect("error XML remains readable")
            .code(),
        Some("NoSuchKey")
    );
    server
        .rules(json!([{"operation":"GetObject","action":{"type":"delay","milliseconds":600000}}]))
        .await;
    {
        let request = server.raw("GET", "/testing/missing", "", b"");
        tokio::pin!(request);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        loop {
            tokio::select! {
                response = &mut request => panic!("delay ended before reset: {response:?}"),
                () = tokio::task::yield_now() => {},
            }
            let (_, faults) = server.admin("GET", "faults", json!(null)).await;
            assert_eq!(faults["rules"][0]["action"]["milliseconds"], 600000);
            if tokio::time::Instant::now() >= deadline {
                break;
            }
        }
        server.admin("DELETE", "faults", json!(null)).await;
        let response = tokio::time::timeout(Duration::from_secs(2), &mut request)
            .await
            .expect("reset releases sleeping request");
        assert!(response.starts_with(b"HTTP/1.1 404"));
    }
    server.server.shutdown().await;
}
