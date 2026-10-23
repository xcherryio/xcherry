// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Conditional writes and reads.

use std::sync::Arc;

use aws_sdk_s3::primitives::ByteStream;

use crate::objects::put_text;
use crate::support::{ConformanceTarget, error_code_and_status};

const CONCURRENT_CREATORS: usize = 16;

#[tokio::test]
async fn if_none_match_creates_only_once() {
    let mut target = ConformanceTarget::connect().await;
    let bucket = target.create_bucket().await;
    let create = |body: &'static [u8]| {
        target
            .client
            .put_object()
            .bucket(&bucket)
            .key("manifest")
            .if_none_match("*")
            .body(ByteStream::from_static(body))
            .send()
    };
    create(b"first").await.unwrap();
    let second = create(b"second").await.unwrap_err();
    assert_eq!(
        error_code_and_status(&second),
        (Some("PreconditionFailed".to_owned()), Some(412))
    );
    target.cleanup().await;
}

#[tokio::test]
async fn concurrent_if_none_match_has_exactly_one_winner() {
    let mut target = ConformanceTarget::connect().await;
    let bucket = Arc::new(target.create_bucket().await);
    let mut creators = tokio::task::JoinSet::new();
    for creator in 0..CONCURRENT_CREATORS {
        let client = target.client.clone();
        let bucket = Arc::clone(&bucket);
        creators.spawn(async move {
            client
                .put_object()
                .bucket(bucket.as_str())
                .key("contended")
                .if_none_match("*")
                .body(ByteStream::from(format!("creator {creator}").into_bytes()))
                .send()
                .await
        });
    }
    let mut winners = 0;
    while let Some(result) = creators.join_next().await {
        match result.unwrap() {
            Ok(_) => winners += 1,
            Err(error) => {
                let (code, status) = error_code_and_status(&error);
                assert!(
                    status == Some(412)
                        || (status == Some(409)
                            && code.as_deref() == Some("ConditionalRequestConflict")),
                    "losers fail the precondition, got {code:?} {status:?}"
                );
            }
        }
    }
    assert_eq!(winners, 1);
    target.cleanup().await;
}

#[tokio::test]
async fn if_match_compares_the_current_etag() {
    let mut target = ConformanceTarget::connect().await;
    let bucket = target.create_bucket().await;
    let replace = |etag: String| {
        target
            .client
            .put_object()
            .bucket(&bucket)
            .key("manifest")
            .if_match(etag)
            .body(ByteStream::from_static(b"next"))
            .send()
    };
    let missing = replace("\"0123456789abcdef0123456789abcdef\"".to_owned())
        .await
        .unwrap_err();
    assert_eq!(
        error_code_and_status(&missing),
        (Some("NoSuchKey".to_owned()), Some(404))
    );
    let etag = put_text(&target, &bucket, "manifest", "current").await;
    let stale = replace("\"0123456789abcdef0123456789abcdef\"".to_owned())
        .await
        .unwrap_err();
    assert_eq!(
        error_code_and_status(&stale),
        (Some("PreconditionFailed".to_owned()), Some(412))
    );
    replace(etag.clone()).await.unwrap();
    let reused = replace(etag).await.unwrap_err();
    assert_eq!(
        error_code_and_status(&reused).1,
        Some(412),
        "the replaced ETag is no longer current"
    );
    target.cleanup().await;
}

#[tokio::test]
async fn conditional_reads_follow_http_precedence() {
    let mut target = ConformanceTarget::connect().await;
    let bucket = target.create_bucket().await;
    let etag = put_text(&target, &bucket, "k", "v").await;
    let get = || target.client.get_object().bucket(&bucket).key("k");
    get().if_match(etag.clone()).send().await.unwrap();
    let mismatch = get().if_match("\"other\"").send().await.unwrap_err();
    assert_eq!(error_code_and_status(&mismatch).1, Some(412));
    let not_modified = get().if_none_match(etag.clone()).send().await.unwrap_err();
    assert_eq!(error_code_and_status(&not_modified).1, Some(304));
    get().if_none_match("\"other\"").send().await.unwrap();
    target.cleanup().await;
}
