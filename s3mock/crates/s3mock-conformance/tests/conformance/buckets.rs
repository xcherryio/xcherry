// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Bucket creation, listing, emptiness checks, and deletion.

use aws_sdk_s3::primitives::ByteStream;

use crate::support::{ConformanceTarget, error_code_and_status};

#[tokio::test]
async fn bucket_lifecycle() {
    let mut target = ConformanceTarget::connect().await;
    let bucket = target.create_bucket().await;
    target
        .client
        .head_bucket()
        .bucket(&bucket)
        .send()
        .await
        .unwrap();
    let listed = target.client.list_buckets().send().await.unwrap();
    assert!(
        listed
            .buckets()
            .iter()
            .any(|listed| listed.name() == Some(bucket.as_str()))
    );

    let duplicate = target
        .client
        .create_bucket()
        .bucket(&bucket)
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        error_code_and_status(&duplicate).0.as_deref(),
        Some("BucketAlreadyOwnedByYou")
    );

    target
        .client
        .put_object()
        .bucket(&bucket)
        .key("keep")
        .body(ByteStream::from_static(b"x"))
        .send()
        .await
        .unwrap();
    let not_empty = target
        .client
        .delete_bucket()
        .bucket(&bucket)
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        error_code_and_status(&not_empty),
        (Some("BucketNotEmpty".to_owned()), Some(409))
    );
    target.cleanup().await;
}

#[tokio::test]
async fn missing_bucket_reports_no_such_bucket() {
    let target = ConformanceTarget::connect().await;
    let bucket = format!(
        "s3mock-conformance-missing-{}",
        ulid::Ulid::new().to_string().to_ascii_lowercase()
    );
    let head = target
        .client
        .head_bucket()
        .bucket(&bucket)
        .send()
        .await
        .unwrap_err();
    assert_eq!(error_code_and_status(&head).1, Some(404));
    let put = target
        .client
        .put_object()
        .bucket(&bucket)
        .key("k")
        .body(ByteStream::from_static(b"x"))
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        error_code_and_status(&put),
        (Some("NoSuchBucket".to_owned()), Some(404))
    );
    target.cleanup().await;
}
