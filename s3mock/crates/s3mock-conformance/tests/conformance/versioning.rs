// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Bucket versioning: versions, delete markers, version-addressed reads, and suspension.

use aws_sdk_s3::types::{BucketVersioningStatus, VersioningConfiguration};

use crate::objects::put_text;
use crate::support::{ConformanceTarget, error_code_and_status, read_body};

async fn set_versioning(target: &ConformanceTarget, bucket: &str, status: BucketVersioningStatus) {
    target
        .client
        .put_bucket_versioning()
        .bucket(bucket)
        .versioning_configuration(VersioningConfiguration::builder().status(status).build())
        .send()
        .await
        .expect("changing bucket versioning succeeds");
}

#[tokio::test]
async fn enabled_versioning_keeps_every_version() {
    let mut target = ConformanceTarget::connect().await;
    let bucket = target.create_bucket().await;
    set_versioning(&target, &bucket, BucketVersioningStatus::Enabled).await;
    let status = target
        .client
        .get_bucket_versioning()
        .bucket(&bucket)
        .send()
        .await
        .unwrap();
    assert_eq!(status.status(), Some(&BucketVersioningStatus::Enabled));

    let first = target
        .client
        .put_object()
        .bucket(&bucket)
        .key("k")
        .body(b"one".to_vec().into())
        .send()
        .await
        .unwrap();
    put_text(&target, &bucket, "k", "two").await;
    let first_version = first
        .version_id()
        .expect("versioned writes return a version ID")
        .to_owned();
    let latest = target
        .client
        .get_object()
        .bucket(&bucket)
        .key("k")
        .send()
        .await
        .unwrap();
    assert_ne!(latest.version_id(), Some(first_version.as_str()));
    assert_eq!(read_body(latest).await, b"two");
    let old = target
        .client
        .get_object()
        .bucket(&bucket)
        .key("k")
        .version_id(&first_version)
        .send()
        .await
        .unwrap();
    assert_eq!(read_body(old).await, b"one");

    let deleted = target
        .client
        .delete_object()
        .bucket(&bucket)
        .key("k")
        .send()
        .await
        .unwrap();
    assert_eq!(deleted.delete_marker(), Some(true));
    let marker_version = deleted
        .version_id()
        .expect("delete markers have a version ID")
        .to_owned();
    let hidden = target
        .client
        .get_object()
        .bucket(&bucket)
        .key("k")
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        error_code_and_status(&hidden),
        (Some("NoSuchKey".to_owned()), Some(404))
    );
    let marker_read = target
        .client
        .get_object()
        .bucket(&bucket)
        .key("k")
        .version_id(&marker_version)
        .send()
        .await
        .unwrap_err();
    assert_eq!(error_code_and_status(&marker_read).1, Some(405));

    let versions = target
        .client
        .list_object_versions()
        .bucket(&bucket)
        .send()
        .await
        .unwrap();
    assert_eq!(versions.versions().len(), 2);
    assert_eq!(versions.delete_markers().len(), 1);
    assert_eq!(versions.delete_markers()[0].is_latest(), Some(true));
    let listed = target
        .client
        .list_objects_v2()
        .bucket(&bucket)
        .send()
        .await
        .unwrap();
    assert!(
        listed.contents().is_empty(),
        "a delete marker hides the key from ListObjectsV2"
    );

    target
        .client
        .delete_object()
        .bucket(&bucket)
        .key("k")
        .version_id(&marker_version)
        .send()
        .await
        .unwrap();
    let restored = target
        .client
        .get_object()
        .bucket(&bucket)
        .key("k")
        .send()
        .await
        .unwrap();
    assert_eq!(read_body(restored).await, b"two");
    target.cleanup().await;
}

#[tokio::test]
async fn suspended_versioning_overwrites_the_null_version() {
    let mut target = ConformanceTarget::connect().await;
    let bucket = target.create_bucket().await;
    put_text(&target, &bucket, "k", "unversioned").await;
    set_versioning(&target, &bucket, BucketVersioningStatus::Enabled).await;
    put_text(&target, &bucket, "k", "versioned").await;
    set_versioning(&target, &bucket, BucketVersioningStatus::Suspended).await;
    let suspended = target
        .client
        .put_object()
        .bucket(&bucket)
        .key("k")
        .body(b"suspended".to_vec().into())
        .send()
        .await
        .unwrap();
    assert_eq!(suspended.version_id(), Some("null"));

    let versions = target
        .client
        .list_object_versions()
        .bucket(&bucket)
        .send()
        .await
        .unwrap();
    let version_ids: Vec<&str> = versions
        .versions()
        .iter()
        .filter_map(|version| version.version_id())
        .collect();
    assert_eq!(
        version_ids.len(),
        2,
        "the suspended write replaced the original null version"
    );
    assert_eq!(version_ids[0], "null");
    let null_version = target
        .client
        .get_object()
        .bucket(&bucket)
        .key("k")
        .version_id("null")
        .send()
        .await
        .unwrap();
    assert_eq!(read_body(null_version).await, b"suspended");
    target.cleanup().await;
}
