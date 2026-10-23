// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Batch deletion responses, quiet mode, request limits, and versioned deletion.

use aws_sdk_s3::types::{
    BucketVersioningStatus, Delete, ObjectIdentifier, VersioningConfiguration,
};

use crate::objects::put_text;
use crate::support::{ConformanceTarget, error_code_and_status, read_body};

#[tokio::test]
async fn batch_delete_reports_existing_and_missing_keys_as_deleted() {
    let mut target = ConformanceTarget::connect().await;
    let bucket = target.create_bucket().await;
    for key in ["first", "second", "keep"] {
        put_text(&target, &bucket, key, key).await;
    }
    // Missing keys are successful deletions: https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html
    let deleted = target
        .client
        .delete_objects()
        .bucket(&bucket)
        .delete(
            Delete::builder()
                .objects(identifier("first", None))
                .objects(identifier("second", None))
                .objects(identifier("missing", None))
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    assert!(deleted.errors().is_empty());
    let mut keys: Vec<_> = deleted
        .deleted()
        .iter()
        .map(|entry| entry.key().unwrap())
        .collect();
    keys.sort_unstable();
    assert_eq!(keys, ["first", "missing", "second"]);
    for key in ["first", "second", "missing"] {
        let missing = target
            .client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap_err();
        assert_eq!(
            error_code_and_status(&missing),
            (Some("NoSuchKey".to_owned()), Some(404))
        );
    }
    let kept = target
        .client
        .get_object()
        .bucket(&bucket)
        .key("keep")
        .send()
        .await
        .unwrap();
    assert_eq!(read_body(kept).await, b"keep");
    target.cleanup().await;
}

#[tokio::test]
async fn quiet_batch_delete_omits_successes() {
    let mut target = ConformanceTarget::connect().await;
    let bucket = target.create_bucket().await;
    put_text(&target, &bucket, "existing", "body").await;
    // Quiet responses contain errors only: https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html
    let deleted = target
        .client
        .delete_objects()
        .bucket(&bucket)
        .delete(
            Delete::builder()
                .quiet(true)
                .objects(identifier("existing", None))
                .objects(identifier("missing", None))
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    assert!(deleted.deleted().is_empty());
    assert!(deleted.errors().is_empty());
    assert!(
        target
            .client
            .list_objects_v2()
            .bucket(&bucket)
            .send()
            .await
            .unwrap()
            .contents()
            .is_empty()
    );
    target.cleanup().await;
}

#[tokio::test]
async fn versioned_batch_delete_reports_version_ids_and_delete_markers() {
    let mut target = ConformanceTarget::connect().await;
    let bucket = target.create_bucket().await;
    target
        .client
        .put_bucket_versioning()
        .bucket(&bucket)
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await
        .unwrap();
    let first = target
        .client
        .put_object()
        .bucket(&bucket)
        .key("versioned")
        .body(b"old".to_vec().into())
        .send()
        .await
        .unwrap();
    let first_version = first.version_id().unwrap();
    let latest = target
        .client
        .put_object()
        .bucket(&bucket)
        .key("versioned")
        .body(b"latest".to_vec().into())
        .send()
        .await
        .unwrap();
    let latest_version = latest.version_id().unwrap();
    // Version-specific deletion removes only that version: https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html
    let deleted = target
        .client
        .delete_objects()
        .bucket(&bucket)
        .delete(
            Delete::builder()
                .objects(identifier("versioned", Some(first_version)))
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    assert!(deleted.errors().is_empty());
    assert_eq!(deleted.deleted().len(), 1);
    assert_eq!(deleted.deleted()[0].key(), Some("versioned"));
    assert_eq!(deleted.deleted()[0].version_id(), Some(first_version));
    let missing = target
        .client
        .get_object()
        .bucket(&bucket)
        .key("versioned")
        .version_id(first_version)
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        error_code_and_status(&missing),
        (Some("NoSuchVersion".to_owned()), Some(404))
    );
    let get = target
        .client
        .get_object()
        .bucket(&bucket)
        .key("versioned")
        .send()
        .await
        .unwrap();
    assert_eq!(get.version_id(), Some(latest_version));
    assert_eq!(read_body(get).await, b"latest");
    // Omitting VersionId creates a reported delete marker: https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html
    let markers = target
        .client
        .delete_objects()
        .bucket(&bucket)
        .delete(
            Delete::builder()
                .objects(identifier("versioned", None))
                .objects(identifier("never-written", None))
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    assert!(markers.errors().is_empty());
    assert_eq!(markers.deleted().len(), 2);
    for key in ["versioned", "never-written"] {
        let marker = markers
            .deleted()
            .iter()
            .find(|entry| entry.key() == Some(key))
            .unwrap();
        assert_eq!(marker.delete_marker(), Some(true));
        let marker_id = marker
            .delete_marker_version_id()
            .expect("new delete markers have version IDs");
        assert!(!marker_id.is_empty());
        let missing = target
            .client
            .get_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .unwrap_err();
        assert_eq!(
            error_code_and_status(&missing),
            (Some("NoSuchKey".to_owned()), Some(404))
        );
        let listed = target
            .client
            .list_object_versions()
            .bucket(&bucket)
            .prefix(key)
            .send()
            .await
            .unwrap();
        assert!(
            listed
                .delete_markers()
                .iter()
                .any(|entry| entry.key() == Some(key)
                    && entry.version_id() == Some(marker_id)
                    && entry.is_latest() == Some(true))
        );
    }
    let retained = target
        .client
        .get_object()
        .bucket(&bucket)
        .key("versioned")
        .version_id(latest_version)
        .send()
        .await
        .unwrap();
    assert_eq!(read_body(retained).await, b"latest");
    target.cleanup().await;
}

#[tokio::test]
async fn batch_delete_rejects_more_than_one_thousand_keys() {
    let mut target = ConformanceTarget::connect().await;
    let bucket = target.create_bucket().await;
    // Delete requests allow at most 1000 keys: https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html
    let objects = (0..1001)
        .map(|number| identifier(&format!("key-{number:04}"), None))
        .collect();
    let error = target
        .client
        .delete_objects()
        .bucket(&bucket)
        .delete(
            Delete::builder()
                .set_objects(Some(objects))
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        error_code_and_status(&error),
        (Some("MalformedXML".to_owned()), Some(400))
    );
    target.cleanup().await;
}

fn identifier(key: &str, version_id: Option<&str>) -> ObjectIdentifier {
    ObjectIdentifier::builder()
        .key(key)
        .set_version_id(version_id.map(str::to_owned))
        .build()
        .expect("an object identifier with a key is valid")
}
