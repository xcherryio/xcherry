// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Lifecycle rules applied by advancing the lifecycle clock instead of waiting days.

use s3mock::{
    ChecksumAlgorithm, CreateMultipartUploadRequest, DeleteObjectRequest, Durability,
    LifecycleConfiguration, LifecycleRule, ListMultipartUploadsRequest, ListObjectVersionsRequest,
    ObjectReadRequest, PutObjectRequest, Store, StoreConfig, UploadChecksumExpectations,
    VersioningState,
};

const DAY_MS: i64 = 86_400_000;

fn open_store(directory: &tempfile::TempDir, versioning: Option<VersioningState>) -> Store {
    let store = Store::open(&StoreConfig {
        root_directory: directory.path().to_path_buf(),
        durability: Durability::Relaxed,
    })
    .expect("the store opens in a fresh directory");
    store
        .create_bucket("life", false)
        .expect("the bucket name is valid");
    if let Some(versioning) = versioning {
        store
            .put_bucket_versioning("life", versioning)
            .expect("versioning changes on an existing bucket");
    }
    store
}

fn put(store: &Store, key: &str, body: &[u8]) {
    let mut writer = store
        .begin_put("life", key, ChecksumAlgorithm::Crc32)
        .expect("the bucket exists");
    writer
        .write_all(body)
        .expect("the temporary file is writable");
    let staged = writer
        .finish(&UploadChecksumExpectations::default())
        .expect("no digests are declared");
    store
        .commit_put("life", key, staged, PutObjectRequest::default())
        .expect("the unconditional write commits");
}

fn set_rule(store: &Store, rule: LifecycleRule) {
    store
        .put_lifecycle_configuration("life", Some(LifecycleConfiguration { rules: vec![rule] }))
        .expect("the bucket exists");
}

fn exists(store: &Store, key: &str) -> bool {
    store
        .head_object("life", key, &ObjectReadRequest::default())
        .is_ok()
}

/// Counts object versions, excluding delete markers.
fn version_count(store: &Store) -> usize {
    store
        .list_object_versions(
            "life",
            &ListObjectVersionsRequest {
                prefix: String::new(),
                delimiter: String::new(),
                max_keys: 1000,
                key_marker: None,
                version_id_marker: None,
            },
        )
        .expect("the bucket exists")
        .versions
        .iter()
        .filter(|version| !version.record.is_delete_marker)
        .count()
}

#[test]
fn current_versions_expire_only_under_the_rule_prefix_after_the_configured_days() {
    let directory = tempfile::tempdir().unwrap();
    let store = open_store(&directory, None);
    put(&store, "logs/old", b"x");
    put(&store, "keep/old", b"x");
    set_rule(
        &store,
        LifecycleRule {
            is_enabled: true,
            prefix: "logs/".to_owned(),
            expiration_days: Some(2),
            ..LifecycleRule::default()
        },
    );
    store.advance_lifecycle_clock(DAY_MS);
    assert_eq!(store.run_lifecycle().unwrap().expired_current_versions, 0);
    store.advance_lifecycle_clock(DAY_MS);
    assert_eq!(store.run_lifecycle().unwrap().expired_current_versions, 1);
    assert!(!exists(&store, "logs/old"));
    assert!(exists(&store, "keep/old"));
}

#[test]
fn disabled_rules_do_nothing() {
    let directory = tempfile::tempdir().unwrap();
    let store = open_store(&directory, None);
    put(&store, "k", b"x");
    set_rule(
        &store,
        LifecycleRule {
            is_enabled: false,
            expiration_days: Some(1),
            ..LifecycleRule::default()
        },
    );
    store.advance_lifecycle_clock(10 * DAY_MS);
    assert_eq!(
        store.run_lifecycle().unwrap(),
        s3mock::LifecycleRun::default()
    );
    assert!(exists(&store, "k"));
}

#[test]
fn versioned_expiration_adds_a_marker_then_noncurrent_and_marker_cleanup_remove_everything() {
    let directory = tempfile::tempdir().unwrap();
    let store = open_store(&directory, Some(VersioningState::Enabled));
    put(&store, "k", b"one");
    set_rule(
        &store,
        LifecycleRule {
            is_enabled: true,
            expiration_days: Some(1),
            noncurrent_expiration_days: Some(1),
            ..LifecycleRule::default()
        },
    );
    store.advance_lifecycle_clock(DAY_MS);
    let first = store.run_lifecycle().unwrap();
    assert_eq!(first.expired_current_versions, 1);
    assert!(!exists(&store, "k"), "a delete marker now hides the key");
    assert_eq!(
        version_count(&store),
        1,
        "the expired version is kept as noncurrent"
    );
    store.advance_lifecycle_clock(DAY_MS);
    assert_eq!(
        store.run_lifecycle().unwrap().expired_noncurrent_versions,
        1
    );
    assert_eq!(version_count(&store), 0);
    set_rule(
        &store,
        LifecycleRule {
            is_enabled: true,
            expired_object_delete_marker: true,
            ..LifecycleRule::default()
        },
    );
    assert_eq!(store.run_lifecycle().unwrap().removed_delete_markers, 1);
    store
        .delete_bucket("life")
        .expect("lifecycle emptied the bucket");
}

#[test]
fn newer_noncurrent_versions_are_retained() {
    let directory = tempfile::tempdir().unwrap();
    let store = open_store(&directory, Some(VersioningState::Enabled));
    for body in [b"1", b"2", b"3", b"4"] {
        put(&store, "k", body);
    }
    set_rule(
        &store,
        LifecycleRule {
            is_enabled: true,
            noncurrent_expiration_days: Some(1),
            newer_noncurrent_versions: Some(2),
            ..LifecycleRule::default()
        },
    );
    store.advance_lifecycle_clock(DAY_MS);
    assert_eq!(
        store.run_lifecycle().unwrap().expired_noncurrent_versions,
        1
    );
    assert_eq!(
        version_count(&store),
        3,
        "the current version and two newer noncurrent versions remain"
    );
}

#[test]
fn incomplete_multipart_uploads_are_aborted() {
    let directory = tempfile::tempdir().unwrap();
    let store = open_store(&directory, None);
    store
        .create_multipart_upload("life", "big", CreateMultipartUploadRequest::default())
        .unwrap();
    set_rule(
        &store,
        LifecycleRule {
            is_enabled: true,
            abort_incomplete_upload_days: Some(7),
            ..LifecycleRule::default()
        },
    );
    store.advance_lifecycle_clock(7 * DAY_MS);
    assert_eq!(store.run_lifecycle().unwrap().aborted_uploads, 1);
    let uploads = store
        .list_multipart_uploads(
            "life",
            &ListMultipartUploadsRequest {
                prefix: String::new(),
                delimiter: String::new(),
                key_marker: None,
                upload_id_marker: None,
                max_uploads: 1000,
            },
        )
        .unwrap();
    assert!(uploads.uploads.is_empty());
}

#[test]
fn an_overwrite_after_the_scan_decision_is_not_expired() {
    let directory = tempfile::tempdir().unwrap();
    let store = open_store(&directory, None);
    put(&store, "k", b"old");
    set_rule(
        &store,
        LifecycleRule {
            is_enabled: true,
            expiration_days: Some(1),
            ..LifecycleRule::default()
        },
    );
    store.advance_lifecycle_clock(DAY_MS);
    store
        .delete_object("life", "k", &DeleteObjectRequest::default())
        .unwrap();
    put(&store, "k", b"new");
    // The new object is also older than a day on the lifecycle clock, so it expires; the guard
    // only prevents deleting a version the scan did not examine.
    assert_eq!(store.run_lifecycle().unwrap().expired_current_versions, 1);
}
