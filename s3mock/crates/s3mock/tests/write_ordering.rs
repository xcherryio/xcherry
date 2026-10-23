// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Overlapping uploads: a successful conditional write must become the current version.

use s3mock::{
    ChecksumAlgorithm, DeleteObjectRequest, Durability, ObjectReadRequest, PutObjectRequest, Store,
    StoreConfig, UploadChecksumExpectations, VersioningState, WriteCondition,
};

fn open_versioned_store(directory: &tempfile::TempDir) -> Store {
    let store = Store::open(&StoreConfig {
        root_directory: directory.path().to_path_buf(),
        durability: Durability::Relaxed,
        etag_mode: s3mock::EtagMode::Md5,
    })
    .expect("the store test setup succeeds");
    store
        .create_bucket("ordering", false)
        .expect("the store test setup succeeds");
    store
        .put_bucket_versioning("ordering", VersioningState::Enabled)
        .expect("the store test setup succeeds");
    store
}

fn stage(store: &Store, body: &[u8]) -> s3mock::StagedObject {
    let mut writer = store
        .begin_put("ordering", "k", ChecksumAlgorithm::Crc32)
        .expect("the store test setup succeeds");
    writer
        .write_all(body)
        .expect("the store test setup succeeds");
    writer
        .finish(&UploadChecksumExpectations::default())
        .expect("the store test setup succeeds")
}

fn put(store: &Store, staged: s3mock::StagedObject, write_condition: WriteCondition) -> String {
    store
        .commit_put(
            "ordering",
            "k",
            staged,
            PutObjectRequest {
                write_condition,
                ..PutObjectRequest::default()
            },
        )
        .expect("the conditional write succeeds")
        .etag
}

fn current_etag(store: &Store) -> Option<String> {
    store
        .head_object("ordering", "k", &ObjectReadRequest::default())
        .ok()
        .map(|selection| selection.record.etag)
}

#[test]
fn if_match_write_started_first_but_committed_last_becomes_current() {
    let directory = tempfile::tempdir().unwrap();
    let store = open_versioned_store(&directory);
    let slower = stage(&store, b"slower");
    let newer_etag = put(
        &store,
        stage(&store, b"newer"),
        WriteCondition::Unconditional,
    );
    let slower_etag = put(
        &store,
        slower,
        WriteCondition::IfMatch(format!("\"{newer_etag}\"")),
    );
    assert_eq!(current_etag(&store), Some(slower_etag));
}

#[test]
fn if_none_match_write_after_an_intervening_delete_becomes_current() {
    let directory = tempfile::tempdir().unwrap();
    let store = open_versioned_store(&directory);
    let slower = stage(&store, b"slower");
    put(
        &store,
        stage(&store, b"newer"),
        WriteCondition::Unconditional,
    );
    store
        .delete_object("ordering", "k", &DeleteObjectRequest::default())
        .unwrap();
    let slower_etag = put(&store, slower, WriteCondition::IfNoneMatchAny);
    assert_eq!(current_etag(&store), Some(slower_etag));
}

#[test]
fn dropped_uploads_leave_no_temporary_files() {
    let directory = tempfile::tempdir().unwrap();
    let store = open_versioned_store(&directory);
    let temporary_files = || {
        std::fs::read_dir(directory.path().join("tmp"))
            .unwrap()
            .count()
    };
    let mut writer = store
        .begin_put("ordering", "k", ChecksumAlgorithm::Crc32)
        .unwrap();
    writer.write_all(b"never committed").unwrap();
    assert_eq!(temporary_files(), 1);
    drop(writer);
    assert_eq!(temporary_files(), 0);
    let mismatch = store
        .begin_put("ordering", "k", ChecksumAlgorithm::Crc32)
        .unwrap()
        .finish(&UploadChecksumExpectations {
            expected_value_base64: Some("AQIDBA==".to_owned()),
            ..UploadChecksumExpectations::default()
        });
    assert!(mismatch.is_err());
    assert_eq!(temporary_files(), 0);
}
