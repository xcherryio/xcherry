// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! PutObject: stream to a temporary file, publish the data file, then commit metadata.

use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;

use redb::ReadableTable;

use super::{
    BUCKETS, LATEST, Store, VERSIONS, create_directory_durably, decode_json, encode_json,
    load_versions, refresh_latest, relative_data_file, remove_superseded_files, require_bucket,
    validate_object_key,
};
use crate::checksum::{ChecksumAlgorithm, ObjectChecksum};
use crate::data_file::{self, DataFileHeader, ObjectWriter, StagedObject};
use crate::error::StoreError;
use crate::key_codec::{encode_object_key, encode_version_key};
use crate::record::{ContentHeaders, VersionRecord, VersioningState};

/// Precondition a PutObject must satisfy at commit time.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum WriteCondition {
    #[default]
    Unconditional,
    /// `If-None-Match: *`: fail with 412 when a live object exists.
    IfNoneMatchAny,
    /// `If-Match: <etag>`: fail with 404 when no live object exists and 412 when its ETag differs.
    IfMatch(String),
}

/// Object attributes supplied with a PutObject request.
#[derive(Clone, Debug, Default)]
pub struct PutObjectRequest {
    pub content_headers: ContentHeaders,
    pub user_metadata: BTreeMap<String, String>,
    pub write_condition: WriteCondition,
}

/// Result of a committed PutObject.
#[derive(Clone, Debug)]
pub struct PutObjectOutcome {
    /// Unquoted ETag.
    pub etag: String,
    /// Version ID for a versioning-enabled or suspended bucket; `None` for an unversioned bucket.
    pub version_id: Option<String>,
    pub checksum: ObjectChecksum,
}

impl Store {
    /// Starts an upload after checking that the bucket exists and the key is valid.
    pub fn begin_put(
        &self,
        bucket: &str,
        key: &str,
        checksum_algorithm: ChecksumAlgorithm,
    ) -> Result<ObjectWriter, StoreError> {
        validate_object_key(key)?;
        let bucket_record = self.bucket(bucket)?;
        ObjectWriter::create(
            &self.temporary_directory,
            self.next_internal_version(),
            bucket_record.incarnation,
            checksum_algorithm,
        )
    }

    /// Publishes a staged upload as a new version of `key`, enforcing the write condition atomically.
    pub fn commit_put(
        &self,
        bucket: &str,
        key: &str,
        staged: StagedObject,
        request: PutObjectRequest,
    ) -> Result<PutObjectOutcome, StoreError> {
        let PutObjectRequest {
            content_headers,
            user_metadata,
            write_condition,
        } = request;
        let published =
            match self.publish_data_file(bucket, key, &staged, content_headers, user_metadata) {
                Ok(published) => published,
                Err(error) => {
                    staged.discard();
                    return Err(error);
                }
            };
        match self.commit_version(bucket, key, &published, &write_condition) {
            Ok(outcome) => Ok(outcome),
            Err(error) => {
                if let Err(remove_error) = fs::remove_file(&published.path) {
                    tracing::warn!(
                        path = %published.path.display(),
                        error = %remove_error,
                        "failed to remove rejected data file"
                    );
                }
                Err(error)
            }
        }
    }

    /// Writes the footer, renames the file into the bucket directory, and flushes the rename.
    fn publish_data_file(
        &self,
        bucket: &str,
        key: &str,
        staged: &StagedObject,
        content_headers: ContentHeaders,
        user_metadata: BTreeMap<String, String>,
    ) -> Result<PublishedDataFile, StoreError> {
        let bucket_record = self.bucket(bucket)?;
        if bucket_record.incarnation != staged.bucket_incarnation {
            return Err(StoreError::NoSuchBucket(bucket.to_owned()));
        }
        let relative_path = relative_data_file(staged.internal_version);
        let record = VersionRecord {
            internal_version: staged.internal_version.to_string(),
            is_null_version: bucket_record.versioning != VersioningState::Enabled,
            is_delete_marker: false,
            size: staged.size,
            etag: staged.md5_hex.clone(),
            checksum: Some(staged.checksum.clone()),
            content_headers,
            user_metadata,
            last_modified_unix_ms: i64::try_from(staged.internal_version.timestamp_ms())
                .expect("ULID timestamps fit in i64"),
            data_file: Some(relative_path.clone()),
        };
        let header = DataFileHeader {
            bucket: bucket.to_owned(),
            key: key.to_owned(),
            record,
        };
        data_file::append_footer(&staged.temporary_path, &header, self.is_durable())?;
        let bucket_directory = self.bucket_directory(&bucket_record);
        let path = bucket_directory.join(&relative_path);
        let shard_directory = path
            .parent()
            .expect("data file paths have a shard directory");
        create_directory_durably(&bucket_directory, self.is_durable())?;
        create_directory_durably(shard_directory, self.is_durable())?;
        fs::rename(&staged.temporary_path, &path)?;
        if self.is_durable() {
            data_file::sync_directory(shard_directory)?;
        }
        Ok(PublishedDataFile {
            path,
            record: header.record,
            internal_version: staged.internal_version,
            bucket_incarnation: bucket_record.incarnation,
        })
    }

    /// Inserts the version in one write transaction, replacing the null version when the bucket is not versioning-enabled.
    fn commit_version(
        &self,
        bucket: &str,
        key: &str,
        published: &PublishedDataFile,
        write_condition: &WriteCondition,
    ) -> Result<PutObjectOutcome, StoreError> {
        let transaction = self.begin_write()?;
        let mut superseded_files = Vec::new();
        let versioning = {
            let buckets = transaction.open_table(BUCKETS)?;
            let bucket_record = require_bucket(&buckets, bucket)?;
            if bucket_record.incarnation != published.bucket_incarnation {
                return Err(StoreError::NoSuchBucket(bucket.to_owned()));
            }
            let mut latest = transaction.open_table(LATEST)?;
            let mut versions = transaction.open_table(VERSIONS)?;
            let object_key = encode_object_key(bucket, key);
            let current: Option<VersionRecord> = match latest.get(object_key.as_slice())? {
                Some(value) => Some(decode_json(value.value())?),
                None => None,
            };
            check_write_condition(write_condition, current.as_ref(), bucket, key)?;
            // Versioning may change while the body streams; metadata follows the state at commit.
            let mut record = published.record.clone();
            record.is_null_version = bucket_record.versioning != VersioningState::Enabled;
            if record.is_null_version {
                for stored in load_versions(&versions, bucket, key)? {
                    if stored.record.is_null_version {
                        versions.remove(stored.version_key.as_slice())?;
                        superseded_files
                            .extend(self.data_file_path(&bucket_record, &stored.record));
                    }
                }
            }
            let version_key = encode_version_key(bucket, key, published.internal_version);
            versions.insert(version_key.as_slice(), encode_json(&record)?.as_slice())?;
            refresh_latest(&mut latest, &versions, bucket, key)?;
            bucket_record.versioning
        };
        transaction.commit()?;
        remove_superseded_files(&superseded_files);
        Ok(PutObjectOutcome {
            etag: published.record.etag.clone(),
            version_id: version_id_for(versioning, published),
            checksum: published
                .record
                .checksum
                .clone()
                .expect("uploaded versions always carry a checksum"),
        })
    }
}

/// A data file that is durable on disk but not yet referenced by metadata.
struct PublishedDataFile {
    path: PathBuf,
    record: VersionRecord,
    internal_version: ulid::Ulid,
    bucket_incarnation: String,
}

fn check_write_condition(
    write_condition: &WriteCondition,
    current: Option<&VersionRecord>,
    bucket: &str,
    key: &str,
) -> Result<(), StoreError> {
    match (write_condition, current) {
        (WriteCondition::Unconditional, _) | (WriteCondition::IfNoneMatchAny, None) => Ok(()),
        (WriteCondition::IfNoneMatchAny, Some(_)) => Err(StoreError::PreconditionFailed),
        (WriteCondition::IfMatch(_), None) => Err(StoreError::NoSuchKey {
            bucket: bucket.to_owned(),
            key: key.to_owned(),
        }),
        (WriteCondition::IfMatch(expected), Some(current)) => {
            if super::object_read::etag_matches(expected, &current.etag) {
                Ok(())
            } else {
                Err(StoreError::PreconditionFailed)
            }
        }
    }
}

fn version_id_for(versioning: VersioningState, published: &PublishedDataFile) -> Option<String> {
    match versioning {
        VersioningState::Unversioned => None,
        VersioningState::Enabled => Some(published.internal_version.to_string()),
        VersioningState::Suspended => Some(super::NULL_VERSION_ID.to_owned()),
    }
}
