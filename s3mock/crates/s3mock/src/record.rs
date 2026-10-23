// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::checksum::{ChecksumAlgorithm, ChecksumType, ObjectChecksum};

/// Versioning state of a bucket, following S3's three states.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum VersioningState {
    #[default]
    Unversioned,
    Enabled,
    Suspended,
}

/// Bucket configuration persisted in the `buckets` table.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BucketRecord {
    pub name: String,
    /// ULID naming the bucket data directory, so a re-created bucket never reuses old files.
    pub incarnation: String,
    pub created_unix_ms: i64,
    pub versioning: VersioningState,
    /// Stored and returned only; s3mock never enforces retention.
    #[serde(default)]
    pub object_lock: Option<ObjectLockConfiguration>,
    #[serde(default)]
    pub lifecycle: Option<LifecycleConfiguration>,
}

/// The lifecycle rules of a bucket, applied by [`crate::Store::run_lifecycle`].
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LifecycleConfiguration {
    pub rules: Vec<LifecycleRule>,
}

/// One lifecycle rule; s3mock supports prefix filters and day-based actions.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LifecycleRule {
    pub id: Option<String>,
    pub is_enabled: bool,
    /// Applies to keys that start with this prefix; empty matches every key.
    pub prefix: String,
    /// Expires the current version this many days after it was written.
    pub expiration_days: Option<u32>,
    /// Removes a delete marker once it is the only version of its key.
    pub expired_object_delete_marker: bool,
    /// Removes a noncurrent version this many days after it became noncurrent.
    pub noncurrent_expiration_days: Option<u32>,
    /// Keeps this many newest noncurrent versions regardless of age.
    pub newer_noncurrent_versions: Option<u32>,
    /// Aborts multipart uploads this many days after they were initiated.
    pub abort_incomplete_upload_days: Option<u32>,
}

/// Object Lock configuration of a bucket created with Object Lock enabled.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectLockConfiguration {
    pub default_retention: Option<DefaultRetention>,
}

/// Object Lock retention mode.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RetentionMode {
    Governance,
    Compliance,
}

impl RetentionMode {
    /// Returns the S3 name, such as `GOVERNANCE`.
    pub fn s3_name(self) -> &'static str {
        match self {
            RetentionMode::Governance => "GOVERNANCE",
            RetentionMode::Compliance => "COMPLIANCE",
        }
    }

    /// Parses an S3 retention mode name.
    pub fn from_s3_name(name: &str) -> Option<RetentionMode> {
        [RetentionMode::Governance, RetentionMode::Compliance]
            .into_iter()
            .find(|mode| mode.s3_name() == name)
    }
}

/// The default retention rule of an Object Lock configuration.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DefaultRetention {
    pub mode: RetentionMode,
    pub days: Option<u32>,
    pub years: Option<u32>,
}

/// Content headers that S3 stores with an object and returns on GET and HEAD.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContentHeaders {
    pub content_type: Option<String>,
    pub content_encoding: Option<String>,
    pub content_disposition: Option<String>,
    pub content_language: Option<String>,
    pub cache_control: Option<String>,
    pub expires: Option<String>,
}

/// One object version or delete marker, persisted in the `versions` and `latest` tables.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct VersionRecord {
    /// Internal ULID that orders versions and names the data file.
    pub internal_version: String,
    /// Whether S3 reports this version as the `null` version.
    pub is_null_version: bool,
    pub is_delete_marker: bool,
    pub size: u64,
    /// Unquoted ETag.
    pub etag: String,
    pub checksum: Option<ObjectChecksum>,
    pub content_headers: ContentHeaders,
    pub user_metadata: BTreeMap<String, String>,
    pub last_modified_unix_ms: i64,
    /// Data file name relative to the bucket data directory; absent for delete markers.
    pub data_file: Option<String>,
    /// Number of parts for an object completed by a multipart upload.
    #[serde(default)]
    pub parts_count: Option<u32>,
}

/// An in-progress multipart upload, persisted in the `uploads` table.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct UploadRecord {
    pub upload_id: String,
    pub key: String,
    /// The bucket incarnation the upload belongs to.
    pub bucket_incarnation: String,
    pub initiated_unix_ms: i64,
    pub content_headers: ContentHeaders,
    pub user_metadata: BTreeMap<String, String>,
    /// Algorithm the client chose at creation; parts must carry it.
    pub checksum_algorithm: Option<ChecksumAlgorithm>,
    pub checksum_type: ChecksumType,
}

/// One uploaded part, persisted in the `parts` table.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartRecord {
    pub part_number: u32,
    pub size: u64,
    /// Unquoted MD5 ETag of the part.
    pub etag: String,
    pub checksum: ObjectChecksum,
    pub last_modified_unix_ms: i64,
    /// Part file name relative to the bucket data directory.
    pub data_file: String,
}

impl VersionRecord {
    /// Returns the version ID that S3 clients see.
    pub fn version_id(&self) -> String {
        if self.is_null_version {
            "null".to_owned()
        } else {
            self.internal_version.clone()
        }
    }
}
