// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::checksum::ObjectChecksum;

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
