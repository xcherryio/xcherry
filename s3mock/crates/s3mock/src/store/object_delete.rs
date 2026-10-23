// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! DeleteObject: delete markers for versioned buckets and permanent version deletion.

use super::{
    BUCKETS, LATEST, Store, VERSIONS, encode_json, find_version, load_versions, parse_version_id,
    refresh_latest, remove_superseded_files, require_bucket, validate_object_key,
};
use crate::error::StoreError;
use crate::key_codec::encode_version_key;
use crate::record::{ContentHeaders, VersionRecord, VersioningState};

/// Parameters of a DeleteObject request.
#[derive(Clone, Debug, Default)]
pub struct DeleteObjectRequest {
    /// Permanently deletes this version or delete marker instead of adding a delete marker.
    pub version_id: Option<String>,
}

/// Result of a DeleteObject request, echoed through `x-amz-version-id` and `x-amz-delete-marker`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DeleteObjectOutcome {
    pub version_id: Option<String>,
    /// Whether the request created or permanently removed a delete marker.
    pub is_delete_marker: bool,
}

/// A condition re-checked inside the delete transaction, so a stale decision never applies.
pub(super) struct DeleteGuard<'guard> {
    /// Receives the key's versions, newest first, as they are at commit time.
    pub(super) is_still_due: &'guard dyn Fn(&[VersionRecord]) -> bool,
    /// Last-modified time for a delete marker this delete creates, instead of wall-clock time.
    pub(super) marker_last_modified_unix_ms: Option<i64>,
}

impl Store {
    /// Deletes an object; deleting a missing key or version succeeds, as in S3.
    pub fn delete_object(
        &self,
        bucket: &str,
        key: &str,
        request: &DeleteObjectRequest,
    ) -> Result<DeleteObjectOutcome, StoreError> {
        Ok(self
            .delete_object_if(bucket, key, request, None)?
            .expect("an unguarded delete always applies"))
    }

    /// Deletes like [`Store::delete_object`] when `guard` approves the key's current versions.
    ///
    /// Returns `None`, changing nothing, when the guard rejects them.
    pub(super) fn delete_object_if(
        &self,
        bucket: &str,
        key: &str,
        request: &DeleteObjectRequest,
        guard: Option<&DeleteGuard<'_>>,
    ) -> Result<Option<DeleteObjectOutcome>, StoreError> {
        validate_object_key(key)?;
        let addressed_version = request
            .version_id
            .as_deref()
            .map(parse_version_id)
            .transpose()?;
        let transaction = self.begin_write()?;
        let mut superseded_files = Vec::new();
        let outcome = {
            let bucket_record = require_bucket(&transaction.open_table(BUCKETS)?, bucket)?;
            let mut latest = transaction.open_table(LATEST)?;
            let mut versions = transaction.open_table(VERSIONS)?;
            let stored_versions = load_versions(&versions, bucket, key)?;
            if let Some(guard) = guard {
                let records: Vec<VersionRecord> = stored_versions
                    .iter()
                    .map(|stored| stored.record.clone())
                    .collect();
                if !(guard.is_still_due)(&records) {
                    return Ok(None);
                }
            }
            let outcome = match (&request.version_id, addressed_version) {
                (Some(version_id), Some(internal_version)) => {
                    let removed = find_version(stored_versions, internal_version);
                    let is_delete_marker = removed
                        .as_ref()
                        .is_some_and(|stored| stored.record.is_delete_marker);
                    if let Some(stored) = removed {
                        versions.remove(stored.version_key.as_slice())?;
                        superseded_files
                            .extend(self.data_file_path(&bucket_record, &stored.record));
                    }
                    DeleteObjectOutcome {
                        version_id: Some(version_id.clone()),
                        is_delete_marker,
                    }
                }
                _ => {
                    let replaces_null_version =
                        bucket_record.versioning != VersioningState::Enabled;
                    if replaces_null_version {
                        for stored in stored_versions
                            .into_iter()
                            .filter(|stored| stored.record.is_null_version)
                        {
                            versions.remove(stored.version_key.as_slice())?;
                            superseded_files
                                .extend(self.data_file_path(&bucket_record, &stored.record));
                        }
                    }
                    match bucket_record.versioning {
                        VersioningState::Unversioned => DeleteObjectOutcome::default(),
                        VersioningState::Enabled | VersioningState::Suspended => {
                            let marker = self.new_delete_marker(
                                replaces_null_version,
                                guard.and_then(|guard| guard.marker_last_modified_unix_ms),
                            );
                            let internal_version = marker
                                .internal_version
                                .parse()
                                .expect("delete markers use ULIDs");
                            let version_key = encode_version_key(bucket, key, internal_version);
                            versions
                                .insert(version_key.as_slice(), encode_json(&marker)?.as_slice())?;
                            DeleteObjectOutcome {
                                version_id: Some(marker.version_id()),
                                is_delete_marker: true,
                            }
                        }
                    }
                }
            };
            refresh_latest(&mut latest, &versions, bucket, key)?;
            outcome
        };
        transaction.commit()?;
        remove_superseded_files(&superseded_files);
        Ok(Some(outcome))
    }

    fn new_delete_marker(
        &self,
        is_null_version: bool,
        last_modified_unix_ms: Option<i64>,
    ) -> VersionRecord {
        let internal_version = self.next_internal_version();
        VersionRecord {
            internal_version: internal_version.to_string(),
            is_null_version,
            is_delete_marker: true,
            size: 0,
            etag: String::new(),
            checksum: None,
            content_headers: ContentHeaders::default(),
            user_metadata: Default::default(),
            last_modified_unix_ms: last_modified_unix_ms.unwrap_or_else(|| {
                i64::try_from(internal_version.timestamp_ms()).expect("ULID timestamps fit in i64")
            }),
            data_file: None,
            parts_count: None,
        }
    }
}
