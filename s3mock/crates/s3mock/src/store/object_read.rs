// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! GetObject and HeadObject: version selection, conditional reads, and byte ranges.

use std::fs::File;
use std::io;
use std::path::PathBuf;

use redb::ReadableDatabase;

use super::{
    BUCKETS, LATEST, Store, VERSIONS, decode_json, find_version, load_versions, parse_version_id,
    require_bucket,
};
use crate::data_file;
use crate::error::StoreError;
use crate::key_codec::encode_object_key;
use crate::record::{VersionRecord, VersioningState};

/// A reader retries when an overwrite removed the data file between lookup and open.
const MAX_READ_ATTEMPTS: usize = 3;

/// HTTP conditional-read headers, with dates as Unix seconds.
#[derive(Clone, Debug, Default)]
pub struct ReadConditions {
    pub if_match: Option<String>,
    pub if_none_match: Option<String>,
    pub if_modified_since_unix_seconds: Option<i64>,
    pub if_unmodified_since_unix_seconds: Option<i64>,
}

/// A single byte range from a `Range: bytes=` header.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ByteRangeRequest {
    /// `bytes=<start>-<end>` or `bytes=<start>-`.
    FromStart {
        start: u64,
        end_inclusive: Option<u64>,
    },
    /// `bytes=-<length>`: the last `length` bytes.
    Suffix { length: u64 },
}

/// The byte range a read serves, already clamped to the object size.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ResolvedRange {
    pub start: u64,
    pub end_inclusive: u64,
}

impl ResolvedRange {
    /// Returns the number of bytes in the range.
    pub fn length(&self) -> u64 {
        self.end_inclusive - self.start + 1
    }
}

/// Parameters of a GetObject or HeadObject request.
#[derive(Clone, Debug, Default)]
pub struct ObjectReadRequest {
    pub version_id: Option<String>,
    pub conditions: ReadConditions,
    pub range: Option<ByteRangeRequest>,
}

/// The version a read selected, without its body.
#[derive(Clone, Debug)]
pub struct ObjectSelection {
    pub record: VersionRecord,
    /// `None` serves the whole object.
    pub range: Option<ResolvedRange>,
    /// Versioning state of the bucket; S3 omits `x-amz-version-id` for unversioned buckets.
    pub bucket_versioning: VersioningState,
}

/// A selected version with its data file opened at the first served byte.
#[derive(Debug)]
pub struct ObjectBody {
    pub selection: ObjectSelection,
    pub file: File,
    /// Number of bytes to read from `file`.
    pub length: u64,
}

impl Store {
    /// Selects the version a HeadObject request returns.
    pub fn head_object(
        &self,
        bucket: &str,
        key: &str,
        request: &ObjectReadRequest,
    ) -> Result<ObjectSelection, StoreError> {
        self.select_object(bucket, key, request)
            .map(|(selection, _)| selection)
    }

    /// Selects the version a GetObject request returns and opens its data file.
    pub fn read_object(
        &self,
        bucket: &str,
        key: &str,
        request: &ObjectReadRequest,
    ) -> Result<ObjectBody, StoreError> {
        let mut attempt = 1;
        loop {
            let (selection, path) = self.select_object(bucket, key, request)?;
            let path = path.expect("live versions always have a data file");
            let start = selection.range.map_or(0, |range| range.start);
            let length = selection
                .range
                .map_or(selection.record.size, |range| range.length());
            match data_file::open_at(&path, start) {
                Ok(file) => {
                    return Ok(ObjectBody {
                        selection,
                        file,
                        length,
                    });
                }
                Err(error)
                    if error.kind() == io::ErrorKind::NotFound && attempt < MAX_READ_ATTEMPTS =>
                {
                    attempt += 1;
                }
                Err(error) => return Err(error.into()),
            }
        }
    }

    fn select_object(
        &self,
        bucket: &str,
        key: &str,
        request: &ObjectReadRequest,
    ) -> Result<(ObjectSelection, Option<PathBuf>), StoreError> {
        let transaction = self.database.begin_read()?;
        let bucket_record = require_bucket(&transaction.open_table(BUCKETS)?, bucket)?;
        let record = match &request.version_id {
            Some(version_id) => {
                let internal_version = parse_version_id(version_id)?;
                let versions = transaction.open_table(VERSIONS)?;
                let stored = find_version(load_versions(&versions, bucket, key)?, internal_version)
                    .ok_or_else(|| StoreError::NoSuchVersion {
                        version_id: version_id.clone(),
                    })?;
                if stored.record.is_delete_marker {
                    return Err(StoreError::MethodNotAllowedOnDeleteMarker {
                        version_id: version_id.clone(),
                        last_modified_unix_ms: stored.record.last_modified_unix_ms,
                    });
                }
                stored.record
            }
            None => {
                let latest = transaction.open_table(LATEST)?;
                match latest.get(encode_object_key(bucket, key).as_slice())? {
                    Some(value) => decode_json(value.value())?,
                    None => {
                        let versions = transaction.open_table(VERSIONS)?;
                        return Err(
                            match load_versions(&versions, bucket, key)?.into_iter().next() {
                                Some(newest) => StoreError::DeleteMarker {
                                    key: key.to_owned(),
                                    version_id: newest.record.version_id(),
                                },
                                None => StoreError::NoSuchKey {
                                    bucket: bucket.to_owned(),
                                    key: key.to_owned(),
                                },
                            },
                        );
                    }
                }
            }
        };
        evaluate_read_conditions(&request.conditions, &record)?;
        let range = request
            .range
            .map(|range| resolve_range(range, record.size))
            .transpose()?;
        let path = self.data_file_path(&bucket_record, &record);
        Ok((
            ObjectSelection {
                record,
                range,
                bucket_versioning: bucket_record.versioning,
            },
            path,
        ))
    }
}

/// Applies RFC 9110 precedence: `If-Match` overrides `If-Unmodified-Since`, `If-None-Match` overrides `If-Modified-Since`.
fn evaluate_read_conditions(
    conditions: &ReadConditions,
    record: &VersionRecord,
) -> Result<(), StoreError> {
    let last_modified_seconds = record.last_modified_unix_ms.div_euclid(1000);
    match (
        &conditions.if_match,
        conditions.if_unmodified_since_unix_seconds,
    ) {
        (Some(if_match), _) if !etag_matches(if_match, &record.etag) => {
            return Err(StoreError::PreconditionFailed);
        }
        (None, Some(since)) if last_modified_seconds > since => {
            return Err(StoreError::PreconditionFailed);
        }
        _ => {}
    }
    match (
        &conditions.if_none_match,
        conditions.if_modified_since_unix_seconds,
    ) {
        (Some(if_none_match), _) if etag_matches(if_none_match, &record.etag) => {
            Err(StoreError::NotModified)
        }
        (None, Some(since)) if last_modified_seconds <= since => Err(StoreError::NotModified),
        _ => Ok(()),
    }
}

/// Returns whether an `If-Match`/`If-None-Match` value (`*` or a list of quoted or weak ETags) matches.
pub(super) fn etag_matches(header_value: &str, etag: &str) -> bool {
    header_value.split(',').any(|candidate| {
        let candidate = candidate.trim();
        candidate == "*" || candidate.trim_start_matches("W/").trim_matches('"') == etag
    })
}

fn resolve_range(range: ByteRangeRequest, object_size: u64) -> Result<ResolvedRange, StoreError> {
    let unsatisfiable = StoreError::InvalidRange { object_size };
    if object_size == 0 {
        return Err(unsatisfiable);
    }
    let last_byte = object_size - 1;
    match range {
        ByteRangeRequest::FromStart {
            start,
            end_inclusive,
        } => {
            if start > last_byte || end_inclusive.is_some_and(|end| end < start) {
                return Err(unsatisfiable);
            }
            Ok(ResolvedRange {
                start,
                end_inclusive: end_inclusive.map_or(last_byte, |end| end.min(last_byte)),
            })
        }
        ByteRangeRequest::Suffix { length } => {
            if length == 0 {
                return Err(unsatisfiable);
            }
            Ok(ResolvedRange {
                start: object_size.saturating_sub(length),
                end_inclusive: last_byte,
            })
        }
    }
}
