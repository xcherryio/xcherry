// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! ListObjectsV2 and ListObjectVersions with prefix, delimiter folding, and pagination.

use redb::{ReadableDatabase, ReadableTable};

use super::{
    BUCKETS, LATEST, Store, VERSIONS, decode_json, find_version, load_versions, parse_version_id,
    require_bucket,
};
use crate::checksum::{decode_base64, encode_base64};
use crate::error::StoreError;
use crate::key_codec::{decode_object_key, encode_key_prefix, encode_object_key, prefix_successor};
use crate::record::VersionRecord;

/// S3 never returns more than 1000 entries per page.
pub const MAX_KEYS_LIMIT: usize = 1000;

/// Parameters of a ListObjectsV2 request.
#[derive(Clone, Debug)]
pub struct ListObjectsRequest {
    pub prefix: String,
    /// An empty delimiter disables folding, as in S3.
    pub delimiter: String,
    /// Clamped to [`MAX_KEYS_LIMIT`].
    pub max_keys: usize,
    pub continuation_token: Option<String>,
    pub start_after: Option<String>,
}

/// One object in a ListObjectsV2 page.
#[derive(Clone, Debug)]
pub struct ListedObject {
    pub key: String,
    pub record: VersionRecord,
}

/// One page of ListObjectsV2 results.
#[derive(Clone, Debug, Default)]
pub struct ListObjectsPage {
    pub objects: Vec<ListedObject>,
    pub common_prefixes: Vec<String>,
    pub is_truncated: bool,
    pub next_continuation_token: Option<String>,
}

impl ListObjectsPage {
    /// Returns `KeyCount`: objects plus common prefixes.
    pub fn key_count(&self) -> usize {
        self.objects.len() + self.common_prefixes.len()
    }
}

/// Parameters of a ListObjectVersions request.
#[derive(Clone, Debug)]
pub struct ListObjectVersionsRequest {
    pub prefix: String,
    pub delimiter: String,
    pub max_keys: usize,
    pub key_marker: Option<String>,
    pub version_id_marker: Option<String>,
}

/// One version or delete marker in a ListObjectVersions page.
#[derive(Clone, Debug)]
pub struct ListedVersion {
    pub key: String,
    pub record: VersionRecord,
    pub is_latest: bool,
}

/// One page of ListObjectVersions results.
#[derive(Clone, Debug, Default)]
pub struct ListObjectVersionsPage {
    pub versions: Vec<ListedVersion>,
    pub common_prefixes: Vec<String>,
    pub is_truncated: bool,
    pub next_key_marker: Option<String>,
    pub next_version_id_marker: Option<String>,
}

/// Where the previous page stopped, carried in the continuation token.
enum ResumePoint {
    AfterKey(String),
    AfterCommonPrefix(String),
}

impl Store {
    /// Lists the live objects of a bucket in key order.
    pub fn list_objects(
        &self,
        bucket: &str,
        request: &ListObjectsRequest,
    ) -> Result<ListObjectsPage, StoreError> {
        let transaction = self.database.begin_read()?;
        require_bucket(&transaction.open_table(BUCKETS)?, bucket)?;
        let latest = transaction.open_table(LATEST)?;
        let prefix_start = encode_key_prefix(bucket, &request.prefix);
        let end = prefix_successor(&prefix_start)
            .expect("encoded key prefixes contain a byte below 0xFF");
        let mut cursor = prefix_start;
        if let Some(start_after) = &request.start_after {
            cursor = cursor.max(after_key(bucket, start_after));
        }
        if let Some(token) = &request.continuation_token {
            cursor = cursor.max(match decode_continuation_token(token)? {
                ResumePoint::AfterKey(key) => after_key(bucket, &key),
                ResumePoint::AfterCommonPrefix(prefix) => after_prefix(bucket, &prefix),
            });
        }
        let max_keys = request.max_keys.min(MAX_KEYS_LIMIT);
        let mut page = ListObjectsPage::default();
        let mut resume_point = None;
        while let Some(entry) = first_entry(&latest, &cursor, &end)? {
            if page.key_count() == max_keys {
                page.is_truncated = max_keys > 0;
                break;
            }
            let key = decode_object_key(&entry.key, bucket);
            if let Some(common_prefix) =
                fold_common_prefix(&key, &request.prefix, &request.delimiter)
            {
                cursor = after_prefix(bucket, &common_prefix);
                // S3 returns only common prefixes after StartAfter.
                if is_at_or_before(&common_prefix, request.start_after.as_deref()) {
                    continue;
                }
                page.common_prefixes.push(common_prefix.clone());
                resume_point = Some(ResumePoint::AfterCommonPrefix(common_prefix));
                continue;
            }
            cursor = after_key(bucket, &key);
            page.objects.push(ListedObject {
                key: key.clone(),
                record: decode_json(&entry.value)?,
            });
            resume_point = Some(ResumePoint::AfterKey(key));
        }
        if page.is_truncated {
            page.next_continuation_token =
                resume_point.map(|point| encode_continuation_token(&point));
        }
        Ok(page)
    }

    /// Lists every version and delete marker, keys ascending and versions newest first.
    pub fn list_object_versions(
        &self,
        bucket: &str,
        request: &ListObjectVersionsRequest,
    ) -> Result<ListObjectVersionsPage, StoreError> {
        let transaction = self.database.begin_read()?;
        require_bucket(&transaction.open_table(BUCKETS)?, bucket)?;
        let versions = transaction.open_table(VERSIONS)?;
        let prefix_start = encode_key_prefix(bucket, &request.prefix);
        let end = prefix_successor(&prefix_start)
            .expect("encoded key prefixes contain a byte below 0xFF");
        let mut cursor = prefix_start;
        if let Some(key_marker) = &request.key_marker {
            let marker_cursor = match &request.version_id_marker {
                None => after_key(bucket, key_marker),
                Some(version_id_marker) => {
                    let internal_version = parse_version_id(version_id_marker)?;
                    let stored = find_version(load_versions(&versions, bucket, key_marker)?, internal_version)
                        .ok_or_else(|| {
                            StoreError::InvalidArgument(format!(
                                "version-id-marker {version_id_marker:?} is not a version of {key_marker:?}"
                            ))
                        })?;
                    immediate_successor(&stored.version_key)
                }
            };
            cursor = cursor.max(marker_cursor);
        }
        let max_keys = request.max_keys.min(MAX_KEYS_LIMIT);
        let mut page = ListObjectVersionsPage::default();
        let mut entry_count = 0;
        while let Some(entry) = first_entry(&versions, &cursor, &end)? {
            if entry_count == max_keys {
                page.is_truncated = max_keys > 0;
                break;
            }
            let key = decode_object_key(&entry.key, bucket);
            if let Some(common_prefix) =
                fold_common_prefix(&key, &request.prefix, &request.delimiter)
            {
                cursor = after_prefix(bucket, &common_prefix);
                // S3 returns only common prefixes after the key marker.
                if is_at_or_before(&common_prefix, request.key_marker.as_deref()) {
                    continue;
                }
                entry_count += 1;
                page.common_prefixes.push(common_prefix.clone());
                page.next_key_marker = Some(common_prefix);
                page.next_version_id_marker = None;
                continue;
            }
            entry_count += 1;
            let record: VersionRecord = decode_json(&entry.value)?;
            let object_key = encode_object_key(bucket, &key);
            let newest = first_entry(&versions, &object_key, &after_key(bucket, &key))?
                .expect("the current entry is a version of this key");
            cursor = immediate_successor(&entry.key);
            page.next_key_marker = Some(key.clone());
            page.next_version_id_marker = Some(record.version_id());
            page.versions.push(ListedVersion {
                key,
                record,
                is_latest: newest.key == entry.key,
            });
        }
        if !page.is_truncated {
            page.next_key_marker = None;
            page.next_version_id_marker = None;
        }
        Ok(page)
    }
}

/// An owned table entry, so the scan can seek again without holding a borrowed range.
struct TableEntry {
    key: Vec<u8>,
    value: Vec<u8>,
}

/// Returns the first entry in `[start, end)`.
fn first_entry(
    table: &impl ReadableTable<&'static [u8], &'static [u8]>,
    start: &[u8],
    end: &[u8],
) -> Result<Option<TableEntry>, StoreError> {
    if start >= end {
        return Ok(None);
    }
    match table.range(start..end)?.next() {
        Some(entry) => {
            let (key, value) = entry?;
            Ok(Some(TableEntry {
                key: key.value().to_vec(),
                value: value.value().to_vec(),
            }))
        }
        None => Ok(None),
    }
}

fn is_at_or_before(common_prefix: &str, marker: Option<&str>) -> bool {
    marker.is_some_and(|marker| common_prefix <= marker)
}

/// Returns the common prefix that `key` folds into, if the delimiter occurs after `prefix`.
fn fold_common_prefix(key: &str, prefix: &str, delimiter: &str) -> Option<String> {
    if delimiter.is_empty() {
        return None;
    }
    let remainder = key.strip_prefix(prefix)?;
    remainder
        .find(delimiter)
        .map(|index| key[..prefix.len() + index + delimiter.len()].to_owned())
}

/// Smallest encoded key after every entry of `key`, in both the latest and versions tables.
fn after_key(bucket: &str, key: &str) -> Vec<u8> {
    prefix_successor(&encode_object_key(bucket, key))
        .expect("encoded object keys end with a terminator below 0xFF")
}

/// Smallest encoded key after every key starting with `prefix`.
fn after_prefix(bucket: &str, prefix: &str) -> Vec<u8> {
    prefix_successor(&encode_key_prefix(bucket, prefix))
        .expect("encoded key prefixes contain a byte below 0xFF")
}

fn immediate_successor(encoded_key: &[u8]) -> Vec<u8> {
    let mut successor = encoded_key.to_vec();
    successor.push(0);
    successor
}

fn encode_continuation_token(point: &ResumePoint) -> String {
    let (tag, value) = match point {
        ResumePoint::AfterKey(key) => ('k', key),
        ResumePoint::AfterCommonPrefix(prefix) => ('p', prefix),
    };
    encode_base64(format!("{tag}{value}").as_bytes())
}

fn decode_continuation_token(token: &str) -> Result<ResumePoint, StoreError> {
    let invalid =
        || StoreError::InvalidArgument("the continuation token provided is incorrect".to_owned());
    let decoded =
        String::from_utf8(decode_base64(token).ok_or_else(invalid)?).map_err(|_| invalid())?;
    let mut characters = decoded.chars();
    match characters.next() {
        Some('k') => Ok(ResumePoint::AfterKey(characters.as_str().to_owned())),
        Some('p') => Ok(ResumePoint::AfterCommonPrefix(
            characters.as_str().to_owned(),
        )),
        _ => Err(invalid()),
    }
}
