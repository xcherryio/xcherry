// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Multipart uploads: creation, parts, abort, and listing. Completion lives in `multipart_complete`.

use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use redb::{ReadableDatabase, ReadableTable, Table};
use ulid::Ulid;

use super::object_listing::{
    after_key, after_prefix, first_entry, fold_common_prefix, immediate_successor, is_at_or_before,
};
use super::object_write::CompletedUpload;
use super::{
    BUCKETS, MAX_KEYS_LIMIT, PARTS, Store, UPLOADS, UPLOADS_DIRECTORY_NAME,
    create_directory_durably, decode_json, encode_json, require_bucket, validate_object_key,
};
use crate::checksum::{ChecksumAlgorithm, ChecksumType};
use crate::data_file::{self, ObjectWriter, StagedObject};
use crate::error::StoreError;
use crate::key_codec::{
    decode_object_key, encode_key_prefix, encode_part_key, encode_upload_key, prefix_successor,
};
use crate::record::{BucketRecord, ContentHeaders, PartRecord, UploadRecord};

/// The highest part number S3 accepts.
pub const MAX_PART_NUMBER: u32 = 10_000;

/// Attributes fixed when a multipart upload is created.
#[derive(Clone, Debug, Default)]
pub struct CreateMultipartUploadRequest {
    pub content_headers: ContentHeaders,
    pub user_metadata: BTreeMap<String, String>,
    pub checksum_algorithm: Option<ChecksumAlgorithm>,
    /// Defaults to FULL_OBJECT for CRC64NVME and COMPOSITE for the other algorithms, as in S3.
    pub checksum_type: Option<ChecksumType>,
}

/// One entry of a CompleteMultipartUpload part list.
#[derive(Clone, Debug)]
pub struct CompletedPart {
    pub part_number: u32,
    /// The part ETag, quoted or not.
    pub etag: String,
    /// The part checksum the client listed, in the upload's algorithm.
    pub checksum_base64: Option<String>,
}

/// Parameters of a ListParts request.
#[derive(Clone, Debug)]
pub struct ListPartsRequest {
    /// Lists parts with a greater number; 0 starts at the first part.
    pub part_number_marker: u32,
    pub max_parts: usize,
}

/// One page of ListParts results.
#[derive(Clone, Debug)]
pub struct PartsPage {
    pub upload: UploadRecord,
    pub parts: Vec<PartRecord>,
    pub is_truncated: bool,
    pub next_part_number_marker: Option<u32>,
}

/// Parameters of a ListMultipartUploads request.
#[derive(Clone, Debug)]
pub struct ListMultipartUploadsRequest {
    pub prefix: String,
    pub delimiter: String,
    pub key_marker: Option<String>,
    pub upload_id_marker: Option<String>,
    pub max_uploads: usize,
}

/// One page of ListMultipartUploads results, ordered by key and then initiation time.
#[derive(Clone, Debug, Default)]
pub struct MultipartUploadsPage {
    pub uploads: Vec<UploadRecord>,
    pub common_prefixes: Vec<String>,
    pub is_truncated: bool,
    pub next_key_marker: Option<String>,
    pub next_upload_id_marker: Option<String>,
}

impl Store {
    /// Starts a multipart upload; the object stays invisible until the upload completes.
    pub fn create_multipart_upload(
        &self,
        bucket: &str,
        key: &str,
        request: CreateMultipartUploadRequest,
    ) -> Result<UploadRecord, StoreError> {
        validate_object_key(key)?;
        let checksum_type =
            resolve_checksum_type(request.checksum_algorithm, request.checksum_type)?;
        let upload_id = self.next_internal_version();
        let transaction = self.begin_write()?;
        let record = {
            let bucket_record = require_bucket(&transaction.open_table(BUCKETS)?, bucket)?;
            let record = UploadRecord {
                upload_id: upload_id.to_string(),
                key: key.to_owned(),
                bucket_incarnation: bucket_record.incarnation,
                initiated_unix_ms: unix_ms(upload_id),
                content_headers: request.content_headers,
                user_metadata: request.user_metadata,
                checksum_algorithm: request.checksum_algorithm,
                checksum_type,
            };
            let mut uploads = transaction.open_table(UPLOADS)?;
            let upload_key = encode_upload_key(bucket, key, upload_id);
            uploads.insert(upload_key.as_slice(), encode_json(&record)?.as_slice())?;
            record
        };
        transaction.commit()?;
        Ok(record)
    }

    /// Starts writing one part. The upload's checksum algorithm, when set, overrides `requested_algorithm`.
    pub fn begin_upload_part(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: u32,
        requested_algorithm: ChecksumAlgorithm,
    ) -> Result<ObjectWriter, StoreError> {
        validate_part_number(part_number)?;
        let upload = self.upload(bucket, key, upload_id)?;
        ObjectWriter::create(
            &self.temporary_directory,
            self.next_internal_version(),
            upload.bucket_incarnation,
            upload.checksum_algorithm.unwrap_or(requested_algorithm),
        )
    }

    /// Stores a staged part, replacing any earlier part with the same number.
    pub fn commit_upload_part(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: u32,
        staged: StagedObject,
    ) -> Result<PartRecord, StoreError> {
        validate_part_number(part_number)?;
        let upload_ulid = parse_upload_id(upload_id)?;
        let bucket_record = self.bucket(bucket)?;
        if bucket_record.incarnation != staged.bucket_incarnation {
            return Err(StoreError::NoSuchBucket(bucket.to_owned()));
        }
        let file_name = staged.upload_id.to_string();
        let relative_path = format!("{UPLOADS_DIRECTORY_NAME}/{upload_id}/{file_name}");
        let upload_directory = self.upload_directory(&bucket_record, upload_id);
        create_directory_durably(&upload_directory, self.is_durable())?;
        let path = upload_directory.join(&file_name);
        let record = PartRecord {
            part_number,
            size: staged.size,
            etag: staged.md5_hex.clone(),
            checksum: staged.checksum.clone(),
            last_modified_unix_ms: unix_ms(staged.upload_id),
            data_file: relative_path,
        };
        if self.is_durable() {
            File::open(staged.temporary_file.path())?.sync_all()?;
        }
        staged.temporary_file.publish(&path)?;
        match self.insert_part(bucket, key, upload_ulid, &bucket_record, &record) {
            Ok(replaced) => {
                if let Some(replaced) = replaced {
                    remove_file_logged(&replaced);
                }
                Ok(record)
            }
            Err(error) => {
                remove_file_logged(&path);
                Err(error)
            }
        }
    }

    fn insert_part(
        &self,
        bucket: &str,
        key: &str,
        upload_ulid: Ulid,
        bucket_record: &BucketRecord,
        record: &PartRecord,
    ) -> Result<Option<PathBuf>, StoreError> {
        if self.is_durable() {
            let path = self.bucket_directory(bucket_record).join(&record.data_file);
            data_file::sync_directory(
                path.parent()
                    .expect("part files live in an upload directory"),
            )?;
        }
        let transaction = self.begin_write()?;
        let replaced = {
            let current_bucket = require_bucket(&transaction.open_table(BUCKETS)?, bucket)?;
            if current_bucket.incarnation != bucket_record.incarnation {
                return Err(StoreError::NoSuchBucket(bucket.to_owned()));
            }
            let upload_key = encode_upload_key(bucket, key, upload_ulid);
            if transaction
                .open_table(UPLOADS)?
                .get(upload_key.as_slice())?
                .is_none()
            {
                return Err(no_such_upload(upload_ulid));
            }
            let mut parts = transaction.open_table(PARTS)?;
            let part_key = encode_part_key(&upload_key, record.part_number);
            let replaced: Option<PartRecord> = match parts.get(part_key.as_slice())? {
                Some(value) => Some(decode_json(value.value())?),
                None => None,
            };
            parts.insert(part_key.as_slice(), encode_json(record)?.as_slice())?;
            replaced.map(|part| self.bucket_directory(bucket_record).join(part.data_file))
        };
        transaction.commit()?;
        Ok(replaced)
    }

    /// Deletes an upload and its parts.
    pub fn abort_multipart_upload(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> Result<(), StoreError> {
        let upload_ulid = parse_upload_id(upload_id)?;
        let transaction = self.begin_write()?;
        let directory = {
            let bucket_record = require_bucket(&transaction.open_table(BUCKETS)?, bucket)?;
            let mut uploads = transaction.open_table(UPLOADS)?;
            let mut parts = transaction.open_table(PARTS)?;
            let upload = CompletedUpload {
                upload_key: encode_upload_key(bucket, key, upload_ulid),
                upload_id: upload_id.to_owned(),
                assembled_parts: Vec::new(),
            };
            remove_upload(&mut uploads, &mut parts, &upload)?;
            self.upload_directory(&bucket_record, upload_id)
        };
        transaction.commit()?;
        remove_upload_directory(&directory);
        Ok(())
    }

    /// Lists the parts of an upload in part-number order.
    pub fn list_parts(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        request: &ListPartsRequest,
    ) -> Result<PartsPage, StoreError> {
        let upload_ulid = parse_upload_id(upload_id)?;
        let transaction = self.database.begin_read()?;
        require_bucket(&transaction.open_table(BUCKETS)?, bucket)?;
        let upload_key = encode_upload_key(bucket, key, upload_ulid);
        let upload: UploadRecord = match transaction
            .open_table(UPLOADS)?
            .get(upload_key.as_slice())?
        {
            Some(value) => decode_json(value.value())?,
            None => return Err(no_such_upload(upload_ulid)),
        };
        let max_parts = request.max_parts.min(MAX_KEYS_LIMIT);
        let all_parts = load_parts(&transaction.open_table(PARTS)?, &upload_key)?;
        let mut remaining = all_parts
            .into_iter()
            .filter(|part| part.part_number > request.part_number_marker);
        let parts: Vec<PartRecord> = remaining.by_ref().take(max_parts).collect();
        let is_truncated = max_parts > 0 && remaining.next().is_some();
        let next_part_number_marker = parts.last().map(|part| part.part_number);
        Ok(PartsPage {
            upload,
            parts,
            is_truncated,
            next_part_number_marker,
        })
    }

    /// Lists in-progress uploads with prefix, delimiter folding, and marker pagination.
    pub fn list_multipart_uploads(
        &self,
        bucket: &str,
        request: &ListMultipartUploadsRequest,
    ) -> Result<MultipartUploadsPage, StoreError> {
        let transaction = self.database.begin_read()?;
        require_bucket(&transaction.open_table(BUCKETS)?, bucket)?;
        let uploads = transaction.open_table(UPLOADS)?;
        let prefix_start = encode_key_prefix(bucket, &request.prefix);
        let end = prefix_successor(&prefix_start)
            .expect("encoded key prefixes contain a byte below 0xFF");
        let mut cursor = prefix_start;
        if let Some(key_marker) = &request.key_marker {
            let marker_cursor = match &request.upload_id_marker {
                Some(upload_id_marker) => {
                    let marker_ulid = parse_upload_id(upload_id_marker)?;
                    immediate_successor(&encode_upload_key(bucket, key_marker, marker_ulid))
                }
                None => after_key(bucket, key_marker),
            };
            cursor = cursor.max(marker_cursor);
        }
        let max_uploads = request.max_uploads.min(MAX_KEYS_LIMIT);
        let mut page = MultipartUploadsPage::default();
        let mut entry_count = 0;
        while let Some(entry) = first_entry(&uploads, &cursor, &end)? {
            let key = decode_object_key(&entry.key, bucket);
            let common_prefix = fold_common_prefix(&key, &request.prefix, &request.delimiter);
            if let Some(common_prefix) = &common_prefix
                && is_at_or_before(common_prefix, request.key_marker.as_deref())
            {
                cursor = after_prefix(bucket, common_prefix);
                continue;
            }
            if entry_count == max_uploads {
                page.is_truncated = max_uploads > 0;
                break;
            }
            entry_count += 1;
            if let Some(common_prefix) = common_prefix {
                cursor = after_prefix(bucket, &common_prefix);
                page.next_key_marker = Some(common_prefix.clone());
                page.next_upload_id_marker = None;
                page.common_prefixes.push(common_prefix);
                continue;
            }
            let upload: UploadRecord = decode_json(&entry.value)?;
            cursor = immediate_successor(&entry.key);
            page.next_key_marker = Some(key);
            page.next_upload_id_marker = Some(upload.upload_id.clone());
            page.uploads.push(upload);
        }
        if !page.is_truncated {
            page.next_key_marker = None;
            page.next_upload_id_marker = None;
        }
        Ok(page)
    }

    /// Loads one upload record, or `NoSuchUpload`.
    pub(super) fn upload(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> Result<UploadRecord, StoreError> {
        let upload_ulid = parse_upload_id(upload_id)?;
        let transaction = self.database.begin_read()?;
        require_bucket(&transaction.open_table(BUCKETS)?, bucket)?;
        let upload_key = encode_upload_key(bucket, key, upload_ulid);
        match transaction
            .open_table(UPLOADS)?
            .get(upload_key.as_slice())?
        {
            Some(value) => decode_json(value.value()),
            None => Err(no_such_upload(upload_ulid)),
        }
    }
}

/// Removes an upload record and all of its part records, or fails with `NoSuchUpload`.
pub(super) fn remove_upload(
    uploads: &mut Table<&'static [u8], &'static [u8]>,
    parts: &mut Table<&'static [u8], &'static [u8]>,
    upload: &CompletedUpload,
) -> Result<(), StoreError> {
    if uploads.remove(upload.upload_key.as_slice())?.is_none() {
        return Err(StoreError::NoSuchUpload {
            upload_id: upload.upload_id.clone(),
        });
    }
    for (part_number, data_file) in &upload.assembled_parts {
        let part_key = encode_part_key(&upload.upload_key, *part_number);
        let current: Option<PartRecord> = match parts.get(part_key.as_slice())? {
            Some(value) => Some(decode_json(value.value())?),
            None => None,
        };
        if current.is_none_or(|part| &part.data_file != data_file) {
            return Err(StoreError::InvalidPart(format!(
                "part {part_number} was replaced during completion"
            )));
        }
    }
    let end = prefix_successor(&upload.upload_key).expect("upload keys contain a byte below 0xFF");
    let mut part_keys = Vec::new();
    for entry in parts.range(upload.upload_key.as_slice()..end.as_slice())? {
        part_keys.push(entry?.0.value().to_vec());
    }
    for part_key in part_keys {
        parts.remove(part_key.as_slice())?;
    }
    Ok(())
}

/// Loads every part of an upload in part-number order.
pub(super) fn load_parts(
    parts: &impl ReadableTable<&'static [u8], &'static [u8]>,
    upload_key: &[u8],
) -> Result<Vec<PartRecord>, StoreError> {
    let end = prefix_successor(upload_key).expect("upload keys contain a byte below 0xFF");
    let mut loaded = Vec::new();
    for entry in parts.range(upload_key..end.as_slice())? {
        loaded.push(decode_json(entry?.1.value())?);
    }
    Ok(loaded)
}

/// Removes the part files of a finished upload; leftovers are collected at the next startup.
pub(super) fn remove_upload_directory(directory: &Path) {
    match fs::remove_dir_all(directory) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => {
            tracing::warn!(path = %directory.display(), %error, "failed to remove upload directory");
        }
    }
}

/// Parses an upload ID; IDs s3mock never issued cannot name an upload.
pub(super) fn parse_upload_id(upload_id: &str) -> Result<Ulid, StoreError> {
    Ulid::from_str(upload_id).map_err(|_| StoreError::NoSuchUpload {
        upload_id: upload_id.to_owned(),
    })
}

fn no_such_upload(upload_ulid: Ulid) -> StoreError {
    StoreError::NoSuchUpload {
        upload_id: upload_ulid.to_string(),
    }
}

fn validate_part_number(part_number: u32) -> Result<(), StoreError> {
    if (1..=MAX_PART_NUMBER).contains(&part_number) {
        Ok(())
    } else {
        Err(StoreError::InvalidArgument(format!(
            "part number must be an integer between 1 and {MAX_PART_NUMBER}"
        )))
    }
}

/// Applies S3's checksum-type defaults and rejects combinations S3 rejects.
fn resolve_checksum_type(
    algorithm: Option<ChecksumAlgorithm>,
    requested: Option<ChecksumType>,
) -> Result<ChecksumType, StoreError> {
    let invalid = |message: &str| Err(StoreError::InvalidRequest(message.to_owned()));
    match (algorithm, requested) {
        (None, None) => Ok(ChecksumType::FullObject),
        (None, Some(_)) => invalid("x-amz-checksum-type requires x-amz-checksum-algorithm"),
        (Some(ChecksumAlgorithm::Crc64Nvme), None | Some(ChecksumType::FullObject)) => {
            Ok(ChecksumType::FullObject)
        }
        (Some(ChecksumAlgorithm::Crc64Nvme), Some(ChecksumType::Composite)) => {
            invalid("CRC64NVME supports only the FULL_OBJECT checksum type")
        }
        (
            Some(ChecksumAlgorithm::Sha1 | ChecksumAlgorithm::Sha256),
            Some(ChecksumType::FullObject),
        ) => invalid("SHA1 and SHA256 support only the COMPOSITE checksum type"),
        (Some(_), None | Some(ChecksumType::Composite)) => Ok(ChecksumType::Composite),
        (
            Some(ChecksumAlgorithm::Crc32 | ChecksumAlgorithm::Crc32c),
            Some(ChecksumType::FullObject),
        ) => Ok(ChecksumType::FullObject),
    }
}

fn remove_file_logged(path: &Path) {
    if let Err(error) = fs::remove_file(path) {
        tracing::warn!(path = %path.display(), %error, "failed to remove part file");
    }
}

fn unix_ms(ulid: Ulid) -> i64 {
    i64::try_from(ulid.timestamp_ms()).expect("ULID timestamps fit in i64")
}
