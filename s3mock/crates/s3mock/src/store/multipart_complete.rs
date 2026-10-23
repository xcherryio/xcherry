// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! CompleteMultipartUpload: validate the part list, concatenate the parts, and commit one version.

use std::fs::File;
use std::io::{self, Read};

use md5::{Digest, Md5};
use redb::ReadableDatabase;

use super::multipart::{CompletedPart, load_parts, parse_upload_id};
use super::object_write::{CompletedUpload, VersionAttributes};
use super::{
    BUCKETS, PARTS, PutObjectOutcome, Store, UPLOADS, WriteCondition, decode_json, require_bucket,
};
use crate::checksum::{ChecksumAlgorithm, ChecksumType, composite_checksum};
use crate::data_file::{ObjectWriter, UploadChecksumExpectations};
use crate::error::StoreError;
use crate::key_codec::encode_upload_key;
use crate::record::{BucketRecord, PartRecord, UploadRecord};

/// S3 rejects non-final parts below 5 MiB.
const MIN_PART_SIZE: u64 = 5 * 1024 * 1024;
const COPY_BUFFER_BYTES: usize = 1024 * 1024;

/// Parameters of a CompleteMultipartUpload request.
#[derive(Clone, Debug, Default)]
pub struct CompleteMultipartUploadRequest {
    pub parts: Vec<CompletedPart>,
    pub write_condition: WriteCondition,
    /// The object checksum from an `x-amz-checksum-*` header; a composite value may omit `-N`.
    pub expected_checksum: Option<(ChecksumAlgorithm, String)>,
    /// From `x-amz-checksum-type`; must equal the type chosen at creation.
    pub expected_checksum_type: Option<ChecksumType>,
    /// From `x-amz-mp-object-size`; must equal the assembled size.
    pub expected_object_size: Option<u64>,
}

impl Store {
    /// Assembles the listed parts into the newest version and deletes the upload.
    ///
    /// The write condition is checked at commit. A rejected completion keeps the upload.
    pub fn complete_multipart_upload(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        request: CompleteMultipartUploadRequest,
    ) -> Result<PutObjectOutcome, StoreError> {
        let upload_ulid = parse_upload_id(upload_id)?;
        let upload_key = encode_upload_key(bucket, key, upload_ulid);
        let (bucket_record, upload, stored_parts) = {
            let transaction = self.database.begin_read()?;
            let bucket_record = require_bucket(&transaction.open_table(BUCKETS)?, bucket)?;
            let upload: UploadRecord = match transaction
                .open_table(UPLOADS)?
                .get(upload_key.as_slice())?
            {
                Some(value) => decode_json(value.value())?,
                None => {
                    return Err(StoreError::NoSuchUpload {
                        upload_id: upload_id.to_owned(),
                    });
                }
            };
            let stored_parts = load_parts(&transaction.open_table(PARTS)?, &upload_key)?;
            (bucket_record, upload, stored_parts)
        };
        if let Some(expected_type) = request.expected_checksum_type
            && expected_type != upload.checksum_type
        {
            return Err(StoreError::BadDigest {
                algorithm: upload.checksum_type.s3_name(),
            });
        }
        let selected = select_parts(
            &request.parts,
            &stored_parts,
            upload.checksum_algorithm.is_some(),
        )?;
        let full_object_algorithm = match upload.checksum_type {
            ChecksumType::FullObject => upload
                .checksum_algorithm
                .unwrap_or(ChecksumAlgorithm::Crc64Nvme),
            // The concatenation still needs a hasher; its value is replaced by the composite checksum.
            ChecksumType::Composite => ChecksumAlgorithm::Crc64Nvme,
        };
        let staged = self
            .concatenate_parts(&bucket_record, &selected, full_object_algorithm)?
            .finish(&UploadChecksumExpectations::default())?;
        if let Some(expected_size) = request.expected_object_size
            && expected_size != staged.size()
        {
            return Err(StoreError::InvalidRequest(format!(
                "x-amz-mp-object-size {expected_size} does not match the assembled size {}",
                staged.size()
            )));
        }
        let part_count = u32::try_from(selected.len()).expect("at most 10000 parts are listed");
        let checksum = match (upload.checksum_type, upload.checksum_algorithm) {
            (ChecksumType::Composite, Some(algorithm)) => {
                let part_checksums: Vec<_> =
                    selected.iter().map(|part| part.checksum.clone()).collect();
                composite_checksum(algorithm, &part_checksums)
            }
            (ChecksumType::Composite, None) | (ChecksumType::FullObject, _) => {
                staged.checksum.clone()
            }
        };
        if let Some((algorithm, expected_value)) = &request.expected_checksum {
            if *algorithm != checksum.algorithm {
                return Err(StoreError::InvalidRequest(format!(
                    "the {} checksum does not match the upload's {} checksum algorithm",
                    algorithm.s3_name(),
                    checksum.algorithm.s3_name()
                )));
            }
            let without_part_count = |value: &str| {
                value
                    .trim()
                    .split('-')
                    .next()
                    .unwrap_or_default()
                    .to_owned()
            };
            if without_part_count(expected_value) != without_part_count(&checksum.base64_value) {
                return Err(StoreError::BadDigest {
                    algorithm: algorithm.s3_name(),
                });
            }
        }
        let attributes = VersionAttributes {
            content_headers: upload.content_headers,
            user_metadata: upload.user_metadata,
            etag: multipart_etag(&selected),
            checksum,
            parts_count: Some(part_count),
        };
        let completed_upload = CompletedUpload {
            upload_key,
            upload_id: upload_id.to_owned(),
            assembled_parts: selected
                .iter()
                .map(|part| (part.part_number, part.data_file.clone()))
                .collect(),
        };
        self.publish_and_commit(
            bucket,
            key,
            staged,
            attributes,
            &request.write_condition,
            Some(&completed_upload),
        )
    }

    fn concatenate_parts(
        &self,
        bucket_record: &BucketRecord,
        parts: &[&PartRecord],
        algorithm: ChecksumAlgorithm,
    ) -> Result<ObjectWriter, StoreError> {
        let mut writer = ObjectWriter::create(
            &self.temporary_directory,
            self.next_internal_version(),
            bucket_record.incarnation.clone(),
            algorithm,
        )?;
        // Open every part first: an open file stays readable even if a concurrent UploadPart replaces it.
        let mut files = Vec::with_capacity(parts.len());
        for part in parts {
            let path = self.bucket_directory(bucket_record).join(&part.data_file);
            match File::open(&path) {
                Ok(file) => files.push(file),
                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                    return Err(StoreError::InvalidPart(format!(
                        "part {} was replaced or removed during completion",
                        part.part_number
                    )));
                }
                Err(error) => return Err(error.into()),
            }
        }
        let mut buffer = vec![0u8; COPY_BUFFER_BYTES];
        for mut file in files {
            loop {
                let read = file.read(&mut buffer)?;
                if read == 0 {
                    break;
                }
                writer.write_all(&buffer[..read])?;
            }
        }
        Ok(writer)
    }
}

/// Matches the client's part list against the stored parts, applying S3's ordering, ETag, and size rules.
///
/// Uploads with a checksum algorithm must list parts 1, 2, 3, and so on without gaps.
fn select_parts<'stored>(
    listed: &[CompletedPart],
    stored: &'stored [PartRecord],
    is_checksummed: bool,
) -> Result<Vec<&'stored PartRecord>, StoreError> {
    if listed.is_empty() {
        return Err(StoreError::InvalidRequest(
            "you must specify at least one part".to_owned(),
        ));
    }
    let mut selected = Vec::with_capacity(listed.len());
    let mut previous_part_number = 0;
    for listed_part in listed {
        let is_out_of_order = if is_checksummed {
            listed_part.part_number != previous_part_number + 1
        } else {
            listed_part.part_number <= previous_part_number
        };
        if is_out_of_order {
            return Err(StoreError::InvalidPartOrder);
        }
        previous_part_number = listed_part.part_number;
        let invalid_part = || StoreError::InvalidPart(format!("part {}", listed_part.part_number));
        let part = stored
            .iter()
            .find(|part| part.part_number == listed_part.part_number)
            .ok_or_else(invalid_part)?;
        if listed_part.etag.trim().trim_matches('"') != part.etag {
            return Err(invalid_part());
        }
        if let Some(checksum) = &listed_part.checksum_base64
            && checksum.trim() != part.checksum.base64_value
        {
            return Err(invalid_part());
        }
        selected.push(part);
    }
    let last_index = selected.len() - 1;
    if let Some(too_small) = selected[..last_index]
        .iter()
        .find(|part| part.size < MIN_PART_SIZE)
    {
        return Err(StoreError::EntityTooSmall {
            part_number: too_small.part_number,
        });
    }
    Ok(selected)
}

/// Returns S3's multipart ETag: the MD5 of the concatenated binary part MD5s, then `-<part count>`.
fn multipart_etag(parts: &[&PartRecord]) -> String {
    let mut digest = Md5::new();
    for part in parts {
        digest.update(hex::decode(&part.etag).expect("stored part ETags are hex MD5 digests"));
    }
    format!("{}-{}", hex::encode(digest.finalize()), parts.len())
}
