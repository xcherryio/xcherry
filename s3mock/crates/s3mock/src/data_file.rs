// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Self-describing data files: `[data][header JSON][u32 header length][u16 format][magic]`.
//!
//! The header follows the data because its size and digests are known only after streaming,
//! and keeping data at offset zero lets range reads map directly onto file offsets.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use md5::Digest as _;
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::checksum::{ChecksumAlgorithm, ChecksumHasher, ObjectChecksum, decode_base64};
use crate::error::StoreError;
use crate::record::VersionRecord;

const FOOTER_MAGIC: &[u8; 8] = b"S3MOCK01";
const FORMAT_VERSION: u16 = 1;
const FOOTER_TRAILER_LEN: u64 = 4 + 2 + 8;

/// Header stored in every data file so the metadata index can be rebuilt from the files.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct DataFileHeader {
    pub(crate) bucket: String,
    pub(crate) key: String,
    pub(crate) record: VersionRecord,
}

/// Checksums the client declared for an upload, verified when the upload finishes.
#[derive(Clone, Debug, Default)]
pub struct UploadChecksumExpectations {
    /// Raw `Content-MD5` header value.
    pub content_md5_base64: Option<String>,
    /// Algorithm the client asked S3 to compute, from `x-amz-sdk-checksum-algorithm` or a checksum header.
    pub algorithm: Option<ChecksumAlgorithm>,
    /// Expected value from a `x-amz-checksum-*` header or an aws-chunked trailer.
    pub expected_value_base64: Option<String>,
}

/// Owns a temporary upload file and deletes it on drop unless it was published.
#[derive(Debug)]
pub(crate) struct TemporaryFile {
    path: PathBuf,
    is_published: bool,
}

impl TemporaryFile {
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    /// Renames the file to `destination`; afterwards the destination owns the bytes.
    pub(crate) fn publish(mut self, destination: &Path) -> io::Result<()> {
        fs::rename(&self.path, destination)?;
        self.is_published = true;
        Ok(())
    }
}

impl Drop for TemporaryFile {
    fn drop(&mut self) {
        if self.is_published {
            return;
        }
        if let Err(error) = fs::remove_file(&self.path) {
            tracing::warn!(path = %self.path.display(), %error, "failed to remove unpublished upload");
        }
    }
}

/// Streams an upload into a temporary file while computing its MD5 and checksum.
///
/// Dropping the writer, or the [`StagedObject`] it produces, deletes the temporary file.
pub struct ObjectWriter {
    upload_id: Ulid,
    bucket_incarnation: String,
    temporary_file: TemporaryFile,
    file: File,
    size: u64,
    md5: md5::Md5,
    checksum: ChecksumHasher,
    checksum_algorithm: ChecksumAlgorithm,
}

/// A fully written and verified upload that is not yet visible to readers.
#[derive(Debug)]
pub struct StagedObject {
    /// Names the data file; version order is assigned separately when the upload commits.
    pub(crate) upload_id: Ulid,
    pub(crate) bucket_incarnation: String,
    pub(crate) temporary_file: TemporaryFile,
    pub(crate) size: u64,
    pub(crate) md5_hex: String,
    pub(crate) checksum: ObjectChecksum,
}

impl ObjectWriter {
    pub(crate) fn create(
        temporary_directory: &Path,
        upload_id: Ulid,
        bucket_incarnation: String,
        checksum_algorithm: ChecksumAlgorithm,
    ) -> Result<ObjectWriter, StoreError> {
        let path = temporary_directory.join(format!("{upload_id}.tmp"));
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)?;
        Ok(ObjectWriter {
            upload_id,
            bucket_incarnation,
            temporary_file: TemporaryFile {
                path,
                is_published: false,
            },
            file,
            size: 0,
            md5: md5::Md5::new(),
            checksum: ChecksumHasher::new(checksum_algorithm),
            checksum_algorithm,
        })
    }

    /// Appends object bytes.
    pub fn write_all(&mut self, data: &[u8]) -> Result<(), StoreError> {
        self.file.write_all(data)?;
        self.md5.update(data);
        self.checksum.update(data);
        self.size += data.len() as u64;
        Ok(())
    }

    /// Returns the checksum algorithm this writer computes.
    pub fn checksum_algorithm(&self) -> ChecksumAlgorithm {
        self.checksum_algorithm
    }

    /// Verifies the declared digests and returns the staged upload; on mismatch the file is deleted.
    pub fn finish(
        self,
        expectations: &UploadChecksumExpectations,
    ) -> Result<StagedObject, StoreError> {
        let md5_bytes = self.md5.finalize();
        let checksum = self.checksum.finish();
        if let Some(content_md5) = &expectations.content_md5_base64 {
            let Some(declared) =
                decode_base64(content_md5).filter(|declared| declared.len() == md5_bytes.len())
            else {
                return Err(StoreError::InvalidDigest);
            };
            if declared.as_slice() != md5_bytes.as_slice() {
                return Err(StoreError::BadDigest { algorithm: "MD5" });
            }
        }
        if let Some(expected) = &expectations.expected_value_base64
            && expected.trim() != checksum.base64_value
        {
            return Err(StoreError::BadDigest {
                algorithm: checksum.algorithm.s3_name(),
            });
        }
        Ok(StagedObject {
            upload_id: self.upload_id,
            bucket_incarnation: self.bucket_incarnation,
            temporary_file: self.temporary_file,
            size: self.size,
            md5_hex: hex::encode(md5_bytes),
            checksum,
        })
    }
}

impl StagedObject {
    /// Returns the object size in bytes.
    pub fn size(&self) -> u64 {
        self.size
    }
}

/// Appends the header footer to a staged data file and optionally flushes it to stable storage.
pub(crate) fn append_footer(
    path: &Path,
    header: &DataFileHeader,
    is_durable: bool,
) -> Result<(), StoreError> {
    let header_json = serde_json::to_vec(header)?;
    let mut file = OpenOptions::new().append(true).open(path)?;
    file.write_all(&header_json)?;
    let header_len = u32::try_from(header_json.len())
        .map_err(|_| StoreError::InvalidRequest("object metadata is too large".to_owned()))?;
    file.write_all(&header_len.to_le_bytes())?;
    file.write_all(&FORMAT_VERSION.to_le_bytes())?;
    file.write_all(FOOTER_MAGIC)?;
    if is_durable {
        file.sync_all()?;
    }
    Ok(())
}

/// Reads the header of a data file, used to rebuild metadata and to validate files.
pub(crate) fn read_header(path: &Path) -> Result<DataFileHeader, StoreError> {
    let corrupt = |reason: &str| StoreError::CorruptDataFile {
        path: path.display().to_string(),
        reason: reason.to_owned(),
    };
    let mut file = File::open(path)?;
    let file_len = file.metadata()?.len();
    if file_len < FOOTER_TRAILER_LEN {
        return Err(corrupt("file is shorter than the footer"));
    }
    file.seek(SeekFrom::Start(file_len - FOOTER_TRAILER_LEN))?;
    let mut trailer = [0u8; FOOTER_TRAILER_LEN as usize];
    file.read_exact(&mut trailer)?;
    if &trailer[6..] != FOOTER_MAGIC {
        return Err(corrupt("footer magic is missing"));
    }
    let header_len = u64::from(u32::from_le_bytes([
        trailer[0], trailer[1], trailer[2], trailer[3],
    ]));
    let format_version = u16::from_le_bytes([trailer[4], trailer[5]]);
    if format_version != FORMAT_VERSION {
        return Err(corrupt("footer format version is unsupported"));
    }
    if header_len + FOOTER_TRAILER_LEN > file_len {
        return Err(corrupt("header length exceeds the file"));
    }
    file.seek(SeekFrom::Start(file_len - FOOTER_TRAILER_LEN - header_len))?;
    let mut header_json = vec![0u8; header_len as usize];
    file.read_exact(&mut header_json)?;
    Ok(serde_json::from_slice(&header_json)?)
}

/// Opens a data file positioned at `offset`, the start of the requested object bytes.
pub(crate) fn open_at(path: &Path, offset: u64) -> io::Result<File> {
    let mut file = File::open(path)?;
    file.seek(SeekFrom::Start(offset))?;
    Ok(file)
}

/// Flushes a directory entry so a rename into it survives a crash.
pub(crate) fn sync_directory(directory: &Path) -> io::Result<()> {
    File::open(directory)?.sync_all()
}
