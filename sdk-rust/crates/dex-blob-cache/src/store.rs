// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::SystemTime;

use sha2::{Digest, Sha256};

use super::BlobCacheError;
use super::config::MAX_BLOB_ID_BYTES;
use super::entry::DiskEntry;
use super::format::{FIXED_HEADER_SIZE, FileHeader, FileMetadata, decode_header, encode_header};

static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Debug)]
pub(crate) struct LocalFileStore {
    root: PathBuf,
    temp: PathBuf,
    blobs: PathBuf,
}

#[derive(Debug)]
pub(crate) struct ScanResult {
    pub(crate) entries: Vec<FileMetadata>,
    pub(crate) invalid_paths: Vec<PathBuf>,
}

pub(crate) struct CommitFailure {
    pub(crate) error: BlobCacheError,
    pub(crate) orphan_path: Option<PathBuf>,
}

impl From<BlobCacheError> for CommitFailure {
    fn from(error: BlobCacheError) -> Self {
        Self {
            error,
            orphan_path: None,
        }
    }
}

impl LocalFileStore {
    pub(crate) fn new(root: &Path) -> Result<Self, BlobCacheError> {
        let root = absolute_path(root)?;
        Ok(Self {
            temp: root.join("tmp"),
            blobs: root.join("blobs"),
            root,
        })
    }

    pub(crate) fn prepare(&self) -> Result<(), BlobCacheError> {
        create_private_directory(&self.root, "create blob cache root")?;
        create_private_directory(&self.temp, "create blob cache temp directory")?;
        create_private_directory(&self.blobs, "create blob cache data directory")
    }

    pub(crate) fn purge_temp(&self) -> Result<(), BlobCacheError> {
        remove_directory_if_present(&self.temp, "purge blob cache temp directory")?;
        create_private_directory(&self.temp, "recreate blob cache temp directory")
    }

    pub(crate) fn scan(&self) -> Result<ScanResult, BlobCacheError> {
        let mut result = ScanResult {
            entries: Vec::new(),
            invalid_paths: Vec::new(),
        };
        self.scan_directory(&self.blobs, &mut result)?;
        result.entries.sort_by(|left, right| {
            right
                .modified
                .cmp(&left.modified)
                .then_with(|| left.path.cmp(&right.path))
        });
        result.invalid_paths.sort();
        Ok(result)
    }

    pub(crate) fn path_for(&self, blob_id: &str) -> PathBuf {
        let digest = Sha256::digest(blob_id.as_bytes());
        let encoded = encode_hex(&digest);
        self.blobs
            .join(&encoded[0..2])
            .join(&encoded[2..4])
            .join(format!("{encoded}.blob"))
    }

    pub(crate) fn commit(
        &self,
        metadata: &FileMetadata,
        payload: &[u8],
    ) -> Result<(), CommitFailure> {
        let parent = metadata
            .path
            .parent()
            .ok_or_else(|| BlobCacheError::InvalidBlob("cache path has no parent".to_owned()))?;
        create_private_directory(parent, "create blob cache shard")?;
        match fs::symlink_metadata(&metadata.path) {
            Ok(_) => {
                return Err(BlobCacheError::ContentMismatch(metadata.blob_id.clone()).into());
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(BlobCacheError::io("inspect final cache path", error).into());
            }
        }

        let (mut temp_file, temp_path) = self.create_temp_file()?;
        let write_result = self.write_temp(&mut temp_file, metadata, payload);
        drop(temp_file);
        let result = write_result.and_then(|()| {
            fs::rename(&temp_path, &metadata.path)
                .map_err(|error| BlobCacheError::io("commit blob cache file", error))
        });
        if let Err(error) = result {
            if let Err(cleanup_error) = remove_file_if_present(&temp_path) {
                return Err(CommitFailure {
                    error: BlobCacheError::Reconciliation(format!(
                        "{error}; remove temporary file: {cleanup_error}"
                    )),
                    orphan_path: Some(temp_path),
                });
            }
            return Err(error.into());
        }
        Ok(())
    }

    pub(crate) fn read(&self, entry: &DiskEntry) -> Result<Vec<u8>, BlobCacheError> {
        let path = &entry.metadata.path;
        let mut file = File::open(path).map_err(|error| {
            BlobCacheError::io(format!("open cache entry {}", self.relative(path)), error)
        })?;
        let file_size = file
            .metadata()
            .map_err(|error| {
                BlobCacheError::io(format!("stat cache entry {}", self.relative(path)), error)
            })?
            .len();
        let (header, blob_id) = self.read_prefix(&mut file, file_size, path)?;
        if blob_id != entry.metadata.blob_id
            || header.checksum != entry.metadata.checksum
            || i64::try_from(file_size).ok() != Some(entry.metadata.size)
        {
            return Err(BlobCacheError::Corrupt(format!(
                "metadata mismatch {}",
                self.relative(path)
            )));
        }
        let payload_size = usize::try_from(header.payload_length).map_err(|_| {
            BlobCacheError::Corrupt(format!("payload too large {}", self.relative(path)))
        })?;
        let mut payload = vec![0_u8; payload_size];
        file.read_exact(&mut payload).map_err(|error| {
            BlobCacheError::Corrupt(format!("read payload {}: {error}", self.relative(path)))
        })?;
        let checksum = crc32c::crc32c(blob_id.as_bytes());
        if crc32c::crc32c_append(checksum, &payload) != header.checksum {
            return Err(BlobCacheError::Corrupt(format!(
                "checksum mismatch {}",
                self.relative(path)
            )));
        }
        Ok(payload)
    }

    pub(crate) fn remove(&self, path: &Path) -> Result<(), BlobCacheError> {
        remove_file_if_present(path).map_err(|error| {
            BlobCacheError::io(format!("remove cache entry {}", self.relative(path)), error)
        })
    }

    pub(crate) fn purge(&self) -> Result<(), BlobCacheError> {
        remove_directory_if_present(&self.temp, "purge blob cache temp directory")?;
        remove_directory_if_present(&self.blobs, "purge blob cache data directory")?;
        self.prepare()
    }

    fn scan_directory(
        &self,
        directory: &Path,
        result: &mut ScanResult,
    ) -> Result<(), BlobCacheError> {
        let entries = fs::read_dir(directory).map_err(|error| {
            BlobCacheError::io(
                format!("scan blob cache directory {}", self.relative(directory)),
                error,
            )
        })?;
        for entry in entries {
            let entry = entry.map_err(|error| BlobCacheError::io("scan blob cache", error))?;
            let file_type = entry
                .file_type()
                .map_err(|error| BlobCacheError::io("inspect blob cache entry", error))?;
            if file_type.is_dir() {
                self.scan_directory(&entry.path(), result)?;
                continue;
            }
            if !file_type.is_file()
                || entry.path().extension().and_then(|value| value.to_str()) != Some("blob")
            {
                continue;
            }
            match self.inspect(&entry.path()) {
                Ok(metadata) => result.entries.push(metadata),
                Err(BlobCacheError::Corrupt(_)) => result.invalid_paths.push(entry.path()),
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    fn inspect(&self, path: &Path) -> Result<FileMetadata, BlobCacheError> {
        let mut file = File::open(path).map_err(|error| {
            BlobCacheError::io(format!("open cache entry {}", self.relative(path)), error)
        })?;
        let file_metadata = file.metadata().map_err(|error| {
            BlobCacheError::io(format!("stat cache entry {}", self.relative(path)), error)
        })?;
        if !file_metadata.file_type().is_file() {
            return Err(BlobCacheError::Corrupt(format!(
                "non-regular file {}",
                self.relative(path)
            )));
        }
        let file_size = file_metadata.len();
        let (header, blob_id) = self.read_prefix(&mut file, file_size, path)?;
        let mut checksum = crc32c::crc32c(blob_id.as_bytes());
        let mut buffer = [0_u8; 64 * 1024];
        let mut remaining = header.payload_length;
        while remaining != 0 {
            let amount = usize::try_from(remaining.min(buffer.len() as u64))
                .expect("read amount is bounded by the buffer length");
            file.read_exact(&mut buffer[..amount]).map_err(|error| {
                BlobCacheError::Corrupt(format!("read payload {}: {error}", self.relative(path)))
            })?;
            checksum = crc32c::crc32c_append(checksum, &buffer[..amount]);
            remaining -= amount as u64;
        }
        if checksum != header.checksum {
            return Err(BlobCacheError::Corrupt(format!(
                "checksum mismatch {}",
                self.relative(path)
            )));
        }

        Ok(FileMetadata {
            blob_id,
            path: path.to_path_buf(),
            size: i64::try_from(file_size).map_err(|_| {
                BlobCacheError::Corrupt(format!("file too large {}", self.relative(path)))
            })?,
            checksum: header.checksum,
            modified: file_metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH),
        })
    }

    fn read_prefix(
        &self,
        file: &mut File,
        file_size: u64,
        path: &Path,
    ) -> Result<(FileHeader, String), BlobCacheError> {
        if file_size < FIXED_HEADER_SIZE as u64 {
            return Err(BlobCacheError::Corrupt(format!(
                "truncated header {}",
                self.relative(path)
            )));
        }
        let mut encoded = [0_u8; FIXED_HEADER_SIZE];
        file.read_exact(&mut encoded).map_err(|error| {
            BlobCacheError::Corrupt(format!("read header {}: {error}", self.relative(path)))
        })?;
        let header = decode_header(&encoded).map_err(|error| {
            BlobCacheError::Corrupt(format!("{error}: {}", self.relative(path)))
        })?;
        let blob_id_size = usize::try_from(header.blob_id_length).map_err(|error| {
            BlobCacheError::Corrupt(format!(
                "read blob ID length {}: {error}",
                self.relative(path)
            ))
        })?;
        if blob_id_size == 0 || blob_id_size > MAX_BLOB_ID_BYTES {
            return Err(BlobCacheError::Corrupt(format!(
                "invalid blob ID length {}",
                self.relative(path)
            )));
        }
        let expected_size = (FIXED_HEADER_SIZE as u64)
            .checked_add(u64::from(header.blob_id_length))
            .and_then(|size| size.checked_add(header.payload_length))
            .ok_or_else(|| {
                BlobCacheError::Corrupt(format!("file length overflow {}", self.relative(path)))
            })?;
        if expected_size != file_size {
            return Err(BlobCacheError::Corrupt(format!(
                "file length mismatch {}",
                self.relative(path)
            )));
        }
        let mut blob_id = vec![0_u8; blob_id_size];
        file.read_exact(&mut blob_id).map_err(|error| {
            BlobCacheError::Corrupt(format!("read blob ID {}: {error}", self.relative(path)))
        })?;
        let blob_id = String::from_utf8(blob_id).map_err(|_| {
            BlobCacheError::Corrupt(format!("invalid blob ID UTF-8 {}", self.relative(path)))
        })?;
        if self.path_for(&blob_id) != path {
            return Err(BlobCacheError::Corrupt(format!(
                "blob ID path mismatch {}",
                self.relative(path)
            )));
        }
        Ok((header, blob_id))
    }

    fn create_temp_file(&self) -> Result<(File, PathBuf), BlobCacheError> {
        for _ in 0..128 {
            let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
            let path = self
                .temp
                .join(format!("blob-{}-{sequence}.tmp", std::process::id()));
            let mut options = OpenOptions::new();
            options.write(true).create_new(true);
            set_private_file_mode(&mut options);
            match options.open(&path) {
                Ok(file) => return Ok((file, path)),
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => {
                    return Err(BlobCacheError::io("create blob cache temp file", error));
                }
            }
        }
        Err(BlobCacheError::Io {
            operation: "create blob cache temp file".to_owned(),
            source: std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                "temporary filename attempts exhausted",
            ),
        })
    }

    fn write_temp(
        &self,
        file: &mut File,
        metadata: &FileMetadata,
        payload: &[u8],
    ) -> Result<(), BlobCacheError> {
        let header = encode_header(FileHeader {
            blob_id_length: u32::try_from(metadata.blob_id.len()).map_err(|_| {
                BlobCacheError::InvalidBlob("blob ID length overflows u32".to_owned())
            })?,
            payload_length: u64::try_from(payload.len()).map_err(|_| {
                BlobCacheError::InvalidBlob("payload length overflows u64".to_owned())
            })?,
            checksum: metadata.checksum,
        });
        file.write_all(&header)
            .map_err(|error| BlobCacheError::io("write blob cache header", error))?;
        file.write_all(metadata.blob_id.as_bytes())
            .map_err(|error| BlobCacheError::io("write blob cache ID", error))?;
        file.write_all(payload)
            .map_err(|error| BlobCacheError::io("write blob cache payload", error))?;
        file.sync_all()
            .map_err(|error| BlobCacheError::io("flush blob cache temp file", error))
    }

    fn relative(&self, path: &Path) -> String {
        path.strip_prefix(&self.root)
            .unwrap_or(path)
            .display()
            .to_string()
    }
}

fn absolute_path(path: &Path) -> Result<PathBuf, BlobCacheError> {
    if path.is_absolute() {
        return Ok(path.to_path_buf());
    }
    std::env::current_dir()
        .map(|current| current.join(path))
        .map_err(|error| BlobCacheError::io("resolve blob cache root", error))
}

fn create_private_directory(path: &Path, operation: &str) -> Result<(), BlobCacheError> {
    fs::create_dir_all(path).map_err(|error| BlobCacheError::io(operation, error))?;
    set_private_directory_mode(path, operation)
}

fn remove_directory_if_present(path: &Path, operation: &str) -> Result<(), BlobCacheError> {
    match fs::remove_dir_all(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(BlobCacheError::io(operation, error)),
    }
}

fn remove_file_if_present(path: &Path) -> std::io::Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

#[cfg(unix)]
fn set_private_directory_mode(path: &Path, operation: &str) -> Result<(), BlobCacheError> {
    use std::os::unix::fs::PermissionsExt;

    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
        .map_err(|error| BlobCacheError::io(operation, error))
}

#[cfg(not(unix))]
fn set_private_directory_mode(_path: &Path, _operation: &str) -> Result<(), BlobCacheError> {
    Ok(())
}

#[cfg(unix)]
fn set_private_file_mode(options: &mut OpenOptions) {
    use std::os::unix::fs::OpenOptionsExt;

    options.mode(0o600);
}

#[cfg(not(unix))]
fn set_private_file_mode(_options: &mut OpenOptions) {}
