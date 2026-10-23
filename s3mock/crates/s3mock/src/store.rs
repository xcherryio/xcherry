// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! The object store: redb metadata plus one self-describing data file per object version.

mod bucket;
mod lifecycle;
mod maintenance;
mod multipart;
mod multipart_complete;
mod object_delete;
mod object_listing;
mod object_read;
mod object_write;

use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Mutex;
use std::sync::atomic::AtomicI64;

use redb::{Database, ReadableTable, Table, TableDefinition, WriteTransaction};
use serde::Serialize;
use serde::de::DeserializeOwned;
use ulid::Ulid;

use crate::error::StoreError;
use crate::key_codec::{encode_object_key, prefix_successor};
use crate::record::{BucketRecord, VersionRecord};

pub use lifecycle::LifecycleRun;
pub use maintenance::OrphanCollection;
pub use multipart::{
    CompletedPart, CreateMultipartUploadRequest, ListMultipartUploadsRequest, ListPartsRequest,
    MAX_PART_NUMBER, MultipartUploadsPage, PartsPage,
};
pub use multipart_complete::CompleteMultipartUploadRequest;
pub use object_delete::{DeleteObjectOutcome, DeleteObjectRequest};
pub use object_listing::MAX_KEYS_LIMIT;
pub use object_listing::{
    ListObjectVersionsPage, ListObjectVersionsRequest, ListObjectsPage, ListObjectsRequest,
    ListedObject, ListedVersion,
};
pub use object_read::{
    ByteRangeRequest, ObjectBody, ObjectReadRequest, ObjectSelection, ReadConditions, ResolvedRange,
};
pub use object_write::{PutObjectOutcome, PutObjectRequest, WriteCondition};

const BUCKETS: TableDefinition<&str, &[u8]> = TableDefinition::new("buckets");
/// Every version and delete marker, keyed by `encode_version_key`, newest first per key.
const VERSIONS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("versions");
/// The current version of every key whose newest version is not a delete marker.
const LATEST: TableDefinition<&[u8], &[u8]> = TableDefinition::new("latest");
/// In-progress multipart uploads, keyed by `encode_upload_key`.
const UPLOADS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("uploads");
/// Uploaded parts, keyed by `encode_part_key`.
const PARTS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("parts");
/// Bucket subdirectory holding one directory of part files per upload.
const UPLOADS_DIRECTORY_NAME: &str = "uploads";

const METADATA_FILE_NAME: &str = "metadata.redb";
const DATA_DIRECTORY_NAME: &str = "data";
const TEMPORARY_DIRECTORY_NAME: &str = "tmp";
const NULL_VERSION_ID: &str = "null";

/// How much the store waits for stable storage before acknowledging a write.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Durability {
    /// Every acknowledged write survives a crash: data files and metadata are flushed with fsync.
    #[default]
    Strict,
    /// Writes skip fsync; a crash may lose acknowledged writes, so crash tests are invalid.
    Relaxed,
}

/// Settings fixed when a store is opened.
#[derive(Clone, Debug)]
pub struct StoreConfig {
    /// Directory holding `metadata.redb`, `data/`, and `tmp/`; created when missing.
    pub root_directory: PathBuf,
    pub durability: Durability,
}

/// A local-disk object store with S3 semantics. Share it across threads with `Arc`.
///
/// Every method blocks on disk I/O; async callers should run them on a blocking thread.
pub struct Store {
    data_directory: PathBuf,
    temporary_directory: PathBuf,
    database: Database,
    durability: Durability,
    /// Monotonic so sequential writes to one key always order by request arrival.
    version_generator: Mutex<ulid::Generator>,
    /// Every later ULID is at least this old because the generator is monotonic, even if the clock steps back.
    opened_at_unix_ms: u64,
    /// Added to wall-clock time for lifecycle decisions only.
    lifecycle_clock_offset_ms: AtomicI64,
}

impl Store {
    /// Opens or creates a store, removing temporary files left by an earlier process.
    pub fn open(config: &StoreConfig) -> Result<Store, StoreError> {
        let data_directory = config.root_directory.join(DATA_DIRECTORY_NAME);
        let temporary_directory = config.root_directory.join(TEMPORARY_DIRECTORY_NAME);
        let is_durable = config.durability == Durability::Strict;
        create_directory_durably(&config.root_directory, is_durable)?;
        create_directory_durably(&data_directory, is_durable)?;
        create_directory_durably(&temporary_directory, is_durable)?;
        let database = Database::create(config.root_directory.join(METADATA_FILE_NAME))?;
        // redb flushes file contents, not the directory entry of a newly created database file.
        if is_durable {
            crate::data_file::sync_directory(&config.root_directory)?;
        }
        let mut version_generator = ulid::Generator::new();
        let opened_at = version_generator
            .generate()
            .expect("a fresh ULID generator cannot overflow");
        let store = Store {
            data_directory,
            temporary_directory,
            database,
            durability: config.durability,
            version_generator: Mutex::new(version_generator),
            opened_at_unix_ms: opened_at.timestamp_ms(),
            lifecycle_clock_offset_ms: AtomicI64::new(0),
        };
        store.create_tables()?;
        store.remove_temporary_files()?;
        Ok(store)
    }

    fn create_tables(&self) -> Result<(), StoreError> {
        let transaction = self.begin_write()?;
        transaction.open_table(BUCKETS)?;
        transaction.open_table(VERSIONS)?;
        transaction.open_table(LATEST)?;
        transaction.open_table(UPLOADS)?;
        transaction.open_table(PARTS)?;
        transaction.commit()?;
        Ok(())
    }

    fn begin_write(&self) -> Result<WriteTransaction, StoreError> {
        let mut transaction = self.database.begin_write()?;
        transaction.set_durability(match self.durability {
            Durability::Strict => redb::Durability::Immediate,
            Durability::Relaxed => redb::Durability::None,
        })?;
        Ok(transaction)
    }

    fn is_durable(&self) -> bool {
        self.durability == Durability::Strict
    }

    fn next_internal_version(&self) -> Ulid {
        self.version_generator
            .lock()
            .expect("the version generator lock is only held for infallible work")
            .generate()
            .expect("2^80 ULIDs within one millisecond cannot be exhausted")
    }

    fn bucket_directory(&self, bucket: &BucketRecord) -> PathBuf {
        self.data_directory.join(&bucket.incarnation)
    }

    fn upload_directory(&self, bucket: &BucketRecord, upload_id: &str) -> PathBuf {
        self.bucket_directory(bucket)
            .join(UPLOADS_DIRECTORY_NAME)
            .join(upload_id)
    }

    fn data_file_path(&self, bucket: &BucketRecord, record: &VersionRecord) -> Option<PathBuf> {
        record
            .data_file
            .as_ref()
            .map(|relative| self.bucket_directory(bucket).join(relative))
    }
}

impl Drop for Store {
    /// Relaxed commits reach disk only after a durable commit, so flush one on close.
    fn drop(&mut self) {
        if self.durability == Durability::Strict {
            return;
        }
        let flushed = self
            .database
            .begin_write()
            .map_err(StoreError::from)
            .and_then(|transaction| transaction.commit().map_err(StoreError::from));
        if let Err(error) = flushed {
            tracing::warn!(%error, "failed to flush relaxed metadata commits on close");
        }
    }
}

/// Relative data file path for a version; the random suffix spreads files across shard directories.
fn relative_data_file(internal_version: Ulid) -> String {
    let name = internal_version.to_string();
    let shard = &name[name.len() - 2..];
    format!("{shard}/{name}")
}

/// Parses a client-supplied version ID: `None` means the null version.
fn parse_version_id(version_id: &str) -> Result<Option<Ulid>, StoreError> {
    if version_id == NULL_VERSION_ID {
        return Ok(None);
    }
    Ulid::from_str(version_id)
        .map(Some)
        .map_err(|_| StoreError::InvalidArgument(format!("invalid version id {version_id:?}")))
}

fn validate_object_key(key: &str) -> Result<(), StoreError> {
    if key.is_empty() {
        return Err(StoreError::InvalidObjectKey("the key is empty".to_owned()));
    }
    if key.len() > 1024 {
        return Err(StoreError::InvalidObjectKey(
            "the key is longer than 1024 bytes".to_owned(),
        ));
    }
    Ok(())
}

fn encode_json<T: Serialize>(value: &T) -> Result<Vec<u8>, StoreError> {
    Ok(serde_json::to_vec(value)?)
}

fn decode_json<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, StoreError> {
    Ok(serde_json::from_slice(bytes)?)
}

fn require_bucket(
    table: &impl ReadableTable<&'static str, &'static [u8]>,
    bucket: &str,
) -> Result<BucketRecord, StoreError> {
    match table.get(bucket)? {
        Some(value) => decode_json(value.value()),
        None => Err(StoreError::NoSuchBucket(bucket.to_owned())),
    }
}

/// One stored version together with its `versions` table key.
struct StoredVersion {
    version_key: Vec<u8>,
    record: VersionRecord,
}

/// Loads every version of one key, newest first.
fn load_versions(
    versions: &impl ReadableTable<&'static [u8], &'static [u8]>,
    bucket: &str,
    key: &str,
) -> Result<Vec<StoredVersion>, StoreError> {
    let object_key = encode_object_key(bucket, key);
    let end = prefix_successor(&object_key)
        .expect("encoded object keys end with a terminator below 0xFF");
    let mut loaded = Vec::new();
    for entry in versions.range(object_key.as_slice()..end.as_slice())? {
        let (version_key, value) = entry?;
        loaded.push(StoredVersion {
            version_key: version_key.value().to_vec(),
            record: decode_json(value.value())?,
        });
    }
    Ok(loaded)
}

/// Finds the version a client addressed by version ID; `None` selects the null version.
fn find_version(
    versions: Vec<StoredVersion>,
    internal_version: Option<Ulid>,
) -> Option<StoredVersion> {
    versions.into_iter().find(|stored| match internal_version {
        None => stored.record.is_null_version,
        Some(ulid) => {
            !stored.record.is_null_version && stored.record.internal_version == ulid.to_string()
        }
    })
}

/// Rewrites the `latest` entry of one key from its newest remaining version.
fn refresh_latest(
    latest: &mut Table<&'static [u8], &'static [u8]>,
    versions: &Table<&'static [u8], &'static [u8]>,
    bucket: &str,
    key: &str,
) -> Result<(), StoreError> {
    let object_key = encode_object_key(bucket, key);
    let newest = load_versions(versions, bucket, key)?.into_iter().next();
    match newest {
        Some(stored) if !stored.record.is_delete_marker => {
            latest.insert(
                object_key.as_slice(),
                encode_json(&stored.record)?.as_slice(),
            )?;
        }
        Some(_) | None => {
            latest.remove(object_key.as_slice())?;
        }
    }
    Ok(())
}

/// Removes data files whose metadata a committed transaction already dropped.
fn remove_superseded_files(paths: &[PathBuf]) {
    for path in paths {
        if let Err(error) = fs::remove_file(path) {
            tracing::warn!(path = %path.display(), %error, "failed to remove superseded data file");
        }
    }
}

/// Creates `directory` and any missing ancestors, flushing each new entry into its parent when durable.
fn create_directory_durably(directory: &Path, is_durable: bool) -> Result<(), StoreError> {
    let mut missing = Vec::new();
    let mut current = directory;
    while !current.is_dir() {
        missing.push(current);
        match current.parent() {
            Some(parent) if !parent.as_os_str().is_empty() => current = parent,
            Some(_) | None => break,
        }
    }
    if missing.is_empty() {
        return Ok(());
    }
    fs::create_dir_all(directory)?;
    if is_durable {
        for created in missing.iter().rev() {
            crate::data_file::sync_directory(&parent_directory(created))?;
        }
    }
    Ok(())
}

/// Returns the directory that holds `path`'s entry; a bare relative name lives in `.`.
fn parent_directory(path: &Path) -> PathBuf {
    match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent.to_path_buf(),
        Some(_) | None => PathBuf::from("."),
    }
}
