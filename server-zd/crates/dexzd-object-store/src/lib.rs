// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Object storage interface used by server-zd, with an S3 implementation and a fault-injecting in-memory implementation for tests.
//!
//! Keys passed to [`ObjectStore`] are relative to the store's configured prefix, for example
//! `log/node-a/0000000001/00000000000000000042`.
//!
//! Every server-zd commit depends on conditional writes, so implementations must honor
//! [`WriteCondition`] exactly.

mod file_download;
mod in_memory;
mod instrumented;
mod object_store_error;
mod s3;
mod s3_error;
mod startup_self_check;

use std::path::Path;

pub use in_memory::{FaultAction, FaultInjector, InMemoryObjectStore};
pub use instrumented::InstrumentedObjectStore;
pub use object_store_error::{ObjectStoreError, ObjectStoreErrorKind, ObjectStoreOperation};
pub use s3::{S3Credentials, S3ObjectStore, S3ObjectStoreConfig};
pub use startup_self_check::{StartupSelfCheckError, StartupSelfCheckFailure, startup_self_check};

use async_trait::async_trait;
use bytes::Bytes;

/// Result of an object storage call.
pub type ObjectStoreResult<T> = Result<T, ObjectStoreError>;

/// Precondition of a write.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteCondition {
    /// Write regardless of the current object.
    Unconditional,
    /// Create only when no object exists at the key (`If-None-Match: *`).
    CreateOnly,
    /// Replace only when the current object's ETag equals `etag` (`If-Match`).
    ReplaceIfEtagMatches {
        /// ETag returned by an earlier read or write of the object.
        etag: String,
    },
}

/// Outcome of a conditional write that reached the store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteOutcome {
    /// The object was written.
    Written {
        /// ETag of the new object.
        etag: String,
    },
    /// The precondition failed (HTTP 412, or S3 404 `NoSuchKey` for `If-Match`); nothing was written.
    PreconditionFailed,
}

/// Inclusive byte range of an object.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ByteRange {
    /// First byte offset.
    pub start: u64,
    /// Last byte offset, inclusive. Reads past the end return the available bytes.
    pub end_inclusive: u64,
}

/// Content and metadata of an object read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectContent {
    /// The requested bytes: the whole object, or the requested range.
    pub bytes: Bytes,
    /// ETag of the object version that was read.
    pub etag: String,
    /// Size of the whole object in bytes.
    pub object_size: u64,
}

/// Metadata of an object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectMetadata {
    /// Key relative to the store prefix.
    pub key: String,
    /// Size in bytes.
    pub size: u64,
    /// ETag of the current version.
    pub etag: String,
    /// Last write time in microseconds since the Unix epoch, when available.
    pub last_modified_unix_micros: Option<i64>,
}

/// One page of a listing.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ObjectListing {
    /// Objects in ascending key order.
    pub objects: Vec<ObjectMetadata>,
    /// Pass as `start_after` to read the next page; `None` when the listing is complete.
    pub next_start_after: Option<String>,
}

/// Object storage operations used by server-zd.
///
/// Implementations are shared across tasks (`Arc<dyn ObjectStore>`).
///
/// Large bodies may use multipart internally. The [`WriteCondition`] then applies to
/// `CompleteMultipartUpload`, so the result matches a single PUT.
///
/// HTTP 409 `ConditionalRequestConflict` means a concurrent operation, not a false condition.
/// Implementations retry it (restarting a multipart upload) and report
/// [`ObjectStoreErrorKind::Transient`] if it persists.
#[async_trait]
pub trait ObjectStore: Send + Sync + 'static {
    /// Writes `body` at `key` if `condition` holds.
    ///
    /// Returns [`WriteOutcome::PreconditionFailed`] when the store rejected the condition. An
    /// error of kind [`ObjectStoreErrorKind::OutcomeUnknown`] means the write may or may not
    /// have happened; callers must read the key to find out.
    /// A missing key fails [`WriteCondition::ReplaceIfEtagMatches`] with `PreconditionFailed`.
    async fn put(
        &self,
        key: &str,
        body: Bytes,
        condition: WriteCondition,
    ) -> ObjectStoreResult<WriteOutcome>;

    /// Streams the file at `path` to `key` if `condition` holds; same outcomes as [`Self::put`].
    async fn put_file(
        &self,
        key: &str,
        path: &Path,
        condition: WriteCondition,
    ) -> ObjectStoreResult<WriteOutcome>;

    /// Reads the whole object, or returns `None` when it does not exist.
    async fn get(&self, key: &str) -> ObjectStoreResult<Option<ObjectContent>>;

    /// Reads a byte range of the object, or returns `None` when it does not exist.
    async fn get_range(
        &self,
        key: &str,
        range: ByteRange,
    ) -> ObjectStoreResult<Option<ObjectContent>>;

    /// Streams the object into a new file at `path` and returns its size, or `None` when the
    /// object does not exist (no file is created then).
    ///
    /// Implementations write to a temporary file and rename it on success. On error no file
    /// remains at `path`.
    async fn get_to_file(&self, key: &str, path: &Path) -> ObjectStoreResult<Option<u64>>;

    /// Returns the object's metadata, or `None` when it does not exist.
    async fn head(&self, key: &str) -> ObjectStoreResult<Option<ObjectMetadata>>;

    /// Deletes the object. Deleting a missing object succeeds.
    async fn delete(&self, key: &str) -> ObjectStoreResult<()>;

    /// Deletes up to 1000 objects. Missing objects count as deleted.
    async fn delete_many(&self, keys: &[String]) -> ObjectStoreResult<()>;

    /// Lists objects whose keys start with `prefix` and sort after `start_after`, at most
    /// `max_keys` per page (1-1000).
    async fn list(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        max_keys: u32,
    ) -> ObjectStoreResult<ObjectListing>;
}
