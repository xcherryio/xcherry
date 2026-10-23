// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::io;

/// Error returned by [`crate::Store`] operations, mapped by the server to S3 error codes.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("bucket {0:?} does not exist")]
    NoSuchBucket(String),
    #[error("bucket {0:?} already exists")]
    BucketAlreadyOwnedByYou(String),
    #[error("bucket {0:?} is not empty")]
    BucketNotEmpty(String),
    #[error("bucket name {0:?} is invalid")]
    InvalidBucketName(String),
    #[error("object key is invalid: {0}")]
    InvalidObjectKey(String),
    #[error("object {key:?} does not exist in bucket {bucket:?}")]
    NoSuchKey { bucket: String, key: String },
    #[error("version {version_id:?} does not exist")]
    NoSuchVersion { version_id: String },
    #[error("the latest version of {key:?} is a delete marker")]
    DeleteMarker { key: String, version_id: String },
    #[error("the requested version {version_id:?} is a delete marker")]
    MethodNotAllowedOnDeleteMarker {
        version_id: String,
        last_modified_unix_ms: i64,
    },
    #[error("at least one of the preconditions did not hold")]
    PreconditionFailed,
    #[error("the object was not modified")]
    NotModified,
    #[error("the requested range is not satisfiable for an object of {object_size} bytes")]
    InvalidRange { object_size: u64 },
    #[error("the {algorithm} checksum does not match the uploaded content")]
    BadDigest { algorithm: &'static str },
    #[error("the request is invalid: {0}")]
    InvalidRequest(String),
    #[error("an argument is invalid: {0}")]
    InvalidArgument(String),
    #[error("the Content-MD5 header is not valid base64-encoded MD5")]
    InvalidDigest,
    #[error("data file {path:?} is corrupt: {reason}")]
    CorruptDataFile { path: String, reason: String },
    #[error("metadata store error: {0}")]
    Metadata(#[from] redb::Error),
    #[error("metadata record is corrupt: {0}")]
    CorruptMetadata(#[from] serde_json::Error),
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
}

macro_rules! impl_from_redb_error {
    ($($error_type:ty),+) => {
        $(impl From<$error_type> for StoreError {
            fn from(error: $error_type) -> StoreError {
                StoreError::Metadata(error.into())
            }
        })+
    };
}

impl_from_redb_error!(
    redb::DatabaseError,
    redb::TransactionError,
    redb::TableError,
    redb::StorageError,
    redb::CommitError,
    redb::SetDurabilityError
);
