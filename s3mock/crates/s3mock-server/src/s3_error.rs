// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! S3 error responses: HTTP status, error code, and the headers S3 adds to some errors.

use std::time::{Duration, UNIX_EPOCH};

use http::StatusCode;
use s3mock::StoreError;

/// An error rendered as an S3 `<Error>` document, or as a bare status for HEAD requests.
#[derive(Debug)]
pub(crate) struct S3Error {
    pub(crate) status: StatusCode,
    pub(crate) code: &'static str,
    pub(crate) message: String,
    /// Extra response headers, such as `x-amz-delete-marker` on a 404.
    pub(crate) headers: Vec<(&'static str, String)>,
}

impl S3Error {
    pub(crate) fn new(
        status: StatusCode,
        code: &'static str,
        message: impl Into<String>,
    ) -> S3Error {
        S3Error {
            status,
            code,
            message: message.into(),
            headers: Vec::new(),
        }
    }

    pub(crate) fn not_implemented(operation: &str) -> S3Error {
        S3Error::new(
            StatusCode::NOT_IMPLEMENTED,
            "NotImplemented",
            format!("s3mock does not implement {operation}"),
        )
    }

    pub(crate) fn invalid_argument(message: impl Into<String>) -> S3Error {
        S3Error::new(StatusCode::BAD_REQUEST, "InvalidArgument", message)
    }

    pub(crate) fn malformed_xml(message: impl Into<String>) -> S3Error {
        S3Error::new(StatusCode::BAD_REQUEST, "MalformedXML", message)
    }

    pub(crate) fn incomplete_body(message: impl Into<String>) -> S3Error {
        S3Error::new(StatusCode::BAD_REQUEST, "IncompleteBody", message)
    }

    pub(crate) fn internal(message: impl Into<String>) -> S3Error {
        S3Error::new(StatusCode::INTERNAL_SERVER_ERROR, "InternalError", message)
    }

    fn with_header(mut self, name: &'static str, value: impl Into<String>) -> S3Error {
        self.headers.push((name, value.into()));
        self
    }
}

impl From<StoreError> for S3Error {
    fn from(error: StoreError) -> S3Error {
        let message = error.to_string();
        match error {
            StoreError::NoSuchBucket(_) => {
                S3Error::new(StatusCode::NOT_FOUND, "NoSuchBucket", message)
            }
            StoreError::BucketAlreadyOwnedByYou(_) => {
                S3Error::new(StatusCode::CONFLICT, "BucketAlreadyOwnedByYou", message)
            }
            StoreError::BucketNotEmpty(_) => {
                S3Error::new(StatusCode::CONFLICT, "BucketNotEmpty", message)
            }
            StoreError::InvalidBucketName(_) => {
                S3Error::new(StatusCode::BAD_REQUEST, "InvalidBucketName", message)
            }
            StoreError::InvalidObjectKey(_) => {
                S3Error::new(StatusCode::BAD_REQUEST, "KeyTooLongError", message)
            }
            StoreError::NoSuchKey { .. } => {
                S3Error::new(StatusCode::NOT_FOUND, "NoSuchKey", message)
            }
            StoreError::NoSuchVersion { .. } => {
                S3Error::new(StatusCode::NOT_FOUND, "NoSuchVersion", message)
            }
            StoreError::DeleteMarker { version_id, .. } => {
                S3Error::new(StatusCode::NOT_FOUND, "NoSuchKey", message)
                    .with_header("x-amz-delete-marker", "true")
                    .with_header("x-amz-version-id", version_id)
            }
            StoreError::MethodNotAllowedOnDeleteMarker {
                version_id,
                last_modified_unix_ms,
            } => S3Error::new(StatusCode::METHOD_NOT_ALLOWED, "MethodNotAllowed", message)
                .with_header("x-amz-delete-marker", "true")
                .with_header("x-amz-version-id", version_id)
                .with_header("last-modified", format_http_date(last_modified_unix_ms)),
            StoreError::PreconditionFailed => S3Error::new(
                StatusCode::PRECONDITION_FAILED,
                "PreconditionFailed",
                message,
            ),
            StoreError::NotModified => {
                S3Error::new(StatusCode::NOT_MODIFIED, "NotModified", message)
            }
            StoreError::InvalidRange { object_size } => {
                S3Error::new(StatusCode::RANGE_NOT_SATISFIABLE, "InvalidRange", message)
                    .with_header("content-range", format!("bytes */{object_size}"))
            }
            StoreError::BadDigest { .. } => {
                S3Error::new(StatusCode::BAD_REQUEST, "BadDigest", message)
            }
            StoreError::InvalidDigest => {
                S3Error::new(StatusCode::BAD_REQUEST, "InvalidDigest", message)
            }
            StoreError::InvalidRequest(_) => {
                S3Error::new(StatusCode::BAD_REQUEST, "InvalidRequest", message)
            }
            StoreError::InvalidArgument(_) => S3Error::invalid_argument(message),
            StoreError::InvalidBucketState(_) => {
                S3Error::new(StatusCode::CONFLICT, "InvalidBucketState", message)
            }
            StoreError::ObjectLockConfigurationNotFound(_) => S3Error::new(
                StatusCode::NOT_FOUND,
                "ObjectLockConfigurationNotFoundError",
                message,
            ),
            StoreError::NoSuchLifecycleConfiguration(_) => S3Error::new(
                StatusCode::NOT_FOUND,
                "NoSuchLifecycleConfiguration",
                message,
            ),
            StoreError::NoSuchUpload { .. } => {
                S3Error::new(StatusCode::NOT_FOUND, "NoSuchUpload", message)
            }
            StoreError::InvalidPart(_) => {
                S3Error::new(StatusCode::BAD_REQUEST, "InvalidPart", message)
            }
            StoreError::InvalidPartOrder => {
                S3Error::new(StatusCode::BAD_REQUEST, "InvalidPartOrder", message)
            }
            StoreError::EntityTooSmall { .. } => {
                S3Error::new(StatusCode::BAD_REQUEST, "EntityTooSmall", message)
            }
            StoreError::CorruptDataFile { .. }
            | StoreError::Metadata(_)
            | StoreError::CorruptMetadata(_)
            | StoreError::Io(_) => {
                tracing::error!(%error, "store operation failed");
                S3Error::internal("we encountered an internal error, please try again")
            }
        }
    }
}

/// Formats Unix milliseconds as an HTTP date, such as `Last-Modified`.
pub(crate) fn format_http_date(unix_ms: i64) -> String {
    let since_epoch =
        Duration::from_millis(u64::try_from(unix_ms).expect("stored timestamps are after 1970"));
    httpdate::fmt_http_date(UNIX_EPOCH + since_epoch)
}
