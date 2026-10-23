// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::fmt;

/// Object storage operation that failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectStoreOperation {
    /// [`crate::ObjectStore::put`] or [`crate::ObjectStore::put_file`].
    Put,
    /// [`crate::ObjectStore::get`], [`crate::ObjectStore::get_range`], or
    /// [`crate::ObjectStore::get_to_file`].
    Get,
    /// [`crate::ObjectStore::head`].
    Head,
    /// [`crate::ObjectStore::delete`] or [`crate::ObjectStore::delete_many`].
    Delete,
    /// [`crate::ObjectStore::list`].
    List,
}

impl fmt::Display for ObjectStoreOperation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Put => "put",
            Self::Get => "get",
            Self::Head => "head",
            Self::Delete => "delete",
            Self::List => "list",
        })
    }
}

/// How a failed call may be handled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectStoreErrorKind {
    /// A write may or may not have taken effect (timeout or connection loss after the request
    /// was sent, or a server error on a write). Read the key to learn the outcome.
    OutcomeUnknown,
    /// The call failed without effect and may succeed if retried (throttling, unavailable,
    /// connection refused, server error on a read).
    Transient,
    /// Retrying cannot help (access denied, missing bucket, invalid request).
    Permanent,
    /// The stored content failed an integrity check.
    Corrupt,
}

impl fmt::Display for ObjectStoreErrorKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::OutcomeUnknown => "outcome unknown",
            Self::Transient => "transient failure",
            Self::Permanent => "permanent failure",
            Self::Corrupt => "corrupt content",
        })
    }
}

/// Error from an object storage call, naming the operation and key.
#[derive(Debug, thiserror::Error)]
#[error("object storage {operation} of {key:?} failed ({kind}): {detail}")]
pub struct ObjectStoreError {
    /// The failed operation.
    pub operation: ObjectStoreOperation,
    /// Key relative to the store prefix (empty for batch deletes and listings without a key).
    pub key: String,
    /// How the failure may be handled.
    pub kind: ObjectStoreErrorKind,
    /// Human-readable detail from the store or client.
    pub detail: String,
}

impl ObjectStoreError {
    /// Creates an error.
    pub fn new(
        operation: ObjectStoreOperation,
        key: impl Into<String>,
        kind: ObjectStoreErrorKind,
        detail: impl Into<String>,
    ) -> Self {
        Self {
            operation,
            key: key.into(),
            kind,
            detail: detail.into(),
        }
    }

    /// Returns true when retrying the same call may succeed.
    pub fn is_retryable(&self) -> bool {
        matches!(
            self.kind,
            ObjectStoreErrorKind::OutcomeUnknown | ObjectStoreErrorKind::Transient
        )
    }
}
