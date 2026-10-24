// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use crate::{ObjectStore, ObjectStoreError, WriteCondition, WriteOutcome};
use bytes::Bytes;

/// The conditional-write guarantee that failed during startup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartupSelfCheckFailure {
    /// The initial create did not succeed.
    InitialCreate,
    /// A duplicate create was accepted or failed unexpectedly.
    DuplicateCreate,
    /// A replacement with an incorrect ETag was accepted or failed unexpectedly.
    WrongEtagReplacement,
    /// A replacement with the current ETag was rejected or failed unexpectedly.
    CorrectEtagReplacement,
    /// The probe object could not be removed.
    Cleanup,
}

/// Failure of an object-store startup probe.
#[derive(Debug, thiserror::Error)]
#[error("object-store startup check {check:?} failed: {detail}")]
pub struct StartupSelfCheckError {
    /// The failed check.
    pub check: StartupSelfCheckFailure,
    /// Description of the observed result.
    pub detail: String,
    /// Store error, if the operation failed.
    #[source]
    pub source: Option<ObjectStoreError>,
}

/// Verifies create-only and ETag replacement semantics, then removes its probe object.
pub async fn startup_self_check(store: &dyn ObjectStore) -> Result<(), StartupSelfCheckError> {
    let key = format!(
        "probe/{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock is after the Unix epoch")
            .as_nanos()
    );
    let first = store
        .put(
            &key,
            Bytes::from_static(b"probe"),
            WriteCondition::CreateOnly,
        )
        .await;
    let initial_etag = match first {
        Ok(WriteOutcome::Written { etag }) => etag,
        other => {
            let mut observed = failure(StartupSelfCheckFailure::InitialCreate, other);
            if let Err(cleanup_error) = store.delete(&key).await {
                observed
                    .detail
                    .push_str(&format!("; cleanup failed: {cleanup_error}"));
            }
            return Err(observed);
        }
    };
    let duplicate = store
        .put(
            &key,
            Bytes::from_static(b"duplicate"),
            WriteCondition::CreateOnly,
        )
        .await;
    if !matches!(duplicate, Ok(WriteOutcome::PreconditionFailed)) {
        let mut observed = failure(StartupSelfCheckFailure::DuplicateCreate, duplicate);
        if let Err(cleanup_error) = store.delete(&key).await {
            observed
                .detail
                .push_str(&format!("; cleanup failed: {cleanup_error}"));
        }
        return Err(observed);
    }
    let replacement = store
        .put(
            &key,
            Bytes::from_static(b"replacement"),
            WriteCondition::ReplaceIfEtagMatches {
                etag: "\"wrong-etag\"".to_owned(),
            },
        )
        .await;
    if !matches!(replacement, Ok(WriteOutcome::PreconditionFailed)) {
        let mut observed = failure(StartupSelfCheckFailure::WrongEtagReplacement, replacement);
        if let Err(cleanup_error) = store.delete(&key).await {
            observed
                .detail
                .push_str(&format!("; cleanup failed: {cleanup_error}"));
        }
        return Err(observed);
    }
    let correct_replacement = store
        .put(
            &key,
            Bytes::from_static(b"replacement"),
            WriteCondition::ReplaceIfEtagMatches { etag: initial_etag },
        )
        .await;
    if !matches!(correct_replacement, Ok(WriteOutcome::Written { .. })) {
        let mut observed = failure(
            StartupSelfCheckFailure::CorrectEtagReplacement,
            correct_replacement,
        );
        if let Err(cleanup_error) = store.delete(&key).await {
            observed
                .detail
                .push_str(&format!("; cleanup failed: {cleanup_error}"));
        }
        return Err(observed);
    }
    store
        .delete(&key)
        .await
        .map_err(|source| StartupSelfCheckError {
            check: StartupSelfCheckFailure::Cleanup,
            detail: "deleting probe failed".to_owned(),
            source: Some(source),
        })
}

fn failure(
    check: StartupSelfCheckFailure,
    result: Result<WriteOutcome, ObjectStoreError>,
) -> StartupSelfCheckError {
    match result {
        Ok(outcome) => StartupSelfCheckError {
            check,
            detail: format!("unexpected outcome: {outcome:?}"),
            source: None,
        },
        Err(source) => StartupSelfCheckError {
            check,
            detail: "storage operation failed".to_owned(),
            source: Some(source),
        },
    }
}
