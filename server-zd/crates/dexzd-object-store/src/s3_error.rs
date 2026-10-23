// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use aws_sdk_s3::error::{ProvideErrorMetadata, SdkError};

use crate::{ObjectStoreError, ObjectStoreErrorKind, ObjectStoreOperation, ObjectStoreResult};

pub(crate) fn required<'a>(
    value: Option<&'a str>,
    operation: ObjectStoreOperation,
    key: &str,
    field: &str,
) -> ObjectStoreResult<&'a str> {
    value.ok_or_else(|| {
        ObjectStoreError::new(
            operation,
            key,
            ObjectStoreErrorKind::Permanent,
            format!("response omitted {field}"),
        )
    })
}

pub(crate) fn is_precondition_error<E: ProvideErrorMetadata>(
    error: &SdkError<E>,
    is_replace_if_etag_matches: bool,
) -> bool {
    let status = error
        .raw_response()
        .map(|response| response.status().as_u16());
    status == Some(412)
        || (is_replace_if_etag_matches
            && status == Some(404)
            && error
                .as_service_error()
                .and_then(ProvideErrorMetadata::code)
                == Some("NoSuchKey"))
}

pub(crate) fn is_conflict_error(error: &ObjectStoreError) -> bool {
    error.kind == ObjectStoreErrorKind::Transient
        && error
            .detail
            .starts_with("HTTP 409 ConditionalRequestConflict:")
}

pub(crate) fn map_sdk_error<E>(
    operation: ObjectStoreOperation,
    key: &str,
    is_write: bool,
    error: SdkError<E>,
) -> ObjectStoreError
where
    E: ProvideErrorMetadata + std::fmt::Display,
{
    let status = error
        .raw_response()
        .map(|response| response.status().as_u16());
    let code = error
        .as_service_error()
        .and_then(ProvideErrorMetadata::code)
        .unwrap_or("");
    if status == Some(409) {
        return ObjectStoreError::new(
            operation,
            key,
            ObjectStoreErrorKind::Transient,
            format!("HTTP 409 ConditionalRequestConflict: {error}"),
        );
    }
    let kind = match &error {
        SdkError::ConstructionFailure(_) => ObjectStoreErrorKind::Permanent,
        SdkError::TimeoutError(_) | SdkError::ResponseError(_) => {
            if is_write {
                ObjectStoreErrorKind::OutcomeUnknown
            } else {
                ObjectStoreErrorKind::Transient
            }
        }
        SdkError::DispatchFailure(dispatch) if dispatch.is_user() => {
            ObjectStoreErrorKind::Permanent
        }
        SdkError::DispatchFailure(dispatch)
            if is_write
                && !dispatch
                    .as_connector_error()
                    .is_some_and(is_connect_failure) =>
        {
            ObjectStoreErrorKind::OutcomeUnknown
        }
        SdkError::DispatchFailure(_) => ObjectStoreErrorKind::Transient,
        SdkError::ServiceError(_)
            if status == Some(429)
                || matches!(code, "SlowDown" | "Throttling" | "ThrottlingException") =>
        {
            ObjectStoreErrorKind::Transient
        }
        SdkError::ServiceError(_) if matches!(status, Some(500..=599)) => {
            if is_write {
                ObjectStoreErrorKind::OutcomeUnknown
            } else {
                ObjectStoreErrorKind::Transient
            }
        }
        SdkError::ServiceError(_) => ObjectStoreErrorKind::Permanent,
        _ => ObjectStoreErrorKind::Permanent,
    };
    ObjectStoreError::new(
        operation,
        key,
        kind,
        format!("{error}; status={status:?}; code={code}"),
    )
}

fn is_connect_failure(dispatch: &aws_sdk_s3::error::ConnectorError) -> bool {
    let mut source: Option<&(dyn std::error::Error + 'static)> = Some(dispatch);
    while let Some(error) = source {
        if let Some(io_error) = error.downcast_ref::<std::io::Error>()
            && matches!(
                io_error.kind(),
                std::io::ErrorKind::ConnectionRefused
                    | std::io::ErrorKind::AddrNotAvailable
                    | std::io::ErrorKind::NotFound
            )
        {
            return true;
        }
        source = error.source();
    }
    false
}
