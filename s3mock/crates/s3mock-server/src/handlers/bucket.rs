// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Bucket operations.

use std::sync::Arc;

use http::{HeaderMap, Response, StatusCode};
use http_body_util::{BodyExt, Limited};
use hyper::body::Incoming;
use s3mock::{DefaultRetention, ObjectLockConfiguration, RetentionMode, Store, VersioningState};

use super::OWNER_ID;
use super::request_body::read_small_body;
use crate::request_headers::header_text;
use crate::s3_error::S3Error;
use crate::server::{S3Response, empty_body, run_store_operation, xml_response};
use crate::xml::{XmlElement, XmlWriter, format_iso8601, parse_document};

/// Largest configuration document accepted in a request body.
const MAX_CONFIGURATION_BYTES: usize = 64 * 1024;
/// s3mock reports one region so SDK region redirects never trigger.
const REGION: &str = "us-east-1";

pub(crate) async fn list_buckets(store: Arc<Store>) -> Result<S3Response, S3Error> {
    let buckets = run_store_operation(&store, Store::list_buckets).await?;
    let mut document = XmlWriter::new("ListAllMyBucketsResult");
    document
        .open("Owner")
        .text_element("ID", OWNER_ID)
        .text_element("DisplayName", OWNER_ID)
        .close("Owner")
        .open("Buckets");
    for bucket in &buckets {
        document
            .open("Bucket")
            .text_element("Name", &bucket.name)
            .text_element("CreationDate", &format_iso8601(bucket.created_unix_ms))
            .close("Bucket");
    }
    document.close("Buckets");
    xml_response(document.finish("ListAllMyBucketsResult"))
}

pub(crate) async fn create_bucket(
    store: Arc<Store>,
    bucket: String,
    headers: &HeaderMap,
) -> Result<S3Response, S3Error> {
    let location = format!("/{bucket}");
    let is_object_lock_enabled = header_text(headers, "x-amz-bucket-object-lock-enabled")
        .is_some_and(|value| value.trim().eq_ignore_ascii_case("true"));
    run_store_operation(&store, move |store| {
        store.create_bucket(&bucket, is_object_lock_enabled)
    })
    .await?;
    status_response(StatusCode::OK, &[("location", location.as_str())])
}

pub(crate) async fn head_bucket(store: Arc<Store>, bucket: String) -> Result<S3Response, S3Error> {
    run_store_operation(&store, move |store| store.bucket(&bucket)).await?;
    status_response(StatusCode::OK, &[("x-amz-bucket-region", REGION)])
}

pub(crate) async fn delete_bucket(
    store: Arc<Store>,
    bucket: String,
) -> Result<S3Response, S3Error> {
    run_store_operation(&store, move |store| store.delete_bucket(&bucket)).await?;
    status_response(StatusCode::NO_CONTENT, &[])
}

/// Returns an empty LocationConstraint, which means us-east-1.
pub(crate) async fn get_bucket_location(
    store: Arc<Store>,
    bucket: String,
) -> Result<S3Response, S3Error> {
    run_store_operation(&store, move |store| store.bucket(&bucket)).await?;
    xml_response(XmlWriter::new("LocationConstraint").finish("LocationConstraint"))
}

pub(crate) async fn get_bucket_versioning(
    store: Arc<Store>,
    bucket: String,
) -> Result<S3Response, S3Error> {
    let record = run_store_operation(&store, move |store| store.bucket(&bucket)).await?;
    let mut document = XmlWriter::new("VersioningConfiguration");
    let status = match record.versioning {
        VersioningState::Unversioned => None,
        VersioningState::Enabled => Some("Enabled"),
        VersioningState::Suspended => Some("Suspended"),
    };
    document.optional_text_element("Status", status);
    xml_response(document.finish("VersioningConfiguration"))
}

pub(crate) async fn put_bucket_versioning(
    store: Arc<Store>,
    bucket: String,
    body: Incoming,
) -> Result<S3Response, S3Error> {
    let body = Limited::new(body, MAX_CONFIGURATION_BYTES)
        .collect()
        .await
        .map_err(|error| {
            S3Error::malformed_xml(format!("failed to read the configuration: {error}"))
        })?
        .to_bytes();
    let document = parse_document(&body, "VersioningConfiguration")?;
    let versioning = match document.child_text("Status")?.map(str::trim) {
        Some("Enabled") => VersioningState::Enabled,
        Some("Suspended") => VersioningState::Suspended,
        _ => {
            return Err(S3Error::malformed_xml(
                "Status must be Enabled or Suspended",
            ));
        }
    };
    run_store_operation(&store, move |store| {
        store.put_bucket_versioning(&bucket, versioning)
    })
    .await?;
    status_response(StatusCode::OK, &[])
}

pub(crate) async fn get_object_lock_configuration(
    store: Arc<Store>,
    bucket: String,
) -> Result<S3Response, S3Error> {
    let configuration = run_store_operation(&store, move |store| {
        store.object_lock_configuration(&bucket)
    })
    .await?;
    let mut document = XmlWriter::new("ObjectLockConfiguration");
    document.text_element("ObjectLockEnabled", "Enabled");
    if let Some(retention) = &configuration.default_retention {
        document
            .open("Rule")
            .open("DefaultRetention")
            .text_element("Mode", retention.mode.s3_name())
            .optional_text_element(
                "Days",
                retention.days.map(|days| days.to_string()).as_deref(),
            )
            .optional_text_element(
                "Years",
                retention.years.map(|years| years.to_string()).as_deref(),
            )
            .close("DefaultRetention")
            .close("Rule");
    }
    xml_response(document.finish("ObjectLockConfiguration"))
}

pub(crate) async fn put_object_lock_configuration(
    store: Arc<Store>,
    bucket: String,
    headers: &HeaderMap,
    body: Incoming,
) -> Result<S3Response, S3Error> {
    let body = read_small_body(body, headers, MAX_CONFIGURATION_BYTES).await?;
    body.verify_integrity(headers, false)?;
    let document = parse_document(&body.bytes, "ObjectLockConfiguration")?;
    if document.child_text("ObjectLockEnabled")?.map(str::trim) != Some("Enabled") {
        return Err(S3Error::malformed_xml("ObjectLockEnabled must be Enabled"));
    }
    let default_retention = match document.child("Rule") {
        None => None,
        Some(rule) => Some(parse_default_retention(
            rule.child("DefaultRetention")
                .ok_or_else(|| S3Error::malformed_xml("a Rule needs a DefaultRetention"))?,
        )?),
    };
    run_store_operation(&store, move |store| {
        store.put_object_lock_configuration(&bucket, ObjectLockConfiguration { default_retention })
    })
    .await?;
    status_response(StatusCode::OK, &[])
}

fn parse_default_retention(retention: &XmlElement) -> Result<DefaultRetention, S3Error> {
    let invalid = || {
        S3Error::malformed_xml(
            "DefaultRetention needs a GOVERNANCE or COMPLIANCE Mode and exactly one of Days or Years",
        )
    };
    let mode = retention
        .child_text("Mode")?
        .and_then(|mode| RetentionMode::from_s3_name(mode.trim()))
        .ok_or_else(invalid)?;
    let parse_count = |element: &str| -> Result<Option<u32>, S3Error> {
        retention
            .child_text(element)?
            .map(|text| {
                text.trim()
                    .parse::<u32>()
                    .ok()
                    .filter(|count| *count > 0)
                    .ok_or_else(invalid)
            })
            .transpose()
    };
    let (days, years) = (parse_count("Days")?, parse_count("Years")?);
    if days.is_some() == years.is_some() {
        return Err(invalid());
    }
    Ok(DefaultRetention { mode, days, years })
}

pub(super) fn status_response(
    status: StatusCode,
    headers: &[(&str, &str)],
) -> Result<S3Response, S3Error> {
    let mut builder = Response::builder().status(status);
    for (name, value) in headers {
        builder = builder.header(*name, *value);
    }
    builder
        .body(empty_body())
        .map_err(|error| S3Error::internal(format!("failed to build a response: {error}")))
}
