// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Bucket lifecycle configuration: prefix filters and day-based expiration actions.

use std::collections::BTreeMap;
use std::sync::Arc;

use http::{HeaderMap, StatusCode};
use hyper::body::Incoming;
use s3mock::{LifecycleConfiguration, LifecycleRule, Store};

use super::bucket::status_response;
use super::request_body::read_small_body;
use crate::s3_error::S3Error;
use crate::server::{S3Response, run_store_operation, xml_response};
use crate::xml::{XmlWriter, parse_document};

const MAX_LIFECYCLE_BODY_BYTES: usize = 256 * 1024;
/// S3 accepts at most 1000 rules per configuration.
const MAX_LIFECYCLE_RULES: usize = 1000;
/// Rule elements s3mock cannot apply; accepting them silently would hide a test gap.
const UNSUPPORTED_RULE_ELEMENTS: [(&str, &str); 6] = [
    ("Date", "date-based expiration"),
    ("StorageClass", "storage class transitions"),
    ("Tag", "tag filters"),
    ("ObjectSizeGreaterThan", "object size filters"),
    ("ObjectSizeLessThan", "object size filters"),
    (
        "NewerNoncurrentVersionsTransition",
        "noncurrent transitions",
    ),
];

pub(crate) async fn put_bucket_lifecycle_configuration(
    store: Arc<Store>,
    bucket: String,
    headers: &HeaderMap,
    body: Incoming,
) -> Result<S3Response, S3Error> {
    let body = read_small_body(body, headers, MAX_LIFECYCLE_BODY_BYTES).await?;
    body.verify_integrity(headers, false)?;
    let document = parse_document(&body.bytes, "LifecycleConfiguration")?;
    let rules = document
        .children_named("Rule")
        .map(|rule| parse_rule(rule.leaf_texts()))
        .collect::<Result<Vec<_>, S3Error>>()?;
    if rules.is_empty() || rules.len() > MAX_LIFECYCLE_RULES {
        return Err(S3Error::malformed_xml(format!(
            "a lifecycle configuration has between 1 and {MAX_LIFECYCLE_RULES} rules"
        )));
    }
    let configuration = LifecycleConfiguration { rules };
    run_store_operation(&store, move |store| {
        store.put_lifecycle_configuration(&bucket, Some(configuration))
    })
    .await?;
    status_response(StatusCode::OK, &[])
}

pub(crate) async fn get_bucket_lifecycle_configuration(
    store: Arc<Store>,
    bucket: String,
) -> Result<S3Response, S3Error> {
    let configuration =
        run_store_operation(&store, move |store| store.lifecycle_configuration(&bucket)).await?;
    let mut document = XmlWriter::new("LifecycleConfiguration");
    for rule in &configuration.rules {
        write_rule(&mut document, rule);
    }
    xml_response(document.finish("LifecycleConfiguration"))
}

pub(crate) async fn delete_bucket_lifecycle(
    store: Arc<Store>,
    bucket: String,
) -> Result<S3Response, S3Error> {
    run_store_operation(&store, move |store| {
        store.put_lifecycle_configuration(&bucket, None)
    })
    .await?;
    status_response(StatusCode::NO_CONTENT, &[])
}

fn parse_rule(fields: BTreeMap<String, String>) -> Result<LifecycleRule, S3Error> {
    if let Some((_, feature)) = UNSUPPORTED_RULE_ELEMENTS
        .iter()
        .find(|(element, _)| fields.contains_key(*element))
    {
        return Err(S3Error::not_implemented(&format!("lifecycle {feature}")));
    }
    let is_enabled = match fields.get("Status").map(String::as_str) {
        Some("Enabled") => true,
        Some("Disabled") => false,
        _ => {
            return Err(S3Error::malformed_xml(
                "every Rule needs Status Enabled or Disabled",
            ));
        }
    };
    let days = |element: &str| -> Result<Option<u32>, S3Error> {
        fields
            .get(element)
            .map(|text| {
                text.trim().parse::<u32>().map_err(|_| {
                    S3Error::malformed_xml(format!("{element} must be a non-negative integer"))
                })
            })
            .transpose()
    };
    let rule = LifecycleRule {
        id: fields.get("ID").cloned(),
        is_enabled,
        prefix: fields.get("Prefix").cloned().unwrap_or_default(),
        expiration_days: days("Days")?,
        expired_object_delete_marker: fields
            .get("ExpiredObjectDeleteMarker")
            .is_some_and(|value| value.trim() == "true"),
        noncurrent_expiration_days: days("NoncurrentDays")?,
        newer_noncurrent_versions: days("NewerNoncurrentVersions")?,
        abort_incomplete_upload_days: days("DaysAfterInitiation")?,
    };
    if rule.expiration_days.is_none()
        && !rule.expired_object_delete_marker
        && rule.noncurrent_expiration_days.is_none()
        && rule.abort_incomplete_upload_days.is_none()
    {
        return Err(S3Error::malformed_xml(
            "every Rule needs at least one action",
        ));
    }
    Ok(rule)
}

fn write_rule(document: &mut XmlWriter, rule: &LifecycleRule) {
    document
        .open("Rule")
        .optional_text_element("ID", rule.id.as_deref())
        .open("Filter")
        .text_element("Prefix", &rule.prefix)
        .close("Filter")
        .text_element(
            "Status",
            if rule.is_enabled {
                "Enabled"
            } else {
                "Disabled"
            },
        );
    if rule.expiration_days.is_some() || rule.expired_object_delete_marker {
        document
            .open("Expiration")
            .optional_text_element(
                "Days",
                rule.expiration_days.map(|days| days.to_string()).as_deref(),
            )
            .optional_text_element(
                "ExpiredObjectDeleteMarker",
                rule.expired_object_delete_marker.then_some("true"),
            )
            .close("Expiration");
    }
    if let Some(days) = rule.noncurrent_expiration_days {
        document
            .open("NoncurrentVersionExpiration")
            .text_element("NoncurrentDays", &days.to_string())
            .optional_text_element(
                "NewerNoncurrentVersions",
                rule.newer_noncurrent_versions
                    .map(|count| count.to_string())
                    .as_deref(),
            )
            .close("NoncurrentVersionExpiration");
    }
    if let Some(days) = rule.abort_incomplete_upload_days {
        document
            .open("AbortIncompleteMultipartUpload")
            .text_element("DaysAfterInitiation", &days.to_string())
            .close("AbortIncompleteMultipartUpload");
    }
    document.close("Rule");
}
