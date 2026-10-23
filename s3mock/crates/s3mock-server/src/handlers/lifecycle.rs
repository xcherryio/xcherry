// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Bucket lifecycle configuration: prefix filters and day-based expiration actions.

use std::sync::Arc;

use crate::server::RequestBody;
use http::{HeaderMap, StatusCode};
use s3mock::{LifecycleConfiguration, LifecycleRule, Store};

use super::bucket::status_response;
use super::request_body::read_small_body;
use crate::s3_error::S3Error;
use crate::server::{S3Response, run_store_operation, xml_response};
use crate::xml::{XmlElement, XmlWriter, parse_document};

const MAX_LIFECYCLE_BODY_BYTES: usize = 256 * 1024;
/// S3 accepts at most 1000 rules per configuration.
const MAX_LIFECYCLE_RULES: usize = 1000;
/// AWS limits NewerNoncurrentVersions to this range.
const MAX_NEWER_NONCURRENT_VERSIONS: u32 = 100;

pub(crate) async fn put_bucket_lifecycle_configuration(
    store: Arc<Store>,
    bucket: String,
    headers: &HeaderMap,
    body: RequestBody,
) -> Result<S3Response, S3Error> {
    let body = read_small_body(body, headers, MAX_LIFECYCLE_BODY_BYTES).await?;
    body.verify_integrity(headers, false)?;
    let document = parse_document(&body.bytes, "LifecycleConfiguration")?;
    let rules = document
        .children_named("Rule")
        .map(parse_rule)
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

fn parse_rule(rule: &XmlElement) -> Result<LifecycleRule, S3Error> {
    for child in &rule.children {
        match child.name.as_str() {
            "Transition" | "NoncurrentVersionTransition" => {
                return Err(S3Error::not_implemented(
                    "lifecycle storage class transitions",
                ));
            }
            "ID"
            | "Status"
            | "Filter"
            | "Prefix"
            | "Expiration"
            | "NoncurrentVersionExpiration"
            | "AbortIncompleteMultipartUpload" => {
                if rule.children_named(&child.name).count() > 1 {
                    return Err(S3Error::malformed_xml(format!(
                        "a Rule has more than one {}",
                        child.name
                    )));
                }
            }
            other => {
                return Err(S3Error::malformed_xml(format!(
                    "unexpected Rule element {other}"
                )));
            }
        }
    }
    let is_enabled = match rule.child_text("Status")?.map(str::trim) {
        Some("Enabled") => true,
        Some("Disabled") => false,
        _ => {
            return Err(S3Error::malformed_xml(
                "every Rule needs Status Enabled or Disabled",
            ));
        }
    };
    let prefix = match (rule.child("Filter"), rule.child_text("Prefix")?) {
        (Some(filter), None) => parse_filter(filter)?,
        (None, Some(legacy_prefix)) => legacy_prefix.to_owned(),
        (Some(_), Some(_)) => {
            return Err(S3Error::malformed_xml("a Rule has both Filter and Prefix"));
        }
        (None, None) => {
            return Err(S3Error::malformed_xml(
                "every Rule needs a Filter or a Prefix",
            ));
        }
    };
    let mut parsed = LifecycleRule {
        id: rule.child_text("ID")?.map(str::to_owned),
        is_enabled,
        prefix,
        ..LifecycleRule::default()
    };
    if let Some(expiration) = rule.child("Expiration") {
        expect_only_children(expiration, &["Days", "Date", "ExpiredObjectDeleteMarker"])?;
        if expiration.child("Date").is_some() {
            return Err(S3Error::not_implemented("lifecycle date-based expiration"));
        }
        parsed.expiration_days = optional_positive(expiration, "Days")?;
        parsed.expired_object_delete_marker = expiration
            .child_text("ExpiredObjectDeleteMarker")?
            .map(|value| match value.trim() {
                "true" => Ok(true),
                "false" => Ok(false),
                _ => Err(S3Error::malformed_xml(
                    "ExpiredObjectDeleteMarker must be true or false",
                )),
            })
            .transpose()?;
        match (parsed.expiration_days, parsed.expired_object_delete_marker) {
            (Some(_), Some(_)) => {
                return Err(S3Error::malformed_xml(
                    "Expiration cannot combine Days with ExpiredObjectDeleteMarker",
                ));
            }
            (None, None) => {
                return Err(S3Error::malformed_xml(
                    "Expiration needs Days or ExpiredObjectDeleteMarker",
                ));
            }
            (Some(_), None) | (None, Some(_)) => {}
        }
    }
    if let Some(noncurrent) = rule.child("NoncurrentVersionExpiration") {
        expect_only_children(noncurrent, &["NoncurrentDays", "NewerNoncurrentVersions"])?;
        parsed.noncurrent_expiration_days = Some(
            optional_positive(noncurrent, "NoncurrentDays")?.ok_or_else(|| {
                S3Error::malformed_xml("NoncurrentVersionExpiration needs NoncurrentDays")
            })?,
        );
        parsed.newer_noncurrent_versions =
            optional_positive(noncurrent, "NewerNoncurrentVersions")?;
        if let Some(retained) = parsed.newer_noncurrent_versions {
            if retained > MAX_NEWER_NONCURRENT_VERSIONS {
                return Err(S3Error::invalid_argument(format!(
                    "NewerNoncurrentVersions must be between 1 and {MAX_NEWER_NONCURRENT_VERSIONS}"
                )));
            }
            if rule.child("Filter").is_none() {
                return Err(S3Error::invalid_argument(
                    "NewerNoncurrentVersions requires a Filter",
                ));
            }
        }
    }
    if let Some(abort) = rule.child("AbortIncompleteMultipartUpload") {
        expect_only_children(abort, &["DaysAfterInitiation"])?;
        parsed.abort_incomplete_upload_days = Some(
            optional_positive(abort, "DaysAfterInitiation")?.ok_or_else(|| {
                S3Error::malformed_xml("AbortIncompleteMultipartUpload needs DaysAfterInitiation")
            })?,
        );
    }
    if rule.child("Expiration").is_none()
        && rule.child("NoncurrentVersionExpiration").is_none()
        && rule.child("AbortIncompleteMultipartUpload").is_none()
    {
        return Err(S3Error::malformed_xml(
            "every Rule needs at least one action",
        ));
    }
    Ok(parsed)
}

/// Returns the prefix a Filter selects; tag and size filters are not supported.
fn parse_filter(filter: &XmlElement) -> Result<String, S3Error> {
    if filter.children.len() > 1 {
        return Err(S3Error::malformed_xml(
            "a Filter has at most one condition; use And to combine",
        ));
    }
    let Some(condition) = filter.children.first() else {
        return Ok(String::new());
    };
    match condition.name.as_str() {
        "Prefix" => filter
            .child_text("Prefix")
            .map(|prefix| prefix.unwrap_or_default().to_owned()),
        "And" => {
            expect_only_children(
                condition,
                &[
                    "Prefix",
                    "Tag",
                    "ObjectSizeGreaterThan",
                    "ObjectSizeLessThan",
                ],
            )?;
            if condition.child("Tag").is_some() {
                return Err(S3Error::not_implemented("lifecycle tag filters"));
            }
            if condition.child("ObjectSizeGreaterThan").is_some()
                || condition.child("ObjectSizeLessThan").is_some()
            {
                return Err(S3Error::not_implemented("lifecycle object size filters"));
            }
            Ok(condition
                .child_text("Prefix")?
                .unwrap_or_default()
                .to_owned())
        }
        "Tag" => Err(S3Error::not_implemented("lifecycle tag filters")),
        "ObjectSizeGreaterThan" | "ObjectSizeLessThan" => {
            Err(S3Error::not_implemented("lifecycle object size filters"))
        }
        other => Err(S3Error::malformed_xml(format!(
            "unexpected Filter element {other}"
        ))),
    }
}

fn expect_only_children(element: &XmlElement, allowed: &[&str]) -> Result<(), S3Error> {
    for child in &element.children {
        if !allowed.contains(&child.name.as_str()) {
            return Err(S3Error::malformed_xml(format!(
                "unexpected {} element {}",
                element.name, child.name
            )));
        }
        if element.children_named(&child.name).count() > 1 {
            return Err(S3Error::malformed_xml(format!(
                "{} has more than one {}",
                element.name, child.name
            )));
        }
    }
    Ok(())
}

fn optional_positive(element: &XmlElement, name: &str) -> Result<Option<u32>, S3Error> {
    element
        .child_text(name)?
        .map(|text| {
            text.trim()
                .parse::<u32>()
                .ok()
                .filter(|value| *value > 0)
                .ok_or_else(|| {
                    S3Error::invalid_argument(format!("{name} must be a positive integer"))
                })
        })
        .transpose()
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
    if rule.expiration_days.is_some() || rule.expired_object_delete_marker.is_some() {
        document
            .open("Expiration")
            .optional_text_element(
                "Days",
                rule.expiration_days.map(|days| days.to_string()).as_deref(),
            )
            .optional_text_element(
                "ExpiredObjectDeleteMarker",
                rule.expired_object_delete_marker
                    .map(|is_enabled| if is_enabled { "true" } else { "false" }),
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
