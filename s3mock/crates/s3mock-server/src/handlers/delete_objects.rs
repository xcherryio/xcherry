// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! DeleteObjects: up to 1000 independent deletes reported in one document.

use std::sync::Arc;

use http::HeaderMap;
use hyper::body::Incoming;
use s3mock::{DeleteObjectOutcome, DeleteObjectRequest, Store, StoreError};

use super::request_body::read_small_body;
use crate::s3_error::S3Error;
use crate::server::{S3Response, run_store_operation, xml_response};
use crate::xml::{XmlWriter, parse_document};

/// S3 accepts at most this many keys per request.
const MAX_DELETE_OBJECTS: usize = 1000;
/// Fits 1000 maximum-length keys even when every byte is written as a five-byte entity.
const MAX_DELETE_BODY_BYTES: usize = 8 * 1024 * 1024;

struct DeleteTarget {
    key: String,
    version_id: Option<String>,
}

pub(crate) async fn delete_objects(
    store: Arc<Store>,
    bucket: String,
    headers: HeaderMap,
    body: Incoming,
) -> Result<S3Response, S3Error> {
    {
        let bucket = bucket.clone();
        run_store_operation(&store, move |store| store.bucket(&bucket)).await?;
    }
    let body = read_small_body(body, &headers, MAX_DELETE_BODY_BYTES).await?;
    body.verify_integrity(&headers, true)?;
    let document = parse_document(&body.bytes, "Delete")?;
    let is_quiet = document
        .child_text("Quiet")?
        .is_some_and(|quiet| quiet.trim().eq_ignore_ascii_case("true"));
    let targets = document
        .children_named("Object")
        .map(|object| {
            let key = object
                .child_text("Key")?
                .ok_or_else(|| S3Error::malformed_xml("every Object needs a Key"))?
                .to_owned();
            Ok(DeleteTarget {
                key,
                version_id: object
                    .child_text("VersionId")?
                    .map(str::trim)
                    .filter(|version_id| !version_id.is_empty())
                    .map(str::to_owned),
            })
        })
        .collect::<Result<Vec<_>, S3Error>>()?;
    if targets.is_empty() || targets.len() > MAX_DELETE_OBJECTS {
        return Err(S3Error::malformed_xml(format!(
            "a Delete request lists between 1 and {MAX_DELETE_OBJECTS} objects"
        )));
    }
    let results = run_store_operation(&store, move |store| {
        Ok::<_, StoreError>(
            targets
                .into_iter()
                .map(|target| {
                    let request = DeleteObjectRequest {
                        version_id: target.version_id.clone(),
                    };
                    let result = store.delete_object(&bucket, &target.key, &request);
                    (target, result)
                })
                .collect::<Vec<_>>(),
        )
    })
    .await?;
    let mut document = XmlWriter::new("DeleteResult");
    for (target, result) in results {
        match result {
            Ok(outcome) => {
                if !is_quiet {
                    write_deleted(&mut document, &target, &outcome);
                }
            }
            Err(error) => {
                let error = S3Error::from(error);
                document
                    .open("Error")
                    .text_element("Key", &target.key)
                    .optional_text_element("VersionId", target.version_id.as_deref())
                    .text_element("Code", error.code)
                    .text_element("Message", &error.message)
                    .close("Error");
            }
        }
    }
    xml_response(document.finish("DeleteResult"))
}

fn write_deleted(document: &mut XmlWriter, target: &DeleteTarget, outcome: &DeleteObjectOutcome) {
    document.open("Deleted").text_element("Key", &target.key);
    if target.version_id.is_some() {
        document.optional_text_element("VersionId", target.version_id.as_deref());
        if outcome.is_delete_marker {
            document.text_element("DeleteMarker", "true");
            document.optional_text_element("DeleteMarkerVersionId", target.version_id.as_deref());
        }
    } else if outcome.is_delete_marker {
        document
            .text_element("DeleteMarker", "true")
            .optional_text_element("DeleteMarkerVersionId", outcome.version_id.as_deref());
    }
    document.close("Deleted");
}
