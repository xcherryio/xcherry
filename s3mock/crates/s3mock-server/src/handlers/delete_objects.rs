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
use crate::xml::{XmlWriter, first_element_text, repeated_elements};

/// S3 accepts at most this many keys per request.
const MAX_DELETE_OBJECTS: usize = 1000;
const MAX_DELETE_BODY_BYTES: usize = 2 * 1024 * 1024;

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
    let body = read_small_body(body, &headers, MAX_DELETE_BODY_BYTES).await?;
    body.verify_integrity(&headers, true)?;
    let is_quiet = first_element_text(&body.bytes, "Quiet")?
        .is_some_and(|quiet| quiet.eq_ignore_ascii_case("true"));
    let targets = repeated_elements(&body.bytes, "Object")?
        .into_iter()
        .map(|mut fields| {
            let key = fields
                .remove("Key")
                .ok_or_else(|| S3Error::malformed_xml("every Object needs a Key"))?;
            Ok(DeleteTarget {
                key,
                version_id: fields
                    .remove("VersionId")
                    .filter(|version_id| !version_id.is_empty()),
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
