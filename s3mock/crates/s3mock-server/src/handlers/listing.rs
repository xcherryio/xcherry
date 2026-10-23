// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! ListObjectsV2 and ListObjectVersions responses.

use std::sync::Arc;

use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};
use s3mock::{ListObjectVersionsRequest, ListObjectsRequest, MAX_KEYS_LIMIT, Store, VersionRecord};

use super::OWNER_ID;
use crate::routing::QueryParameters;
use crate::s3_error::S3Error;
use crate::server::{S3Response, run_store_operation, xml_response};
use crate::xml::{XmlWriter, format_iso8601};

/// With `encoding-type=url`, everything except RFC 3986 unreserved characters and `/` is escaped.
const URL_ENCODED_LISTING: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'_')
    .remove(b'.')
    .remove(b'~')
    .remove(b'/');
const STORAGE_CLASS: &str = "STANDARD";

/// Applies `encoding-type=url` to keys, prefixes, and delimiters in listing responses.
struct ListingEncoder {
    is_url_encoded: bool,
}

impl ListingEncoder {
    fn from_query(query: &QueryParameters) -> Result<ListingEncoder, S3Error> {
        match query.get("encoding-type") {
            None => Ok(ListingEncoder {
                is_url_encoded: false,
            }),
            Some("url") => Ok(ListingEncoder {
                is_url_encoded: true,
            }),
            Some(other) => Err(S3Error::invalid_argument(format!(
                "invalid encoding-type {other:?}"
            ))),
        }
    }

    fn encode(&self, text: &str) -> String {
        if self.is_url_encoded {
            utf8_percent_encode(text, URL_ENCODED_LISTING).to_string()
        } else {
            text.to_owned()
        }
    }

    fn encoding_type(&self) -> Option<&'static str> {
        self.is_url_encoded.then_some("url")
    }
}

pub(crate) async fn list_objects_v2(
    store: Arc<Store>,
    bucket: String,
    query: QueryParameters,
) -> Result<S3Response, S3Error> {
    let encoder = ListingEncoder::from_query(&query)?;
    let request = ListObjectsRequest {
        prefix: query.get("prefix").unwrap_or_default().to_owned(),
        delimiter: query.get("delimiter").unwrap_or_default().to_owned(),
        max_keys: max_keys(&query)?,
        continuation_token: query.get("continuation-token").map(str::to_owned),
        start_after: query.get("start-after").map(str::to_owned),
    };
    let page = {
        let (bucket, request) = (bucket.clone(), request.clone());
        run_store_operation(&store, move |store| store.list_objects(&bucket, &request)).await?
    };
    let mut document = XmlWriter::new("ListBucketResult");
    document
        .text_element("Name", &bucket)
        .text_element("Prefix", &encoder.encode(&request.prefix))
        .optional_text_element(
            "Delimiter",
            non_empty(&request.delimiter)
                .map(|delimiter| encoder.encode(delimiter))
                .as_deref(),
        )
        .text_element("MaxKeys", &request.max_keys.to_string())
        .optional_text_element("EncodingType", encoder.encoding_type())
        .text_element("KeyCount", &page.key_count().to_string())
        .text_element("IsTruncated", &page.is_truncated.to_string())
        .optional_text_element("ContinuationToken", request.continuation_token.as_deref())
        .optional_text_element(
            "NextContinuationToken",
            page.next_continuation_token.as_deref(),
        )
        .optional_text_element(
            "StartAfter",
            request
                .start_after
                .as_deref()
                .map(|start| encoder.encode(start))
                .as_deref(),
        );
    for object in &page.objects {
        document
            .open("Contents")
            .text_element("Key", &encoder.encode(&object.key));
        write_object_attributes(&mut document, &object.record);
        document.close("Contents");
    }
    write_common_prefixes(&mut document, &page.common_prefixes, &encoder);
    xml_response(document.finish("ListBucketResult"))
}

pub(crate) async fn list_object_versions(
    store: Arc<Store>,
    bucket: String,
    query: QueryParameters,
) -> Result<S3Response, S3Error> {
    let encoder = ListingEncoder::from_query(&query)?;
    let request = ListObjectVersionsRequest {
        prefix: query.get("prefix").unwrap_or_default().to_owned(),
        delimiter: query.get("delimiter").unwrap_or_default().to_owned(),
        max_keys: max_keys(&query)?,
        key_marker: query
            .get("key-marker")
            .and_then(non_empty)
            .map(str::to_owned),
        version_id_marker: query
            .get("version-id-marker")
            .and_then(non_empty)
            .map(str::to_owned),
    };
    let page = {
        let (bucket, request) = (bucket.clone(), request.clone());
        run_store_operation(&store, move |store| {
            store.list_object_versions(&bucket, &request)
        })
        .await?
    };
    let mut document = XmlWriter::new("ListVersionsResult");
    document
        .text_element("Name", &bucket)
        .text_element("Prefix", &encoder.encode(&request.prefix))
        .text_element(
            "KeyMarker",
            &encoder.encode(request.key_marker.as_deref().unwrap_or_default()),
        )
        .text_element(
            "VersionIdMarker",
            request.version_id_marker.as_deref().unwrap_or_default(),
        )
        .optional_text_element(
            "NextKeyMarker",
            page.next_key_marker
                .as_deref()
                .map(|marker| encoder.encode(marker))
                .as_deref(),
        )
        .optional_text_element(
            "NextVersionIdMarker",
            page.next_version_id_marker.as_deref(),
        )
        .text_element("MaxKeys", &request.max_keys.to_string())
        .optional_text_element(
            "Delimiter",
            non_empty(&request.delimiter)
                .map(|delimiter| encoder.encode(delimiter))
                .as_deref(),
        )
        .optional_text_element("EncodingType", encoder.encoding_type())
        .text_element("IsTruncated", &page.is_truncated.to_string());
    for version in &page.versions {
        let element = if version.record.is_delete_marker {
            "DeleteMarker"
        } else {
            "Version"
        };
        document
            .open(element)
            .text_element("Key", &encoder.encode(&version.key))
            .text_element("VersionId", &version.record.version_id())
            .text_element("IsLatest", &version.is_latest.to_string());
        if version.record.is_delete_marker {
            document.text_element(
                "LastModified",
                &format_iso8601(version.record.last_modified_unix_ms),
            );
        } else {
            write_object_attributes(&mut document, &version.record);
        }
        document
            .open("Owner")
            .text_element("ID", OWNER_ID)
            .text_element("DisplayName", OWNER_ID)
            .close("Owner")
            .close(element);
    }
    write_common_prefixes(&mut document, &page.common_prefixes, &encoder);
    xml_response(document.finish("ListVersionsResult"))
}

fn write_object_attributes(document: &mut XmlWriter, record: &VersionRecord) {
    document
        .text_element(
            "LastModified",
            &format_iso8601(record.last_modified_unix_ms),
        )
        .text_element("ETag", &format!("\"{}\"", record.etag));
    if let Some(checksum) = &record.checksum {
        document
            .text_element("ChecksumAlgorithm", checksum.algorithm.s3_name())
            .text_element("ChecksumType", "FULL_OBJECT");
    }
    document
        .text_element("Size", &record.size.to_string())
        .text_element("StorageClass", STORAGE_CLASS);
}

fn write_common_prefixes(
    document: &mut XmlWriter,
    common_prefixes: &[String],
    encoder: &ListingEncoder,
) {
    for common_prefix in common_prefixes {
        document
            .open("CommonPrefixes")
            .text_element("Prefix", &encoder.encode(common_prefix))
            .close("CommonPrefixes");
    }
}

fn max_keys(query: &QueryParameters) -> Result<usize, S3Error> {
    match query.get("max-keys") {
        None => Ok(MAX_KEYS_LIMIT),
        Some(value) => value
            .parse::<usize>()
            .map(|max_keys| max_keys.min(MAX_KEYS_LIMIT))
            .map_err(|_| {
                S3Error::invalid_argument(format!(
                    "max-keys {value:?} is not a non-negative integer"
                ))
            }),
    }
}

fn non_empty(text: &str) -> Option<&str> {
    (!text.is_empty()).then_some(text)
}
