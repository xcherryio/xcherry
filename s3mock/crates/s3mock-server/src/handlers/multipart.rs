// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Multipart upload operations.

use std::sync::Arc;

use http::{HeaderMap, StatusCode};
use hyper::body::Incoming;
use s3mock::{
    ChecksumAlgorithm, ChecksumType, CompleteMultipartUploadRequest, CompletedPart,
    CreateMultipartUploadRequest, ListMultipartUploadsRequest, ListPartsRequest, MAX_KEYS_LIMIT,
    MAX_PART_NUMBER, PartRecord, Store, UploadChecksumExpectations, UploadRecord,
};

use super::OWNER_ID;
use super::bucket::status_response;
use super::listing::{ListingEncoder, non_empty};
use super::object::{expected_checksum, stream_upload};
use super::request_body::read_small_body;
use crate::request_headers::{
    body_encoding, content_headers, header_text, upload_checksum_plan, user_metadata,
    write_condition,
};
use crate::routing::QueryParameters;
use crate::s3_error::S3Error;
use crate::server::{S3Response, run_store_operation, xml_response};
use crate::xml::{XmlWriter, format_iso8601, parse_document};

/// A part list for 10000 parts fits comfortably in this many bytes.
const MAX_COMPLETE_BODY_BYTES: usize = 4 * 1024 * 1024;
const STORAGE_CLASS: &str = "STANDARD";

pub(crate) async fn create_multipart_upload(
    store: Arc<Store>,
    bucket: String,
    key: String,
    headers: &HeaderMap,
) -> Result<S3Response, S3Error> {
    let checksum_algorithm = header_text(headers, "x-amz-checksum-algorithm")
        .map(|name| {
            ChecksumAlgorithm::from_s3_name(name.trim()).ok_or_else(|| {
                S3Error::invalid_argument(format!("unsupported checksum algorithm {name:?}"))
            })
        })
        .transpose()?;
    let checksum_type = header_text(headers, "x-amz-checksum-type")
        .map(|name| {
            ChecksumType::from_s3_name(name.trim()).ok_or_else(|| {
                S3Error::invalid_argument(format!("unsupported checksum type {name:?}"))
            })
        })
        .transpose()?;
    let request = CreateMultipartUploadRequest {
        content_headers: content_headers(headers),
        user_metadata: user_metadata(headers),
        checksum_algorithm,
        checksum_type,
    };
    let upload = {
        let (bucket, key) = (bucket.clone(), key.clone());
        run_store_operation(&store, move |store| {
            store.create_multipart_upload(&bucket, &key, request)
        })
        .await?
    };
    let mut document = XmlWriter::new("InitiateMultipartUploadResult");
    document
        .text_element("Bucket", &bucket)
        .text_element("Key", &key)
        .text_element("UploadId", &upload.upload_id);
    let mut response = xml_response(document.finish("InitiateMultipartUploadResult"))?;
    if let Some(algorithm) = upload.checksum_algorithm {
        let response_headers = response.headers_mut();
        response_headers.insert(
            "x-amz-checksum-algorithm",
            http::HeaderValue::from_static(algorithm.s3_name()),
        );
        response_headers.insert(
            "x-amz-checksum-type",
            http::HeaderValue::from_static(upload.checksum_type.s3_name()),
        );
    }
    Ok(response)
}

pub(crate) async fn upload_part(
    store: Arc<Store>,
    bucket: String,
    key: String,
    query: QueryParameters,
    headers: HeaderMap,
    body: Incoming,
) -> Result<S3Response, S3Error> {
    if headers.contains_key("x-amz-copy-source") {
        return Err(S3Error::not_implemented("UploadPartCopy"));
    }
    let upload_id = required_upload_id(&query)?;
    let part_number = query
        .get("partNumber")
        .and_then(|value| value.parse::<u32>().ok())
        .filter(|part_number| (1..=MAX_PART_NUMBER).contains(part_number))
        .ok_or_else(|| {
            S3Error::invalid_argument(format!(
                "Part number must be an integer between 1 and {MAX_PART_NUMBER}, inclusive"
            ))
        })?;
    let encoding = body_encoding(&headers)?;
    let checksum_plan = upload_checksum_plan(&headers)?;
    let writer = {
        let (bucket, key, upload_id) = (bucket.clone(), key.clone(), upload_id.clone());
        let algorithm = checksum_plan.algorithm;
        run_store_operation(&store, move |store| {
            store.begin_upload_part(&bucket, &key, &upload_id, part_number, algorithm)
        })
        .await?
    };
    if checksum_plan.is_client_requested && writer.checksum_algorithm() != checksum_plan.algorithm {
        return Err(S3Error::new(
            StatusCode::BAD_REQUEST,
            "InvalidRequest",
            "the part checksum algorithm must match the CreateMultipartUpload checksum algorithm",
        ));
    }
    let is_checksum_echoed =
        checksum_plan.is_client_requested || writer.checksum_algorithm() != checksum_plan.algorithm;
    let (writer, trailers) = stream_upload(writer, body, encoding).await?;
    let expectations = UploadChecksumExpectations {
        content_md5_base64: header_text(&headers, "content-md5").map(str::to_owned),
        algorithm: Some(writer.checksum_algorithm()),
        expected_value_base64: expected_checksum(&checksum_plan, &trailers)?,
    };
    let part = run_store_operation(&store, move |store| {
        let staged = writer.finish(&expectations)?;
        store.commit_upload_part(&bucket, &key, &upload_id, part_number, staged)
    })
    .await?;
    let etag = format!("\"{}\"", part.etag);
    let mut response_headers = vec![("etag", etag.as_str())];
    if is_checksum_echoed {
        response_headers.push((
            part.checksum.algorithm.header_name(),
            part.checksum.base64_value.as_str(),
        ));
    }
    status_response(StatusCode::OK, &response_headers)
}

pub(crate) async fn complete_multipart_upload(
    store: Arc<Store>,
    bucket: String,
    key: String,
    query: QueryParameters,
    headers: &HeaderMap,
    body: Incoming,
) -> Result<S3Response, S3Error> {
    let upload_id = required_upload_id(&query)?;
    let write_condition = write_condition(headers)?;
    let expected_checksum = ChecksumAlgorithm::ALL.into_iter().find_map(|algorithm| {
        header_text(headers, algorithm.header_name())
            .map(|value| (algorithm, value.trim().to_owned()))
    });
    let expected_checksum_type = header_text(headers, "x-amz-checksum-type")
        .map(|name| {
            ChecksumType::from_s3_name(name.trim()).ok_or_else(|| {
                S3Error::invalid_argument(format!("unsupported checksum type {name:?}"))
            })
        })
        .transpose()?;
    let expected_object_size = header_text(headers, "x-amz-mp-object-size")
        .map(|size| {
            size.trim().parse::<u64>().map_err(|_| {
                S3Error::invalid_argument("x-amz-mp-object-size must be a non-negative integer")
            })
        })
        .transpose()?;
    let body = read_small_body(body, headers, MAX_COMPLETE_BODY_BYTES).await?;
    // Checksum headers on this request describe the object, not the part list.
    body.verify_content_md5(headers)?;
    let document = parse_document(&body.bytes, "CompleteMultipartUpload")?;
    let parts = document
        .children_named("Part")
        .map(|part| {
            let part_number = part
                .child_text("PartNumber")
                .and_then(|value| value.trim().parse::<u32>().ok())
                .ok_or_else(|| S3Error::malformed_xml("every Part needs a numeric PartNumber"))?;
            let etag = part
                .child_text("ETag")
                .map(str::to_owned)
                .ok_or_else(|| S3Error::malformed_xml("every Part needs an ETag"))?;
            let checksum_base64 = ChecksumAlgorithm::ALL.into_iter().find_map(|algorithm| {
                part.child_text(algorithm.xml_element_name())
                    .map(str::to_owned)
            });
            Ok(CompletedPart {
                part_number,
                etag,
                checksum_base64,
            })
        })
        .collect::<Result<Vec<_>, S3Error>>()?;
    if parts.is_empty() {
        return Err(S3Error::malformed_xml(
            "The XML you provided was not well-formed or did not validate against our published schema",
        ));
    }
    let request = CompleteMultipartUploadRequest {
        parts,
        write_condition,
        expected_checksum,
        expected_checksum_type,
        expected_object_size,
    };
    let outcome = {
        let (bucket, key) = (bucket.clone(), key.clone());
        run_store_operation(&store, move |store| {
            store.complete_multipart_upload(&bucket, &key, &upload_id, request)
        })
        .await?
    };
    let mut document = XmlWriter::new("CompleteMultipartUploadResult");
    document
        .text_element("Location", &format!("/{bucket}/{key}"))
        .text_element("Bucket", &bucket)
        .text_element("Key", &key)
        .text_element("ETag", &format!("\"{}\"", outcome.etag))
        .text_element(
            outcome.checksum.algorithm.xml_element_name(),
            &outcome.checksum.base64_value,
        )
        .text_element("ChecksumType", outcome.checksum.checksum_type.s3_name());
    let mut response = xml_response(document.finish("CompleteMultipartUploadResult"))?;
    if let Some(version_id) = &outcome.version_id {
        response
            .headers_mut()
            .insert("x-amz-version-id", crate::server::header_value(version_id));
    }
    Ok(response)
}

pub(crate) async fn abort_multipart_upload(
    store: Arc<Store>,
    bucket: String,
    key: String,
    query: QueryParameters,
) -> Result<S3Response, S3Error> {
    let upload_id = required_upload_id(&query)?;
    run_store_operation(&store, move |store| {
        store.abort_multipart_upload(&bucket, &key, &upload_id)
    })
    .await?;
    status_response(StatusCode::NO_CONTENT, &[])
}

pub(crate) async fn list_parts(
    store: Arc<Store>,
    bucket: String,
    key: String,
    query: QueryParameters,
) -> Result<S3Response, S3Error> {
    let upload_id = required_upload_id(&query)?;
    let request = ListPartsRequest {
        part_number_marker: parse_count(&query, "part-number-marker", 0)?,
        max_parts: parse_count(&query, "max-parts", MAX_KEYS_LIMIT)?,
    };
    let page = {
        let (bucket, key, upload_id) = (bucket.clone(), key.clone(), upload_id.clone());
        let request = request.clone();
        run_store_operation(&store, move |store| {
            store.list_parts(&bucket, &key, &upload_id, &request)
        })
        .await?
    };
    let mut document = XmlWriter::new("ListPartsResult");
    document
        .text_element("Bucket", &bucket)
        .text_element("Key", &key)
        .text_element("UploadId", &upload_id)
        .text_element("PartNumberMarker", &request.part_number_marker.to_string())
        .optional_text_element(
            "NextPartNumberMarker",
            page.next_part_number_marker
                .map(|marker| marker.to_string())
                .as_deref(),
        )
        .text_element(
            "MaxParts",
            &request.max_parts.min(MAX_KEYS_LIMIT).to_string(),
        )
        .text_element("IsTruncated", &page.is_truncated.to_string());
    for part in &page.parts {
        write_part(&mut document, part);
    }
    write_owner(&mut document, "Initiator");
    write_owner(&mut document, "Owner");
    document.text_element("StorageClass", STORAGE_CLASS);
    write_upload_checksum(&mut document, &page.upload);
    xml_response(document.finish("ListPartsResult"))
}

pub(crate) async fn list_multipart_uploads(
    store: Arc<Store>,
    bucket: String,
    query: QueryParameters,
) -> Result<S3Response, S3Error> {
    let encoder = ListingEncoder::from_query(&query)?;
    let request = ListMultipartUploadsRequest {
        prefix: query.get("prefix").unwrap_or_default().to_owned(),
        delimiter: query.get("delimiter").unwrap_or_default().to_owned(),
        key_marker: query
            .get("key-marker")
            .and_then(non_empty)
            .map(str::to_owned),
        upload_id_marker: query
            .get("upload-id-marker")
            .and_then(non_empty)
            .map(str::to_owned),
        max_uploads: parse_count(&query, "max-uploads", MAX_KEYS_LIMIT)?.min(MAX_KEYS_LIMIT),
    };
    let page = {
        let (bucket, request) = (bucket.clone(), request.clone());
        run_store_operation(&store, move |store| {
            store.list_multipart_uploads(&bucket, &request)
        })
        .await?
    };
    let mut document = XmlWriter::new("ListMultipartUploadsResult");
    document
        .text_element("Bucket", &bucket)
        .text_element(
            "KeyMarker",
            &encoder.encode(request.key_marker.as_deref().unwrap_or_default()),
        )
        .text_element(
            "UploadIdMarker",
            request.upload_id_marker.as_deref().unwrap_or_default(),
        )
        .optional_text_element(
            "NextKeyMarker",
            page.next_key_marker
                .as_deref()
                .map(|marker| encoder.encode(marker))
                .as_deref(),
        )
        .optional_text_element("NextUploadIdMarker", page.next_upload_id_marker.as_deref())
        .text_element("Prefix", &encoder.encode(&request.prefix))
        .optional_text_element(
            "Delimiter",
            non_empty(&request.delimiter)
                .map(|delimiter| encoder.encode(delimiter))
                .as_deref(),
        )
        .text_element("MaxUploads", &request.max_uploads.to_string())
        .optional_text_element("EncodingType", encoder.encoding_type())
        .text_element("IsTruncated", &page.is_truncated.to_string());
    for upload in &page.uploads {
        document
            .open("Upload")
            .text_element("Key", &encoder.encode(&upload.key))
            .text_element("UploadId", &upload.upload_id);
        write_owner(&mut document, "Initiator");
        write_owner(&mut document, "Owner");
        document
            .text_element("StorageClass", STORAGE_CLASS)
            .text_element("Initiated", &format_iso8601(upload.initiated_unix_ms));
        write_upload_checksum(&mut document, upload);
        document.close("Upload");
    }
    for common_prefix in &page.common_prefixes {
        document
            .open("CommonPrefixes")
            .text_element("Prefix", &encoder.encode(common_prefix))
            .close("CommonPrefixes");
    }
    xml_response(document.finish("ListMultipartUploadsResult"))
}

fn write_part(document: &mut XmlWriter, part: &PartRecord) {
    document
        .open("Part")
        .text_element("PartNumber", &part.part_number.to_string())
        .text_element("LastModified", &format_iso8601(part.last_modified_unix_ms))
        .text_element("ETag", &format!("\"{}\"", part.etag))
        .text_element("Size", &part.size.to_string())
        .text_element(
            part.checksum.algorithm.xml_element_name(),
            &part.checksum.base64_value,
        )
        .close("Part");
}

fn write_owner(document: &mut XmlWriter, element: &str) {
    document
        .open(element)
        .text_element("ID", OWNER_ID)
        .text_element("DisplayName", OWNER_ID)
        .close(element);
}

fn write_upload_checksum(document: &mut XmlWriter, upload: &UploadRecord) {
    if let Some(algorithm) = upload.checksum_algorithm {
        document
            .text_element("ChecksumAlgorithm", algorithm.s3_name())
            .text_element("ChecksumType", upload.checksum_type.s3_name());
    }
}

fn required_upload_id(query: &QueryParameters) -> Result<String, S3Error> {
    query
        .get("uploadId")
        .filter(|upload_id| !upload_id.is_empty())
        .map(str::to_owned)
        .ok_or_else(|| S3Error::invalid_argument("uploadId is required"))
}

fn parse_count<T: std::str::FromStr>(
    query: &QueryParameters,
    name: &str,
    default: T,
) -> Result<T, S3Error> {
    match query.get(name) {
        None => Ok(default),
        Some(value) => value.parse().map_err(|_| {
            S3Error::invalid_argument(format!("{name} {value:?} is not a non-negative integer"))
        }),
    }
}
