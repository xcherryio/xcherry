// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! PutObject, GetObject, HeadObject, and DeleteObject.

use std::sync::Arc;

use bytes::Bytes;
use futures_util::TryStreamExt;
use http::{HeaderMap, HeaderValue, Response, StatusCode};
use http_body_util::{BodyExt, StreamBody};
use hyper::body::{Frame, Incoming};
use s3mock::{
    DeleteObjectRequest, ObjectReadRequest, ObjectSelection, ObjectWriter, PutObjectRequest, Store,
    StoreError, UploadChecksumExpectations, VersioningState,
};
use tokio::io::AsyncReadExt;
use tokio::sync::mpsc;
use tokio_util::io::ReaderStream;

use super::bucket::status_response;
use crate::aws_chunked::AwsChunkedDecoder;
use crate::request_headers::{
    BodyEncoding, UploadChecksumPlan, body_encoding, byte_range, content_headers, header_text,
    read_conditions, upload_checksum_plan, user_metadata, write_condition,
};
use crate::routing::QueryParameters;
use crate::s3_error::{S3Error, format_http_date};
use crate::server::{S3Response, header_value, run_store_operation};

/// Decoded chunks buffered between the network reader and the file writer.
const UPLOAD_CHANNEL_CAPACITY: usize = 8;
const DOWNLOAD_READ_BUFFER_BYTES: usize = 256 * 1024;
const DEFAULT_CONTENT_TYPE: &str = "binary/octet-stream";

/// Overrides that GetObject accepts as query parameters, paired with the header each one sets.
const RESPONSE_OVERRIDES: [(&str, &str); 6] = [
    ("response-content-type", "content-type"),
    ("response-content-disposition", "content-disposition"),
    ("response-content-encoding", "content-encoding"),
    ("response-content-language", "content-language"),
    ("response-cache-control", "cache-control"),
    ("response-expires", "expires"),
];

pub(crate) async fn put_object(
    store: Arc<Store>,
    bucket: String,
    key: String,
    headers: HeaderMap,
    body: Incoming,
) -> Result<S3Response, S3Error> {
    let encoding = body_encoding(&headers)?;
    let checksum_plan = upload_checksum_plan(&headers)?;
    let request = PutObjectRequest {
        content_headers: content_headers(&headers),
        user_metadata: user_metadata(&headers),
        write_condition: write_condition(&headers)?,
    };
    let content_md5_base64 = header_text(&headers, "content-md5").map(str::to_owned);
    let writer = {
        let (bucket, key) = (bucket.clone(), key.clone());
        let algorithm = checksum_plan.algorithm;
        run_store_operation(&store, move |store| {
            store.begin_put(&bucket, &key, algorithm)
        })
        .await?
    };
    let (writer, trailers) = stream_upload(writer, body, encoding).await?;
    let expectations = UploadChecksumExpectations {
        content_md5_base64,
        algorithm: Some(checksum_plan.algorithm),
        expected_value_base64: expected_checksum(&checksum_plan, &trailers)?,
    };
    let outcome = run_store_operation(&store, move |store| {
        let staged = writer.finish(&expectations)?;
        store.commit_put(&bucket, &key, staged, request)
    })
    .await?;
    let mut response_headers = vec![("etag", format!("\"{}\"", outcome.etag))];
    if let Some(version_id) = outcome.version_id {
        response_headers.push(("x-amz-version-id", version_id));
    }
    if checksum_plan.is_client_requested {
        response_headers.push((
            outcome.checksum.algorithm.header_name(),
            outcome.checksum.base64_value,
        ));
        response_headers.push((
            "x-amz-checksum-type",
            outcome.checksum.checksum_type.s3_name().to_owned(),
        ));
    }
    let borrowed: Vec<(&str, &str)> = response_headers
        .iter()
        .map(|(name, value)| (*name, value.as_str()))
        .collect();
    status_response(StatusCode::OK, &borrowed)
}

/// Streams the request body into the writer on a blocking thread and returns the aws-chunked trailers.
pub(super) async fn stream_upload(
    writer: ObjectWriter,
    body: Incoming,
    encoding: BodyEncoding,
) -> Result<(ObjectWriter, Vec<(String, String)>), S3Error> {
    let (sender, receiver) = mpsc::channel(UPLOAD_CHANNEL_CAPACITY);
    let write_task = tokio::task::spawn_blocking(move || write_chunks(writer, receiver));
    let received = receive_body(body, encoding, &sender).await;
    drop(sender);
    let written = write_task
        .await
        .map_err(|error| S3Error::internal(format!("upload writer task failed: {error}")))?;
    // Dropping an unfinished writer deletes its temporary file.
    match (received, written) {
        (Ok(trailers), Ok(writer)) => Ok((writer, trailers)),
        (Err(error), Ok(_)) => Err(error),
        // A writer failure explains why the reader stopped.
        (Ok(_) | Err(_), Err(error)) => Err(S3Error::from(error)),
    }
}

fn write_chunks(
    mut writer: ObjectWriter,
    mut receiver: mpsc::Receiver<Bytes>,
) -> Result<ObjectWriter, StoreError> {
    while let Some(chunk) = receiver.blocking_recv() {
        writer.write_all(&chunk)?;
    }
    Ok(writer)
}

async fn receive_body(
    mut body: Incoming,
    encoding: BodyEncoding,
    sender: &mpsc::Sender<Bytes>,
) -> Result<Vec<(String, String)>, S3Error> {
    let mut decoder = match encoding {
        BodyEncoding::Plain => None,
        BodyEncoding::AwsChunked { .. } => Some(AwsChunkedDecoder::new()),
    };
    let mut decoded_length: u64 = 0;
    let mut http_trailers = Vec::new();
    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|error| {
            S3Error::incomplete_body(format!("failed to read the body: {error}"))
        })?;
        let data = match frame.into_data() {
            Ok(data) => data,
            Err(frame) => {
                if let Ok(trailers) = frame.into_trailers() {
                    http_trailers.extend(trailers.iter().filter_map(|(name, value)| {
                        Some((name.as_str().to_owned(), value.to_str().ok()?.to_owned()))
                    }));
                }
                continue;
            }
        };
        let chunks = match &mut decoder {
            None => vec![data],
            Some(decoder) => {
                let mut decoded = Vec::new();
                decoder.push(&data, &mut decoded)?;
                decoded
            }
        };
        for chunk in chunks {
            decoded_length += u64::try_from(chunk.len()).expect("chunk lengths fit in u64");
            sender
                .send(chunk)
                .await
                .map_err(|_| S3Error::internal("the upload writer stopped"))?;
        }
    }
    let mut trailers = match decoder {
        None => Vec::new(),
        Some(decoder) => decoder.finish()?,
    };
    trailers.extend(http_trailers);
    if let BodyEncoding::AwsChunked {
        decoded_length: Some(expected),
    } = encoding
        && expected != decoded_length
    {
        return Err(S3Error::incomplete_body(format!(
            "x-amz-decoded-content-length is {expected} but the body decoded to {decoded_length} bytes"
        )));
    }
    Ok(trailers)
}

pub(super) fn expected_checksum(
    plan: &UploadChecksumPlan,
    trailers: &[(String, String)],
) -> Result<Option<String>, S3Error> {
    if let Some(value) = &plan.expected_header_value {
        return Ok(Some(value.clone()));
    }
    let Some(trailer_name) = &plan.trailer_name else {
        return Ok(None);
    };
    trailers
        .iter()
        .find(|(name, _)| name == trailer_name)
        .map(|(_, value)| Some(value.clone()))
        .ok_or_else(|| {
            S3Error::incomplete_body(format!("the declared trailer {trailer_name} is missing"))
        })
}

pub(crate) async fn get_object(
    store: Arc<Store>,
    bucket: String,
    key: String,
    query: QueryParameters,
    headers: &HeaderMap,
) -> Result<S3Response, S3Error> {
    let overrides = response_overrides(&query)?;
    let request = object_read_request(&query, headers);
    let body = run_store_operation(&store, move |store| {
        store.read_object(&bucket, &key, &request)
    })
    .await?;
    let file = tokio::fs::File::from_std(body.file).take(body.length);
    let stream = ReaderStream::with_capacity(file, DOWNLOAD_READ_BUFFER_BYTES).map_ok(Frame::data);
    let builder = object_response_builder(
        &body.selection,
        &overrides,
        is_checksum_mode_enabled(headers),
    );
    builder
        .body(StreamBody::new(stream).boxed())
        .map_err(|error| S3Error::internal(format!("failed to build a response: {error}")))
}

pub(crate) async fn head_object(
    store: Arc<Store>,
    bucket: String,
    key: String,
    query: QueryParameters,
    headers: &HeaderMap,
) -> Result<S3Response, S3Error> {
    let overrides = response_overrides(&query)?;
    let request = object_read_request(&query, headers);
    let selection = run_store_operation(&store, move |store| {
        store.head_object(&bucket, &key, &request)
    })
    .await?;
    object_response_builder(&selection, &overrides, is_checksum_mode_enabled(headers))
        .body(crate::server::empty_body())
        .map_err(|error| S3Error::internal(format!("failed to build a response: {error}")))
}

pub(crate) async fn delete_object(
    store: Arc<Store>,
    bucket: String,
    key: String,
    query: QueryParameters,
) -> Result<S3Response, S3Error> {
    let request = DeleteObjectRequest {
        version_id: query.get("versionId").map(str::to_owned),
    };
    let outcome = run_store_operation(&store, move |store| {
        store.delete_object(&bucket, &key, &request)
    })
    .await?;
    let mut response_headers = Vec::new();
    if let Some(version_id) = &outcome.version_id {
        response_headers.push(("x-amz-version-id", version_id.as_str()));
    }
    if outcome.is_delete_marker {
        response_headers.push(("x-amz-delete-marker", "true"));
    }
    status_response(StatusCode::NO_CONTENT, &response_headers)
}

fn object_read_request(query: &QueryParameters, headers: &HeaderMap) -> ObjectReadRequest {
    ObjectReadRequest {
        version_id: query.get("versionId").map(str::to_owned),
        conditions: read_conditions(headers),
        range: byte_range(headers),
    }
}

pub(super) fn is_checksum_mode_enabled(headers: &HeaderMap) -> bool {
    header_text(headers, "x-amz-checksum-mode")
        .is_some_and(|mode| mode.eq_ignore_ascii_case("ENABLED"))
}

/// Validates the `response-*` query parameters, which are client input, as header values.
fn response_overrides(
    query: &QueryParameters,
) -> Result<Vec<(&'static str, HeaderValue)>, S3Error> {
    RESPONSE_OVERRIDES
        .iter()
        .filter_map(|(parameter, header)| {
            query
                .get(parameter)
                .map(|value| (*parameter, *header, value))
        })
        .map(|(parameter, header, value)| {
            HeaderValue::from_str(value)
                .map(|value| (header, value))
                .map_err(|_| {
                    S3Error::invalid_argument(format!("{parameter} is not a valid header value"))
                })
        })
        .collect()
}

/// Builds the status and headers that GetObject and HeadObject share.
fn object_response_builder(
    selection: &ObjectSelection,
    overrides: &[(&'static str, HeaderValue)],
    is_checksum_mode_enabled: bool,
) -> http::response::Builder {
    let record = &selection.record;
    let mut builder = Response::builder()
        .status(if selection.range.is_some() {
            StatusCode::PARTIAL_CONTENT
        } else {
            StatusCode::OK
        })
        .header("etag", header_value(&format!("\"{}\"", record.etag)))
        .header(
            "last-modified",
            header_value(&format_http_date(record.last_modified_unix_ms)),
        )
        .header("accept-ranges", "bytes");
    builder = match selection.range {
        Some(range) => builder.header("content-length", range.length()).header(
            "content-range",
            header_value(&format!(
                "bytes {}-{}/{}",
                range.start, range.end_inclusive, record.size
            )),
        ),
        None => builder.header("content-length", record.size),
    };
    let stored = &record.content_headers;
    let stored_headers = [
        (
            "content-type",
            stored
                .content_type
                .as_deref()
                .or(Some(DEFAULT_CONTENT_TYPE)),
        ),
        ("content-disposition", stored.content_disposition.as_deref()),
        ("content-encoding", stored.content_encoding.as_deref()),
        ("content-language", stored.content_language.as_deref()),
        ("cache-control", stored.cache_control.as_deref()),
        ("expires", stored.expires.as_deref()),
    ];
    for (name, stored_value) in stored_headers {
        let override_value = overrides
            .iter()
            .find(|(header, _)| *header == name)
            .map(|(_, value)| value);
        match (override_value, stored_value) {
            (Some(value), _) => builder = builder.header(name, value.clone()),
            (None, Some(value)) => builder = builder.header(name, header_value(value)),
            (None, None) => {}
        }
    }
    for (name, value) in &record.user_metadata {
        builder = builder.header(format!("x-amz-meta-{name}"), header_value(value));
    }
    if selection.bucket_versioning != VersioningState::Unversioned {
        builder = builder.header("x-amz-version-id", header_value(&record.version_id()));
    }
    if is_checksum_mode_enabled
        && selection.range.is_none()
        && let Some(checksum) = &record.checksum
    {
        builder = builder
            .header(
                checksum.algorithm.header_name(),
                header_value(&checksum.base64_value),
            )
            .header("x-amz-checksum-type", checksum.checksum_type.s3_name());
    }
    builder
}
