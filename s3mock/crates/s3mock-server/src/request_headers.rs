// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Parsing of S3 request headers into store requests.

use std::collections::BTreeMap;
use std::time::UNIX_EPOCH;

use http::HeaderMap;
use s3mock::{ByteRangeRequest, ChecksumAlgorithm, ContentHeaders, ReadConditions, WriteCondition};

use crate::s3_error::S3Error;

const USER_METADATA_PREFIX: &str = "x-amz-meta-";
const AWS_CHUNKED: &str = "aws-chunked";

/// Returns a header value, treating a non-UTF-8 value as absent.
pub(crate) fn header_text<'headers>(
    headers: &'headers HeaderMap,
    name: &str,
) -> Option<&'headers str> {
    headers.get(name).and_then(|value| value.to_str().ok())
}

/// How the request body is framed on the wire.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum BodyEncoding {
    Plain,
    /// `aws-chunked`, with the decoded length from `x-amz-decoded-content-length` when sent.
    AwsChunked {
        decoded_length: Option<u64>,
    },
}

pub(crate) fn body_encoding(headers: &HeaderMap) -> Result<BodyEncoding, S3Error> {
    let is_content_encoded = header_text(headers, "content-encoding").is_some_and(|value| {
        value
            .split(',')
            .any(|coding| coding.trim().eq_ignore_ascii_case(AWS_CHUNKED))
    });
    let is_streaming_payload = header_text(headers, "x-amz-content-sha256")
        .is_some_and(|value| value.starts_with("STREAMING-"));
    if !is_content_encoded && !is_streaming_payload {
        return Ok(BodyEncoding::Plain);
    }
    let decoded_length = header_text(headers, "x-amz-decoded-content-length")
        .map(|value| {
            value.parse::<u64>().map_err(|_| {
                S3Error::invalid_argument("x-amz-decoded-content-length is not a number")
            })
        })
        .transpose()?;
    Ok(BodyEncoding::AwsChunked { decoded_length })
}

/// The checksum a PutObject computes and, when the client sent one, the value it must match.
#[derive(Clone, Debug)]
pub(crate) struct UploadChecksumPlan {
    pub(crate) algorithm: ChecksumAlgorithm,
    /// Whether the client asked for this algorithm, so S3 echoes the checksum in responses.
    pub(crate) is_client_requested: bool,
    pub(crate) expected_header_value: Option<String>,
    /// Trailer that carries the expected value in an aws-chunked body.
    pub(crate) trailer_name: Option<String>,
}

pub(crate) fn upload_checksum_plan(headers: &HeaderMap) -> Result<UploadChecksumPlan, S3Error> {
    let header_checksum = ChecksumAlgorithm::ALL.into_iter().find_map(|algorithm| {
        header_text(headers, algorithm.header_name()).map(|value| (algorithm, value))
    });
    if let Some((algorithm, value)) = header_checksum {
        return Ok(UploadChecksumPlan {
            algorithm,
            is_client_requested: true,
            expected_header_value: Some(value.to_owned()),
            trailer_name: None,
        });
    }
    if let Some(trailer) = header_text(headers, "x-amz-trailer") {
        let trailer = trailer.trim().to_ascii_lowercase();
        let algorithm = ChecksumAlgorithm::from_header_name(&trailer).ok_or_else(|| {
            S3Error::invalid_argument(format!("unsupported x-amz-trailer {trailer:?}"))
        })?;
        return Ok(UploadChecksumPlan {
            algorithm,
            is_client_requested: true,
            expected_header_value: None,
            trailer_name: Some(trailer),
        });
    }
    if let Some(name) = header_text(headers, "x-amz-sdk-checksum-algorithm") {
        let algorithm = ChecksumAlgorithm::from_s3_name(name).ok_or_else(|| {
            S3Error::invalid_argument(format!("unsupported checksum algorithm {name:?}"))
        })?;
        return Ok(UploadChecksumPlan {
            algorithm,
            is_client_requested: true,
            expected_header_value: None,
            trailer_name: None,
        });
    }
    // S3 computes CRC64NVME for uploads that carry no checksum.
    Ok(UploadChecksumPlan {
        algorithm: ChecksumAlgorithm::Crc64Nvme,
        is_client_requested: false,
        expected_header_value: None,
        trailer_name: None,
    })
}

/// Content headers stored with the object; the `aws-chunked` transfer coding is not part of them.
pub(crate) fn content_headers(headers: &HeaderMap) -> ContentHeaders {
    let owned = |name: &str| header_text(headers, name).map(str::to_owned);
    let content_encoding = header_text(headers, "content-encoding")
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|coding| !coding.eq_ignore_ascii_case(AWS_CHUNKED))
                .collect::<Vec<_>>()
                .join(", ")
        })
        .filter(|value| !value.is_empty());
    ContentHeaders {
        content_type: owned("content-type"),
        content_encoding,
        content_disposition: owned("content-disposition"),
        content_language: owned("content-language"),
        cache_control: owned("cache-control"),
        expires: owned("expires"),
    }
}

/// Collects `x-amz-meta-*` headers with lowercase names and the prefix removed.
pub(crate) fn user_metadata(headers: &HeaderMap) -> BTreeMap<String, String> {
    headers
        .iter()
        .filter_map(|(name, value)| {
            let name = name.as_str().strip_prefix(USER_METADATA_PREFIX)?;
            Some((name.to_owned(), value.to_str().ok()?.to_owned()))
        })
        .collect()
}

pub(crate) fn write_condition(headers: &HeaderMap) -> Result<WriteCondition, S3Error> {
    match (
        header_text(headers, "if-none-match"),
        header_text(headers, "if-match"),
    ) {
        (Some(_), Some(_)) => Err(S3Error::not_implemented(
            "PutObject with both If-None-Match and If-Match",
        )),
        (Some(value), None) if value.trim() == "*" => Ok(WriteCondition::IfNoneMatchAny),
        (Some(_), None) => Err(S3Error::not_implemented(
            "PutObject with an If-None-Match value other than *",
        )),
        (None, Some(value)) => Ok(WriteCondition::IfMatch(value.trim().to_owned())),
        (None, None) => Ok(WriteCondition::Unconditional),
    }
}

/// Parses conditional-read headers; an unparsable date is ignored, as RFC 9110 requires.
pub(crate) fn read_conditions(headers: &HeaderMap) -> ReadConditions {
    let owned = |name: &str| header_text(headers, name).map(str::to_owned);
    ReadConditions {
        if_match: owned("if-match"),
        if_none_match: owned("if-none-match"),
        if_modified_since_unix_seconds: header_text(headers, "if-modified-since")
            .and_then(parse_http_date_seconds),
        if_unmodified_since_unix_seconds: header_text(headers, "if-unmodified-since")
            .and_then(parse_http_date_seconds),
    }
}

fn parse_http_date_seconds(value: &str) -> Option<i64> {
    let time = httpdate::parse_http_date(value).ok()?;
    let seconds = time.duration_since(UNIX_EPOCH).ok()?.as_secs();
    i64::try_from(seconds).ok()
}

/// Parses a single `bytes=` range; multiple or malformed ranges are ignored and the full object is served.
pub(crate) fn byte_range(headers: &HeaderMap) -> Option<ByteRangeRequest> {
    let specification = header_text(headers, "range")?
        .trim()
        .strip_prefix("bytes=")?;
    if specification.contains(',') {
        return None;
    }
    let (start, end) = specification.split_once('-')?;
    let (start, end) = (start.trim(), end.trim());
    if start.is_empty() {
        return Some(ByteRangeRequest::Suffix {
            length: end.parse().ok()?,
        });
    }
    Some(ByteRangeRequest::FromStart {
        start: start.parse().ok()?,
        end_inclusive: if end.is_empty() {
            None
        } else {
            Some(end.parse().ok()?)
        },
    })
}
