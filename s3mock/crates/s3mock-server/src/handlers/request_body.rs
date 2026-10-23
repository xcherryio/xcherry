// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Small request bodies such as XML documents, read fully into memory.

use bytes::Bytes;
use http::HeaderMap;
use http_body_util::{BodyExt, Limited};
use s3mock::{ChecksumAlgorithm, md5_base64};

use crate::aws_chunked::AwsChunkedDecoder;
use crate::request_headers::{BodyEncoding, body_encoding, header_text};
use crate::s3_error::S3Error;

/// A request body with the integrity headers and trailers that arrived with it.
pub(super) struct SmallBody {
    pub(super) bytes: Bytes,
    trailers: Vec<(String, String)>,
}

/// Reads a body of at most `limit` bytes, decoding aws-chunked framing.
pub(super) async fn read_small_body(
    body: crate::server::RequestBody,
    headers: &HeaderMap,
    limit: usize,
) -> Result<SmallBody, S3Error> {
    let encoding = body_encoding(headers)?;
    let raw = Limited::new(body, limit)
        .collect()
        .await
        .map_err(|error| S3Error::incomplete_body(format!("failed to read the body: {error}")))?
        .to_bytes();
    match encoding {
        BodyEncoding::Plain => Ok(SmallBody {
            bytes: raw,
            trailers: Vec::new(),
        }),
        BodyEncoding::AwsChunked { .. } => {
            let mut decoder = AwsChunkedDecoder::new();
            let mut decoded = Vec::new();
            decoder.push(&raw, &mut decoded)?;
            let trailers = decoder.finish()?;
            Ok(SmallBody {
                bytes: Bytes::from(decoded.concat()),
                trailers,
            })
        }
    }
}

impl SmallBody {
    /// Verifies only `Content-MD5`, for requests whose checksum headers describe something else.
    pub(super) fn verify_content_md5(&self, headers: &HeaderMap) -> Result<(), S3Error> {
        match header_text(headers, "content-md5") {
            Some(content_md5) if content_md5.trim() != md5_base64(&self.bytes) => {
                Err(S3Error::from(s3mock::StoreError::BadDigest {
                    algorithm: "MD5",
                }))
            }
            Some(_) | None => Ok(()),
        }
    }

    /// Verifies `Content-MD5` and `x-amz-checksum-*` values; `is_required` demands at least one.
    pub(super) fn verify_integrity(
        &self,
        headers: &HeaderMap,
        is_required: bool,
    ) -> Result<(), S3Error> {
        let mut has_digest = false;
        if let Some(content_md5) = header_text(headers, "content-md5") {
            has_digest = true;
            if content_md5.trim() != md5_base64(&self.bytes) {
                return Err(S3Error::from(s3mock::StoreError::BadDigest {
                    algorithm: "MD5",
                }));
            }
        }
        for algorithm in ChecksumAlgorithm::ALL {
            let declared = header_text(headers, algorithm.header_name()).or_else(|| {
                self.trailers
                    .iter()
                    .find(|(name, _)| name == algorithm.header_name())
                    .map(|(_, value)| value.as_str())
            });
            if let Some(declared) = declared {
                has_digest = true;
                if declared.trim() != algorithm.base64_checksum(&self.bytes) {
                    return Err(S3Error::from(s3mock::StoreError::BadDigest {
                        algorithm: algorithm.s3_name(),
                    }));
                }
            }
        }
        if is_required && !has_digest {
            return Err(S3Error::new(
                http::StatusCode::BAD_REQUEST,
                "InvalidRequest",
                "Missing required header for this request: Content-MD5",
            ));
        }
        Ok(())
    }
}
