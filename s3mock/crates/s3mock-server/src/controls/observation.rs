// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Records actual transferred body bytes, including incomplete response streams.

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use bytes::Bytes;
use http::{HeaderMap, Method, Uri};
use http_body::{Body, Frame, SizeHint};
use serde::Serialize;

use super::{Controls, FaultAction, S3OperationName};
use crate::routing::{self, S3Operation};
use crate::server::ResponseBody;

#[derive(Clone, Serialize)]
pub(crate) struct RequestObservation {
    pub(crate) operation: S3OperationName,
    pub(crate) bucket: String,
    #[serde(skip)]
    pub(crate) has_existing_bucket: bool,
    pub(crate) key: String,
    pub(crate) if_match: Option<String>,
    pub(crate) if_none_match: Option<String>,
    pub(crate) if_modified_since: Option<String>,
    pub(crate) if_unmodified_since: Option<String>,
    pub(crate) status: u16,
    pub(crate) duration_ms: u64,
    pub(crate) bytes_received: u64,
    pub(crate) bytes_sent: u64,
    #[serde(serialize_with = "serialize_fault_name")]
    pub(crate) injected_fault: Option<FaultAction>,
}

impl RequestObservation {
    pub(crate) fn new(method: &Method, uri: &Uri, headers: &HeaderMap) -> Self {
        let operation = routing::route(method, uri);
        let (name, bucket, key) = match &operation {
            Ok(S3Operation::ListBuckets) => (S3OperationName::ListBuckets, "", ""),
            Ok(S3Operation::GetObjectLockConfiguration { bucket }) => (
                S3OperationName::GetObjectLockConfiguration,
                bucket.as_str(),
                "",
            ),
            Ok(S3Operation::PutObjectLockConfiguration { bucket }) => (
                S3OperationName::PutObjectLockConfiguration,
                bucket.as_str(),
                "",
            ),
            Ok(S3Operation::DeleteObjects { bucket }) => {
                (S3OperationName::DeleteObjects, bucket.as_str(), "")
            }
            Ok(S3Operation::ListMultipartUploads { bucket, .. }) => {
                (S3OperationName::ListMultipartUploads, bucket.as_str(), "")
            }
            Ok(S3Operation::CreateMultipartUpload { bucket, key, .. }) => (
                S3OperationName::CreateMultipartUpload,
                bucket.as_str(),
                key.as_str(),
            ),
            Ok(S3Operation::UploadPart { bucket, key, .. }) => {
                (S3OperationName::UploadPart, bucket.as_str(), key.as_str())
            }
            Ok(S3Operation::CompleteMultipartUpload { bucket, key, .. }) => (
                S3OperationName::CompleteMultipartUpload,
                bucket.as_str(),
                key.as_str(),
            ),
            Ok(S3Operation::AbortMultipartUpload { bucket, key, .. }) => (
                S3OperationName::AbortMultipartUpload,
                bucket.as_str(),
                key.as_str(),
            ),
            Ok(S3Operation::ListParts { bucket, key, .. }) => {
                (S3OperationName::ListParts, bucket.as_str(), key.as_str())
            }
            Ok(S3Operation::CreateBucket { bucket }) => {
                (S3OperationName::CreateBucket, bucket.as_str(), "")
            }
            Ok(S3Operation::HeadBucket { bucket }) => {
                (S3OperationName::HeadBucket, bucket.as_str(), "")
            }
            Ok(S3Operation::DeleteBucket { bucket }) => {
                (S3OperationName::DeleteBucket, bucket.as_str(), "")
            }
            Ok(S3Operation::GetBucketLocation { bucket }) => {
                (S3OperationName::GetBucketLocation, bucket.as_str(), "")
            }
            Ok(S3Operation::GetBucketVersioning { bucket }) => {
                (S3OperationName::GetBucketVersioning, bucket.as_str(), "")
            }
            Ok(S3Operation::PutBucketVersioning { bucket }) => {
                (S3OperationName::PutBucketVersioning, bucket.as_str(), "")
            }
            Ok(S3Operation::ListObjectsV2 { bucket, .. }) => {
                (S3OperationName::ListObjectsV2, bucket.as_str(), "")
            }
            Ok(S3Operation::ListObjectVersions { bucket, .. }) => {
                (S3OperationName::ListObjectVersions, bucket.as_str(), "")
            }
            Ok(S3Operation::PutObject { bucket, key }) => {
                (S3OperationName::PutObject, bucket.as_str(), key.as_str())
            }
            Ok(S3Operation::GetObject { bucket, key, .. }) => {
                (S3OperationName::GetObject, bucket.as_str(), key.as_str())
            }
            Ok(S3Operation::HeadObject { bucket, key, .. }) => {
                (S3OperationName::HeadObject, bucket.as_str(), key.as_str())
            }
            Ok(S3Operation::DeleteObject { bucket, key, .. }) => {
                (S3OperationName::DeleteObject, bucket.as_str(), key.as_str())
            }
            Ok(S3Operation::GetBucketLifecycleConfiguration { bucket }) => (
                S3OperationName::GetBucketLifecycleConfiguration,
                bucket.as_str(),
                "",
            ),
            Ok(S3Operation::PutBucketLifecycleConfiguration { bucket }) => (
                S3OperationName::PutBucketLifecycleConfiguration,
                bucket.as_str(),
                "",
            ),
            Ok(S3Operation::DeleteBucketLifecycle { bucket }) => {
                (S3OperationName::DeleteBucketLifecycle, bucket.as_str(), "")
            }
            Err(_) => (S3OperationName::Unsupported, "", ""),
        };
        Self {
            operation: name,
            bucket: bucket.into(),
            has_existing_bucket: false,
            key: key.into(),
            if_match: header_value(headers, "if-match"),
            if_none_match: header_value(headers, "if-none-match"),
            if_modified_since: header_value(headers, "if-modified-since"),
            if_unmodified_since: header_value(headers, "if-unmodified-since"),
            status: 0,
            duration_ms: 0,
            bytes_received: 0,
            bytes_sent: 0,
            injected_fault: None,
        }
    }

    pub(crate) fn has_conditional_header(&self) -> bool {
        self.if_match.is_some()
            || self.if_none_match.is_some()
            || self.if_modified_since.is_some()
            || self.if_unmodified_since.is_some()
    }
}

fn header_value(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .map(|value| String::from_utf8_lossy(value.as_bytes()).into_owned())
}

pub(crate) struct ObservedBody {
    body: ResponseBody,
    controls: Arc<Controls>,
    observation: RequestObservation,
    received: Arc<AtomicU64>,
    started: Instant,
    should_corrupt: bool,
    should_truncate: bool,
    has_truncated: bool,
    flush_delay: Option<Pin<Box<tokio::time::Sleep>>>,
}

impl ObservedBody {
    pub(crate) fn new(
        body: ResponseBody,
        controls: Arc<Controls>,
        observation: RequestObservation,
        received: Arc<AtomicU64>,
        started: Instant,
        fault: Option<&FaultAction>,
    ) -> Self {
        let is_successful_body = matches!(observation.status, 200 | 206);
        Self {
            body,
            controls,
            observation,
            received,
            started,
            should_corrupt: is_successful_body
                && matches!(fault, Some(FaultAction::CorruptBody {})),
            should_truncate: is_successful_body
                && matches!(fault, Some(FaultAction::TruncateResponse {})),
            has_truncated: false,
            flush_delay: None,
        }
    }
}

impl Body for ObservedBody {
    type Data = Bytes;
    type Error = io::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, io::Error>>> {
        if let Some(delay) = &mut self.flush_delay {
            std::task::ready!(delay.as_mut().poll(context));
        }
        if self.has_truncated {
            return Poll::Ready(Some(Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "injected truncated response",
            ))));
        }
        match Pin::new(&mut self.body).poll_frame(context) {
            Poll::Ready(Some(Ok(frame))) => {
                let frame = match frame.into_data() {
                    Ok(mut data) => {
                        if self.should_corrupt && !data.is_empty() {
                            let mut changed = data.to_vec();
                            changed[0] ^= 1;
                            data = Bytes::from(changed);
                            self.should_corrupt = false;
                        }
                        if self.should_truncate {
                            data.truncate(data.len() / 2);
                            self.has_truncated = true;
                            // Yield once so Hyper flushes the prefix before the injected stream error.
                            self.flush_delay =
                                Some(Box::pin(tokio::time::sleep(Duration::from_millis(1))));
                        }
                        self.observation.bytes_sent += data.len() as u64;
                        Frame::data(data)
                    }
                    Err(frame) => frame,
                };
                Poll::Ready(Some(Ok(frame)))
            }
            Poll::Ready(None) if self.should_truncate => {
                self.has_truncated = true;
                Poll::Ready(Some(Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "injected truncated response",
                ))))
            }
            other => other,
        }
    }

    fn is_end_stream(&self) -> bool {
        !self.should_truncate && self.body.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        if self.should_truncate {
            SizeHint::default()
        } else {
            self.body.size_hint()
        }
    }
}

impl Drop for ObservedBody {
    fn drop(&mut self) {
        self.observation.bytes_received = self.received.load(Ordering::Relaxed);
        self.observation.duration_ms = self
            .started
            .elapsed()
            .as_millis()
            .try_into()
            .unwrap_or(u64::MAX);
        if self.has_truncated && self.observation.bytes_sent == 0 {
            self.observation.status = 0;
        }
        self.controls.record(self.observation.clone());
    }
}

fn serialize_fault_name<S: serde::Serializer>(
    fault: &Option<FaultAction>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    match fault {
        Some(action) => {
            let value = serde_json::to_value(action).map_err(serde::ser::Error::custom)?;
            value["type"].serialize(serializer)
        }
        None => serializer.serialize_none(),
    }
}
