// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::path::Path;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use async_trait::async_trait;
use aws_sdk_s3::Client;
use aws_sdk_s3::config::{
    BehaviorVersion, Credentials, Region, retry::RetryConfig, timeout::TimeoutConfig,
};
use aws_sdk_s3::error::{ProvideErrorMetadata, SdkError};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart, Delete, ObjectIdentifier};
use bytes::Bytes;

use crate::{
    ByteRange, ObjectContent, ObjectListing, ObjectMetadata, ObjectStore, ObjectStoreError,
    ObjectStoreErrorKind, ObjectStoreOperation, ObjectStoreResult, WriteCondition, WriteOutcome,
};

const DEFAULT_PART_SIZE: usize = 8 * 1024 * 1024;

/// Credentials used by an S3 object store.
#[derive(Debug, Clone)]
pub enum S3Credentials {
    /// Resolve credentials from the AWS default provider chain.
    DefaultChain,
    /// Sign requests with fixed credentials, usually for a local S3-compatible endpoint.
    Static {
        access_key_id: String,
        secret_access_key: String,
    },
}

/// Configuration of an S3 object store. Keys passed to the store are relative to `key_prefix`.
#[derive(Debug, Clone)]
pub struct S3ObjectStoreConfig {
    /// S3 endpoint override, or `None` for the region's AWS endpoint.
    pub endpoint: Option<String>,
    /// AWS region used for endpoint selection and request signing.
    pub region: String,
    /// Existing bucket holding all objects.
    pub bucket: String,
    /// Prefix prepended to every relative key. An absent trailing slash is added.
    pub key_prefix: String,
    /// Whether to put the bucket in the URL path.
    pub force_path_style: bool,
    /// Credentials provider.
    pub credentials: S3Credentials,
    /// Maximum duration of one SDK request.
    pub request_timeout: Duration,
    /// Bodies at least this size use multipart upload; default 8 MiB.
    pub multipart_threshold: usize,
    /// Multipart part size; default 8 MiB and must be at least 5 MiB.
    pub multipart_part_size: usize,
}

impl S3ObjectStoreConfig {
    /// Creates configuration with 8 MiB multipart defaults and default-chain credentials.
    pub fn new(region: impl Into<String>, bucket: impl Into<String>) -> Self {
        Self {
            endpoint: None,
            region: region.into(),
            bucket: bucket.into(),
            key_prefix: String::new(),
            force_path_style: false,
            credentials: S3Credentials::DefaultChain,
            request_timeout: Duration::from_secs(30),
            multipart_threshold: DEFAULT_PART_SIZE,
            multipart_part_size: DEFAULT_PART_SIZE,
        }
    }
}

/// S3 implementation of [`ObjectStore`] with conditional writes and multipart uploads.
pub struct S3ObjectStore {
    client: Client,
    read_client: Client,
    bucket: String,
    key_prefix: String,
    multipart_threshold: usize,
    multipart_part_size: usize,
}

impl S3ObjectStore {
    /// Builds a client with the configured endpoint, credentials, timeout, and multipart policy.
    pub async fn new(config: S3ObjectStoreConfig) -> ObjectStoreResult<Self> {
        if config.bucket.is_empty()
            || config.multipart_part_size < 5 * 1024 * 1024
            || config.multipart_threshold == 0
        {
            return Err(ObjectStoreError::new(
                ObjectStoreOperation::Put,
                "",
                ObjectStoreErrorKind::Permanent,
                "bucket, multipart threshold, or part size is invalid",
            ));
        }
        let shared = aws_config::defaults(BehaviorVersion::latest())
            .region(Region::new(config.region))
            .load()
            .await;
        let mut builder = aws_sdk_s3::Config::from(&shared)
            .to_builder()
            .force_path_style(config.force_path_style)
            .timeout_config(
                TimeoutConfig::builder()
                    .operation_timeout(config.request_timeout)
                    .build(),
            )
            .retry_config(RetryConfig::disabled());
        if let Some(endpoint) = config.endpoint {
            builder = builder.endpoint_url(endpoint);
        }
        if let S3Credentials::Static {
            access_key_id,
            secret_access_key,
        } = config.credentials
        {
            builder = builder.credentials_provider(Credentials::new(
                access_key_id,
                secret_access_key,
                None,
                None,
                "dexzd-object-store",
            ));
        }
        let write_config = builder.build();
        let read_config = write_config
            .to_builder()
            .retry_config(RetryConfig::standard().with_max_attempts(3))
            .build();
        Ok(Self {
            client: Client::from_conf(write_config),
            read_client: Client::from_conf(read_config),
            bucket: config.bucket,
            key_prefix: if config.key_prefix.is_empty() {
                String::new()
            } else {
                format!("{}/", config.key_prefix.trim_end_matches('/'))
            },
            multipart_threshold: config.multipart_threshold,
            multipart_part_size: config.multipart_part_size,
        })
    }

    fn full_key(&self, key: &str) -> String {
        format!("{}{}", self.key_prefix, key)
    }

    async fn put_single(
        &self,
        key: &str,
        body: Bytes,
        condition: WriteCondition,
    ) -> ObjectStoreResult<WriteOutcome> {
        let is_conditional = condition != WriteCondition::Unconditional;
        let mut request = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(self.full_key(key))
            .body(ByteStream::from(body));
        request = match condition {
            WriteCondition::Unconditional => request,
            WriteCondition::CreateOnly => request.if_none_match("*"),
            WriteCondition::ReplaceIfEtagMatches { etag } => request.if_match(etag),
        };
        let response = match request.send().await {
            Ok(response) => response,
            Err(error) if is_conditional && is_precondition_error(&error) => {
                return Ok(WriteOutcome::PreconditionFailed);
            }
            Err(error) => return Err(map_sdk_error(ObjectStoreOperation::Put, key, true, error)),
        };
        Ok(WriteOutcome::Written {
            etag: required(response.e_tag(), ObjectStoreOperation::Put, key, "ETag")?.to_owned(),
        })
    }

    async fn put_multipart(
        &self,
        key: &str,
        body: Bytes,
        condition: WriteCondition,
    ) -> ObjectStoreResult<WriteOutcome> {
        let full_key = self.full_key(key);
        let started = self
            .client
            .create_multipart_upload()
            .bucket(&self.bucket)
            .key(&full_key)
            .send()
            .await
            .map_err(|error| map_sdk_error(ObjectStoreOperation::Put, key, false, error))?;
        let upload_id = required(
            started.upload_id(),
            ObjectStoreOperation::Put,
            key,
            "upload ID",
        )?
        .to_owned();
        let result = self
            .upload_and_complete(key, &full_key, &upload_id, body, condition)
            .await;
        if (result.is_err() || matches!(result, Ok(WriteOutcome::PreconditionFailed)))
            && let Err(abort_error) = self
                .client
                .abort_multipart_upload()
                .bucket(&self.bucket)
                .key(&full_key)
                .upload_id(&upload_id)
                .send()
                .await
        {
            tracing::warn!(key, error = %abort_error, "failed to abort multipart upload");
        }
        result
    }

    async fn upload_and_complete(
        &self,
        key: &str,
        full_key: &str,
        upload_id: &str,
        body: Bytes,
        condition: WriteCondition,
    ) -> ObjectStoreResult<WriteOutcome> {
        let mut parts = Vec::new();
        for (index, chunk) in body.chunks(self.multipart_part_size).enumerate() {
            let part_number = i32::try_from(index + 1).map_err(|error| {
                ObjectStoreError::new(
                    ObjectStoreOperation::Put,
                    key,
                    ObjectStoreErrorKind::Permanent,
                    error.to_string(),
                )
            })?;
            let response = self
                .client
                .upload_part()
                .bucket(&self.bucket)
                .key(full_key)
                .upload_id(upload_id)
                .part_number(part_number)
                .body(ByteStream::from(Bytes::copy_from_slice(chunk)))
                .send()
                .await
                .map_err(|error| map_sdk_error(ObjectStoreOperation::Put, key, true, error))?;
            let etag = required(
                response.e_tag(),
                ObjectStoreOperation::Put,
                key,
                "part ETag",
            )?;
            parts.push(
                CompletedPart::builder()
                    .part_number(part_number)
                    .e_tag(etag)
                    .build(),
            );
        }
        self.complete_upload(key, full_key, upload_id, parts, condition)
            .await
    }

    async fn put_multipart_file(
        &self,
        key: &str,
        path: &Path,
        condition: WriteCondition,
    ) -> ObjectStoreResult<WriteOutcome> {
        let full_key = self.full_key(key);
        let started = self
            .client
            .create_multipart_upload()
            .bucket(&self.bucket)
            .key(&full_key)
            .send()
            .await
            .map_err(|error| map_sdk_error(ObjectStoreOperation::Put, key, false, error))?;
        let upload_id = required(
            started.upload_id(),
            ObjectStoreOperation::Put,
            key,
            "upload ID",
        )?
        .to_owned();
        let result = self
            .upload_file_and_complete(key, &full_key, &upload_id, path, condition)
            .await;
        if (result.is_err() || matches!(result, Ok(WriteOutcome::PreconditionFailed)))
            && let Err(abort_error) = self
                .client
                .abort_multipart_upload()
                .bucket(&self.bucket)
                .key(&full_key)
                .upload_id(&upload_id)
                .send()
                .await
        {
            tracing::warn!(key, error = %abort_error, "failed to abort multipart upload");
        }
        result
    }

    async fn upload_file_and_complete(
        &self,
        key: &str,
        full_key: &str,
        upload_id: &str,
        path: &Path,
        condition: WriteCondition,
    ) -> ObjectStoreResult<WriteOutcome> {
        let mut file = tokio::fs::File::open(path).await.map_err(|error| {
            ObjectStoreError::new(
                ObjectStoreOperation::Put,
                key,
                ObjectStoreErrorKind::Permanent,
                format!("opening {}: {error}", path.display()),
            )
        })?;
        let mut parts = Vec::new();
        let mut part_number = 1;
        loop {
            let mut buffer = vec![0; self.multipart_part_size];
            let mut filled = 0;
            while filled < buffer.len() {
                let read = file.read(&mut buffer[filled..]).await.map_err(|error| {
                    ObjectStoreError::new(
                        ObjectStoreOperation::Put,
                        key,
                        ObjectStoreErrorKind::Permanent,
                        format!("reading {}: {error}", path.display()),
                    )
                })?;
                if read == 0 {
                    break;
                }
                filled += read;
            }
            if filled == 0 {
                break;
            }
            buffer.truncate(filled);
            let response = self
                .client
                .upload_part()
                .bucket(&self.bucket)
                .key(full_key)
                .upload_id(upload_id)
                .part_number(part_number)
                .body(ByteStream::from(buffer))
                .send()
                .await
                .map_err(|error| map_sdk_error(ObjectStoreOperation::Put, key, true, error))?;
            let etag = required(
                response.e_tag(),
                ObjectStoreOperation::Put,
                key,
                "part ETag",
            )?;
            parts.push(
                CompletedPart::builder()
                    .part_number(part_number)
                    .e_tag(etag)
                    .build(),
            );
            part_number += 1;
        }
        self.complete_upload(key, full_key, upload_id, parts, condition)
            .await
    }

    async fn complete_upload(
        &self,
        key: &str,
        full_key: &str,
        upload_id: &str,
        parts: Vec<CompletedPart>,
        condition: WriteCondition,
    ) -> ObjectStoreResult<WriteOutcome> {
        let is_conditional = condition != WriteCondition::Unconditional;
        let upload = CompletedMultipartUpload::builder()
            .set_parts(Some(parts))
            .build();
        let mut request = self
            .client
            .complete_multipart_upload()
            .bucket(&self.bucket)
            .key(full_key)
            .upload_id(upload_id)
            .multipart_upload(upload);
        request = match condition {
            WriteCondition::Unconditional => request,
            WriteCondition::CreateOnly => request.if_none_match("*"),
            WriteCondition::ReplaceIfEtagMatches { etag } => request.if_match(etag),
        };
        let response = match request.send().await {
            Ok(response) => response,
            Err(error) if is_conditional && is_precondition_error(&error) => {
                return Ok(WriteOutcome::PreconditionFailed);
            }
            Err(error) => return Err(map_sdk_error(ObjectStoreOperation::Put, key, true, error)),
        };
        Ok(WriteOutcome::Written {
            etag: required(response.e_tag(), ObjectStoreOperation::Put, key, "ETag")?.to_owned(),
        })
    }

    async fn get_inner(
        &self,
        key: &str,
        range: Option<ByteRange>,
    ) -> ObjectStoreResult<Option<ObjectContent>> {
        let mut request = self
            .read_client
            .get_object()
            .bucket(&self.bucket)
            .key(self.full_key(key));
        if let Some(range) = range {
            if range.start > range.end_inclusive {
                return Err(ObjectStoreError::new(
                    ObjectStoreOperation::Get,
                    key,
                    ObjectStoreErrorKind::Permanent,
                    "range start exceeds end",
                ));
            }
            request = request.range(format!("bytes={}-{}", range.start, range.end_inclusive));
        }
        let response = match request.send().await {
            Ok(response) => response,
            Err(error) => {
                let status = error
                    .raw_response()
                    .map(|response| response.status().as_u16());
                if status == Some(416) && range.is_some() {
                    return Ok(self.head(key).await?.map(|metadata| ObjectContent {
                        bytes: Bytes::new(),
                        etag: metadata.etag,
                        object_size: metadata.size,
                    }));
                }
                let is_missing_key = status == Some(404)
                    && error
                        .as_service_error()
                        .and_then(ProvideErrorMetadata::code)
                        != Some("NoSuchBucket");
                let mapped = map_sdk_error(ObjectStoreOperation::Get, key, false, error);
                return if is_missing_key {
                    Ok(None)
                } else {
                    Err(mapped)
                };
            }
        };
        let etag = required(response.e_tag(), ObjectStoreOperation::Get, key, "ETag")?.to_owned();
        let object_size = response
            .content_range()
            .and_then(|value| value.rsplit_once('/'))
            .and_then(|(_, size)| size.parse::<u64>().ok())
            .or_else(|| {
                response
                    .content_length()
                    .and_then(|size| u64::try_from(size).ok())
            })
            .ok_or_else(|| {
                ObjectStoreError::new(
                    ObjectStoreOperation::Get,
                    key,
                    ObjectStoreErrorKind::Permanent,
                    "response omitted object size",
                )
            })?;
        let bytes = response
            .body
            .collect()
            .await
            .map_err(|error| {
                ObjectStoreError::new(
                    ObjectStoreOperation::Get,
                    key,
                    ObjectStoreErrorKind::Transient,
                    error.to_string(),
                )
            })?
            .into_bytes();
        Ok(Some(ObjectContent {
            bytes,
            etag,
            object_size,
        }))
    }
}

#[async_trait]
impl ObjectStore for S3ObjectStore {
    async fn put(
        &self,
        key: &str,
        body: Bytes,
        condition: WriteCondition,
    ) -> ObjectStoreResult<WriteOutcome> {
        if body.len() >= self.multipart_threshold {
            self.put_multipart(key, body, condition).await
        } else {
            self.put_single(key, body, condition).await
        }
    }

    async fn put_file(
        &self,
        key: &str,
        path: &Path,
        condition: WriteCondition,
    ) -> ObjectStoreResult<WriteOutcome> {
        let size = tokio::fs::metadata(path)
            .await
            .map_err(|error| {
                ObjectStoreError::new(
                    ObjectStoreOperation::Put,
                    key,
                    ObjectStoreErrorKind::Permanent,
                    format!("reading {}: {error}", path.display()),
                )
            })?
            .len();
        if size >= self.multipart_threshold as u64 {
            return self.put_multipart_file(key, path, condition).await;
        }
        let body = ByteStream::from_path(path).await.map_err(|error| {
            ObjectStoreError::new(
                ObjectStoreOperation::Put,
                key,
                ObjectStoreErrorKind::Permanent,
                format!("reading {}: {error}", path.display()),
            )
        })?;
        let is_conditional = condition != WriteCondition::Unconditional;
        let mut request = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(self.full_key(key))
            .body(body);
        request = match condition {
            WriteCondition::Unconditional => request,
            WriteCondition::CreateOnly => request.if_none_match("*"),
            WriteCondition::ReplaceIfEtagMatches { etag } => request.if_match(etag),
        };
        let response = match request.send().await {
            Ok(response) => response,
            Err(error) if is_conditional && is_precondition_error(&error) => {
                return Ok(WriteOutcome::PreconditionFailed);
            }
            Err(error) => return Err(map_sdk_error(ObjectStoreOperation::Put, key, true, error)),
        };
        Ok(WriteOutcome::Written {
            etag: required(response.e_tag(), ObjectStoreOperation::Put, key, "ETag")?.to_owned(),
        })
    }

    async fn get(&self, key: &str) -> ObjectStoreResult<Option<ObjectContent>> {
        self.get_inner(key, None).await
    }

    async fn get_range(
        &self,
        key: &str,
        range: ByteRange,
    ) -> ObjectStoreResult<Option<ObjectContent>> {
        self.get_inner(key, Some(range)).await
    }

    async fn get_to_file(&self, key: &str, path: &Path) -> ObjectStoreResult<Option<u64>> {
        let response = match self
            .read_client
            .get_object()
            .bucket(&self.bucket)
            .key(self.full_key(key))
            .send()
            .await
        {
            Ok(response) => response,
            Err(error) => {
                let status = error
                    .raw_response()
                    .map(|response| response.status().as_u16());
                let is_missing_key = status == Some(404)
                    && error
                        .as_service_error()
                        .and_then(ProvideErrorMetadata::code)
                        != Some("NoSuchBucket");
                let mapped = map_sdk_error(ObjectStoreOperation::Get, key, false, error);
                return if is_missing_key {
                    Ok(None)
                } else {
                    Err(mapped)
                };
            }
        };
        let size = u64::try_from(response.content_length().unwrap_or(-1)).map_err(|error| {
            ObjectStoreError::new(
                ObjectStoreOperation::Get,
                key,
                ObjectStoreErrorKind::Permanent,
                error.to_string(),
            )
        })?;
        let mut file = tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .await
            .map_err(|error| {
                ObjectStoreError::new(
                    ObjectStoreOperation::Get,
                    key,
                    ObjectStoreErrorKind::Permanent,
                    format!("creating {}: {error}", path.display()),
                )
            })?;
        let mut body = response.body;
        while let Some(chunk) = body.try_next().await.map_err(|error| {
            ObjectStoreError::new(
                ObjectStoreOperation::Get,
                key,
                ObjectStoreErrorKind::Transient,
                error.to_string(),
            )
        })? {
            file.write_all(&chunk).await.map_err(|error| {
                ObjectStoreError::new(
                    ObjectStoreOperation::Get,
                    key,
                    ObjectStoreErrorKind::Permanent,
                    format!("writing {}: {error}", path.display()),
                )
            })?;
        }
        file.flush().await.map_err(|error| {
            ObjectStoreError::new(
                ObjectStoreOperation::Get,
                key,
                ObjectStoreErrorKind::Permanent,
                format!("flushing {}: {error}", path.display()),
            )
        })?;
        Ok(Some(size))
    }

    async fn head(&self, key: &str) -> ObjectStoreResult<Option<ObjectMetadata>> {
        match self
            .read_client
            .head_object()
            .bucket(&self.bucket)
            .key(self.full_key(key))
            .send()
            .await
        {
            Ok(response) => Ok(Some(ObjectMetadata {
                key: key.to_owned(),
                size: u64::try_from(response.content_length().unwrap_or(-1)).map_err(|error| {
                    ObjectStoreError::new(
                        ObjectStoreOperation::Head,
                        key,
                        ObjectStoreErrorKind::Permanent,
                        error.to_string(),
                    )
                })?,
                etag: required(response.e_tag(), ObjectStoreOperation::Head, key, "ETag")?
                    .to_owned(),
            })),
            Err(error) => {
                let status = error
                    .raw_response()
                    .map(|response| response.status().as_u16());
                let mapped = map_sdk_error(ObjectStoreOperation::Head, key, false, error);
                if status != Some(404) {
                    return Err(mapped);
                }
                self.read_client
                    .head_bucket()
                    .bucket(&self.bucket)
                    .send()
                    .await
                    .map_err(|bucket_error| {
                        map_sdk_error(ObjectStoreOperation::Head, key, false, bucket_error)
                    })?;
                Ok(None)
            }
        }
    }

    async fn delete(&self, key: &str) -> ObjectStoreResult<()> {
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(self.full_key(key))
            .send()
            .await
            .map_err(|error| map_sdk_error(ObjectStoreOperation::Delete, key, true, error))?;
        Ok(())
    }

    async fn delete_many(&self, keys: &[String]) -> ObjectStoreResult<()> {
        if keys.len() > 1000 {
            return Err(ObjectStoreError::new(
                ObjectStoreOperation::Delete,
                "",
                ObjectStoreErrorKind::Permanent,
                "delete_many accepts at most 1000 keys",
            ));
        }
        for chunk in keys.chunks(1000) {
            let objects = chunk
                .iter()
                .map(|key| {
                    ObjectIdentifier::builder()
                        .key(self.full_key(key))
                        .build()
                        .map_err(|error| {
                            ObjectStoreError::new(
                                ObjectStoreOperation::Delete,
                                key,
                                ObjectStoreErrorKind::Permanent,
                                error.to_string(),
                            )
                        })
                })
                .collect::<ObjectStoreResult<Vec<_>>>()?;
            let delete = Delete::builder()
                .set_objects(Some(objects))
                .quiet(true)
                .build()
                .map_err(|error| {
                    ObjectStoreError::new(
                        ObjectStoreOperation::Delete,
                        "",
                        ObjectStoreErrorKind::Permanent,
                        error.to_string(),
                    )
                })?;
            let response = self
                .client
                .delete_objects()
                .bucket(&self.bucket)
                .delete(delete)
                .send()
                .await
                .map_err(|error| map_sdk_error(ObjectStoreOperation::Delete, "", true, error))?;
            if let Some(error) = response.errors().first() {
                let kind = match error.code().unwrap_or("") {
                    "SlowDown" | "Throttling" | "ThrottlingException" => {
                        ObjectStoreErrorKind::Transient
                    }
                    "InternalError" | "ServiceUnavailable" => ObjectStoreErrorKind::OutcomeUnknown,
                    _ => ObjectStoreErrorKind::Permanent,
                };
                return Err(ObjectStoreError::new(
                    ObjectStoreOperation::Delete,
                    error.key().unwrap_or(""),
                    kind,
                    error.message().unwrap_or("batch deletion failed"),
                ));
            }
        }
        Ok(())
    }

    async fn list(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        max_keys: u32,
    ) -> ObjectStoreResult<ObjectListing> {
        if !(1..=1000).contains(&max_keys) {
            return Err(ObjectStoreError::new(
                ObjectStoreOperation::List,
                prefix,
                ObjectStoreErrorKind::Permanent,
                "max_keys must be 1..=1000",
            ));
        }
        let mut request = self
            .read_client
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(self.full_key(prefix))
            .max_keys(max_keys as i32);
        if let Some(after) = start_after {
            request = request.start_after(self.full_key(after));
        }
        let response = request
            .send()
            .await
            .map_err(|error| map_sdk_error(ObjectStoreOperation::List, prefix, false, error))?;
        let objects = response
            .contents()
            .iter()
            .map(|object| {
                let full_key = required(
                    object.key(),
                    ObjectStoreOperation::List,
                    prefix,
                    "object key",
                )?;
                let key = full_key.strip_prefix(&self.key_prefix).ok_or_else(|| {
                    ObjectStoreError::new(
                        ObjectStoreOperation::List,
                        prefix,
                        ObjectStoreErrorKind::Permanent,
                        "listed key escaped configured prefix",
                    )
                })?;
                let size = u64::try_from(object.size().unwrap_or(-1)).map_err(|error| {
                    ObjectStoreError::new(
                        ObjectStoreOperation::List,
                        prefix,
                        ObjectStoreErrorKind::Permanent,
                        error.to_string(),
                    )
                })?;
                Ok(ObjectMetadata {
                    key: key.to_owned(),
                    size,
                    etag: required(object.e_tag(), ObjectStoreOperation::List, prefix, "ETag")?
                        .to_owned(),
                })
            })
            .collect::<ObjectStoreResult<Vec<_>>>()?;
        let next_start_after = if response.is_truncated().unwrap_or(false) {
            objects.last().map(|object| object.key.clone())
        } else {
            None
        };
        Ok(ObjectListing {
            objects,
            next_start_after,
        })
    }
}

fn required<'a>(
    value: Option<&'a str>,
    operation: ObjectStoreOperation,
    key: &str,
    field: &str,
) -> ObjectStoreResult<&'a str> {
    value.ok_or_else(|| {
        ObjectStoreError::new(
            operation,
            key,
            ObjectStoreErrorKind::Permanent,
            format!("response omitted {field}"),
        )
    })
}

fn is_precondition_error<E>(error: &SdkError<E>) -> bool {
    matches!(
        error
            .raw_response()
            .map(|response| response.status().as_u16()),
        Some(409 | 412)
    )
}

fn map_sdk_error<E>(
    operation: ObjectStoreOperation,
    key: &str,
    is_write: bool,
    error: SdkError<E>,
) -> ObjectStoreError
where
    E: ProvideErrorMetadata + std::fmt::Display,
{
    let status = error
        .raw_response()
        .map(|response| response.status().as_u16());
    let code = error
        .as_service_error()
        .and_then(ProvideErrorMetadata::code)
        .unwrap_or("");
    let kind = match &error {
        SdkError::ConstructionFailure(_) => ObjectStoreErrorKind::Permanent,
        SdkError::TimeoutError(_) | SdkError::ResponseError(_) => {
            if is_write {
                ObjectStoreErrorKind::OutcomeUnknown
            } else {
                ObjectStoreErrorKind::Transient
            }
        }
        SdkError::DispatchFailure(dispatch) if dispatch.is_user() => {
            ObjectStoreErrorKind::Permanent
        }
        SdkError::DispatchFailure(dispatch)
            if is_write
                && !dispatch
                    .as_connector_error()
                    .is_some_and(is_connect_failure) =>
        {
            ObjectStoreErrorKind::OutcomeUnknown
        }
        SdkError::DispatchFailure(_) => ObjectStoreErrorKind::Transient,
        SdkError::ServiceError(_)
            if status == Some(429)
                || matches!(code, "SlowDown" | "Throttling" | "ThrottlingException") =>
        {
            ObjectStoreErrorKind::Transient
        }
        SdkError::ServiceError(_) if matches!(status, Some(500..=599)) => {
            if is_write {
                ObjectStoreErrorKind::OutcomeUnknown
            } else {
                ObjectStoreErrorKind::Transient
            }
        }
        SdkError::ServiceError(_) => ObjectStoreErrorKind::Permanent,
        _ => ObjectStoreErrorKind::Permanent,
    };
    ObjectStoreError::new(
        operation,
        key,
        kind,
        format!("{error}; status={status:?}; code={code}"),
    )
}

fn is_connect_failure(dispatch: &aws_sdk_s3::error::ConnectorError) -> bool {
    let mut source: Option<&(dyn std::error::Error + 'static)> = Some(dispatch);
    while let Some(error) = source {
        if let Some(io_error) = error.downcast_ref::<std::io::Error>()
            && matches!(
                io_error.kind(),
                std::io::ErrorKind::ConnectionRefused
                    | std::io::ErrorKind::AddrNotAvailable
                    | std::io::ErrorKind::NotFound
            )
        {
            return true;
        }
        source = error.source();
    }
    false
}
