// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use dexzd_metrics::{NodeMetrics, ObjectOperation, classify_object_kind};

use crate::{
    ByteRange, ObjectContent, ObjectListing, ObjectMetadata, ObjectStore, ObjectStoreResult,
    WriteCondition, WriteOutcome,
};

/// Counts logical object storage requests after SDK retries have finished.
pub struct InstrumentedObjectStore {
    inner: Arc<dyn ObjectStore>,
    metrics: NodeMetrics,
}

impl InstrumentedObjectStore {
    /// Wraps one store without changing its object semantics.
    pub fn new(inner: Arc<dyn ObjectStore>, metrics: NodeMetrics) -> Self {
        Self { inner, metrics }
    }

    fn record<T>(&self, operation: ObjectOperation, key: &str, result: &ObjectStoreResult<T>) {
        self.metrics
            .object_requests
            .record(operation, classify_object_kind(key));
        if result.is_err() {
            self.metrics.record_object_error(operation);
        }
    }
}

#[async_trait]
impl ObjectStore for InstrumentedObjectStore {
    async fn put(
        &self,
        key: &str,
        body: Bytes,
        condition: WriteCondition,
    ) -> ObjectStoreResult<WriteOutcome> {
        let bytes = body.len() as u64;
        let result = self.inner.put(key, body, condition).await;
        self.record(ObjectOperation::Put, key, &result);
        if matches!(result, Ok(WriteOutcome::Written { .. })) {
            self.metrics.object_put_bytes.inc_by(bytes);
        }
        result
    }

    async fn put_file(
        &self,
        key: &str,
        path: &Path,
        condition: WriteCondition,
    ) -> ObjectStoreResult<WriteOutcome> {
        let result = self.inner.put_file(key, path, condition).await;
        self.record(ObjectOperation::Put, key, &result);
        if matches!(result, Ok(WriteOutcome::Written { .. })) {
            match tokio::fs::metadata(path).await {
                Ok(metadata) => {
                    self.metrics.object_put_bytes.inc_by(metadata.len());
                }
                Err(error) => tracing::warn!(%error, "uploaded file size could not be sampled"),
            }
        }
        result
    }

    async fn get(&self, key: &str) -> ObjectStoreResult<Option<ObjectContent>> {
        let result = self.inner.get(key).await;
        self.record(ObjectOperation::Get, key, &result);
        result
    }

    async fn get_range(
        &self,
        key: &str,
        range: ByteRange,
    ) -> ObjectStoreResult<Option<ObjectContent>> {
        let result = self.inner.get_range(key, range).await;
        self.record(ObjectOperation::Get, key, &result);
        result
    }

    async fn get_to_file(&self, key: &str, path: &Path) -> ObjectStoreResult<Option<u64>> {
        let result = self.inner.get_to_file(key, path).await;
        self.record(ObjectOperation::Get, key, &result);
        result
    }

    async fn head(&self, key: &str) -> ObjectStoreResult<Option<ObjectMetadata>> {
        let result = self.inner.head(key).await;
        self.record(ObjectOperation::Get, key, &result);
        result
    }

    async fn delete(&self, key: &str) -> ObjectStoreResult<()> {
        let result = self.inner.delete(key).await;
        self.record(ObjectOperation::Delete, key, &result);
        result
    }

    async fn delete_many(&self, keys: &[String]) -> ObjectStoreResult<()> {
        let result = self.inner.delete_many(keys).await;
        self.record(
            ObjectOperation::Delete,
            keys.first().map(String::as_str).unwrap_or(""),
            &result,
        );
        result
    }

    async fn list(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        max_keys: u32,
    ) -> ObjectStoreResult<ObjectListing> {
        let result = self.inner.list(prefix, start_after, max_keys).await;
        self.record(ObjectOperation::List, prefix, &result);
        result
    }
}
