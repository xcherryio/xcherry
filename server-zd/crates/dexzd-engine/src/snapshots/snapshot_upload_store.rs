// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::future::Future;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use dexzd_object_store::{
    ByteRange, ObjectContent, ObjectListing, ObjectMetadata, ObjectStore, ObjectStoreError,
    ObjectStoreErrorKind, ObjectStoreOperation, ObjectStoreResult, WriteCondition, WriteOutcome,
};

use crate::commit::CommitState;

/// Object storage for one snapshot upload that abandons file transfers once the node stops.
///
/// The snapshot manifest write is the only `put`. From that write on, every operation runs to
/// completion.
pub(crate) struct SnapshotUploadStore {
    inner: Arc<dyn ObjectStore>,
    state: Arc<CommitState>,
    has_started_manifest_write: AtomicBool,
}

impl SnapshotUploadStore {
    pub(crate) fn new(inner: Arc<dyn ObjectStore>, state: Arc<CommitState>) -> Self {
        Self {
            inner,
            state,
            has_started_manifest_write: AtomicBool::new(false),
        }
    }

    /// Runs a file-phase request unless the node stops first.
    async fn run_file_request<T>(
        &self,
        operation: ObjectStoreOperation,
        key: &str,
        request: impl Future<Output = ObjectStoreResult<T>> + Send,
    ) -> ObjectStoreResult<T> {
        if self.has_started_manifest_write.load(Ordering::Acquire) {
            return request.await;
        }
        if self.state.stop_reason().is_some() {
            return Err(abandoned(operation, key));
        }
        tokio::select! {
            biased;
            _ = self.state.wait_stopped() => Err(abandoned(operation, key)),
            result = request => result,
        }
    }
}

#[async_trait]
impl ObjectStore for SnapshotUploadStore {
    async fn put(
        &self,
        key: &str,
        body: Bytes,
        condition: WriteCondition,
    ) -> ObjectStoreResult<WriteOutcome> {
        if !self.has_started_manifest_write.load(Ordering::Acquire)
            && self.state.stop_reason().is_some()
        {
            return Err(abandoned(ObjectStoreOperation::Put, key));
        }
        // A manifest without the node metadata update makes its snapshot number conflict forever.
        self.has_started_manifest_write
            .store(true, Ordering::Release);
        self.inner.put(key, body, condition).await
    }

    async fn put_file(
        &self,
        key: &str,
        path: &Path,
        condition: WriteCondition,
    ) -> ObjectStoreResult<WriteOutcome> {
        self.run_file_request(
            ObjectStoreOperation::Put,
            key,
            self.inner.put_file(key, path, condition),
        )
        .await
    }

    async fn get(&self, key: &str) -> ObjectStoreResult<Option<ObjectContent>> {
        self.inner.get(key).await
    }

    async fn get_range(
        &self,
        key: &str,
        range: ByteRange,
    ) -> ObjectStoreResult<Option<ObjectContent>> {
        self.inner.get_range(key, range).await
    }

    async fn get_to_file(&self, key: &str, path: &Path) -> ObjectStoreResult<Option<u64>> {
        self.run_file_request(
            ObjectStoreOperation::Get,
            key,
            self.inner.get_to_file(key, path),
        )
        .await
    }

    async fn head(&self, key: &str) -> ObjectStoreResult<Option<ObjectMetadata>> {
        self.run_file_request(ObjectStoreOperation::Head, key, self.inner.head(key))
            .await
    }

    async fn delete(&self, key: &str) -> ObjectStoreResult<()> {
        self.inner.delete(key).await
    }

    async fn delete_many(&self, keys: &[String]) -> ObjectStoreResult<()> {
        self.inner.delete_many(keys).await
    }

    async fn list(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        max_keys: u32,
    ) -> ObjectStoreResult<ObjectListing> {
        self.inner.list(prefix, start_after, max_keys).await
    }
}

fn abandoned(operation: ObjectStoreOperation, key: &str) -> ObjectStoreError {
    ObjectStoreError::new(
        operation,
        key,
        ObjectStoreErrorKind::Transient,
        "the node stopped serving, so the snapshot upload was abandoned",
    )
}
