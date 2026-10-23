// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;

use crate::{
    ByteRange, ObjectContent, ObjectListing, ObjectMetadata, ObjectStore, ObjectStoreError,
    ObjectStoreErrorKind, ObjectStoreOperation, ObjectStoreResult, WriteCondition, WriteOutcome,
};

/// An injected outcome for one in-memory object-store call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultAction {
    /// Run the operation normally.
    Proceed,
    /// Return a transient error without applying the operation.
    FailBeforeEffect,
    /// Apply the operation, then report an unknown outcome.
    ApplyThenOutcomeUnknown,
    /// Wait before running the operation. Tokio's paused clock controls the wait.
    Delay(Duration),
    /// Return modified bytes from a successful read, preserving the stored object.
    CorruptOnRead,
}

/// Selects an action for each call. The decision method may track its own sequence.
pub trait FaultInjector: Send + Sync {
    /// Returns the action for the operation and relative key.
    fn action(&self, operation: ObjectStoreOperation, key: &str) -> FaultAction;
}

#[derive(Clone)]
struct StoredObject {
    body: Bytes,
    etag: String,
}

#[derive(Default)]
struct State {
    objects: BTreeMap<String, StoredObject>,
    next_etag: u64,
    counts: BTreeMap<(String, String), u64>,
}

/// Ordered, fault-injecting object store for deterministic simulation.
#[derive(Default)]
pub struct InMemoryObjectStore {
    state: Mutex<State>,
    injector: Option<Arc<dyn FaultInjector>>,
}

impl InMemoryObjectStore {
    /// Creates an empty store without injected faults.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates an empty store whose calls consult `injector`.
    pub fn with_injector(injector: Arc<dyn FaultInjector>) -> Self {
        Self {
            state: Mutex::new(State::default()),
            injector: Some(injector),
        }
    }

    /// Returns the number of calls for an operation whose relative key starts with `prefix`.
    pub fn call_count(&self, operation: ObjectStoreOperation, prefix: &str) -> u64 {
        self.state
            .lock()
            .expect("in-memory object-store mutex is not poisoned")
            .counts
            .iter()
            .filter(|((name, key), _)| name == &operation.to_string() && key.starts_with(prefix))
            .map(|(_, count)| count)
            .sum()
    }

    async fn before_call(
        &self,
        operation: ObjectStoreOperation,
        key: &str,
    ) -> ObjectStoreResult<FaultAction> {
        {
            let mut state = self
                .state
                .lock()
                .expect("in-memory object-store mutex is not poisoned");
            *state
                .counts
                .entry((operation.to_string(), key.to_owned()))
                .or_default() += 1;
        }
        let action = self
            .injector
            .as_ref()
            .map_or(FaultAction::Proceed, |injector| {
                injector.action(operation, key)
            });
        match action {
            FaultAction::FailBeforeEffect => Err(ObjectStoreError::new(
                operation,
                key,
                ObjectStoreErrorKind::Transient,
                "injected failure before effect",
            )),
            FaultAction::Delay(duration) => {
                tokio::time::sleep(duration).await;
                Ok(FaultAction::Proceed)
            }
            other => Ok(other),
        }
    }

    fn after_call<T>(
        &self,
        action: FaultAction,
        operation: ObjectStoreOperation,
        key: &str,
        result: T,
    ) -> ObjectStoreResult<T> {
        if action == FaultAction::ApplyThenOutcomeUnknown {
            Err(ObjectStoreError::new(
                operation,
                key,
                ObjectStoreErrorKind::OutcomeUnknown,
                "injected failure after effect",
            ))
        } else {
            Ok(result)
        }
    }

    fn metadata(key: &str, object: &StoredObject) -> ObjectMetadata {
        ObjectMetadata {
            key: key.to_owned(),
            size: object.body.len() as u64,
            etag: object.etag.clone(),
        }
    }

    fn content(object: &StoredObject, bytes: Bytes, action: FaultAction) -> ObjectContent {
        let bytes = if action == FaultAction::CorruptOnRead && !bytes.is_empty() {
            let mut corrupted = bytes.to_vec();
            corrupted[0] ^= 1;
            Bytes::from(corrupted)
        } else {
            bytes
        };
        ObjectContent {
            bytes,
            etag: object.etag.clone(),
            object_size: object.body.len() as u64,
        }
    }
}

#[async_trait]
impl ObjectStore for InMemoryObjectStore {
    async fn put(
        &self,
        key: &str,
        body: Bytes,
        condition: WriteCondition,
    ) -> ObjectStoreResult<WriteOutcome> {
        let action = self.before_call(ObjectStoreOperation::Put, key).await?;
        let mut state = self
            .state
            .lock()
            .expect("in-memory object-store mutex is not poisoned");
        let current = state.objects.get(key);
        let matches = match condition {
            WriteCondition::Unconditional => true,
            WriteCondition::CreateOnly => current.is_none(),
            WriteCondition::ReplaceIfEtagMatches { etag } => {
                current.is_some_and(|object| object.etag == etag)
            }
        };
        if !matches {
            return Ok(WriteOutcome::PreconditionFailed);
        }
        state.next_etag += 1;
        let etag = format!("\"{:016x}\"", state.next_etag);
        state.objects.insert(
            key.to_owned(),
            StoredObject {
                body,
                etag: etag.clone(),
            },
        );
        self.after_call(
            action,
            ObjectStoreOperation::Put,
            key,
            WriteOutcome::Written { etag },
        )
    }

    async fn put_file(
        &self,
        key: &str,
        path: &Path,
        condition: WriteCondition,
    ) -> ObjectStoreResult<WriteOutcome> {
        let body = tokio::fs::read(path).await.map_err(|error| {
            ObjectStoreError::new(
                ObjectStoreOperation::Put,
                key,
                ObjectStoreErrorKind::Permanent,
                format!("reading {}: {error}", path.display()),
            )
        })?;
        self.put(key, Bytes::from(body), condition).await
    }

    async fn get(&self, key: &str) -> ObjectStoreResult<Option<ObjectContent>> {
        let action = self.before_call(ObjectStoreOperation::Get, key).await?;
        let state = self
            .state
            .lock()
            .expect("in-memory object-store mutex is not poisoned");
        let result = state
            .objects
            .get(key)
            .map(|object| Self::content(object, object.body.clone(), action));
        self.after_call(action, ObjectStoreOperation::Get, key, result)
    }

    async fn get_range(
        &self,
        key: &str,
        range: ByteRange,
    ) -> ObjectStoreResult<Option<ObjectContent>> {
        if range.start > range.end_inclusive {
            return Err(ObjectStoreError::new(
                ObjectStoreOperation::Get,
                key,
                ObjectStoreErrorKind::Permanent,
                "range start exceeds end",
            ));
        }
        let action = self.before_call(ObjectStoreOperation::Get, key).await?;
        let state = self
            .state
            .lock()
            .expect("in-memory object-store mutex is not poisoned");
        let result = state.objects.get(key).map(|object| {
            let start = usize::try_from(range.start)
                .unwrap_or(usize::MAX)
                .min(object.body.len());
            let end = usize::try_from(range.end_inclusive)
                .unwrap_or(usize::MAX)
                .saturating_add(1)
                .min(object.body.len());
            Self::content(object, object.body.slice(start..end.max(start)), action)
        });
        self.after_call(action, ObjectStoreOperation::Get, key, result)
    }

    async fn get_to_file(&self, key: &str, path: &Path) -> ObjectStoreResult<Option<u64>> {
        let Some(content) = self.get(key).await? else {
            return Ok(None);
        };
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
        use tokio::io::AsyncWriteExt;
        file.write_all(&content.bytes).await.map_err(|error| {
            ObjectStoreError::new(
                ObjectStoreOperation::Get,
                key,
                ObjectStoreErrorKind::Permanent,
                format!("writing {}: {error}", path.display()),
            )
        })?;
        Ok(Some(content.object_size))
    }

    async fn head(&self, key: &str) -> ObjectStoreResult<Option<ObjectMetadata>> {
        let action = self.before_call(ObjectStoreOperation::Head, key).await?;
        let state = self
            .state
            .lock()
            .expect("in-memory object-store mutex is not poisoned");
        let result = state
            .objects
            .get(key)
            .map(|object| Self::metadata(key, object));
        self.after_call(action, ObjectStoreOperation::Head, key, result)
    }

    async fn delete(&self, key: &str) -> ObjectStoreResult<()> {
        let action = self.before_call(ObjectStoreOperation::Delete, key).await?;
        self.state
            .lock()
            .expect("in-memory object-store mutex is not poisoned")
            .objects
            .remove(key);
        self.after_call(action, ObjectStoreOperation::Delete, key, ())
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
        let action = self.before_call(ObjectStoreOperation::Delete, "").await?;
        let mut state = self
            .state
            .lock()
            .expect("in-memory object-store mutex is not poisoned");
        for key in keys {
            state.objects.remove(key);
        }
        drop(state);
        self.after_call(action, ObjectStoreOperation::Delete, "", ())
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
        let action = self.before_call(ObjectStoreOperation::List, prefix).await?;
        let state = self
            .state
            .lock()
            .expect("in-memory object-store mutex is not poisoned");
        let mut matching = state.objects.iter().filter(|(key, _)| {
            key.starts_with(prefix) && start_after.is_none_or(|after| key.as_str() > after)
        });
        let objects: Vec<_> = matching
            .by_ref()
            .take(max_keys as usize)
            .map(|(key, object)| Self::metadata(key, object))
            .collect();
        let next_start_after = if matching.next().is_some() {
            objects.last().map(|object| object.key.clone())
        } else {
            None
        };
        self.after_call(
            action,
            ObjectStoreOperation::List,
            prefix,
            ObjectListing {
                objects,
                next_start_after,
            },
        )
    }
}
