// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Persistent, bounded local cache for Dex blob payloads.
//!
//! [`BlobCache`] stores immutable payloads by blob ID and uses an admission and eviction policy to
//! keep committed files within the configured byte budget.

#![deny(missing_docs)]

mod config;
mod entry;
mod error;
mod format;
mod policy;
mod store;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};

pub use config::BlobCacheConfig;
use config::validate_blob_id;
use entry::DiskEntry;
pub use error::BlobCacheError;
use format::{FileMetadata, calculate_metadata};
use policy::{DiskPolicyCallback, PolicyCache, new_policy};
use store::LocalFileStore;

/// Stores immutable Dex blob payloads in a bounded local directory.
///
/// A cache is thread-safe. Reads and writes may run concurrently, while deletion and close
/// coordinate with in-flight operations. [`BlobCache::open`] recovers valid committed entries
/// after a process restart and removes interrupted writes. Writes are not synchronized to disk,
/// so a crash may leave a missing or corrupt entry that callers must fetch again.
///
/// # Examples
///
/// ```no_run
/// use dex_blob_cache::{BlobCache, BlobCacheConfig};
///
/// let config = BlobCacheConfig::new("/tmp/dex-blobs", 64 * 1024 * 1024, 0)?;
/// let cache = BlobCache::open(config)?;
/// assert!(cache.put("orders/123", b"payload")?);
/// assert_eq!(cache.get("orders/123")?, Some(b"payload".to_vec()));
/// cache.close()?;
/// # Ok::<(), dex_blob_cache::BlobCacheError>(())
/// ```
pub struct BlobCache {
    config: BlobCacheConfig,
    store: LocalFileStore,
    policy: RwLock<Option<PolicyCache>>,
    lifecycle: RwLock<()>,
    commit: Mutex<()>,
    callback: DiskPolicyCallback,
    closed: AtomicBool,
}

enum ExistingEntry {
    Missing,
    Reused,
}

impl BlobCache {
    /// Opens a cache and recovers its committed entries.
    ///
    /// `config` supplies the owned directory, byte budget, and admission-policy sizing. The call
    /// creates the directory when needed and returns only after recovery completes.
    ///
    /// # Errors
    ///
    /// Returns [`BlobCacheError`] when the directory cannot be prepared, recovery finds an
    /// unrecoverable storage failure, or the admission policy cannot be initialized.
    pub fn open(config: BlobCacheConfig) -> Result<Self, BlobCacheError> {
        let store = LocalFileStore::new(config.directory())?;
        store.prepare()?;
        let callback = DiskPolicyCallback::new(store.clone());
        let policy = new_policy(&config, callback.clone())?;
        let cache = Self {
            config,
            store,
            policy: RwLock::new(Some(policy)),
            lifecycle: RwLock::new(()),
            commit: Mutex::new(()),
            callback,
            closed: AtomicBool::new(false),
        };
        if let Err(error) = cache.recover() {
            cache.close_policy_preserving_files();
            return Err(error);
        }
        Ok(cache)
    }

    /// Reads one payload and records an admission-policy access.
    ///
    /// Returns `Ok(Some(payload))` for a valid entry and `Ok(None)` when the ID is absent or its
    /// file disappeared or became corrupt. Corrupt entries are invalidated before returning.
    ///
    /// # Errors
    ///
    /// Returns [`BlobCacheError::InvalidBlob`] for an invalid ID, [`BlobCacheError::Closed`] after
    /// close, or a storage/policy error that cannot be treated as a cache miss.
    pub fn get(&self, blob_id: &str) -> Result<Option<Vec<u8>>, BlobCacheError> {
        validate_blob_id(blob_id)?;
        let _lifecycle = self
            .lifecycle
            .read()
            .expect("blob cache lifecycle lock is poisoned");
        self.require_open()?;

        let entry = {
            let policy = self
                .policy
                .read()
                .expect("blob cache policy lock is poisoned");
            let policy = policy.as_ref().ok_or(BlobCacheError::Closed)?;
            policy.get(blob_id).map(|entry| Arc::clone(entry.value()))
        };
        let Some(entry) = entry else {
            return Ok(None);
        };
        let Some(lease) = entry.acquire_read() else {
            return Ok(None);
        };
        let result = self.store.read(&entry);
        drop(lease);
        match result {
            Ok(payload) => Ok(Some(payload)),
            Err(error) if error.is_missing_or_corrupt() => {
                self.invalidate_entry(&entry)?;
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }

    /// Attempts to admit an immutable payload under `blob_id`.
    ///
    /// Returns `Ok(true)` when the payload is committed or the identical payload already exists.
    /// Returns `Ok(false)` when the payload exceeds the byte budget or the policy rejects it.
    /// Reusing an ID for different bytes is an error.
    ///
    /// # Errors
    ///
    /// Returns [`BlobCacheError`] for invalid IDs, content mismatches, closed lifecycle, or failed
    /// filesystem and policy operations.
    pub fn put(&self, blob_id: &str, payload: &[u8]) -> Result<bool, BlobCacheError> {
        validate_blob_id(blob_id)?;
        let _lifecycle = self
            .lifecycle
            .read()
            .expect("blob cache lifecycle lock is poisoned");
        self.require_open()?;
        let _commit = self
            .commit
            .lock()
            .expect("blob cache commit lock is poisoned");
        self.callback.retry_cleanup()?;

        let metadata = calculate_metadata(blob_id, payload, self.store.path_for(blob_id))?;
        if metadata.size > self.config.max_bytes() {
            return Ok(false);
        }
        if matches!(
            self.reuse_existing(&metadata, payload)?,
            ExistingEntry::Reused
        ) {
            return Ok(true);
        }

        let entry = Arc::new(DiskEntry::pending(metadata));
        self.callback.reset_error();
        let inserted = self.with_policy(|policy| {
            Ok(policy.insert(blob_id.to_owned(), Arc::clone(&entry), entry.metadata.size))
        })?;
        if !inserted {
            entry.begin_eviction();
            return Ok(false);
        }
        self.wait_for_policy()?;
        if let Some(error) = self.callback.take_error() {
            self.remove_candidate(&entry)?;
            return Err(BlobCacheError::Reconciliation(error));
        }
        if !self.policy_contains(blob_id, &entry)? {
            entry.begin_eviction();
            return Ok(false);
        }

        if let Err(failure) = self.store.commit(&entry.metadata, payload) {
            if let Some(path) = failure.orphan_path {
                self.callback.add_cleanup_path(&path);
            }
            self.remove_candidate(&entry)?;
            return Err(failure.error);
        }
        if !entry.mark_ready() {
            return Err(BlobCacheError::Reconciliation(
                "admitted entry left pending state".to_owned(),
            ));
        }
        Ok(true)
    }

    /// Deletes one blob if present.
    ///
    /// Missing IDs succeed, making deletion idempotent.
    ///
    /// # Errors
    ///
    /// Returns [`BlobCacheError`] for an invalid ID, a closed cache, or a failed storage/policy
    /// operation.
    pub fn delete(&self, blob_id: &str) -> Result<(), BlobCacheError> {
        validate_blob_id(blob_id)?;
        let _lifecycle = self
            .lifecycle
            .read()
            .expect("blob cache lifecycle lock is poisoned");
        self.require_open()?;
        let _commit = self
            .commit
            .lock()
            .expect("blob cache commit lock is poisoned");
        self.callback.retry_cleanup()?;

        let entry = self.policy_entry(blob_id)?;
        let Some(entry) = entry else {
            let path = self.store.path_for(blob_id);
            if let Err(error) = self.store.remove(&path) {
                self.callback.add_cleanup_path(&path);
                return Err(error);
            }
            return Ok(());
        };
        entry.begin_eviction();
        if let Err(error) = self.store.remove(&entry.metadata.path) {
            entry.restore_ready();
            return Err(error);
        }
        self.with_policy(|policy| {
            policy.remove(&blob_id.to_owned());
            Ok(())
        })?;
        self.wait_for_policy()
    }

    /// Removes every cache entry while keeping the cache open.
    ///
    /// The call excludes concurrent cache operations until both policy and disk state are cleared.
    ///
    /// # Errors
    ///
    /// Returns [`BlobCacheError::Closed`] after close or a reconciliation/storage failure when the
    /// cache cannot fully purge its state.
    pub fn delete_all(&self) -> Result<(), BlobCacheError> {
        let _lifecycle = self
            .lifecycle
            .write()
            .expect("blob cache lifecycle lock is poisoned");
        self.require_open()?;
        let _commit = self
            .commit
            .lock()
            .expect("blob cache commit lock is poisoned");

        self.callback.reset_error();
        self.with_policy(|policy| {
            policy
                .clear()
                .map_err(|error| BlobCacheError::Policy(error.to_string()))
        })?;
        self.wait_for_policy()?;
        let callback_error = self.callback.take_error();
        if let Err(error) = self.store.purge() {
            self.callback.require_purge();
            return Err(match callback_error {
                Some(callback_error) => BlobCacheError::Reconciliation(format!(
                    "{callback_error}; purge blob cache: {error}"
                )),
                None => error,
            });
        }
        self.callback.reset_after_purge();
        Ok(())
    }

    /// Releases policy resources and rejects future operations.
    ///
    /// Close is idempotent and preserves committed files for the next [`BlobCache::open`].
    ///
    /// # Errors
    ///
    /// Returns a cleanup error after closing when a previously deferred file removal still fails.
    pub fn close(&self) -> Result<(), BlobCacheError> {
        let _lifecycle = self
            .lifecycle
            .write()
            .expect("blob cache lifecycle lock is poisoned");
        if self.closed.load(Ordering::Acquire) {
            return Ok(());
        }
        let _commit = self
            .commit
            .lock()
            .expect("blob cache commit lock is poisoned");
        let cleanup_result = self.callback.retry_cleanup();
        self.close_policy_preserving_files();
        cleanup_result
    }

    fn recover(&self) -> Result<(), BlobCacheError> {
        let _commit = self
            .commit
            .lock()
            .expect("blob cache commit lock is poisoned");
        self.store.purge_temp().map_err(|error| {
            BlobCacheError::Reconciliation(format!("purge interrupted writes: {error}"))
        })?;
        let scan = self.store.scan().map_err(|error| {
            BlobCacheError::Reconciliation(format!("scan committed files: {error}"))
        })?;
        for path in scan.invalid_paths {
            self.store.remove(&path).map_err(|error| {
                BlobCacheError::Reconciliation(format!("remove invalid file: {error}"))
            })?;
        }
        for metadata in scan.entries {
            self.recover_entry(metadata)?;
        }
        Ok(())
    }

    fn recover_entry(&self, metadata: FileMetadata) -> Result<(), BlobCacheError> {
        if metadata.size > self.config.max_bytes() {
            return self.store.remove(&metadata.path).map_err(|error| {
                BlobCacheError::Reconciliation(format!("remove oversized file: {error}"))
            });
        }

        let entry = Arc::new(DiskEntry::ready(metadata));
        self.callback.reset_error();
        let inserted = self.with_policy(|policy| {
            Ok(policy.insert(
                entry.metadata.blob_id.clone(),
                Arc::clone(&entry),
                entry.metadata.size,
            ))
        })?;
        if !inserted {
            entry.begin_eviction();
            return self.store.remove(&entry.metadata.path).map_err(|error| {
                BlobCacheError::Reconciliation(format!("remove rejected file: {error}"))
            });
        }
        self.wait_for_policy()?;
        if let Some(error) = self.callback.take_error() {
            return Err(BlobCacheError::Reconciliation(error));
        }
        if self.policy_contains(&entry.metadata.blob_id, &entry)? {
            return Ok(());
        }
        self.store.remove(&entry.metadata.path).map_err(|error| {
            BlobCacheError::Reconciliation(format!("remove dropped file: {error}"))
        })
    }

    fn reuse_existing(
        &self,
        metadata: &FileMetadata,
        payload: &[u8],
    ) -> Result<ExistingEntry, BlobCacheError> {
        let Some(entry) = self.policy_entry(&metadata.blob_id)? else {
            return Ok(ExistingEntry::Missing);
        };
        let Some(lease) = entry.acquire_read() else {
            return Ok(ExistingEntry::Missing);
        };
        let result = self.store.read(&entry);
        drop(lease);
        match result {
            Ok(existing) => {
                if entry.metadata.size != metadata.size
                    || entry.metadata.checksum != metadata.checksum
                    || existing != payload
                {
                    return Err(BlobCacheError::ContentMismatch(metadata.blob_id.clone()));
                }
                Ok(ExistingEntry::Reused)
            }
            Err(error) if error.is_missing_or_corrupt() => {
                entry.begin_eviction();
                if let Err(remove_error) = self.store.remove(&entry.metadata.path) {
                    self.callback.add_cleanup_path(&entry.metadata.path);
                    self.remove_policy_key(&metadata.blob_id)?;
                    return Err(remove_error);
                }
                self.remove_policy_key(&metadata.blob_id)?;
                Ok(ExistingEntry::Missing)
            }
            Err(error) => Err(error),
        }
    }

    fn invalidate_entry(&self, entry: &Arc<DiskEntry>) -> Result<(), BlobCacheError> {
        let _commit = self
            .commit
            .lock()
            .expect("blob cache commit lock is poisoned");
        let current = self.policy_entry(&entry.metadata.blob_id)?;
        if !current
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(current, entry))
        {
            return Ok(());
        }
        entry.begin_eviction();
        let remove_result = self.store.remove(&entry.metadata.path);
        self.remove_policy_key(&entry.metadata.blob_id)?;
        if remove_result.is_err() {
            self.callback.add_cleanup_path(&entry.metadata.path);
        }
        remove_result
    }

    fn remove_candidate(&self, entry: &Arc<DiskEntry>) -> Result<(), BlobCacheError> {
        if self.policy_contains(&entry.metadata.blob_id, entry)? {
            self.remove_policy_key(&entry.metadata.blob_id)?;
        }
        entry.begin_eviction();
        Ok(())
    }

    fn remove_policy_key(&self, blob_id: &str) -> Result<(), BlobCacheError> {
        self.with_policy(|policy| {
            policy.remove(&blob_id.to_owned());
            Ok(())
        })?;
        self.wait_for_policy()
    }

    fn policy_entry(&self, blob_id: &str) -> Result<Option<Arc<DiskEntry>>, BlobCacheError> {
        self.with_policy(|policy| Ok(policy.get(blob_id).map(|entry| Arc::clone(entry.value()))))
    }

    fn policy_contains(
        &self,
        blob_id: &str,
        expected: &Arc<DiskEntry>,
    ) -> Result<bool, BlobCacheError> {
        self.with_policy(|policy| {
            Ok(policy
                .get(blob_id)
                .is_some_and(|current| Arc::ptr_eq(current.value(), expected)))
        })
    }

    fn wait_for_policy(&self) -> Result<(), BlobCacheError> {
        self.with_policy(|policy| {
            policy
                .wait()
                .map_err(|error| BlobCacheError::Policy(error.to_string()))
        })
    }

    fn with_policy<T>(
        &self,
        operation: impl FnOnce(&PolicyCache) -> Result<T, BlobCacheError>,
    ) -> Result<T, BlobCacheError> {
        let policy = self
            .policy
            .read()
            .expect("blob cache policy lock is poisoned");
        operation(policy.as_ref().ok_or(BlobCacheError::Closed)?)
    }

    fn require_open(&self) -> Result<(), BlobCacheError> {
        if self.closed.load(Ordering::Acquire) {
            Err(BlobCacheError::Closed)
        } else {
            Ok(())
        }
    }

    fn close_policy_preserving_files(&self) {
        self.callback.set_closing();
        self.closed.store(true, Ordering::Release);
        let policy = self
            .policy
            .write()
            .expect("blob cache policy lock is poisoned")
            .take();
        drop(policy);
    }
}

impl Drop for BlobCache {
    fn drop(&mut self) {
        self.callback.set_closing();
    }
}
