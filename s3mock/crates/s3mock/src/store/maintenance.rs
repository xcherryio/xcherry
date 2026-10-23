// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Startup cleanup of temporary files and of data files that no committed metadata references.

use std::fs;
use std::io;
use std::path::Path;
use std::str::FromStr;

use redb::ReadableDatabase;
use ulid::Ulid;

use super::{BUCKETS, Store, VERSIONS, decode_json, load_versions, relative_data_file};
use crate::data_file;
use crate::error::StoreError;
use crate::record::BucketRecord;

/// Counts reported by [`Store::collect_orphan_data_files`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct OrphanCollection {
    pub examined_files: u64,
    pub removed_files: u64,
}

impl Store {
    /// Removes uploads that an earlier process never finished; the database lock rules out live writers.
    pub(super) fn remove_temporary_files(&self) -> Result<(), StoreError> {
        for entry in fs::read_dir(&self.temporary_directory)? {
            let path = entry?.path();
            if let Err(error) = fs::remove_file(&path) {
                tracing::warn!(path = %path.display(), %error, "failed to remove leftover temporary file");
            }
        }
        Ok(())
    }

    /// Removes data files left by writes that crashed or failed before their metadata commit.
    ///
    /// Only files older than this store's open time are examined, so writes in flight are never touched.
    /// Safe to run on a background thread while the store serves requests.
    pub fn collect_orphan_data_files(&self) -> Result<OrphanCollection, StoreError> {
        let mut collection = OrphanCollection::default();
        let buckets: Vec<BucketRecord> = {
            let transaction = self.database.begin_read()?;
            let table = transaction.open_table(BUCKETS)?;
            let mut buckets = Vec::new();
            for entry in table.range::<&str>(..)? {
                let (_, value) = entry?;
                buckets.push(decode_json(value.value())?);
            }
            buckets
        };
        let live_incarnations: Vec<&str> = buckets
            .iter()
            .map(|bucket| bucket.incarnation.as_str())
            .collect();
        for entry in fs::read_dir(&self.data_directory)? {
            let directory = entry?.path();
            let name = directory
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or_default()
                .to_owned();
            if live_incarnations.contains(&name.as_str()) {
                continue;
            }
            if self.is_older_than_open(&name) {
                remove_directory(&directory);
            }
        }
        for bucket in &buckets {
            self.collect_bucket_orphans(bucket, &mut collection)?;
        }
        Ok(collection)
    }

    fn collect_bucket_orphans(
        &self,
        bucket: &BucketRecord,
        collection: &mut OrphanCollection,
    ) -> Result<(), StoreError> {
        let bucket_directory = self.bucket_directory(bucket);
        let shards = match fs::read_dir(&bucket_directory) {
            Ok(shards) => shards,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error.into()),
        };
        for shard in shards {
            for file in fs::read_dir(shard?.path())? {
                let path = file?.path();
                let name = path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .unwrap_or_default()
                    .to_owned();
                if !self.is_older_than_open(&name) {
                    continue;
                }
                collection.examined_files += 1;
                if !self.is_referenced(bucket, &name, &path)? {
                    match fs::remove_file(&path) {
                        Ok(()) => collection.removed_files += 1,
                        Err(error) => {
                            tracing::warn!(path = %path.display(), %error, "failed to remove orphan data file");
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// A data file is live when a version of its header's key points back at this file.
    fn is_referenced(
        &self,
        bucket: &BucketRecord,
        file_name: &str,
        path: &Path,
    ) -> Result<bool, StoreError> {
        let header = match data_file::read_header(path) {
            Ok(header) => header,
            Err(StoreError::CorruptDataFile { reason, .. }) => {
                tracing::warn!(path = %path.display(), %reason, "treating unreadable data file as an orphan");
                return Ok(false);
            }
            Err(error) => return Err(error),
        };
        let transaction = self.database.begin_read()?;
        let versions = transaction.open_table(VERSIONS)?;
        let expected_relative_path =
            relative_data_file(Ulid::from_str(file_name).expect("checked by is_older_than_open"));
        Ok(load_versions(&versions, &bucket.name, &header.key)?
            .iter()
            .any(|stored| {
                stored.record.data_file.as_deref() == Some(expected_relative_path.as_str())
            }))
    }

    fn is_older_than_open(&self, name: &str) -> bool {
        Ulid::from_str(name).is_ok_and(|ulid| ulid.timestamp_ms() < self.opened_at_unix_ms)
    }
}

fn remove_directory(directory: &Path) {
    if let Err(error) = fs::remove_dir_all(directory) {
        tracing::warn!(path = %directory.display(), %error, "failed to remove orphan bucket directory");
    }
}
