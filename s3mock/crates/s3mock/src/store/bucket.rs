// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Bucket lifecycle and versioning configuration.

use std::fs;
use std::net::Ipv4Addr;

use redb::{ReadableDatabase, ReadableTable};

use super::{BUCKETS, PARTS, Store, UPLOADS, VERSIONS, decode_json, encode_json, require_bucket};
use crate::error::StoreError;
use crate::key_codec::{encode_bucket_prefix, prefix_successor};
use crate::record::{
    BucketRecord, LifecycleConfiguration, ObjectLockConfiguration, VersioningState,
};

impl Store {
    /// Creates an empty bucket; Object Lock also enables versioning, as in S3.
    pub fn create_bucket(
        &self,
        bucket: &str,
        is_object_lock_enabled: bool,
    ) -> Result<BucketRecord, StoreError> {
        validate_bucket_name(bucket)?;
        let incarnation = self.next_internal_version();
        let record = BucketRecord {
            name: bucket.to_owned(),
            incarnation: incarnation.to_string(),
            created_unix_ms: i64::try_from(incarnation.timestamp_ms())
                .expect("ULID timestamps fit in i64"),
            versioning: if is_object_lock_enabled {
                VersioningState::Enabled
            } else {
                VersioningState::Unversioned
            },
            object_lock: is_object_lock_enabled.then(ObjectLockConfiguration::default),
            lifecycle: None,
        };
        let transaction = self.begin_write()?;
        {
            let mut buckets = transaction.open_table(BUCKETS)?;
            if buckets.get(bucket)?.is_some() {
                return Err(StoreError::BucketAlreadyOwnedByYou(bucket.to_owned()));
            }
            buckets.insert(bucket, encode_json(&record)?.as_slice())?;
        }
        transaction.commit()?;
        Ok(record)
    }

    /// Deletes a bucket that holds no versions or delete markers.
    pub fn delete_bucket(&self, bucket: &str) -> Result<(), StoreError> {
        let transaction = self.begin_write()?;
        let record = {
            let mut buckets = transaction.open_table(BUCKETS)?;
            let record = require_bucket(&buckets, bucket)?;
            let versions = transaction.open_table(VERSIONS)?;
            let bucket_prefix = encode_bucket_prefix(bucket);
            let end = prefix_successor(&bucket_prefix)
                .expect("encoded bucket prefixes end with a terminator below 0xFF");
            if versions
                .range(bucket_prefix.as_slice()..end.as_slice())?
                .next()
                .is_some()
            {
                return Err(StoreError::BucketNotEmpty(bucket.to_owned()));
            }
            buckets.remove(bucket)?;
            // In-progress multipart uploads end with the bucket; their files go with its directory.
            for table in [UPLOADS, PARTS] {
                let mut uploads_or_parts = transaction.open_table(table)?;
                let mut keys = Vec::new();
                for entry in uploads_or_parts.range(bucket_prefix.as_slice()..end.as_slice())? {
                    keys.push(entry?.0.value().to_vec());
                }
                for entry_key in keys {
                    uploads_or_parts.remove(entry_key.as_slice())?;
                }
            }
            record
        };
        transaction.commit()?;
        let directory = self.bucket_directory(&record);
        match fs::remove_dir_all(&directory) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                tracing::warn!(path = %directory.display(), %error, "failed to remove deleted bucket directory");
            }
        }
        Ok(())
    }

    /// Returns the bucket record, or `NoSuchBucket`.
    pub fn bucket(&self, bucket: &str) -> Result<BucketRecord, StoreError> {
        let transaction = self.database.begin_read()?;
        require_bucket(&transaction.open_table(BUCKETS)?, bucket)
    }

    /// Lists every bucket in name order.
    pub fn list_buckets(&self) -> Result<Vec<BucketRecord>, StoreError> {
        let transaction = self.database.begin_read()?;
        let buckets = transaction.open_table(BUCKETS)?;
        let mut records = Vec::new();
        for entry in buckets.range::<&str>(..)? {
            let (_, value) = entry?;
            records.push(decode_json(value.value())?);
        }
        Ok(records)
    }

    /// Enables or suspends versioning; a versioned bucket can never return to unversioned.
    pub fn put_bucket_versioning(
        &self,
        bucket: &str,
        versioning: VersioningState,
    ) -> Result<(), StoreError> {
        if versioning == VersioningState::Unversioned {
            return Err(StoreError::InvalidRequest(
                "versioning status must be Enabled or Suspended".to_owned(),
            ));
        }
        let transaction = self.begin_write()?;
        {
            let mut buckets = transaction.open_table(BUCKETS)?;
            let mut record = require_bucket(&buckets, bucket)?;
            if record.object_lock.is_some() && versioning != VersioningState::Enabled {
                return Err(StoreError::InvalidBucketState(
                    "versioning cannot be suspended on an Object Lock bucket".to_owned(),
                ));
            }
            record.versioning = versioning;
            buckets.insert(bucket, encode_json(&record)?.as_slice())?;
        }
        transaction.commit()?;
        Ok(())
    }

    /// Stores an Object Lock configuration; the bucket must have versioning enabled.
    pub fn put_object_lock_configuration(
        &self,
        bucket: &str,
        configuration: ObjectLockConfiguration,
    ) -> Result<(), StoreError> {
        let transaction = self.begin_write()?;
        {
            let mut buckets = transaction.open_table(BUCKETS)?;
            let mut record = require_bucket(&buckets, bucket)?;
            if record.versioning != VersioningState::Enabled {
                return Err(StoreError::InvalidBucketState(
                    "Object Lock requires a versioning-enabled bucket".to_owned(),
                ));
            }
            record.object_lock = Some(configuration);
            buckets.insert(bucket, encode_json(&record)?.as_slice())?;
        }
        transaction.commit()?;
        Ok(())
    }

    /// Returns the Object Lock configuration, or `ObjectLockConfigurationNotFound`.
    pub fn object_lock_configuration(
        &self,
        bucket: &str,
    ) -> Result<ObjectLockConfiguration, StoreError> {
        self.bucket(bucket)?
            .object_lock
            .ok_or_else(|| StoreError::ObjectLockConfigurationNotFound(bucket.to_owned()))
    }

    /// Replaces the bucket lifecycle configuration; `None` deletes it.
    pub fn put_lifecycle_configuration(
        &self,
        bucket: &str,
        configuration: Option<LifecycleConfiguration>,
    ) -> Result<(), StoreError> {
        let transaction = self.begin_write()?;
        {
            let mut buckets = transaction.open_table(BUCKETS)?;
            let mut record = require_bucket(&buckets, bucket)?;
            record.lifecycle = configuration;
            buckets.insert(bucket, encode_json(&record)?.as_slice())?;
        }
        transaction.commit()?;
        Ok(())
    }

    /// Returns the lifecycle configuration, or `NoSuchLifecycleConfiguration`.
    pub fn lifecycle_configuration(
        &self,
        bucket: &str,
    ) -> Result<LifecycleConfiguration, StoreError> {
        self.bucket(bucket)?
            .lifecycle
            .ok_or_else(|| StoreError::NoSuchLifecycleConfiguration(bucket.to_owned()))
    }
}

/// Applies the S3 general-purpose bucket naming rules.
fn validate_bucket_name(bucket: &str) -> Result<(), StoreError> {
    let invalid = |reason: &str| Err(StoreError::InvalidBucketName(format!("{bucket}: {reason}")));
    if !(3..=63).contains(&bucket.len()) {
        return invalid("the name must be 3 to 63 characters long");
    }
    let bytes = bucket.as_bytes();
    if !bytes.iter().all(|byte| {
        byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'.' || *byte == b'-'
    }) {
        return invalid("the name may contain only lowercase letters, digits, dots, and hyphens");
    }
    let is_alphanumeric = |byte: u8| byte.is_ascii_lowercase() || byte.is_ascii_digit();
    if !is_alphanumeric(bytes[0]) || !is_alphanumeric(bytes[bytes.len() - 1]) {
        return invalid("the name must begin and end with a letter or digit");
    }
    if bucket.contains("..") {
        return invalid("the name must not contain two adjacent dots");
    }
    if bucket.parse::<Ipv4Addr>().is_ok() {
        return invalid("the name must not be formatted as an IP address");
    }
    if ["xn--", "sthree-", "amzn-s3-demo-"]
        .iter()
        .any(|prefix| bucket.starts_with(prefix))
        || ["-s3alias", "--ol-s3", "--x-s3", "--table-s3"]
            .iter()
            .any(|suffix| bucket.ends_with(suffix))
    {
        return invalid("the name uses a reserved prefix or suffix");
    }
    Ok(())
}
