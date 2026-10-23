// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Lifecycle expiration with an adjustable clock, so tests can make objects old instantly.

use std::sync::atomic::Ordering;
use std::time::{SystemTime, UNIX_EPOCH};

use redb::ReadableDatabase;

use super::object_delete::{DeleteGuard, DeleteObjectRequest};
use super::{Store, UPLOADS, VERSIONS, decode_json};
use crate::error::StoreError;
use crate::key_codec::{decode_object_key, encode_bucket_prefix, prefix_successor};
use crate::record::{BucketRecord, LifecycleRule, UploadRecord, VersionRecord};

const MILLISECONDS_PER_DAY: i64 = 86_400_000;

/// Counts of what one lifecycle pass removed.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LifecycleRun {
    pub expired_current_versions: u64,
    pub removed_delete_markers: u64,
    pub expired_noncurrent_versions: u64,
    pub aborted_uploads: u64,
}

/// A removal decided during the read-only scan and re-validated when applied.
enum LifecycleAction {
    ExpireCurrent {
        key: String,
        internal_version: String,
    },
    RemoveVersion {
        key: String,
        version_id: String,
        internal_version: String,
        is_expired_delete_marker: bool,
    },
    AbortUpload {
        key: String,
        upload_id: String,
    },
}

impl Store {
    /// Returns the lifecycle clock: wall-clock time plus the offset set by `advance_lifecycle_clock`.
    pub fn lifecycle_now_unix_ms(&self) -> i64 {
        let wall_clock = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|elapsed| {
                i64::try_from(elapsed.as_millis()).expect("the current time fits in i64")
            })
            .expect("the system clock is after 1970");
        wall_clock + self.lifecycle_clock_offset_ms.load(Ordering::Relaxed)
    }

    /// Returns how far the lifecycle clock is ahead of wall-clock time, in milliseconds.
    pub fn lifecycle_clock_offset_ms(&self) -> i64 {
        self.lifecycle_clock_offset_ms.load(Ordering::Relaxed)
    }

    /// Moves the lifecycle clock forward by `milliseconds` and returns the total offset.
    ///
    /// Only lifecycle decisions use this clock; object timestamps keep wall-clock time.
    pub fn advance_lifecycle_clock(&self, milliseconds: i64) -> i64 {
        self.lifecycle_clock_offset_ms
            .fetch_add(milliseconds, Ordering::Relaxed)
            + milliseconds
    }

    /// Applies every enabled lifecycle rule once. Anything due at the current lifecycle time is removed.
    pub fn run_lifecycle(&self) -> Result<LifecycleRun, StoreError> {
        let now = self.lifecycle_now_unix_ms();
        let mut run = LifecycleRun::default();
        for bucket in self.list_buckets()? {
            let Some(configuration) = &bucket.lifecycle else {
                continue;
            };
            let rules: Vec<&LifecycleRule> = configuration
                .rules
                .iter()
                .filter(|rule| rule.is_enabled)
                .collect();
            if rules.is_empty() {
                continue;
            }
            for action in self.plan_lifecycle_actions(&bucket, &rules, now)? {
                self.apply_lifecycle_action(&bucket, &rules, now, action, &mut run)?;
            }
        }
        Ok(run)
    }

    fn plan_lifecycle_actions(
        &self,
        bucket: &BucketRecord,
        rules: &[&LifecycleRule],
        now: i64,
    ) -> Result<Vec<LifecycleAction>, StoreError> {
        let transaction = self.database.begin_read()?;
        let bucket_prefix = encode_bucket_prefix(&bucket.name);
        let end = prefix_successor(&bucket_prefix)
            .expect("encoded bucket prefixes end with a terminator below 0xFF");
        let mut actions = Vec::new();
        let mut current_key: Option<String> = None;
        let mut key_versions: Vec<VersionRecord> = Vec::new();
        for entry in transaction
            .open_table(VERSIONS)?
            .range(bucket_prefix.as_slice()..end.as_slice())?
        {
            let (version_key, value) = entry?;
            let key = decode_object_key(version_key.value(), &bucket.name);
            if current_key.as_deref() != Some(key.as_str()) {
                if let Some(previous) = current_key.take() {
                    plan_key_actions(&previous, &key_versions, rules, now, &mut actions);
                }
                key_versions.clear();
                current_key = Some(key);
            }
            key_versions.push(decode_json(value.value())?);
        }
        if let Some(previous) = current_key {
            plan_key_actions(&previous, &key_versions, rules, now, &mut actions);
        }
        for entry in transaction
            .open_table(UPLOADS)?
            .range(bucket_prefix.as_slice()..end.as_slice())?
        {
            let upload: UploadRecord = decode_json(entry?.1.value())?;
            let is_due = matching_rules(rules, &upload.key).any(|rule| {
                rule.abort_incomplete_upload_days
                    .is_some_and(|days| is_older_than_days(upload.initiated_unix_ms, days, now))
            });
            if is_due {
                actions.push(LifecycleAction::AbortUpload {
                    key: upload.key,
                    upload_id: upload.upload_id,
                });
            }
        }
        Ok(actions)
    }

    fn apply_lifecycle_action(
        &self,
        bucket: &BucketRecord,
        rules: &[&LifecycleRule],
        now: i64,
        action: LifecycleAction,
        run: &mut LifecycleRun,
    ) -> Result<(), StoreError> {
        let (key, request) = match &action {
            LifecycleAction::AbortUpload { key, upload_id } => {
                match self.abort_multipart_upload(&bucket.name, key, upload_id) {
                    Ok(()) => run.aborted_uploads += 1,
                    // A client completed or aborted it since the scan.
                    Err(StoreError::NoSuchUpload { .. }) => {}
                    Err(error) => return Err(error),
                }
                return Ok(());
            }
            LifecycleAction::ExpireCurrent { key, .. } => (key, DeleteObjectRequest::default()),
            LifecycleAction::RemoveVersion {
                key, version_id, ..
            } => (
                key,
                DeleteObjectRequest {
                    version_id: Some(version_id.clone()),
                },
            ),
        };
        // Writes since the scan may have changed what is due, so the decision is remade at commit.
        let is_still_due = |versions: &[VersionRecord]| {
            let mut replanned = Vec::new();
            plan_key_actions(key, versions, rules, now, &mut replanned);
            replanned
                .iter()
                .any(|candidate| candidate.is_same_removal(&action))
        };
        let guard = DeleteGuard {
            is_still_due: &is_still_due,
            // Lifecycle-created markers start their noncurrent period at lifecycle time.
            marker_last_modified_unix_ms: Some(now),
        };
        if self
            .delete_object_if(&bucket.name, key, &request, Some(&guard))?
            .is_none()
        {
            return Ok(());
        }
        match action {
            LifecycleAction::ExpireCurrent { .. } => run.expired_current_versions += 1,
            LifecycleAction::RemoveVersion {
                is_expired_delete_marker: true,
                ..
            } => run.removed_delete_markers += 1,
            LifecycleAction::RemoveVersion {
                is_expired_delete_marker: false,
                ..
            } => run.expired_noncurrent_versions += 1,
            LifecycleAction::AbortUpload { .. } => {}
        }
        Ok(())
    }
}

impl LifecycleAction {
    /// Returns whether both actions remove the same version in the same way.
    fn is_same_removal(&self, other: &LifecycleAction) -> bool {
        match (self, other) {
            (
                LifecycleAction::ExpireCurrent {
                    internal_version: left,
                    ..
                },
                LifecycleAction::ExpireCurrent {
                    internal_version: right,
                    ..
                },
            )
            | (
                LifecycleAction::RemoveVersion {
                    internal_version: left,
                    ..
                },
                LifecycleAction::RemoveVersion {
                    internal_version: right,
                    ..
                },
            ) => left == right,
            (
                LifecycleAction::ExpireCurrent { .. }
                | LifecycleAction::RemoveVersion { .. }
                | LifecycleAction::AbortUpload { .. },
                _,
            ) => false,
        }
    }
}

/// Decides the removals for one key from its versions, newest first.
fn plan_key_actions(
    key: &str,
    versions: &[VersionRecord],
    rules: &[&LifecycleRule],
    now: i64,
    actions: &mut Vec<LifecycleAction>,
) {
    let matching: Vec<&LifecycleRule> = matching_rules(rules, key).collect();
    if matching.is_empty() {
        return;
    }
    let Some(current) = versions.first() else {
        return;
    };
    if current.is_delete_marker {
        // A lone marker is removed by ExpiredObjectDeleteMarker, or by a Days rule once it is old enough.
        let is_marker_due = matching.iter().any(|rule| {
            rule.expired_object_delete_marker == Some(true)
                || rule.expiration_days.is_some_and(|days| {
                    is_older_than_days(current.last_modified_unix_ms, days, now)
                })
        });
        if versions.len() == 1 && is_marker_due {
            actions.push(LifecycleAction::RemoveVersion {
                key: key.to_owned(),
                version_id: current.version_id(),
                internal_version: current.internal_version.clone(),
                is_expired_delete_marker: true,
            });
        }
    } else if matching.iter().any(|rule| {
        rule.expiration_days
            .is_some_and(|days| is_older_than_days(current.last_modified_unix_ms, days, now))
    }) {
        actions.push(LifecycleAction::ExpireCurrent {
            key: key.to_owned(),
            internal_version: current.internal_version.clone(),
        });
    }
    // A version becomes noncurrent when its successor is written.
    for (noncurrent_index, pair) in versions.windows(2).enumerate() {
        let (successor, noncurrent) = (&pair[0], &pair[1]);
        let is_due = matching.iter().any(|rule| {
            let is_retained = rule
                .newer_noncurrent_versions
                .is_some_and(|retained| noncurrent_index < retained as usize);
            !is_retained
                && rule.noncurrent_expiration_days.is_some_and(|days| {
                    is_older_than_days(successor.last_modified_unix_ms, days, now)
                })
        });
        if is_due {
            actions.push(LifecycleAction::RemoveVersion {
                key: key.to_owned(),
                version_id: noncurrent.version_id(),
                internal_version: noncurrent.internal_version.clone(),
                is_expired_delete_marker: false,
            });
        }
    }
}

fn matching_rules<'rules>(
    rules: &'rules [&'rules LifecycleRule],
    key: &'rules str,
) -> impl Iterator<Item = &'rules LifecycleRule> {
    rules
        .iter()
        .copied()
        .filter(move |rule| key.starts_with(&rule.prefix))
}

fn is_older_than_days(since_unix_ms: i64, days: u32, now: i64) -> bool {
    now - since_unix_ms >= i64::from(days) * MILLISECONDS_PER_DAY
}
