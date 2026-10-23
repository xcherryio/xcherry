// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::io::AsyncWriteExt;

use crate::{ObjectStoreError, ObjectStoreErrorKind, ObjectStoreOperation, ObjectStoreResult};

static NEXT_DOWNLOAD_ID: AtomicU64 = AtomicU64::new(0);

pub(crate) struct TemporaryDownloadFile {
    file: Option<tokio::fs::File>,
    temporary_path: PathBuf,
    target_path: PathBuf,
    key: String,
    is_committed: bool,
}

impl TemporaryDownloadFile {
    pub(crate) async fn create(key: &str, target_path: &Path) -> ObjectStoreResult<Self> {
        let name = target_path.file_name().ok_or_else(|| {
            ObjectStoreError::new(
                ObjectStoreOperation::Get,
                key,
                ObjectStoreErrorKind::Permanent,
                "download path has no file name",
            )
        })?;
        for _ in 0..16 {
            let mut temporary_name = OsString::from(".");
            temporary_name.push(name);
            temporary_name.push(format!(
                ".dexzd-{}-{}.tmp",
                std::process::id(),
                NEXT_DOWNLOAD_ID.fetch_add(1, Ordering::Relaxed)
            ));
            let temporary_path = target_path.with_file_name(temporary_name);
            match tokio::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temporary_path)
                .await
            {
                Ok(file) => {
                    return Ok(Self {
                        file: Some(file),
                        temporary_path,
                        target_path: target_path.to_path_buf(),
                        key: key.to_owned(),
                        is_committed: false,
                    });
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => {
                    return Err(ObjectStoreError::new(
                        ObjectStoreOperation::Get,
                        key,
                        ObjectStoreErrorKind::Permanent,
                        format!("creating temporary download: {error}"),
                    ));
                }
            }
        }
        Err(ObjectStoreError::new(
            ObjectStoreOperation::Get,
            key,
            ObjectStoreErrorKind::Permanent,
            "could not allocate a temporary download path",
        ))
    }

    pub(crate) async fn write_all(&mut self, bytes: &[u8]) -> ObjectStoreResult<()> {
        self.file
            .as_mut()
            .expect("temporary download file is open")
            .write_all(bytes)
            .await
            .map_err(|error| {
                ObjectStoreError::new(
                    ObjectStoreOperation::Get,
                    &self.key,
                    ObjectStoreErrorKind::Permanent,
                    format!("writing temporary download: {error}"),
                )
            })
    }

    pub(crate) async fn commit(mut self) -> ObjectStoreResult<()> {
        let mut file = self.file.take().expect("temporary download file is open");
        file.flush()
            .await
            .map_err(|error| self.file_error("flushing", error))?;
        file.sync_all()
            .await
            .map_err(|error| self.file_error("syncing", error))?;
        drop(file);
        match std::fs::symlink_metadata(&self.target_path) {
            Ok(_) => {
                return Err(ObjectStoreError::new(
                    ObjectStoreOperation::Get,
                    &self.key,
                    ObjectStoreErrorKind::Permanent,
                    format!(
                        "download target already exists: {}",
                        self.target_path.display()
                    ),
                ));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(self.file_error("checking target", error)),
        }
        std::fs::rename(&self.temporary_path, &self.target_path)
            .map_err(|error| self.file_error("renaming", error))?;
        self.is_committed = true;
        Ok(())
    }

    fn file_error(&self, action: &str, error: std::io::Error) -> ObjectStoreError {
        ObjectStoreError::new(
            ObjectStoreOperation::Get,
            &self.key,
            ObjectStoreErrorKind::Permanent,
            format!("{action} download {}: {error}", self.target_path.display()),
        )
    }
}

impl Drop for TemporaryDownloadFile {
    fn drop(&mut self) {
        if self.is_committed {
            return;
        }
        drop(self.file.take());
        if let Err(error) = std::fs::remove_file(&self.temporary_path)
            && error.kind() != std::io::ErrorKind::NotFound
        {
            tracing::warn!(path = %self.temporary_path.display(), %error, "failed to remove temporary download");
        }
    }
}
