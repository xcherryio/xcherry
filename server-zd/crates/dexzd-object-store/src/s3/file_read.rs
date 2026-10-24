// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::io;
use std::path::Path;
use std::time::Duration;

use bytes::Bytes;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::time::Instant;

use crate::{ObjectStoreError, ObjectStoreErrorKind, ObjectStoreOperation, ObjectStoreResult};

pub(super) async fn read_single_put_file(
    path: &Path,
    max_bytes: usize,
    request_timeout: Duration,
    key: &str,
) -> ObjectStoreResult<Option<Bytes>> {
    let deadline = Instant::now() + request_timeout;
    let mut file = tokio::time::timeout_at(deadline, tokio::fs::File::open(path))
        .await
        .map_err(|_| read_timeout(key, path))?
        .map_err(|error| read_error(key, path, error))?;
    read_single_put_body(&mut file, max_bytes, deadline, key, path).await
}

async fn read_single_put_body<R: AsyncRead + Unpin>(
    reader: &mut R,
    max_bytes: usize,
    deadline: Instant,
    key: &str,
    path: &Path,
) -> ObjectStoreResult<Option<Bytes>> {
    let mut body = Vec::new();
    let mut limited_reader = reader.take((max_bytes + 1) as u64);
    tokio::time::timeout_at(deadline, limited_reader.read_to_end(&mut body))
        .await
        .map_err(|_| read_timeout(key, path))?
        .map_err(|error| read_error(key, path, error))?;
    Ok((body.len() <= max_bytes).then(|| Bytes::from(body)))
}

fn read_timeout(key: &str, path: &Path) -> ObjectStoreError {
    ObjectStoreError::new(
        ObjectStoreOperation::Put,
        key,
        ObjectStoreErrorKind::Transient,
        format!("reading {} for put timed out", path.display()),
    )
}

fn read_error(key: &str, path: &Path, error: io::Error) -> ObjectStoreError {
    ObjectStoreError::new(
        ObjectStoreOperation::Put,
        key,
        ObjectStoreErrorKind::Permanent,
        format!("reading {}: {error}", path.display()),
    )
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::io::{AsyncSeekExt, AsyncWriteExt};

    use super::{read_single_put_body, read_single_put_file};
    use crate::{ObjectStoreErrorKind, ObjectStoreOperation};

    #[tokio::test]
    async fn file_growing_after_metadata_exceeds_the_single_put_cap() {
        let directory = tempfile::tempdir().expect("the temporary directory is created");
        let path = directory.path().join("growing-file");
        tokio::fs::write(&path, b"small")
            .await
            .expect("the initial file is written");
        let observed_size = tokio::fs::metadata(&path)
            .await
            .expect("the initial metadata is readable")
            .len();
        assert!(observed_size < 16);
        let mut file = tokio::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .await
            .expect("the file opens for append");
        file.write_all(&[b'x'; 32])
            .await
            .expect("the file grows after metadata");
        file.flush().await.expect("the file is flushed");
        assert!(
            read_single_put_file(&path, 16, Duration::from_secs(1), "growing")
                .await
                .expect("the bounded read succeeds")
                .is_none()
        );
        let mut reader = tokio::fs::File::open(&path)
            .await
            .expect("the grown file opens");
        assert!(
            read_single_put_body(
                &mut reader,
                16,
                tokio::time::Instant::now() + Duration::from_secs(1),
                "growing",
                &path,
            )
            .await
            .expect("the bounded read succeeds")
            .is_none()
        );
        assert_eq!(
            reader
                .stream_position()
                .await
                .expect("the position is readable"),
            17
        );
    }

    #[tokio::test]
    async fn stalled_preflight_reader_returns_a_transient_put_error() {
        let (mut reader, _writer) = tokio::io::duplex(16);
        let path = std::path::Path::new("/stalled-file");
        let error = read_single_put_body(
            &mut reader,
            16,
            tokio::time::Instant::now() + Duration::from_millis(20),
            "stalled",
            path,
        )
        .await
        .expect_err("the stalled read times out");
        assert_eq!(error.operation, ObjectStoreOperation::Put);
        assert_eq!(error.kind, ObjectStoreErrorKind::Transient);
        assert!(error.detail.contains("/stalled-file"));
        assert!(error.detail.contains("timed out"));
    }
}
