// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::fs::{self, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Barrier};
use std::thread;

use dex_blob_cache::{BlobCache, BlobCacheConfig, BlobCacheError};
use sha2::{Digest, Sha256};

#[test]
fn blob_cache_round_trips_deletes_and_enforces_immutable_ids() {
    let directory = TestDirectory::new();
    let cache = BlobCache::open(config(directory.path(), 1 << 20)).unwrap();

    assert!(cache.put("string-id", b"hello").unwrap());
    let mut payload = cache.get("string-id").unwrap().unwrap();
    assert_eq!(payload, b"hello");
    payload[0] = b'X';
    assert_eq!(cache.get("string-id").unwrap().unwrap(), b"hello");

    assert!(cache.put("string-id", b"hello").unwrap());
    assert!(matches!(
        cache.put("string-id", b"different"),
        Err(BlobCacheError::ContentMismatch(_))
    ));
    cache.delete("string-id").unwrap();
    assert_eq!(cache.get("string-id").unwrap(), None);
    cache.delete("missing-id").unwrap();

    cache.close().unwrap();
    cache.close().unwrap();
    assert!(matches!(
        cache.get("string-id"),
        Err(BlobCacheError::Closed)
    ));
}

#[test]
fn blob_cache_rejects_oversized_values_without_writing() {
    let directory = TestDirectory::new();
    let cache = BlobCache::open(config(directory.path(), 64)).unwrap();

    assert!(!cache.put("oversized", &[0_u8; 128]).unwrap());
    assert_eq!(owned_bytes(directory.path()), 0);
    cache.close().unwrap();
}

#[test]
fn blob_cache_validates_configuration_and_blob_ids() {
    let directory = TestDirectory::new();
    assert!(matches!(
        BlobCacheConfig::new("", 1024, 10_000),
        Err(BlobCacheError::InvalidConfig(_))
    ));
    assert!(matches!(
        BlobCacheConfig::new(directory.path(), 0, 10_000),
        Err(BlobCacheError::InvalidConfig(_))
    ));
    assert!(matches!(
        BlobCacheConfig::new(directory.path(), 1024, -1),
        Err(BlobCacheError::InvalidConfig(_))
    ));
    assert_eq!(
        BlobCacheConfig::new(directory.path(), 1024, 0)
            .unwrap()
            .frequency_counters(),
        10_000
    );

    let cache = BlobCache::open(config(directory.path(), 1024)).unwrap();
    assert!(matches!(
        cache.put("", b"payload"),
        Err(BlobCacheError::InvalidBlob(_))
    ));
    cache.close().unwrap();
}

#[test]
fn blob_cache_preserves_valid_files_and_removes_corruption_on_restart() {
    let directory = TestDirectory::new();
    let first = BlobCache::open(config(directory.path(), 1 << 20)).unwrap();
    assert!(first.put("warm-id", b"persisted").unwrap());
    assert!(first.put("corrupt-id", b"payload").unwrap());
    first.close().unwrap();

    let corrupt_path = path_for(directory.path(), "corrupt-id");
    let mut corrupt_file = OpenOptions::new().write(true).open(&corrupt_path).unwrap();
    corrupt_file.seek(SeekFrom::Start(24)).unwrap();
    corrupt_file.write_all(&[0xff]).unwrap();
    let temporary = directory.path().join("tmp").join("interrupted.tmp");
    fs::write(&temporary, b"partial").unwrap();

    let second = BlobCache::open(config(directory.path(), 1 << 20)).unwrap();
    assert_eq!(second.get("warm-id").unwrap().unwrap(), b"persisted");
    assert_eq!(second.get("corrupt-id").unwrap(), None);
    assert!(!corrupt_path.exists());
    assert!(!temporary.exists());
    second.close().unwrap();
}

#[test]
fn blob_cache_rejects_damaged_files_and_accepts_refetch() {
    for damage in ["truncated", "corrupted"] {
        let directory = TestDirectory::new();
        let cache = BlobCache::open(config(directory.path(), 1 << 20)).unwrap();
        let blob_id = "refetched-id";
        let payload = b"original-payload";
        assert!(cache.put(blob_id, payload).unwrap());
        let path = path_for(directory.path(), blob_id);

        match damage {
            "truncated" => {
                let file = OpenOptions::new().write(true).open(&path).unwrap();
                file.set_len((24 + blob_id.len() + 2) as u64).unwrap();
            }
            "corrupted" => {
                let mut file = OpenOptions::new().write(true).open(&path).unwrap();
                file.seek(SeekFrom::Start((24 + blob_id.len()) as u64))
                    .unwrap();
                file.write_all(&[0xff]).unwrap();
            }
            _ => unreachable!("the test enumerates only known damage modes"),
        }

        assert_eq!(cache.get(blob_id).unwrap(), None, "{damage}");
        assert!(!path.exists(), "{damage}");
        assert!(cache.put(blob_id, payload).unwrap(), "{damage}");
        assert_eq!(cache.get(blob_id).unwrap().unwrap(), payload, "{damage}");
        cache.close().unwrap();
    }
}

#[test]
fn blob_cache_commits_one_file_for_concurrent_same_id_puts() {
    let directory = TestDirectory::new();
    let cache = Arc::new(BlobCache::open(config(directory.path(), 1 << 20)).unwrap());
    let barrier = Arc::new(Barrier::new(17));
    let mut threads = Vec::new();
    for _ in 0..16 {
        let cache = Arc::clone(&cache);
        let barrier = Arc::clone(&barrier);
        threads.push(thread::spawn(move || {
            barrier.wait();
            assert!(cache.put("same-id", b"same-payload").unwrap());
        }));
    }
    barrier.wait();
    for worker in threads {
        worker.join().unwrap();
    }

    assert_eq!(regular_files(&directory.path().join("blobs")).len(), 1);
    cache.close().unwrap();
}

#[test]
fn blob_cache_commits_distinct_files_for_concurrent_puts() {
    let directory = TestDirectory::new();
    let cache = Arc::new(BlobCache::open(config(directory.path(), 1 << 20)).unwrap());
    let barrier = Arc::new(Barrier::new(17));
    let mut threads = Vec::new();
    for worker_index in 0..16 {
        let cache = Arc::clone(&cache);
        let barrier = Arc::clone(&barrier);
        threads.push(thread::spawn(move || {
            barrier.wait();
            assert!(
                cache
                    .put(&format!("distinct-{worker_index}"), b"payload")
                    .unwrap()
            );
        }));
    }
    barrier.wait();
    for worker in threads {
        worker.join().unwrap();
    }

    for worker_index in 0..16 {
        assert_eq!(
            cache
                .get(&format!("distinct-{worker_index}"))
                .unwrap()
                .unwrap(),
            b"payload"
        );
    }
    assert_eq!(regular_files(&directory.path().join("blobs")).len(), 16);
    cache.close().unwrap();
}

#[test]
fn blob_cache_reads_go_compatible_dxbc_version_one() {
    let directory = TestDirectory::new();
    let initial = BlobCache::open(config(directory.path(), 1 << 20)).unwrap();
    initial.close().unwrap();

    let blob_id = b"fixture-id";
    let payload = b"fixture-payload";
    let path = path_for(directory.path(), "fixture-id");
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    let mut encoded = vec![0_u8; 24];
    encoded[0..4].copy_from_slice(b"DXBC");
    encoded[4] = 1;
    encoded[8..12].copy_from_slice(&(blob_id.len() as u32).to_le_bytes());
    encoded[12..20].copy_from_slice(&(payload.len() as u64).to_le_bytes());
    encoded[20..24].copy_from_slice(&0xf8b9_3c03_u32.to_le_bytes());
    encoded.extend_from_slice(blob_id);
    encoded.extend_from_slice(payload);
    fs::write(path, encoded).unwrap();

    let cache = BlobCache::open(config(directory.path(), 1 << 20)).unwrap();
    assert_eq!(cache.get("fixture-id").unwrap().unwrap(), payload);
    cache.close().unwrap();
}

#[test]
fn blob_cache_reconciles_a_smaller_restart_budget() {
    let directory = TestDirectory::new();
    let first = BlobCache::open(config(directory.path(), 1 << 20)).unwrap();
    for index in 0..8 {
        assert!(first.put(&format!("restart-{index}"), &[0_u8; 64]).unwrap());
    }
    first.close().unwrap();

    let limit = 24 + "restart-0".len() as i64 + 64;
    let second = BlobCache::open(config(directory.path(), limit)).unwrap();
    assert!(owned_bytes(directory.path()) <= limit as u64);
    second.close().unwrap();
}

#[test]
fn blob_cache_handles_concurrent_reads_writes_and_purges() {
    let directory = TestDirectory::new();
    let cache = Arc::new(BlobCache::open(config(directory.path(), 4096)).unwrap());
    let barrier = Arc::new(Barrier::new(9));
    let mut threads = Vec::new();

    for worker in 0..8 {
        let cache = Arc::clone(&cache);
        let barrier = Arc::clone(&barrier);
        threads.push(thread::spawn(move || {
            barrier.wait();
            for iteration in 0..40 {
                let blob_id = format!("worker-{worker}-{iteration}");
                if cache.put(&blob_id, &[iteration as u8]).unwrap() {
                    let _ = cache.get(&blob_id).unwrap();
                }
            }
        }));
    }
    barrier.wait();
    for _ in 0..5 {
        cache.delete_all().unwrap();
    }
    for worker in threads {
        worker.join().unwrap();
    }

    assert!(owned_bytes(directory.path()) <= 4096);
    assert!(regular_files(&directory.path().join("tmp")).is_empty());
    cache.close().unwrap();
}

#[test]
fn blob_cache_delete_all_is_reusable() {
    let directory = TestDirectory::new();
    let cache = BlobCache::open(config(directory.path(), 1 << 20)).unwrap();
    assert!(cache.put("before", b"payload").unwrap());
    cache.delete_all().unwrap();
    assert_eq!(cache.get("before").unwrap(), None);
    assert!(cache.put("after", b"fresh").unwrap());
    assert_eq!(cache.get("after").unwrap().unwrap(), b"fresh");
    cache.close().unwrap();
}

#[cfg(unix)]
#[test]
fn blob_cache_retries_failed_delete_for_untracked_path() {
    use std::os::unix::fs::PermissionsExt;

    let directory = TestDirectory::new();
    let cache = BlobCache::open(config(directory.path(), 1 << 20)).unwrap();
    let orphan_path = path_for(directory.path(), "untracked");
    let parent = orphan_path.parent().unwrap();
    fs::create_dir_all(parent).unwrap();
    fs::write(&orphan_path, b"orphan").unwrap();

    fs::set_permissions(parent, fs::Permissions::from_mode(0o500)).unwrap();
    let delete_result = cache.delete("untracked");
    fs::set_permissions(parent, fs::Permissions::from_mode(0o700)).unwrap();

    assert!(delete_result.is_err());
    assert!(cache.put("cleanup-trigger", b"payload").unwrap());
    assert!(!orphan_path.exists());
    cache.close().unwrap();
}

#[cfg(unix)]
#[test]
fn blob_cache_uses_private_file_permissions() {
    use std::os::unix::fs::PermissionsExt;

    let directory = TestDirectory::new();
    let cache = BlobCache::open(config(directory.path(), 1 << 20)).unwrap();
    assert!(cache.put("private", b"payload").unwrap());

    let mode = fs::metadata(path_for(directory.path(), "private"))
        .unwrap()
        .permissions()
        .mode();
    assert_eq!(mode & 0o077, 0);
    cache.close().unwrap();
}

fn config(directory: &Path, max_bytes: i64) -> BlobCacheConfig {
    BlobCacheConfig::new(directory, max_bytes, 10_000)
        .expect("the blob cache test fixture was initialized successfully")
}

fn path_for(root: &Path, blob_id: &str) -> PathBuf {
    let digest = Sha256::digest(blob_id.as_bytes());
    let encoded = encode_hex(&digest);
    root.join("blobs")
        .join(&encoded[0..2])
        .join(&encoded[2..4])
        .join(format!("{encoded}.blob"))
}

fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

fn owned_bytes(root: &Path) -> u64 {
    regular_files(&root.join("tmp"))
        .into_iter()
        .chain(regular_files(&root.join("blobs")))
        .map(|path| {
            fs::metadata(path)
                .expect("the blob cache test fixture was initialized successfully")
                .len()
        })
        .sum()
}

fn regular_files(root: &Path) -> Vec<PathBuf> {
    if !root.exists() {
        return Vec::new();
    }
    let mut files = Vec::new();
    for entry in
        fs::read_dir(root).expect("the blob cache test fixture was initialized successfully")
    {
        let entry = entry.expect("the blob cache test fixture was initialized successfully");
        let file_type = entry
            .file_type()
            .expect("the blob cache test fixture was initialized successfully");
        if file_type.is_dir() {
            files.extend(regular_files(&entry.path()));
        } else if file_type.is_file() {
            files.push(entry.path());
        }
    }
    files
}

struct TestDirectory {
    directory: tempfile::TempDir,
}

impl TestDirectory {
    fn new() -> Self {
        Self {
            directory: tempfile::Builder::new()
                .prefix("dex-rust-blob-cache-")
                .tempdir()
                .expect("the blob cache test fixture was initialized successfully"),
        }
    }

    fn path(&self) -> &Path {
        self.directory.path()
    }
}
