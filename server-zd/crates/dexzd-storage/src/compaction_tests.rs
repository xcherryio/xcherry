// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::path::Path;
use std::time::{Duration, Instant};

use dexzd_keyspace::ColumnFamily;

use crate::{LocalReadView, LocalStore, RocksDbLocalStore, RocksDbStoreOptions, WriteBatch};

const FLOWS: u64 = 2_000;

/// Writes like a replay: overwrites Flow rows, appends history, and updates partition metadata.
fn write_replay_like_batches(store: &RocksDbLocalStore, batches: u64) {
    for batch_number in 0..batches {
        let mut batch = WriteBatch::new();
        for record in 0..4 {
            let flow = (batch_number * 4 + record) % FLOWS;
            batch.put(
                ColumnFamily::Flow,
                flow.to_be_bytes().to_vec(),
                batch_number.to_be_bytes().repeat(32),
            );
            let mut history_key = flow.to_be_bytes().to_vec();
            history_key.extend_from_slice(&batch_number.to_be_bytes());
            batch.put(ColumnFamily::History, history_key, vec![7; 64]);
        }
        batch.put(
            ColumnFamily::PartitionMeta,
            (batch_number % 64).to_be_bytes().to_vec(),
            batch_number.to_be_bytes().to_vec(),
        );
        store.write(batch).expect("replay-like batch writes");
    }
}

/// Returns the bytes of the write-ahead log files and of every file in `directory`.
fn wal_and_total_bytes(directory: &Path) -> (u64, u64) {
    let mut wal_bytes = 0;
    let mut total_bytes = 0;
    for entry in std::fs::read_dir(directory).expect("store directory lists") {
        let entry = entry.expect("store directory entry reads");
        let Ok(metadata) = entry.metadata() else {
            // RocksDB deleted the file after it was listed.
            continue;
        };
        total_bytes += metadata.len();
        if entry.file_name().to_string_lossy().ends_with(".log") {
            wal_bytes += metadata.len();
        }
    }
    (wal_bytes, total_bytes)
}

#[test]
fn background_compaction_releases_the_write_ahead_log_of_a_live_store() {
    let directory = tempfile::tempdir().expect("temporary directory exists");
    let path = directory.path().join("db");
    let store = RocksDbLocalStore::open(&path, RocksDbStoreOptions::default()).expect("opens");
    write_replay_like_batches(&store, 40_000);
    let (wal_before, total_before) = wal_and_total_bytes(&path);
    assert!(
        wal_before * 2 > total_before,
        "the write-ahead log holds the writes: {wal_before} of {total_before} bytes"
    );

    store
        .start_background_compaction()
        .expect("compaction starts");
    let deadline = Instant::now() + Duration::from_secs(60);
    let (wal_after, total_after) = loop {
        let (wal_bytes, total_bytes) = wal_and_total_bytes(&path);
        if wal_bytes == 0 || Instant::now() >= deadline {
            break (wal_bytes, total_bytes);
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    assert_eq!(wal_after, 0, "compaction released the write-ahead log");
    assert!(
        total_after * 3 < total_before,
        "the store shrank from {total_before} to {total_after} bytes"
    );
    assert_eq!(
        store
            .get(ColumnFamily::Flow, &(FLOWS - 1).to_be_bytes())
            .expect("Flow row reads"),
        Some(39_999_u64.to_be_bytes().repeat(32))
    );
    let history_rows = store
        .scan(
            ColumnFamily::History,
            None,
            None,
            crate::ScanDirection::Forward,
            usize::MAX,
        )
        .expect("history scans");
    assert_eq!(history_rows.len(), 160_000);
}

#[test]
fn the_write_ahead_log_cap_bounds_the_log_while_writing() {
    let directory = tempfile::tempdir().expect("temporary directory exists");
    let mut sizes = Vec::new();
    for max_total_wal_bytes in [u64::MAX, 4 * 1024 * 1024] {
        let path = directory.path().join(format!("db-{max_total_wal_bytes}"));
        let store = RocksDbLocalStore::open(
            &path,
            RocksDbStoreOptions {
                max_total_wal_bytes,
                ..RocksDbStoreOptions::default()
            },
        )
        .expect("opens");
        write_replay_like_batches(&store, 40_000);
        sizes.push(wal_and_total_bytes(&path).0);
    }
    let (uncapped, capped) = (sizes[0], sizes[1]);
    assert!(
        uncapped > 24 * 1024 * 1024,
        "uncapped log: {uncapped} bytes"
    );
    assert!(capped <= 8 * 1024 * 1024, "capped log: {capped} bytes");
}

#[test]
fn dropping_a_store_stops_its_compaction_and_releases_the_directory() {
    let directory = tempfile::tempdir().expect("temporary directory exists");
    let path = directory.path().join("db");
    let store = RocksDbLocalStore::open(&path, RocksDbStoreOptions::default()).expect("opens");
    write_replay_like_batches(&store, 40_000);
    store
        .start_background_compaction()
        .expect("compaction starts");
    store
        .start_background_compaction()
        .expect("a second request while compacting is accepted");

    let dropping = Instant::now();
    drop(store);
    assert!(
        dropping.elapsed() < Duration::from_secs(10),
        "drop took {:?}",
        dropping.elapsed()
    );
    let reopened = RocksDbLocalStore::open(&path, RocksDbStoreOptions::default()).expect("reopens");
    assert_eq!(
        reopened
            .get(ColumnFamily::Flow, &(FLOWS - 1).to_be_bytes())
            .expect("Flow row reads"),
        Some(39_999_u64.to_be_bytes().repeat(32))
    );
}
