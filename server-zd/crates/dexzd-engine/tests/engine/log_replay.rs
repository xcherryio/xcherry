// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::path::Path;
use std::time::Duration;

use dexzd_engine::{CommitUrgency, LocalStoreConfig};
use dexzd_storage::RocksDbStoreOptions;
use tokio::time::Instant;

use crate::harness::{TestCluster, TestNode};

const GET_DELAY: Duration = Duration::from_millis(10);

#[tokio::test(start_paused = true)]
async fn cold_replay_reads_ahead_through_closed_and_open_streams_at_ten_ms_gets() {
    const BATCHES: u32 = 200;
    let mut cluster = TestCluster::new();
    let first = cluster.start_in_memory().await;
    first.commit_prompt("replayed-flow", BATCHES as usize).await;
    first.node.shutdown().await.expect("lease released");
    cluster.faults.delay_every_get_under("log/", GET_DELAY);

    // An empty store of a new incarnation replays incarnation 1 as the open stream, then closes it.
    let mut config = cluster.config(LocalStoreConfig::InMemory);
    config.node.incarnation = 2;
    let started = Instant::now();
    let second = cluster
        .start_with(&config)
        .await
        .expect("node starts with a new incarnation");
    let open_stream_replay = started.elapsed();
    assert_eq!(
        second.committed_count("replayed-flow").await,
        u64::from(BATCHES)
    );
    second.node.shutdown().await.expect("lease released");

    // Later empty stores replay incarnation 1 as a closed stream.
    let mut closed_stream_replays = Vec::new();
    for depth in [64, 1] {
        config.recovery.log_read_ahead.max_read_ahead_batches = depth;
        let started = Instant::now();
        let node = cluster.start_with(&config).await.expect("node restarts");
        closed_stream_replays.push(started.elapsed());
        assert_eq!(
            node.committed_count("replayed-flow").await,
            u64::from(BATCHES)
        );
        node.node.shutdown().await.expect("lease released");
    }
    let sequential_reads = GET_DELAY * BATCHES;
    assert!(
        open_stream_replay < sequential_reads / 8,
        "open stream replay took {open_stream_replay:?}"
    );
    assert!(
        closed_stream_replays[0] < sequential_reads / 8,
        "closed stream replay at depth 64 took {:?}",
        closed_stream_replays[0]
    );
    assert!(
        closed_stream_replays[1] >= sequential_reads,
        "closed stream replay at depth 1 took {:?}",
        closed_stream_replays[1]
    );
}

#[tokio::test(start_paused = true)]
async fn a_bulk_cold_replay_compacts_the_rebuilt_store_in_the_background() {
    const FLOWS: usize = 2_000;
    const COMMANDS: usize = 20_000;
    let directory = tempfile::tempdir().expect("temporary directory");
    let mut cluster = TestCluster::new();
    let first = cluster.start_in_memory().await;
    commit_across_flows(&first, FLOWS, COMMANDS).await;
    first.node.shutdown().await.expect("lease released");

    let mut sizes = Vec::new();
    for (store_name, compact_after_replay_bytes) in [("kept", u64::MAX), ("compacted", 0)] {
        let path = directory.path().join(store_name);
        let mut config = cluster.config(LocalStoreConfig::RocksDb {
            directory: path.clone(),
            options: RocksDbStoreOptions::default(),
        });
        config.recovery.compact_after_replay_bytes = compact_after_replay_bytes;
        let node = cluster.start_with(&config).await.expect("node starts");
        assert_eq!(
            node.committed_count("flow-7").await,
            (COMMANDS / FLOWS) as u64
        );
        let sizes_after_replay =
            wait_for_released_log(&path, compact_after_replay_bytes == 0).await;
        sizes.push(sizes_after_replay);
        assert_eq!(
            node.committed_count(&format!("flow-{}", FLOWS - 1)).await,
            (COMMANDS / FLOWS) as u64
        );
        node.node.shutdown().await.expect("lease released");
    }
    let [(kept_wal, kept_total), (compacted_wal, compacted_total)] = sizes[..] else {
        panic!("two stores were measured: {sizes:?}");
    };
    assert!(
        kept_wal * 2 > kept_total,
        "the write-ahead log holds the replay: {kept_wal} of {kept_total} bytes"
    );
    assert!(
        compacted_wal < 64 * 1024,
        "compaction released the write-ahead log: {compacted_wal} bytes"
    );
    assert!(
        compacted_total * 2 < kept_total,
        "compaction shrank the store from {kept_total} to {compacted_total} bytes"
    );
}

/// Submits `commands` prompt commands round-robin over `flows` Flows and waits for each to commit.
async fn commit_across_flows(node: &TestNode, flows: usize, commands: usize) {
    let mut tickets = Vec::new();
    for index in 0..commands {
        let (_, ticket) = node
            .submit(&format!("flow-{}", index % flows), CommitUrgency::Prompt)
            .expect("submit succeeds");
        tickets.push(ticket);
        if tickets.len() == 256 {
            for ticket in tickets.drain(..) {
                ticket.wait().await.expect("command commits");
            }
        }
    }
    for ticket in tickets {
        ticket.wait().await.expect("command commits");
    }
}

/// Returns the write-ahead log and total bytes of the store at `path`, after waiting up to 60 s of
/// real time for the log to be released when `is_compacting`.
async fn wait_for_released_log(path: &Path, is_compacting: bool) -> (u64, u64) {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || {
        let deadline = std::time::Instant::now() + Duration::from_secs(60);
        loop {
            let (wal_bytes, total_bytes) = wal_and_total_bytes(&path);
            if !is_compacting || wal_bytes < 64 * 1024 || std::time::Instant::now() >= deadline {
                return (wal_bytes, total_bytes);
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    })
    .await
    .expect("size polling completes")
}

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
