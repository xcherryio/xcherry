// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::collections::BTreeSet;
use std::sync::Arc;

use bytes::Bytes;
use dexzd_engine::SystemEngineIdSource;
use dexzd_object_store::{ObjectStore, WriteCondition};
use dexzd_protocol::dexzd::v1::{FlowCommand, Manifest, flow_command::Command};
use prost::Message;

use crate::compat_node::{engine_node_config, start_engine_node};
use crate::format_decoding::{DecodedObjects, decode_objects};
use crate::recording::record_workload;
use crate::scripted_clock::ScriptedClock;
use crate::state_dump::{
    StateDump, dump_committed_state, parse_state_dump, render_state_dump, state_dump_differences,
};
use crate::testdata_tree::{
    OBJECTS_DIRECTORY, STATE_DUMP_FILE, TestdataVersion, load_objects, read_files,
    recorded_objects, testdata_versions, version_digest,
};

/// Digest of every data file of each version. Versions are immutable: add `v<N+1>` instead.
const PINNED_VERSION_DIGESTS: &[(&str, &str)] = &[("v1", "f48ed41cc3e158adad535b3e44cc23d0")];
/// The restoring node starts this long after the last recorded command.
const RESTART_DELAY_MICROS: i64 = 60 * 60 * 1_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SnapshotUse {
    /// Restore the recorded snapshot, then replay the log after it.
    Restore,
    /// Ignore the snapshot and replay the whole log into an empty store.
    Ignore,
}

#[tokio::test(start_paused = true)]
async fn every_version_restores_its_snapshot_and_replays_to_the_recorded_state() {
    for version in recorded_versions() {
        assert_replays_to_recorded_state(&version, SnapshotUse::Restore).await;
    }
}

#[tokio::test(start_paused = true)]
async fn every_version_replays_its_whole_log_to_the_recorded_state() {
    for version in recorded_versions() {
        assert_replays_to_recorded_state(&version, SnapshotUse::Ignore).await;
    }
}

#[test]
fn every_version_decodes_with_the_current_protobuf_types() {
    let mut recorded_kinds = BTreeSet::new();
    for version in recorded_versions() {
        let decoded = decode_version(&version);
        assert!(
            decoded.log_batches > 0 && decoded.snapshot_rows > 0 && decoded.hard_delete_values > 0,
            "testdata/{} records log batches, a snapshot, and hard-delete values",
            version.name
        );
        assert!(
            decoded.widest_batch_frames > 1 && decoded.largest_frame_records > 1,
            "testdata/{} records a batch with several partition frames and a frame with several records",
            version.name
        );
        recorded_kinds.extend(decoded.commands.iter().map(command_kind));
    }
    let missing: Vec<&str> = ALL_COMMAND_KINDS
        .iter()
        .copied()
        .filter(|kind| !recorded_kinds.contains(kind))
        .collect();
    assert!(
        missing.is_empty(),
        "no testdata version records {missing:?}; extend the workload and add a new version"
    );
}

#[test]
fn every_version_keeps_its_pinned_bytes() {
    let versions = recorded_versions();
    let recorded: Vec<&str> = versions
        .iter()
        .map(|version| version.name.as_str())
        .collect();
    let pinned: Vec<&str> = PINNED_VERSION_DIGESTS
        .iter()
        .map(|(name, _)| *name)
        .collect();
    assert_eq!(
        recorded, pinned,
        "every testdata version has exactly one pinned digest"
    );
    for (version, (_, pinned_digest)) in versions.iter().zip(PINNED_VERSION_DIGESTS) {
        assert_eq!(
            version_digest(&version.directory),
            *pinned_digest,
            "testdata/{} changed; recorded versions are immutable, so add a new version instead",
            version.name
        );
    }
}

#[tokio::test(start_paused = true)]
async fn the_workload_records_identical_bytes_every_time() {
    let first = record_workload().await;
    let second = record_workload().await;
    assert!(
        recorded_objects(&first.object_store).await == recorded_objects(&second.object_store).await,
        "two recordings write identical objects"
    );
    assert_eq!(
        render_state_dump(&first.state),
        render_state_dump(&second.state),
        "two recordings commit identical state"
    );
}

fn recorded_versions() -> Vec<TestdataVersion> {
    let versions = testdata_versions();
    assert!(!versions.is_empty(), "testdata holds at least one version");
    versions
}

fn decode_version(version: &TestdataVersion) -> DecodedObjects {
    decode_objects(&read_files(&version.directory.join(OBJECTS_DIRECTORY)))
}

async fn assert_replays_to_recorded_state(version: &TestdataVersion, snapshot_use: SnapshotUse) {
    let recorded_text = std::fs::read_to_string(version.directory.join(STATE_DUMP_FILE))
        .expect("read the recorded state dump");
    let recorded = parse_state_dump(&recorded_text);
    let replayed = replay_version(version, snapshot_use).await;
    let differences = state_dump_differences(&recorded, &replayed);
    assert!(
        differences.is_empty(),
        "testdata/{} replayed with {snapshot_use:?} differs in {} rows:\n{}",
        version.name,
        differences.len(),
        differences.join("\n")
    );
}

/// Starts a node on the recorded objects and returns its committed state after recovery.
async fn replay_version(version: &TestdataVersion, snapshot_use: SnapshotUse) -> StateDump {
    let decoded = decode_version(version);
    let object_store = Arc::new(load_objects(&version.directory).await);
    if snapshot_use == SnapshotUse::Ignore {
        forget_snapshots(object_store.as_ref(), decoded.manifest.clone()).await;
    }
    let mut nodes = decoded.manifest.nodes.values();
    let node = nodes.next().expect("the manifest names the recording node");
    assert!(nodes.next().is_none(), "one node recorded the testdata");
    let last_command_micros = decoded
        .commands
        .iter()
        .map(|command| command.proposed_at_micros)
        .max()
        .expect("the log holds commands");
    let clock = Arc::new(ScriptedClock::starting_at(
        last_command_micros + RESTART_DELAY_MICROS,
    ));
    let config = engine_node_config(
        &node.node_id,
        node.incarnation,
        decoded.cluster.partition_count,
    );
    let engine =
        start_engine_node(&config, object_store, clock, Arc::new(SystemEngineIdSource)).await;
    let state = dump_committed_state(
        engine
            .committer()
            .committed_view()
            .expect("open a committed view"),
    )
    .await;
    engine
        .shutdown()
        .await
        .expect("the node releases its lease");
    state
}

/// Removes every snapshot reference, so recovery starts from an empty store at seq 1.
async fn forget_snapshots(object_store: &dyn ObjectStore, mut manifest: Manifest) {
    for partition in &mut manifest.partitions {
        partition.snapshot = None;
    }
    manifest.version += 1;
    object_store
        .put(
            "manifest",
            Bytes::from(manifest.encode_to_vec()),
            WriteCondition::Unconditional,
        )
        .await
        .expect("rewrite the manifest");
}

const ALL_COMMAND_KINDS: &[&str] = &[
    "FlowStart",
    "FlowStop",
    "FlowConfigUpdate",
    "AttributesSet",
    "ChannelPublish",
    "ChannelMessageDelete",
    "StepAttemptStarted",
    "StepAttemptHeartbeat",
    "StepAttemptFailed",
    "StepMethodCompleted",
    "RpcCommitted",
    "TimersFired",
    "TimerSkip",
    "TimeTravel",
    "InboxMessage",
    "OutboxAck",
    "HardDeleteObjectsDeleted",
    "RunPurge",
];

/// Names the command body; a new command kind fails to compile until it is listed here.
fn command_kind(command: &FlowCommand) -> &'static str {
    match command
        .command
        .as_ref()
        .expect("recorded commands have a body")
    {
        Command::FlowStart(_) => "FlowStart",
        Command::FlowStop(_) => "FlowStop",
        Command::FlowConfigUpdate(_) => "FlowConfigUpdate",
        Command::AttributesSet(_) => "AttributesSet",
        Command::ChannelPublish(_) => "ChannelPublish",
        Command::ChannelMessageDelete(_) => "ChannelMessageDelete",
        Command::StepAttemptStarted(_) => "StepAttemptStarted",
        Command::StepAttemptHeartbeat(_) => "StepAttemptHeartbeat",
        Command::StepAttemptFailed(_) => "StepAttemptFailed",
        Command::StepMethodCompleted(_) => "StepMethodCompleted",
        Command::RpcCommitted(_) => "RpcCommitted",
        Command::TimersFired(_) => "TimersFired",
        Command::TimerSkip(_) => "TimerSkip",
        Command::TimeTravel(_) => "TimeTravel",
        Command::InboxMessage(_) => "InboxMessage",
        Command::OutboxAck(_) => "OutboxAck",
        Command::HardDeleteObjectsDeleted(_) => "HardDeleteObjectsDeleted",
        Command::RunPurge(_) => "RunPurge",
    }
}
