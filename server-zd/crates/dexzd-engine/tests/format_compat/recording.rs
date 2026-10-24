// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::fs;
use std::path::Path;
use std::sync::Arc;

use dexzd_cluster_metadata::{
    AttributeIndexDeclaration, METADATA_FORMAT_VERSION, register_attribute_indexes,
};
use dexzd_keyspace::{KEY_LAYOUT_VERSION, PartitionHasher};
use dexzd_object_store::InMemoryObjectStore;
use dexzd_protocol::dex::IndexType;
use dexzd_protocol::dexzd::v1::{ClusterObject, PartitionHashAlgorithm};
use prost::Message;

use crate::compat_node::{engine_node_config, start_engine_node};
use crate::scripted_clock::ScriptedClock;
use crate::seeded_id_source::SeededIdSource;
use crate::state_dump::{StateDump, dump_committed_state, render_state_dump};
use crate::testdata_tree::{STATE_DUMP_FILE, write_objects};
use crate::workload::{WORKLOAD_SEED, Workload, put_new_object};

pub(crate) const NODE_ID: &str = "node-a";
pub(crate) const NODE_INCARNATION: u32 = 1;
pub(crate) const PARTITION_COUNT: u32 = 4;
/// 2027-01-15T08:00:00Z.
const WORKLOAD_START_MICROS: i64 = 1_800_000_000_000_000;

/// Object storage and committed state after the scripted workload.
pub(crate) struct Recording {
    pub(crate) object_store: Arc<InMemoryObjectStore>,
    pub(crate) state: StateDump,
}

impl Recording {
    /// Writes the objects and the state dump into an empty `output_directory`.
    pub(crate) async fn write_to(&self, output_directory: &Path) {
        write_objects(&self.object_store, output_directory).await;
        fs::write(
            output_directory.join(STATE_DUMP_FILE),
            render_state_dump(&self.state),
        )
        .expect("write the state dump");
    }
}

/// Runs the scripted workload on a fresh cluster: commands, a snapshot, then more commands.
pub(crate) async fn record_workload() -> Recording {
    let clock = Arc::new(ScriptedClock::starting_at(WORKLOAD_START_MICROS));
    let ids = Arc::new(SeededIdSource::new(WORKLOAD_SEED, clock.clone()));
    let object_store = Arc::new(InMemoryObjectStore::new());
    let hasher = seed_cluster_object(&object_store, &clock, &ids).await;
    register_attribute_indexes(
        object_store.as_ref(),
        clock.as_ref(),
        0,
        &[AttributeIndexDeclaration {
            index_key: "status".to_owned(),
            index_type: IndexType::Keyword as i32,
        }],
    )
    .await
    .expect("register an Attribute index");
    let config = engine_node_config(NODE_ID, NODE_INCARNATION, PARTITION_COUNT);
    let node = start_engine_node(&config, object_store.clone(), clock.clone(), ids.clone()).await;
    let workload = Workload {
        node: &node,
        clock: &clock,
        ids: &ids,
        object_store: &object_store,
        hasher: &hasher,
    };
    let open_runs = workload.run_before_snapshot().await;
    node.take_snapshot().await.expect("the snapshot succeeds");
    workload.run_after_snapshot(open_runs).await;
    let state = dump_committed_state(
        node.committer()
            .committed_view()
            .expect("open a committed view"),
    )
    .await;
    node.shutdown().await.expect("the node releases its lease");
    Recording {
        object_store,
        state,
    }
}

/// Writes a cluster object with seeded identifiers, so bootstrap keeps it.
///
/// Returns the partition hasher it defines.
async fn seed_cluster_object(
    object_store: &InMemoryObjectStore,
    clock: &ScriptedClock,
    ids: &SeededIdSource,
) -> PartitionHasher {
    let now = dexzd_clock::WallClock::now_unix_micros(clock);
    let cluster_id = ids.next_uuid_v7();
    let partition_hash_key = ids.next_uuid_v7().into_bytes();
    let cluster = ClusterObject {
        format_version: METADATA_FORMAT_VERSION,
        cluster_id: cluster_id.as_bytes().to_vec(),
        partition_count: PARTITION_COUNT,
        partition_hash_algorithm: PartitionHashAlgorithm::Siphash24 as i32,
        partition_hash_key: partition_hash_key.to_vec(),
        key_layout_version: KEY_LAYOUT_VERSION,
        created_at_unix_micros: now,
    };
    put_new_object(object_store, "cluster", cluster.encode_to_vec()).await;
    PartitionHasher::new(&partition_hash_key, PARTITION_COUNT).expect("valid partition hasher")
}
