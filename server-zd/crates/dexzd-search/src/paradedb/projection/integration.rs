// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use dexzd_apply::{FlowStateReader, StateReadError, StateRow};
use dexzd_keyspace::row_keys::{run_singleton_key, tenant_run_index_key};
use dexzd_keyspace::{ColumnFamily, FlowHash, IndexKind, PartitionId, RunId, RunTable, TenantId};
use dexzd_protocol::dex::FlowStatus;
use dexzd_protocol::dexzd::v1::SearchDocumentRecord;
use dexzd_visibility_query::FieldRegistry;
use prost::Message;
use tokio_postgres::{Client, Config};

use super::super::ParadeDbSearchBackend;
use super::{ParadeDbProjector, ProjectionError, SearchDocumentChange};
use crate::{SearchBackend, SearchError, SearchRequest};

#[derive(Default)]
struct CommittedDocuments(BTreeMap<(ColumnFamily, Vec<u8>), Vec<u8>>);

impl CommittedDocuments {
    fn insert(
        &mut self,
        partition_id: PartitionId,
        tenant_id: TenantId,
        flow_hash: FlowHash,
        run_id: RunId,
        record: &SearchDocumentRecord,
    ) {
        self.0.insert(
            (
                ColumnFamily::PartitionIndex,
                tenant_run_index_key(
                    partition_id,
                    IndexKind::SearchDocument,
                    tenant_id,
                    flow_hash,
                    run_id,
                ),
            ),
            record.version.to_be_bytes().to_vec(),
        );
        self.0.insert(
            (
                ColumnFamily::Flow,
                run_singleton_key(flow_hash, run_id, RunTable::SearchDocument),
            ),
            record.encode_to_vec(),
        );
    }
}

impl FlowStateReader for CommittedDocuments {
    fn get(
        &self,
        column_family: ColumnFamily,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, StateReadError> {
        Ok(self.0.get(&(column_family, key.to_vec())).cloned())
    }

    fn scan(
        &self,
        column_family: ColumnFamily,
        start: &[u8],
        end: &[u8],
        limit: usize,
    ) -> Result<Vec<StateRow>, StateReadError> {
        Ok(self
            .0
            .range((column_family, start.to_vec())..(column_family, end.to_vec()))
            .take(limit)
            .map(|((_, key), value)| (key.clone(), value.clone()))
            .collect())
    }
}

fn document(tenant_id: TenantId, run_id: RunId, version: u64) -> SearchDocumentRecord {
    SearchDocumentRecord {
        version,
        tenant_id: tenant_id.0,
        flow_id: format!("flow-{run_id}"),
        run_id: run_id.as_bytes().to_vec(),
        flow_type: "ProjectorFlow".into(),
        status: FlowStatus::Running as i32,
        started_at_micros: 1_000_000,
        ..Default::default()
    }
}

fn upsert(
    tenant_id: TenantId,
    run_id: RunId,
    record: &SearchDocumentRecord,
) -> SearchDocumentChange {
    SearchDocumentChange::Upsert {
        tenant_id,
        run_id,
        encoded_document: record.encode_to_vec(),
    }
}

async fn listed_runs(backend: &ParadeDbSearchBackend, tenant_id: TenantId) -> Vec<String> {
    backend
        .search(SearchRequest {
            tenant_id,
            query: "",
            page_size: 20,
            page_token: None,
            field_registry: &FieldRegistry::new(),
        })
        .await
        .expect("search projected documents")
        .entries
        .into_iter()
        .map(|entry| entry.run_id)
        .collect()
}

#[cfg(test)]
pub(crate) async fn run_projector_scenarios_for_test(
    database_url: &str,
    admin_client: &Client,
    nonce: u128,
) {
    for pool_size in [1, 2] {
        let schema_name = format!(
            "dexzd_projector_{}_{}_{}",
            std::process::id(),
            nonce,
            pool_size
        );
        admin_client
            .batch_execute(&format!("CREATE SCHEMA {schema_name}"))
            .await
            .expect("create empty projector schema");
        let mut configuration: Config = database_url.parse().expect("parse CI ParadeDB URL");
        configuration.options(format!("-c search_path={schema_name},public,pdb"));
        let backend = Arc::new(
            ParadeDbSearchBackend::connect(&configuration, pool_size, None, Duration::from_secs(2))
                .await
                .expect("create schema on empty database"),
        );
        let projector = Arc::new(ParadeDbProjector::new(backend.clone()));
        let schema_row = {
            let slot = backend.lock_next_slot().await;
            slot.client
                .query_typed_one(
                    "SELECT version FROM dexzd_visibility_schema_version WHERE id = 1",
                    &[],
                )
                .await
                .expect("read created schema version")
        };
        assert_eq!(schema_row.get::<_, i32>(0), 1);
        assert_eq!(projector.read_progress(PartitionId(4)).await.unwrap(), None);
        let repeated_backend =
            ParadeDbSearchBackend::connect(&configuration, 1, None, Duration::from_secs(2))
                .await
                .expect("ensure existing schema idempotently");
        drop(repeated_backend);

        let partition_id = PartitionId(4);
        let other_partition_id = PartitionId(5);
        let tenant_id = TenantId(17);
        let first_run = RunId([1; 16]);
        let second_run = RunId([2; 16]);
        let rebuilt_run = RunId([3; 16]);
        let other_run = RunId([4; 16]);
        let first_record = document(tenant_id, first_run, 1);
        projector
            .apply_batch(
                partition_id,
                1,
                &[upsert(tenant_id, first_run, &first_record)],
            )
            .await
            .expect("apply first committed batch");
        assert_eq!(
            projector.read_progress(partition_id).await.unwrap(),
            Some(1)
        );
        assert_eq!(
            listed_runs(&backend, tenant_id).await,
            [first_run.to_string()]
        );

        let changed_record = document(tenant_id, first_run, 2);
        projector
            .apply_batch(
                partition_id,
                1,
                &[upsert(tenant_id, first_run, &changed_record)],
            )
            .await
            .expect("ignore replayed position");
        let version_row = {
            let slot = backend.lock_next_slot().await;
            slot.client
                .query_typed_one(
                    "SELECT version FROM dexzd_visibility_runs WHERE run_id = $1::text",
                    &[(
                        &first_run.to_string() as &(dyn tokio_postgres::types::ToSql + Sync),
                        tokio_postgres::types::Type::TEXT,
                    )],
                )
                .await
                .expect("read preserved run version")
        };
        assert_eq!(version_row.get::<_, i64>(0), 1);
        assert!(matches!(
            projector.apply_batch(partition_id, 3, &[]).await,
            Err(ProjectionError::PositionGap { expected: 2, .. })
        ));
        assert!(matches!(
            projector
                .apply_batch(
                    partition_id,
                    2,
                    &[SearchDocumentChange::Upsert {
                        tenant_id,
                        run_id: first_run,
                        encoded_document: vec![0xff],
                    }],
                )
                .await,
            Err(ProjectionError::InvalidState(_))
        ));
        assert_eq!(
            projector.read_progress(partition_id).await.unwrap(),
            Some(1)
        );
        assert_eq!(
            listed_runs(&backend, tenant_id).await,
            [first_run.to_string()]
        );
        let second_record = document(tenant_id, second_run, 1);
        projector
            .apply_batch(
                partition_id,
                2,
                &[
                    upsert(tenant_id, first_run, &changed_record),
                    upsert(tenant_id, second_run, &second_record),
                ],
            )
            .await
            .expect("update and add runs");
        assert_eq!(
            projector.read_progress(partition_id).await.unwrap(),
            Some(2)
        );
        projector
            .apply_batch(
                partition_id,
                3,
                &[SearchDocumentChange::Delete {
                    tenant_id,
                    run_id: first_run,
                }],
            )
            .await
            .expect("delete purged run");
        assert_eq!(
            listed_runs(&backend, tenant_id).await,
            [second_run.to_string()]
        );

        let other_record = document(tenant_id, other_run, 1);
        projector
            .apply_batch(
                other_partition_id,
                1,
                &[upsert(tenant_id, other_run, &other_record)],
            )
            .await
            .expect("project another partition");
        let rebuilt_record = document(tenant_id, rebuilt_run, 1);
        let mut committed_view = CommittedDocuments::default();
        committed_view.insert(
            partition_id,
            tenant_id,
            FlowHash(8),
            rebuilt_run,
            &rebuilt_record,
        );
        projector
            .rebuild_partition(partition_id, 4, &committed_view)
            .await
            .expect("replace stale partition rows");
        assert_eq!(
            projector.read_progress(partition_id).await.unwrap(),
            Some(4)
        );
        let listed = listed_runs(&backend, tenant_id).await;
        assert_eq!(listed.len(), 2);
        assert!(listed.contains(&rebuilt_run.to_string()));
        assert!(listed.contains(&other_run.to_string()));
        assert!(matches!(
            projector
                .rebuild_partition(partition_id, 3, &committed_view)
                .await,
            Err(ProjectionError::StaleRebuild { .. })
        ));

        let mut held_slots = Vec::new();
        for _ in 0..pool_size {
            held_slots.push(backend.lock_next_slot().await);
        }
        let deadline = tokio::time::timeout(
            Duration::from_millis(20),
            backend.search(SearchRequest {
                tenant_id,
                query: "",
                page_size: 1,
                page_token: None,
                field_registry: &FieldRegistry::new(),
            }),
        )
        .await;
        assert!(deadline.is_err());
        let search_backend = backend.clone();
        let projection_writer = projector.clone();
        tokio::time::timeout(Duration::from_secs(10), async move {
            let searching = async {
                for _ in 0..20 {
                    let runs = listed_runs(&search_backend, tenant_id).await;
                    assert_eq!(runs.len(), 2);
                }
            };
            let projecting = async {
                for position in 5..=24 {
                    projection_writer
                        .apply_batch(partition_id, position, &[])
                        .await
                        .expect("queue projection behind busy pool");
                }
            };
            let release = async {
                tokio::task::yield_now().await;
                drop(held_slots);
            };
            tokio::join!(searching, projecting, release);
        })
        .await
        .expect("search and projection complete with a small shared pool");
        assert_eq!(
            projector.read_progress(partition_id).await.unwrap(),
            Some(24)
        );

        let mut inspected_slots = Vec::new();
        for _ in 0..pool_size {
            let slot = backend.lock_next_slot().await;
            let prepared = slot
                .client
                .query_typed_one("SELECT count(*) FROM pg_prepared_statements", &[])
                .await
                .expect("inspect named prepared statements");
            assert_eq!(prepared.get::<_, i64>(0), 0);
            inspected_slots.push(slot);
        }
        drop(inspected_slots);
        drop(projector);
        drop(backend);

        admin_client
            .batch_execute(&format!(
                "UPDATE {schema_name}.dexzd_visibility_schema_version SET version = 99 WHERE id = 1"
            ))
            .await
            .expect("set future schema version");
        let newer_schema =
            ParadeDbSearchBackend::connect(&configuration, 1, None, Duration::from_secs(2)).await;
        assert!(
            matches!(newer_schema, Err(SearchError::Internal(message)) if message.contains("version 99"))
        );
        admin_client
            .batch_execute(&format!("DROP SCHEMA {schema_name} CASCADE"))
            .await
            .expect("drop projector schema");
    }
}
