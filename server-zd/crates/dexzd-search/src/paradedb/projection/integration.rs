// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use dexzd_apply::{FlowStateReader, StateReadError, StateRow};
use dexzd_keyspace::row_keys::{run_singleton_key, tenant_run_index_key};
use dexzd_keyspace::{ColumnFamily, FlowHash, IndexKind, PartitionId, RunId, RunTable, TenantId};
use dexzd_protocol::dex::FlowStatus;
use dexzd_protocol::dexzd::v1::{LogPosition, RunRecord, SearchDocumentRecord};
use dexzd_visibility_query::FieldRegistry;
use prost::Message;
use tokio_postgres::{Client, Config};

use super::super::ParadeDbSearchBackend;
use super::{ParadeDbProjector, ProjectionError, ProjectionMetricRecorder, SearchDocumentChange};
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
        self.0.insert(
            (
                ColumnFamily::Flow,
                run_singleton_key(flow_hash, run_id, RunTable::RunRecord),
            ),
            RunRecord {
                tenant_id: tenant_id.0,
                flow_id: record.flow_id.clone(),
                run_id: run_id.as_bytes().to_vec(),
                flow_type: record.flow_type.clone(),
                status: record.status,
                ..RunRecord::default()
            }
            .encode_to_vec(),
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

struct FailingDocuments {
    documents: CommittedDocuments,
    reads: AtomicUsize,
}

#[derive(Default)]
struct CountingProjectionMetrics(AtomicUsize);

impl ProjectionMetricRecorder for CountingProjectionMetrics {
    fn record_skipped_search_document(
        &self,
        partition_id: PartitionId,
        tenant_id: TenantId,
        _run_id: RunId,
    ) {
        assert_eq!(partition_id, PartitionId(4));
        assert_eq!(tenant_id, TenantId(17));
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

impl FlowStateReader for FailingDocuments {
    fn get(
        &self,
        column_family: ColumnFamily,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, StateReadError> {
        if self.reads.fetch_add(1, Ordering::Relaxed) == 1 {
            return Err(StateReadError {
                column_family,
                detail: "injected committed-view read failure".into(),
            });
        }
        self.documents.get(column_family, key)
    }

    fn scan(
        &self,
        column_family: ColumnFamily,
        start: &[u8],
        end: &[u8],
        limit: usize,
    ) -> Result<Vec<StateRow>, StateReadError> {
        self.documents.scan(column_family, start, end, limit)
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

fn position(sequence: u64) -> LogPosition {
    LogPosition {
        node_id: "projector-node".into(),
        incarnation: 1,
        seq: sequence,
    }
}

fn assert_send<T: Send>(_: T) {}

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

async fn skipped_reason(
    backend: &ParadeDbSearchBackend,
    tenant_id: TenantId,
    run_id: RunId,
) -> Option<String> {
    let slot = backend.lock_next_slot().await;
    let tenant_column = tenant_id.0.to_string();
    let run_column = run_id.to_string();
    slot.client
        .query_typed_opt(
            "SELECT reason FROM dexzd_visibility_skipped_documents WHERE tenant_id = $1::text AND run_id = $2::text",
            &[
                (&tenant_column as &(dyn tokio_postgres::types::ToSql + Sync), tokio_postgres::types::Type::TEXT),
                (&run_column, tokio_postgres::types::Type::TEXT),
            ],
        )
        .await
        .expect("read skipped document")
        .map(|row| row.get(0))
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
        let metrics = Arc::new(CountingProjectionMetrics::default());
        let projector = Arc::new(ParadeDbProjector::new_with_metric_recorder(
            backend.clone(),
            metrics.clone(),
        ));
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
        assert_eq!(schema_row.get::<_, i32>(0), 3);
        assert_eq!(projector.read_progress(PartitionId(4)).await.unwrap(), None);
        let mut locked_slot = backend.lock_next_slot().await;
        let table_lock = locked_slot
            .client
            .transaction()
            .await
            .expect("begin visibility table lock");
        table_lock
            .batch_execute("LOCK TABLE dexzd_visibility_runs IN ROW EXCLUSIVE MODE")
            .await
            .expect("hold visibility writer lock");
        let repeated_backend = tokio::time::timeout(
            Duration::from_secs(3),
            ParadeDbSearchBackend::connect(&configuration, 1, None, Duration::from_secs(2)),
        )
        .await
        .expect("current schema setup does not wait on visibility table")
        .expect("ensure existing schema idempotently");
        table_lock
            .rollback()
            .await
            .expect("release visibility table lock");
        drop(locked_slot);
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
                None,
                &position(10),
                &[upsert(tenant_id, first_run, &first_record)],
            )
            .await
            .expect("apply first committed batch");
        assert_eq!(
            projector.read_progress(partition_id).await.unwrap(),
            Some(position(10))
        );
        assert_eq!(
            listed_runs(&backend, tenant_id).await,
            [first_run.to_string()]
        );

        let changed_record = document(tenant_id, first_run, 2);
        projector
            .apply_batch(
                partition_id,
                None,
                &position(10),
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
            projector
                .apply_batch(partition_id, None, &position(13), &[])
                .await,
            Err(ProjectionError::PositionMismatch { .. })
        ));
        projector
            .apply_batch(
                partition_id,
                Some(&position(10)),
                &position(11),
                &[SearchDocumentChange::Upsert {
                    tenant_id,
                    run_id: first_run,
                    encoded_document: vec![0xff],
                }],
            )
            .await
            .expect("skip poison document and advance progress");
        assert_eq!(
            projector.read_progress(partition_id).await.unwrap(),
            Some(position(11))
        );
        assert!(listed_runs(&backend, tenant_id).await.is_empty());
        assert!(
            skipped_reason(&backend, tenant_id, first_run)
                .await
                .is_some_and(|reason| reason.contains("decode committed search document"))
        );
        assert_eq!(metrics.0.load(Ordering::Relaxed), 1);
        projector
            .apply_batch(partition_id, Some(&position(10)), &position(11), &[])
            .await
            .expect("skip replay without recording poison twice");
        assert_eq!(metrics.0.load(Ordering::Relaxed), 1);
        let second_record = document(tenant_id, second_run, 1);
        projector
            .apply_batch(
                partition_id,
                Some(&position(11)),
                &position(13),
                &[
                    upsert(tenant_id, first_run, &changed_record),
                    upsert(tenant_id, second_run, &second_record),
                ],
            )
            .await
            .expect("update and add runs");
        assert_eq!(
            projector.read_progress(partition_id).await.unwrap(),
            Some(position(13))
        );
        assert_eq!(skipped_reason(&backend, tenant_id, first_run).await, None);
        projector
            .apply_batch(
                partition_id,
                Some(&position(13)),
                &position(17),
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
                None,
                &position(8),
                &[upsert(tenant_id, other_run, &other_record)],
            )
            .await
            .expect("project another partition");
        let new_owner_position = LogPosition {
            node_id: "replacement-node".into(),
            incarnation: 0,
            seq: 1,
        };
        projector
            .apply_batch(
                other_partition_id,
                Some(&position(8)),
                &new_owner_position,
                &[],
            )
            .await
            .expect("advance after a node change and sequence restart");
        assert_eq!(
            projector.read_progress(other_partition_id).await.unwrap(),
            Some(new_owner_position)
        );
        let rebuilt_record = document(tenant_id, rebuilt_run, 1);
        let mut failing_documents = CommittedDocuments::default();
        failing_documents.insert(
            partition_id,
            tenant_id,
            FlowHash(8),
            rebuilt_run,
            &rebuilt_record,
        );
        let extra_run = RunId([7; 16]);
        failing_documents.insert(
            partition_id,
            tenant_id,
            FlowHash(9),
            extra_run,
            &document(tenant_id, extra_run, 1),
        );
        let failing_view: Arc<dyn FlowStateReader + Send + Sync> = Arc::new(FailingDocuments {
            documents: failing_documents,
            reads: AtomicUsize::new(0),
        });
        assert!(matches!(
            projector
                .rebuild_partition(partition_id, &position(19), failing_view)
                .await,
            Err(ProjectionError::StateRead(_))
        ));
        assert_eq!(
            projector.read_progress(partition_id).await.unwrap(),
            Some(position(17))
        );
        let existing_runs = listed_runs(&backend, tenant_id).await;
        assert!(existing_runs.contains(&second_run.to_string()));
        assert!(!existing_runs.contains(&rebuilt_run.to_string()));
        let mut committed_view = CommittedDocuments::default();
        committed_view.insert(
            partition_id,
            tenant_id,
            FlowHash(8),
            rebuilt_run,
            &rebuilt_record,
        );
        let purging_run = RunId([7; 16]);
        let purging_hash = FlowHash(11);
        committed_view.insert(
            partition_id,
            tenant_id,
            purging_hash,
            purging_run,
            &document(tenant_id, purging_run, 1),
        );
        committed_view.0.insert(
            (
                ColumnFamily::Flow,
                run_singleton_key(purging_hash, purging_run, RunTable::RunRecord),
            ),
            RunRecord {
                tenant_id: tenant_id.0,
                run_id: purging_run.as_bytes().to_vec(),
                is_purging: true,
                ..RunRecord::default()
            }
            .encode_to_vec(),
        );
        let poison_run = RunId([6; 16]);
        committed_view.insert(
            partition_id,
            tenant_id,
            FlowHash(10),
            poison_run,
            &document(TenantId(18), poison_run, 1),
        );
        let committed_view: Arc<dyn FlowStateReader + Send + Sync> = Arc::new(committed_view);
        assert_send(projector.rebuild_partition(
            partition_id,
            &position(19),
            committed_view.clone(),
        ));
        assert_send(projector.apply_batch(partition_id, Some(&position(17)), &position(19), &[]));
        projector
            .rebuild_partition(partition_id, &position(19), committed_view.clone())
            .await
            .expect("replace stale partition rows");
        assert_eq!(
            projector.read_progress(partition_id).await.unwrap(),
            Some(position(19))
        );
        let listed = listed_runs(&backend, tenant_id).await;
        assert_eq!(listed.len(), 2);
        assert!(listed.contains(&rebuilt_run.to_string()));
        assert!(listed.contains(&other_run.to_string()));
        assert!(!listed.contains(&poison_run.to_string()));
        assert!(!listed.contains(&purging_run.to_string()));
        assert!(
            skipped_reason(&backend, tenant_id, poison_run)
                .await
                .is_some()
        );
        assert_eq!(metrics.0.load(Ordering::Relaxed), 2);
        projector
            .apply_batch(
                partition_id,
                Some(&position(19)),
                &position(20),
                &[SearchDocumentChange::Delete {
                    tenant_id,
                    run_id: poison_run,
                }],
            )
            .await
            .expect("clear skipped document on purge");
        assert_eq!(skipped_reason(&backend, tenant_id, poison_run).await, None);
        assert!(matches!(
            projector
                .rebuild_partition(partition_id, &position(17), committed_view.clone())
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
        if pool_size == 2 {
            let released_slot = held_slots.pop().expect("two slots were acquired");
            let (runs, ()) = tokio::time::timeout(Duration::from_secs(2), async {
                tokio::join!(listed_runs(&backend, tenant_id), async {
                    tokio::task::yield_now().await;
                    drop(released_slot);
                })
            })
            .await
            .expect("queued search takes the first released slot");
            assert_eq!(runs.len(), 2);
            held_slots.push(backend.lock_next_slot().await);
        }
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
                for sequence in 21..=39 {
                    projection_writer
                        .apply_batch(
                            partition_id,
                            Some(&position(sequence - 1)),
                            &position(sequence),
                            &[],
                        )
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
            Some(position(39))
        );
        if pool_size == 2 {
            let previous = position(39);
            let batch_position = position(43);
            let rebuild_position = position(44);
            let (batch_result, rebuild_result) =
                tokio::time::timeout(Duration::from_secs(10), async {
                    tokio::join!(
                        projector.apply_batch(partition_id, Some(&previous), &batch_position, &[],),
                        projector.rebuild_partition(
                            partition_id,
                            &rebuild_position,
                            committed_view.clone(),
                        )
                    )
                })
                .await
                .expect("same-partition writers serialize");
            batch_result.expect("batch writer completes");
            rebuild_result.expect("rebuild writer completes");
            assert_eq!(
                projector.read_progress(partition_id).await.unwrap(),
                Some(position(44))
            );
        }

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
                "DROP TABLE {schema_name}.dexzd_visibility_skipped_documents; \
                 UPDATE {schema_name}.dexzd_visibility_schema_version SET version = 2 WHERE id = 1"
            ))
            .await
            .expect("simulate schema version without skipped-document records");
        let version_two_backend = Arc::new(
            ParadeDbSearchBackend::connect(&configuration, 1, None, Duration::from_secs(2))
                .await
                .expect("upgrade schema version two"),
        );
        let version_two_projector = ParadeDbProjector::new(version_two_backend.clone());
        assert_eq!(
            version_two_projector
                .read_progress(partition_id)
                .await
                .unwrap(),
            Some(position(if pool_size == 2 { 44 } else { 39 }))
        );
        assert_eq!(listed_runs(&version_two_backend, tenant_id).await.len(), 2);
        drop(version_two_projector);
        drop(version_two_backend);

        admin_client
            .batch_execute(&format!(
                "ALTER TABLE {schema_name}.dexzd_visibility_runs DROP COLUMN partition_id CASCADE; \
                 ALTER TABLE {schema_name}.dexzd_visibility_progress DROP COLUMN applied_node_id; \
                 ALTER TABLE {schema_name}.dexzd_visibility_progress DROP COLUMN applied_incarnation; \
                 ALTER TABLE {schema_name}.dexzd_visibility_progress DROP COLUMN applied_seq; \
                 UPDATE {schema_name}.dexzd_visibility_schema_version SET version = 1 WHERE id = 1"
            ))
            .await
            .expect("simulate an older visibility schema");
        let upgraded_backend = Arc::new(
            ParadeDbSearchBackend::connect(&configuration, 1, None, Duration::from_secs(2))
                .await
                .expect("upgrade an existing visibility schema"),
        );
        let upgraded_projector = ParadeDbProjector::new(upgraded_backend.clone());
        assert_eq!(
            upgraded_projector
                .read_progress(partition_id)
                .await
                .unwrap(),
            None
        );
        assert!(listed_runs(&upgraded_backend, tenant_id).await.is_empty());
        drop(upgraded_projector);
        drop(upgraded_backend);

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
