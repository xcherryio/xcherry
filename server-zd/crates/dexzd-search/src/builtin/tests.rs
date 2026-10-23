// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use dexzd_apply::{FlowStateReader, StateReadError, StateRow};
use dexzd_keyspace::row_keys::{flow_list_index_key, run_singleton_key, tenant_run_index_key};
use dexzd_keyspace::{ColumnFamily, FlowHash, IndexKind, PartitionId, RunId, RunTable, TenantId};
use dexzd_protocol::dex::{self, FlowStatus, IndexType};
use dexzd_protocol::dexzd::v1::{SearchAttributeValue, SearchDocumentRecord};
use dexzd_visibility_query::{
    FieldRegistry, SearchDocument, compare_documents, evaluate, parse_and_check,
};
use prost::Message;

use super::{BuiltinSearchBackend, IndexedDocument};
use crate::{SearchBackend, SearchError, SearchRequest};

#[derive(Default)]
struct MemoryState {
    rows: BTreeMap<(ColumnFamily, Vec<u8>), Vec<u8>>,
    scanned_kinds: Mutex<Vec<u8>>,
    scan_batch_limit: AtomicUsize,
}

impl MemoryState {
    fn insert(
        &mut self,
        partition: u16,
        tenant_id: u64,
        ordinal: u8,
        record: SearchDocumentRecord,
    ) {
        let tenant = TenantId(tenant_id);
        let partition = PartitionId(partition);
        let flow_hash = FlowHash((u64::from(partition.0) << 48) | u64::from(ordinal));
        let run_id = RunId::from_slice(&record.run_id).expect("fixture run ID");
        self.rows.insert(
            (
                ColumnFamily::Flow,
                run_singleton_key(flow_hash, run_id, RunTable::SearchDocument),
            ),
            record.encode_to_vec(),
        );
        self.rows.insert(
            (
                ColumnFamily::PartitionIndex,
                tenant_run_index_key(
                    partition,
                    IndexKind::SearchDocument,
                    tenant,
                    flow_hash,
                    run_id,
                ),
            ),
            record.version.to_be_bytes().to_vec(),
        );
        self.rows.insert(
            (
                ColumnFamily::PartitionIndex,
                flow_list_index_key(
                    partition,
                    tenant,
                    &record.flow_type,
                    u8::try_from(record.status).expect("fixture status"),
                    flow_hash,
                    run_id,
                ),
            ),
            Vec::new(),
        );
    }
}

impl FlowStateReader for MemoryState {
    fn get(
        &self,
        column_family: ColumnFamily,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, StateReadError> {
        Ok(self.rows.get(&(column_family, key.to_vec())).cloned())
    }

    fn scan(
        &self,
        column_family: ColumnFamily,
        start: &[u8],
        end: &[u8],
        limit: usize,
    ) -> Result<Vec<StateRow>, StateReadError> {
        if column_family == ColumnFamily::PartitionIndex {
            self.scanned_kinds
                .lock()
                .expect("fixture scan lock")
                .push(start[2]);
        }
        let batch_limit = self.scan_batch_limit.load(Ordering::Relaxed);
        let limit = if batch_limit == 0 {
            limit
        } else {
            limit.min(batch_limit)
        };
        Ok(self
            .rows
            .range((column_family, start.to_vec())..(column_family, end.to_vec()))
            .take(limit)
            .map(|((_, key), value)| (key.clone(), value.clone()))
            .collect())
    }
}

fn record(
    tenant_id: u64,
    ordinal: u8,
    flow_type: &str,
    status: FlowStatus,
    start_micros: i64,
    close_micros: i64,
) -> SearchDocumentRecord {
    let mut run_id = [0; 16];
    run_id[15] = ordinal;
    SearchDocumentRecord {
        version: 1,
        tenant_id,
        flow_id: format!("flow-{ordinal}"),
        run_id: run_id.to_vec(),
        flow_type: flow_type.into(),
        status: status.into(),
        started_at_micros: start_micros,
        closed_at_micros: close_micros,
        ..Default::default()
    }
}

fn fields() -> FieldRegistry {
    let mut fields = FieldRegistry::new();
    fields.insert("Color", IndexType::Keyword);
    fields.insert("Tags", IndexType::KeywordArray);
    fields.insert("Description", IndexType::Text);
    fields
}

fn request<'a>(
    query: &'a str,
    token: Option<&'a str>,
    page_size: usize,
    fields: &'a FieldRegistry,
) -> SearchRequest<'a> {
    SearchRequest {
        tenant_id: TenantId(7),
        query,
        page_size,
        page_token: token,
        field_registry: fields,
    }
}

fn fixture() -> (Arc<MemoryState>, Vec<SearchDocumentRecord>) {
    let mut state = MemoryState::default();
    let records = vec![
        record(7, 1, "Checkout", FlowStatus::Running, 10, 0),
        record(7, 2, "Billing", FlowStatus::Completed, 20, 40),
        record(7, 3, "Checkout", FlowStatus::Failed, 30, 30),
    ];
    for (index, record) in records.iter().cloned().enumerate() {
        state.insert(index as u16, 7, (index + 1) as u8, record);
    }
    state.insert(
        1,
        8,
        99,
        record(8, 99, "Checkout", FlowStatus::Completed, 100, 100),
    );
    (Arc::new(state), records)
}

#[tokio::test]
async fn searches_all_partitions_and_pages_in_default_order() {
    let (state, _) = fixture();
    state.scan_batch_limit.store(1, Ordering::Relaxed);
    let backend = BuiltinSearchBackend::new(state.clone(), 3).expect("backend");
    let fields = fields();
    let first = backend
        .search(request("", None, 2, &fields))
        .await
        .expect("first page");
    assert_eq!(
        first
            .entries
            .iter()
            .map(|entry| entry.flow_id.as_str())
            .collect::<Vec<_>>(),
        vec!["flow-1", "flow-2"]
    );
    assert_eq!(first.entries[0].flow_status, FlowStatus::Running as i32);
    assert_eq!(
        first.entries[0].start_time.as_ref().expect("start").nanos,
        10_000
    );
    assert!(first.entries[0].close_time.is_none());
    let second = backend
        .search(request("", first.next_page_token.as_deref(), 2, &fields))
        .await
        .expect("second page");
    assert_eq!(
        second
            .entries
            .iter()
            .map(|entry| entry.flow_id.as_str())
            .collect::<Vec<_>>(),
        vec!["flow-3"]
    );
    assert!(second.next_page_token.is_none());
    let pinned = backend
        .search(request("FlowType = 'Checkout'", None, 10, &fields))
        .await
        .expect("pinned search");
    assert_eq!(
        pinned
            .entries
            .iter()
            .map(|entry| entry.flow_id.as_str())
            .collect::<Vec<_>>(),
        vec!["flow-1", "flow-3"]
    );
    assert!(
        state
            .scanned_kinds
            .lock()
            .expect("scan kinds")
            .contains(&(IndexKind::FlowList as u8))
    );
    let ordered = backend
        .search(request("ORDER BY StartTime DESC", None, 10, &fields))
        .await
        .expect("ordered search");
    assert_eq!(
        ordered
            .entries
            .iter()
            .map(|entry| entry.flow_id.as_str())
            .collect::<Vec<_>>(),
        vec!["flow-3", "flow-2", "flow-1"]
    );
}

#[tokio::test]
async fn rejects_changed_and_corrupt_tokens() {
    let (state, _) = fixture();
    let backend = BuiltinSearchBackend::new(state, 3).expect("backend");
    let fields = fields();
    let first = backend
        .search(request("", None, 1, &fields))
        .await
        .expect("first page");
    let token = first.next_page_token.expect("next token");
    assert!(matches!(
        backend
            .search(request("ORDER BY StartTime", Some(&token), 1, &fields))
            .await,
        Err(SearchError::InvalidArgument(_))
    ));
    let mut corrupt = token.into_bytes();
    corrupt[42] = if corrupt[42] == b'0' { b'1' } else { b'0' };
    let corrupt = String::from_utf8(corrupt).expect("ASCII token");
    assert!(matches!(
        backend
            .search(request("", Some(&corrupt), 1, &fields))
            .await,
        Err(SearchError::InvalidArgument(_))
    ));
    assert!(matches!(
        backend.search(request("", None, 0, &fields)).await,
        Err(SearchError::InvalidArgument(_))
    ));
}

#[tokio::test]
async fn enforces_scan_budget_and_tenant_scope() {
    let (state, _) = fixture();
    let backend = BuiltinSearchBackend::with_scan_budget(state, 3, 2).expect("backend");
    let fields = fields();
    assert!(
        matches!(backend.search(request("", None, 10, &fields)).await, Err(SearchError::ResourceExhausted(message)) if message.contains("ParadeDB"))
    );
    let pinned = backend
        .search(request("FlowType = 'Billing'", None, 10, &fields))
        .await
        .expect("pinned under budget");
    assert_eq!(pinned.entries.len(), 1);
    assert_eq!(pinned.entries[0].flow_id, "flow-2");
}

#[tokio::test]
async fn applies_compatibility_and_attribute_values() {
    let mut state = MemoryState::default();
    let mut running = record(7, 1, "Checkout", FlowStatus::Running, 1, 0);
    running.work_queue_permissions = vec!["charge".into()];
    running.attributes = vec![
        SearchAttributeValue {
            index_key: "Color".into(),
            index_type: IndexType::Keyword.into(),
            value: Some(dex::Value {
                kind: Some(dex::value::Kind::StringValue("red".into())),
            }),
        },
        SearchAttributeValue {
            index_key: "Tags".into(),
            index_type: IndexType::KeywordArray.into(),
            value: Some(dex::Value {
                kind: Some(dex::value::Kind::ObjValue(dex::EncodedObject {
                    encoding: "json".into(),
                    payload: br#"["charge","ship"]"#.to_vec(),
                })),
            }),
        },
    ];
    state.insert(0, 7, 1, running);
    state.insert(
        1,
        7,
        2,
        record(7, 2, "Checkout", FlowStatus::ContinuedAsNew, 2, 3),
    );
    let backend = BuiltinSearchBackend::new(Arc::new(state), 2).expect("backend");
    let fields = fields();
    let engine = backend
        .search(request("WorkflowType = 'Engine'", None, 10, &fields))
        .await
        .expect("Engine compatibility");
    assert_eq!(engine.entries.len(), 2);
    assert_eq!(engine.entries[0].indexed_attributes[0].key, "Color");
    assert_eq!(
        engine.entries[0].indexed_attributes[0]
            .value
            .as_ref()
            .expect("wire value")
            .kind,
        Some(dex::value::Kind::StringValue("red".into()))
    );
    let permissions = engine.entries[0]
        .indexed_attributes
        .iter()
        .find(|attribute| attribute.key == "DexWorkQueuePermissions")
        .expect("permission index value");
    assert!(matches!(
        permissions.value.as_ref().and_then(|value| value.kind.as_ref()),
        Some(dex::value::Kind::ObjValue(object)) if object.payload == br#"["charge"]"#
    ));
    let continued = backend
        .search(request(
            "ExecutionStatus = 'ContinuedAsNew'",
            None,
            10,
            &fields,
        ))
        .await
        .expect("status compatibility");
    assert!(continued.entries.is_empty());
    let array = backend
        .search(request("Tags = 'ship'", None, 10, &fields))
        .await
        .expect("array containment");
    assert_eq!(array.entries.len(), 1);
}

#[tokio::test]
async fn seeded_documents_match_the_shared_evaluator() {
    let mut state = MemoryState::default();
    let mut records = Vec::new();
    for ordinal in 1..=60 {
        let status = if ordinal % 5 == 0 {
            FlowStatus::Running
        } else {
            FlowStatus::Completed
        };
        let flow_type = if ordinal % 3 == 0 {
            "Billing"
        } else {
            "Checkout"
        };
        let close = if status == FlowStatus::Running {
            0
        } else {
            i64::from(ordinal) * 10
        };
        let mut document = record(7, ordinal, flow_type, status, i64::from(ordinal), close);
        if ordinal % 4 != 0 {
            document.attributes.push(SearchAttributeValue {
                index_key: "Color".into(),
                index_type: IndexType::Keyword.into(),
                value: Some(dex::Value {
                    kind: Some(dex::value::Kind::StringValue(
                        if ordinal % 2 == 0 { "red" } else { "blue" }.into(),
                    )),
                }),
            });
        }
        state.insert(u16::from(ordinal % 3), 7, ordinal, document.clone());
        records.push(document);
    }
    let backend = BuiltinSearchBackend::new(Arc::new(state), 3).expect("backend");
    let fields = fields();
    for text in [
        "",
        "FlowType = 'Checkout'",
        "Color = 'red'",
        "Color IS NULL",
        "NOT Color = 'red'",
        "ExecutionStatus = 'Running'",
        "FlowType = 'Billing' AND Color != 'blue' ORDER BY StartTime ASC",
        "ORDER BY CloseTime DESC",
    ] {
        let query = parse_and_check(text, &fields).expect("checked query");
        let mut expected = records
            .iter()
            .map(|record| {
                let run_id = RunId::from_slice(&record.run_id).expect("run ID");
                IndexedDocument::decode(&record.encode_to_vec(), TenantId(7), run_id)
                    .expect("document")
            })
            .filter(|document| evaluate(&query, document))
            .collect::<Vec<_>>();
        expected.sort_by(|left, right| compare_documents(&query, left, right));
        let expected_ids = expected
            .iter()
            .map(|document| document.field_value("RunId").expect("ID"))
            .collect::<Vec<_>>();
        let mut actual_ids = Vec::new();
        let mut token = None;
        loop {
            let page = backend
                .search(request(text, token.as_deref(), 7, &fields))
                .await
                .expect(text);
            actual_ids.extend(
                page.entries
                    .iter()
                    .map(|entry| dexzd_visibility_query::SearchValue::String(entry.run_id.clone())),
            );
            token = page.next_page_token;
            if token.is_none() {
                break;
            }
        }
        assert_eq!(actual_ids, expected_ids, "{text}");
    }
}
