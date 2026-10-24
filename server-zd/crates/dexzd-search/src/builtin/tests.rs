// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use dexzd_apply::{FlowStateReader, StateReadError, StateRow};
use dexzd_keyspace::row_keys::{flow_list_index_key, run_singleton_key, tenant_run_index_key};
use dexzd_keyspace::{ColumnFamily, FlowHash, IndexKind, PartitionId, RunId, RunTable, TenantId};
use dexzd_protocol::dex::{self, FlowStatus, IndexType};
use dexzd_protocol::dexzd::v1::{SearchAttributeValue, SearchDocumentRecord};
use dexzd_visibility_query::{
    FieldRegistry, SearchDocument, compare_documents, evaluate, parse_and_check,
};
use prost::Message;

use super::{BuiltinSearchBackend, CommittedSearchViewProvider, IndexedDocument};
use crate::{SearchBackend, SearchError, SearchRequest};

#[derive(Default)]
struct MemoryState {
    rows: BTreeMap<(ColumnFamily, Vec<u8>), Vec<u8>>,
    scanned_kinds: Mutex<Vec<u8>>,
    scan_batch_limit: AtomicUsize,
    scan_hook: Mutex<Option<Box<dyn Fn() + Send + Sync>>>,
}

struct MemoryViewProvider {
    view: RwLock<Arc<MemoryState>>,
    openings: AtomicUsize,
}

impl MemoryViewProvider {
    fn new(view: Arc<MemoryState>) -> Arc<Self> {
        Arc::new(Self {
            view: RwLock::new(view),
            openings: AtomicUsize::new(0),
        })
    }

    fn replace(&self, view: Arc<MemoryState>) {
        *self.view.write().expect("view lock") = view;
    }
}

impl CommittedSearchViewProvider for MemoryViewProvider {
    fn committed_view(&self) -> Result<Arc<dyn FlowStateReader + Send + Sync>, SearchError> {
        self.openings.fetch_add(1, Ordering::Relaxed);
        let view = self.view.read().expect("view lock").clone();
        Ok(view)
    }
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
        let scan_hook = self.scan_hook.lock().expect("scan hook lock").take();
        if let Some(scan_hook) = scan_hook {
            scan_hook();
        }
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
    let backend =
        BuiltinSearchBackend::new(MemoryViewProvider::new(state.clone()), 3).expect("backend");
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
    let backend = BuiltinSearchBackend::new(MemoryViewProvider::new(state), 3).expect("backend");
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
    assert_eq!(
        backend
            .search(request("", None, 0, &fields))
            .await
            .expect("default page")
            .entries
            .len(),
        3
    );
}

#[tokio::test]
async fn defaults_and_clamps_page_size() {
    let mut state = MemoryState::default();
    for partition in 0..4_u16 {
        for ordinal in 1..=251_u8 {
            let mut document = record(
                7,
                ordinal,
                "Checkout",
                FlowStatus::Running,
                i64::from(ordinal),
                0,
            );
            document.run_id[14] = partition as u8;
            state.insert(partition, 7, ordinal, document);
        }
    }
    let backend =
        BuiltinSearchBackend::new(MemoryViewProvider::new(Arc::new(state)), 4).expect("backend");
    let fields = fields();
    for requested_size in [0, usize::MAX] {
        let first = backend
            .search(request("", None, requested_size, &fields))
            .await
            .expect("first bounded page");
        assert_eq!(first.entries.len(), crate::search_backend::MAX_PAGE_SIZE);
        let token = first.next_page_token.expect("remaining runs");
        let second = backend
            .search(request("", Some(&token), requested_size, &fields))
            .await
            .expect("second bounded page");
        assert_eq!(second.entries.len(), 4);
        assert!(second.next_page_token.is_none());
    }
}

#[tokio::test]
async fn each_search_uses_one_consistent_committed_view() {
    let mut first_state = MemoryState::default();
    first_state.insert(0, 7, 1, record(7, 1, "Checkout", FlowStatus::Running, 1, 0));
    first_state.insert(1, 7, 2, record(7, 2, "Checkout", FlowStatus::Running, 2, 0));
    let first_state = Arc::new(first_state);
    let mut next_state = MemoryState::default();
    next_state.insert(0, 7, 3, record(7, 3, "Checkout", FlowStatus::Running, 3, 0));
    let next_state = Arc::new(next_state);
    let provider = MemoryViewProvider::new(first_state.clone());
    first_state
        .scan_hook
        .lock()
        .expect("scan hook lock")
        .replace(Box::new({
            let provider = provider.clone();
            move || provider.replace(next_state.clone())
        }));
    let backend = BuiltinSearchBackend::new(provider.clone(), 2).expect("backend");
    let fields = fields();
    let first = backend
        .search(request("", None, 10, &fields))
        .await
        .expect("first committed view");
    assert_eq!(
        first
            .entries
            .iter()
            .map(|entry| entry.flow_id.as_str())
            .collect::<Vec<_>>(),
        ["flow-2", "flow-1"]
    );
    assert_eq!(provider.openings.load(Ordering::Relaxed), 1);
    let second = backend
        .search(request("", None, 10, &fields))
        .await
        .expect("next committed view");
    assert_eq!(second.entries.len(), 1);
    assert_eq!(second.entries[0].flow_id, "flow-3");
    assert_eq!(provider.openings.load(Ordering::Relaxed), 2);
}

#[tokio::test]
async fn enforces_scan_budget_and_tenant_scope() {
    let (state, _) = fixture();
    let backend = BuiltinSearchBackend::with_scan_budget(MemoryViewProvider::new(state), 3, 2)
        .expect("backend");
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
    let backend =
        BuiltinSearchBackend::new(MemoryViewProvider::new(Arc::new(state)), 2).expect("backend");
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
async fn compares_quoted_numeric_literals_as_numbers() {
    let mut state = MemoryState::default();
    for (ordinal, amount, quantity) in [(1, 0.01, 3), (2, 2.5, 4)] {
        let mut flow = record(7, ordinal, "Checkout", FlowStatus::Running, 1, 0);
        flow.attributes = vec![
            SearchAttributeValue {
                index_key: "Amount".into(),
                index_type: IndexType::Double.into(),
                value: Some(dex::Value {
                    kind: Some(dex::value::Kind::DoubleValue(amount)),
                }),
            },
            SearchAttributeValue {
                index_key: "Quantity".into(),
                index_type: IndexType::Int.into(),
                value: Some(dex::Value {
                    kind: Some(dex::value::Kind::IntValue(quantity)),
                }),
            },
        ];
        state.insert(u16::from(ordinal), 7, ordinal, flow);
    }
    let backend =
        BuiltinSearchBackend::new(MemoryViewProvider::new(Arc::new(state)), 3).expect("backend");
    let mut fields = fields();
    fields.insert("Amount", IndexType::Double);
    fields.insert("Quantity", IndexType::Int);
    for (query, expected_flow_ids) in [
        ("Amount = '0.01'", vec!["flow-1"]),
        ("Amount = \"2.5\"", vec!["flow-2"]),
        ("Amount > '1e-1'", vec!["flow-2"]),
        ("Amount IN ('0.010', 2.5)", vec!["flow-1", "flow-2"]),
        ("Amount BETWEEN '0' AND '1'", vec!["flow-1"]),
        ("Quantity = '4'", vec!["flow-2"]),
        ("Quantity NOT IN ('4')", vec!["flow-1"]),
        ("Quantity BETWEEN '+3' AND 3", vec!["flow-1"]),
    ] {
        let page = backend
            .search(request(query, None, 10, &fields))
            .await
            .expect(query);
        assert_eq!(
            page.entries
                .iter()
                .map(|entry| entry.flow_id.as_str())
                .collect::<Vec<_>>(),
            expected_flow_ids,
            "{query}"
        );
    }
    let error = backend
        .search(request("Amount = 'cheap'", None, 10, &fields))
        .await
        .expect_err("non-numeric DOUBLE literal");
    assert!(
        matches!(&error, SearchError::InvalidArgument(message) if message.contains("invalid DOUBLE literal for Amount")),
        "{error}"
    );
}

#[tokio::test]
async fn quoted_numeric_bounds_and_hex_floats_match_indexed_values() {
    let mut state = MemoryState::default();
    for (ordinal, quantity, amount) in [
        (1, 7, 0.25),
        (2, 8, -3.0),
        (3, -2, 1.5),
        (4, 1_000, 3.5),
        (5, 9_007_199_254_740_993, 2.5),
        (6, 9_007_199_254_740_992, 0.0),
        (7, -9_007_199_254_740_992, 0.0),
        (8, i64::MAX - 1, 0.0),
        (9, i64::MAX, 0.0),
        (10, i64::MAX - 2, 0.0),
    ] {
        let mut flow = record(
            7,
            ordinal,
            "Checkout",
            FlowStatus::Running,
            i64::from(ordinal),
            0,
        );
        flow.attributes = vec![
            SearchAttributeValue {
                index_key: "Quantity".into(),
                index_type: IndexType::Int.into(),
                value: Some(dex::Value {
                    kind: Some(dex::value::Kind::IntValue(quantity)),
                }),
            },
            SearchAttributeValue {
                index_key: "Amount".into(),
                index_type: IndexType::Double.into(),
                value: Some(dex::Value {
                    kind: Some(dex::value::Kind::DoubleValue(amount)),
                }),
            },
        ];
        state.insert(u16::from(ordinal), 7, ordinal, flow);
    }
    let backend =
        BuiltinSearchBackend::new(MemoryViewProvider::new(Arc::new(state)), 11).expect("backend");
    let mut fields = fields();
    fields.insert("Quantity", IndexType::Int);
    fields.insert("Amount", IndexType::Double);
    for (query, expected_flow_ids) in [
        ("Quantity = '7.0'", vec!["flow-1"]),
        ("Quantity = '1e3'", vec!["flow-4"]),
        ("Quantity = '9007199254740993'", vec!["flow-5"]),
        ("Quantity = '9007199254740993.0'", vec!["flow-5"]),
        ("Quantity = '9007199254740992.5'", vec!["flow-6"]),
        ("Quantity = '-9007199254740992.5'", vec!["flow-7"]),
        ("Quantity = '7.5'", vec![]),
        ("Quantity IN ('7', '7.5')", vec!["flow-1"]),
        (
            "Quantity > '7.5'",
            vec![
                "flow-2", "flow-4", "flow-5", "flow-6", "flow-8", "flow-9", "flow-10",
            ],
        ),
        ("Quantity <= '-1.5'", vec!["flow-3", "flow-7"]),
        ("Quantity BETWEEN '1.5' AND '3.5'", vec![]),
        (
            "Quantity >= '9223372036854775806.5'",
            vec!["flow-8", "flow-9"],
        ),
        (
            "Quantity < '9223372036854775806.5'",
            vec![
                "flow-1", "flow-2", "flow-3", "flow-4", "flow-5", "flow-6", "flow-7", "flow-10",
            ],
        ),
        (
            "Quantity BETWEEN '9223372036854775806.5' AND '9223372036854775807'",
            vec!["flow-8", "flow-9"],
        ),
        (
            "Quantity != '7.5'",
            vec![
                "flow-1", "flow-2", "flow-3", "flow-4", "flow-5", "flow-6", "flow-7", "flow-8",
                "flow-9", "flow-10",
            ],
        ),
        ("Amount = '0x1p-2'", vec!["flow-1"]),
        ("Amount = '-0X1.8p1'", vec!["flow-2"]),
    ] {
        let page = backend
            .search(request(query, None, 10, &fields))
            .await
            .expect(query);
        let actual: BTreeSet<&str> = page
            .entries
            .iter()
            .map(|entry| entry.flow_id.as_str())
            .collect();
        let expected: BTreeSet<&str> = expected_flow_ids.into_iter().collect();
        assert_eq!(actual, expected, "{query}");
    }
    for query in [
        "Quantity = '9223372036854775808'",
        "Quantity = '0x1p2'",
        "Quantity = 'NaN'",
        "Quantity = '1_0'",
        "Quantity = ' 7'",
        "Quantity = '7d'",
        "Amount = 'Inf'",
        "Amount = 'NaN'",
        "Amount = '1e999'",
        "Amount = '0x1p1024'",
        "Amount = '1_0'",
        "Amount = ' 1'",
        "Amount = '1d'",
    ] {
        let error = backend
            .search(request(query, None, 10, &fields))
            .await
            .expect_err(query);
        assert!(
            matches!(error, SearchError::InvalidArgument(_)),
            "{query}: {error}"
        );
    }
}

#[tokio::test]
async fn random_documents_with_sort_ties_match_the_shared_evaluator() {
    let mut state = MemoryState::default();
    let mut records = Vec::new();
    let mut random_state = 0x9ed7_01a3_4c28_5b6f_u64;
    let mut sort_keys = BTreeSet::new();
    for ordinal in 1..=120_u8 {
        let status = if next_pseudorandom(&mut random_state).is_multiple_of(5) {
            FlowStatus::Running
        } else {
            FlowStatus::Completed
        };
        let flow_type = if next_pseudorandom(&mut random_state).is_multiple_of(3) {
            "Billing"
        } else {
            "Checkout"
        };
        let start = 1 + (next_pseudorandom(&mut random_state) % 5) as i64;
        let close = if status == FlowStatus::Running {
            0
        } else {
            start + 1 + (next_pseudorandom(&mut random_state) % 4) as i64
        };
        sort_keys.insert((
            status == FlowStatus::Running,
            (close != 0).then_some(close),
            start,
        ));
        let mut document = record(7, ordinal, flow_type, status, start, close);
        if !next_pseudorandom(&mut random_state).is_multiple_of(4) {
            document.attributes.push(SearchAttributeValue {
                index_key: "Color".into(),
                index_type: IndexType::Keyword.into(),
                value: Some(dex::Value {
                    kind: Some(dex::value::Kind::StringValue(
                        if next_pseudorandom(&mut random_state).is_multiple_of(2) {
                            "red"
                        } else {
                            "blue"
                        }
                        .into(),
                    )),
                }),
            });
        }
        let partition = (next_pseudorandom(&mut random_state) % 3) as u16;
        state.insert(partition, 7, ordinal, document.clone());
        records.push(document);
    }
    assert!(sort_keys.len() < records.len());
    let backend =
        BuiltinSearchBackend::new(MemoryViewProvider::new(Arc::new(state)), 3).expect("backend");
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

fn next_pseudorandom(state: &mut u64) -> u64 {
    *state = state
        .wrapping_mul(6_364_136_223_846_793_005)
        .wrapping_add(1_442_695_040_888_963_407);
    *state >> 32
}
