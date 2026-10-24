// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::collections::BTreeMap;

use dexzd_apply::{ApplyOutcome, StateWrite};
use dexzd_keyspace::TenantId;
use dexzd_keyspace::row_keys::{prefix_range, run_singleton_key, tenant_run_index_key};
use dexzd_keyspace::{ColumnFamily, FlowHash, IndexKind, PartitionId, RunId, RunTable, run_prefix};
use dexzd_protocol::dex::{self, IndexType};
use dexzd_protocol::dexzd::v1::{SearchAttributeValue, SearchDocumentRecord};
use dexzd_visibility_query::{SearchDocument, SearchValue};
use prost::Message;

use super::wire_value::{decode_wire_value, encode_wire_value};
use super::{
    MIGRATION_SQL, ProjectionError, SearchDocumentChange, SearchRow, SearchRowError, SqlValue,
    extract_batch_search_document_changes, extract_search_document_changes, upsert_statement,
};

#[derive(Default)]
struct Document(BTreeMap<String, SearchValue>);

impl Document {
    fn with(mut self, field: &str, value: SearchValue) -> Self {
        self.0.insert(field.to_owned(), value);
        self
    }
}

impl SearchDocument for Document {
    fn field_value(&self, field: &str) -> Option<SearchValue> {
        self.0.get(field).cloned()
    }
}

fn document() -> Document {
    Document::default()
        .with("RunId", SearchValue::String("run-1".into()))
        .with("WorkflowId", SearchValue::String("flow-1".into()))
        .with("WorkflowType", SearchValue::String("Engine".into()))
        .with("FlowType", SearchValue::String("CheckoutFlow".into()))
        .with("ExecutionStatus", SearchValue::String("Running".into()))
        .with("StartTime", SearchValue::Datetime(1_000_000_000))
}

#[test]
fn migration_has_tenant_first_and_versioned_documents() {
    assert!(MIGRATION_SQL.contains("PRIMARY KEY (tenant_id, run_id)"));
    assert!(MIGRATION_SQL.contains("(tenant_id, is_running DESC"));
    assert!(MIGRATION_SQL.contains("(tenant_id, flow_type"));
    assert!(MIGRATION_SQL.contains("(tenant_id, workflow_id)"));
    assert!(MIGRATION_SQL.contains("(tenant_id::pdb.literal)"));
    assert!(MIGRATION_SQL.contains("dexzd_visibility_progress"));
    assert!(MIGRATION_SQL.contains("partition_id integer"));
    for bucket in [
        "sa_keyword",
        "sa_text",
        "sa_number",
        "sa_number_types",
        "sa_datetime",
        "sa_bool",
        "sa_wire",
    ] {
        assert!(MIGRATION_SQL.contains(bucket));
    }
}

#[test]
fn extracts_ordered_document_writes_and_run_purge() {
    let partition_id = PartitionId(3);
    let tenant_id = TenantId(7);
    let flow_hash = FlowHash(42);
    let run_id = RunId([5; 16]);
    let record = SearchDocumentRecord {
        version: 1,
        tenant_id: tenant_id.0,
        run_id: run_id.as_bytes().to_vec(),
        ..Default::default()
    };
    let encoded_document = record.encode_to_vec();
    let (start, end) = prefix_range(&run_prefix(flow_hash, run_id));
    let outcome = ApplyOutcome {
        writes: vec![
            StateWrite::Put {
                column_family: ColumnFamily::Flow,
                key: run_singleton_key(flow_hash, run_id, RunTable::RunRecord),
                value: vec![0],
            },
            StateWrite::Put {
                column_family: ColumnFamily::Flow,
                key: run_singleton_key(flow_hash, run_id, RunTable::SearchDocument),
                value: encoded_document.clone(),
            },
            StateWrite::Delete {
                column_family: ColumnFamily::PartitionIndex,
                key: tenant_run_index_key(
                    partition_id,
                    IndexKind::SearchDocument,
                    tenant_id,
                    flow_hash,
                    run_id,
                ),
            },
            StateWrite::DeleteRange {
                column_family: ColumnFamily::Flow,
                start,
                end,
            },
        ],
        effects: vec![],
        reply: Default::default(),
        run_id: Some(run_id),
    };
    let changes = extract_search_document_changes(partition_id, &outcome).unwrap();
    assert_eq!(
        changes,
        vec![
            SearchDocumentChange::Upsert {
                tenant_id,
                run_id,
                encoded_document,
            },
            SearchDocumentChange::Delete { tenant_id, run_id },
        ]
    );
    let mut missing_delete = outcome;
    missing_delete.writes.remove(2);
    assert!(matches!(
        extract_search_document_changes(partition_id, &missing_delete),
        Err(ProjectionError::InvalidState(_))
    ));
}

#[test]
fn extracts_every_outcome_of_one_batch_in_order() {
    let partition_id = PartitionId(3);
    let tenant_id = TenantId(7);
    let flow_hash = FlowHash(42);
    let first_run = RunId([5; 16]);
    let second_run = RunId([6; 16]);
    let encoded_document = |run_id: RunId| {
        SearchDocumentRecord {
            version: 13,
            tenant_id: tenant_id.0,
            run_id: run_id.as_bytes().to_vec(),
            ..Default::default()
        }
        .encode_to_vec()
    };
    let first_outcome = ApplyOutcome {
        writes: [first_run, second_run]
            .into_iter()
            .map(|run_id| StateWrite::Put {
                column_family: ColumnFamily::Flow,
                key: run_singleton_key(flow_hash, run_id, RunTable::SearchDocument),
                value: encoded_document(run_id),
            })
            .collect(),
        effects: vec![],
        reply: Default::default(),
        run_id: Some(first_run),
    };
    let second_outcome = ApplyOutcome {
        writes: vec![StateWrite::Delete {
            column_family: ColumnFamily::PartitionIndex,
            key: tenant_run_index_key(
                partition_id,
                IndexKind::SearchDocument,
                tenant_id,
                flow_hash,
                first_run,
            ),
        }],
        effects: vec![],
        reply: Default::default(),
        run_id: Some(first_run),
    };
    let changes =
        extract_batch_search_document_changes(partition_id, &[first_outcome, second_outcome])
            .unwrap();
    assert_eq!(
        changes,
        vec![
            SearchDocumentChange::Upsert {
                tenant_id,
                run_id: first_run,
                encoded_document: encoded_document(first_run),
            },
            SearchDocumentChange::Upsert {
                tenant_id,
                run_id: second_run,
                encoded_document: encoded_document(second_run),
            },
            SearchDocumentChange::Delete {
                tenant_id,
                run_id: first_run,
            },
        ]
    );
}

#[test]
fn rejects_malformed_search_document_keys() {
    let partition_id = PartitionId(3);
    let run_id = RunId([5; 16]);
    let flow_hash = FlowHash(42);
    let mut malformed_document_key = run_singleton_key(flow_hash, run_id, RunTable::SearchDocument);
    malformed_document_key.push(0);
    let outcome = ApplyOutcome {
        writes: vec![StateWrite::Put {
            column_family: ColumnFamily::Flow,
            key: malformed_document_key,
            value: vec![],
        }],
        effects: vec![],
        reply: Default::default(),
        run_id: Some(run_id),
    };
    assert!(matches!(
        extract_search_document_changes(partition_id, &outcome),
        Err(ProjectionError::InvalidState(_))
    ));
    let mut malformed_index_key = tenant_run_index_key(
        partition_id,
        IndexKind::SearchDocument,
        TenantId(7),
        flow_hash,
        run_id,
    );
    malformed_index_key.pop();
    let outcome = ApplyOutcome {
        writes: vec![StateWrite::Delete {
            column_family: ColumnFamily::PartitionIndex,
            key: malformed_index_key,
        }],
        effects: vec![],
        reply: Default::default(),
        run_id: Some(run_id),
    };
    assert!(matches!(
        extract_search_document_changes(partition_id, &outcome),
        Err(ProjectionError::InvalidState(_))
    ));
}

#[test]
fn maps_typed_attributes_and_binds_upsert_values() {
    let injection = "x'); DROP TABLE dexzd_visibility_runs; --";
    let document = document()
        .with("keyword", SearchValue::String(injection.into()))
        .with("tags", SearchValue::StringArray(vec!["charge".into()]))
        .with("text", SearchValue::String("Rust Engineer".into()))
        .with("number", SearchValue::Int(42))
        .with("double", SearchValue::Double(12.5))
        .with("datetime", SearchValue::Datetime(123_456_789))
        .with("flag", SearchValue::Bool(true));
    let fields = vec![
        ("keyword".into(), IndexType::Keyword),
        ("tags".into(), IndexType::KeywordArray),
        ("text".into(), IndexType::Text),
        ("number".into(), IndexType::Int),
        ("double".into(), IndexType::Double),
        ("datetime".into(), IndexType::Datetime),
        ("flag".into(), IndexType::Bool),
    ];
    let row = SearchRow::from_document(TenantId(1), 7, &document, &fields).expect("typed document");
    assert_eq!(row.tenant_id, "1");
    assert_eq!(row.sa_keyword.len(), 2);
    assert_eq!(row.sa_text.len(), 1);
    assert_eq!(row.sa_number.len(), 2);
    assert_eq!(row.sa_number_types.len(), 2);
    assert_eq!(
        row.sa_number_types.get("number"),
        Some(&SearchValue::Int(i64::from(IndexType::Int as i32)))
    );
    assert_eq!(
        row.sa_number_types.get("double"),
        Some(&SearchValue::Int(i64::from(IndexType::Double as i32)))
    );
    assert_eq!(row.sa_datetime.len(), 1);
    assert_eq!(row.sa_bool.len(), 1);
    assert!(row.sa_wire.is_empty());
    let statement = upsert_statement(&row);
    assert!(
        statement
            .sql
            .contains("ON CONFLICT (tenant_id, run_id) DO UPDATE")
    );
    assert!(
        statement
            .sql
            .contains("WHERE EXCLUDED.version > dexzd_visibility_runs.version")
    );
    assert!(!statement.sql.contains(injection));
    assert!(
        statement
            .parameters
            .contains(&SqlValue::Text(injection.into()))
    );
    assert_eq!(statement.parameter_refs().len(), statement.parameters.len());
}

#[test]
fn preserves_original_datetime_and_null_wire_values() {
    let original_datetime = dex::Value {
        kind: Some(dex::value::Kind::StringValue(
            "1970-01-01T01:00:01+01:00".into(),
        )),
    };
    let original_null = dex::Value {
        kind: Some(dex::value::Kind::NullValue(0)),
    };
    let row = SearchRow::from_document(TenantId(1), 1, &document(), &[])
        .expect("typed row")
        .with_wire_attributes(&[
            SearchAttributeValue {
                index_key: "When".into(),
                index_type: IndexType::Datetime.into(),
                value: Some(original_datetime.clone()),
            },
            SearchAttributeValue {
                index_key: "Missing".into(),
                index_type: IndexType::Keyword.into(),
                value: Some(original_null.clone()),
            },
        ])
        .expect("stored wire values");
    for (field, expected) in [("When", original_datetime), ("Missing", original_null)] {
        let Some(SearchValue::String(encoded)) = row.sa_wire.get(field) else {
            panic!("missing stored wire value for {field}");
        };
        assert_eq!(
            decode_wire_value(field, &serde_json::json!(encoded)).expect("decode wire value"),
            expected
        );
        assert_eq!(
            encode_wire_value(&expected),
            SearchValue::String(encoded.clone())
        );
    }
    let error = row
        .with_wire_attributes(&[SearchAttributeValue {
            index_key: "Bad".into(),
            index_type: IndexType::Keyword.into(),
            value: None,
        }])
        .expect_err("missing wire value");
    assert_eq!(
        error,
        SearchRowError::MissingWireValue {
            field: "Bad".into()
        }
    );
}

#[test]
fn rejects_missing_and_wrongly_typed_projection_fields() {
    let missing = SearchRow::from_document(TenantId(1), 1, &Document::default(), &[])
        .expect_err("missing run ID");
    assert_eq!(missing, SearchRowError::MissingField { field: "RunId" });
    let wrong = document().with("keyword", SearchValue::Int(3));
    let mismatch = SearchRow::from_document(
        TenantId(1),
        1,
        &wrong,
        &[("keyword".into(), IndexType::Keyword)],
    )
    .expect_err("wrong attribute type");
    assert_eq!(
        mismatch,
        SearchRowError::TypeMismatch {
            field: "keyword".into(),
            index_type: IndexType::Keyword
        }
    );
    let collision = SearchRow::from_document(
        TenantId(1),
        1,
        &document(),
        &[("RunId".into(), IndexType::Keyword)],
    )
    .expect_err("system field collision");
    assert_eq!(
        collision,
        SearchRowError::SystemFieldCollision {
            field: "RunId".into()
        }
    );
    let invalid = document().with("double", SearchValue::Double(f64::NAN));
    assert_eq!(
        SearchRow::from_document(
            TenantId(1),
            1,
            &invalid,
            &[("double".into(), IndexType::Double)]
        )
        .expect_err("non-finite double"),
        SearchRowError::NonFiniteDouble {
            field: "double".into()
        }
    );
}

fn query_fields() -> dexzd_visibility_query::FieldRegistry {
    let mut fields = dexzd_visibility_query::FieldRegistry::new();
    for (name, index_type) in [
        ("keyword", IndexType::Keyword),
        ("tags", IndexType::KeywordArray),
        ("text", IndexType::Text),
        ("number", IndexType::Int),
        ("double", IndexType::Double),
        ("datetime", IndexType::Datetime),
        ("flag", IndexType::Bool),
        ("Custom Key", IndexType::Keyword),
    ] {
        fields.insert(name, index_type);
    }
    fields
}

#[test]
fn golden_sql_covers_operators_and_types() {
    use dexzd_visibility_query::parse_and_check;

    use super::translate_query;

    let cases: Vec<(&str, &str, Vec<SqlValue>)> = vec![
        ("", "TRUE", vec![]),
        (
            "WorkflowId = 'a'",
            "(workflow_id) COLLATE \"C\" = $2::text",
            vec![SqlValue::Text("a".into())],
        ),
        (
            "WorkflowId != 'a'",
            "NOT ((workflow_id) COLLATE \"C\" = $2::text)",
            vec![SqlValue::Text("a".into())],
        ),
        (
            "WorkflowId <> 'a'",
            "NOT ((workflow_id) COLLATE \"C\" = $2::text)",
            vec![SqlValue::Text("a".into())],
        ),
        (
            "WorkflowId > 'a'",
            "(workflow_id) COLLATE \"C\" > $2::text",
            vec![SqlValue::Text("a".into())],
        ),
        (
            "WorkflowId >= 'a'",
            "(workflow_id) COLLATE \"C\" >= $2::text",
            vec![SqlValue::Text("a".into())],
        ),
        (
            "WorkflowId < 'a'",
            "(workflow_id) COLLATE \"C\" < $2::text",
            vec![SqlValue::Text("a".into())],
        ),
        (
            "WorkflowId <= 'a'",
            "(workflow_id) COLLATE \"C\" <= $2::text",
            vec![SqlValue::Text("a".into())],
        ),
        (
            "WorkflowId IN ('a', 'b')",
            "((workflow_id) COLLATE \"C\" = $2::text OR (workflow_id) COLLATE \"C\" = $3::text)",
            vec![SqlValue::Text("a".into()), SqlValue::Text("b".into())],
        ),
        (
            "WorkflowId NOT IN ('a', 'b')",
            "NOT ((workflow_id) COLLATE \"C\" = $2::text OR (workflow_id) COLLATE \"C\" = $3::text)",
            vec![SqlValue::Text("a".into()), SqlValue::Text("b".into())],
        ),
        (
            "WorkflowId BETWEEN 'a' AND 'b'",
            "((workflow_id) COLLATE \"C\" >= $2::text) AND ((workflow_id) COLLATE \"C\" <= $3::text)",
            vec![SqlValue::Text("a".into()), SqlValue::Text("b".into())],
        ),
        (
            "WorkflowId STARTS_WITH 'a%'",
            "left(workflow_id, char_length($2::text)) = $2::text",
            vec![SqlValue::Text("a%".into())],
        ),
        ("CloseTime IS NULL", "close_time_ns IS NULL", vec![]),
        ("CloseTime IS NOT NULL", "close_time_ns IS NOT NULL", vec![]),
        ("WorkflowType = 'Engine'", "TRUE", vec![]),
        ("ExecutionStatus = 'ContinuedAsNew'", "FALSE", vec![]),
        (
            "ActiveStepTypes = 'ChargeOrder'",
            "$2::text = ANY(active_step_types)",
            vec![SqlValue::Text("ChargeOrder".into())],
        ),
        (
            "keyword = 'red'",
            "(sa_keyword ->> $2::text) COLLATE \"C\" = $3::text",
            vec![
                SqlValue::Text("keyword".into()),
                SqlValue::Text("red".into()),
            ],
        ),
        (
            "keyword STARTS_WITH 'r%'",
            "left(sa_keyword ->> $2::text, char_length($3::text)) = $3::text",
            vec![
                SqlValue::Text("keyword".into()),
                SqlValue::Text("r%".into()),
            ],
        ),
        (
            "tags IN ('charge', 'ship')",
            "(CASE WHEN (sa_keyword -> $2::text) IS NULL THEN NULL ELSE EXISTS (SELECT 1 FROM jsonb_array_elements_text(sa_keyword -> $2::text) AS element(value) WHERE element.value = $3::text) END OR CASE WHEN (sa_keyword -> $2::text) IS NULL THEN NULL ELSE EXISTS (SELECT 1 FROM jsonb_array_elements_text(sa_keyword -> $2::text) AS element(value) WHERE element.value = $4::text) END)",
            vec![
                SqlValue::Text("tags".into()),
                SqlValue::Text("charge".into()),
                SqlValue::Text("ship".into()),
            ],
        ),
        (
            "tags != 'charge'",
            "NOT (CASE WHEN (sa_keyword -> $2::text) IS NULL THEN NULL ELSE EXISTS (SELECT 1 FROM jsonb_array_elements_text(sa_keyword -> $2::text) AS element(value) WHERE element.value = $3::text) END)",
            vec![
                SqlValue::Text("tags".into()),
                SqlValue::Text("charge".into()),
            ],
        ),
        (
            "text = 'Rust Engineer'",
            "regexp_split_to_array(lower(sa_text ->> $2::text), '[^[:alnum:]]+') @> $3::text[]",
            vec![
                SqlValue::Text("text".into()),
                SqlValue::TextArray(vec!["rust".into(), "engineer".into()]),
            ],
        ),
        (
            "number >= 7",
            "(sa_number ->> $2::text)::bigint >= $3::bigint",
            vec![SqlValue::Text("number".into()), SqlValue::Int(7)],
        ),
        (
            "double < 3.5",
            "(sa_number ->> $2::text)::double precision < $3::double precision",
            vec![SqlValue::Text("double".into()), SqlValue::Double(3.5)],
        ),
        (
            "datetime = '2026-07-03T00:00:00Z'",
            "(sa_datetime ->> $2::text)::bigint = $3::bigint",
            vec![
                SqlValue::Text("datetime".into()),
                SqlValue::Int(1_783_036_800_000_000_000),
            ],
        ),
        (
            "flag = true",
            "(sa_bool ->> $2::text)::boolean = $3::boolean",
            vec![SqlValue::Text("flag".into()), SqlValue::Bool(true)],
        ),
        (
            "`Custom Key` = 'x'",
            "(sa_keyword ->> $2::text) COLLATE \"C\" = $3::text",
            vec![
                SqlValue::Text("Custom Key".into()),
                SqlValue::Text("x".into()),
            ],
        ),
    ];
    for (input, expected_predicate, values) in cases {
        let query = parse_and_check(input, &query_fields()).expect(input);
        let statement = translate_query(&query, "tenant", 10, None).expect(input);
        let bm25_anchor = if [
            "keyword",
            "tags",
            "text",
            "number",
            "double",
            "datetime",
            "flag",
            "`Custom Key`",
        ]
        .iter()
        .any(|field| input.starts_with(field))
        {
            " AND tenant_id @@@ pdb.term($1::text)"
        } else {
            ""
        };
        let expected_sql = format!(
            "SELECT {} FROM dexzd_visibility_runs WHERE tenant_id = $1::text AND coalesce(({expected_predicate}), FALSE){bm25_anchor} {} LIMIT ${}::bigint",
            super::query::SELECT_COLUMNS,
            super::query::DEFAULT_ORDER,
            values.len() + 2
        );
        assert_eq!(statement.sql, expected_sql, "{input}");
        let mut expected_parameters = vec![SqlValue::Text("tenant".into())];
        expected_parameters.extend(values);
        expected_parameters.push(SqlValue::Int(10));
        assert_eq!(statement.parameters, expected_parameters, "{input}");
    }
}

#[test]
fn tenant_scope_boolean_precedence_and_keyset_are_bound() {
    use dexzd_visibility_query::parse_and_check;

    use super::{PageCursor, SqlTranslationError, translate_query};

    let query =
        parse_and_check("WorkflowId = 'a' OR WorkflowId = 'b'", &query_fields()).expect("OR query");
    let statement = translate_query(&query, "tenant-a", 3, None).expect("translate OR query");
    assert!(statement.sql.contains("WHERE tenant_id = $1::text AND coalesce((((workflow_id) COLLATE \"C\" = $2::text) OR ((workflow_id) COLLATE \"C\" = $3::text)), FALSE)"));
    assert_eq!(
        statement.parameters,
        vec![
            SqlValue::Text("tenant-a".into()),
            SqlValue::Text("a".into()),
            SqlValue::Text("b".into()),
            SqlValue::Int(3),
        ]
    );

    let empty = parse_and_check("", &query_fields()).expect("empty query");
    let cursor = PageCursor {
        is_running: true,
        close_time_ns: None,
        start_time_ns: 100,
        run_id: "run-1".into(),
    };
    let statement = translate_query(&empty, "tenant-a", 2, Some(&cursor)).expect("default cursor");
    assert!(
        statement
            .sql
            .contains("is_running < $3::boolean OR (is_running = $3::boolean")
    );
    assert!(
        statement
            .sql
            .contains("close_time_ns IS NOT DISTINCT FROM $4::bigint")
    );
    assert!(statement.sql.contains("start_time_ns < $5::bigint"));
    assert_eq!(
        statement.parameters,
        vec![
            SqlValue::Text("tenant-a".into()),
            SqlValue::Text("run-1".into()),
            SqlValue::Bool(true),
            SqlValue::OptionalInt(None),
            SqlValue::Int(100),
            SqlValue::Int(2),
        ]
    );

    let ascending =
        parse_and_check("ORDER BY StartTime ASC", &query_fields()).expect("start order");
    let statement =
        translate_query(&ascending, "tenant-a", 2, Some(&cursor)).expect("start cursor");
    assert!(statement.sql.contains("start_time_ns > $3::bigint"));
    assert!(
        statement
            .sql
            .contains("ORDER BY start_time_ns ASC, run_id COLLATE \"C\" ASC")
    );
    let close = parse_and_check("ORDER BY CloseTime DESC", &query_fields()).expect("close order");
    let statement = translate_query(&close, "tenant-a", 2, Some(&cursor)).expect("close cursor");
    assert!(statement.sql.contains("close_time_ns < $3::bigint"));
    assert!(
        statement
            .sql
            .contains("close_time_ns IS NOT DISTINCT FROM $3::bigint")
    );
    assert!(statement.sql.contains("NULLS LAST"));

    assert_eq!(
        translate_query(&empty, "", 2, None).expect_err("empty tenant"),
        SqlTranslationError::InvalidPagination {
            reason: "tenant ID is empty"
        }
    );
    assert_eq!(
        translate_query(&empty, "tenant", 0, None).expect_err("zero size"),
        SqlTranslationError::InvalidPagination {
            reason: "page size must be positive"
        }
    );
    let bad_cursor = PageCursor {
        run_id: String::new(),
        ..cursor
    };
    assert_eq!(
        translate_query(&empty, "tenant", 2, Some(&bad_cursor)).expect_err("empty cursor"),
        SqlTranslationError::InvalidPagination {
            reason: "cursor run ID is empty"
        }
    );
}
