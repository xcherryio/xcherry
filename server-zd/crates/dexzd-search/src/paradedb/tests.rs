// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::collections::BTreeMap;

use dexzd_protocol::dex::IndexType;
use dexzd_visibility_query::{SearchDocument, SearchValue};

use super::{MIGRATION_SQL, SearchRow, SearchRowError, SqlValue, upsert_statement};

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
    for bucket in [
        "sa_keyword",
        "sa_text",
        "sa_number",
        "sa_number_types",
        "sa_datetime",
        "sa_bool",
    ] {
        assert!(MIGRATION_SQL.contains(bucket));
    }
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
    let row = SearchRow::from_document("tenant-1", 7, &document, &fields).expect("typed document");
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
fn rejects_missing_and_wrongly_typed_projection_fields() {
    let missing = SearchRow::from_document("tenant", 1, &Document::default(), &[])
        .expect_err("missing run ID");
    assert_eq!(missing, SearchRowError::MissingField { field: "RunId" });
    let wrong = document().with("keyword", SearchValue::Int(3));
    let mismatch = SearchRow::from_document(
        "tenant",
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
        "tenant",
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
            "tenant",
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
