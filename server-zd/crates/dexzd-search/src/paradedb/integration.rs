// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

use dexzd_protocol::dex::IndexType;
use dexzd_visibility_query::{
    FieldRegistry, SearchDocument, SearchValue, compare_documents, evaluate, parse_and_check,
};
use tokio_postgres::NoTls;

use super::{MIGRATION_SQL, PageCursor, SearchRow, translate_query, upsert_statement};

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

fn fields() -> (FieldRegistry, Vec<(String, IndexType)>) {
    let application_fields = vec![
        ("keyword".to_owned(), IndexType::Keyword),
        ("tags".to_owned(), IndexType::KeywordArray),
        ("text".to_owned(), IndexType::Text),
        ("number".to_owned(), IndexType::Int),
        ("double".to_owned(), IndexType::Double),
        ("flag".to_owned(), IndexType::Bool),
        ("datetime".to_owned(), IndexType::Datetime),
    ];
    let mut registry = FieldRegistry::new();
    for (field, index_type) in &application_fields {
        registry.insert(field.clone(), *index_type);
    }
    (registry, application_fields)
}

fn run(run_id: &str, workflow_id: &str, status: &str, start: i64, close: Option<i64>) -> Document {
    let mut document = Document::default()
        .with("RunId", SearchValue::String(run_id.into()))
        .with("WorkflowId", SearchValue::String(workflow_id.into()))
        .with("WorkflowType", SearchValue::String("Engine".into()))
        .with("FlowType", SearchValue::String("CheckoutFlow".into()))
        .with("ExecutionStatus", SearchValue::String(status.into()))
        .with("StartTime", SearchValue::Datetime(start));
    if let Some(close) = close {
        document = document.with("CloseTime", SearchValue::Datetime(close));
    }
    document
}

#[tokio::test]
async fn paradedb_matches_builtin_evaluator() {
    let Ok(database_url) = std::env::var("DEX_ZD_PARADEDB_URL") else {
        return;
    };
    let (client, connection) = tokio_postgres::connect(&database_url, NoTls)
        .await
        .expect("connect to CI ParadeDB");
    let connection_task = tokio::spawn(connection);
    client
        .batch_execute("CREATE EXTENSION IF NOT EXISTS pg_search")
        .await
        .expect("install pg_search");
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("wall clock after Unix epoch")
        .as_nanos();
    let schema_name = format!("dexzd_search_{}_{}", std::process::id(), nonce);
    client
        .batch_execute(&format!(
            "CREATE SCHEMA {schema_name}; SET search_path TO {schema_name}, public, pdb;"
        ))
        .await
        .expect("create isolated schema");
    client
        .batch_execute(MIGRATION_SQL)
        .await
        .expect("create visibility schema");

    let documents = vec![
        run("r1", "flow-a", "Running", 1_000_000_000, None)
            .with("keyword", SearchValue::String("red".into()))
            .with("tags", SearchValue::StringArray(vec!["charge".into()]))
            .with("text", SearchValue::String("Senior Rust Engineer".into()))
            .with("number", SearchValue::Int(2))
            .with("double", SearchValue::Double(1.5))
            .with("flag", SearchValue::Bool(true))
            .with("datetime", SearchValue::Datetime(1_000_000_000)),
        run(
            "r2",
            "flow-b",
            "Completed",
            2_000_000_000,
            Some(5_000_000_000),
        )
        .with("keyword", SearchValue::String("blue".into()))
        .with("tags", SearchValue::StringArray(vec!["ship".into()]))
        .with("text", SearchValue::String("Principal Go Engineer".into()))
        .with("number", SearchValue::Int(7))
        .with("double", SearchValue::Double(3.5))
        .with("flag", SearchValue::Bool(false))
        .with("datetime", SearchValue::Datetime(2_000_000_000)),
        run("r3", "flow-c", "Failed", 3_000_000_000, Some(4_000_000_000))
            .with("keyword", SearchValue::String("red".into()))
            .with(
                "tags",
                SearchValue::StringArray(vec!["charge".into(), "ship".into()]),
            )
            .with("text", SearchValue::String("Rust Backend".into()))
            .with("number", SearchValue::Int(5))
            .with("double", SearchValue::Double(2.5))
            .with("flag", SearchValue::Bool(true)),
        run(
            "r4",
            "flow-d",
            "Completed",
            4_000_000_000,
            Some(6_000_000_000),
        )
        .with("tags", SearchValue::StringArray(Vec::new())),
    ];
    let (registry, application_fields) = fields();
    let mut rows = BTreeMap::new();
    for document in &documents {
        let row = SearchRow::from_document("tenant-a", 1, document, &application_fields)
            .expect("map document");
        let statement = upsert_statement(&row);
        client
            .execute(&statement.sql, &statement.parameter_refs())
            .await
            .expect("insert run");
        rows.insert(row.run_id.clone(), row);
    }
    let other_tenant = run(
        "other",
        "flow-a",
        "Completed",
        1_000_000_000,
        Some(2_000_000_000),
    );
    let other_row = SearchRow::from_document("tenant-b", 1, &other_tenant, &application_fields)
        .expect("map other tenant");
    let statement = upsert_statement(&other_row);
    client
        .execute(&statement.sql, &statement.parameter_refs())
        .await
        .expect("insert other tenant");

    let mut stale_row = rows.get("r1").expect("versioned row").clone();
    stale_row
        .sa_keyword
        .insert("keyword".into(), SearchValue::String("stale".into()));
    for version in [1, 0] {
        stale_row.version = version;
        let statement = upsert_statement(&stale_row);
        client
            .execute(&statement.sql, &statement.parameter_refs())
            .await
            .expect("stale upsert");
    }
    let keyword: String = client.query_one(
        "SELECT sa_keyword ->> 'keyword' FROM dexzd_visibility_runs WHERE tenant_id = 'tenant-a' AND run_id = 'r1'",
        &[],
    ).await.expect("read guarded row").get(0);
    assert_eq!(keyword, "red");

    for input in [
        "",
        "WorkflowId = 'flow-a'",
        "WorkflowId != 'flow-a'",
        "WorkflowId IN ('flow-a', 'flow-c')",
        "WorkflowId STARTS_WITH 'flow-'",
        "keyword = 'red'",
        "keyword STARTS_WITH 'r'",
        "keyword NOT IN ('red')",
        "NOT (keyword = 'red')",
        "tags = 'charge'",
        "tags IN ('ship')",
        "tags != 'charge'",
        "tags IS NOT NULL",
        "number >= 5",
        "double < 3.0",
        "flag = true",
        "datetime >= '1970-01-01T00:00:02Z'",
        "datetime IS NULL",
        "text = 'rust'",
        "text = 'rust engineer'",
        "text != 'rust'",
        "CloseTime IS NULL",
        "CloseTime IS NOT NULL",
        "ExecutionStatus = 'ContinuedAsNew'",
        "WorkflowType = 'Engine'",
        "(keyword = 'red' OR flag = false) AND number IS NOT NULL",
        "FlowType = 'CheckoutFlow' ORDER BY StartTime ASC",
        "FlowType = 'CheckoutFlow' ORDER BY CloseTime DESC",
    ] {
        let query = parse_and_check(input, &registry).expect("checked query");
        let mut expected: Vec<&Document> = documents
            .iter()
            .filter(|document| evaluate(&query, *document))
            .collect();
        expected.sort_by(|left, right| compare_documents(&query, *left, *right));
        let expected_ids: Vec<String> = expected
            .iter()
            .map(|document| match document.field_value("RunId") {
                Some(SearchValue::String(run_id)) => run_id,
                _ => panic!("test run must have an ID"),
            })
            .collect();
        let statement =
            translate_query(&query, "tenant-a", 100, None).expect("translate checked query");
        let actual = client
            .query(&statement.sql, &statement.parameter_refs())
            .await
            .expect(input);
        let actual_ids: Vec<String> = actual
            .iter()
            .map(|row| row.get::<_, String>("run_id"))
            .collect();
        assert_eq!(actual_ids, expected_ids, "{input}");
    }

    for order in ["", "ORDER BY StartTime ASC", "ORDER BY CloseTime DESC"] {
        let query = parse_and_check(order, &registry).expect("ordered query");
        let mut expected: Vec<&Document> = documents.iter().collect();
        expected.sort_by(|left, right| compare_documents(&query, *left, *right));
        let expected_ids: Vec<String> = expected
            .iter()
            .map(|document| match document.field_value("RunId") {
                Some(SearchValue::String(run_id)) => run_id,
                _ => panic!("test run must have an ID"),
            })
            .collect();
        let first = translate_query(&query, "tenant-a", 2, None).expect("first page");
        let first_page = client
            .query(&first.sql, &first.parameter_refs())
            .await
            .expect("first page SQL");
        assert_eq!(first_page.len(), 2);
        let last_id: String = first_page[1].get("run_id");
        let cursor = PageCursor::from_row(rows.get(&last_id).expect("cursor row"));
        let next = translate_query(&query, "tenant-a", 2, Some(&cursor)).expect("next page");
        let next_page = client
            .query(&next.sql, &next.parameter_refs())
            .await
            .expect("next page SQL");
        let actual_ids: Vec<String> = first_page
            .iter()
            .chain(next_page.iter())
            .map(|row| row.get("run_id"))
            .collect();
        assert_eq!(actual_ids, expected_ids, "{order}");
    }

    client
        .batch_execute(&format!("DROP SCHEMA {schema_name} CASCADE"))
        .await
        .expect("drop isolated schema");
    drop(client);
    connection_task
        .await
        .expect("connection task")
        .expect("connection shutdown");
}
