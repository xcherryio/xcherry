// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::cmp::Ordering;
use std::collections::BTreeMap;

use dexzd_protocol::dex::IndexType;
use dexzd_visibility_query::{
    FieldRegistry, SearchDocument, SearchValue, SortDirection, compare_documents, evaluate,
    parse_and_check,
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

fn registry() -> FieldRegistry {
    let mut fields = FieldRegistry::new();
    for (name, index_type) in [
        ("keyword1", IndexType::Keyword),
        ("keyword2", IndexType::Keyword),
        ("keyword3", IndexType::Keyword),
        ("text1", IndexType::Text),
        ("int1", IndexType::Int),
        ("double1", IndexType::Double),
        ("bool1", IndexType::Bool),
        ("datetime1", IndexType::Datetime),
        ("array1", IndexType::KeywordArray),
        ("CustomKeywordField", IndexType::Keyword),
        ("CustomDatetimeField", IndexType::Datetime),
        ("CustomDoubleField", IndexType::Double),
        ("Custom Key", IndexType::Keyword),
        ("OrderNumber", IndexType::Keyword),
        ("NOT", IndexType::Keyword),
        ("flag", IndexType::Bool),
    ] {
        fields.insert(name, index_type);
    }
    fields
}

fn check(query: &str) -> dexzd_visibility_query::Query {
    parse_and_check(query, &registry()).unwrap_or_else(|error| panic!("{query}: {error}"))
}

#[test]
fn evaluates_all_operators_and_types() {
    let document = Document::default()
        .with("WorkflowId", SearchValue::String("flow-42".into()))
        .with("FlowType", SearchValue::String("CheckoutFlow".into()))
        .with("ExecutionStatus", SearchValue::String("Running".into()))
        .with("keyword1", SearchValue::String("ready".into()))
        .with("OrderNumber", SearchValue::String("12345".into()))
        .with("text1", SearchValue::String("Senior Rust-Engineer".into()))
        .with("int1", SearchValue::Int(7))
        .with("double1", SearchValue::Double(14_000.5))
        .with("bool1", SearchValue::Bool(true))
        .with(
            "array1",
            SearchValue::StringArray(vec!["charge".into(), "ship".into()]),
        )
        .with(
            "ActiveStepTypes",
            SearchValue::StringArray(vec!["ChargeOrder".into()]),
        )
        .with(
            "StartTime",
            SearchValue::Datetime(1_783_036_800_000_000_000),
        )
        .with(
            "datetime1",
            SearchValue::Datetime(1_783_036_800_000_000_000),
        );
    for query in [
        "",
        "   ",
        "WorkflowId = 'flow-42'",
        "WorkflowId != 'other'",
        "WorkflowId <> 'other'",
        "WorkflowId > 'flow-41'",
        "WorkflowId >= 'flow-42'",
        "WorkflowId < 'flow-43'",
        "WorkflowId <= 'flow-42'",
        "WorkflowId IN ('other', 'flow-42')",
        "WorkflowId NOT IN ('other', 'missing')",
        "WorkflowId BETWEEN 'flow-40' AND 'flow-50'",
        "WorkflowId STARTS_WITH 'flow-'",
        "keyword1 IS NOT NULL",
        "OrderNumber = 12345",
        "CloseTime IS NULL",
        "text1 = 'rust senior'",
        "text1 = 'RUST engineer'",
        "text1 != 'principal'",
        "text1 IN ('principal', 'rust')",
        "int1 = 7",
        "int1 BETWEEN 6 AND 8",
        "int1 > -1",
        "int1 <= 7",
        "int1 = '7'",
        "int1 = \"7\"",
        "int1 = '+7'",
        "int1 = '0007'",
        "int1 != '8'",
        "int1 > '-1'",
        "int1 IN ('6', 7)",
        "int1 NOT IN ('6', '8')",
        "int1 BETWEEN '6' AND 8",
        "double1 = 14000.5",
        "double1 = 1.40005e4",
        "double1 >= 14000",
        "double1 = '14000.5'",
        "double1 = \"1.40005e4\"",
        "double1 = '+14000.50'",
        "double1 = '14000.5e0'",
        "double1 >= '14000'",
        "double1 < '1.5E4'",
        "double1 IN ('1', 14000.5)",
        "double1 BETWEEN '14000.' AND '14001'",
        "bool1 = TRUE",
        "bool1 = 'true'",
        "bool1 != false",
        "bool1 IN (false, true)",
        "array1 = 'charge'",
        "array1 IN ('other', 'ship')",
        "array1 != 'other'",
        "array1 NOT IN ('other', 'missing')",
        "ActiveStepTypes = 'ChargeOrder'",
        "StartTime = '2026-07-03T00:00:00Z'",
        "datetime1 BETWEEN '2026-07-02T00:00:00Z' AND '2026-07-04T00:00:00Z'",
        "WorkflowType = 'Engine'",
        "ExecutionStatus STARTS_WITH 'Run'",
        "NOT WorkflowId = 'other' AND (int1 = 7 OR bool1 = false)",
    ] {
        assert!(evaluate(&check(query), &document), "{query}");
    }
    for query in [
        "WorkflowId = 'other'",
        "WorkflowId IN ('other', 'missing')",
        "WorkflowId NOT IN ('flow-42')",
        "WorkflowId STARTS_WITH 'other'",
        "keyword1 IS NULL",
        "CloseTime IS NOT NULL",
        "text1 = 'rust principal'",
        "text1 != 'engineer'",
        "array1 = 'other'",
        "array1 != 'charge'",
        "array1 NOT IN ('ship')",
        "int1 BETWEEN 8 AND 9",
        "int1 = '8'",
        "int1 IN ('6', '8')",
        "int1 BETWEEN '8' AND '9'",
        "double1 = '0.01'",
        "double1 != '14000.5'",
        "double1 < '.5'",
        "double1 NOT IN ('14000.5')",
        "bool1 = false",
        "StartTime < '2026-07-02T00:00:00Z'",
        "ExecutionStatus = 'ContinuedAsNew'",
    ] {
        assert!(!evaluate(&check(query), &document), "{query}");
    }
}

#[test]
fn precedence_escapes_and_null_semantics() {
    let document = Document::default()
        .with("keyword1", SearchValue::String("a'b\"c".into()))
        .with("Custom Key", SearchValue::String("hello".into()))
        .with("NOT", SearchValue::String("reserved".into()))
        .with("bool1", SearchValue::Bool(true));
    assert!(evaluate(&check("keyword1 = 'a''b\\\"c'"), &document));
    assert!(evaluate(&check("keyword1 = \"a'b\"\"c\""), &document));
    assert!(evaluate(&check("`Custom Key` = 'hello'"), &document));
    assert!(evaluate(&check("`NOT` = 'reserved'"), &document));
    assert!(evaluate(
        &check("not bool1 = false and keyword1 = 'a''b\\\"c'"),
        &document
    ));
    assert!(evaluate(
        &check("bool1 = true OR bool1 = false AND keyword1 = 'missing'"),
        &document
    ));
    assert!(!evaluate(
        &check("(bool1 = true OR bool1 = false) AND keyword1 = 'missing'"),
        &document
    ));
    for query in [
        "CloseTime != '2026-07-03T00:00:00Z'",
        "CloseTime NOT IN ('2026-07-03T00:00:00Z')",
        "NOT CloseTime = '2026-07-03T00:00:00Z'",
        "NOT CloseTime IN ('2026-07-03T00:00:00Z')",
        "NOT (CloseTime != '2026-07-03T00:00:00Z')",
    ] {
        assert!(!evaluate(&check(query), &document), "{query}");
    }
}

#[test]
fn errors_have_byte_offsets_and_specific_reasons() {
    for (query, offset, reason) in [
        ("unknown = 'x'", 0, "unknown visibility field"),
        ("é = 'x'", 0, "unknown visibility field"),
        (
            "int1 = 'seven'",
            7,
            "invalid INT literal for int1: \"seven\"",
        ),
        ("int1 = '7.5'", 7, "is not a whole number"),
        ("int1 = '1e3'", 7, "is not a whole number"),
        ("int1 = ''", 7, "invalid INT literal"),
        ("int1 = ' 7'", 7, "invalid INT literal"),
        ("int1 = '9223372036854775808'", 7, "out of range"),
        ("int1 IN (6, 'x')", 12, "invalid INT literal"),
        ("int1 BETWEEN '6' AND '8.5'", 21, "invalid INT literal"),
        ("int1 = 1e3", 7, "expected Int literal"),
        (
            "double1 = 'abc'",
            10,
            "invalid DOUBLE literal for double1: \"abc\"",
        ),
        ("double1 = ' 0.01'", 10, "is not a decimal number"),
        ("double1 = '0.01 '", 10, "is not a decimal number"),
        ("double1 = '1,000'", 10, "is not a decimal number"),
        ("double1 = '0x1p-2'", 10, "is not a decimal number"),
        ("double1 = 'NaN'", 10, "is not a decimal number"),
        ("double1 = '-Infinity'", 10, "is not a decimal number"),
        ("double1 = '1e999'", 10, "out of range"),
        ("double1 IN ('1', 'two')", 17, "invalid DOUBLE literal"),
        ("double1 STARTS_WITH '1'", 8, "unsupported for double1"),
        ("keyword1 = true", 11, "expected Keyword literal"),
        ("text1 > 'a'", 6, "unsupported for text1"),
        ("array1 BETWEEN 'a' AND 'b'", 7, "unsupported for array1"),
        ("bool1 STARTS_WITH 't'", 6, "unsupported for bool1"),
        ("StartTime = 'yesterday'", 12, "invalid RFC3339"),
        ("int1 = 9223372036854775808", 7, "out of range"),
        ("double1 = 1e999", 10, "out of range"),
        ("keyword1 = 'unterminated", 11, "unterminated"),
        ("keyword1 = 'a\\q'", 13, "unsupported escape"),
        ("keyword1 ! 'x'", 9, "unsupported operator !"),
        ("keyword1 LIKE 'x'", 9, "unsupported operator"),
        ("keyword1 IN ()", 13, "expected Keyword literal"),
        (
            "keyword1 = 'x' ORDER BY FlowType",
            24,
            "unsupported ORDER BY field",
        ),
        (
            "keyword1 = 'x' LIMIT 1",
            15,
            "unsupported trailing query part",
        ),
        (
            "ExecutionStatus = 'Unknown'",
            18,
            "unsupported ExecutionStatus",
        ),
        ("bool1 = 'maybe'", 8, "expected Bool literal"),
        ("array1 = true", 9, "expected KeywordArray literal"),
        ("keyword1 BETWEEN 'a' b", 21, "expected AND"),
        ("WorkflowId IN ('a'", 18, "expected )"),
        ("int1 = 1e", 7, "expected digits in numeric exponent"),
    ] {
        let error = parse_and_check(query, &registry()).expect_err(query);
        assert_eq!(error.offset, offset, "{query}: {error}");
        assert!(error.message.contains(reason), "{query}: {error}");
    }
}

#[test]
fn compares_default_and_explicit_order() {
    let running = Document::default()
        .with("RunId", SearchValue::String("r1".into()))
        .with("ExecutionStatus", SearchValue::String("Running".into()))
        .with("StartTime", SearchValue::Datetime(10));
    let finished = Document::default()
        .with("RunId", SearchValue::String("r2".into()))
        .with("ExecutionStatus", SearchValue::String("Completed".into()))
        .with("StartTime", SearchValue::Datetime(20))
        .with("CloseTime", SearchValue::Datetime(30));
    let earlier = Document::default()
        .with("RunId", SearchValue::String("r3".into()))
        .with("ExecutionStatus", SearchValue::String("Failed".into()))
        .with("StartTime", SearchValue::Datetime(15))
        .with("CloseTime", SearchValue::Datetime(25));
    assert_eq!(
        compare_documents(&check(""), &running, &finished),
        Ordering::Less
    );
    assert_eq!(
        compare_documents(&check(""), &finished, &earlier),
        Ordering::Less
    );
    let ascending = check("ORDER BY StartTime ASC");
    assert_eq!(
        ascending.order_by(),
        Some(("StartTime", SortDirection::Ascending))
    );
    assert_eq!(
        compare_documents(&ascending, &running, &finished),
        Ordering::Less
    );
    let descending = check("FlowType = 'x' ORDER BY CloseTime DESC");
    assert_eq!(
        descending.order_by(),
        Some(("CloseTime", SortDirection::Descending))
    );
    assert_eq!(
        compare_documents(&descending, &finished, &earlier),
        Ordering::Less
    );
    assert_eq!(
        compare_documents(&descending, &finished, &running),
        Ordering::Less
    );
}

#[test]
fn repository_query_corpus_parses_and_type_checks() {
    for query in [
        "WorkflowId = 'flow-1'", // SDK client contracts, all five languages.
        "CustomKeywordField = 'sf-value'", // SDK SearchFlows integration suites.
        "FlowType = 'CheckoutFlow' AND keyword1 = 'ready' AND WorkflowId IN ('a', 'b')", // Go example slots.
        "FlowType = 'AgenticCustomerRefundFlow' AND keyword1 = 'synthetic-search@example.com'", // Go refund example.
        "FlowType = 'AgenticCustomerRefundFlow' AND keyword2 = 'awaiting-manager-rule'",
        "FlowType = 'AgenticCustomerRefundFlow' AND double1 = 14000",
        "FlowType = 'JobPostingFlow' AND text1 = 'Principal'", // Go job post example.
        "FlowType = 'DealDSLFlow' AND keyword1 IS NOT NULL",   // Go deal example.
        "FlowType = 'DealDSLFlow' AND keyword3='buyer' AND keyword1='process'",
        "(FlowType = 'CheckoutFlow' OR FlowType = 'RefundFlow') AND (WorkflowType = \"Engine\")", // Web search.
        "ExecutionStatus = \"Running\" AND FlowType = \"Checkout\"", // Web basic query.
        "WorkflowId = \"order-42\" AND StartTime >= \"2026-07-01T00:00:00Z\"", // Web advanced query.
        "ExecutionStatus IN (\"Running\", \"Failed\")", // Web advanced syntax.
        "OrderNumber = 12345 AND flag = \"true\"",      // Web basic builder for Keyword and Bool.
        "(OrderNumber = 12345) AND (FlowType = 'CheckoutFlow')", // Example-server wrapper.
        "DexWorkQueuePermissions = 'charge' AND FlowType = 'CheckoutFlow'", // Web v2 search.
        "FlowType = 'CheckoutFlow' AND (DexWorkQueuePermissions = 'refund.manage' OR DexWorkQueuePermissions = 'refund.message')", // Web v2 permission union.
        "FlowType = 'CheckoutFlow' AND StartTime >= '2026-07-01T00:00:00.123456789Z'", // Web v2 datetime filter.
        "(WorkflowId = 'a' OR WorkflowId = 'b') AND WorkflowType = 'Engine'", // Web search wrapper.
        "WorkflowId = 'a' AND ExecutionStatus != \"ContinuedAsNew\"",         // Server integration.
        "WorkflowId = 'a' AND ExecutionStatus = \"ContinuedAsNew\"",          // Server integration.
        "WorkflowId = 'a' OR WorkflowId = 'b'",                               // Server integration.
        "CustomDatetimeField='2026-07-03T00:00:00Z' AND CustomDoubleField='0.01'", // Server persistence integration.
        "",
        "   ", // Empty SDK and server searches.
    ] {
        check(query);
    }
}

#[test]
fn cadence_only_query_spellings_are_rejected() {
    for query in ["WorkflowID = 'flow-1'", "CloseStatus = 'CONTINUED_AS_NEW'"] {
        let error = parse_and_check(query, &registry()).expect_err(query);
        assert!(
            error.message.contains("unknown visibility field"),
            "{error}"
        );
    }
}

#[test]
fn null_logic_keeps_negations_unknown() {
    let document = Document::default().with("bool1", SearchValue::Bool(true));
    for input in [
        "keyword2 = 'x'",
        "keyword2 != 'x'",
        "keyword2 IN ('x')",
        "keyword2 NOT IN ('x')",
        "NOT keyword2 = 'x'",
        "NOT keyword2 IN ('x')",
        "NOT (keyword2 != 'x')",
        "keyword2 = 'x' AND bool1 = true",
        "keyword2 = 'x' OR bool1 = false",
    ] {
        assert!(!evaluate(&check(input), &document), "{input}");
    }
    for input in [
        "keyword2 IS NULL",
        "NOT keyword2 IS NOT NULL",
        "keyword2 = 'x' OR bool1 = true",
        "keyword2 = 'x' AND bool1 = false OR bool1 = true",
    ] {
        assert!(evaluate(&check(input), &document), "{input}");
    }
}

#[test]
fn untrusted_expression_depth_and_count_are_bounded() {
    let nested = format!("{}keyword1 = 'x'{}", "(".repeat(70), ")".repeat(70));
    let error = parse_and_check(&nested, &registry()).expect_err("deep nesting");
    assert!(error.message.contains("nesting exceeds 64"), "{error}");
    let nested_not = format!("{}keyword1 = 'x'", "NOT ".repeat(70));
    let error = parse_and_check(&nested_not, &registry()).expect_err("deep NOT");
    assert!(error.message.contains("nesting exceeds 64"), "{error}");

    let long_chain = vec!["keyword1 = 'x'"; 10_001].join(" OR ");
    let error = parse_and_check(&long_chain, &registry()).expect_err("too many expressions");
    assert!(error.message.contains("10000 expressions"), "{error}");
    let accepted_chain = vec!["keyword1 = 'x'"; 5_000].join(" OR ");
    let accepted = check(&accepted_chain);
    assert!(!evaluate(&accepted, &Document::default()));
    let oversized = " ".repeat(256 * 1024 + 1);
    let error = parse_and_check(&oversized, &registry()).expect_err("oversized query");
    assert_eq!(error.offset, 256 * 1024);
}
