// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::collections::BTreeMap;

use dexzd_visibility_query::SearchValue;

use super::{SearchRow, SqlStatement, SqlValue};

/// Builds an idempotent upsert for a versioned search row.
#[must_use]
pub fn upsert_statement(row: &SearchRow) -> SqlStatement {
    let mut parameters = vec![
        SqlValue::Text(row.tenant_id.clone()),
        SqlValue::Text(row.run_id.clone()),
        SqlValue::Text(row.workflow_id.clone()),
        SqlValue::Text(row.workflow_type.clone()),
        SqlValue::Text(row.flow_type.clone()),
        SqlValue::Text(row.execution_status.clone()),
        SqlValue::Int(row.start_time_ns),
        SqlValue::OptionalInt(row.close_time_ns),
        SqlValue::OptionalText(row.dex_parent_flow_id.clone()),
        SqlValue::OptionalTextArray(row.active_step_types.clone()),
        SqlValue::OptionalTextArray(row.dex_work_queue_permissions.clone()),
        SqlValue::Int(row.version),
    ];
    let keyword_bucket = bucket_expression(&row.sa_keyword, &mut parameters);
    let text_bucket = bucket_expression(&row.sa_text, &mut parameters);
    let number_bucket = bucket_expression(&row.sa_number, &mut parameters);
    let number_types_bucket = bucket_expression(&row.sa_number_types, &mut parameters);
    let datetime_bucket = bucket_expression(&row.sa_datetime, &mut parameters);
    let bool_bucket = bucket_expression(&row.sa_bool, &mut parameters);
    let wire_bucket = bucket_expression(&row.sa_wire, &mut parameters);
    let sql = format!(
        "INSERT INTO dexzd_visibility_runs (tenant_id, run_id, workflow_id, workflow_type, flow_type, execution_status, start_time_ns, close_time_ns, dex_parent_flow_id, active_step_types, dex_work_queue_permissions, version, sa_keyword, sa_text, sa_number, sa_number_types, sa_datetime, sa_bool, sa_wire) VALUES ($1::text, $2::text, $3::text, $4::text, $5::text, $6::text, $7::bigint, $8::bigint, $9::text, $10::text[], $11::text[], $12::bigint, {keyword_bucket}, {text_bucket}, {number_bucket}, {number_types_bucket}, {datetime_bucket}, {bool_bucket}, {wire_bucket}) ON CONFLICT (tenant_id, run_id) DO UPDATE SET workflow_id = EXCLUDED.workflow_id, workflow_type = EXCLUDED.workflow_type, flow_type = EXCLUDED.flow_type, execution_status = EXCLUDED.execution_status, start_time_ns = EXCLUDED.start_time_ns, close_time_ns = EXCLUDED.close_time_ns, dex_parent_flow_id = EXCLUDED.dex_parent_flow_id, active_step_types = EXCLUDED.active_step_types, dex_work_queue_permissions = EXCLUDED.dex_work_queue_permissions, version = EXCLUDED.version, sa_keyword = EXCLUDED.sa_keyword, sa_text = EXCLUDED.sa_text, sa_number = EXCLUDED.sa_number, sa_number_types = EXCLUDED.sa_number_types, sa_datetime = EXCLUDED.sa_datetime, sa_bool = EXCLUDED.sa_bool, sa_wire = EXCLUDED.sa_wire WHERE EXCLUDED.version > dexzd_visibility_runs.version"
    );
    SqlStatement { sql, parameters }
}

fn bucket_expression(
    bucket: &BTreeMap<String, SearchValue>,
    parameters: &mut Vec<SqlValue>,
) -> String {
    if bucket.is_empty() {
        return "'{}'::jsonb".to_owned();
    }
    let mut fields = Vec::with_capacity(bucket.len());
    for (name, value) in bucket {
        parameters.push(SqlValue::Text(name.clone()));
        let key_placeholder = format!("${}::text", parameters.len());
        let (bind_value, sql_type) = match value {
            SearchValue::String(value) => (SqlValue::Text(value.clone()), "text"),
            SearchValue::StringArray(value) => (SqlValue::TextArray(value.clone()), "text[]"),
            SearchValue::Int(value) | SearchValue::Datetime(value) => {
                (SqlValue::Int(*value), "bigint")
            }
            SearchValue::Double(value) => (SqlValue::Double(*value), "double precision"),
            SearchValue::Bool(value) => (SqlValue::Bool(*value), "boolean"),
        };
        parameters.push(bind_value);
        let value_placeholder = format!("${}::{sql_type}", parameters.len());
        fields.push(format!("{key_placeholder}, to_jsonb({value_placeholder})"));
    }
    format!("jsonb_build_object({})", fields.join(", "))
}
