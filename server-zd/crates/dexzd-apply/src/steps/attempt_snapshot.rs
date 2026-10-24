// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! The snapshot an attempt reads (plan section 8.1, C17): recomputed at every attempt start,
//! after the attempt took its locks, and pinned until the attempt ends.

use std::collections::BTreeMap;

use dexzd_protocol::dex::ChannelInfo;
use dexzd_protocol::dexzd::v1::{
    LoadedChannelMessagesRecord, MethodSnapshotRecord, StepExecutionRecord, StepMethod,
    StoredKeyValue,
};

use crate::ApplyError;
use crate::command_handlers::run_cursor::RunCursor;
use crate::state_access::FlowTransaction;
use crate::values::{ValueHolder, hold_stored_value};

/// Selections of one method, from its Step options.
struct MethodSelections {
    attribute_map_instances: Vec<String>,
    channel_names: Vec<String>,
    channel_map_instances: Vec<String>,
}

fn method_selections(step: &StepExecutionRecord, method: StepMethod) -> MethodSelections {
    let options = step.options.clone().unwrap_or_default();
    let (mut attribute_map_instances, mut channel_names, mut channel_map_instances) = match method {
        StepMethod::WaitFor => (
            options.wait_for_load_attribute_map_instances,
            options.wait_for_load_channel_names,
            options.wait_for_load_channel_map_instances,
        ),
        StepMethod::Execute | StepMethod::Unspecified => (
            options.execute_load_attribute_map_instances,
            options.execute_load_channel_names,
            options.execute_load_channel_map_instances,
        ),
    };
    for selection in [
        &mut attribute_map_instances,
        &mut channel_names,
        &mut channel_map_instances,
    ] {
        selection.sort();
        selection.dedup();
    }
    MethodSelections {
        attribute_map_instances,
        channel_names,
        channel_map_instances,
    }
}

/// Builds and pins the snapshot of a starting attempt.
pub(crate) fn compute_attempt_snapshot(
    transaction: &mut FlowTransaction<'_>,
    cursor: &mut RunCursor,
    step: &StepExecutionRecord,
    method: StepMethod,
    attempt: u32,
) -> Result<MethodSnapshotRecord, ApplyError> {
    let selections = method_selections(step, method);
    let pin = ValueHolder::AttemptSnapshot {
        step_execution_id: step.step_execution_id.clone(),
        method: method as i32,
        attempt,
    };
    let mut attributes = Vec::new();
    for (key, record) in cursor.rows.attributes(transaction)? {
        if !is_attribute_selected(&key, &selections.attribute_map_instances) {
            continue;
        }
        if let Some(stored) = &record.value {
            hold_stored_value(transaction, cursor.rows, stored, &pin)?;
        }
        attributes.push(StoredKeyValue {
            key,
            value: record.value,
        });
    }
    let channel_metas = cursor.rows.channel_metas(transaction)?;
    let channel_infos: BTreeMap<String, ChannelInfo> = channel_metas
        .iter()
        .filter(|(_, meta)| meta.pending_count > 0)
        .map(|(name, meta)| {
            (
                name.clone(),
                ChannelInfo {
                    size: i32::try_from(meta.pending_count).unwrap_or(i32::MAX),
                },
            )
        })
        .collect();
    let mut loaded_names: Vec<String> = selections.channel_names.clone();
    for instance in &selections.channel_map_instances {
        if instance.ends_with('/') {
            loaded_names.extend(
                channel_infos
                    .keys()
                    .filter(|name| name.starts_with(instance.as_str()))
                    .cloned(),
            );
        } else {
            loaded_names.push(instance.clone());
        }
    }
    loaded_names.sort();
    loaded_names.dedup();
    let mut loaded_channel_messages = Vec::with_capacity(loaded_names.len());
    for channel_name in loaded_names {
        let messages = cursor
            .rows
            .channel_messages(transaction, &channel_name, usize::MAX)?;
        for message in &messages {
            if let Some(stored) = &message.value {
                hold_stored_value(transaction, cursor.rows, stored, &pin)?;
            }
        }
        loaded_channel_messages.push(LoadedChannelMessagesRecord {
            channel_name,
            messages,
        });
    }
    Ok(MethodSnapshotRecord {
        attributes,
        channel_infos,
        loaded_channel_messages,
        loaded_attribute_map_instances: selections.attribute_map_instances,
        loaded_channel_names: selections.channel_names,
        loaded_channel_map_instances: selections.channel_map_instances,
        condition_results: match method {
            StepMethod::Execute => step.condition_results.clone(),
            StepMethod::WaitFor | StepMethod::Unspecified => None,
        },
    })
}

/// Plain Attributes are always loaded; AttributeMap instances (`map/instance`) only when
/// selected exactly or by a `map/` prefix (as in the Go Server).
fn is_attribute_selected(key: &str, attribute_map_instances: &[String]) -> bool {
    if !key.contains('/') {
        return true;
    }
    attribute_map_instances.iter().any(|selection| {
        if selection.ends_with('/') {
            key.starts_with(selection.as_str())
        } else {
            key == selection
        }
    })
}

#[cfg(test)]
mod tests {
    use super::is_attribute_selected;

    #[test]
    fn attribute_selection_matches_the_go_server() {
        let selections = vec!["orders/".to_owned(), "users/alice".to_owned()];
        assert!(is_attribute_selected("status", &selections));
        assert!(is_attribute_selected("orders/1", &selections));
        assert!(is_attribute_selected("users/alice", &selections));
        assert!(!is_attribute_selected("users/bob", &selections));
        assert!(!is_attribute_selected("items/1", &[]));
    }
}
