// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::collections::BTreeSet;

use dexzd_keyspace::{FlowHash, RunId};
use dexzd_protocol::dex::{GetAttributesResponse, Kv, Value, value};
use dexzd_protocol::dexzd::v1::{HardDeleteObjectReference, stored_value};

use crate::FlowStateReader;
use crate::read_model::{ReadModelError, RunView};
use crate::values::wire_value;

/// Attribute object the API server must fetch before returning GetAttributes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HardDeleteAttribute {
    /// Attribute key in the response.
    pub key: String,
    /// Object to fetch from hard-delete storage.
    pub object: HardDeleteObjectReference,
}

/// GetAttributes data and objects requiring server-side hydration.
#[derive(Debug, Clone, PartialEq)]
pub struct AttributesRead {
    /// Wire response with sorted Attributes and references for large values.
    pub response: GetAttributesResponse,
    /// Hard-delete objects whose values must replace the matching response entries.
    pub hard_delete_attributes: Vec<HardDeleteAttribute>,
}

/// Reads selected Attributes in key order; `all_keys` includes AttributeMap instances.
pub fn get_attributes(
    reader: &dyn FlowStateReader,
    flow_hash: FlowHash,
    run_id: RunId,
    keys: &[String],
    all_keys: bool,
) -> Result<AttributesRead, ReadModelError> {
    let view = RunView::open(reader, flow_hash, run_id)?;
    let selected: BTreeSet<String> = if all_keys {
        view.rows
            .attributes(&view.transaction)?
            .into_iter()
            .map(|(key, _)| key)
            .collect()
    } else {
        keys.iter().cloned().collect()
    };
    let mut attributes = Vec::new();
    let mut hard_delete_attributes = Vec::new();
    for key in selected {
        let Some(record) = view.rows.attribute(&view.transaction, &key)? else {
            continue;
        };
        let Some(stored) = record.value.as_ref() else {
            continue;
        };
        let value = match stored.kind.as_ref() {
            Some(stored_value::Kind::HardDeleteObject(object)) => {
                hard_delete_attributes.push(HardDeleteAttribute {
                    key: key.clone(),
                    object: object.clone(),
                });
                None
            }
            _ => Some(wire_value(view.rows, stored)),
        };
        attributes.push(Kv { key, value });
    }
    Ok(AttributesRead {
        response: GetAttributesResponse { attributes },
        hard_delete_attributes,
    })
}

pub(super) fn all_attribute_kvs(view: &RunView<'_>) -> Result<Vec<Kv>, ReadModelError> {
    let attributes: Vec<Kv> = view
        .rows
        .attributes(&view.transaction)?
        .into_iter()
        .filter_map(|(key, record)| {
            record.value.map(|stored| Kv {
                key,
                value: Some(match stored.kind.as_ref() {
                    Some(stored_value::Kind::HardDeleteObject(object)) => {
                        hard_delete_reference(view.rows.run_id, object)
                    }
                    _ => wire_value(view.rows, &stored),
                }),
            })
        })
        .collect();
    Ok(attributes)
}

fn hard_delete_reference(run_id: RunId, object: &HardDeleteObjectReference) -> Value {
    let reference = format!("hd1|{}|{}", run_id, object.object_id);
    let kind = if object.is_string_value {
        value::Kind::InternalBlobIdForStringValue(reference)
    } else {
        value::Kind::InternalBlobIdForObjValue(reference)
    };
    Value { kind: Some(kind) }
}
