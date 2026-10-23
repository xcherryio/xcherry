// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::collections::BTreeSet;

use dexzd_keyspace::{FlowHash, RunId};
use dexzd_protocol::dex::{EncodedObject, GetAttributesResponse, Kv, Value, value};
use dexzd_protocol::dexzd::v1::{HardDeleteObjectReference, stored_value};

use crate::FlowStateReader;
use crate::read_model::{ReadModelError, RunView};
use crate::values::wire_value;

const PERMISSIONS_ATTRIBUTE_KEY: &str = "DexWorkQueuePermissions";

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
            .chain(std::iter::once(PERMISSIONS_ATTRIBUTE_KEY.to_owned()))
            .collect()
    } else {
        keys.iter().cloned().collect()
    };
    let mut attributes = Vec::new();
    let mut hard_delete_attributes = Vec::new();
    for key in selected {
        if key == PERMISSIONS_ATTRIBUTE_KEY {
            let permissions = view.rows.permission_set(&view.transaction)?.permissions;
            if !permissions.is_empty() {
                attributes.push(Kv {
                    key,
                    value: Some(permissions_value(&permissions)),
                });
            }
            continue;
        }
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
    let mut attributes: Vec<Kv> = view
        .rows
        .attributes(&view.transaction)?
        .into_iter()
        .filter_map(|(key, record)| {
            record.value.map(|stored| Kv {
                key,
                value: Some(wire_value(view.rows, &stored)),
            })
        })
        .collect();
    let permissions = view.rows.permission_set(&view.transaction)?.permissions;
    if !permissions.is_empty() {
        attributes.push(Kv {
            key: PERMISSIONS_ATTRIBUTE_KEY.to_owned(),
            value: Some(permissions_value(&permissions)),
        });
        attributes.sort_by(|left, right| left.key.cmp(&right.key));
    }
    Ok(attributes)
}

fn permissions_value(permissions: &[String]) -> Value {
    let mut json = String::from("[");
    for (index, permission) in permissions.iter().enumerate() {
        if index > 0 {
            json.push(',');
        }
        json.push('"');
        for character in permission.chars() {
            match character {
                '"' => json.push_str("\\\""),
                '\\' => json.push_str("\\\\"),
                '\n' => json.push_str("\\n"),
                '\r' => json.push_str("\\r"),
                '\t' => json.push_str("\\t"),
                character if character < ' ' => {
                    json.push_str(&format!("\\u{:04x}", u32::from(character)));
                }
                character => json.push(character),
            }
        }
        json.push('"');
    }
    json.push(']');
    Value {
        kind: Some(value::Kind::ObjValue(EncodedObject {
            encoding: "json".to_owned(),
            payload: json.into_bytes(),
        })),
    }
}
