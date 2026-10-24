// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::collections::BTreeSet;

use dexzd_protocol::dex::{EncodedObject, IndexConfig, IndexType, Value, value};

pub(crate) const ATTRIBUTE_KEY: &str = "DexWorkQueuePermissions";

pub(crate) fn decode(value: Option<&Value>) -> (Vec<String>, bool) {
    let Some(value) = value else {
        return (Vec::new(), true);
    };
    let Some(kind) = value.kind.as_ref() else {
        return (Vec::new(), false);
    };
    let value::Kind::ObjValue(object) = kind else {
        return (Vec::new(), matches!(kind, value::Kind::NullValue(_)));
    };
    if object.encoding != "json" {
        return (Vec::new(), false);
    }
    let Ok(permissions) = serde_json::from_slice::<Option<Vec<String>>>(&object.payload) else {
        return (Vec::new(), false);
    };
    let sorted: BTreeSet<String> = permissions.unwrap_or_default().into_iter().collect();
    (sorted.into_iter().collect(), true)
}

pub(crate) fn projected_value(permissions: &[String]) -> Value {
    if permissions.is_empty() {
        return Value {
            kind: Some(value::Kind::NullValue(0)),
        };
    }
    let json = serde_json::to_string(permissions)
        .expect("serializing a string slice into JSON cannot fail")
        .replace('<', "\\u003c")
        .replace('>', "\\u003e")
        .replace('&', "\\u0026")
        .replace('\u{2028}', "\\u2028")
        .replace('\u{2029}', "\\u2029");
    Value {
        kind: Some(value::Kind::ObjValue(EncodedObject {
            encoding: "json".to_owned(),
            payload: json.into_bytes(),
        })),
    }
}

pub(crate) fn index_config() -> IndexConfig {
    IndexConfig {
        enable: true,
        r#type: IndexType::KeywordArray as i32,
        index_key: ATTRIBUTE_KEY.to_owned(),
    }
}
