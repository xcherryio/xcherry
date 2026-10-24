// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use dexzd_protocol::dex;
use dexzd_visibility_query::SearchValue;
use prost::Message;
use serde_json::Value as JsonValue;

use crate::SearchError;

pub(super) fn encode_wire_value(value: &dex::Value) -> SearchValue {
    let bytes = value.encode_to_vec();
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write;
        write!(&mut encoded, "{byte:02x}").expect("writing into a String cannot fail");
    }
    SearchValue::String(encoded)
}

pub(super) fn decode_wire_value(field: &str, value: &JsonValue) -> Result<dex::Value, SearchError> {
    let encoded = value.as_str().ok_or_else(|| invalid_wire_value(field))?;
    if !encoded.len().is_multiple_of(2) {
        return Err(invalid_wire_value(field));
    }
    let bytes = encoded
        .as_bytes()
        .chunks_exact(2)
        .map(|digits| {
            let high = hex_digit(digits[0]).ok_or_else(|| invalid_wire_value(field))?;
            let low = hex_digit(digits[1]).ok_or_else(|| invalid_wire_value(field))?;
            Ok((high << 4) | low)
        })
        .collect::<Result<Vec<_>, SearchError>>()?;
    dex::Value::decode(bytes.as_slice()).map_err(|_| invalid_wire_value(field))
}

fn hex_digit(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}

fn invalid_wire_value(field: &str) -> SearchError {
    SearchError::Internal(format!(
        "ParadeDB field {field} has an invalid stored wire value"
    ))
}
