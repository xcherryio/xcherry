// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

/// Encodes strings as a JSON array exactly like Go's `json.Marshal` of a `[]string`,
/// including its HTML-safe escapes, so the projection matches the Go Server byte for byte.
pub(crate) fn encode_string_array(values: &[String]) -> Vec<u8> {
    let mut encoded = String::from("[");
    for (index, text) in values.iter().enumerate() {
        if index > 0 {
            encoded.push(',');
        }
        encoded.push('"');
        for character in text.chars() {
            match character {
                '"' => encoded.push_str("\\\""),
                '\\' => encoded.push_str("\\\\"),
                '\n' => encoded.push_str("\\n"),
                '\r' => encoded.push_str("\\r"),
                '\t' => encoded.push_str("\\t"),
                '<' | '>' | '&' | '\u{2028}' | '\u{2029}' => {
                    encoded.push_str(&format!("\\u{:04x}", u32::from(character)));
                }
                character if u32::from(character) < 0x20 => {
                    encoded.push_str(&format!("\\u{:04x}", u32::from(character)));
                }
                character => encoded.push(character),
            }
        }
        encoded.push('"');
    }
    encoded.push(']');
    encoded.into_bytes()
}
