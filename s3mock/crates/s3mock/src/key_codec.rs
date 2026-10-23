// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Order-preserving encoding of (bucket, key, version) tuples into redb keys.
//!
//! Each component escapes `0x00` as `0x00 0xFF` and ends with `0x00 0x00`, so byte-wise
//! comparison of encoded keys equals S3's byte-wise ordering of the original keys.

use ulid::Ulid;

const ESCAPE: u8 = 0x00;
const ESCAPED_ZERO: u8 = 0xFF;
const TERMINATOR: [u8; 2] = [0x00, 0x00];

fn append_escaped(output: &mut Vec<u8>, component: &[u8]) {
    for &byte in component {
        output.push(byte);
        if byte == ESCAPE {
            output.push(ESCAPED_ZERO);
        }
    }
}

/// Encodes the prefix shared by every object key of `bucket`.
pub(crate) fn encode_bucket_prefix(bucket: &str) -> Vec<u8> {
    let mut output = Vec::with_capacity(bucket.len() + 2);
    append_escaped(&mut output, bucket.as_bytes());
    output.extend_from_slice(&TERMINATOR);
    output
}

/// Encodes `(bucket, key)` as the key of the latest-version table.
pub(crate) fn encode_object_key(bucket: &str, key: &str) -> Vec<u8> {
    let mut output = encode_bucket_prefix(bucket);
    append_escaped(&mut output, key.as_bytes());
    output.extend_from_slice(&TERMINATOR);
    output
}

/// Encodes the shared prefix of every key in `bucket` that starts with `key_prefix`.
pub(crate) fn encode_key_prefix(bucket: &str, key_prefix: &str) -> Vec<u8> {
    let mut output = encode_bucket_prefix(bucket);
    append_escaped(&mut output, key_prefix.as_bytes());
    output
}

/// Encodes `(bucket, key, version)` so that newer versions of a key sort first.
pub(crate) fn encode_version_key(bucket: &str, key: &str, internal_version: Ulid) -> Vec<u8> {
    let mut output = encode_object_key(bucket, key);
    output.extend_from_slice(&(!internal_version.0).to_be_bytes());
    output
}

/// Encodes `(bucket, key, upload)` so uploads of a key sort by initiation time.
pub(crate) fn encode_upload_key(bucket: &str, key: &str, upload_id: Ulid) -> Vec<u8> {
    let mut output = encode_object_key(bucket, key);
    output.extend_from_slice(&upload_id.0.to_be_bytes());
    output
}

/// Encodes a part of an upload so parts sort by part number.
pub(crate) fn encode_part_key(upload_key: &[u8], part_number: u32) -> Vec<u8> {
    let mut output = upload_key.to_vec();
    output.extend_from_slice(&part_number.to_be_bytes());
    output
}

/// Decodes the object key from an encoded latest-table or version-table key.
pub(crate) fn decode_object_key(encoded: &[u8], bucket: &str) -> String {
    let bucket_prefix = encode_bucket_prefix(bucket);
    let mut index = bucket_prefix.len();
    let mut key_bytes = Vec::new();
    while index + 1 < encoded.len() {
        let byte = encoded[index];
        if byte != ESCAPE {
            key_bytes.push(byte);
            index += 1;
            continue;
        }
        if encoded[index + 1] == ESCAPED_ZERO {
            key_bytes.push(ESCAPE);
            index += 2;
            continue;
        }
        break;
    }
    String::from_utf8(key_bytes).expect("encoded object keys are built from UTF-8 strings")
}

/// Returns the smallest byte string greater than every string starting with `prefix`, if any.
pub(crate) fn prefix_successor(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut successor = prefix.to_vec();
    while let Some(last) = successor.pop() {
        if last != u8::MAX {
            successor.push(last + 1);
            return Some(successor);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encoded_keys_preserve_byte_order() {
        let keys = ["", "a", "a\0", "a\0b", "a/b", "ab", "b", "\u{e9}"];
        let mut sorted = keys.to_vec();
        sorted.sort();
        let mut encoded: Vec<Vec<u8>> = keys
            .iter()
            .map(|key| encode_object_key("bucket", key))
            .collect();
        encoded.sort();
        let decoded: Vec<String> = encoded
            .iter()
            .map(|value| decode_object_key(value, "bucket"))
            .collect();
        assert_eq!(decoded, sorted);
    }

    #[test]
    fn newer_versions_sort_first() {
        let older = Ulid::from_parts(1, 0);
        let newer = Ulid::from_parts(2, 0);
        assert!(encode_version_key("b", "k", newer) < encode_version_key("b", "k", older));
        assert!(encode_version_key("b", "k", older) < encode_version_key("b", "k\0", newer));
    }
}
