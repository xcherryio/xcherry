// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

/// Error for a node ID that cannot appear in object keys.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("node ID {node_id:?} must match ^[a-z0-9][a-z0-9-]{{0,62}}$")]
pub struct NodeIdentifierError {
    /// The rejected node ID.
    pub node_id: String,
}

/// Validates a configured node ID. Node IDs are path segments of object keys
/// (`log/<node ID>/...`), so they never contain a separator or another node ID as a prefix
/// segment.
pub fn validate_node_identifier(node_id: &str) -> Result<(), NodeIdentifierError> {
    let bytes = node_id.as_bytes();
    let is_valid = !bytes.is_empty()
        && bytes.len() <= 63
        && (bytes[0].is_ascii_lowercase() || bytes[0].is_ascii_digit())
        && bytes
            .iter()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'-');
    if is_valid {
        Ok(())
    } else {
        Err(NodeIdentifierError {
            node_id: node_id.to_owned(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::validate_node_identifier;

    #[test]
    fn node_identifiers_use_a_key_safe_alphabet() {
        for valid in ["a", "dev", "node-1", "0", &"a".repeat(63)] {
            assert!(validate_node_identifier(valid).is_ok(), "{valid}");
        }
        for invalid in [
            "",
            "-a",
            "A",
            "a/0000000001",
            "a.b",
            "a_b",
            "a b",
            &"a".repeat(64),
        ] {
            assert!(validate_node_identifier(invalid).is_err(), "{invalid}");
        }
    }
}
