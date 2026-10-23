// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::fmt;
use std::str::FromStr;

/// Errors from parsing an identifier received from a client or Worker.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum IdentifierError {
    /// The run ID text is not a hyphenated UUID.
    #[error("run ID {text:?} is not a valid UUID")]
    InvalidRunId {
        /// The rejected text.
        text: String,
    },
    /// The run ID bytes do not have length 16.
    #[error("run ID must be 16 bytes, got {length}")]
    InvalidRunIdLength {
        /// The rejected length.
        length: usize,
    },
}

/// Tenant that owns a Flow. Tenant 0 is the default tenant of a single-tenant cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct TenantId(pub u64);

impl TenantId {
    /// The only tenant of a single-tenant cluster.
    pub const DEFAULT: Self = Self(0);

    /// Returns the 8-byte big-endian encoding used in keys and in the partition hash.
    pub fn to_be_bytes(self) -> [u8; 8] {
        self.0.to_be_bytes()
    }
}

/// Partition number in `0..partition_count`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PartitionId(pub u16);

impl PartitionId {
    /// Returns the 2-byte big-endian encoding used as the prefix of partition-scoped keys.
    pub fn to_be_bytes(self) -> [u8; 2] {
        self.0.to_be_bytes()
    }
}

impl fmt::Display for PartitionId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

/// 64-bit keyed hash of (tenant ID, Flow ID). Its high bits select the partition, and it
/// prefixes every Flow key so that a partition is a contiguous key range.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FlowHash(pub u64);

impl FlowHash {
    /// Returns the 8-byte big-endian encoding used as the first bytes of Flow keys.
    pub fn to_be_bytes(self) -> [u8; 8] {
        self.0.to_be_bytes()
    }
}

/// Run ID: a UUIDv7 that identifies one run of a Flow and stays the same across time travel.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RunId(pub [u8; 16]);

impl RunId {
    /// Creates a run ID from its 16-byte key encoding.
    ///
    /// Returns [`IdentifierError::InvalidRunIdLength`] when `bytes` is not 16 bytes long.
    pub fn from_slice(bytes: &[u8]) -> Result<Self, IdentifierError> {
        let array: [u8; 16] =
            bytes
                .try_into()
                .map_err(|_| IdentifierError::InvalidRunIdLength {
                    length: bytes.len(),
                })?;
        Ok(Self(array))
    }

    /// Returns the 16-byte key encoding.
    pub fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}

impl fmt::Display for RunId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", uuid::Uuid::from_bytes(self.0).hyphenated())
    }
}

impl fmt::Debug for RunId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "RunId({self})")
    }
}

impl FromStr for RunId {
    type Err = IdentifierError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        // Only the canonical 36-character form is accepted, so one run has one wire spelling.
        if text.len() != 36 {
            return Err(IdentifierError::InvalidRunId {
                text: text.to_owned(),
            });
        }
        let parsed = uuid::Uuid::try_parse(text).map_err(|_| IdentifierError::InvalidRunId {
            text: text.to_owned(),
        })?;
        if parsed.hyphenated().to_string() != text {
            return Err(IdentifierError::InvalidRunId {
                text: text.to_owned(),
            });
        }
        Ok(Self(*parsed.as_bytes()))
    }
}

/// Identity of one stored value inside a run: the run seq of the command that wrote it and the
/// value's position inside that command. Apply assigns it, so it is deterministic.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ValueId {
    /// Run seq of the command that wrote the value.
    pub run_seq: u64,
    /// Zero-based position of the value among the values written by that command.
    pub index_in_command: u32,
}

impl ValueId {
    /// Returns the 12-byte big-endian key suffix.
    pub fn to_key_suffix(self) -> [u8; 12] {
        let mut suffix = [0_u8; 12];
        suffix[..8].copy_from_slice(&self.run_seq.to_be_bytes());
        suffix[8..].copy_from_slice(&self.index_in_command.to_be_bytes());
        suffix
    }
}

#[cfg(test)]
mod tests {
    use super::{RunId, ValueId};

    #[test]
    fn run_id_round_trips_through_canonical_text() {
        let text = "0192f3a4-5b6c-7d8e-9fa0-b1c2d3e4f5a6";
        let run_id: RunId = text.parse().unwrap();
        assert_eq!(run_id.to_string(), text);
    }

    #[test]
    fn run_id_rejects_non_canonical_text() {
        for text in [
            "0192F3A4-5B6C-7D8E-9FA0-B1C2D3E4F5A6",
            "0192f3a45b6c7d8e9fa0b1c2d3e4f5a6",
            "{0192f3a4-5b6c-7d8e-9fa0-b1c2d3e4f5a6}",
            "",
        ] {
            assert!(text.parse::<RunId>().is_err(), "{text}");
        }
    }

    #[test]
    fn value_id_key_suffix_orders_by_seq_then_index() {
        let first = ValueId {
            run_seq: 7,
            index_in_command: 9,
        }
        .to_key_suffix();
        let second = ValueId {
            run_seq: 8,
            index_in_command: 0,
        }
        .to_key_suffix();
        assert!(first < second);
    }
}
