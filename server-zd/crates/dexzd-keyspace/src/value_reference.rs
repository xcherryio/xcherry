// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::fmt;
use std::str::FromStr;

use crate::{RunId, ValueId};

/// Prefix of every value reference that server-zd sends in `Value.internal_blob_id_*`.
pub const VALUE_REFERENCE_PREFIX: &str = "zd1|";

/// Errors from parsing a value reference received from a client or Worker.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ValueReferenceError {
    /// The text is not `zd1|<run ID>|<seq>-<index>`.
    #[error("value reference {text:?} is not a server-zd value reference")]
    Malformed {
        /// The rejected text.
        text: String,
    },
}

/// Wire reference to a value stored by server-zd: `zd1|<run ID>|<run seq>-<index in command>`.
///
/// SDKs treat the text as opaque and may cache content by it, so one reference always names
/// the same content: every write creates a new [`ValueId`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ValueReference {
    /// Run that owns the value.
    pub run_id: RunId,
    /// The value inside the run.
    pub value_id: ValueId,
}

impl fmt::Display for ValueReference {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{VALUE_REFERENCE_PREFIX}{}|{}-{}",
            self.run_id, self.value_id.run_seq, self.value_id.index_in_command
        )
    }
}

impl FromStr for ValueReference {
    type Err = ValueReferenceError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let malformed = || ValueReferenceError::Malformed {
            text: text.to_owned(),
        };
        let rest = text
            .strip_prefix(VALUE_REFERENCE_PREFIX)
            .ok_or_else(malformed)?;
        let (run_id_text, value_text) = rest.split_once('|').ok_or_else(malformed)?;
        let run_id: RunId = run_id_text.parse().map_err(|_| malformed())?;
        let (seq_text, index_text) = value_text.split_once('-').ok_or_else(malformed)?;
        let is_canonical_number = |digits: &str| {
            !digits.is_empty()
                && digits.bytes().all(|byte| byte.is_ascii_digit())
                && (digits == "0" || !digits.starts_with('0'))
        };
        if !is_canonical_number(seq_text) || !is_canonical_number(index_text) {
            return Err(malformed());
        }
        let run_seq: u64 = seq_text.parse().map_err(|_| malformed())?;
        // Run seqs start at 1, so seq 0 can only come from a forged reference.
        if run_seq == 0 {
            return Err(malformed());
        }
        let index_in_command = index_text.parse().map_err(|_| malformed())?;
        Ok(Self {
            run_id,
            value_id: ValueId {
                run_seq,
                index_in_command,
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::ValueReference;
    use crate::{RunId, ValueId};

    #[test]
    fn value_reference_round_trips() {
        let reference = ValueReference {
            run_id: "0192f3a4-5b6c-7d8e-9fa0-b1c2d3e4f5a6"
                .parse::<RunId>()
                .unwrap(),
            value_id: ValueId {
                run_seq: 12,
                index_in_command: 0,
            },
        };
        let text = reference.to_string();
        assert_eq!(text, "zd1|0192f3a4-5b6c-7d8e-9fa0-b1c2d3e4f5a6|12-0");
        assert_eq!(text.parse::<ValueReference>().unwrap(), reference);
    }

    #[test]
    fn value_reference_rejects_foreign_and_malformed_text() {
        for text in [
            "",
            "blob-123",
            "zd1|",
            "zd1|0192f3a4-5b6c-7d8e-9fa0-b1c2d3e4f5a6",
            "zd1|0192f3a4-5b6c-7d8e-9fa0-b1c2d3e4f5a6|12",
            "zd1|0192f3a4-5b6c-7d8e-9fa0-b1c2d3e4f5a6|012-0",
            "zd1|0192f3a4-5b6c-7d8e-9fa0-b1c2d3e4f5a6|12-+1",
            "zd1|0192f3a4-5b6c-7d8e-9fa0-b1c2d3e4f5a6|0-0",
            "zd1|not-a-uuid|1-0",
            "zd2|0192f3a4-5b6c-7d8e-9fa0-b1c2d3e4f5a6|1-0",
        ] {
            assert!(text.parse::<ValueReference>().is_err(), "{text}");
        }
    }
}
