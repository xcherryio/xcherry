// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use dexzd_keyspace::{RunId, TenantId};
use dexzd_visibility_query::{SearchDocument, SearchValue};

use crate::SearchError;

const TOKEN_VERSION: u8 = 1;
const TOKEN_BYTES: usize = 59;
const CHECKSUM_START: usize = TOKEN_BYTES - 8;

#[derive(Debug, Clone)]
pub(super) struct PagePosition {
    pub(super) is_running: bool,
    pub(super) close_time_ns: Option<i64>,
    pub(super) start_time_ns: i64,
    pub(super) run_id: RunId,
}

impl PagePosition {
    pub(super) fn encode(&self, tenant_id: TenantId, query: &str) -> String {
        let mut bytes = Vec::with_capacity(TOKEN_BYTES);
        bytes.push(TOKEN_VERSION);
        bytes.extend_from_slice(&tenant_id.to_be_bytes());
        bytes.extend_from_slice(&fingerprint(query).to_be_bytes());
        bytes.push(u8::from(self.is_running));
        bytes.push(u8::from(self.close_time_ns.is_some()));
        bytes.extend_from_slice(&self.close_time_ns.unwrap_or_default().to_be_bytes());
        bytes.extend_from_slice(&self.start_time_ns.to_be_bytes());
        bytes.extend_from_slice(self.run_id.as_bytes());
        bytes.extend_from_slice(&fingerprint(&bytes).to_be_bytes());
        let mut token = String::with_capacity(bytes.len() * 2);
        for byte in bytes {
            use std::fmt::Write;
            write!(&mut token, "{byte:02x}").expect("writing into a String cannot fail");
        }
        token
    }

    pub(super) fn decode(
        token: &str,
        tenant_id: TenantId,
        query: &str,
    ) -> Result<Self, SearchError> {
        if token.len() != TOKEN_BYTES * 2 {
            return Err(invalid_token("length"));
        }
        let bytes = token
            .as_bytes()
            .chunks_exact(2)
            .map(|digits| {
                let high = hex_digit(digits[0]).ok_or_else(|| invalid_token("encoding"))?;
                let low = hex_digit(digits[1]).ok_or_else(|| invalid_token("encoding"))?;
                Ok((high << 4) | low)
            })
            .collect::<Result<Vec<_>, SearchError>>()?;
        if bytes[0] != TOKEN_VERSION {
            return Err(invalid_token("version"));
        }
        if read_u64(&bytes[1..9]) != tenant_id.0 {
            return Err(invalid_token("tenant"));
        }
        if read_u64(&bytes[9..17]) != fingerprint(query) {
            return Err(invalid_token("query changed"));
        }
        if read_u64(&bytes[CHECKSUM_START..]) != fingerprint(&bytes[..CHECKSUM_START]) {
            return Err(invalid_token("checksum"));
        }
        let is_running = match bytes[17] {
            0 => false,
            1 => true,
            _ => return Err(invalid_token("running flag")),
        };
        let close_time_ns = match bytes[18] {
            0 => None,
            1 => Some(read_i64(&bytes[19..27])),
            _ => return Err(invalid_token("close-time flag")),
        };
        let start_time_ns = read_i64(&bytes[27..35]);
        let run_id =
            RunId::from_slice(&bytes[35..CHECKSUM_START]).map_err(|_| invalid_token("run ID"))?;
        Ok(Self {
            is_running,
            close_time_ns,
            start_time_ns,
            run_id,
        })
    }
}

impl SearchDocument for PagePosition {
    fn field_value(&self, field: &str) -> Option<SearchValue> {
        match field {
            "RunId" => Some(SearchValue::String(self.run_id.to_string())),
            "ExecutionStatus" => Some(SearchValue::String(
                if self.is_running {
                    "Running"
                } else {
                    "Completed"
                }
                .to_owned(),
            )),
            "StartTime" => Some(SearchValue::Datetime(self.start_time_ns)),
            "CloseTime" => self.close_time_ns.map(SearchValue::Datetime),
            _ => None,
        }
    }
}

fn invalid_token(reason: &str) -> SearchError {
    SearchError::InvalidArgument(format!("invalid page token {reason}"))
}

fn hex_digit(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}

fn read_u64(bytes: &[u8]) -> u64 {
    u64::from_be_bytes(
        bytes
            .try_into()
            .expect("validated token has eight-byte fields"),
    )
}

fn read_i64(bytes: &[u8]) -> i64 {
    i64::from_be_bytes(
        bytes
            .try_into()
            .expect("validated token has eight-byte fields"),
    )
}

fn fingerprint(bytes: impl AsRef<[u8]>) -> u64 {
    bytes
        .as_ref()
        .iter()
        .fold(0xcbf29ce484222325_u64, |hash, byte| {
            (hash ^ u64::from(*byte)).wrapping_mul(0x100000001b3)
        })
}
