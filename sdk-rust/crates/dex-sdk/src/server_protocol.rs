// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use dex_protocol::dex::ServerInfo;

pub(crate) const MINIMUM_SUPPORTED_SERVER_PROTOCOL_VERSION: u32 = 1;
pub(crate) const MAXIMUM_SUPPORTED_SERVER_PROTOCOL_VERSION: u32 = 3;
pub(crate) const HARD_DELETE_ATTRIBUTE_PROTOCOL_VERSION: u32 = 3;
pub(crate) const SDK_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Shares one negotiated Server protocol version; zero means negotiation has not succeeded.
#[derive(Clone, Default)]
pub(crate) struct NegotiatedServerProtocolVersion {
    version: Arc<AtomicU32>,
}

impl NegotiatedServerProtocolVersion {
    pub(crate) fn store(&self, version: u32) {
        self.version.store(version, Ordering::Release);
    }

    pub(crate) fn has_negotiated(&self) -> bool {
        self.version() != 0
    }

    /// Returns `Ok(())` when the negotiated Server accepts hard-delete Attribute writes, and an
    /// error naming `attribute_key` when negotiation is missing or below protocol version 3.
    pub(crate) fn require_hard_delete_attribute_support(
        &self,
        attribute_key: &str,
    ) -> Result<(), String> {
        let version = self.version();
        if version >= HARD_DELETE_ATTRIBUTE_PROTOCOL_VERSION {
            return Ok(());
        }
        let negotiated = if version == 0 {
            "no protocol version was negotiated".to_string()
        } else {
            format!("the negotiated protocol version is {version}")
        };
        Err(format!(
            "hard-delete Attribute {attribute_key} requires Server protocol version \
             {HARD_DELETE_ATTRIBUTE_PROTOCOL_VERSION} or later, but {negotiated}"
        ))
    }

    fn version(&self) -> u32 {
        self.version.load(Ordering::Acquire)
    }
}

pub(crate) fn negotiate_server_protocol(server_info: &ServerInfo) -> Result<u32, String> {
    let server_minimum = server_info.minimum_supported_protocol_version;
    let server_current = server_info.current_protocol_version;
    if MINIMUM_SUPPORTED_SERVER_PROTOCOL_VERSION == 0
        || MAXIMUM_SUPPORTED_SERVER_PROTOCOL_VERSION == 0
        || MINIMUM_SUPPORTED_SERVER_PROTOCOL_VERSION > MAXIMUM_SUPPORTED_SERVER_PROTOCOL_VERSION
    {
        return Err(compatibility_error(
            server_info,
            "Rust SDK protocol interval is invalid",
        ));
    }
    if server_minimum == 0 || server_current == 0 || server_minimum > server_current {
        return Err(compatibility_error(
            server_info,
            "Server protocol interval is invalid",
        ));
    }
    let negotiated = server_current.min(MAXIMUM_SUPPORTED_SERVER_PROTOCOL_VERSION);
    if negotiated < server_minimum || negotiated < MINIMUM_SUPPORTED_SERVER_PROTOCOL_VERSION {
        return Err(compatibility_error(
            server_info,
            "protocol intervals do not overlap",
        ));
    }
    Ok(negotiated)
}

pub(crate) fn server_info_request_error(error: impl std::fmt::Display) -> String {
    format!(
        "Rust SDK version \"{SDK_VERSION}\" protocol \
         [{MINIMUM_SUPPORTED_SERVER_PROTOCOL_VERSION},{MAXIMUM_SUPPORTED_SERVER_PROTOCOL_VERSION}] \
         is incompatible with Server version \
         \"unknown\" protocol [unknown,unknown]: GetServerInfo failed: {error}"
    )
}

fn compatibility_error(server_info: &ServerInfo, reason: &str) -> String {
    let server_version = if server_info.server_version.is_empty() {
        "unknown"
    } else {
        &server_info.server_version
    };
    format!(
        "Rust SDK version \"{SDK_VERSION}\" protocol \
         [{MINIMUM_SUPPORTED_SERVER_PROTOCOL_VERSION},{MAXIMUM_SUPPORTED_SERVER_PROTOCOL_VERSION}] \
         is incompatible with Server version \"{server_version}\" protocol \
         [{},{}]: {reason}",
        server_info.minimum_supported_protocol_version, server_info.current_protocol_version
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn negotiates_highest_common_protocol() {
        for (server_current, expected) in [(1, 1), (2, 2), (3, 3), (4, 3)] {
            let negotiated = negotiate_server_protocol(&ServerInfo {
                server_version: "newer".to_string(),
                minimum_supported_protocol_version: 1,
                current_protocol_version: server_current,
            })
            .expect("compatible interval");
            assert_eq!(negotiated, expected);
        }
    }

    #[test]
    fn hard_delete_attribute_writes_require_protocol_version_three() {
        let negotiated = NegotiatedServerProtocolVersion::default();
        let error = negotiated
            .require_hard_delete_attribute_support("api-key")
            .expect_err("missing negotiation fails closed");
        assert!(error.contains("api-key"));
        assert!(error.contains("no protocol version was negotiated"));
        for version in [1, 2] {
            negotiated.store(version);
            let error = negotiated
                .require_hard_delete_attribute_support("api-key")
                .expect_err("old Server is rejected");
            assert!(error.contains("protocol version 3 or later"));
            assert!(error.contains(&format!("negotiated protocol version is {version}")));
        }
        negotiated.store(3);
        negotiated
            .require_hard_delete_attribute_support("api-key")
            .expect("protocol 3 accepts hard-delete writes");
    }

    #[test]
    fn rejects_invalid_and_disjoint_server_intervals() {
        for server_info in [
            ServerInfo {
                server_version: "zero".to_string(),
                minimum_supported_protocol_version: 0,
                current_protocol_version: 0,
            },
            ServerInfo {
                server_version: "reversed".to_string(),
                minimum_supported_protocol_version: 2,
                current_protocol_version: 1,
            },
            ServerInfo {
                server_version: "future".to_string(),
                minimum_supported_protocol_version: 4,
                current_protocol_version: 4,
            },
        ] {
            let error = negotiate_server_protocol(&server_info).expect_err("incompatible interval");
            assert!(error.contains("Rust SDK version"));
            assert!(error.contains(&server_info.server_version));
        }
    }

    #[test]
    fn runtime_version_matches_release() {
        let expected = std::env::var("DEX_EXPECTED_SDK_VERSION")
            .unwrap_or_else(|_| env!("CARGO_PKG_VERSION").to_string());
        assert_eq!(SDK_VERSION, expected);
    }
}
