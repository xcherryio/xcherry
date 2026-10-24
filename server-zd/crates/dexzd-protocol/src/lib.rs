// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Generated protobuf and gRPC bindings for the public Dex protocol and the server-zd internal formats.

/// Generated public Dex protocol from `protos/dex.proto`.
#[expect(
    clippy::allow_attributes,
    clippy::allow_attributes_without_reason,
    clippy::large_enum_variant,
    reason = "The protobuf generator owns these declarations and their lint attributes."
)]
pub mod dex {
    tonic::include_proto!("dex");
}

/// Generated server-zd internal persistent formats from `server-zd/proto/dexzd/v1`.
pub mod dexzd {
    /// Version 1 of the internal formats.
    #[expect(
        clippy::large_enum_variant,
        reason = "The protobuf generator owns these declarations."
    )]
    pub mod v1 {
        tonic::include_proto!("dexzd.v1");
    }
}
