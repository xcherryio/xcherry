// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use prost::Message;
use xxhash_rust::xxh3::xxh3_128;

/// Returns the 16-byte big-endian xxh3-128 of the request encoding; ordered maps make it canonical.
pub(crate) fn request_fingerprint(request: &impl Message) -> Vec<u8> {
    xxh3_128(&request.encode_to_vec()).to_be_bytes().to_vec()
}
