// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use md5::Digest as _;
use serde::{Deserialize, Serialize};

/// Checksum algorithm that S3 accepts through `x-amz-checksum-*` headers and trailers.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChecksumAlgorithm {
    Crc32,
    Crc32c,
    Crc64Nvme,
    Sha1,
    Sha256,
}

impl ChecksumAlgorithm {
    pub const ALL: [ChecksumAlgorithm; 5] = [
        ChecksumAlgorithm::Crc32,
        ChecksumAlgorithm::Crc32c,
        ChecksumAlgorithm::Crc64Nvme,
        ChecksumAlgorithm::Sha1,
        ChecksumAlgorithm::Sha256,
    ];

    /// Returns the S3 algorithm name, such as `CRC32C`.
    pub fn s3_name(self) -> &'static str {
        match self {
            ChecksumAlgorithm::Crc32 => "CRC32",
            ChecksumAlgorithm::Crc32c => "CRC32C",
            ChecksumAlgorithm::Crc64Nvme => "CRC64NVME",
            ChecksumAlgorithm::Sha1 => "SHA1",
            ChecksumAlgorithm::Sha256 => "SHA256",
        }
    }

    /// Returns the lowercase header name, such as `x-amz-checksum-crc32c`.
    pub fn header_name(self) -> &'static str {
        match self {
            ChecksumAlgorithm::Crc32 => "x-amz-checksum-crc32",
            ChecksumAlgorithm::Crc32c => "x-amz-checksum-crc32c",
            ChecksumAlgorithm::Crc64Nvme => "x-amz-checksum-crc64nvme",
            ChecksumAlgorithm::Sha1 => "x-amz-checksum-sha1",
            ChecksumAlgorithm::Sha256 => "x-amz-checksum-sha256",
        }
    }

    /// Parses an S3 algorithm name case-insensitively.
    pub fn from_s3_name(name: &str) -> Option<ChecksumAlgorithm> {
        ChecksumAlgorithm::ALL
            .into_iter()
            .find(|algorithm| algorithm.s3_name().eq_ignore_ascii_case(name))
    }

    /// Parses a `x-amz-checksum-*` header name case-insensitively.
    pub fn from_header_name(name: &str) -> Option<ChecksumAlgorithm> {
        ChecksumAlgorithm::ALL
            .into_iter()
            .find(|algorithm| algorithm.header_name().eq_ignore_ascii_case(name))
    }
}

/// A checksum value encoded in base64, as S3 transmits it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectChecksum {
    pub algorithm: ChecksumAlgorithm,
    pub base64_value: String,
}

/// Incremental hasher for one checksum algorithm.
pub(crate) enum ChecksumHasher {
    Crc32(crc32fast::Hasher),
    Crc32c(u32),
    Crc64Nvme(crc64fast_nvme::Digest),
    Sha1(sha1::Sha1),
    Sha256(sha2::Sha256),
}

impl ChecksumHasher {
    pub(crate) fn new(algorithm: ChecksumAlgorithm) -> ChecksumHasher {
        match algorithm {
            ChecksumAlgorithm::Crc32 => ChecksumHasher::Crc32(crc32fast::Hasher::new()),
            ChecksumAlgorithm::Crc32c => ChecksumHasher::Crc32c(0),
            ChecksumAlgorithm::Crc64Nvme => {
                ChecksumHasher::Crc64Nvme(crc64fast_nvme::Digest::new())
            }
            ChecksumAlgorithm::Sha1 => ChecksumHasher::Sha1(sha1::Sha1::new()),
            ChecksumAlgorithm::Sha256 => ChecksumHasher::Sha256(sha2::Sha256::new()),
        }
    }

    pub(crate) fn update(&mut self, data: &[u8]) {
        match self {
            ChecksumHasher::Crc32(hasher) => hasher.update(data),
            ChecksumHasher::Crc32c(state) => *state = crc32c::crc32c_append(*state, data),
            ChecksumHasher::Crc64Nvme(digest) => digest.write(data),
            ChecksumHasher::Sha1(hasher) => hasher.update(data),
            ChecksumHasher::Sha256(hasher) => hasher.update(data),
        }
    }

    pub(crate) fn finish(self) -> ObjectChecksum {
        let (algorithm, bytes) = match self {
            ChecksumHasher::Crc32(hasher) => (
                ChecksumAlgorithm::Crc32,
                hasher.finalize().to_be_bytes().to_vec(),
            ),
            ChecksumHasher::Crc32c(state) => {
                (ChecksumAlgorithm::Crc32c, state.to_be_bytes().to_vec())
            }
            ChecksumHasher::Crc64Nvme(digest) => (
                ChecksumAlgorithm::Crc64Nvme,
                digest.sum64().to_be_bytes().to_vec(),
            ),
            ChecksumHasher::Sha1(hasher) => (ChecksumAlgorithm::Sha1, hasher.finalize().to_vec()),
            ChecksumHasher::Sha256(hasher) => {
                (ChecksumAlgorithm::Sha256, hasher.finalize().to_vec())
            }
        };
        ObjectChecksum {
            algorithm,
            base64_value: BASE64.encode(bytes),
        }
    }
}

/// Encodes raw bytes in standard base64.
pub(crate) fn encode_base64(bytes: &[u8]) -> String {
    BASE64.encode(bytes)
}

/// Decodes standard base64, returning `None` for malformed input.
pub fn decode_base64(value: &str) -> Option<Vec<u8>> {
    BASE64.decode(value.trim()).ok()
}
