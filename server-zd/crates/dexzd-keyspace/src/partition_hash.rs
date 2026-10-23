// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::hash::Hasher;

use siphasher::sip::SipHasher24;

use crate::{FlowHash, PartitionId, TenantId};

/// Errors from building a [`PartitionHasher`] from the cluster object.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PartitionHashError {
    /// The partition count is not a power of two in `1..=65536`.
    #[error("partition count {partition_count} must be a power of two between 1 and 65536")]
    InvalidPartitionCount {
        /// The rejected partition count.
        partition_count: u32,
    },
    /// The hash key is not 16 bytes.
    #[error("partition hash key must be 16 bytes, got {length}")]
    InvalidKeyLength {
        /// The rejected length.
        length: usize,
    },
}

/// Maps (tenant ID, Flow ID) to a [`FlowHash`] and a [`PartitionId`] with keyed SipHash-2-4.
///
/// The key and partition count come from the cluster object and never change, so every node
/// computes the same partition for the same Flow.
#[derive(Clone)]
pub struct PartitionHasher {
    key0: u64,
    key1: u64,
    partition_bits: u32,
}

impl PartitionHasher {
    /// Creates a hasher from the cluster object's 16-byte key and partition count.
    ///
    /// Returns an error when the key is not 16 bytes or the count is not a power of two in
    /// `1..=65536`.
    pub fn new(key: &[u8], partition_count: u32) -> Result<Self, PartitionHashError> {
        let key: [u8; 16] = key
            .try_into()
            .map_err(|_| PartitionHashError::InvalidKeyLength { length: key.len() })?;
        if !partition_count.is_power_of_two() || partition_count > 65_536 {
            return Err(PartitionHashError::InvalidPartitionCount { partition_count });
        }
        let (key0_bytes, key1_bytes) = key.split_at(8);
        Ok(Self {
            key0: u64::from_le_bytes(key0_bytes.try_into().expect("split_at(8) of 16 bytes")),
            key1: u64::from_le_bytes(key1_bytes.try_into().expect("split_at(8) of 16 bytes")),
            partition_bits: partition_count.trailing_zeros(),
        })
    }

    /// Returns the number of partitions.
    pub fn partition_count(&self) -> u32 {
        1_u32 << self.partition_bits
    }

    /// Returns the number of high hash bits that select the partition.
    pub fn partition_bits(&self) -> u32 {
        self.partition_bits
    }

    /// Hashes tenant ID (8 bytes big-endian) followed by the UTF-8 Flow ID.
    pub fn flow_hash(&self, tenant_id: TenantId, flow_id: &str) -> FlowHash {
        let mut hasher = SipHasher24::new_with_keys(self.key0, self.key1);
        hasher.write(&tenant_id.to_be_bytes());
        hasher.write(flow_id.as_bytes());
        FlowHash(hasher.finish())
    }

    /// Returns the partition selected by the high bits of `flow_hash`.
    pub fn partition_of(&self, flow_hash: FlowHash) -> PartitionId {
        if self.partition_bits == 0 {
            return PartitionId(0);
        }
        let partition = flow_hash.0 >> (64 - self.partition_bits);
        PartitionId(u16::try_from(partition).expect("at most 16 partition bits"))
    }

    /// Returns the partition of (tenant ID, Flow ID).
    pub fn partition_of_flow(&self, tenant_id: TenantId, flow_id: &str) -> PartitionId {
        self.partition_of(self.flow_hash(tenant_id, flow_id))
    }
}

impl std::fmt::Debug for PartitionHasher {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The key is a secret that prevents crafted Flow IDs from targeting one partition.
        formatter
            .debug_struct("PartitionHasher")
            .field("partition_count", &self.partition_count())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::{PartitionHashError, PartitionHasher};
    use crate::{PartitionId, TenantId};

    const KEY: [u8; 16] = [7; 16];

    #[test]
    fn rejects_bad_partition_counts_and_keys() {
        for count in [0, 3, 1000, 131_072] {
            assert_eq!(
                PartitionHasher::new(&KEY, count).unwrap_err(),
                PartitionHashError::InvalidPartitionCount {
                    partition_count: count
                }
            );
        }
        assert!(matches!(
            PartitionHasher::new(&KEY[..15], 1024),
            Err(PartitionHashError::InvalidKeyLength { length: 15 })
        ));
    }

    #[test]
    fn partition_uses_high_bits_and_tenant_changes_hash() {
        let hasher = PartitionHasher::new(&KEY, 1024).unwrap();
        let hash = hasher.flow_hash(TenantId::DEFAULT, "order-1");
        assert_eq!(
            hasher.partition_of(hash),
            PartitionId((hash.0 >> 54) as u16)
        );
        assert_ne!(hash, hasher.flow_hash(TenantId(1), "order-1"));
        assert_eq!(hash, hasher.flow_hash(TenantId::DEFAULT, "order-1"));
    }

    #[test]
    fn single_partition_cluster_maps_everything_to_zero() {
        let hasher = PartitionHasher::new(&KEY, 1).unwrap();
        assert_eq!(
            hasher.partition_of_flow(TenantId::DEFAULT, "a"),
            PartitionId(0)
        );
    }

    #[test]
    fn known_hash_value_never_changes() {
        // Changing this value would move every existing Flow to another partition.
        let hasher = PartitionHasher::new(&[0_u8; 16], 1024).unwrap();
        let hash = hasher.flow_hash(TenantId::DEFAULT, "flow");
        assert_eq!(format!("{:016x}", hash.0), KNOWN_FLOW_HASH);
    }

    // Cross-checked with an independent SipHash-2-4 implementation.
    const KNOWN_FLOW_HASH: &str = "3a72068066124e22";
}
