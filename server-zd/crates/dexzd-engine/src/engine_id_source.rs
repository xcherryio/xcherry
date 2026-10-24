// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

#[cfg(feature = "test-util")]
use std::sync::Arc;
#[cfg(feature = "test-util")]
use std::sync::atomic::{AtomicU64, Ordering};

#[cfg(feature = "test-util")]
use dexzd_clock::WallClock;
use uuid::Uuid;

/// Creates the IDs the engine persists; tests may inject a seeded source for byte-identical runs.
pub trait EngineIdSource: Send + Sync + 'static {
    /// Returns a new UUIDv7 that names a stamped command.
    fn command_id(&self) -> Uuid;

    /// Returns the process ID that an EpochStart batch records for diagnostics.
    fn writer_process_id(&self) -> String;

    /// Returns a new UUIDv7 that names a local store lineage.
    fn lineage_id(&self) -> Uuid;

    /// Returns a new UUIDv7 that names a run, for a client start or a child Flow start.
    fn run_id(&self) -> Uuid;

    /// Returns a new UUIDv7 that names a Channel message published by a client, an RPC, or a
    /// Worker result.
    fn channel_message_id(&self) -> Uuid;

    /// Returns a new UUIDv7 that names an uploaded hard-delete Attribute object.
    fn hard_delete_object_id(&self) -> Uuid;

    /// Returns a new UUIDv7 that names the task of one dispatched method attempt.
    fn dispatch_task_id(&self) -> Uuid;
}

/// [`EngineIdSource`] backed by the system clock and random numbers.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemEngineIdSource;

impl EngineIdSource for SystemEngineIdSource {
    fn command_id(&self) -> Uuid {
        Uuid::now_v7()
    }

    fn writer_process_id(&self) -> String {
        Uuid::now_v7().to_string()
    }

    fn lineage_id(&self) -> Uuid {
        Uuid::now_v7()
    }

    fn run_id(&self) -> Uuid {
        Uuid::now_v7()
    }

    fn channel_message_id(&self) -> Uuid {
        Uuid::now_v7()
    }

    fn hard_delete_object_id(&self) -> Uuid {
        Uuid::now_v7()
    }

    fn dispatch_task_id(&self) -> Uuid {
        Uuid::now_v7()
    }
}

/// Seeded [`EngineIdSource`]: UUIDv7s that follow a seed, a clock, and the call order.
#[cfg(feature = "test-util")]
pub struct SeededEngineIdSourceForTest {
    seed: u64,
    issued: AtomicU64,
    clock: Arc<dyn WallClock>,
}

#[cfg(feature = "test-util")]
impl SeededEngineIdSourceForTest {
    /// Creates a source; IDs increase while `clock` never moves back. Share it between processes.
    pub fn new(seed: u64, clock: Arc<dyn WallClock>) -> Self {
        Self {
            seed,
            issued: AtomicU64::new(0),
            clock,
        }
    }

    fn next_uuid_v7(&self) -> Uuid {
        let index = self.issued.fetch_add(1, Ordering::SeqCst);
        let millis = u64::try_from(self.clock.now_unix_micros() / 1_000)
            .expect("test clocks run after 1970");
        let mixed = splitmix64(self.seed ^ splitmix64(index));
        // 42 counter bits fill rand_a and the top of rand_b; 32 mixed bits fill the rest.
        let counter = index & ((1 << 42) - 1);
        let mut random = [0_u8; 10];
        random[..2].copy_from_slice(&u16::try_from(counter >> 30).expect("12 bits").to_be_bytes());
        let tail = ((counter & ((1 << 30) - 1)) << 32) | (mixed & u64::from(u32::MAX));
        random[2..].copy_from_slice(&tail.to_be_bytes());
        uuid::Builder::from_unix_timestamp_millis(millis, &random).into_uuid()
    }
}

#[cfg(feature = "test-util")]
impl EngineIdSource for SeededEngineIdSourceForTest {
    fn command_id(&self) -> Uuid {
        self.next_uuid_v7()
    }

    fn writer_process_id(&self) -> String {
        self.next_uuid_v7().to_string()
    }

    fn lineage_id(&self) -> Uuid {
        self.next_uuid_v7()
    }

    fn run_id(&self) -> Uuid {
        self.next_uuid_v7()
    }

    fn channel_message_id(&self) -> Uuid {
        self.next_uuid_v7()
    }

    fn hard_delete_object_id(&self) -> Uuid {
        self.next_uuid_v7()
    }

    fn dispatch_task_id(&self) -> Uuid {
        self.next_uuid_v7()
    }
}

#[cfg(feature = "test-util")]
fn splitmix64(value: u64) -> u64 {
    let mut mixed = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    mixed ^ (mixed >> 31)
}

#[cfg(all(test, feature = "test-util"))]
mod tests {
    use std::sync::Arc;

    use dexzd_clock::WallClock;

    use super::{EngineIdSource, SeededEngineIdSourceForTest};

    struct FixedClock;

    impl WallClock for FixedClock {
        fn now_unix_micros(&self) -> i64 {
            1_700_000_000_000_000
        }
    }

    #[test]
    fn seeded_ids_repeat_per_seed_and_increase_within_one_millisecond() {
        let first = SeededEngineIdSourceForTest::new(7, Arc::new(FixedClock));
        let second = SeededEngineIdSourceForTest::new(7, Arc::new(FixedClock));
        let other_seed = SeededEngineIdSourceForTest::new(8, Arc::new(FixedClock));
        let ids: Vec<_> = (0..100).map(|_| first.run_id()).collect();
        let repeated: Vec<_> = (0..100).map(|_| second.run_id()).collect();
        assert_eq!(ids, repeated);
        assert!(ids.windows(2).all(|pair| pair[0] < pair[1]));
        assert!(ids.iter().all(|id| id.get_version_num() == 7));
        assert_ne!(ids[0], other_seed.run_id());
    }
}
