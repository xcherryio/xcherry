// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use dexzd_clock::WallClock;
use dexzd_engine::EngineIdSource;
use uuid::Uuid;
use xxhash_rust::xxh3::xxh3_128_with_seed;

use crate::scripted_clock::ScriptedClock;

/// Seeded, counter-derived UUIDv7 values; the engine and the workload share one sequence.
pub(crate) struct SeededIdSource {
    seed: u64,
    issued: AtomicU64,
    clock: Arc<ScriptedClock>,
}

impl SeededIdSource {
    pub(crate) fn new(seed: u64, clock: Arc<ScriptedClock>) -> Self {
        Self {
            seed,
            issued: AtomicU64::new(0),
            clock,
        }
    }

    pub(crate) fn next_uuid_v7(&self) -> Uuid {
        let index = self.issued.fetch_add(1, Ordering::SeqCst);
        let random = xxh3_128_with_seed(&index.to_be_bytes(), self.seed).to_be_bytes();
        let millis = u64::try_from(self.clock.now_unix_micros() / 1000)
            .expect("scripted time is after 1970");
        let random_tail: &[u8; 10] = random[..10].try_into().expect("ten bytes");
        uuid::Builder::from_unix_timestamp_millis(millis, random_tail).into_uuid()
    }
}

impl EngineIdSource for SeededIdSource {
    fn command_id(&self) -> Uuid {
        self.next_uuid_v7()
    }

    fn writer_process_id(&self) -> String {
        self.next_uuid_v7().to_string()
    }

    fn lineage_id(&self) -> Uuid {
        self.next_uuid_v7()
    }
}
