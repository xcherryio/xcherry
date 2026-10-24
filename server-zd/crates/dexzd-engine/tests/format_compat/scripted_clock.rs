// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::sync::atomic::{AtomicI64, Ordering};

use dexzd_clock::WallClock;

/// Wall clock that moves only when the test advances it, so recorded timestamps never drift.
pub(crate) struct ScriptedClock {
    now_micros: AtomicI64,
}

impl ScriptedClock {
    pub(crate) fn starting_at(now_micros: i64) -> Self {
        Self {
            now_micros: AtomicI64::new(now_micros),
        }
    }

    pub(crate) fn advance(&self, micros: i64) {
        self.now_micros.fetch_add(micros, Ordering::SeqCst);
    }
}

impl WallClock for ScriptedClock {
    fn now_unix_micros(&self) -> i64 {
        self.now_micros.load(Ordering::SeqCst)
    }
}
