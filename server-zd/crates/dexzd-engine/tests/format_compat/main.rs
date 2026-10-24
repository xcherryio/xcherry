// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Persistent format compatibility: every recorded `testdata/v<N>` restores, replays, and decodes
//! with the current code. See `server-zd/testdata/README.md`.

mod compat_node;
mod format_decoding;
mod generator;
mod golden_replay;
mod recording;
mod scripted_clock;
mod seeded_id_source;
mod state_dump;
mod testdata_tree;
mod workload;
