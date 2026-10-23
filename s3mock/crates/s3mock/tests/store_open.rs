// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Opening a store at relative and nested roots, as the `s3mock` binary's default directory does.

use std::path::PathBuf;

use s3mock::{Durability, Store, StoreConfig};

/// The only test in this binary, because it changes the process working directory.
#[test]
fn strict_store_opens_at_relative_and_nested_roots() {
    let directory = tempfile::tempdir().unwrap();
    std::env::set_current_dir(directory.path()).unwrap();
    for root in ["s3mock-data", "nested/one/two"] {
        let store = Store::open(&StoreConfig {
            root_directory: PathBuf::from(root),
            durability: Durability::Strict,
        })
        .unwrap();
        store.create_bucket("opened", false).unwrap();
        assert!(directory.path().join(root).join("metadata.redb").is_file());
    }
}
