// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! A second store cannot open a directory that another store holds.

use s3mock::{Durability, Store, StoreConfig};

#[test]
fn a_second_store_on_the_same_directory_fails_to_open() {
    let directory = tempfile::tempdir().unwrap();
    let config = StoreConfig {
        root_directory: directory.path().to_path_buf(),
        durability: Durability::Strict,
    };
    let first = Store::open(&config).unwrap();
    assert!(
        Store::open(&config).is_err(),
        "the metadata database is exclusively locked"
    );
    drop(first);
    Store::open(&config).expect("the directory opens again after the first store closes");
}
