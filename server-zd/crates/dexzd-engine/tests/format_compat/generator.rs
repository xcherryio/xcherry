// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::fs;
use std::path::PathBuf;

use crate::recording::record_workload;

/// Directory the generator writes; it must not exist yet or be empty.
const OUTPUT_DIRECTORY_VARIABLE: &str = "DEXZD_FORMAT_COMPAT_OUTPUT";

#[tokio::test(start_paused = true)]
#[ignore = "writes a new testdata version; set DEXZD_FORMAT_COMPAT_OUTPUT and run explicitly"]
async fn generate_format_compat_testdata() {
    let output_directory = PathBuf::from(
        std::env::var_os(OUTPUT_DIRECTORY_VARIABLE)
            .unwrap_or_else(|| panic!("set {OUTPUT_DIRECTORY_VARIABLE} to a new directory")),
    );
    if output_directory.exists() {
        let is_empty = fs::read_dir(&output_directory)
            .expect("read the output directory")
            .next()
            .is_none();
        assert!(
            is_empty,
            "refusing to overwrite {}; recorded versions are immutable",
            output_directory.display()
        );
    }
    fs::create_dir_all(&output_directory).expect("create the output directory");
    record_workload().await.write_to(&output_directory).await;
}
