// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::path::PathBuf;

const INTERNAL_PROTO_FILES: &[&str] = &[
    "dexzd/v1/log.proto",
    "dexzd/v1/metadata.proto",
    "dexzd/v1/snapshot.proto",
];

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let public_proto_directory = PathBuf::from("../../../protos");
    let internal_proto_directory = PathBuf::from("../../proto");
    let mut protos = vec![public_proto_directory.join("dex.proto")];
    protos.extend(
        INTERNAL_PROTO_FILES
            .iter()
            .map(|file| internal_proto_directory.join(file)),
    );
    let mut prost_config = tonic_prost_build::Config::new();
    prost_config.protoc_executable(protoc_bin_vendored::protoc_bin_path()?);
    // Apply must be deterministic, so every proto map becomes a BTreeMap.
    prost_config.btree_map(["."]);
    let includes = [
        public_proto_directory,
        internal_proto_directory,
        protoc_bin_vendored::include_path()?,
    ];
    tonic_prost_build::configure().compile_with_config(prost_config, &protos, &includes)?;
    for proto in &protos {
        println!("cargo:rerun-if-changed={}", proto.display());
    }
    Ok(())
}
