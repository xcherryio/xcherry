// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::net::SocketAddr;

use crate::{DevCommandOptions, start_dev_server};

#[tokio::test]
async fn metrics_listener_is_off_by_default() {
    let directory = tempfile::tempdir().expect("temporary directory exists");
    let reserved = std::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .expect("loopback port is available");
    let api_address = reserved.local_addr().expect("loopback address exists");
    drop(reserved);
    let server = start_dev_server(&DevCommandOptions {
        data_dir: directory.path().to_path_buf(),
        listen: api_address,
        log_dir: None,
        config: None,
    })
    .await
    .expect("dev server starts");
    assert!(server.metrics_address().is_none());
    server.shutdown().await.expect("dev server shuts down");
}
