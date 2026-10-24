// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::process::Stdio;
use std::time::Duration;

use tempfile::TempDir;
use tokio::process::Command;

#[tokio::test]
async fn dev_mode_completes_snapshot_with_anonymous_object_store() {
    let directory = TempDir::new().expect("the temporary directory is created");
    let config_path = directory.path().join("config.yaml");
    std::fs::write(
        &config_path,
        "cluster:\n  partitionCount: 16\nsnapshot:\n  interval: 3s\n",
    )
    .expect("the dev configuration is written");
    let log_path = directory.path().join("server.log");
    let log = std::fs::File::create(&log_path).expect("the process log is created");
    let listener =
        std::net::TcpListener::bind("127.0.0.1:0").expect("a loopback port is available");
    let address = listener
        .local_addr()
        .expect("the loopback address is readable");
    drop(listener);
    let mut process = Command::new(env!("CARGO_BIN_EXE_dex-server-zd"))
        .arg("dev")
        .arg("--data-dir")
        .arg(directory.path().join("data"))
        .arg("--listen")
        .arg(address.to_string())
        .arg("--config")
        .arg(&config_path)
        .env("RUST_LOG", "info")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::from(log))
        .kill_on_drop(true)
        .spawn()
        .expect("the dev process starts");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        let output = std::fs::read_to_string(&log_path).expect("the process log is readable");
        if output.contains("snapshot complete") {
            assert!(
                !output.contains("snapshot failed; retrying later"),
                "{output}"
            );
            break;
        }
        assert!(
            process
                .try_wait()
                .expect("the process status is readable")
                .is_none(),
            "the dev process stopped before completing a snapshot: {output}"
        );
        assert!(
            tokio::time::Instant::now() < deadline,
            "the dev process did not complete a snapshot: {output}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    process.kill().await.expect("the dev process stops");
    process.wait().await.expect("the dev process is reaped");
}
