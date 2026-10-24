// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! A graceful shutdown whose lease release object storage rejects exits 1.

use s3mock_server::SignatureVerification;
use tempfile::TempDir;

use super::harness::{S3Emulator, ServeConfig, ServeProcess, free_loopback_address};
use super::{HELD_LEASE_DURATION, PRIMARY_BUCKET, PRIMARY_PREFIX, SHUTDOWN_BUDGET};

/// SIGTERM exits 1 when the release write is rejected, and the lease stays held.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_rejected_lease_release_makes_sigterm_exit_one() {
    let primary = S3Emulator::start(PRIMARY_BUCKET, SignatureVerification::Strict).await;
    let primary_endpoint = primary.endpoint();
    let directory = TempDir::new().expect("a temporary test directory can be created");
    let spawn_process = |name: &str| {
        let address = free_loopback_address();
        let config_path = directory.path().join(format!("{name}.yaml"));
        ServeConfig {
            primary_endpoint: &primary_endpoint,
            hard_delete_endpoint: None,
            listen: address,
            local_directory: &directory.path().join(format!("{name}-store")),
            lease_duration: HELD_LEASE_DURATION,
            fail_if_held: true,
            parade_db: None,
        }
        .write(&config_path);
        ServeProcess::spawn(
            &config_path,
            address,
            directory.path().join(format!("{name}.log")),
        )
    };

    let mut first = spawn_process("first");
    first.wait_until_serving().await;
    // 503 is a transient rejection, so the release fails at once instead of retrying.
    primary
        .set_fault_rules(&format!(
            r#"[{{"operation":"PutObject","key_prefix":"{PRIMARY_PREFIX}/lease/","action":{{"type":"error","status":503}}}}]"#
        ))
        .await;
    let first_status = first.terminate(SHUTDOWN_BUDGET).await;
    let first_log = first.log();
    assert_eq!(first_status.code(), Some(1), "{first_log}");
    assert!(
        first_log.contains("shutdown did not finish: engine failed: releasing node"),
        "{first_log}"
    );
    // A renewal that hit the rule would stop the node and exit 1 for another reason.
    assert!(!first_log.contains("the node stopped"), "{first_log}");
    primary.set_fault_rules("[]").await;

    let mut successor = spawn_process("successor");
    let successor_status = successor.wait_for_exit(SHUTDOWN_BUDGET).await;
    let successor_log = successor.log();
    assert_eq!(successor_status.code(), Some(1), "{successor_log}");
    assert!(
        successor_log.contains("lease is held by another process"),
        "{successor_log}"
    );
    primary.shutdown().await;
}
