// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Library defaults remain independent of command-line environment settings.

#[test]
fn library_defaults_ignore_process_environment() {
    const MARKER: &str = "S3MOCK_LIBRARY_DEFAULTS_CHILD";
    if std::env::var(MARKER).as_deref() != Ok("1") {
        let output = std::process::Command::new(
            std::env::current_exe().expect("the test executable exists"),
        )
        .args([
            "--exact",
            "configuration::library_defaults_ignore_process_environment",
            "--nocapture",
        ])
        .env(MARKER, "1")
        .env("S3MOCK_SIGNATURE_VERIFICATION", "strict")
        .env("S3MOCK_ACCESS_KEY_ID", "unexpected")
        .env("S3MOCK_FAULT_SEED", "invalid-integer")
        .output()
        .expect("the child test starts");
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }
    let config = s3mock_server::ServerConfig::default();
    assert_eq!(
        config.signature_verification,
        s3mock_server::SignatureVerification::Off
    );
    assert_eq!(config.access_key_id, "s3mock");
    assert_eq!(config.fault_seed, 0);
    tokio::runtime::Runtime::new()
        .expect("the runtime starts")
        .block_on(async {
            let directory = tempfile::tempdir().expect("the store directory exists");
            let store = s3mock::Store::open(&s3mock::StoreConfig {
                root_directory: directory.path().to_path_buf(),
                durability: s3mock::Durability::Relaxed,
                etag_mode: s3mock::EtagMode::Md5,
            })
            .expect("the store opens");
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("the listener binds");
            let server = s3mock_server::start(std::sync::Arc::new(store), listener)
                .expect("the server ignores invalid environment configuration");
            super::support::client(&server, "any", "credentials")
                .list_buckets()
                .send()
                .await
                .expect("default library servers accept unsigned requests");
            server.shutdown().await;
        });
}
