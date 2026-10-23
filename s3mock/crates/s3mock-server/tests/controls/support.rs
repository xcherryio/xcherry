// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Isolated loopback server and HTTP admin client for testing controls.

use std::sync::Arc;
use std::time::Duration;

use aws_sdk_s3::Client;
use aws_sdk_s3::config::{
    BehaviorVersion, Credentials, Region, RequestChecksumCalculation, ResponseChecksumValidation,
    retry::RetryConfig,
};
use clap::Parser;
use s3mock::{Durability, Store, StoreConfig};
use s3mock_server::{ServerConfig, ServerHandle};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

pub(crate) struct RunningServer {
    pub(crate) server: ServerHandle,
    pub(crate) client: Client,
    pub(crate) directory: tempfile::TempDir,
}

impl RunningServer {
    pub(crate) async fn start(arguments: &[&str]) -> Self {
        let directory = tempfile::tempdir().expect("the controls integration fixture succeeds");
        let config =
            ServerConfig::parse_from(std::iter::once("s3mock").chain(arguments.iter().copied()));
        let store = Store::open(&StoreConfig {
            root_directory: directory.path().to_path_buf(),
            durability: Durability::Relaxed,
            etag_mode: config.etag_mode.into(),
        })
        .expect("the controls integration fixture succeeds");
        store
            .create_bucket("testing", false)
            .expect("the controls integration fixture succeeds");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("the controls integration fixture succeeds");
        let server = s3mock_server::start_with_config(Arc::new(store), listener, config)
            .expect("the controls integration fixture succeeds");
        let client = client(&server, "s3mock", "s3mock");
        Self {
            server,
            client,
            directory,
        }
    }

    pub(crate) async fn raw(
        &self,
        method: &str,
        path: &str,
        headers: &str,
        body: &[u8],
    ) -> Vec<u8> {
        let mut stream = TcpStream::connect(self.server.local_address())
            .await
            .expect("the controls integration fixture succeeds");
        let head = format!(
            "{method} {path} HTTP/1.1\r\nhost: {}\r\nconnection: close\r\ncontent-length: {}\r\n{headers}\r\n",
            self.server.local_address(),
            body.len()
        );
        stream
            .write_all(head.as_bytes())
            .await
            .expect("the controls integration fixture succeeds");
        stream
            .write_all(body)
            .await
            .expect("the controls integration fixture succeeds");
        let mut response = Vec::new();
        tokio::time::timeout(Duration::from_secs(10), stream.read_to_end(&mut response))
            .await
            .expect("the controls integration fixture succeeds")
            .expect("the controls integration fixture succeeds");
        response
    }

    pub(crate) async fn admin(&self, method: &str, path: &str, body: Value) -> (u16, Value) {
        let bytes = if body.is_null() {
            Vec::new()
        } else {
            serde_json::to_vec(&body).expect("the controls integration fixture succeeds")
        };
        let response = self
            .raw(
                method,
                &format!("/_s3mock/{path}"),
                "content-type: application/json\r\n",
                &bytes,
            )
            .await;
        let boundary = response
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
            .expect("the controls integration fixture succeeds");
        let status = std::str::from_utf8(&response[9..12])
            .expect("the controls integration fixture succeeds")
            .parse()
            .expect("the controls integration fixture succeeds");
        (
            status,
            serde_json::from_slice(&response[boundary + 4..])
                .expect("the controls integration fixture succeeds"),
        )
    }

    pub(crate) async fn rules(&self, rules: Value) {
        let (status, _) = self.admin("PUT", "faults", json!({"rules": rules})).await;
        assert_eq!(status, 200);
    }

    pub(crate) async fn put(&self, key: &str, body: &'static str) {
        self.client
            .put_object()
            .bucket("testing")
            .key(key)
            .body(body.as_bytes().to_vec().into())
            .send()
            .await
            .expect("the controls integration fixture succeeds");
    }

    pub(crate) async fn get(&self, key: &str) -> Vec<u8> {
        self.client
            .get_object()
            .bucket("testing")
            .key(key)
            .send()
            .await
            .expect("the controls integration fixture succeeds")
            .body
            .collect()
            .await
            .expect("the controls integration fixture succeeds")
            .into_bytes()
            .to_vec()
    }

    pub(crate) async fn requests(&self, count: usize) -> Value {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let (_, log) = self.admin("GET", "requests", json!(null)).await;
            if log["requests"]
                .as_array()
                .expect("the controls integration fixture succeeds")
                .len()
                >= count
            {
                return log;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "request observations did not settle"
            );
            tokio::task::yield_now().await;
        }
    }
}

pub(crate) fn client(server: &ServerHandle, access_key: &str, secret: &str) -> Client {
    Client::from_conf(
        aws_sdk_s3::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new("us-east-1"))
            .endpoint_url(format!("http://{}", server.local_address()))
            .force_path_style(true)
            .retry_config(RetryConfig::disabled())
            .request_checksum_calculation(RequestChecksumCalculation::WhenRequired)
            .response_checksum_validation(ResponseChecksumValidation::WhenRequired)
            .credentials_provider(Credentials::new(access_key, secret, None, None, "testing"))
            .build(),
    )
}
