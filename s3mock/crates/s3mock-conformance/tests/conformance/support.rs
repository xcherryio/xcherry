// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Harness for the S3 conformance suite: one target and its cleanup per test.

use std::sync::Arc;

use aws_sdk_s3::Client;
use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
use s3mock::{Durability, Store, StoreConfig};
use s3mock_server::ServerHandle;
use tempfile::TempDir;

const TARGET_ENVIRONMENT_VARIABLE: &str = "S3MOCK_CONFORMANCE_TARGET";
const REGION: &str = "us-east-1";

/// The endpoint one test talks to, with the resources it must release.
pub(crate) struct ConformanceTarget {
    pub(crate) client: Client,
    /// Present only for s3mock targets.
    s3mock: Option<S3mockInstance>,
    created_buckets: Vec<String>,
}

struct S3mockInstance {
    server: ServerHandle,
    store: Arc<Store>,
    data_directory: TempDir,
}

impl ConformanceTarget {
    /// Connects to the target selected by `S3MOCK_CONFORMANCE_TARGET` (`s3mock` by default).
    pub(crate) async fn connect() -> ConformanceTarget {
        match std::env::var(TARGET_ENVIRONMENT_VARIABLE).as_deref() {
            Ok("aws") => {
                let config = aws_config::defaults(BehaviorVersion::latest()).load().await;
                ConformanceTarget {
                    client: Client::from_conf(
                        aws_sdk_s3::Config::from(&config)
                            .to_builder()
                            .region(Region::new(REGION))
                            .force_path_style(true)
                            .build(),
                    ),
                    s3mock: None,
                    created_buckets: Vec::new(),
                }
            }
            Ok("s3mock") | Err(std::env::VarError::NotPresent) => {
                let data_directory =
                    tempfile::tempdir().expect("the temporary directory is writable");
                let (server, store) = start_s3mock(&data_directory).await;
                let client = s3mock_client(&server);
                ConformanceTarget {
                    client,
                    s3mock: Some(S3mockInstance {
                        server,
                        store,
                        data_directory,
                    }),
                    created_buckets: Vec::new(),
                }
            }
            Ok(other) => {
                panic!("{TARGET_ENVIRONMENT_VARIABLE} must be s3mock or aws, not {other:?}")
            }
            Err(error) => panic!("{TARGET_ENVIRONMENT_VARIABLE} is unreadable: {error}"),
        }
    }

    /// Returns whether the target is s3mock, for tests of s3mock-only behavior such as restarts.
    pub(crate) fn is_s3mock(&self) -> bool {
        self.s3mock.is_some()
    }

    /// Creates a bucket with a unique name that [`ConformanceTarget::cleanup`] deletes.
    pub(crate) async fn create_bucket(&mut self) -> String {
        self.create_bucket_with_object_lock(false).await
    }

    pub(crate) async fn create_bucket_with_object_lock(
        &mut self,
        is_object_lock_enabled: bool,
    ) -> String {
        let bucket = format!(
            "s3mock-conformance-{}",
            ulid::Ulid::new().to_string().to_ascii_lowercase()
        );
        self.client
            .create_bucket()
            .bucket(&bucket)
            .set_object_lock_enabled_for_bucket(is_object_lock_enabled.then_some(true))
            .send()
            .await
            .expect("creating a uniquely named bucket succeeds");
        self.created_buckets.push(bucket.clone());
        bucket
    }

    /// Restarts the s3mock server and store on the same data directory, as a process restart would.
    pub(crate) async fn restart_s3mock(&mut self) {
        let instance = self
            .s3mock
            .take()
            .expect("restart_s3mock requires an s3mock target");
        instance.server.shutdown().await;
        let store =
            Arc::into_inner(instance.store).expect("the server released its store references");
        tokio::task::spawn_blocking(move || drop(store))
            .await
            .expect("closing the store does not panic");
        let (server, store) = start_s3mock(&instance.data_directory).await;
        self.client = s3mock_client(&server);
        self.s3mock = Some(S3mockInstance {
            server,
            store,
            data_directory: instance.data_directory,
        });
    }

    /// Deletes every version in the buckets this test created, then the buckets, then stops s3mock.
    pub(crate) async fn cleanup(mut self) {
        for bucket in std::mem::take(&mut self.created_buckets) {
            empty_bucket(&self.client, &bucket).await;
            self.client
                .delete_bucket()
                .bucket(&bucket)
                .send()
                .await
                .expect("deleting an emptied bucket succeeds");
        }
        if let Some(instance) = self.s3mock.take() {
            instance.server.shutdown().await;
        }
    }
}

async fn start_s3mock(data_directory: &TempDir) -> (ServerHandle, Arc<Store>) {
    let config = StoreConfig {
        root_directory: data_directory.path().to_path_buf(),
        durability: Durability::Strict,
        etag_mode: s3mock::EtagMode::Md5,
    };
    let store = Arc::new(
        tokio::task::spawn_blocking(move || Store::open(&config))
            .await
            .expect("opening the store does not panic")
            .expect("the store opens in a fresh directory"),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("binding a free loopback port succeeds");
    let server = s3mock_server::start(Arc::clone(&store), listener)
        .expect("the listener has a local address");
    (server, store)
}

fn s3mock_client(server: &ServerHandle) -> Client {
    let config = aws_sdk_s3::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new(REGION))
        .endpoint_url(format!("http://{}", server.local_address()))
        .force_path_style(true)
        .credentials_provider(Credentials::new(
            "s3mock",
            "s3mock",
            None,
            None,
            "s3mock-conformance",
        ))
        .build();
    Client::from_conf(config)
}

async fn empty_bucket(client: &Client, bucket: &str) {
    loop {
        let page = client
            .list_object_versions()
            .bucket(bucket)
            .send()
            .await
            .expect("listing versions of a test bucket succeeds");
        let versions = page
            .versions()
            .iter()
            .map(|version| (version.key(), version.version_id()))
            .chain(
                page.delete_markers()
                    .iter()
                    .map(|marker| (marker.key(), marker.version_id())),
            );
        let mut is_empty = true;
        for (key, version_id) in versions {
            is_empty = false;
            client
                .delete_object()
                .bucket(bucket)
                .key(key.expect("listed versions have keys"))
                .set_version_id(version_id.map(str::to_owned))
                .send()
                .await
                .expect("deleting a listed version succeeds");
        }
        if is_empty {
            return;
        }
    }
}

/// Returns the S3 error code and HTTP status of a failed request.
pub(crate) fn error_code_and_status<E, R>(
    error: &aws_sdk_s3::error::SdkError<E, R>,
) -> (Option<String>, Option<u16>)
where
    E: aws_sdk_s3::error::ProvideErrorMetadata,
    R: HttpStatus,
{
    let code = error
        .as_service_error()
        .and_then(|service| service.code())
        .map(str::to_owned);
    let status = error.raw_response().map(HttpStatus::status_code);
    (code, status)
}

/// Exposes the status code of the raw response type the SDK attaches to errors.
pub(crate) trait HttpStatus {
    fn status_code(&self) -> u16;
}

impl HttpStatus for aws_sdk_s3::config::http::HttpResponse {
    fn status_code(&self) -> u16 {
        self.status().as_u16()
    }
}

/// Reads a whole object body.
pub(crate) async fn read_body(
    output: aws_sdk_s3::operation::get_object::GetObjectOutput,
) -> Vec<u8> {
    output
        .body
        .collect()
        .await
        .expect("the object body streams completely")
        .into_bytes()
        .to_vec()
}
