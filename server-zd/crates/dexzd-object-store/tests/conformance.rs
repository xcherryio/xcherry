// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::sync::Arc;

use bytes::Bytes;
use dexzd_object_store::{
    ByteRange, InMemoryObjectStore, ObjectStore, S3Credentials, S3ObjectStore, S3ObjectStoreConfig,
    WriteCondition, WriteOutcome, startup_self_check,
};
use s3mock::{Durability, EtagMode, Store, StoreConfig};
use tempfile::TempDir;

async fn check_store(store: &dyn ObjectStore) {
    assert!(
        store
            .get("missing")
            .await
            .expect("the conformance operation succeeds")
            .is_none()
    );
    assert!(
        store
            .head("missing")
            .await
            .expect("the conformance operation succeeds")
            .is_none()
    );
    assert!(
        store
            .get_range(
                "missing",
                ByteRange {
                    start: 0,
                    end_inclusive: 2
                }
            )
            .await
            .expect("the conformance operation succeeds")
            .is_none()
    );
    let initial = store
        .put(
            "objects/a",
            Bytes::from_static(b"abcdef"),
            WriteCondition::CreateOnly,
        )
        .await
        .expect("the conformance operation succeeds");
    let WriteOutcome::Written { etag } = initial else {
        panic!("initial create must succeed")
    };
    assert_eq!(
        store
            .put(
                "objects/a",
                Bytes::from_static(b"wrong"),
                WriteCondition::CreateOnly
            )
            .await
            .expect("the conformance operation succeeds"),
        WriteOutcome::PreconditionFailed
    );
    assert_eq!(
        store
            .put(
                "objects/a",
                Bytes::from_static(b"wrong"),
                WriteCondition::ReplaceIfEtagMatches {
                    etag: "wrong".to_owned()
                }
            )
            .await
            .expect("the conformance operation succeeds"),
        WriteOutcome::PreconditionFailed
    );
    assert_eq!(
        store
            .get("objects/a")
            .await
            .expect("the conformance operation succeeds")
            .expect("the conformance operation succeeds")
            .bytes,
        Bytes::from_static(b"abcdef")
    );
    let range = store
        .get_range(
            "objects/a",
            ByteRange {
                start: 2,
                end_inclusive: 100,
            },
        )
        .await
        .expect("the conformance operation succeeds")
        .expect("the conformance operation succeeds");
    assert_eq!(range.bytes, Bytes::from_static(b"cdef"));
    assert_eq!(range.object_size, 6);
    assert_eq!(range.etag, etag);
    let beyond = store
        .get_range(
            "objects/a",
            ByteRange {
                start: 100,
                end_inclusive: 200,
            },
        )
        .await
        .expect("the conformance operation succeeds")
        .expect("the conformance operation succeeds");
    assert!(beyond.bytes.is_empty());
    assert_eq!(beyond.object_size, 6);
    let next = store
        .put(
            "objects/a",
            Bytes::from_static(b"next"),
            WriteCondition::ReplaceIfEtagMatches { etag },
        )
        .await
        .expect("the conformance operation succeeds");
    assert!(matches!(next, WriteOutcome::Written { .. }));
    assert_eq!(
        store
            .head("objects/a")
            .await
            .expect("the conformance operation succeeds")
            .expect("the conformance operation succeeds")
            .size,
        4
    );
    store
        .put(
            "objects/b",
            Bytes::from_static(b"b"),
            WriteCondition::Unconditional,
        )
        .await
        .expect("the conformance operation succeeds");
    store
        .put(
            "objects/c",
            Bytes::from_static(b"c"),
            WriteCondition::Unconditional,
        )
        .await
        .expect("the conformance operation succeeds");
    let first = store
        .list("objects/", None, 2)
        .await
        .expect("the conformance operation succeeds");
    assert_eq!(
        first
            .objects
            .iter()
            .map(|object| object.key.as_str())
            .collect::<Vec<_>>(),
        ["objects/a", "objects/b"]
    );
    let second = store
        .list("objects/", first.next_start_after.as_deref(), 2)
        .await
        .expect("the conformance operation succeeds");
    assert_eq!(
        second
            .objects
            .iter()
            .map(|object| object.key.as_str())
            .collect::<Vec<_>>(),
        ["objects/c"]
    );
    assert!(second.next_start_after.is_none());
    let directory = tempfile::tempdir().expect("the conformance operation succeeds");
    let input = directory.path().join("input");
    let output = directory.path().join("output");
    tokio::fs::write(&input, b"from-file")
        .await
        .expect("the conformance operation succeeds");
    store
        .put_file("file", &input, WriteCondition::CreateOnly)
        .await
        .expect("the conformance operation succeeds");
    assert_eq!(
        store
            .get_to_file("file", &output)
            .await
            .expect("the conformance operation succeeds"),
        Some(9)
    );
    assert_eq!(
        tokio::fs::read(&output)
            .await
            .expect("the conformance operation succeeds"),
        b"from-file"
    );
    let absent_path = directory.path().join("absent");
    assert_eq!(
        store
            .get_to_file("absent", &absent_path)
            .await
            .expect("the conformance operation succeeds"),
        None
    );
    assert!(!absent_path.exists());
    store
        .delete_many(&[
            "objects/a".to_owned(),
            "objects/b".to_owned(),
            "absent".to_owned(),
        ])
        .await
        .expect("the conformance operation succeeds");
    assert!(
        store
            .get("objects/a")
            .await
            .expect("the conformance operation succeeds")
            .is_none()
    );
    store
        .delete("objects/c")
        .await
        .expect("the conformance operation succeeds");
    store
        .delete("objects/c")
        .await
        .expect("the conformance operation succeeds");
    assert_eq!(
        store
            .delete_many(&vec!["unused".to_owned(); 1001])
            .await
            .expect_err("the conformance operation fails")
            .kind,
        dexzd_object_store::ObjectStoreErrorKind::Permanent
    );
    let large = Bytes::from(vec![42; 6 * 1024 * 1024]);
    let created = store
        .put("large", large.clone(), WriteCondition::CreateOnly)
        .await
        .expect("the conformance operation succeeds");
    let WriteOutcome::Written { etag } = created else {
        panic!("multipart create must succeed")
    };
    assert_eq!(
        store
            .put("large", large.clone(), WriteCondition::CreateOnly)
            .await
            .expect("the conformance operation succeeds"),
        WriteOutcome::PreconditionFailed
    );
    assert_eq!(
        store
            .put(
                "large",
                large.clone(),
                WriteCondition::ReplaceIfEtagMatches {
                    etag: "wrong".to_owned()
                }
            )
            .await
            .expect("the conformance operation succeeds"),
        WriteOutcome::PreconditionFailed
    );
    assert!(matches!(
        store
            .put(
                "large",
                large,
                WriteCondition::ReplaceIfEtagMatches { etag }
            )
            .await
            .expect("the conformance operation succeeds"),
        WriteOutcome::Written { .. }
    ));
    let large_file = directory.path().join("large-file");
    tokio::fs::write(&large_file, vec![7; 6 * 1024 * 1024])
        .await
        .expect("the conformance operation succeeds");
    assert!(matches!(
        store
            .put_file("large-file", &large_file, WriteCondition::CreateOnly)
            .await
            .expect("the conformance operation succeeds"),
        WriteOutcome::Written { .. }
    ));
    assert_eq!(
        store
            .head("large-file")
            .await
            .expect("the conformance operation succeeds")
            .expect("the conformance operation succeeds")
            .size,
        6 * 1024 * 1024
    );
    startup_self_check(store)
        .await
        .expect("the conformance operation succeeds");
}

#[tokio::test]
async fn in_memory_conformance() {
    check_store(&InMemoryObjectStore::new()).await;
}

async fn s3_conformance(durability: Durability) {
    let directory = TempDir::new().expect("the conformance operation succeeds");
    let store = Store::open(&StoreConfig {
        root_directory: directory.path().to_path_buf(),
        durability,
        etag_mode: EtagMode::Opaque,
    })
    .expect("the conformance operation succeeds");
    store
        .create_bucket("object-store-conformance", false)
        .expect("the conformance operation succeeds");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("the conformance operation succeeds");
    let server = s3mock_server::start(Arc::new(store), listener)
        .expect("the conformance operation succeeds");
    let mut config = S3ObjectStoreConfig::new("us-east-1", "object-store-conformance");
    config.endpoint = Some(format!("http://{}", server.local_address()));
    config.key_prefix = "dexzd/conformance".to_owned();
    config.force_path_style = true;
    config.credentials = S3Credentials::Static {
        access_key_id: "s3mock".to_owned(),
        secret_access_key: "s3mock".to_owned(),
    };
    config.multipart_threshold = 5 * 1024 * 1024;
    config.multipart_part_size = 5 * 1024 * 1024;
    let object_store = S3ObjectStore::new(config)
        .await
        .expect("the conformance operation succeeds");
    check_store(&object_store).await;
    server.shutdown().await;
}

#[tokio::test]
async fn strict_s3_conformance() {
    s3_conformance(Durability::Strict).await;
}

#[tokio::test]
async fn relaxed_s3_conformance() {
    s3_conformance(Durability::Relaxed).await;
}

#[derive(Default)]
struct ScriptedFaults {
    actions: std::sync::Mutex<std::collections::VecDeque<dexzd_object_store::FaultAction>>,
}

impl ScriptedFaults {
    fn push(&self, action: dexzd_object_store::FaultAction) {
        self.actions
            .lock()
            .expect("the conformance operation succeeds")
            .push_back(action);
    }
}

impl dexzd_object_store::FaultInjector for ScriptedFaults {
    fn action(
        &self,
        _operation: dexzd_object_store::ObjectStoreOperation,
        _key: &str,
    ) -> dexzd_object_store::FaultAction {
        self.actions
            .lock()
            .expect("the conformance operation succeeds")
            .pop_front()
            .unwrap_or(dexzd_object_store::FaultAction::Proceed)
    }
}

#[tokio::test]
async fn in_memory_faults_and_counters() {
    use dexzd_object_store::{FaultAction, ObjectStoreErrorKind, ObjectStoreOperation};
    let faults = Arc::new(ScriptedFaults::default());
    let store = InMemoryObjectStore::with_injector(faults.clone());
    faults.push(FaultAction::FailBeforeEffect);
    assert_eq!(
        store
            .put(
                "cost/a",
                Bytes::from_static(b"one"),
                WriteCondition::CreateOnly
            )
            .await
            .expect_err("the conformance operation fails")
            .kind,
        ObjectStoreErrorKind::Transient
    );
    assert!(
        store
            .head("cost/a")
            .await
            .expect("the conformance operation succeeds")
            .is_none()
    );
    faults.push(FaultAction::ApplyThenOutcomeUnknown);
    assert_eq!(
        store
            .put(
                "cost/a",
                Bytes::from_static(b"one"),
                WriteCondition::CreateOnly
            )
            .await
            .expect_err("the conformance operation fails")
            .kind,
        ObjectStoreErrorKind::OutcomeUnknown
    );
    assert_eq!(
        store
            .get("cost/a")
            .await
            .expect("the conformance operation succeeds")
            .expect("the conformance operation succeeds")
            .bytes,
        Bytes::from_static(b"one")
    );
    faults.push(FaultAction::CorruptOnRead);
    assert_ne!(
        store
            .get("cost/a")
            .await
            .expect("the conformance operation succeeds")
            .expect("the conformance operation succeeds")
            .bytes,
        Bytes::from_static(b"one")
    );
    assert_eq!(
        store
            .get("cost/a")
            .await
            .expect("the conformance operation succeeds")
            .expect("the conformance operation succeeds")
            .bytes,
        Bytes::from_static(b"one")
    );
    faults.push(FaultAction::Delay(std::time::Duration::from_millis(1)));
    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(1), store.head("cost/a"))
            .await
            .expect("the conformance operation succeeds")
            .expect("the conformance operation succeeds")
            .is_some()
    );
    assert_eq!(store.call_count(ObjectStoreOperation::Put, "cost/"), 2);
    assert_eq!(store.call_count(ObjectStoreOperation::Put, "other/"), 0);
    assert_eq!(store.call_count(ObjectStoreOperation::Get, "cost/"), 3);
}

#[tokio::test]
async fn startup_check_names_failed_condition() {
    use dexzd_object_store::{FaultAction, StartupSelfCheckFailure};
    let faults = Arc::new(ScriptedFaults::default());
    faults.push(FaultAction::Proceed);
    faults.push(FaultAction::FailBeforeEffect);
    let store = InMemoryObjectStore::with_injector(faults);
    let error = startup_self_check(&store)
        .await
        .expect_err("the conformance operation fails");
    assert_eq!(error.check, StartupSelfCheckFailure::DuplicateCreate);
}

#[tokio::test]
async fn s3_missing_bucket_and_access_denied_are_permanent() {
    use dexzd_object_store::ObjectStoreErrorKind;
    use s3mock_server::{ServerConfig, SignatureVerification};

    let directory = TempDir::new().expect("the conformance operation succeeds");
    let store = Store::open(&StoreConfig {
        root_directory: directory.path().to_path_buf(),
        durability: Durability::Relaxed,
        etag_mode: EtagMode::Opaque,
    })
    .expect("the conformance operation succeeds");
    store
        .create_bucket("present-bucket", false)
        .expect("the conformance operation succeeds");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("the conformance operation succeeds");
    let server = s3mock_server::start_with_config(
        Arc::new(store),
        listener,
        ServerConfig {
            signature_verification: SignatureVerification::Strict,
            ..ServerConfig::default()
        },
    )
    .expect("the conformance operation succeeds");
    let endpoint = format!("http://{}", server.local_address());
    let mut missing = S3ObjectStoreConfig::new("us-east-1", "missing-bucket");
    missing.endpoint = Some(endpoint.clone());
    missing.force_path_style = true;
    missing.credentials = S3Credentials::Static {
        access_key_id: "s3mock".to_owned(),
        secret_access_key: "s3mock".to_owned(),
    };
    let missing = S3ObjectStore::new(missing)
        .await
        .expect("the conformance operation succeeds");
    assert_eq!(
        missing
            .get("object")
            .await
            .expect_err("the conformance operation fails")
            .kind,
        ObjectStoreErrorKind::Permanent
    );
    assert_eq!(
        missing
            .head("object")
            .await
            .expect_err("the conformance operation fails")
            .kind,
        ObjectStoreErrorKind::Permanent
    );
    assert_eq!(
        missing
            .put(
                "object",
                Bytes::from_static(b"body"),
                WriteCondition::CreateOnly
            )
            .await
            .expect_err("the conformance operation fails")
            .kind,
        ObjectStoreErrorKind::Permanent
    );
    assert_eq!(
        missing
            .list("", None, 10)
            .await
            .expect_err("the conformance operation fails")
            .kind,
        ObjectStoreErrorKind::Permanent
    );

    let mut forbidden = S3ObjectStoreConfig::new("us-east-1", "present-bucket");
    forbidden.endpoint = Some(endpoint);
    forbidden.force_path_style = true;
    forbidden.credentials = S3Credentials::Static {
        access_key_id: "wrong".to_owned(),
        secret_access_key: "wrong".to_owned(),
    };
    let forbidden = S3ObjectStore::new(forbidden)
        .await
        .expect("the conformance operation succeeds");
    assert_eq!(
        forbidden
            .get("object")
            .await
            .expect_err("the conformance operation fails")
            .kind,
        ObjectStoreErrorKind::Permanent
    );
    assert_eq!(
        forbidden
            .put(
                "object",
                Bytes::from_static(b"body"),
                WriteCondition::CreateOnly
            )
            .await
            .expect_err("the conformance operation fails")
            .kind,
        ObjectStoreErrorKind::Permanent
    );
    server.shutdown().await;
}

async fn set_s3mock_fault(address: std::net::SocketAddr, rule: &str) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let body = format!("{{\"rules\":[{rule}]}}");
    let mut stream = tokio::net::TcpStream::connect(address)
        .await
        .expect("the conformance operation succeeds");
    let request = format!(
        "PUT /_s3mock/faults HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    );
    stream
        .write_all(request.as_bytes())
        .await
        .expect("the conformance operation succeeds");
    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .await
        .expect("the conformance operation succeeds");
    assert!(
        response.starts_with(b"HTTP/1.1 200"),
        "{}",
        String::from_utf8_lossy(&response)
    );
}

#[tokio::test]
async fn s3_conflicts_and_ambiguous_writes() {
    use dexzd_object_store::ObjectStoreErrorKind;
    let directory = TempDir::new().expect("the conformance operation succeeds");
    let store = Store::open(&StoreConfig {
        root_directory: directory.path().to_path_buf(),
        durability: Durability::Relaxed,
        etag_mode: EtagMode::Opaque,
    })
    .expect("the conformance operation succeeds");
    store
        .create_bucket("fault-bucket", false)
        .expect("the conformance operation succeeds");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("the conformance operation succeeds");
    let server = s3mock_server::start(Arc::new(store), listener)
        .expect("the conformance operation succeeds");
    let mut config = S3ObjectStoreConfig::new("us-east-1", "fault-bucket");
    config.endpoint = Some(format!("http://{}", server.local_address()));
    config.force_path_style = true;
    config.credentials = S3Credentials::Static {
        access_key_id: "s3mock".to_owned(),
        secret_access_key: "s3mock".to_owned(),
    };
    let object_store = S3ObjectStore::new(config)
        .await
        .expect("the conformance operation succeeds");

    set_s3mock_fault(server.local_address(), r#"{"operation":"PutObject","bucket":"fault-bucket","key_prefix":"conflict/","has_conditional_header":true,"action":{"type":"error","status":409}}"#).await;
    assert_eq!(
        object_store
            .put(
                "conflict/key",
                Bytes::from_static(b"body"),
                WriteCondition::CreateOnly
            )
            .await
            .expect("the conformance operation succeeds"),
        WriteOutcome::PreconditionFailed
    );
    set_s3mock_fault(server.local_address(), r#"{"operation":"PutObject","bucket":"fault-bucket","key_prefix":"failed/","action":{"type":"error","status":500}}"#).await;
    assert_eq!(
        object_store
            .put(
                "failed/key",
                Bytes::from_static(b"body"),
                WriteCondition::CreateOnly
            )
            .await
            .expect_err("the conformance operation fails")
            .kind,
        ObjectStoreErrorKind::OutcomeUnknown
    );
    set_s3mock_fault(server.local_address(), r#"{"operation":"PutObject","bucket":"fault-bucket","key_prefix":"committed/","action":{"type":"commit_then_drop"}}"#).await;
    let committed_error = object_store
        .put(
            "committed/key",
            Bytes::from_static(b"body"),
            WriteCondition::CreateOnly,
        )
        .await
        .expect_err("the conformance operation fails");
    assert_eq!(
        committed_error.kind,
        ObjectStoreErrorKind::OutcomeUnknown,
        "{committed_error}"
    );
    assert_eq!(
        object_store
            .get("committed/key")
            .await
            .expect("the conformance operation succeeds")
            .expect("the conformance operation succeeds")
            .bytes,
        Bytes::from_static(b"body")
    );
    set_s3mock_fault(server.local_address(), r#"{"operation":"GetObject","bucket":"fault-bucket","key_prefix":"committed/","action":{"type":"error","status":500}}"#).await;
    assert_eq!(
        object_store
            .get("committed/key")
            .await
            .expect_err("the conformance operation fails")
            .kind,
        ObjectStoreErrorKind::Transient
    );
    server.shutdown().await;
}

#[tokio::test]
async fn s3_connection_refused_is_transient() {
    use dexzd_object_store::ObjectStoreErrorKind;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("the conformance operation succeeds");
    let address = listener
        .local_addr()
        .expect("the conformance operation succeeds");
    drop(listener);
    let mut config = S3ObjectStoreConfig::new("us-east-1", "unreachable-bucket");
    config.endpoint = Some(format!("http://{address}"));
    config.force_path_style = true;
    config.request_timeout = std::time::Duration::from_secs(2);
    config.credentials = S3Credentials::Static {
        access_key_id: "s3mock".to_owned(),
        secret_access_key: "s3mock".to_owned(),
    };
    let store = S3ObjectStore::new(config)
        .await
        .expect("the conformance operation succeeds");
    assert_eq!(
        store
            .get("key")
            .await
            .expect_err("the conformance operation fails")
            .kind,
        ObjectStoreErrorKind::Transient
    );
    assert_eq!(
        store
            .put(
                "key",
                Bytes::from_static(b"body"),
                WriteCondition::CreateOnly
            )
            .await
            .expect_err("the conformance operation fails")
            .kind,
        ObjectStoreErrorKind::Transient
    );
}
