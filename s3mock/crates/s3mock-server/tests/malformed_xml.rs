// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Raw XML request bodies that SDKs never send: the server must parse them exactly or reject them.

use std::sync::Arc;

use s3mock::{
    ChecksumAlgorithm, Durability, ObjectReadRequest, PutObjectRequest, Store, StoreConfig,
    UploadChecksumExpectations, md5_base64,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

struct RunningServer {
    server: s3mock_server::ServerHandle,
    store: Arc<Store>,
    _directory: tempfile::TempDir,
}

async fn start_with_objects(keys: &[&str]) -> RunningServer {
    let directory = tempfile::tempdir().expect("the temporary directory is writable");
    let store = Arc::new(
        Store::open(&StoreConfig {
            root_directory: directory.path().to_path_buf(),
            durability: Durability::Relaxed,
        })
        .expect("the store opens in a fresh directory"),
    );
    store
        .create_bucket("raw", false)
        .expect("the bucket name is valid");
    for key in keys {
        let mut writer = store
            .begin_put("raw", key, ChecksumAlgorithm::Crc32)
            .expect("the bucket exists");
        writer
            .write_all(b"x")
            .expect("the temporary file is writable");
        let staged = writer
            .finish(&UploadChecksumExpectations::default())
            .expect("no digests are declared");
        store
            .commit_put("raw", key, staged, PutObjectRequest::default())
            .expect("the write commits");
    }
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("a loopback port is free");
    let server =
        s3mock_server::start(Arc::clone(&store), listener).expect("the listener has an address");
    RunningServer {
        server,
        store,
        _directory: directory,
    }
}

/// Sends a request with a Content-MD5 for `body` and returns the status code and response body.
async fn send_with_md5(server: &RunningServer, method_and_path: &str, body: &str) -> (u16, String) {
    let mut stream = TcpStream::connect(server.server.local_address())
        .await
        .expect("the test server accepts connections");
    let request = format!(
        "{method_and_path} HTTP/1.1\r\nhost: localhost\r\ncontent-md5: {}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        md5_base64(body.as_bytes()),
        body.len()
    );
    stream
        .write_all(request.as_bytes())
        .await
        .expect("the test server reads the request");
    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .await
        .expect("the server sends a complete response");
    let response = String::from_utf8_lossy(&response).into_owned();
    let status = response[9..12]
        .parse()
        .expect("the response starts with a status line");
    (status, response)
}

fn exists(server: &RunningServer, key: &str) -> bool {
    server
        .store
        .head_object("raw", key, &ObjectReadRequest::default())
        .is_ok()
}

#[tokio::test]
async fn a_comment_inside_a_key_does_not_change_which_key_is_deleted() {
    let server = start_with_objects(&["foobar", "bar"]).await;
    let (status, _) = send_with_md5(
        &server,
        "POST /raw?delete",
        "<Delete><Object><Key>foo<!--comment-->bar</Key></Object></Delete>",
    )
    .await;
    assert_eq!(status, 200);
    assert!(!exists(&server, "foobar"));
    assert!(exists(&server, "bar"));
    server.server.shutdown().await;
}

#[tokio::test]
async fn cdata_keys_are_honored() {
    let server = start_with_objects(&["a&b"]).await;
    let (status, _) = send_with_md5(
        &server,
        "POST /raw?delete",
        "<Delete><Object><Key><![CDATA[a&b]]></Key></Object></Delete>",
    )
    .await;
    assert_eq!(status, 200);
    assert!(!exists(&server, "a&b"));
    server.server.shutdown().await;
}

#[tokio::test]
async fn truncated_documents_change_nothing() {
    let server = start_with_objects(&["keep"]).await;
    let (status, response) = send_with_md5(
        &server,
        "POST /raw?delete",
        "<Delete><Object><Key>keep</Key></Object>",
    )
    .await;
    assert_eq!(status, 400);
    assert!(response.contains("MalformedXML"));
    assert!(exists(&server, "keep"));
    let (status, _) = send_with_md5(
        &server,
        "POST /raw?delete",
        "<Other><Object><Key>keep</Key></Object></Other>",
    )
    .await;
    assert_eq!(status, 400, "the root element must be Delete");
    assert!(exists(&server, "keep"));
    server.server.shutdown().await;
}

#[tokio::test]
async fn mixed_content_is_rejected() {
    let server = start_with_objects(&["foobar"]).await;
    let (status, _) = send_with_md5(
        &server,
        "POST /raw?delete",
        "<Delete><Object><Key>foo<Nested>x</Nested>bar</Key></Object></Delete>",
    )
    .await;
    assert_eq!(status, 400);
    assert!(exists(&server, "foobar"));
    server.server.shutdown().await;
}
