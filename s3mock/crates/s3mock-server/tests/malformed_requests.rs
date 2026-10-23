// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Raw HTTP requests that SDKs never send: the server must reject them without leaking resources.

use std::sync::Arc;

use s3mock::{Durability, Store, StoreConfig};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

struct RunningServer {
    server: s3mock_server::ServerHandle,
    directory: tempfile::TempDir,
}

async fn start_with_bucket() -> RunningServer {
    let directory = tempfile::tempdir().expect("the raw HTTP test harness I/O succeeds");
    let store = Store::open(&StoreConfig {
        root_directory: directory.path().to_path_buf(),
        durability: Durability::Relaxed,
    })
    .expect("the raw HTTP test harness I/O succeeds");
    store
        .create_bucket("raw", false)
        .expect("the raw HTTP test harness I/O succeeds");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("the raw HTTP test harness I/O succeeds");
    let server = s3mock_server::start(Arc::new(store), listener)
        .expect("the raw HTTP test harness I/O succeeds");
    RunningServer { server, directory }
}

/// Sends one aws-chunked PutObject and returns the response status code.
async fn put_aws_chunked(server: &RunningServer, extra_headers: &str, body: &[u8]) -> u16 {
    let mut stream = TcpStream::connect(server.server.local_address())
        .await
        .expect("the raw HTTP test harness I/O succeeds");
    let head = format!(
        "PUT /raw/object HTTP/1.1\r\nhost: localhost\r\ncontent-encoding: aws-chunked\r\n         x-amz-content-sha256: STREAMING-UNSIGNED-PAYLOAD-TRAILER\r\n{extra_headers}         content-length: {}\r\nconnection: close\r\n\r\n",
        body.len()
    );
    stream
        .write_all(head.as_bytes())
        .await
        .expect("the raw HTTP test harness I/O succeeds");
    stream
        .write_all(body)
        .await
        .expect("the raw HTTP test harness I/O succeeds");
    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .await
        .expect("the raw HTTP test harness I/O succeeds");
    let status_line = String::from_utf8_lossy(&response[..12]).into_owned();
    status_line[9..12]
        .parse()
        .expect("the raw HTTP test harness I/O succeeds")
}

fn temporary_file_count(server: &RunningServer) -> usize {
    std::fs::read_dir(server.directory.path().join("tmp"))
        .expect("the raw HTTP test harness I/O succeeds")
        .count()
}

#[tokio::test]
async fn excessive_trailers_are_rejected() {
    let server = start_with_bucket().await;
    let mut body = b"0\r\n".to_vec();
    for _ in 0..100 {
        body.extend_from_slice(b"foo:bar\r\n");
    }
    body.extend_from_slice(b"\r\n");
    assert_eq!(put_aws_chunked(&server, "", &body).await, 400);
    assert_eq!(temporary_file_count(&server), 0);
    server.server.shutdown().await;
}

#[tokio::test]
async fn a_missing_declared_trailer_rejects_the_upload_and_removes_its_file() {
    let server = start_with_bucket().await;
    let status = put_aws_chunked(
        &server,
        "x-amz-trailer: x-amz-checksum-crc32\r\n",
        b"1\r\nx\r\n0\r\n\r\n",
    )
    .await;
    assert_eq!(status, 400);
    assert_eq!(temporary_file_count(&server), 0);
    server.server.shutdown().await;
}

#[tokio::test]
async fn an_oversized_chunk_header_is_rejected() {
    let server = start_with_bucket().await;
    let mut body = format!("1;chunk-signature={}\r\n", "a".repeat(5000)).into_bytes();
    body.extend_from_slice(b"x\r\n0\r\n\r\n");
    assert_eq!(put_aws_chunked(&server, "", &body).await, 400);
    server.server.shutdown().await;
}
