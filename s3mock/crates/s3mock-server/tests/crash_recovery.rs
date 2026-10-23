// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Crash consistency: SIGKILL the `s3mock` binary during concurrent writes, restart it on the same
//! directory, and check that every acknowledged object survives intact and nothing is half-written.

use std::path::Path;
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::process::{Child, Command};
use tokio::sync::Mutex;

const WRITERS: usize = 8;
const CRASH_ROUNDS: usize = 3;
const LOAD_BEFORE_KILL: Duration = Duration::from_millis(400);

struct RunningBinary {
    child: Child,
    address: String,
}

/// Starts the binary on a free port and waits for its "serving" log line.
async fn start_binary(data_directory: &Path) -> RunningBinary {
    let mut child = Command::new(env!("CARGO_BIN_EXE_s3mock"))
        .args(["--listen-address", "127.0.0.1:0", "--data-directory"])
        .arg(data_directory)
        .env("NO_COLOR", "1")
        .env("RUST_LOG", "info")
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .expect("the s3mock binary starts");
    let stdout = child.stdout.take().expect("stdout is piped");
    let mut lines = BufReader::new(stdout).lines();
    let address = loop {
        let line = lines
            .next_line()
            .await
            .expect("the binary log is readable")
            .expect("the binary logs its address before exiting");
        if let Some(start) = line.find("address=") {
            let rest = &line[start + "address=".len()..];
            let address: String = rest
                .chars()
                .skip_while(|character| !character.is_ascii_digit())
                .take_while(|character| {
                    character.is_ascii_digit() || *character == '.' || *character == ':'
                })
                .collect();
            break address;
        }
    };
    RunningBinary { child, address }
}

/// Sends one request and returns the status and body; `None` means the connection failed.
async fn request(address: &str, method: &str, path: &str, body: &[u8]) -> Option<(u16, Vec<u8>)> {
    let mut stream = TcpStream::connect(address).await.ok()?;
    let head = format!(
        "{method} {path} HTTP/1.1\r\nhost: localhost\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(head.as_bytes()).await.ok()?;
    stream.write_all(body).await.ok()?;
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.ok()?;
    let header_end = response
        .windows(4)
        .position(|window| window == b"\r\n\r\n")?;
    let status = std::str::from_utf8(response.get(9..12)?)
        .ok()?
        .parse()
        .ok()?;
    Some((status, response[header_end + 4..].to_vec()))
}

fn body_for(key: &str) -> Vec<u8> {
    let length = 1024 + key.len() * 997 % 60_000;
    key.as_bytes()
        .iter()
        .copied()
        .cycle()
        .take(length)
        .collect()
}

fn listed_keys(listing: &[u8]) -> Vec<String> {
    let text = String::from_utf8_lossy(listing);
    text.split("<Key>")
        .skip(1)
        .filter_map(|rest| rest.split("</Key>").next())
        .map(str::to_owned)
        .collect()
}

#[tokio::test]
async fn acknowledged_writes_survive_sigkill_and_nothing_is_half_written() {
    let directory = tempfile::tempdir().expect("the temporary directory is writable");
    let acknowledged = Arc::new(Mutex::new(Vec::<String>::new()));
    let mut binary = start_binary(directory.path()).await;
    let (status, _) = request(&binary.address, "PUT", "/crash", b"")
        .await
        .expect("the server answers");
    assert_eq!(status, 200);
    for round in 0..CRASH_ROUNDS {
        let is_running = Arc::new(AtomicBool::new(true));
        let mut writers = tokio::task::JoinSet::new();
        for writer in 0..WRITERS {
            let address = binary.address.clone();
            let acknowledged = Arc::clone(&acknowledged);
            let is_running = Arc::clone(&is_running);
            writers.spawn(async move {
                let mut sequence = 0;
                while is_running.load(Ordering::Relaxed) {
                    let key = format!("r{round}-w{writer}-{sequence}");
                    sequence += 1;
                    match request(&address, "PUT", &format!("/crash/{key}"), &body_for(&key)).await
                    {
                        Some((200, _)) => acknowledged.lock().await.push(key),
                        Some(_) | None => return,
                    }
                }
            });
        }
        tokio::time::sleep(LOAD_BEFORE_KILL).await;
        binary
            .child
            .kill()
            .await
            .expect("SIGKILL reaches the child");
        is_running.store(false, Ordering::Relaxed);
        while writers.join_next().await.is_some() {}

        binary = start_binary(directory.path()).await;
        let temporary_files = std::fs::read_dir(directory.path().join("tmp"))
            .expect("the store keeps a tmp directory")
            .count();
        assert_eq!(temporary_files, 0, "startup removes unfinished uploads");
        for key in acknowledged.lock().await.iter() {
            let (status, body) = request(&binary.address, "GET", &format!("/crash/{key}"), b"")
                .await
                .expect("the restarted server answers");
            assert_eq!(status, 200, "acknowledged object {key} survives the crash");
            assert_eq!(body, body_for(key), "acknowledged object {key} is intact");
        }
        let mut continuation = String::new();
        loop {
            let (status, listing) = request(
                &binary.address,
                "GET",
                &format!("/crash?list-type=2{continuation}"),
                b"",
            )
            .await
            .expect("the restarted server answers");
            assert_eq!(status, 200);
            for key in listed_keys(&listing) {
                let (status, body) = request(&binary.address, "GET", &format!("/crash/{key}"), b"")
                    .await
                    .expect("the restarted server answers");
                assert_eq!(status, 200);
                assert_eq!(body, body_for(&key), "listed object {key} is complete");
            }
            let text = String::from_utf8_lossy(&listing).into_owned();
            match text
                .split("<NextContinuationToken>")
                .nth(1)
                .and_then(|rest| rest.split('<').next())
            {
                Some(token) => {
                    let encoded: String = token
                        .bytes()
                        .map(|byte| match byte {
                            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' => (byte as char).to_string(),
                            _ => format!("%{byte:02X}"),
                        })
                        .collect();
                    continuation = format!("&continuation-token={encoded}");
                }
                None => break,
            }
        }
    }
    assert!(
        !acknowledged.lock().await.is_empty(),
        "the load produced acknowledged writes"
    );
    binary.child.kill().await.expect("the final child stops");
}
