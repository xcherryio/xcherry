// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::{JoinHandle, JoinSet};

type RecordedConnection = Arc<Mutex<Vec<u8>>>;
type RecordedConnections = Arc<Mutex<Vec<RecordedConnection>>>;

pub(super) struct RecordingS3Proxy {
    address: SocketAddr,
    connections: RecordedConnections,
    accept_task: Option<JoinHandle<()>>,
}

impl RecordingS3Proxy {
    pub(super) async fn start(upstream_address: SocketAddr) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("the recording proxy binds");
        let address = listener.local_addr().expect("the proxy has an address");
        let connections = Arc::new(Mutex::new(Vec::new()));
        let recorded_connections = Arc::clone(&connections);
        let accept_task = tokio::spawn(async move {
            let mut connection_tasks = JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let (client, _) = accepted.expect("the recording proxy accepts connections");
                        let recorded_bytes = Arc::new(Mutex::new(Vec::new()));
                        recorded_connections.lock().expect("proxy records remain available")
                            .push(Arc::clone(&recorded_bytes));
                        connection_tasks.spawn(async move {
                            if let Err(error) = forward_connection(client, upstream_address, recorded_bytes).await {
                                tracing::debug!(?error, "recording proxy connection closed");
                            }
                        });
                    }
                    completed = connection_tasks.join_next(), if !connection_tasks.is_empty() => {
                        completed.expect("a connection task completed")
                            .expect("the recording proxy task remains healthy");
                    }
                }
            }
        });
        Self {
            address,
            connections,
            accept_task: Some(accept_task),
        }
    }

    pub(super) fn address(&self) -> SocketAddr {
        self.address
    }

    pub(super) fn multipart_request_headers(&self) -> Vec<String> {
        let connections = self
            .connections
            .lock()
            .expect("proxy records remain available");
        let mut requests = Vec::new();
        for connection in connections.iter() {
            let bytes = connection.lock().expect("proxy bytes remain available");
            for prefix in [
                b"POST /anonymous-file-uploads/multipart?".as_slice(),
                b"PUT /anonymous-file-uploads/multipart?".as_slice(),
            ] {
                for (index, window) in bytes.windows(prefix.len()).enumerate() {
                    if window != prefix {
                        continue;
                    }
                    let request = &bytes[index..];
                    let Some(end) = request.windows(4).position(|window| window == b"\r\n\r\n")
                    else {
                        continue;
                    };
                    if end > 16 * 1024 {
                        continue;
                    }
                    let header = std::str::from_utf8(&request[..end])
                        .expect("multipart request headers are UTF-8");
                    requests.push(header.to_owned());
                }
            }
        }
        requests
    }

    pub(super) async fn shutdown(mut self) {
        let accept_task = self.accept_task.take().expect("the proxy task is active");
        accept_task.abort();
        match accept_task.await {
            Err(error) if error.is_cancelled() => {}
            result => panic!("the recording proxy must stop cleanly: {result:?}"),
        }
    }
}

impl Drop for RecordingS3Proxy {
    fn drop(&mut self) {
        if let Some(accept_task) = &self.accept_task {
            accept_task.abort();
        }
    }
}

async fn forward_connection(
    client: TcpStream,
    upstream_address: SocketAddr,
    recorded_bytes: RecordedConnection,
) -> std::io::Result<()> {
    let upstream = TcpStream::connect(upstream_address).await?;
    let (mut client_reader, mut client_writer) = client.into_split();
    let (mut upstream_reader, mut upstream_writer) = upstream.into_split();
    let request_forward = async {
        let mut buffer = [0_u8; 16 * 1024];
        loop {
            let count = client_reader.read(&mut buffer).await?;
            if count == 0 {
                upstream_writer.shutdown().await?;
                break;
            }
            recorded_bytes
                .lock()
                .expect("proxy bytes remain available")
                .extend_from_slice(&buffer[..count]);
            upstream_writer.write_all(&buffer[..count]).await?;
        }
        Ok::<(), std::io::Error>(())
    };
    let response_forward = async {
        tokio::io::copy(&mut upstream_reader, &mut client_writer).await?;
        client_writer.shutdown().await?;
        Ok::<(), std::io::Error>(())
    };
    tokio::try_join!(request_forward, response_forward)?;
    Ok(())
}
