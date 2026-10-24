// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::net::SocketAddr;
use std::sync::Arc;

use dexzd_metrics::encode_registry;
use prometheus_client::registry::Registry;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;

use crate::ServerError;

#[cfg(test)]
mod tests;

pub(crate) struct MetricsServer {
    address: SocketAddr,
    stop: CancellationToken,
    task: JoinHandle<()>,
}

impl MetricsServer {
    pub(crate) async fn start(
        listen: &str,
        registry: Arc<Registry>,
    ) -> Result<Option<Self>, ServerError> {
        if listen.is_empty() {
            return Ok(None);
        }
        let address = listen
            .parse::<SocketAddr>()
            .expect("validated metrics.listen is a socket address");
        let listener = TcpListener::bind(address)
            .await
            .map_err(|source| ServerError::Listen { address, source })?;
        let address = listener
            .local_addr()
            .map_err(|source| ServerError::Listen { address, source })?;
        let stop = CancellationToken::new();
        let stopped = stop.clone();
        let task = tokio::spawn(async move { serve(listener, registry, stopped).await });
        Ok(Some(Self {
            address,
            stop,
            task,
        }))
    }

    pub(crate) fn address(&self) -> SocketAddr {
        self.address
    }

    pub(crate) async fn shutdown(mut self) {
        self.stop.cancel();
        if let Err(error) = (&mut self.task).await {
            tracing::warn!(%error, "metrics listener task failed");
        }
    }
}

impl Drop for MetricsServer {
    fn drop(&mut self) {
        self.stop.cancel();
        self.task.abort();
    }
}

async fn serve(listener: TcpListener, registry: Arc<Registry>, stop: CancellationToken) {
    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            () = stop.cancelled() => break,
            accepted = listener.accept() => match accepted {
                Ok((stream, _)) => {
                    let registry = registry.clone();
                    connections.spawn(async move {
                        if let Err(error) = respond(stream, registry).await {
                            tracing::debug!(%error, "metrics connection ended");
                        }
                    });
                }
                Err(error) => tracing::warn!(%error, "metrics accept failed"),
            },
            Some(result) = connections.join_next(), if !connections.is_empty() => {
                if let Err(error) = result { tracing::warn!(%error, "metrics connection task failed"); }
            },
        }
    }
    connections.abort_all();
    while let Some(result) = connections.join_next().await {
        if let Err(error) = result
            && !error.is_cancelled()
        {
            tracing::warn!(%error, "metrics connection task failed");
        }
    }
}

async fn respond(mut stream: TcpStream, registry: Arc<Registry>) -> std::io::Result<()> {
    let mut request = [0_u8; 8192];
    let mut received = 0;
    while received < request.len() && !request[..received].contains(&b'\n') {
        let count = stream.read(&mut request[received..]).await?;
        if count == 0 {
            break;
        }
        received += count;
    }
    let first_line = std::str::from_utf8(&request[..received]).unwrap_or("");
    let is_metrics = first_line.starts_with("GET /metrics HTTP/1.");
    let (status, content_type, body) = if is_metrics {
        match encode_registry(&registry) {
            Ok(body) => (
                "200 OK",
                "application/openmetrics-text; version=1.0.0; charset=utf-8",
                body,
            ),
            Err(error) => {
                tracing::error!(%error, "could not encode metrics");
                (
                    "500 Internal Server Error",
                    "text/plain; charset=utf-8",
                    "metrics encoding failed\n".to_owned(),
                )
            }
        }
    } else {
        (
            "404 Not Found",
            "text/plain; charset=utf-8",
            "not found\n".to_owned(),
        )
    };
    let headers = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(headers.as_bytes()).await?;
    stream.write_all(body.as_bytes()).await?;
    stream.shutdown().await
}
