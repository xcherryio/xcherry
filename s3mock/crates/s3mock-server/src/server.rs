// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Connection handling, request dispatch, and common response headers.

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use std::time::Instant;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use bytes::Bytes;
use http::{HeaderValue, Method, Request, Response, StatusCode};
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Empty, Full};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use hyper_util::server::graceful::GracefulShutdown;
use s3mock::{Store, StoreError};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::{JoinHandle, JoinSet};

use crate::config::ServerConfig;
use crate::controls::{Controls, FaultAction, ObservedBody, RequestObservation};
use crate::handlers;
use crate::routing::{self, S3Operation};
use crate::s3_error::S3Error;
use crate::xml::XmlWriter;

/// How long shutdown waits for in-flight requests before dropping their connections.
const SHUTDOWN_GRACE_PERIOD: Duration = Duration::from_secs(10);

pub(crate) type RequestBody = BoxBody<Bytes, io::Error>;
pub(crate) type ResponseBody = BoxBody<Bytes, io::Error>;
pub(crate) type S3Response = Response<ResponseBody>;

/// A running server; dropping it without [`ServerHandle::shutdown`] aborts the accept loop.
pub struct ServerHandle {
    local_address: SocketAddr,
    shutdown_sender: Option<oneshot::Sender<()>>,
    accept_task: JoinHandle<()>,
}

impl ServerHandle {
    /// Returns the bound address, which resolves port 0 to the chosen port.
    pub fn local_address(&self) -> SocketAddr {
        self.local_address
    }

    /// Stops accepting connections and waits for in-flight requests to finish.
    pub async fn shutdown(mut self) {
        if let Some(sender) = self.shutdown_sender.take()
            && sender.send(()).is_err()
        {
            tracing::warn!("the accept loop ended before shutdown was requested");
        }
        if let Err(error) = (&mut self.accept_task).await {
            tracing::error!(%error, "the accept loop failed");
        }
    }
}

impl Drop for ServerHandle {
    fn drop(&mut self) {
        self.accept_task.abort();
    }
}

/// Serves S3 requests from `listener` until [`ServerHandle::shutdown`] is called.
pub fn start(store: Arc<Store>, listener: TcpListener) -> io::Result<ServerHandle> {
    start_with_config(store, listener, ServerConfig::default())
}

/// Serves S3 and admin requests using explicit testing and authentication configuration.
pub fn start_with_config(
    store: Arc<Store>,
    listener: TcpListener,
    config: ServerConfig,
) -> io::Result<ServerHandle> {
    let controls = Arc::new(Controls::new(config));
    let local_address = listener.local_addr()?;
    let (shutdown_sender, shutdown_receiver) = oneshot::channel();
    let accept_task = tokio::spawn(accept_connections(
        store,
        controls,
        listener,
        shutdown_receiver,
    ));
    Ok(ServerHandle {
        local_address,
        shutdown_sender: Some(shutdown_sender),
        accept_task,
    })
}

async fn accept_connections(
    store: Arc<Store>,
    controls: Arc<Controls>,
    listener: TcpListener,
    mut shutdown_receiver: oneshot::Receiver<()>,
) {
    let graceful = GracefulShutdown::new();
    let connection_builder = hyper::server::conn::http1::Builder::new();
    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            _ = &mut shutdown_receiver => break,
            Some(finished) = connections.join_next(), if !connections.is_empty() => {
                if let Err(error) = finished {
                    tracing::error!(%error, "a connection task panicked");
                }
            }
            accepted = listener.accept() => {
                let (stream, peer_address) = match accepted {
                    Ok(accepted) => accepted,
                    Err(error) => {
                        tracing::warn!(%error, "failed to accept a connection");
                        continue;
                    }
                };
                let store = Arc::clone(&store);
                let controls = Arc::clone(&controls);
                let service = service_fn(move |request| handle_request(Arc::clone(&store), Arc::clone(&controls), request));
                let connection = graceful.watch(connection_builder.serve_connection(TokioIo::new(stream), service));
                connections.spawn(async move {
                    if let Err(error) = connection.await {
                        tracing::debug!(%peer_address, %error, "connection ended with an error");
                    }
                });
            }
        }
    }
    if tokio::time::timeout(SHUTDOWN_GRACE_PERIOD, graceful.shutdown())
        .await
        .is_err()
    {
        tracing::warn!("in-flight requests did not finish within the shutdown grace period");
    }
    connections.shutdown().await;
}

async fn handle_request(
    store: Arc<Store>,
    controls: Arc<Controls>,
    request: Request<Incoming>,
) -> Result<S3Response, io::Error> {
    let request = request.map(|body| body.map_err(io::Error::other).boxed());
    if request.uri().path().starts_with("/_s3mock/") {
        return Ok(controls.admin(store, request).await);
    }
    let started = Instant::now();
    let mut observation =
        RequestObservation::new(request.method(), request.uri(), request.headers());
    let request_id = ulid::Ulid::new().to_string();
    let is_head = request.method() == Method::HEAD;
    let received = Arc::new(AtomicU64::new(0));
    let counter = Arc::clone(&received);
    let request = request.map(|body| {
        body.map_frame(move |frame| {
            if let Some(data) = frame.data_ref() {
                counter.fetch_add(data.len() as u64, Ordering::Relaxed);
            }
            frame
        })
        .boxed()
    });
    let verified = crate::signature::verify_request(request, &controls.config).await;
    let fault = if verified.is_ok() {
        controls.select_fault(&observation)
    } else {
        None
    };
    observation.injected_fault = fault.as_ref().map(|action| action.name().to_owned());
    if let Some(FaultAction::Delay { milliseconds }) = fault {
        tokio::time::sleep(Duration::from_millis(milliseconds)).await;
    }
    let result = match verified {
        Err(error) => Err(error),
        Ok(request) => match fault.as_ref().and_then(FaultAction::error) {
            Some(error) => Err(error),
            None => dispatch(store, request).await,
        },
    };
    if matches!(fault, Some(FaultAction::CommitThenDrop))
        && result
            .as_ref()
            .is_ok_and(|response| response.status().is_success())
    {
        observation.bytes_received = received.load(Ordering::Relaxed);
        observation.duration_ms = started.elapsed().as_millis().try_into().unwrap_or(u64::MAX);
        controls.record(observation);
        return Err(io::Error::new(
            io::ErrorKind::ConnectionAborted,
            "injected drop after commit",
        ));
    }
    let mut response = match result {
        Ok(response) => response,
        Err(error) => render_error(error, &request_id, is_head),
    };
    let headers = response.headers_mut();
    headers.insert("x-amz-request-id", header_value(&request_id));
    headers.insert(
        "x-amz-id-2",
        header_value(&BASE64.encode(request_id.as_bytes())),
    );
    headers.insert("server", HeaderValue::from_static("s3mock"));
    observation.status = response.status().as_u16();
    let (parts, body) = response.into_parts();
    let body = ObservedBody::new(
        body,
        controls,
        observation,
        received,
        started,
        fault.as_ref(),
    )
    .boxed();
    Ok(Response::from_parts(parts, body))
}

async fn dispatch(store: Arc<Store>, request: Request<RequestBody>) -> Result<S3Response, S3Error> {
    let operation = routing::route(request.method(), request.uri())?;
    let (parts, body) = request.into_parts();
    match operation {
        S3Operation::ListBuckets => handlers::list_buckets(store).await,
        S3Operation::CreateBucket { bucket } => {
            handlers::create_bucket(store, bucket, &parts.headers).await
        }
        S3Operation::GetObjectLockConfiguration { bucket } => {
            handlers::get_object_lock_configuration(store, bucket).await
        }
        S3Operation::PutObjectLockConfiguration { bucket } => {
            handlers::put_object_lock_configuration(store, bucket, &parts.headers, body).await
        }
        S3Operation::ListMultipartUploads { bucket, query } => {
            handlers::list_multipart_uploads(store, bucket, query).await
        }
        S3Operation::DeleteObjects { bucket } => {
            handlers::delete_objects(store, bucket, parts.headers, body).await
        }
        S3Operation::CreateMultipartUpload { bucket, key } => {
            handlers::create_multipart_upload(store, bucket, key, &parts.headers).await
        }
        S3Operation::UploadPart { bucket, key, query } => {
            handlers::upload_part(store, bucket, key, query, parts.headers, body).await
        }
        S3Operation::CompleteMultipartUpload { bucket, key, query } => {
            handlers::complete_multipart_upload(store, bucket, key, query, &parts.headers, body)
                .await
        }
        S3Operation::AbortMultipartUpload { bucket, key, query } => {
            handlers::abort_multipart_upload(store, bucket, key, query).await
        }
        S3Operation::ListParts { bucket, key, query } => {
            handlers::list_parts(store, bucket, key, query).await
        }
        S3Operation::HeadBucket { bucket } => handlers::head_bucket(store, bucket).await,
        S3Operation::DeleteBucket { bucket } => handlers::delete_bucket(store, bucket).await,
        S3Operation::GetBucketLocation { bucket } => {
            handlers::get_bucket_location(store, bucket).await
        }
        S3Operation::GetBucketVersioning { bucket } => {
            handlers::get_bucket_versioning(store, bucket).await
        }
        S3Operation::PutBucketVersioning { bucket } => {
            handlers::put_bucket_versioning(store, bucket, body).await
        }
        S3Operation::ListObjectsV2 { bucket, query } => {
            handlers::list_objects_v2(store, bucket, query).await
        }
        S3Operation::ListObjectVersions { bucket, query } => {
            handlers::list_object_versions(store, bucket, query).await
        }
        S3Operation::PutObject { bucket, key } => {
            handlers::put_object(store, bucket, key, parts.headers, body).await
        }
        S3Operation::GetObject { bucket, key, query } => {
            handlers::get_object(store, bucket, key, query, &parts.headers).await
        }
        S3Operation::HeadObject { bucket, key, query } => {
            handlers::head_object(store, bucket, key, query, &parts.headers).await
        }
        S3Operation::DeleteObject { bucket, key, query } => {
            handlers::delete_object(store, bucket, key, query).await
        }
    }
}

fn render_error(error: S3Error, request_id: &str, is_head: bool) -> S3Response {
    let has_body = !is_head && error.status != StatusCode::NOT_MODIFIED;
    let body = if has_body {
        let mut document = XmlWriter::without_namespace("Error");
        document
            .text_element("Code", error.code)
            .text_element("Message", &error.message)
            .text_element("RequestId", request_id);
        full_body(document.finish("Error"))
    } else {
        empty_body()
    };
    let mut builder = Response::builder().status(error.status);
    if has_body {
        builder = builder.header("content-type", "application/xml");
    }
    for (name, value) in &error.headers {
        builder = builder.header(*name, value.as_str());
    }
    builder
        .body(body)
        .expect("error responses use valid static header names")
}

/// Runs a blocking store operation on the blocking thread pool.
pub(crate) async fn run_store_operation<T, F>(
    store: &Arc<Store>,
    operation: F,
) -> Result<T, S3Error>
where
    T: Send + 'static,
    F: FnOnce(&Store) -> Result<T, StoreError> + Send + 'static,
{
    let store = Arc::clone(store);
    tokio::task::spawn_blocking(move || operation(&store))
        .await
        .map_err(|error| S3Error::internal(format!("store task failed: {error}")))?
        .map_err(S3Error::from)
}

pub(crate) fn full_body(content: impl Into<Bytes>) -> ResponseBody {
    Full::new(content.into())
        .map_err(|never| match never {})
        .boxed()
}

pub(crate) fn empty_body() -> ResponseBody {
    Empty::new().map_err(|never| match never {}).boxed()
}

pub(crate) fn xml_response(document: String) -> Result<S3Response, S3Error> {
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/xml")
        .body(full_body(document))
        .map_err(|error| S3Error::internal(format!("failed to build a response: {error}")))
}

/// Converts text that originated as a valid header value, or that the server generated, into a header value.
pub(crate) fn header_value(text: &str) -> HeaderValue {
    HeaderValue::from_str(text).expect("stored metadata and generated values are valid header text")
}
