// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::convert::Infallible;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Instant;

use bytes::Bytes;
use dexzd_metrics::{ApiLatency, ApiRequests, GrpcCode, RpcClass, classify_rpc};
use http_body::{Body as HttpBody, Frame, SizeHint};
use tonic::body::Body;
use tonic::codegen::{BoxFuture, Service, http};
use tower_layer::Layer;

/// Counts completed public RPCs outside authentication and excludes health checks.
#[derive(Clone)]
pub struct RequestMetricsLayer {
    requests: ApiRequests,
    latency: ApiLatency,
}

impl RequestMetricsLayer {
    /// Creates a layer with one node's fixed counters.
    pub fn new(requests: ApiRequests, latency: ApiLatency) -> Self {
        Self { requests, latency }
    }
}

impl<S> Layer<S> for RequestMetricsLayer {
    type Service = RequestMetricsService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        RequestMetricsService {
            inner,
            requests: self.requests.clone(),
            latency: self.latency.clone(),
        }
    }
}

/// Transport wrapper that records the final gRPC status after the body closes.
#[derive(Clone)]
pub struct RequestMetricsService<S> {
    inner: S,
    requests: ApiRequests,
    latency: ApiLatency,
}

impl<S> Service<http::Request<Body>> for RequestMetricsService<S>
where
    S: Service<http::Request<Body>, Response = http::Response<Body>, Error = Infallible>
        + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
{
    type Response = http::Response<Body>;
    type Error = Infallible;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(&mut self, context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(context)
    }

    fn call(&mut self, request: http::Request<Body>) -> Self::Future {
        let rpc_class = classify_rpc(request.uri().path());
        let requests = self.requests.clone();
        let latency = self.latency.clone();
        let started_at = Instant::now();
        let mut inner = self.inner.clone();
        Box::pin(async move {
            let response = inner.call(request).await?;
            let Some(rpc_class) = rpc_class else {
                return Ok(response);
            };
            let initial_code = status_code(response.headers()).unwrap_or(GrpcCode::Ok);
            let (parts, body) = response.into_parts();
            if body.is_end_stream() {
                requests.record(rpc_class, initial_code);
                latency.observe(rpc_class, started_at.elapsed().as_secs_f64());
                return Ok(http::Response::from_parts(parts, body));
            }
            let body = Body::new(RequestMetricsBody {
                inner: body,
                completion: Some((requests, rpc_class)),
                latency,
                started_at,
                code: initial_code,
            });
            Ok(http::Response::from_parts(parts, body))
        })
    }
}

fn status_code(headers: &http::HeaderMap) -> Option<GrpcCode> {
    headers
        .get("grpc-status")?
        .to_str()
        .ok()?
        .parse::<u8>()
        .ok()
        .map(GrpcCode::from_number)
}

struct RequestMetricsBody {
    inner: Body,
    completion: Option<(ApiRequests, RpcClass)>,
    latency: ApiLatency,
    started_at: Instant,
    code: GrpcCode,
}

impl RequestMetricsBody {
    fn finish(&mut self) {
        if let Some((requests, rpc_class)) = self.completion.take() {
            requests.record(rpc_class, self.code);
            self.latency
                .observe(rpc_class, self.started_at.elapsed().as_secs_f64());
        }
    }
}

impl HttpBody for RequestMetricsBody {
    type Data = Bytes;
    type Error = tonic::Status;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        match Pin::new(&mut self.inner).poll_frame(context) {
            Poll::Ready(Some(Ok(frame))) => {
                if let Some(trailers) = frame.trailers_ref()
                    && let Some(code) = status_code(trailers)
                {
                    self.code = code;
                    self.finish();
                }
                Poll::Ready(Some(Ok(frame)))
            }
            Poll::Ready(Some(Err(error))) => {
                self.code = GrpcCode::from_number(error.code() as u8);
                self.finish();
                Poll::Ready(Some(Err(error)))
            }
            Poll::Ready(None) => {
                self.finish();
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }

    fn is_end_stream(&self) -> bool {
        false
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

impl Drop for RequestMetricsBody {
    fn drop(&mut self) {
        if self.completion.is_some() {
            self.code = GrpcCode::Cancelled;
            self.finish();
        }
    }
}
