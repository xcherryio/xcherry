# s3mock-server

## Purpose

S3-compatible HTTP server that serves an s3mock store over path-style URLs.

## Design

`s3mock_server::start` serves a `tokio::net::TcpListener` and returns a `ServerHandle` for
graceful shutdown. Tests bind port 0 and read the chosen address from the handle.

`start_with_config` configures faults, request observations, and signature verification. See the
[workspace README](../../README.md#testing-controls) for flags and the JSON admin endpoint contract.
The admin dispatcher lives in `controls/admin.rs`, separately from the S3 router, for additional controls.
