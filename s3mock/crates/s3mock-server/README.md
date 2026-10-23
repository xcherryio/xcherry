# s3mock-server

## Purpose

S3-compatible HTTP server that serves an s3mock store over path-style URLs.

## Design

`s3mock_server::start` serves a `tokio::net::TcpListener` and returns a `ServerHandle` for
graceful shutdown. Tests bind port 0 and read the chosen address from the handle.
