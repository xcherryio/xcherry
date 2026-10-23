# s3mock-server

Purpose: the `s3mock` binary and library that serve an s3mock store as a path-style S3 endpoint.

`s3mock_server::start` serves a `tokio::net::TcpListener` and returns a `ServerHandle` for
graceful shutdown. Tests bind port 0 and read the chosen address from the handle.
