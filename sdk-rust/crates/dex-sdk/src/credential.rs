// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::error::Error;
use std::fmt;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

use tonic::metadata::MetadataValue;
use tonic::service::Interceptor;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;
use tonic::{Request, Status};

/// Supplies the current bearer token before each Dex request.
///
/// Return the bare token without a `Bearer ` prefix. Implementations may be called concurrently.
/// The synchronous call can run on a Tokio worker thread; it must return promptly without network
/// I/O. A call deadline cannot interrupt a blocked implementation. Sources requiring slow I/O
/// should refresh in the background and return a cached token. An error fails the request locally
/// with gRPC `Unauthenticated`. Pull Worker streams evaluate the token when each stream opens; a
/// failed open backs off for about one minute. The token travels over Dex's plaintext gRPC
/// connection, so protect the network path.
///
/// ```no_run
/// use std::sync::{Arc, RwLock};
/// use dex_sdk::{ClientOptions, Credential, CredentialError};
/// struct RotatingCredential { current: RwLock<Option<String>> }
/// impl Credential for RotatingCredential {
///     fn token(&self) -> Result<String, CredentialError> {
///         self.current.read()
///             .map_err(|_| CredentialError::new("credential lock poisoned"))?
///             .clone()
///             .ok_or_else(|| CredentialError::new("token unavailable"))
///     }
/// }
/// let credential = Arc::new(RotatingCredential {
///     current: RwLock::new(Some("example-token".to_string())),
/// });
/// let client_options = ClientOptions::new().credential(credential);
/// ```
pub trait Credential: Send + Sync {
    /// Returns the current bare token, or an error that fails the request locally.
    ///
    /// Implementations must be safe for concurrent synchronous calls and return promptly. Do not
    /// include the `Bearer ` prefix. A credential failure maps to gRPC `Unauthenticated` without
    /// contacting Dex.
    fn token(&self) -> Result<String, CredentialError>;
}

impl fmt::Debug for dyn Credential {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Credential")
    }
}

/// An error obtaining a bearer token before a Dex request.
///
/// Return this from a custom [`Credential`] when no usable token is available. Avoid including
/// token contents in the error message.
///
/// ```
/// use dex_sdk::CredentialError;
/// let error = CredentialError::new("token unavailable");
/// assert_eq!(error.to_string(), "token unavailable");
/// ```
#[derive(Debug, Clone)]
pub struct CredentialError {
    message: String,
}

impl CredentialError {
    /// Creates an error for a credential source failure.
    ///
    /// `message` is shown in the local service error and must not contain a token. See the
    /// [`CredentialError`] example.
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for CredentialError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for CredentialError {}

/// Reads a token file at construction and after its modification time or size changes.
///
/// File metadata is checked at most once per second. Surrounding whitespace is removed. If the
/// file becomes empty or unreadable, requests fail until a later check succeeds; no stale token
/// is sent. The constructor fails when the initial file is unreadable or empty. Checks and reads
/// use a mutex, briefly serializing concurrent callers. Use a local filesystem; slow network
/// filesystems can delay calls. Replace the token atomically by writing a temporary file and
/// renaming it. An empty or partial token can fail pull stream opens, which back off for about one
/// minute.
///
/// ```no_run
/// use std::sync::Arc;
/// use dex_sdk::{ClientOptions, FileCredential, WorkerOptions};
/// let credential = Arc::new(FileCredential::new("/var/run/secrets/dex/token")?);
/// let client_options = ClientOptions::new().credential(credential.clone());
/// let worker_options = WorkerOptions::new().credential(credential);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub struct FileCredential {
    path: PathBuf,
    state: Mutex<FileCredentialState>,
}

struct FileCredentialState {
    token: Result<String, CredentialError>,
    modified: SystemTime,
    size: u64,
    checked_at: Instant,
}

impl fmt::Debug for FileCredential {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FileCredential")
            .field("path", &self.path)
            .field("token", &"<redacted>")
            .finish()
    }
}

impl FileCredential {
    /// Opens `path` and reads its initial nonempty token.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the file cannot be inspected or read, or contains only whitespace.
    /// See the [`FileCredential`] example for sharing the credential with a Client and Worker.
    pub fn new(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let metadata = fs::metadata(&path)?;
        let token = read_token(&path)?;
        Ok(Self {
            path,
            state: Mutex::new(FileCredentialState {
                token: Ok(token),
                modified: metadata.modified()?,
                size: metadata.len(),
                checked_at: Instant::now(),
            }),
        })
    }
}

impl Credential for FileCredential {
    fn token(&self) -> Result<String, CredentialError> {
        let mut state = self.state.lock().expect("credential file lock poisoned");
        if state.checked_at.elapsed() >= Duration::from_secs(1) {
            state.checked_at = Instant::now();
            state.token = match fs::metadata(&self.path) {
                Ok(metadata) => match metadata.modified() {
                    Ok(modified)
                        if state.token.is_ok()
                            && modified == state.modified
                            && metadata.len() == state.size =>
                    {
                        state.token.clone()
                    }
                    Ok(modified) => {
                        let token = read_token(&self.path).map_err(credential_file_error);
                        if token.is_ok() {
                            state.modified = modified;
                            state.size = metadata.len();
                        }
                        token
                    }
                    Err(error) => Err(credential_file_error(error)),
                },
                Err(error) => Err(credential_file_error(error)),
            };
        }
        state.token.clone()
    }
}

fn read_token(path: &Path) -> io::Result<String> {
    let token = fs::read_to_string(path)?.trim().to_string();
    if token.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("credential file {} is empty", path.display()),
        ));
    }
    Ok(token)
}

fn credential_file_error(error: io::Error) -> CredentialError {
    CredentialError::new(format!("read credential file: {error}"))
}

pub(crate) type CredentialChannel = InterceptedService<Channel, CredentialInterceptor>;

#[derive(Clone)]
pub(crate) struct CredentialInterceptor {
    credential: Option<Arc<dyn Credential>>,
}

impl CredentialInterceptor {
    pub(crate) fn new(credential: Option<Arc<dyn Credential>>) -> Self {
        Self { credential }
    }
}

impl Interceptor for CredentialInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        if let Some(credential) = &self.credential {
            let token = credential
                .token()
                .map_err(|error| Status::unauthenticated(error.to_string()))?;
            let value = MetadataValue::try_from(format!("Bearer {token}"))
                .map_err(|_| Status::unauthenticated("invalid bearer token metadata"))?;
            request.metadata_mut().insert("authorization", value);
        }
        Ok(request)
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll};

    use dex_protocol::dex::worker_task_service_client::WorkerTaskServiceClient;
    use dex_protocol::dex::worker_task_service_server::{
        WorkerTaskService, WorkerTaskServiceServer,
    };
    use dex_protocol::dex::{
        HealthInfo, PollWorkerTaskRequest, PollWorkerTaskResponse, ServerInfo,
    };
    use tokio::net::TcpListener;
    use tokio::sync::{mpsc, oneshot};
    use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
    use tonic::codegen::{BoxFuture, Service, http};
    use tonic::server::{NamedService, UnaryService};
    use tonic::transport::{Endpoint, Server};
    use tonic::{Response, Streaming};

    use super::*;
    use crate::{
        BlobCache, BlobCacheConfig, Client, ClientOptions, PullWorkerOptions, Registry, Worker,
        WorkerOptions,
    };

    #[derive(Clone)]
    struct FakeFlowService {
        headers: Arc<Mutex<Vec<Option<String>>>>,
    }

    impl NamedService for FakeFlowService {
        const NAME: &'static str = "dex.FlowService";
    }

    impl<B> Service<http::Request<B>> for FakeFlowService
    where
        B: tonic::codegen::Body + Send + 'static,
        B::Error: Into<tonic::codegen::StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::Body>;
        type Error = Infallible;
        type Future = BoxFuture<Self::Response, Self::Error>;

        fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, request: http::Request<B>) -> Self::Future {
            let is_server_info = request.uri().path() == "/dex.FlowService/GetServerInfo";
            let header = request
                .headers()
                .get("authorization")
                .map(|value| value.to_str().expect("ASCII bearer header").to_string());
            self.headers.lock().expect("headers lock").push(header);
            Box::pin(async move {
                if is_server_info {
                    let mut grpc = tonic::server::Grpc::new(tonic_prost::ProstCodec::default());
                    Ok(grpc.unary(GetServerInfo, request).await)
                } else {
                    let mut grpc = tonic::server::Grpc::new(tonic_prost::ProstCodec::default());
                    Ok(grpc.unary(HealthCheck, request).await)
                }
            })
        }
    }

    struct HealthCheck;

    impl UnaryService<()> for HealthCheck {
        type Response = HealthInfo;
        type Future = BoxFuture<Response<Self::Response>, Status>;

        fn call(&mut self, _: Request<()>) -> Self::Future {
            Box::pin(async { Ok(Response::new(HealthInfo::default())) })
        }
    }

    struct GetServerInfo;

    impl UnaryService<()> for GetServerInfo {
        type Response = ServerInfo;
        type Future = BoxFuture<Response<Self::Response>, Status>;

        fn call(&mut self, _: Request<()>) -> Self::Future {
            Box::pin(async { Ok(Response::new(ServerInfo::default())) })
        }
    }

    #[derive(Clone)]
    struct FakeTaskService {
        headers: Arc<Mutex<Vec<Option<String>>>>,
    }

    #[tonic::async_trait]
    impl WorkerTaskService for FakeTaskService {
        type PollWorkerTaskStream = ReceiverStream<Result<PollWorkerTaskResponse, Status>>;

        async fn poll_worker_task(
            &self,
            request: Request<Streaming<PollWorkerTaskRequest>>,
        ) -> Result<Response<Self::PollWorkerTaskStream>, Status> {
            let header = request
                .metadata()
                .get("authorization")
                .map(|value| value.to_str().expect("ASCII bearer header").to_string());
            self.headers.lock().expect("headers lock").push(header);
            let (_sender, receiver) = mpsc::channel(1);
            Ok(Response::new(ReceiverStream::new(receiver)))
        }
    }

    struct MutableCredential {
        token: Mutex<Result<String, CredentialError>>,
        calls: AtomicUsize,
    }

    impl Credential for MutableCredential {
        fn token(&self) -> Result<String, CredentialError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.token.lock().expect("token lock").clone()
        }
    }

    #[test]
    fn file_credential_rotates_and_fails_closed() {
        let directory = tempfile::tempdir().expect("token directory");
        let path = directory.path().join("token");
        assert!(FileCredential::new(&path).is_err());
        fs::write(&path, " \n").expect("write empty token");
        assert!(FileCredential::new(&path).is_err());
        fs::write(&path, "  first\n").expect("write initial token");
        let credential = FileCredential::new(&path).expect("initial token");
        assert_eq!(credential.token().expect("initial token"), "first");
        fs::write(&path, " second-token \n").expect("rotate token");
        assert_eq!(credential.token().expect("cached token"), "first");
        credential.state.lock().expect("file state lock").checked_at -= Duration::from_secs(1);
        assert_eq!(credential.token().expect("rotated token"), "second-token");
        fs::write(&path, " \n").expect("empty token");
        credential.state.lock().expect("file state lock").checked_at -= Duration::from_secs(1);
        assert!(credential.token().is_err());
        fs::remove_file(&path).expect("remove token");
        credential.state.lock().expect("file state lock").checked_at -= Duration::from_secs(1);
        assert!(credential.token().is_err());
        fs::write(&path, "restored").expect("restore token");
        credential.state.lock().expect("file state lock").checked_at -= Duration::from_secs(1);
        assert_eq!(credential.token().expect("restored token"), "restored");
    }

    #[test]
    fn file_credential_debug_redacts_token() {
        let directory = tempfile::tempdir().expect("token directory");
        let path = directory.path().join("token");
        fs::write(&path, "private-token-value").expect("write token");
        let credential = Arc::new(FileCredential::new(&path).expect("file credential"));
        let debug = format!("{credential:?}");
        assert!(!debug.contains("private-token-value"));
        assert!(debug.contains("<redacted>"));
    }

    #[test]
    fn flow_service_unary_uses_current_credential_and_default_has_no_header() {
        let runtime = tokio::runtime::Runtime::new().expect("server runtime");
        let listener = runtime
            .block_on(TcpListener::bind("127.0.0.1:0"))
            .expect("bind fake FlowService");
        let address = listener.local_addr().expect("fake server address");
        let headers = Arc::new(Mutex::new(Vec::new()));
        let (shutdown, stopped) = oneshot::channel::<()>();
        runtime.spawn(
            Server::builder()
                .add_service(FakeFlowService {
                    headers: Arc::clone(&headers),
                })
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                    stopped.await.expect("FlowService shutdown signal");
                }),
        );
        let credential = Arc::new(MutableCredential {
            token: Mutex::new(Ok("first".to_string())),
            calls: AtomicUsize::new(0),
        });
        let directory = tempfile::tempdir().expect("blob cache directory");
        let cache = Arc::new(
            BlobCache::open(
                BlobCacheConfig::new(directory.path(), 1024 * 1024, 0).expect("cache config"),
            )
            .expect("open cache"),
        );
        let without_credential = Client::try_new(
            Registry::new(),
            Arc::clone(&cache),
            ClientOptions::new().server_address(address.to_string()),
        )
        .expect("default Client");
        without_credential.health_check().expect("default call");
        let with_credential = Client::try_new(
            Registry::new(),
            Arc::clone(&cache),
            ClientOptions::new()
                .server_address(address.to_string())
                .credential(credential.clone()),
        )
        .expect("authenticated Client");
        with_credential.health_check().expect("first call");
        *credential.token.lock().expect("token lock") = Ok("second".to_string());
        with_credential.health_check().expect("rotated call");
        *credential.token.lock().expect("token lock") =
            Err(CredentialError::new("token unavailable"));
        let error = with_credential.health_check().expect_err("local error");
        assert_eq!(
            error.service_error().expect("service error").code(),
            tonic::Code::Unauthenticated
        );
        *credential.token.lock().expect("token lock") = Ok("worker".to_string());
        let worker = Worker::try_new(
            Registry::new(),
            cache,
            WorkerOptions::new()
                .server_address(address.to_string())
                .credential(credential.clone())
                .pull(PullWorkerOptions::new()),
        )
        .expect("authenticated Worker");
        worker
            .start()
            .expect_err("fake protocol interval is invalid");
        assert_eq!(credential.calls.load(Ordering::SeqCst), 4);
        assert_eq!(
            *headers.lock().expect("headers lock"),
            vec![
                None,
                Some("Bearer first".to_string()),
                Some("Bearer second".to_string()),
                Some("Bearer worker".to_string())
            ]
        );
        shutdown.send(()).expect("stop fake FlowService");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pull_stream_open_reevaluates_credential() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fake WorkerTaskService");
        let address = listener.local_addr().expect("fake server address");
        let headers = Arc::new(Mutex::new(Vec::new()));
        let (shutdown, stopped) = oneshot::channel::<()>();
        let server = tokio::spawn(
            Server::builder()
                .add_service(WorkerTaskServiceServer::new(FakeTaskService {
                    headers: Arc::clone(&headers),
                }))
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                    stopped.await.expect("WorkerTaskService shutdown signal");
                }),
        );
        let credential = Arc::new(MutableCredential {
            token: Mutex::new(Ok("first".to_string())),
            calls: AtomicUsize::new(0),
        });
        let channel = Endpoint::from_shared(format!("http://{address}"))
            .expect("endpoint")
            .connect()
            .await
            .expect("connect fake WorkerTaskService");
        let mut default_client = WorkerTaskServiceClient::new(CredentialChannel::new(
            channel.clone(),
            CredentialInterceptor::new(None),
        ));
        default_client
            .poll_worker_task(tokio_stream::empty::<PollWorkerTaskRequest>())
            .await
            .expect("open default task stream");
        let mut client = WorkerTaskServiceClient::new(CredentialChannel::new(
            channel,
            CredentialInterceptor::new(Some(credential.clone())),
        ));
        for token in ["first", "second"] {
            *credential.token.lock().expect("token lock") = Ok(token.to_string());
            client
                .poll_worker_task(tokio_stream::empty::<PollWorkerTaskRequest>())
                .await
                .expect("open task stream");
        }
        *credential.token.lock().expect("token lock") =
            Err(CredentialError::new("token unavailable"));
        let error = client
            .poll_worker_task(tokio_stream::empty::<PollWorkerTaskRequest>())
            .await
            .expect_err("local task stream error");
        assert_eq!(error.code(), tonic::Code::Unauthenticated);
        assert_eq!(credential.calls.load(Ordering::SeqCst), 3);
        assert_eq!(
            *headers.lock().expect("headers lock"),
            vec![
                None,
                Some("Bearer first".to_string()),
                Some("Bearer second".to_string())
            ]
        );
        shutdown.send(()).expect("stop fake WorkerTaskService");
        server
            .await
            .expect("join fake WorkerTaskService")
            .expect("serve task service");
    }
}
