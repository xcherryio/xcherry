// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{ExitStatus, Stdio};
use std::sync::Arc;
use std::time::Duration;

use dexzd_protocol::dex::worker_task_service_client::WorkerTaskServiceClient;
use dexzd_protocol::dex::{
    InvokeExecuteMethodOutput, InvokeExecuteMethodRequest, InvokeExecuteMethodResponse,
    InvokeWaitForMethodOutput, InvokeWaitForMethodResponse, PollWorkerTaskFrame,
    PollWorkerTaskRegister, PollWorkerTaskRequest, PollWorkerTaskResponse, WaitingCondition,
    invoke_execute_method_output, invoke_wait_for_method_output, poll_worker_task,
    poll_worker_task_frame, poll_worker_task_request, poll_worker_task_response,
};
use s3mock::{Durability, EtagMode, ListObjectsRequest, Store, StoreConfig};
use s3mock_server::{ServerConfig, ServerHandle, SignatureVerification};
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt, copy_bidirectional};
use tokio::process::{Child, Command};
use tokio::sync::{mpsc, watch};
use tokio::task::{JoinHandle, JoinSet};
use tokio_postgres::NoTls;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::transport::Channel;
use tonic_health::pb::HealthCheckRequest;
use tonic_health::pb::health_check_response::ServingStatus;
use tonic_health::pb::health_client::HealthClient;

/// Test-only keys that the strict primary emulator verifies.
pub(crate) const ACCESS_KEY_ID: &str = "serve-test-access-key";
pub(crate) const SECRET_ACCESS_KEY: &str = "serve-test-secret-key";
const SESSION_TOKEN: &str = "serve-test-session-token";
/// Service names that readiness probes check.
const READINESS_SERVICE_NAMES: [&str; 3] = ["", "dex.FlowService", "dex.WorkerTaskService"];
const STARTUP_BUDGET: Duration = Duration::from_secs(30);

/// An s3mock server in this process with one bucket.
pub(crate) struct S3Emulator {
    store: Arc<Store>,
    server: ServerHandle,
    _directory: TempDir,
}

impl S3Emulator {
    /// Starts on a free loopback port; strict verification accepts only [`ACCESS_KEY_ID`].
    pub(crate) async fn start(bucket: &str, signature_verification: SignatureVerification) -> Self {
        let directory = TempDir::new().expect("a temporary emulator directory can be created");
        let store = Store::open(&StoreConfig {
            root_directory: directory.path().to_path_buf(),
            durability: Durability::Relaxed,
            etag_mode: EtagMode::Md5,
        })
        .expect("the emulator store opens");
        store
            .create_bucket(bucket, false)
            .expect("the emulator bucket is created");
        let store = Arc::new(store);
        let listener = tokio::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .await
            .expect("the emulator binds a loopback port");
        let server = s3mock_server::start_with_config(
            store.clone(),
            listener,
            ServerConfig {
                data_directory: directory.path().to_path_buf(),
                signature_verification,
                access_key_id: ACCESS_KEY_ID.to_owned(),
                secret_access_key: SECRET_ACCESS_KEY.to_owned(),
                ..ServerConfig::default()
            },
        )
        .expect("the emulator starts");
        Self {
            store,
            server,
            _directory: directory,
        }
    }

    pub(crate) fn endpoint(&self) -> String {
        format!("http://{}", self.server.local_address())
    }

    /// Returns the first key segments of every object in `bucket`, such as `cluster-a/`.
    pub(crate) fn top_level_prefixes(&self, bucket: &str) -> Vec<String> {
        self.store
            .list_objects(
                bucket,
                &ListObjectsRequest {
                    prefix: String::new(),
                    delimiter: "/".to_owned(),
                    max_keys: 1_000,
                    continuation_token: None,
                    start_after: None,
                },
            )
            .expect("the emulator lists objects")
            .common_prefixes
    }

    /// Returns whether `bucket` holds an object whose key starts with `prefix`.
    pub(crate) fn has_object_under(&self, bucket: &str, prefix: &str) -> bool {
        !self
            .store
            .list_objects(
                bucket,
                &ListObjectsRequest {
                    prefix: prefix.to_owned(),
                    delimiter: String::new(),
                    max_keys: 1,
                    continuation_token: None,
                    start_after: None,
                },
            )
            .expect("the emulator lists objects")
            .objects
            .is_empty()
    }

    /// Returns every key in `bucket` that starts with `prefix`, in key order.
    pub(crate) fn keys_under(&self, bucket: &str, prefix: &str) -> Vec<String> {
        let mut keys = Vec::new();
        let mut continuation_token = None;
        loop {
            let page = self
                .store
                .list_objects(
                    bucket,
                    &ListObjectsRequest {
                        prefix: prefix.to_owned(),
                        delimiter: String::new(),
                        max_keys: 1_000,
                        continuation_token,
                        start_after: None,
                    },
                )
                .expect("the emulator lists objects");
            keys.extend(page.objects.into_iter().map(|object| object.key));
            if !page.is_truncated {
                return keys;
            }
            continuation_token = page.next_continuation_token;
        }
    }

    /// Replaces the emulator's fault rules with `rules`, a JSON array of s3mock fault rules.
    pub(crate) async fn set_fault_rules(&self, rules: &str) {
        let response = self
            .admin_request("PUT", "/_s3mock/faults", &format!(r#"{{"rules":{rules}}}"#))
            .await;
        assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    }

    /// Returns the emulator's JSON log of recent requests.
    pub(crate) async fn request_log(&self) -> String {
        self.admin_request("GET", "/_s3mock/requests", "").await
    }

    async fn admin_request(&self, method: &str, path: &str, body: &str) -> String {
        let address = self.server.local_address();
        let mut stream = tokio::net::TcpStream::connect(address)
            .await
            .expect("the emulator admin endpoint is reachable");
        stream
            .write_all(
                format!(
                    "{method} {path} HTTP/1.1\r\nHost: {address}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                )
                .as_bytes(),
            )
            .await
            .expect("the admin request is sent");
        let mut response = Vec::new();
        stream
            .read_to_end(&mut response)
            .await
            .expect("the admin response is complete");
        String::from_utf8(response).expect("the admin response is UTF-8")
    }

    pub(crate) async fn shutdown(self) {
        self.server.shutdown().await;
    }
}

/// Settings that differ between the test processes; everything else stays at its default.
pub(crate) struct ServeConfig<'a> {
    pub(crate) primary_endpoint: &'a str,
    pub(crate) hard_delete_endpoint: Option<&'a str>,
    pub(crate) listen: SocketAddr,
    pub(crate) local_directory: &'a Path,
    pub(crate) lease_duration: &'a str,
    pub(crate) fail_if_held: bool,
    /// Selects `search.backend: paradedb` when set; builtin otherwise.
    pub(crate) parade_db: Option<ParadeDbSettings<'a>>,
}

/// The ParadeDB database and schema that `search.backend: paradedb` uses.
pub(crate) struct ParadeDbSettings<'a> {
    pub(crate) connection_string: &'a str,
    pub(crate) schema: &'a str,
}

impl ServeConfig<'_> {
    /// Writes the YAML file that `serve --config` reads.
    pub(crate) fn write(&self, path: &Path) {
        self.write_with_snapshot_schedule(path, "1h", "24h");
    }

    /// Writes the YAML file with the given `snapshot.interval` and `snapshot.logDeletionDelay`.
    pub(crate) fn write_with_snapshot_schedule(
        &self,
        path: &Path,
        snapshot_interval: &str,
        log_deletion_delay: &str,
    ) {
        let hard_delete_store = match self.hard_delete_endpoint {
            Some(endpoint) => format!(
                "  hardDeleteStore:\n    endpoint: {endpoint}\n    region: us-east-1\n    bucket: {}\n    prefix: hard-delete\n    forcePathStyle: true\n    credentialsSource: anonymous\n",
                super::HARD_DELETE_BUCKET
            ),
            None => String::new(),
        };
        let search = match &self.parade_db {
            Some(parade_db) => format!(
                "search:\n  backend: paradedb\n  paradeDbConnectionString: '{}'\n  paradeDbSchema: {}\n  paradeDbConnectionPoolSize: 2\n",
                parade_db.connection_string, parade_db.schema
            ),
            None => "search:\n  backend: builtin\n".to_owned(),
        };
        let contents = format!(
            "cluster:\n  nodeId: {node_id}\n  partitionCount: 16\nobjectStore:\n  endpoint: {endpoint}\n  region: us-east-1\n  bucket: {bucket}\n  prefix: {prefix}\n  forcePathStyle: true\n  credentialsSource: environment\n{hard_delete_store}localStorage:\n  directory: {directory}\n  blockCacheBytes: 16777216\nlease:\n  duration: {lease}\n  failIfHeld: {fail_if_held}\nsnapshot:\n  interval: {snapshot_interval}\n  logDeletionDelay: {log_deletion_delay}\n  retainedCount: 2\napi:\n  listen: \"{listen}\"\n{search}",
            node_id = super::NODE_ID,
            endpoint = self.primary_endpoint,
            bucket = super::PRIMARY_BUCKET,
            prefix = super::PRIMARY_PREFIX,
            directory = self.local_directory.display(),
            lease = self.lease_duration,
            fail_if_held = self.fail_if_held,
            listen = self.listen,
        );
        fs::write(path, contents).expect("the serve configuration is written");
    }
}

/// A `dex-server-zd serve` child process; dropping it kills the process.
pub(crate) struct ServeProcess {
    child: Child,
    address: SocketAddr,
    log_path: PathBuf,
}

impl ServeProcess {
    /// Starts `serve --config` with environment credentials for the primary store.
    pub(crate) fn spawn(config_path: &Path, address: SocketAddr, log_path: PathBuf) -> Self {
        let log = fs::File::create(&log_path).expect("the process log file is created");
        let child = Command::new(env!("CARGO_BIN_EXE_dex-server-zd"))
            .arg("serve")
            .arg("--config")
            .arg(config_path)
            .env("DEX_ZD_OBJECT_STORE_ACCESS_KEY_ID", ACCESS_KEY_ID)
            .env("DEX_ZD_OBJECT_STORE_SECRET_ACCESS_KEY", SECRET_ACCESS_KEY)
            .env("DEX_ZD_OBJECT_STORE_SESSION_TOKEN", SESSION_TOKEN)
            .env("RUST_LOG", "info")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(log)
            .kill_on_drop(true)
            .spawn()
            .expect("dex-server-zd starts");
        Self {
            child,
            address,
            log_path,
        }
    }

    /// Waits until every readiness name is SERVING and returns a connected channel.
    pub(crate) async fn wait_until_serving(&mut self) -> Channel {
        let deadline = tokio::time::Instant::now() + STARTUP_BUDGET;
        loop {
            if let Some(status) = self
                .child
                .try_wait()
                .expect("the process status is readable")
            {
                panic!(
                    "dex-server-zd exited with {status} before serving:\n{}",
                    self.log()
                );
            }
            if let Some(channel) = self.serving_channel().await {
                return channel;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "dex-server-zd did not serve within {STARTUP_BUDGET:?}:\n{}",
                self.log()
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Returns whether every readiness name reports SERVING at some point within `budget`.
    pub(crate) async fn is_serving_within(&self, budget: Duration) -> bool {
        let deadline = tokio::time::Instant::now() + budget;
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return false;
            }
            if let Ok(Some(_)) = tokio::time::timeout(remaining, self.serving_channel()).await {
                return true;
            }
            tokio::time::sleep(remaining.min(Duration::from_millis(50))).await;
        }
    }

    /// Waits until the process log contains `line`, failing if the process exits first or
    /// `budget` elapses.
    pub(crate) async fn wait_for_log_line(&mut self, line: &str, budget: Duration) {
        let deadline = tokio::time::Instant::now() + budget;
        loop {
            if self.log().contains(line) {
                return;
            }
            if let Some(status) = self
                .child
                .try_wait()
                .expect("the process status is readable")
            {
                panic!(
                    "dex-server-zd exited with {status} before logging {line:?}:\n{}",
                    self.log()
                );
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "dex-server-zd did not log {line:?} within {budget:?}:\n{}",
                self.log()
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    async fn serving_channel(&self) -> Option<Channel> {
        let channel = Channel::from_shared(format!("http://{}", self.address))
            .expect("the server address is a valid URI")
            .connect()
            .await
            .ok()?;
        let mut health = HealthClient::new(channel.clone());
        for service in READINESS_SERVICE_NAMES {
            let response = health
                .check(HealthCheckRequest {
                    service: service.to_owned(),
                })
                .await
                .ok()?;
            if response.get_ref().status != ServingStatus::Serving as i32 {
                return None;
            }
        }
        Some(channel)
    }

    /// Sends SIGTERM and waits for the process to exit.
    pub(crate) async fn terminate(&mut self, budget: Duration) -> ExitStatus {
        let process_id = self.child.id().expect("the process is still running");
        let signal_status = Command::new("kill")
            .arg("-TERM")
            .arg(process_id.to_string())
            .status()
            .await
            .expect("kill runs");
        assert!(signal_status.success(), "kill -TERM {process_id} failed");
        self.wait_for_exit(budget).await
    }

    /// Waits for the process to exit by itself within `budget`.
    pub(crate) async fn wait_for_exit(&mut self, budget: Duration) -> ExitStatus {
        match tokio::time::timeout(budget, self.child.wait()).await {
            Ok(status) => status.expect("the process exit status is readable"),
            Err(_) => panic!(
                "dex-server-zd did not exit within {budget:?}:\n{}",
                self.log()
            ),
        }
    }

    /// Returns everything the process wrote to standard error.
    pub(crate) fn log(&self) -> String {
        fs::read_to_string(&self.log_path)
            .unwrap_or_else(|error| format!("<cannot read {}: {error}>", self.log_path.display()))
    }
}

/// An isolated schema in the ParadeDB database that `DEX_ZD_PARADEDB_URL` names.
pub(crate) struct ParadeDbSchema {
    client: tokio_postgres::Client,
    connection: JoinHandle<()>,
    name: String,
}

impl ParadeDbSchema {
    /// Installs `pg_search` and creates an empty schema with a name unique to this run.
    pub(crate) async fn create(database_url: &str) -> Self {
        let (client, connection) = tokio_postgres::connect(database_url, NoTls)
            .await
            .expect("the test connects to ParadeDB");
        let connection = tokio::spawn(async move {
            if let Err(error) = connection.await {
                eprintln!("the ParadeDB test connection closed: {error}");
            }
        });
        client
            .batch_execute("CREATE EXTENSION IF NOT EXISTS pg_search")
            .await
            .expect("pg_search is installed");
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("the wall clock is after the Unix epoch")
            .as_nanos();
        let name = format!("dexzd_serve_{}_{nonce}", std::process::id());
        client
            .batch_execute(&format!("CREATE SCHEMA {name}"))
            .await
            .expect("the test schema is created");
        Self {
            client,
            connection,
            name,
        }
    }

    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    /// Deletes every projected run and position, so only a rebuild can restore them.
    pub(crate) async fn clear_projection(&self) {
        self.client
            .batch_execute(&format!(
                "TRUNCATE TABLE {0}.dexzd_visibility_runs, {0}.dexzd_visibility_progress",
                self.name
            ))
            .await
            .expect("the projection tables are cleared");
    }

    pub(crate) async fn drop_schema(self) {
        self.client
            .batch_execute(&format!("DROP SCHEMA {} CASCADE", self.name))
            .await
            .expect("the test schema is dropped");
        self.connection.abort();
    }
}

/// Relays PostgreSQL connections and can cut them while the server keeps committing.
pub(crate) struct ParadeDbTcpProxy {
    address: SocketAddr,
    availability: watch::Sender<bool>,
    active_connections: watch::Receiver<usize>,
    task: JoinHandle<()>,
}

impl ParadeDbTcpProxy {
    pub(crate) async fn start(database_url: &str) -> Self {
        let configuration = database_url
            .parse::<tokio_postgres::Config>()
            .expect("the ParadeDB URL is valid");
        let host = match configuration.get_hosts().first() {
            Some(tokio_postgres::config::Host::Tcp(host)) => host.clone(),
            _ => panic!("the ParadeDB URL names a TCP host"),
        };
        let port = configuration.get_ports().first().copied().unwrap_or(5432);
        let listener = tokio::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .await
            .expect("the ParadeDB proxy binds a loopback port");
        let address = listener.local_addr().expect("the proxy has an address");
        let (availability, mut is_available) = watch::channel(true);
        let (connection_count, active_connections) = watch::channel(0_usize);
        let task = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let (incoming, _) = accepted.expect("the proxy accepts connections");
                        if !*is_available.borrow() {
                            continue;
                        }
                        connection_count.send_modify(|count| *count += 1);
                        let active = connection_count.clone();
                        let availability = is_available.clone();
                        let host = host.clone();
                        connections.spawn(async move {
                            relay_parade_db_connection(incoming, &host, port, availability).await;
                            active.send_modify(|count| *count -= 1);
                        });
                    }
                    Some(result) = connections.join_next(), if !connections.is_empty() => {
                        result.expect("a proxy connection task completes");
                    }
                    changed = is_available.changed() => {
                        changed.expect("the proxy controller stays open");
                    }
                }
            }
        });
        Self {
            address,
            availability,
            active_connections,
            task,
        }
    }

    pub(crate) fn connection_string(&self, database_url: &str) -> String {
        let (credentials, authority_and_path) = database_url
            .rsplit_once('@')
            .expect("the ParadeDB URL includes credentials");
        let (_, path) = authority_and_path
            .split_once('/')
            .expect("the ParadeDB URL includes a database path");
        format!("{credentials}@127.0.0.1:{}/{path}", self.address.port())
    }

    pub(crate) async fn cut(&mut self) {
        self.availability
            .send(false)
            .expect("the proxy task remains live");
        while *self.active_connections.borrow() != 0 {
            self.active_connections
                .changed()
                .await
                .expect("the proxy reports active connections");
        }
    }

    pub(crate) fn restore(&self) {
        self.availability
            .send(true)
            .expect("the proxy task remains live");
    }

    pub(crate) async fn shutdown(mut self) {
        self.task.abort();
        let error = (&mut self.task)
            .await
            .expect_err("the proxy task is aborted");
        assert!(error.is_cancelled(), "the proxy task failed: {error}");
    }
}

impl Drop for ParadeDbTcpProxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn relay_parade_db_connection(
    mut incoming: tokio::net::TcpStream,
    host: &str,
    port: u16,
    mut availability: watch::Receiver<bool>,
) {
    let mut outgoing = match tokio::net::TcpStream::connect((host, port)).await {
        Ok(outgoing) => outgoing,
        Err(error) => {
            eprintln!("the ParadeDB proxy could not connect: {error}");
            return;
        }
    };
    tokio::select! {
        result = copy_bidirectional(&mut incoming, &mut outgoing) => {
            if let Err(error) = result {
                eprintln!("the ParadeDB proxy connection ended: {error}");
            }
        }
        changed = availability.changed() => {
            changed.expect("the proxy controller stays open");
        }
    }
}

/// Returns a loopback address whose port was free a moment ago.
pub(crate) fn free_loopback_address() -> SocketAddr {
    std::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .and_then(|listener| listener.local_addr())
        .expect("a free loopback port exists")
}

/// Decides the result of one Execute call.
pub(crate) type ExecuteHandler =
    Box<dyn Fn(&InvokeExecuteMethodRequest) -> InvokeExecuteMethodResponse + Send + Sync>;

/// A minimal pull Worker for one Flow type: WaitFor proceeds at once; Execute calls `handler`.
pub(crate) struct PullWorker {
    frames: mpsc::UnboundedSender<PollWorkerTaskRequest>,
    task: JoinHandle<()>,
}

impl PullWorker {
    /// Registers on the Flow type queue and waits for `registered`.
    pub(crate) async fn start(
        mut client: WorkerTaskServiceClient<Channel>,
        flow_type: &str,
        handler: ExecuteHandler,
    ) -> Self {
        let (frames, outgoing) = mpsc::unbounded_channel();
        frames
            .send(PollWorkerTaskRequest {
                frame: Some(poll_worker_task_request::Frame::Register(
                    PollWorkerTaskRegister {
                        queue: flow_type.to_owned(),
                        app_worker_id: "serve-process-worker".to_owned(),
                        flow_types: vec![flow_type.to_owned()],
                        sdk_name: "dexzd-server-tests".to_owned(),
                        sdk_version: "0".to_owned(),
                    },
                )),
            })
            .expect("the outgoing stream is open");
        let mut incoming = client
            .poll_worker_task(UnboundedReceiverStream::new(outgoing))
            .await
            .expect("PollWorkerTask opens")
            .into_inner();
        let first = incoming
            .next()
            .await
            .expect("the server answers the registration")
            .expect("the registration succeeds");
        assert!(
            matches!(
                first.frame,
                Some(poll_worker_task_response::Frame::Registered(_))
            ),
            "the first frame is registered, got {first:?}"
        );
        let replies = frames.clone();
        let task = tokio::spawn(async move {
            while let Some(Ok(response)) = incoming.next().await {
                if let Some(reply) = reply_to(response, &handler)
                    && replies.send(reply).is_err()
                {
                    return;
                }
            }
        });
        Self { frames, task }
    }

    /// Closes the Worker's stream.
    pub(crate) async fn stop(self) {
        drop(self.frames);
        self.task.abort();
        if let Err(error) = self.task.await {
            assert!(error.is_cancelled(), "the Worker task panicked: {error}");
        }
    }
}

fn reply_to(
    response: PollWorkerTaskResponse,
    handler: &ExecuteHandler,
) -> Option<PollWorkerTaskRequest> {
    let task = match response.frame? {
        poll_worker_task_response::Frame::Task(task) => task,
        poll_worker_task_response::Frame::Ping(_) => {
            return Some(PollWorkerTaskRequest {
                frame: Some(poll_worker_task_request::Frame::Pong(Default::default())),
            });
        }
        _ => return None,
    };
    let output = match task.request? {
        poll_worker_task::Request::WaitFor(_) => {
            poll_worker_task_frame::Output::WaitFor(InvokeWaitForMethodOutput {
                output: Some(invoke_wait_for_method_output::Output::Result(
                    InvokeWaitForMethodResponse {
                        waiting_condition: Some(WaitingCondition::default()),
                        ..InvokeWaitForMethodResponse::default()
                    },
                )),
            })
        }
        poll_worker_task::Request::Execute(request) => {
            poll_worker_task_frame::Output::Execute(InvokeExecuteMethodOutput {
                output: Some(invoke_execute_method_output::Output::Result(handler(
                    &request,
                ))),
            })
        }
        poll_worker_task::Request::Rpc(_) => return None,
    };
    Some(PollWorkerTaskRequest {
        frame: Some(poll_worker_task_request::Frame::TaskFrame(
            PollWorkerTaskFrame {
                task_id: task.task_id,
                output: Some(output),
            },
        )),
    })
}
