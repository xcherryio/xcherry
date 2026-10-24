// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! The object store emulator, serve processes, supervised restarts, and failure reporting of
//! robustness tests.

use std::fs;
use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{ExitStatus, Stdio};
use std::sync::Arc;
use std::time::Duration;

use s3mock::{Durability, EtagMode, Store, StoreConfig};
use s3mock_server::{ServerConfig, ServerHandle, SignatureVerification};
use tempfile::TempDir;
use tokio::process::{Child, Command};
use tonic::transport::Channel;
use tonic_health::pb::HealthCheckRequest;
use tonic_health::pb::health_check_response::ServingStatus;
use tonic_health::pb::health_client::HealthClient;

use super::workload::{CompletedChain, VerificationReport, Workload, WorkloadWorker};
use crate::harness;
use crate::{NODE_ID, PRIMARY_BUCKET, PRIMARY_PREFIX};

const LOG_TAIL_LINES: usize = 150;
const READINESS_SERVICE_NAMES: [&str; 3] = ["", "dex.FlowService", "dex.WorkerTaskService"];
const STARTUP_BUDGET: Duration = Duration::from_secs(30);
/// Exceeds the server's 14.5 s shutdown total, with slack for a loaded machine.
pub(crate) const PROCESS_SHUTDOWN_BUDGET: Duration = Duration::from_secs(20);
/// Bounds one readiness probe; a process binds its port before it holds the lease.
const READINESS_PROBE_TIMEOUT: Duration = Duration::from_secs(2);
const READINESS_POLL_INTERVAL: Duration = Duration::from_millis(50);
/// Bounds the wait for the kernel to report a process stopped after SIGSTOP.
const STOP_BUDGET: Duration = Duration::from_secs(5);
const STOP_POLL_INTERVAL: Duration = Duration::from_millis(5);
/// A new epoch found a batch that a killed process wrote past the recovery gap.
const STALE_BATCH_STOP_REASON: &str =
    "an unacknowledged batch of an earlier epoch holds the next seq; recovery voids it";

/// An s3mock server in this process with the primary bucket and fsync-durable writes.
///
/// It skips signature checks, because unoptimized SigV4 checks slow every request.
pub(crate) struct DurableEmulator {
    server: ServerHandle,
    _directory: TempDir,
}

impl DurableEmulator {
    pub(crate) async fn start() -> Self {
        let directory = TempDir::new().expect("a temporary emulator directory can be created");
        let store = Store::open(&StoreConfig {
            root_directory: directory.path().to_path_buf(),
            durability: Durability::Strict,
            etag_mode: EtagMode::Md5,
        })
        .expect("the emulator store opens");
        store
            .create_bucket(PRIMARY_BUCKET, false)
            .expect("the emulator bucket is created");
        let listener = tokio::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .await
            .expect("the emulator binds a loopback port");
        let server = s3mock_server::start_with_config(
            Arc::new(store),
            listener,
            ServerConfig {
                data_directory: directory.path().to_path_buf(),
                signature_verification: SignatureVerification::Off,
                ..ServerConfig::default()
            },
        )
        .expect("the emulator starts");
        Self {
            server,
            _directory: directory,
        }
    }

    pub(crate) fn endpoint(&self) -> String {
        format!("http://{}", self.server.local_address())
    }

    pub(crate) async fn shutdown(self) {
        self.server.shutdown().await;
    }
}

/// A `dex-server-zd serve` child process that tests can kill or signal; dropping it kills it.
pub(crate) struct RobustnessProcess {
    child: Child,
    address: SocketAddr,
    log_path: PathBuf,
}

impl RobustnessProcess {
    /// Starts `serve --config` with environment credentials for the primary store.
    pub(crate) fn spawn(config_path: &Path, address: SocketAddr, log_path: PathBuf) -> Self {
        let log = fs::File::create(&log_path).expect("the process log file is created");
        let child = Command::new(env!("CARGO_BIN_EXE_dex-server-zd"))
            .arg("serve")
            .arg("--config")
            .arg(config_path)
            .env("DEX_ZD_OBJECT_STORE_ACCESS_KEY_ID", harness::ACCESS_KEY_ID)
            .env(
                "DEX_ZD_OBJECT_STORE_SECRET_ACCESS_KEY",
                harness::SECRET_ACCESS_KEY,
            )
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
        match self.wait_until_serving_or_exit().await {
            Ok(channel) => channel,
            Err(status) => panic!(
                "dex-server-zd exited with {status} before serving:\n{}",
                self.log()
            ),
        }
    }

    /// Like [`Self::wait_until_serving`], but returns the exit status if the process exits first.
    pub(crate) async fn wait_until_serving_or_exit(&mut self) -> Result<Channel, ExitStatus> {
        let deadline = tokio::time::Instant::now() + STARTUP_BUDGET;
        loop {
            if let Some(status) = self.exit_status() {
                return Err(status);
            }
            if let Some(channel) = self.serving_channel().await {
                return Ok(channel);
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "dex-server-zd did not serve within {STARTUP_BUDGET:?}:\n{}",
                self.log()
            );
            tokio::time::sleep(READINESS_POLL_INTERVAL).await;
        }
    }

    async fn serving_channel(&self) -> Option<Channel> {
        let channel = tokio::time::timeout(
            READINESS_PROBE_TIMEOUT,
            Channel::from_shared(format!("http://{}", self.address))
                .expect("the server address is a valid URI")
                .connect(),
        )
        .await
        .ok()?
        .ok()?;
        let mut health = HealthClient::new(channel.clone());
        for service in READINESS_SERVICE_NAMES {
            let response = tokio::time::timeout(
                READINESS_PROBE_TIMEOUT,
                health.check(HealthCheckRequest {
                    service: service.to_owned(),
                }),
            )
            .await
            .ok()?
            .ok()?;
            if response.get_ref().status != ServingStatus::Serving as i32 {
                return None;
            }
        }
        Some(channel)
    }

    /// Sends SIGTERM and waits for the process to exit.
    pub(crate) async fn terminate(&mut self, budget: Duration) -> ExitStatus {
        self.send_signal("TERM").await;
        self.wait_for_exit(budget).await
    }

    /// Sends the named signal, such as `STOP` or `CONT`, without waiting for any effect.
    pub(crate) async fn send_signal(&self, signal_name: &str) {
        let process_id = self.child.id().expect("the process is still running");
        let signal_status = Command::new("kill")
            .arg(format!("-{signal_name}"))
            .arg(process_id.to_string())
            .status()
            .await
            .expect("kill runs");
        assert!(
            signal_status.success(),
            "kill -{signal_name} {process_id} failed"
        );
    }

    /// Sends SIGSTOP and waits until the kernel reports the process stopped, so it runs no code.
    pub(crate) async fn freeze_and_wait_until_stopped(&mut self) {
        self.send_signal("STOP").await;
        let process_id = self.child.id().expect("the process is still running");
        let deadline = tokio::time::Instant::now() + STOP_BUDGET;
        while !is_stopped(process_id).await {
            if let Some(status) = self.exit_status() {
                panic!(
                    "dex-server-zd exited with {status} instead of stopping:\n{}",
                    self.log()
                );
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "dex-server-zd did not stop within {STOP_BUDGET:?} of SIGSTOP:\n{}",
                self.log()
            );
            tokio::time::sleep(STOP_POLL_INTERVAL).await;
        }
    }

    /// Sends SIGKILL and waits until the process is reaped, so its files and port are free.
    pub(crate) async fn kill(&mut self) -> ExitStatus {
        self.child.kill().await.expect("SIGKILL is delivered");
        self.child
            .wait()
            .await
            .expect("the process exit status is readable")
    }

    /// Returns the exit status if the process has already exited.
    pub(crate) fn exit_status(&mut self) -> Option<ExitStatus> {
        self.child
            .try_wait()
            .expect("the process status is readable")
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

/// The `serve` settings that robustness tests vary; a waiting process never fails on a held lease.
pub(crate) struct RobustnessServeConfig<'a> {
    pub(crate) primary_endpoint: &'a str,
    pub(crate) listen: SocketAddr,
    pub(crate) local_directory: &'a Path,
    pub(crate) lease_duration: &'a str,
    pub(crate) snapshot_interval: &'a str,
    pub(crate) log_deletion_delay: &'a str,
}

impl RobustnessServeConfig<'_> {
    /// Writes the YAML file that `serve --config` reads.
    pub(crate) fn write(&self, path: &Path) {
        let contents = format!(
            "cluster:\n  nodeId: {NODE_ID}\n  partitionCount: 16\nobjectStore:\n  endpoint: {endpoint}\n  region: us-east-1\n  bucket: {PRIMARY_BUCKET}\n  prefix: {PRIMARY_PREFIX}\n  forcePathStyle: true\n  credentialsSource: environment\nlocalStorage:\n  directory: {directory}\n  blockCacheBytes: 16777216\nlease:\n  duration: {lease}\n  failIfHeld: false\nsnapshot:\n  interval: {snapshot_interval}\n  retainedCount: 2\n  logDeletionDelay: {log_deletion_delay}\napi:\n  listen: \"{listen}\"\nsearch:\n  backend: builtin\n",
            endpoint = self.primary_endpoint,
            directory = self.local_directory.display(),
            lease = self.lease_duration,
            snapshot_interval = self.snapshot_interval,
            log_deletion_delay = self.log_deletion_delay,
            listen = self.listen,
        );
        fs::write(path, contents).expect("the serve configuration is written");
    }
}

/// Spawns every serve process of one test; if the test panics, prints their logs and `summary`.
pub(crate) struct ServeFleet {
    log_directory: PathBuf,
    log_paths: Vec<PathBuf>,
    summary: Box<dyn Fn() -> String + Send + Sync>,
}

impl ServeFleet {
    pub(crate) fn new(
        log_directory: &Path,
        summary: Box<dyn Fn() -> String + Send + Sync>,
    ) -> Self {
        Self {
            log_directory: log_directory.to_path_buf(),
            log_paths: Vec::new(),
            summary,
        }
    }

    /// Starts `serve --config` with its log in `<name>.log`.
    pub(crate) fn spawn(
        &mut self,
        config_path: &Path,
        address: SocketAddr,
        name: &str,
    ) -> RobustnessProcess {
        let log_path = self.log_directory.join(format!("{name}.log"));
        self.log_paths.push(log_path.clone());
        RobustnessProcess::spawn(config_path, address, log_path)
    }
}

impl Drop for ServeFleet {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            return;
        }
        eprintln!("robustness workload at failure:\n{}", (self.summary)());
        for path in &self.log_paths {
            let log = fs::read_to_string(path)
                .unwrap_or_else(|error| format!("<cannot read {}: {error}>", path.display()));
            let lines: Vec<&str> = log.lines().collect();
            let tail = &lines[lines.len().saturating_sub(LOG_TAIL_LINES)..];
            eprintln!(
                "--- {} (last {} of {} lines) ---\n{}",
                path.display(),
                tail.len(),
                lines.len(),
                tail.join("\n")
            );
        }
    }
}

/// A serving process with a connected Worker.
struct Incarnation {
    process: RobustnessProcess,
    channel: Channel,
    worker: WorkloadWorker,
}

/// A serve process restarted on one configuration, allowing one stale-batch stop after each SIGKILL.
pub(crate) struct SupervisedNode {
    config_path: PathBuf,
    address: SocketAddr,
    name: String,
    incarnation_count: usize,
    stale_batch_restart_count: usize,
    incarnation: Option<Incarnation>,
    may_stop_on_stale_batch: bool,
    must_void_stale_batch: bool,
}

impl SupervisedNode {
    /// Starts the first incarnation; `follows_kill` means a killed process last wrote the log.
    pub(crate) async fn start(
        fleet: &mut ServeFleet,
        workload: &Workload,
        config_path: &Path,
        address: SocketAddr,
        name: &str,
        follows_kill: bool,
    ) -> Self {
        let mut node = Self {
            config_path: config_path.to_path_buf(),
            address,
            name: name.to_owned(),
            incarnation_count: 0,
            stale_batch_restart_count: 0,
            incarnation: None,
            may_stop_on_stale_batch: follows_kill,
            must_void_stale_batch: false,
        };
        node.launch(fleet, workload).await;
        node
    }

    pub(crate) fn channel(&self) -> Channel {
        self.current().channel.clone()
    }

    /// Returns the log file name of incarnation `number`, counted from 1.
    pub(crate) fn incarnation_log_name(&self, number: usize) -> String {
        format!("{}-{number}.log", self.name)
    }

    pub(crate) fn stale_batch_restart_count(&self) -> usize {
        self.stale_batch_restart_count
    }

    /// SIGKILLs the process, then starts the next incarnation on the same directory.
    pub(crate) async fn kill_and_restart(&mut self, fleet: &mut ServeFleet, workload: &Workload) {
        let mut killed = self
            .incarnation
            .take()
            .expect("a supervised node has an incarnation between calls");
        killed.process.kill().await;
        killed.worker.stop().await;
        self.may_stop_on_stale_batch = true;
        self.launch(fleet, workload).await;
    }

    /// Waits for `count` more acknowledged calls, restarting a permitted stale-batch stop.
    pub(crate) async fn wait_for_new_acknowledgements(
        &mut self,
        fleet: &mut ServeFleet,
        workload: &Workload,
        count: u64,
    ) {
        let target = workload.progress_target(count);
        loop {
            let incarnation = self
                .incarnation
                .as_mut()
                .expect("a supervised node has an incarnation between calls");
            if workload
                .wait_for_progress_or_exit(&target, &mut incarnation.process)
                .await
                .is_ok()
            {
                return;
            }
            let stopped = self
                .incarnation
                .take()
                .expect("a supervised node has an incarnation between calls");
            stopped.worker.stop().await;
            self.accept_stale_batch_stop(stopped.process);
            self.launch(fleet, workload).await;
        }
    }

    /// Verifies `workload` on the current incarnation; panics if that incarnation exits meanwhile.
    pub(crate) async fn verify(&mut self, workload: &Workload) -> VerificationReport {
        let incarnation = self
            .incarnation
            .as_mut()
            .expect("a supervised node has an incarnation between calls");
        workload
            .verify(incarnation.channel.clone(), &mut incarnation.process)
            .await
    }

    /// Panics unless `chain` is still Completed on the current incarnation.
    pub(crate) async fn verify_completed_chain(
        &mut self,
        workload: &Workload,
        chain: &CompletedChain,
    ) {
        let incarnation = self
            .incarnation
            .as_mut()
            .expect("a supervised node has an incarnation between calls");
        workload
            .verify_completed_chain(incarnation.channel.clone(), &mut incarnation.process, chain)
            .await;
    }

    /// Sends SIGTERM and requires a clean exit.
    pub(crate) async fn terminate(mut self) {
        let mut incarnation = self
            .incarnation
            .take()
            .expect("a supervised node has an incarnation between calls");
        let status = incarnation.process.terminate(PROCESS_SHUTDOWN_BUDGET).await;
        assert_eq!(status.code(), Some(0), "{}", incarnation.process.log());
        incarnation.worker.stop().await;
    }

    /// Starts incarnations until one serves with a Worker, allowing one stale-batch stop.
    async fn launch(&mut self, fleet: &mut ServeFleet, workload: &Workload) {
        loop {
            self.incarnation_count += 1;
            let name = format!("{}-{}", self.name, self.incarnation_count);
            let mut process = fleet.spawn(&self.config_path, self.address, &name);
            let Ok(channel) = process.wait_until_serving_or_exit().await else {
                self.accept_stale_batch_stop(process);
                continue;
            };
            match workload.try_start_worker(channel.clone()).await {
                Ok(worker) => {
                    if self.must_void_stale_batch {
                        assert_replay_voided_a_batch(&process);
                        self.must_void_stale_batch = false;
                    }
                    self.incarnation = Some(Incarnation {
                        process,
                        channel,
                        worker,
                    });
                    return;
                }
                Err(error) => {
                    // A serving process rejects a Worker stream only while it stops.
                    process.wait_for_exit(PROCESS_SHUTDOWN_BUDGET).await;
                    eprintln!("{name}: the Worker did not register before the exit: {error}");
                    self.accept_stale_batch_stop(process);
                }
            }
        }
    }

    /// Panics unless `process` stopped on a stale batch and this kill has not used its restart.
    fn accept_stale_batch_stop(&mut self, mut process: RobustnessProcess) {
        let status = process
            .exit_status()
            .expect("only an exited process is accepted");
        let log = process.log();
        assert!(
            self.may_stop_on_stale_batch
                && status.code() == Some(1)
                && log.contains(STALE_BATCH_STOP_REASON),
            "{} incarnation {} exited with {status}:\n{log}",
            self.name,
            self.incarnation_count
        );
        eprintln!(
            "supervisor: {} incarnation {} stopped on a stale log batch of the killed process; restarting it once",
            self.name, self.incarnation_count
        );
        self.may_stop_on_stale_batch = false;
        self.must_void_stale_batch = true;
        self.stale_batch_restart_count += 1;
    }

    fn current(&self) -> &Incarnation {
        self.incarnation
            .as_ref()
            .expect("a supervised node has an incarnation between calls")
    }
}

/// Returns whether the kernel reports `process_id` stopped by a signal.
async fn is_stopped(process_id: u32) -> bool {
    if cfg!(target_os = "linux") {
        are_all_linux_threads_stopped(process_id)
    } else if cfg!(target_os = "macos") {
        is_macos_process_stopped(process_id).await
    } else {
        panic!("cannot observe a stopped process on this platform");
    }
}

/// The thread group leader can stop while another thread still runs, so every thread must report `T`.
fn are_all_linux_threads_stopped(process_id: u32) -> bool {
    let task_directory = PathBuf::from(format!("/proc/{process_id}/task"));
    let threads = fs::read_dir(&task_directory)
        .unwrap_or_else(|error| panic!("cannot list {}: {error}", task_directory.display()));
    for thread in threads {
        let stat_path = thread
            .unwrap_or_else(|error| panic!("cannot list {}: {error}", task_directory.display()))
            .path()
            .join("stat");
        let stat = match fs::read_to_string(&stat_path) {
            Ok(stat) => stat,
            // A thread that exited runs no code.
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => panic!("cannot read {}: {error}", stat_path.display()),
        };
        // The thread name may contain `)`, so the state follows the last one.
        let state = stat
            .rsplit_once(')')
            .and_then(|(_, fields)| fields.trim_start().chars().next())
            .unwrap_or_else(|| panic!("{} has no state field: {stat}", stat_path.display()));
        if state != 'T' {
            return false;
        }
    }
    true
}

/// `ps -M` shows suspended threads as sleeping, so only the process state shows the stop.
async fn is_macos_process_stopped(process_id: u32) -> bool {
    let output = Command::new("ps")
        .args(["-o", "state=", "-p"])
        .arg(process_id.to_string())
        .output()
        .await
        .expect("ps runs");
    assert!(
        output.status.success(),
        "ps -o state= -p {process_id} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout)
        .trim_start()
        .starts_with('T')
}

/// Requires the recovery of `process` to have voided at least one earlier epoch's batch.
fn assert_replay_voided_a_batch(process: &RobustnessProcess) {
    let log = process.log();
    let voided_batches = replay_field(&log, "voided_batches");
    assert!(
        voided_batches >= 1,
        "the restart after a stale-batch stop must void that batch:\n{log}"
    );
}

/// Returns the numeric `field` of the recovery's log replay line in `log`.
pub(crate) fn replay_field(log: &str, field: &str) -> u64 {
    let line = log
        .lines()
        .find(|line| line.contains("replayed the log stream"))
        .unwrap_or_else(|| panic!("the process logged no log replay:\n{log}"));
    let prefix = format!("{field}=");
    line.split_whitespace()
        .find_map(|token| token.strip_prefix(prefix.as_str()))
        .and_then(|value| value.parse().ok())
        .unwrap_or_else(|| panic!("the replay line has no numeric {field}: {line}"))
}
