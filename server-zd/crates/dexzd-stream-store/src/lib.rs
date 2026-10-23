// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! In-memory Stream store that serves WriteStream, ReadStream, and ListStreamMessages for server-zd.

mod stream_store_config;

pub use stream_store_config::StreamStoreConfig;

use dexzd_protocol::dex::Value;
use prost::Message as ProstMessage;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use thiserror::Error;
use tokio::sync::{Mutex as AsyncMutex, mpsc, watch};
use tokio::task::JoinHandle;

const ESTIMATED_MESSAGE_OVERHEAD_BYTES: u64 = 512;
const TRIM_BATCH_SIZE: usize = 256;
const TRIM_BATCH_PAUSE: Duration = Duration::from_millis(1);

/// Store errors map to gRPC codes below; read None maps to DeadlineExceeded/LONG_POLL_TIME_OUT.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum StreamStoreError {
    /// Disabled storage maps to FailedPrecondition.
    #[error("Stream Store is disabled")]
    Disabled,
    /// Invalid input maps to InvalidArgument.
    #[error("invalid Stream Store argument: {0}")]
    InvalidArgument(&'static str),
    /// Invalid startup configuration has no gRPC mapping.
    #[error("invalid Stream Store configuration: {0}")]
    InvalidConfig(&'static str),
    /// Oversized Values map to ResourceExhausted.
    #[error("serialized Stream Value exceeds configured max_message_bytes")]
    MessageTooLarge,
    /// Exhausted capacity maps to ResourceExhausted.
    #[error("Stream capacity is exhausted; retry after background trimming")]
    CapacityExceeded,
    /// Invalid tokens map to InvalidArgument.
    #[error("invalid Stream resume token")]
    InvalidResumeToken,
    /// Closed storage maps to Unavailable.
    #[error("Stream Store is closed")]
    Closed,
    /// Worker failure maps to Unavailable.
    #[error("Stream Store trim worker failed")]
    TrimWorkerFailed,
}

/// Inputs for one Stream append. API and Worker callers must pass the signed proto capacity.
pub struct WriteStreamInput {
    pub tenant_id: u64,
    pub flow_id: String,
    pub flow_type: String,
    pub stream_name: String,
    pub stream_capacity_bytes: Option<i64>,
    pub value: Value,
    pub source: String,
}

/// One retained Stream message, including a token for the next forward read or reverse page.
#[derive(Clone, Debug)]
pub struct StreamMessage {
    pub value: Value,
    pub resume_token: String,
    pub created_time: SystemTime,
    pub source: String,
}

/// A reverse page of retained messages. An empty next_page_token marks the last page.
pub struct StreamMessagePage {
    pub messages: Vec<StreamMessage>,
    pub next_page_token: String,
}

/// A process-local Stream Store. Call close before dropping it to drain its trim worker.
pub struct StreamStore {
    config: StreamStoreConfig,
    state: Arc<Mutex<StoreState>>,
    trim_sender: mpsc::UnboundedSender<TrimCommand>,
    trim_worker: AsyncMutex<Option<JoinHandle<()>>>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct ScopeKey {
    tenant_id: u64,
    flow_type: String,
    stream_name: String,
}

#[derive(Default)]
struct StoreState {
    scopes: HashMap<ScopeKey, StreamScope>,
    pending_trims: HashMap<ScopeKey, u64>,
    closed: bool,
}

#[derive(Default)]
struct StreamScope {
    total_bytes: u64,
    fifo: VecDeque<Arc<StoredMessage>>,
    instances: HashMap<String, StreamInstance>,
    last_milliseconds: u64,
    last_sequence: u64,
}

struct StreamInstance {
    messages: VecDeque<Arc<StoredMessage>>,
    version_sender: watch::Sender<u64>,
    waiters: usize,
}

struct StoredMessage {
    flow_id: String,
    value: Value,
    source: String,
    charged_bytes: u64,
    identity: (u64, u64),
}

enum TrimCommand {
    Scope(ScopeKey),
    Shutdown,
}

struct ReadWaiter {
    state: Arc<Mutex<StoreState>>,
    scope_key: ScopeKey,
    flow_id: String,
}

impl StreamStore {
    /// Starts the trim worker in the current Tokio runtime. Invalid configuration fails startup.
    pub fn new(config: &StreamStoreConfig) -> Result<Self, StreamStoreError> {
        config.validate().map_err(StreamStoreError::InvalidConfig)?;
        let state = Arc::new(Mutex::new(StoreState::default()));
        let (trim_sender, trim_receiver) = mpsc::unbounded_channel();
        let worker_state = Arc::clone(&state);
        let worker_sender = trim_sender.clone();
        let trim_worker = tokio::spawn(async move {
            run_trim_worker(worker_state, trim_receiver, worker_sender).await
        });
        Ok(Self {
            config: config.clone(),
            state,
            trim_sender,
            trim_worker: AsyncMutex::new(Some(trim_worker)),
        })
    }

    /// Appends a Value independently of Flow existence. Capacity is shared by tenant, Flow type, and Stream name.
    pub async fn write(&self, input: WriteStreamInput) -> Result<(), StreamStoreError> {
        validate_identity(&input.flow_id, &input.flow_type, &input.stream_name)?;
        if input.source.is_empty() {
            return Err(StreamStoreError::InvalidArgument("source is required"));
        }
        let capacity = match input.stream_capacity_bytes {
            Some(capacity) if capacity > 0 => capacity as u64,
            Some(_) => {
                return Err(StreamStoreError::InvalidArgument(
                    "stream_capacity_bytes must be positive",
                ));
            }
            None if self.config.capacity_per_stream_bytes > 0 => {
                self.config.capacity_per_stream_bytes
            }
            None => {
                return Err(StreamStoreError::InvalidArgument(
                    "stream_capacity_bytes is required",
                ));
            }
        };
        if !self.config.enabled {
            return Err(StreamStoreError::Disabled);
        }
        let value_bytes = input.value.encoded_len();
        if value_bytes > self.config.max_message_bytes {
            return Err(StreamStoreError::MessageTooLarge);
        }
        let charged_bytes = u64::try_from(value_bytes)
            .expect("a serialized Value size fits in u64")
            .checked_add(input.flow_id.len() as u64)
            .and_then(|bytes| bytes.checked_add(input.source.len() as u64))
            .and_then(|bytes| bytes.checked_add(ESTIMATED_MESSAGE_OVERHEAD_BYTES))
            .ok_or(StreamStoreError::MessageTooLarge)?;
        let scope_key = ScopeKey {
            tenant_id: input.tenant_id,
            flow_type: input.flow_type,
            stream_name: input.stream_name,
        };
        let mut state = self
            .state
            .lock()
            .expect("Stream Store state mutex is not poisoned");
        if state.closed {
            return Err(StreamStoreError::Closed);
        }
        let scope = state.scopes.entry(scope_key.clone()).or_default();
        let is_full = scope.total_bytes >= capacity
            || charged_bytes > capacity.saturating_sub(scope.total_bytes);
        if is_full {
            schedule_trim(
                &mut state,
                &self.trim_sender,
                scope_key,
                percentage_of(capacity, self.config.trim_target_percent),
            );
            return Err(StreamStoreError::CapacityExceeded);
        }
        let identity = scope.next_identity();
        let entry = Arc::new(StoredMessage {
            flow_id: input.flow_id.clone(),
            value: input.value,
            source: input.source,
            charged_bytes,
            identity,
        });
        scope.total_bytes += charged_bytes;
        scope.fifo.push_back(Arc::clone(&entry));
        let instance = scope.instance(&input.flow_id);
        instance.messages.push_back(entry);
        let next_version = instance.version_sender.borrow().wrapping_add(1);
        instance.version_sender.send_replace(next_version);
        let needs_trim =
            scope.total_bytes >= percentage_of(capacity, self.config.trim_trigger_percent);
        if needs_trim {
            let base_target = percentage_of(capacity, self.config.trim_target_percent);
            schedule_trim(
                &mut state,
                &self.trim_sender,
                scope_key,
                base_target.max(charged_bytes),
            );
        }
        Ok(())
    }

    /// Appends a Worker message with source #<StepExecutionID>; store failures are logged and do not fail the Step.
    pub async fn write_worker_best_effort(
        &self,
        mut input: WriteStreamInput,
        step_execution_id: &str,
    ) -> bool {
        if step_execution_id.is_empty() {
            tracing::warn!(
                flow_type = %input.flow_type,
                stream_name = %input.stream_name,
                source = "#",
                reason = "missing Step execution ID",
                "Worker Stream write was dropped"
            );
            return false;
        }
        input.source = format!("#{step_execution_id}");
        let flow_type = input.flow_type.clone();
        let stream_name = input.stream_name.clone();
        let source = input.source.clone();
        match self.write(input).await {
            Ok(()) => true,
            Err(error) => {
                tracing::warn!(%error, %flow_type, %stream_name, %source, "Worker Stream write was dropped");
                false
            }
        }
    }

    /// Reads the first retained message after a token. Returns None when the supplied deadline expires.
    pub async fn read(
        &self,
        tenant_id: u64,
        flow_type: &str,
        flow_id: &str,
        stream_name: &str,
        resume_token: &str,
        deadline: Instant,
    ) -> Result<Option<StreamMessage>, StreamStoreError> {
        validate_identity(flow_id, flow_type, stream_name)?;
        if !self.config.enabled {
            return Err(StreamStoreError::Disabled);
        }
        let scope_key = ScopeKey {
            tenant_id,
            flow_type: flow_type.to_owned(),
            stream_name: stream_name.to_owned(),
        };
        let cursor = decode_token(resume_token, &scope_key, flow_id)?.unwrap_or((0, 0));
        if deadline <= Instant::now() {
            return Ok(None);
        }
        loop {
            let mut receiver = {
                let mut state = self
                    .state
                    .lock()
                    .expect("Stream Store state mutex is not poisoned");
                if state.closed {
                    return Err(StreamStoreError::Closed);
                }
                let scope = state.scopes.entry(scope_key.clone()).or_default();
                let instance = scope.instance(flow_id);
                let next_index = instance
                    .messages
                    .partition_point(|entry| entry.identity <= cursor);
                if let Some(entry) = instance.messages.get(next_index) {
                    return Ok(Some(make_message(entry, &scope_key)));
                }
                instance.waiters += 1;
                instance.version_sender.subscribe()
            };
            let waiter = ReadWaiter {
                state: Arc::clone(&self.state),
                scope_key: scope_key.clone(),
                flow_id: flow_id.to_owned(),
            };
            let changed = tokio::time::timeout_at(deadline.into(), receiver.changed()).await;
            drop(waiter);
            match changed {
                Ok(Ok(())) => {}
                Ok(Err(_)) => return Err(StreamStoreError::Closed),
                Err(_) => return Ok(None),
            }
        }
    }

    /// Lists retained messages newest first, using an exclusive before-page token.
    pub async fn list(
        &self,
        tenant_id: u64,
        flow_type: &str,
        flow_id: &str,
        stream_name: &str,
        page_size: usize,
        before_page_token: &str,
    ) -> Result<StreamMessagePage, StreamStoreError> {
        validate_identity(flow_id, flow_type, stream_name)?;
        if !self.config.enabled {
            return Err(StreamStoreError::Disabled);
        }
        if page_size == 0 || page_size > self.config.max_read_messages {
            return Err(StreamStoreError::InvalidArgument(
                "page_size must be between 1 and max_read_messages",
            ));
        }
        let scope_key = ScopeKey {
            tenant_id,
            flow_type: flow_type.to_owned(),
            stream_name: stream_name.to_owned(),
        };
        let cursor = decode_token(before_page_token, &scope_key, flow_id)?;
        let state = self
            .state
            .lock()
            .expect("Stream Store state mutex is not poisoned");
        if state.closed {
            return Err(StreamStoreError::Closed);
        }
        let entries = state
            .scopes
            .get(&scope_key)
            .and_then(|scope| scope.instances.get(flow_id))
            .map(|instance| &instance.messages);
        let mut messages = Vec::with_capacity(page_size);
        let mut has_more = false;
        if let Some(entries) = entries {
            let end_index = cursor.map_or(entries.len(), |before| {
                entries.partition_point(|entry| entry.identity < before)
            });
            let start_index = end_index.saturating_sub(page_size);
            has_more = start_index > 0;
            for index in (start_index..end_index).rev() {
                messages.push(make_message(&entries[index], &scope_key));
            }
        }
        let next_page_token = if has_more {
            messages
                .last()
                .expect("a full page has a last message")
                .resume_token
                .clone()
        } else {
            String::new()
        };
        Ok(StreamMessagePage {
            messages,
            next_page_token,
        })
    }

    /// Stops reads and joins the background trim worker. Repeated calls are safe.
    pub async fn close(&self) -> Result<(), StreamStoreError> {
        {
            let mut state = self
                .state
                .lock()
                .expect("Stream Store state mutex is not poisoned");
            state.closed = true;
            for scope in state.scopes.values() {
                for instance in scope.instances.values() {
                    instance.version_sender.send_replace(u64::MAX);
                }
            }
        }
        if self.trim_sender.send(TrimCommand::Shutdown).is_err() {
            tracing::debug!("Stream Store trim worker had already stopped");
        }
        let mut trim_worker = self.trim_worker.lock().await;
        if let Some(worker_handle) = trim_worker.as_mut() {
            let join_result = worker_handle.await;
            *trim_worker = None;
            join_result.map_err(|_| StreamStoreError::TrimWorkerFailed)?;
        }
        Ok(())
    }
}

impl Drop for StreamStore {
    fn drop(&mut self) {
        if let Some(trim_worker) = self.trim_worker.get_mut().take() {
            trim_worker.abort();
        }
    }
}

impl Drop for ReadWaiter {
    fn drop(&mut self) {
        let mut state = self
            .state
            .lock()
            .expect("Stream Store state mutex is not poisoned");
        if let Some(scope) = state.scopes.get_mut(&self.scope_key) {
            if let Some(instance) = scope.instances.get_mut(&self.flow_id) {
                instance.waiters -= 1;
                if instance.waiters == 0 && instance.messages.is_empty() {
                    scope.instances.remove(&self.flow_id);
                }
            }
            if scope.fifo.is_empty() && scope.instances.is_empty() {
                state.scopes.remove(&self.scope_key);
            }
        }
    }
}

impl StreamScope {
    fn instance(&mut self, flow_id: &str) -> &mut StreamInstance {
        self.instances.entry(flow_id.to_owned()).or_insert_with(|| {
            let (version_sender, _) = watch::channel(0);
            StreamInstance {
                messages: VecDeque::new(),
                version_sender,
                waiters: 0,
            }
        })
    }

    fn next_identity(&mut self) -> (u64, u64) {
        let now_milliseconds = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("the system clock is after the Unix epoch")
            .as_millis() as u64;
        if now_milliseconds > self.last_milliseconds {
            self.last_milliseconds = now_milliseconds;
            self.last_sequence = 0;
        } else if self.last_sequence == u64::MAX {
            self.last_milliseconds += 1;
            self.last_sequence = 0;
        } else {
            self.last_sequence += 1;
        }
        (self.last_milliseconds, self.last_sequence)
    }

    fn trim_batch(&mut self, target_bytes: u64) -> bool {
        for _ in 0..TRIM_BATCH_SIZE {
            if self.total_bytes <= target_bytes {
                break;
            }
            let Some(entry) = self.fifo.pop_front() else {
                break;
            };
            let instance = self
                .instances
                .get_mut(&entry.flow_id)
                .expect("a retained Stream entry has its Flow instance");
            let removed = instance
                .messages
                .pop_front()
                .expect("a retained Stream entry is first in its Flow instance");
            assert!(
                Arc::ptr_eq(&entry, &removed),
                "Stream FIFO order is consistent"
            );
            if instance.messages.is_empty() && instance.waiters == 0 {
                self.instances.remove(&entry.flow_id);
            }
            self.total_bytes -= entry.charged_bytes;
        }
        self.total_bytes <= target_bytes || self.fifo.is_empty()
    }
}

async fn run_trim_worker(
    state: Arc<Mutex<StoreState>>,
    mut receiver: mpsc::UnboundedReceiver<TrimCommand>,
    sender: mpsc::UnboundedSender<TrimCommand>,
) {
    while let Some(command) = receiver.recv().await {
        let TrimCommand::Scope(scope_key) = command else {
            return;
        };
        let is_complete = {
            let mut state = state
                .lock()
                .expect("Stream Store state mutex is not poisoned");
            if state.closed {
                return;
            }
            let Some(target_bytes) = state.pending_trims.get(&scope_key).copied() else {
                continue;
            };
            let is_complete = state
                .scopes
                .get_mut(&scope_key)
                .is_none_or(|scope| scope.trim_batch(target_bytes));
            if is_complete {
                state.pending_trims.remove(&scope_key);
                if state
                    .scopes
                    .get(&scope_key)
                    .is_some_and(|scope| scope.fifo.is_empty() && scope.instances.is_empty())
                {
                    state.scopes.remove(&scope_key);
                }
            }
            is_complete
        };
        if !is_complete {
            tokio::time::sleep(TRIM_BATCH_PAUSE).await;
            if sender.send(TrimCommand::Scope(scope_key)).is_err() {
                tracing::warn!("Stream Store trim queue closed during trimming");
                return;
            }
        }
    }
}

fn schedule_trim(
    state: &mut StoreState,
    sender: &mpsc::UnboundedSender<TrimCommand>,
    scope_key: ScopeKey,
    target_bytes: u64,
) {
    if let Some(existing_target) = state.pending_trims.get_mut(&scope_key) {
        *existing_target = (*existing_target).min(target_bytes);
        return;
    }
    state.pending_trims.insert(scope_key.clone(), target_bytes);
    if sender.send(TrimCommand::Scope(scope_key)).is_err() {
        tracing::warn!("Stream Store trim worker is unavailable");
    }
}

fn percentage_of(capacity: u64, percentage: u8) -> u64 {
    capacity / 100 * u64::from(percentage) + capacity % 100 * u64::from(percentage) / 100
}

fn validate_identity(
    flow_id: &str,
    flow_type: &str,
    stream_name: &str,
) -> Result<(), StreamStoreError> {
    if flow_id.is_empty() || flow_type.is_empty() || stream_name.is_empty() {
        return Err(StreamStoreError::InvalidArgument(
            "flow_id, flow_type, and stream_name are required",
        ));
    }
    Ok(())
}

fn make_message(entry: &StoredMessage, scope_key: &ScopeKey) -> StreamMessage {
    StreamMessage {
        value: entry.value.clone(),
        resume_token: encode_token(scope_key, &entry.flow_id, entry.identity),
        created_time: UNIX_EPOCH + Duration::from_millis(entry.identity.0),
        source: entry.source.clone(),
    }
}

fn encode_token(scope_key: &ScopeKey, flow_id: &str, identity: (u64, u64)) -> String {
    let mut bytes = Vec::with_capacity(
        64 + flow_id.len() + scope_key.flow_type.len() + scope_key.stream_name.len(),
    );
    bytes.extend_from_slice(b"DST1");
    bytes.extend_from_slice(&scope_key.tenant_id.to_be_bytes());
    for value in [flow_id, &scope_key.flow_type, &scope_key.stream_name] {
        let length =
            u32::try_from(value.len()).expect("a Rust string length fits in a token field");
        bytes.extend_from_slice(&length.to_be_bytes());
        bytes.extend_from_slice(value.as_bytes());
    }
    bytes.extend_from_slice(&identity.0.to_be_bytes());
    bytes.extend_from_slice(&identity.1.to_be_bytes());
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut token = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        token.push(char::from(HEX[usize::from(byte >> 4)]));
        token.push(char::from(HEX[usize::from(byte & 15)]));
    }
    token
}

fn decode_token(
    token: &str,
    scope_key: &ScopeKey,
    flow_id: &str,
) -> Result<Option<(u64, u64)>, StreamStoreError> {
    if token.is_empty() {
        return Ok(None);
    }
    let invalid = StreamStoreError::InvalidResumeToken;
    let mut bytes = Vec::with_capacity(token.len() / 2);
    let chunks = token.as_bytes().chunks_exact(2);
    if !chunks.remainder().is_empty() {
        return Err(invalid);
    }
    for chunk in chunks {
        let high = hex_nibble(chunk[0]).ok_or(StreamStoreError::InvalidResumeToken)?;
        let low = hex_nibble(chunk[1]).ok_or(StreamStoreError::InvalidResumeToken)?;
        bytes.push(high << 4 | low);
    }
    let mut bytes = bytes.as_slice();
    if take_bytes(&mut bytes, 4) != Some(b"DST1".as_slice()) {
        return Err(invalid);
    }
    let tenant_id = take_u64(&mut bytes).ok_or(StreamStoreError::InvalidResumeToken)?;
    if tenant_id != scope_key.tenant_id {
        return Err(invalid);
    }
    for expected in [flow_id, &scope_key.flow_type, &scope_key.stream_name] {
        let length = take_u32(&mut bytes).ok_or(StreamStoreError::InvalidResumeToken)? as usize;
        if take_bytes(&mut bytes, length) != Some(expected.as_bytes()) {
            return Err(invalid);
        }
    }
    let milliseconds = take_u64(&mut bytes).ok_or(StreamStoreError::InvalidResumeToken)?;
    let sequence = take_u64(&mut bytes).ok_or(StreamStoreError::InvalidResumeToken)?;
    if !bytes.is_empty() {
        return Err(invalid);
    }
    Ok(Some((milliseconds, sequence)))
}

fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}

fn take_bytes<'a>(bytes: &mut &'a [u8], length: usize) -> Option<&'a [u8]> {
    if length > bytes.len() {
        return None;
    }
    let (taken, rest) = bytes.split_at(length);
    *bytes = rest;
    Some(taken)
}

fn take_u32(bytes: &mut &[u8]) -> Option<u32> {
    let raw: [u8; 4] = take_bytes(bytes, 4)?.try_into().ok()?;
    Some(u32::from_be_bytes(raw))
}

fn take_u64(bytes: &mut &[u8]) -> Option<u64> {
    let raw: [u8; 8] = take_bytes(bytes, 8)?.try_into().ok()?;
    Some(u64::from_be_bytes(raw))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "current_thread")]
    async fn cancelled_reader_releases_empty_scope() {
        let store = Arc::new(
            StreamStore::new(&StreamStoreConfig {
                enabled: true,
                ..StreamStoreConfig::default()
            })
            .unwrap(),
        );
        let reader_store = Arc::clone(&store);
        let reader = tokio::spawn(async move {
            reader_store
                .read(
                    0,
                    "OrderFlow",
                    "flow-a",
                    "Progress",
                    "",
                    Instant::now() + Duration::from_secs(10),
                )
                .await
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let has_waiter = store.state.lock().unwrap().scopes.values().any(|scope| {
                    scope
                        .instances
                        .values()
                        .any(|instance| instance.waiters == 1)
                });
                if has_waiter {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        reader.abort();
        assert!(reader.await.unwrap_err().is_cancelled());
        assert!(store.state.lock().unwrap().scopes.is_empty());
        store.close().await.unwrap();
    }
}
