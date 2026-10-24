// Portions of this file are derived from indeedeng/iwf-java-sdk.
// Those portions are licensed under the Apache License, Version 2.0.
// See LICENSES/Apache-2.0.txt and LEGACY_NOTICES.md.
//
// Modifications Copyright (c) 2026 Super Durable, Inc.
//
// Modifications are licensed under the Sustainable Use License 1.0.
// Third-Party Materials remain under the Apache License, Version 2.0.
// See LICENSE and LEGACY_NOTICES.md.

use std::sync::LazyLock;
use std::thread;
use std::time::Duration;

use dex_sdk::{
    Context, Flow, HandlerError, HandlerResult, PersistenceSchema, RetryPolicy, Step, StepDecision,
    StepDurability, StepList, StepOptions, Stream,
};

use crate::support::{is_pull_mode, is_server_zd};

pub(crate) static HEARTBEAT_PROGRESS: LazyLock<Stream<String>> =
    LazyLock::new(|| Stream::new("rust-heartbeat-progress", 1 << 20));

#[derive(Clone)]
pub(crate) struct HeartbeatRecoveryWorkflow {
    sync: SyncHeartbeatStep,
    asynchronous: AsyncHeartbeatStep,
}

impl HeartbeatRecoveryWorkflow {
    pub(crate) fn new() -> Self {
        Self {
            sync: SyncHeartbeatStep,
            asynchronous: AsyncHeartbeatStep,
        }
    }
}

impl Flow for HeartbeatRecoveryWorkflow {
    type StartInput = ();

    fn steps(&self) -> StepList<'_, Self::StartInput> {
        StepList::start(&self.sync).and(&self.asynchronous)
    }

    fn persistence(&self) -> PersistenceSchema {
        PersistenceSchema::new().stream(&HEARTBEAT_PROGRESS)
    }
}

#[derive(Clone)]
struct SyncHeartbeatStep;

impl Step for SyncHeartbeatStep {
    type Input = ();

    fn execute(&self, context: &mut Context, _input: ()) -> HandlerResult<StepDecision> {
        match context.attempt() {
            1 => {
                require_last_heartbeat(context, None)?;
                context.record_heartbeat_value(Some("sync-checkpoint".to_string()))?;
                HEARTBEAT_PROGRESS.write(context, "sync-progress".to_string())?;
                Err(retry_error("persist typed heartbeat"))
            }
            2 => {
                require_last_heartbeat(context, Some(Some("sync-checkpoint".to_string())))?;
                context.record_heartbeat()?;
                Err(retry_error("clear heartbeat"))
            }
            3 => {
                require_last_heartbeat(context, None)?;
                context.record_heartbeat_value(None::<String>)?;
                Err(retry_error("persist null heartbeat"))
            }
            4 => {
                require_last_heartbeat(context, Some(None))?;
                Ok(StepDecision::go_to(&AsyncHeartbeatStep, ()))
            }
            attempt => Err(HandlerError::new(
                "HeartbeatRecoveryFailure",
                format!("unexpected sync attempt {attempt}"),
            )),
        }
    }

    fn options(&self) -> StepOptions<Self::Input> {
        retry_options(StepDurability::Sync)
    }
}

#[derive(Clone)]
struct AsyncHeartbeatStep;

impl Step for AsyncHeartbeatStep {
    type Input = ();

    fn execute(&self, context: &mut Context, _input: ()) -> HandlerResult<StepDecision> {
        // C2: server-zd persists ASYNC heartbeat checkpoints from the first attempt.
        let expected_heartbeat = if (is_server_zd() && context.attempt() > 1)
            || (is_pull_mode() && context.attempt() > 2)
        {
            Some(Some(format!("local-{}", context.attempt() - 1)))
        } else {
            None
        };
        if context.attempt() <= 3 {
            require_last_heartbeat(context, expected_heartbeat)?;
            context.record_heartbeat_value(format!("local-{}", context.attempt()))?;
            HEARTBEAT_PROGRESS.write(context, format!("local-{}", context.attempt()))?;
            return Err(retry_error("exercise local retry"));
        }
        require_last_heartbeat(context, expected_heartbeat)?;
        Ok(StepDecision::graceful_complete("heartbeat-ok".to_string()))
    }

    fn options(&self) -> StepOptions<Self::Input> {
        retry_options(StepDurability::Async)
    }
}

fn retry_options(durability: StepDurability) -> StepOptions<()> {
    StepOptions::new()
        .execute_durability(durability)
        .heartbeat_timeout(Duration::from_secs(10))
        .execute_retry(
            RetryPolicy::new()
                .initial_interval(Duration::from_secs(1))
                .maximum_attempts(4)
                .total_duration(Duration::from_secs(30)),
        )
}

fn require_last_heartbeat(
    context: &Context,
    expected: Option<Option<String>>,
) -> HandlerResult<()> {
    let actual = context.last_heartbeat_value::<Option<String>>()?;
    if actual == expected {
        return Ok(());
    }
    Err(HandlerError::new(
        "HeartbeatRecoveryFailure",
        format!("expected heartbeat {expected:?}, got {actual:?}"),
    ))
}

fn retry_error(message: &str) -> HandlerError {
    HandlerError::new("HeartbeatRecoveryRetry", message)
}

#[derive(Clone)]
pub(crate) struct NoOutputHeartbeatTimeoutWorkflow {
    start: NoOutputHeartbeatStep,
    recovery: HeartbeatTimeoutRecovery,
}

impl NoOutputHeartbeatTimeoutWorkflow {
    pub(crate) fn new() -> Self {
        Self {
            start: NoOutputHeartbeatStep,
            recovery: HeartbeatTimeoutRecovery,
        }
    }
}

impl Flow for NoOutputHeartbeatTimeoutWorkflow {
    type StartInput = ();

    fn steps(&self) -> StepList<'_, Self::StartInput> {
        StepList::start(&self.start).and(&self.recovery)
    }
}

#[derive(Clone)]
struct NoOutputHeartbeatStep;

impl Step for NoOutputHeartbeatStep {
    type Input = ();

    fn execute(&self, _context: &mut Context, _input: ()) -> HandlerResult<StepDecision> {
        thread::sleep(Duration::from_secs(12));
        Ok(StepDecision::graceful_complete("late-result".to_string()))
    }

    fn options(&self) -> StepOptions<Self::Input> {
        StepOptions::new()
            .execute_durability(StepDurability::Sync)
            .execute_method_timeout(Duration::from_secs(20))
            .heartbeat_timeout(Duration::from_secs(10))
            .execute_retry(RetryPolicy::new().maximum_attempts(1))
            .on_execute_failure_proceed_to(&HeartbeatTimeoutRecovery)
    }
}

#[derive(Clone)]
struct HeartbeatTimeoutRecovery;

impl Step for HeartbeatTimeoutRecovery {
    type Input = ();

    fn execute(&self, _context: &mut Context, _input: ()) -> HandlerResult<StepDecision> {
        Ok(StepDecision::graceful_complete(
            "heartbeat-timeout".to_string(),
        ))
    }
}
