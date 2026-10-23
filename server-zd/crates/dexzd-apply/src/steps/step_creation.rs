// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use dexzd_protocol::dex::{StepOptions, Value};
use dexzd_protocol::dexzd::v1::{StepExecutionPhase, StepExecutionRecord, StepMethod};

use crate::ApplyError;
use crate::command_handlers::run_cursor::RunCursor;
use crate::state_access::FlowTransaction;
use crate::steps::{
    TIMEOUT_HANDLER_STEP_EXECUTION_ID, TIMEOUT_HANDLER_STEP_TYPE, make_invocation_pending,
};
use crate::values::{ValueHolder, ValueRejection, store_value};

/// A Step execution to create.
pub(crate) struct NewStepExecution<'input> {
    pub(crate) step_type: &'input str,
    /// Complete value, or a reference to a value of this run that a Worker echoed back.
    pub(crate) input: Option<&'input Value>,
    pub(crate) options: Option<&'input StepOptions>,
    pub(crate) from_step_execution_id: &'input str,
    pub(crate) is_timeout_handler: bool,
}

/// Creates a Step execution with the next `StepType-N` and makes its first method pending
/// (WaitFor, or Execute when `skip_wait_for` is set).
///
/// Returns the Step execution ID, or the input value the caller must reject.
pub(crate) fn create_step_execution(
    transaction: &mut FlowTransaction<'_>,
    cursor: &mut RunCursor,
    now_micros: i64,
    new_step: &NewStepExecution<'_>,
) -> Result<Result<String, ValueRejection>, ApplyError> {
    let (step_execution_id, step_number) = if new_step.is_timeout_handler {
        (TIMEOUT_HANDLER_STEP_EXECUTION_ID.to_owned(), 1)
    } else {
        let mut counter = cursor.rows.step_counter(transaction, new_step.step_type)?;
        counter.last_step_number += 1;
        cursor
            .rows
            .put_step_counter(transaction, new_step.step_type, &counter);
        (
            format!("{}-{}", new_step.step_type, counter.last_step_number),
            counter.last_step_number,
        )
    };
    let input = match new_step.input {
        Some(value) => {
            let holder = ValueHolder::StepInput {
                step_execution_id: step_execution_id.clone(),
            };
            let limits = cursor.value_limits();
            match store_value(
                transaction,
                cursor.rows,
                limits,
                &mut cursor.values,
                value,
                &holder,
            )? {
                Ok(stored) => Some(stored),
                Err(rejection) => return Ok(Err(rejection)),
            }
        }
        None => None,
    };
    let options = new_step.options.cloned().unwrap_or_default();
    let is_wait_for_skipped = options.skip_wait_for || new_step.is_timeout_handler;
    let mut step = StepExecutionRecord {
        step_execution_id: step_execution_id.clone(),
        step_type: if new_step.is_timeout_handler {
            TIMEOUT_HANDLER_STEP_TYPE.to_owned()
        } else {
            new_step.step_type.to_owned()
        },
        step_number,
        input,
        options: Some(options),
        from_step_execution_id: new_step.from_step_execution_id.to_owned(),
        phase: if is_wait_for_skipped {
            StepExecutionPhase::Execute as i32
        } else {
            StepExecutionPhase::WaitFor as i32
        },
        created_seq: cursor.seq,
        created_at_micros: now_micros,
        is_timeout_handler: new_step.is_timeout_handler,
        ..StepExecutionRecord::default()
    };
    // The timeout handler waits for its timer before Execute; the timer arms it later.
    if !new_step.is_timeout_handler {
        let method = if is_wait_for_skipped {
            StepMethod::Execute
        } else {
            StepMethod::WaitFor
        };
        make_invocation_pending(transaction, cursor, &mut step, method, 1, now_micros)?;
    }
    cursor.rows.put_step_execution(transaction, &step);
    Ok(Ok(step_execution_id))
}
