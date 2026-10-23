// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Runtime that proposes, commits, and applies server-zd commands and runs their effects: dispatch, timers, deliveries, and recovery.

mod effects;

pub use effects::{
    EffectDependencies, EffectError, EffectFlow, EffectRunner, EffectRuntime, EffectRuntimeError,
    EffectTasks, EffectTasksHandle, EffectUrgency, EffectsConfig, HARD_DELETE_OBJECT_KEY_PREFIX,
    HardDeleteObjectOwner, HardDeleteObjects, HardDeleteUploadLedger, HardDeleteUploads,
    InvocationStartOutcome, MethodDispatchRequest, MethodDispatchSession, MethodDispatchStartError,
    MethodDispatcher, MethodRequest, NodeMetaUploadLedger, decode_hard_delete_value,
    encode_hard_delete_value, hard_delete_placeholder_value,
};
