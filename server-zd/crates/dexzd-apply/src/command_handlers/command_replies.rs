// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

use dexzd_protocol::dex::ErrorSubStatus;
use dexzd_protocol::dexzd::v1::{CommandErrorReply, CommandReply, EmptyReply, command_reply};

/// gRPC status codes used in replies.
pub(crate) mod grpc_code {
    pub(crate) const INVALID_ARGUMENT: i32 = 3;
    pub(crate) const NOT_FOUND: i32 = 5;
    pub(crate) const ALREADY_EXISTS: i32 = 6;
    pub(crate) const RESOURCE_EXHAUSTED: i32 = 8;
    pub(crate) const FAILED_PRECONDITION: i32 = 9;
    pub(crate) const ABORTED: i32 = 10;
}

pub(crate) fn ok_reply() -> CommandReply {
    CommandReply {
        reply: Some(command_reply::Reply::Ok(EmptyReply {})),
    }
}

pub(crate) fn error_reply(
    grpc_code: i32,
    sub_status: ErrorSubStatus,
    message: impl Into<String>,
) -> CommandReply {
    CommandReply {
        reply: Some(command_reply::Reply::Error(CommandErrorReply {
            grpc_code,
            sub_status: sub_status as i32,
            message: message.into(),
        })),
    }
}
