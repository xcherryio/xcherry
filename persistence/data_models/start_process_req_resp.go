// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: Apache-2.0

package data_models

import (
	"github.com/xcherryio/apis/goapi/xcapi"
	"github.com/xcherryio/xcherry/common/uuid"
)

type (
	StartProcessRequest struct {
		Request                xcapi.ProcessExecutionStartRequest
		NewTaskShardId         int32
		TimeoutTimeUnixSeconds int64
	}

	StartProcessResponse struct {
		ProcessExecutionId         uuid.UUID
		AlreadyStarted             bool
		HasNewImmediateTask        bool
		FailedAtWritingAppDatabase bool
		AppDatabaseWritingError    error
	}
)
