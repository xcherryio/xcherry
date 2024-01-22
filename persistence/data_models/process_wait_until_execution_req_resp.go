// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: Apache-2.0

package data_models

import (
	"github.com/xcherryio/apis/goapi/xcapi"
	"github.com/xcherryio/xcherry/common/uuid"
)

type (
	ProcessWaitUntilExecutionRequest struct {
		ProcessExecutionId uuid.UUID
		StateExecutionId

		Prepare             PrepareStateExecutionResponse
		CommandRequest      xcapi.CommandRequest
		PublishToLocalQueue []xcapi.LocalQueueMessage
		TaskShardId         int32
		TaskSequence        int64
	}

	ProcessWaitUntilExecutionResponse struct {
		HasNewImmediateTask bool
		FireTimestamps      []int64
	}
)
