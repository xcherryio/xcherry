// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: Apache-2.0

package data_models

import "github.com/xcherryio/xcherry/common/uuid"

type (
	ProcessLocalQueueMessagesRequest struct {
		TaskShardId        int32
		TaskSequence       int64
		ProcessExecutionId uuid.UUID
		Messages           []LocalQueueMessageInfoJson
	}

	ProcessLocalQueueMessagesResponse struct {
		HasNewImmediateTask bool
	}
)
