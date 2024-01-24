// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: Apache-2.0

package data_models

import "github.com/xcherryio/xcherry/common/uuid"

type CompleteWaitUntilExecutionRequest struct {
	TaskShardId        int32
	ProcessExecutionId uuid.UUID
	StateExecutionId
	PreviousVersion int32
}
