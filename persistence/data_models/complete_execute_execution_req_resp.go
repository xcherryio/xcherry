// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: Apache-2.0

package data_models

import (
	"github.com/xcherryio/apis/goapi/xcapi"
	"github.com/xcherryio/xcherry/common/uuid"
)

type (
	CompleteExecuteExecutionRequest struct {
		ProcessExecutionId uuid.UUID
		StateExecutionId

		Prepare             PrepareStateExecutionResponse
		StateDecision       xcapi.StateDecision
		PublishToLocalQueue []xcapi.LocalQueueMessage

		AppDatabaseConfig *InternalAppDatabaseConfig
		WriteAppDatabase  *xcapi.AppDatabaseWrite

		UpdateLocalAttributes []xcapi.KeyValue

		TaskShardId  int32
		TaskSequence int64
	}

	CompleteExecuteExecutionResponse struct {
		HasNewImmediateTask        bool
		FailedAtWritingAppDatabase bool
		AppDatabaseWritingError    error
	}
)
