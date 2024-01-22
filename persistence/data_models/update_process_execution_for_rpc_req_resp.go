// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: Apache-2.0

package data_models

import (
	"github.com/xcherryio/apis/goapi/xcapi"
	"github.com/xcherryio/xcherry/common/uuid"
)

type (
	UpdateProcessExecutionForRpcRequest struct {
		Namespace          string
		ProcessId          string
		ProcessType        string
		ProcessExecutionId uuid.UUID

		StateDecision       xcapi.StateDecision
		PublishToLocalQueue []xcapi.LocalQueueMessage

		AppDatabaseConfig *InternalAppDatabaseConfig
		AppDatabaseWrite  *xcapi.AppDatabaseWrite

		WorkerUrl   string
		TaskShardId int32
	}

	UpdateProcessExecutionForRpcResponse struct {
		HasNewImmediateTask      bool
		ProcessNotExists         bool
		FailAtWritingAppDatabase bool
		WritingAppDatabaseError  error
	}
)
