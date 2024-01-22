// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: Apache-2.0

package data_models

import (
	"github.com/xcherryio/apis/goapi/xcapi"
	"github.com/xcherryio/xcherry/common/uuid"
)

type RecoverFromStateExecutionFailureRequest struct {
	Namespace                    string
	ProcessExecutionId           uuid.UUID
	Prepare                      PrepareStateExecutionResponse
	SourceStateExecutionId       StateExecutionId
	SourceFailedStateApi         xcapi.WorkerApiType
	LastFailureStatus            int32
	LastFailureDetails           string
	LastFailureCompletedAttempts int32
	DestinationStateId           string
	DestinationStateConfig       *xcapi.AsyncStateConfig
	DestinationStateInput        xcapi.EncodedObject
	ShardId                      int32
}
