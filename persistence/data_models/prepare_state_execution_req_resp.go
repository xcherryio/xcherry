// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: Apache-2.0

package data_models

import (
	"github.com/xcherryio/apis/goapi/xcapi"
	"github.com/xcherryio/xcherry/common/uuid"
)

type (
	PrepareStateExecutionRequest struct {
		ProcessExecutionId uuid.UUID
		StateExecutionId
	}

	PrepareStateExecutionResponse struct {
		Status StateExecutionStatus
		// only applicable for state execute API
		WaitUntilCommandResults xcapi.CommandResults

		// PreviousVersion is for conditional check in the future transactional update
		PreviousVersion int32

		Input       xcapi.EncodedObject
		Info        AsyncStateExecutionInfoJson
		LastFailure *StateExecutionFailureJson
	}
)
