// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: Apache-2.0

package data_models

import (
	"github.com/xcherryio/apis/goapi/xcapi"
	"github.com/xcherryio/xcherry/common/uuid"
)

type (
	PublishToLocalQueueRequest struct {
		Namespace string
		ProcessId string
		Messages  []xcapi.LocalQueueMessage
	}

	PublishToLocalQueueResponse struct {
		ProcessExecutionId  uuid.UUID
		HasNewImmediateTask bool
		ProcessNotExists    bool
		ProcessNotRunning   bool
	}
)
