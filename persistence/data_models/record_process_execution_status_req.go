// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: Apache-2.0

package data_models

import "github.com/xcherryio/xcherry/common/uuid"

type RecordProcessExecutionStatusRequest struct {
	Namespace          string
	ProcessId          string
	ProcessExecutionId uuid.UUID
	ProcessType        string
	Status             ProcessExecutionStatus
	StartTime          int64
	EndTime            int64
}
