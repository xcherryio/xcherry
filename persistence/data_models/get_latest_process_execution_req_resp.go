// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: Apache-2.0

package data_models

import "github.com/xcherryio/xcherry/common/uuid"

type (
	GetLatestProcessExecutionRequest struct {
		Namespace string
		ProcessId string
	}

	GetLatestProcessExecutionResponse struct {
		NotExists bool

		ProcessExecutionId uuid.UUID
		Status             ProcessExecutionStatus
		StartTimestamp     int64
		AppDatabaseConfig  *InternalAppDatabaseConfig

		// the process type for SDK to look up the process definition class
		ProcessType string
		// the URL for server async service to make callback to worker
		WorkerUrl string
	}
)
