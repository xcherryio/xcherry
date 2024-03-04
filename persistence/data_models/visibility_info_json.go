// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: Apache-2.0

package data_models

import "github.com/xcherryio/xcherry/common/uuid"

type VisibilityInfoJson struct {
	Namespace          string                 `json:"namespace"`
	ProcessId          string                 `json:"processId"`
	ProcessExecutionId uuid.UUID              `json:"processExecutionId"`
	ProcessType        string                 `json:"processType"`
	Status             ProcessExecutionStatus `json:"status"`
	StartTime          *int64                 `json:"startTime"`
	CloseTime          *int64                 `json:"closeTime"`
}
