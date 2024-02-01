// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: Apache-2.0

package data_models

type VisibilityInfoJson struct {
	Namespace   string
	ProcessId   string
	ProcessType string
	Status      ProcessExecutionStatus
	StartTime   int64
	EndTime     int64
}
