// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: Apache-2.0

package data_models

import "github.com/xcherryio/apis/goapi/xcapi"

type (
	StopProcessRequest struct {
		Namespace       string
		ProcessId       string
		ProcessStopType xcapi.ProcessExecutionStopType
		NewTaskShardId  int32
	}

	StopProcessResponse struct {
		NotExists bool
	}
)
