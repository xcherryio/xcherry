// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: Apache-2.0

package data_models

import "github.com/xcherryio/apis/goapi/xcapi"

type (
	StopProcessRequest struct {
		Namespace       string
		ProcessId       string
		ProcessStopType xcapi.ProcessExecutionStopType
	}

	StopProcessResponse struct {
		NotExists bool
	}
)
