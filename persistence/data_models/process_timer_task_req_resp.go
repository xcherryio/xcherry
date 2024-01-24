// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: Apache-2.0

package data_models

type (
	ProcessTimerTaskRequest struct {
		Task TimerTask
	}

	ProcessTimerTaskResponse struct {
		HasNewImmediateTask bool
	}
)
