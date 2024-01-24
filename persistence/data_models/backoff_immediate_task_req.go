// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: Apache-2.0

package data_models

type BackoffImmediateTaskRequest struct {
	LastFailureStatus    int32
	LastFailureDetails   string
	Prep                 PrepareStateExecutionResponse
	Task                 ImmediateTask
	FireTimestampSeconds int64
}
