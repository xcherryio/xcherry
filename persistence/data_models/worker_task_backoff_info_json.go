// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: Apache-2.0

package data_models

type WorkerTaskBackoffInfoJson struct {
	// CompletedAttempts is the number of attempts that have been completed
	// for calculating next backoff interval
	CompletedAttempts int32 `json:"completedAttempts"`
	// FirstAttemptTimestampSeconds is the timestamp of the first attempt
	// for calculating next backoff interval
	FirstAttemptTimestampSeconds int64 `json:"firstAttemptTimestampSeconds"`
}
