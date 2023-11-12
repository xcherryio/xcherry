// Copyright (c) 2023 XDBLab Organization
// SPDX-License-Identifier: BUSL-1.1

package data_models

import (
	"encoding/json"
	"github.com/xdblab/xdb/common/ptr"
	"time"
)

type StateExecutionFailureJson struct {
	StatusCode           *int32  `json:"statusCode"`
	Details              *string `json:"details"`
	CompletedAttempts    *int32  `json:"completedAttempts"`
	LastAttemptTimestamp *int64  `json:"lastAttemptTimestamp"`
}

func CreateStateExecutionFailureBytesForBackoff(status int32, details string, completedAttempts int32) ([]byte, error) {
	obj := StateExecutionFailureJson{
		StatusCode:           &status,
		Details:              &details,
		CompletedAttempts:    &completedAttempts,
		LastAttemptTimestamp: ptr.Any(time.Now().Unix()),
	}
	return json.Marshal(obj)
}