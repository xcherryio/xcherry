// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: BUSL-1.1

package engine

import (
	"github.com/xcherryio/xcherry/common/ptr"
)

// Default: infinite retry with 1 second initial interval, 120 seconds max interval, and 2 backoff factor,
var defaultWorkerTaskBackoffRetryPolicy = xcapi.RetryPolicy{
	InitialIntervalSeconds:         ptr.Any(int32(1)),
	BackoffCoefficient:             ptr.Any(float32(2)),
	MaximumIntervalSeconds:         ptr.Any(int32(120)),
	MaximumAttempts:                ptr.Any(int32(0)),
	MaximumAttemptsDurationSeconds: ptr.Any(int32(0)),
}
