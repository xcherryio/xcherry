// Copyright (c) XDBLab
// SPDX-License-Identifier: BUSL-1.1

package engine

import (
	"github.com/xdblab/xdb-apis/goapi/xdbapi"
	"github.com/xdblab/xdb/common/ptr"
)

// Default: infinite retry with 1 second initial interval, 120 seconds max interval, and 2 backoff factor,
var defaultWorkerTaskBackoffRetryPolicy = xdbapi.RetryPolicy{
	InitialIntervalSeconds:         ptr.Any(int32(1)),
	BackoffCoefficient:             ptr.Any(float32(2)),
	MaximumIntervalSeconds:         ptr.Any(int32(120)),
	MaximumAttempts:                ptr.Any(int32(0)),
	MaximumAttemptsDurationSeconds: ptr.Any(int32(0)),
}
