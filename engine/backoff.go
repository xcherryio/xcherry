// Copyright (c) 2023 XDBLab Organization
// SPDX-License-Identifier: BUSL-1.1

package engine

import (
	"github.com/xdblab/xdb-apis/goapi/xdbapi"
	"math"
	"time"
)

func GetNextBackoff(
	completedAttempts int32, firstAttemptStartTimestampSeconds int64, policy *xdbapi.RetryPolicy,
) (nextBackoffSeconds int32, shouldRetry bool) {
	policy = setDefaultRetryPolicyValue(policy)
	if *policy.MaximumAttempts > 0 && completedAttempts >= *policy.MaximumAttempts {
		return 0, false
	}
	nowSeconds := int64(time.Now().Unix())
	if *policy.MaximumAttemptsDurationSeconds > 0 && firstAttemptStartTimestampSeconds+int64(*policy.MaximumAttemptsDurationSeconds) < nowSeconds {
		return 0, false
	}
	initInterval := *policy.InitialIntervalSeconds
	nextInterval := int32(float64(initInterval) * math.Pow(float64(*policy.BackoffCoefficient), float64(completedAttempts-1)))
	if nextInterval > *policy.MaximumIntervalSeconds {
		nextInterval = *policy.MaximumIntervalSeconds
	}
	return nextInterval, true
}

func setDefaultRetryPolicyValue(policy *xdbapi.RetryPolicy) *xdbapi.RetryPolicy {
	if policy == nil {
		policy = &xdbapi.RetryPolicy{}
	}
	if policy.InitialIntervalSeconds == nil {
		policy.InitialIntervalSeconds = defaultWorkerTaskBackoffRetryPolicy.InitialIntervalSeconds
	}
	if policy.BackoffCoefficient == nil {
		policy.BackoffCoefficient = defaultWorkerTaskBackoffRetryPolicy.BackoffCoefficient
	}
	if policy.MaximumIntervalSeconds == nil {
		policy.MaximumIntervalSeconds = defaultWorkerTaskBackoffRetryPolicy.MaximumIntervalSeconds
	}
	if policy.MaximumAttempts == nil {
		policy.MaximumAttempts = defaultWorkerTaskBackoffRetryPolicy.MaximumAttempts
	}
	if policy.MaximumAttemptsDurationSeconds == nil {
		policy.MaximumAttemptsDurationSeconds = defaultWorkerTaskBackoffRetryPolicy.MaximumAttemptsDurationSeconds
	}
	return policy
}
