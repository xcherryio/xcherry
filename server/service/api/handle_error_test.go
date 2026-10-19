// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package api

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/enums/v1"
	temporalerrordetails "go.temporal.io/api/errordetails/v1"
	"go.temporal.io/api/serviceerror"
	"go.uber.org/cadence/.gen/go/shared"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

// The SDK retries ResourceExhausted until the deadline, so integration tests cannot surface it deterministically.
func TestHandleErrorPreservesTemporalResourceExhausted(t *testing.T) {
	engineError := serviceerror.NewResourceExhausted(
		enums.RESOURCE_EXHAUSTED_CAUSE_RPS_LIMIT,
		"namespace rate limit exceeded",
	)
	for name, err := range map[string]error{
		"direct":  engineError,
		"wrapped": fmt.Errorf("list workflows: %w", engineError),
	} {
		t.Run(name, func(t *testing.T) {
			returnedStatus := status.Convert((&serviceImpl{}).handleError(err))
			require.Equal(t, codes.ResourceExhausted, returnedStatus.Code())
			require.Equal(t, err.Error(), returnedStatus.Message())
			require.Len(t, returnedStatus.Details(), 1)
			failure, ok := returnedStatus.Details()[0].(*temporalerrordetails.ResourceExhaustedFailure)
			require.True(t, ok)
			require.Equal(t, enums.RESOURCE_EXHAUSTED_CAUSE_RPS_LIMIT, failure.GetCause())
		})
	}
}

func TestHandleErrorPreservesGRPCResourceExhaustedRetryHint(t *testing.T) {
	engineStatus, err := status.New(codes.ResourceExhausted, "visibility rate limit exceeded").
		WithDetails(&errdetails.RetryInfo{RetryDelay: durationpb.New(time.Second)})
	require.NoError(t, err)

	returnedStatus := status.Convert((&serviceImpl{}).handleError(fmt.Errorf("search flows: %w", engineStatus.Err())))
	require.Equal(t, codes.ResourceExhausted, returnedStatus.Code())
	require.Contains(t, returnedStatus.Message(), "visibility rate limit exceeded")
	require.Len(t, returnedStatus.Details(), 1)
	retryInfo, ok := returnedStatus.Details()[0].(*errdetails.RetryInfo)
	require.True(t, ok)
	require.Equal(t, time.Second, retryInfo.GetRetryDelay().AsDuration())
}

func TestHandleErrorMapsCadenceRateLimitsToResourceExhausted(t *testing.T) {
	for name, err := range map[string]error{
		"limit exceeded":         &shared.LimitExceededError{Message: "domain visibility rate limit exceeded"},
		"service busy":           &shared.ServiceBusyError{Message: "domain visibility rate limit exceeded"},
		"wrapped limit exceeded": fmt.Errorf("list: %w", &shared.LimitExceededError{Message: "domain visibility rate limit exceeded"}),
	} {
		t.Run(name, func(t *testing.T) {
			returnedStatus := status.Convert((&serviceImpl{}).handleError(err))
			require.Equal(t, codes.ResourceExhausted, returnedStatus.Code())
			require.Equal(t, "domain visibility rate limit exceeded", returnedStatus.Message())
		})
	}
}
