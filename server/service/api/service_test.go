// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package api

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/superdurable/dex/gen/dexpb"
	uclient "github.com/superdurable/dex/service/client"
	"github.com/superdurable/dex/service/common/log"
	"go.temporal.io/api/enums/v1"
	temporalerrordetails "go.temporal.io/api/errordetails/v1"
	"go.temporal.io/api/serviceerror"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

type searchFlowsErrorClient struct {
	uclient.UnifiedClient
	listWorkflowError error
}

func (c *searchFlowsErrorClient) ListWorkflow(
	context.Context,
	*uclient.ListWorkflowExecutionsRequest,
) (*uclient.ListWorkflowExecutionsResponse, error) {
	return nil, c.listWorkflowError
}

func TestSearchFlowsResourceExhausted(t *testing.T) {
	temporalError := serviceerror.NewResourceExhausted(
		enums.RESOURCE_EXHAUSTED_CAUSE_RPS_LIMIT,
		"visibility rate limit exceeded",
	)
	retryHint := &errdetails.RetryInfo{RetryDelay: durationpb.New(2 * time.Second)}
	grpcStatus, err := status.New(codes.ResourceExhausted, "engine quota exceeded").WithDetails(retryHint)
	require.NoError(t, err)
	wrappedTemporalError := fmt.Errorf("list workflows: %w", temporalError)
	wrappedGRPCError := fmt.Errorf("list workflows: %w", grpcStatus.Err())

	tests := []struct {
		name            string
		engineError     error
		expectedMessage string
		expectedDetail  proto.Message
	}{
		{
			name:            "Temporal service error",
			engineError:     temporalError,
			expectedMessage: temporalError.Error(),
			expectedDetail:  &temporalerrordetails.ResourceExhaustedFailure{Cause: enums.RESOURCE_EXHAUSTED_CAUSE_RPS_LIMIT},
		},
		{
			name:            "wrapped Temporal service error",
			engineError:     wrappedTemporalError,
			expectedMessage: wrappedTemporalError.Error(),
			expectedDetail:  &temporalerrordetails.ResourceExhaustedFailure{Cause: enums.RESOURCE_EXHAUSTED_CAUSE_RPS_LIMIT},
		},
		{
			name:            "gRPC status with retry hint",
			engineError:     grpcStatus.Err(),
			expectedMessage: grpcStatus.Message(),
			expectedDetail:  retryHint,
		},
		{
			name:            "wrapped gRPC status with retry hint",
			engineError:     wrappedGRPCError,
			expectedMessage: wrappedGRPCError.Error(),
			expectedDetail:  retryHint,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			logger := &log.MockLogger{}
			apiService := &serviceImpl{
				client: &searchFlowsErrorClient{listWorkflowError: test.engineError},
				logger: logger,
			}
			response, searchError := apiService.SearchFlows(context.Background(), &dexpb.SearchFlowsRequest{})

			require.Nil(t, response)
			returnedStatus, ok := status.FromError(searchError)
			require.True(t, ok)
			require.Equal(t, codes.ResourceExhausted, returnedStatus.Code())
			require.Equal(t, test.expectedMessage, returnedStatus.Message())
			require.Len(t, returnedStatus.Details(), 1)
			returnedDetail, ok := returnedStatus.Details()[0].(proto.Message)
			require.True(t, ok)
			require.True(t, proto.Equal(test.expectedDetail, returnedDetail))
			logger.AssertNotCalled(t, "Error")
		})
	}
}
