// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package temporal

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/superdurable/dex/gen/dexpb"
	"go.temporal.io/api/enums/v1"
	temporalsdk "go.temporal.io/sdk/temporal"
)

func TestActivityProviderAppliesNextRetryDelay(t *testing.T) {
	activityError := &dexpb.InternalActivityError{
		WorkerError: &dexpb.InternalWorkerError{Detail: "worker detail"},
	}
	err := (&activityProvider{}).NewActivityError(
		dexpb.FlowErrorType_FLOW_ERROR_TYPE_WORKER_API_FAIL,
		activityError,
		7,
	)

	var applicationError *temporalsdk.ApplicationError
	require.ErrorAs(t, err, &applicationError)
	require.Equal(t, 7*time.Second, applicationError.NextRetryDelay())
	var decoded *dexpb.InternalActivityError
	require.NoError(t, applicationError.Details(&decoded))
	require.Equal(t, activityError, decoded)
}

func TestWorkflowProviderUsesFlowFailureEnvelope(t *testing.T) {
	provider := &workflowProvider{}
	err := provider.NewFlowError(
		dexpb.FlowErrorType_FLOW_ERROR_TYPE_INTERNAL,
		"invalid close decision type",
	)

	var applicationError *temporalsdk.ApplicationError
	require.ErrorAs(t, err, &applicationError)
	var flowError *dexpb.InternalFlowError
	require.NoError(t, applicationError.Details(&flowError))
	require.Equal(t, "invalid close decision type", flowError.GetServerDetail())
	require.Nil(t, flowError.GetActivityError())

	activityError := &dexpb.InternalActivityError{
		WorkerError: &dexpb.InternalWorkerError{Detail: "worker detail"},
	}
	activityFailure := (&activityProvider{}).NewActivityError(
		dexpb.FlowErrorType_FLOW_ERROR_TYPE_WORKER_API_FAIL,
		activityError,
		0,
	)
	wrappedFailure := provider.NewFlowErrorFromActivityError(activityFailure)
	require.ErrorAs(t, wrappedFailure, &applicationError)
	require.NoError(t, applicationError.Details(&flowError))
	require.Equal(t, activityError, flowError.GetActivityError())
	require.Empty(t, flowError.GetServerDetail())
}

func TestWorkflowProviderMapsWorkerAndTimeoutErrors(t *testing.T) {
	provider := &workflowProvider{}
	original := &dexpb.InternalActivityError{
		WorkerError: &dexpb.InternalWorkerError{
			Detail:     "worker detail",
			ErrorType:  "worker type",
			StackTrace: "worker stack",
		},
	}
	workerFailure := temporalsdk.NewApplicationError(
		"",
		dexpb.FlowErrorType_FLOW_ERROR_TYPE_WORKER_API_FAIL.String(),
		original,
	)

	recoveryError, err := provider.MapToRecoveryError(workerFailure)
	require.NoError(t, err)
	require.Equal(t, "worker detail", recoveryError.GetDetail())
	require.Equal(t, "worker type", recoveryError.GetErrorType())
	flowErrorType, flowResultError, err := provider.MapToFlowResultError(
		provider.NewFlowErrorFromActivityError(workerFailure),
	)
	require.NoError(t, err)
	require.Equal(t, dexpb.FlowErrorType_FLOW_ERROR_TYPE_WORKER_API_FAIL, flowErrorType)
	require.Equal(t, "worker detail", flowResultError.GetDetail())
	require.Equal(t, "worker type", flowResultError.GetErrorType())

	localFailure := &dexpb.InternalLocalStepActivityFailure{
		Attempt:       2,
		ActivityError: original,
	}
	localWorkerFailure := (&activityProvider{}).NewLocalActivityError(
		dexpb.FlowErrorType_FLOW_ERROR_TYPE_WORKER_API_FAIL,
		localFailure,
		11,
	)
	var localApplicationError *temporalsdk.ApplicationError
	require.ErrorAs(t, localWorkerFailure, &localApplicationError)
	require.Equal(t, 11*time.Second, localApplicationError.NextRetryDelay())
	decodedApplicationError, decodedLocalFailure, isApplicationFailure := temporalLocalStepActivityError(localWorkerFailure)
	require.True(t, isApplicationFailure)
	require.Same(t, localApplicationError, decodedApplicationError)
	require.Equal(t, original, decodedLocalFailure.GetActivityError())
	require.Equal(t, int32(2), decodedLocalFailure.GetAttempt())
	var extraDetail *dexpb.InternalActivityError
	require.Error(t, localApplicationError.Details(&decodedLocalFailure, &extraDetail))

	finalFailure := temporalFinalFlowError(decodedApplicationError, decodedLocalFailure.GetActivityError())
	var finalApplicationError *temporalsdk.ApplicationError
	require.ErrorAs(t, finalFailure, &finalApplicationError)
	var finalActivityError *dexpb.InternalActivityError
	require.NoError(t, finalApplicationError.Details(&finalActivityError))
	require.Equal(t, original, finalActivityError)
	var leakedFailure *dexpb.InternalLocalStepActivityFailure
	require.Error(t, temporalApplicationErrorDetails(finalApplicationError, &leakedFailure))

	recoveryError, err = provider.MapToRecoveryError(finalFailure)
	require.NoError(t, err)
	require.Equal(t, "worker detail", recoveryError.GetDetail())
	require.Equal(t, "worker type", recoveryError.GetErrorType())

	timeoutFailure := temporalsdk.NewTimeoutError(enums.TIMEOUT_TYPE_START_TO_CLOSE, nil)
	timeoutError, err := provider.MapToRecoveryError(timeoutFailure)
	require.NoError(t, err)
	require.Equal(t, enums.TIMEOUT_TYPE_START_TO_CLOSE.String(), timeoutError.GetErrorType())
	require.NotEmpty(t, timeoutError.GetDetail())
}
