// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package cadence

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/superdurable/dex/gen/dexpb"
	"github.com/superdurable/dex/service/interpreter/interfaces"
	"go.uber.org/cadence"
	"go.uber.org/cadence/.gen/go/shared"
	"go.uber.org/cadence/workflow"
)

func TestWorkflowProviderUsesFlowFailureEnvelope(t *testing.T) {
	provider := &workflowProvider{}
	err := provider.NewFlowError(
		dexpb.FlowErrorType_FLOW_ERROR_TYPE_INTERNAL,
		"invalid close decision type",
	)

	var customError *cadence.CustomError
	require.ErrorAs(t, err, &customError)
	var flowError *dexpb.InternalFlowError
	require.NoError(t, customError.Details(&flowError))
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
	require.ErrorAs(t, wrappedFailure, &customError)
	require.NoError(t, customError.Details(&flowError))
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
	workerFailure := cadence.NewCustomError(
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
		0,
	)
	customError, decodedLocalFailure, isApplicationFailure := cadenceLocalStepActivityError(localWorkerFailure)
	require.True(t, isApplicationFailure)
	require.Equal(t, original, decodedLocalFailure.GetActivityError())
	require.Equal(t, int32(2), decodedLocalFailure.GetAttempt())
	var extraDetail *dexpb.InternalActivityError
	require.Error(t, customError.Details(&decodedLocalFailure, &extraDetail))

	finalFailure := cadenceFinalFlowError(customError, decodedLocalFailure.GetActivityError())
	var finalCustomError *cadence.CustomError
	require.ErrorAs(t, finalFailure, &finalCustomError)
	var finalActivityError *dexpb.InternalActivityError
	require.NoError(t, finalCustomError.Details(&finalActivityError))
	require.Equal(t, original, finalActivityError)
	var leakedFailure *dexpb.InternalLocalStepActivityFailure
	require.Error(t, finalCustomError.Details(&leakedFailure))

	recoveryError, err = provider.MapToRecoveryError(finalFailure)
	require.NoError(t, err)
	require.Equal(t, "worker detail", recoveryError.GetDetail())
	require.Equal(t, "worker type", recoveryError.GetErrorType())

	timeoutFailure := workflow.NewTimeoutError(shared.TimeoutTypeStartToClose)
	timeoutError, err := provider.MapToRecoveryError(timeoutFailure)
	require.NoError(t, err)
	require.Equal(t, shared.TimeoutTypeStartToClose.String(), timeoutError.GetErrorType())
	require.NotEmpty(t, timeoutError.GetDetail())
}

func TestWorkflowProviderMapsSubFlowStartRejection(t *testing.T) {
	const detail = "pull worker_target cannot specify address"
	rejection := (&activityProvider{}).NewSubFlowStartRejection(detail)
	recoveryError, err := (&workflowProvider{}).MapToRecoveryError(rejection)
	require.NoError(t, err)
	require.Equal(t, detail, recoveryError.GetDetail())
	require.Equal(t, interfaces.SubFlowStartRejectionErrorType, recoveryError.GetErrorType())
}
