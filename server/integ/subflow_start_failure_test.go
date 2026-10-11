// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package integ

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/superdurable/dex/config"
	"github.com/superdurable/dex/gen/dexpb"
	"github.com/superdurable/dex/integ/workflow/common"
	"github.com/superdurable/dex/service"
	"github.com/superdurable/dex/service/common/ptr"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	subFlowStartFailureParentType = "sub-flow-start-failure-parent"
	subFlowStartFailureChildType  = "sub-flow-start-failure-child"
	subFlowStartFailureParentStep = "Parent"
	subFlowStartFailureOutput     = "start-failure-observed"
	// The server validates Attribute Store availability only when starting the SubFlow.
	subFlowStartFailureMissingStore = "missing-attribute-store"
)

type subFlowStartFailureObservation struct {
	isWaitForFailed  bool
	flowStatus       dexpb.FlowStatus
	errorType        dexpb.FlowErrorType
	hasErrorDetail   bool
	hasRecoveryError bool
}

type subFlowStartFailureHandler struct {
	dexpb.UnimplementedWorkerServiceServer
	mu           sync.Mutex
	observations []subFlowStartFailureObservation
}

func TestSubFlowStartFailureTemporal(t *testing.T) {
	if !*temporalIntegTest {
		t.Skip()
	}
	testSubFlowStartFailure(t, service.BackendTypeTemporal)
}

func TestSubFlowStartFailureCadence(t *testing.T) {
	if !*cadenceIntegTest {
		t.Skip()
	}
	testSubFlowStartFailure(t, service.BackendTypeCadence)
}

func testSubFlowStartFailure(t *testing.T, backendType service.BackendType) {
	for i := 0; i < *repeatIntegTest; i++ {
		t.Run(fmt.Sprintf("proceed-on-wait-for-failure-%d", i), func(t *testing.T) {
			doTestSubFlowStartFailure(
				t,
				backendType,
				dexpb.WaitForMethodFailurePolicy_WAIT_FOR_METHOD_FAILURE_POLICY_PROCEED_ON_FAILURE,
			)
		})
		t.Run(fmt.Sprintf("fail-flow-by-default-%d", i), func(t *testing.T) {
			doTestSubFlowStartFailure(
				t,
				backendType,
				dexpb.WaitForMethodFailurePolicy_WAIT_FOR_METHOD_FAILURE_POLICY_UNSPECIFIED,
			)
		})
	}
}

func doTestSubFlowStartFailure(
	t *testing.T,
	backendType service.BackendType,
	waitForFailurePolicy dexpb.WaitForMethodFailurePolicy,
) {
	handler := &subFlowStartFailureHandler{}
	workerTarget := startWorker(t, handler)
	runtime := startDexService(t, DexServiceTestConfig{
		BackendType:        backendType,
		LazyLoading:        ptr.Any(false),
		LocalBlobDirectory: t.TempDir(),
		SubFlowStartActivityConfig: &config.SubFlowStartActivityConfig{
			ScheduleToCloseTimeout: 10 * time.Second,
			RetryPolicy: &config.RetryPolicy{
				InitialInterval: 100 * time.Millisecond,
				MaximumAttempts: 2,
			},
		},
	})
	flowClient := runtime.FlowClient
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	parentFlowID := subFlowStartFailureParentType + "-" + uuid.NewString()
	_, err := flowClient.StartFlow(ctx, &dexpb.StartFlowRequest{
		RequestId:          newRequestID(),
		FlowId:             parentFlowID,
		FlowType:           subFlowStartFailureParentType,
		FlowTimeoutSeconds: 30,
		StartStepType:      subFlowStartFailureParentStep,
		StepInput:          stringValue(subFlowStartFailureOutput),
		StepOptions:        &dexpb.StepOptions{WaitForFailurePolicy: waitForFailurePolicy},
		FlowStartOptions:   withWorkerTarget(&dexpb.FlowStartOptions{}, workerTarget),
	})
	require.NoError(t, err)

	result, err := flowClient.WaitForFlow(ctx, &dexpb.WaitForFlowRequest{
		FlowId:          parentFlowID,
		NeedsResults:    true,
		WaitTimeSeconds: 30,
	})
	require.NoError(t, err)

	if waitForFailurePolicy != dexpb.WaitForMethodFailurePolicy_WAIT_FOR_METHOD_FAILURE_POLICY_PROCEED_ON_FAILURE {
		require.Equal(t, dexpb.FlowStatus_FLOW_STATUS_FAILED, result.GetFlowStatus())
		require.Empty(t, handler.results())
		return
	}

	require.Equal(t, dexpb.FlowStatus_FLOW_STATUS_COMPLETED, result.GetFlowStatus())
	require.Len(t, result.GetResults(), 1)
	completedOutput, err := common.LoadBlobsValue(
		ctx,
		flowClient,
		parentFlowID,
		result.GetResults()[0].GetCompletedStepOutput(),
	)
	require.NoError(t, err)
	require.Equal(t, subFlowStartFailureOutput, completedOutput.GetStringValue())
	require.Equal(t, []subFlowStartFailureObservation{{
		isWaitForFailed:  true,
		flowStatus:       dexpb.FlowStatus_FLOW_STATUS_FAILED,
		errorType:        dexpb.FlowErrorType_FLOW_ERROR_TYPE_SUB_FLOW_START_FAILED,
		hasErrorDetail:   true,
		hasRecoveryError: true,
	}}, handler.results())
}

func (h *subFlowStartFailureHandler) InvokeWaitForMethod(
	_ context.Context,
	request *dexpb.InvokeWaitForMethodRequest,
) (*dexpb.InvokeWaitForMethodResponse, error) {
	common.LogRequest("received SubFlow start failure waitFor request, ", request)
	if request.GetFlowType() != subFlowStartFailureParentType ||
		request.GetStepType() != subFlowStartFailureParentStep {
		return nil, status.Error(codes.InvalidArgument, "unexpected waitFor invocation")
	}
	return &dexpb.InvokeWaitForMethodResponse{
		WaitingCondition: &dexpb.WaitingCondition{
			WaitingConditionType: dexpb.WaitingConditionType_WAITING_CONDITION_TYPE_ALL_COMPLETED,
			SubFlowConditions: []*dexpb.SubFlowCondition{{
				SubFlowType:   subFlowStartFailureChildType,
				StartStepType: subFlowStartFailureParentStep,
				StepInput:     request.GetStepInput(),
				StepOptions:   &dexpb.StepOptions{SkipWaitFor: true},
				Options: &dexpb.SubFlowOptions{
					FlowConfigOverride: &dexpb.FlowConfig{
						AttributeStoreNames: &dexpb.AttributeStoreNames{
							Names: []string{subFlowStartFailureMissingStore},
						},
					},
				},
			}},
		},
	}, nil
}

func (h *subFlowStartFailureHandler) InvokeExecuteMethod(
	_ context.Context,
	request *dexpb.InvokeExecuteMethodRequest,
) (*dexpb.InvokeExecuteMethodResponse, error) {
	common.LogRequest("received SubFlow start failure execute request, ", request)
	if request.GetFlowType() != subFlowStartFailureParentType ||
		request.GetStepType() != subFlowStartFailureParentStep {
		return nil, status.Error(codes.InvalidArgument, "unexpected execute invocation")
	}
	conditionResults := request.GetConditionResults()
	subFlowResults := conditionResults.GetSubFlowResults()
	if len(subFlowResults) != 1 {
		return nil, status.Error(codes.InvalidArgument, "expected one SubFlow result")
	}
	h.mu.Lock()
	h.observations = append(h.observations, subFlowStartFailureObservation{
		isWaitForFailed:  conditionResults.GetWaitForFailed(),
		flowStatus:       subFlowResults[0].GetFlowStatus(),
		errorType:        subFlowResults[0].GetErrorType(),
		hasErrorDetail:   strings.Contains(subFlowResults[0].GetErrorMessage(), subFlowStartFailureMissingStore),
		hasRecoveryError: request.GetContext().GetRecoveryError() != nil,
	})
	h.mu.Unlock()
	return &dexpb.InvokeExecuteMethodResponse{
		StepDecision: &dexpb.StepDecision{
			CloseDecision: common.GracefulCompleteDecision(request.GetStepInput()),
		},
	}, nil
}

func (h *subFlowStartFailureHandler) results() []subFlowStartFailureObservation {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]subFlowStartFailureObservation(nil), h.observations...)
}
