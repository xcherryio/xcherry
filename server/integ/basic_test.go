// Legacy Materials in this file remain under their original licenses.
// See LEGACY_NOTICES.md.

// Modifications Copyright (c) 2026 Super Durable, Inc.
//
// Modifications after the Legacy Cutoff are licensed under the
// Sustainable Use License 1.0.
// Legacy Materials remain under their original licenses.
// See LICENSE and LEGACY_NOTICES.md.

package integ

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/superdurable/dex/gen/dexpb"
	"github.com/superdurable/dex/integ/workflow/basic"
	"github.com/superdurable/dex/service"
	"github.com/superdurable/dex/service/common/ptr"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestBasicFlowTemporal(t *testing.T) {
	if !*temporalIntegTest {
		t.Skip()
	}
	for i := 0; i < *repeatIntegTest; i++ {
		t.Run(fmt.Sprintf("default-%d", i), func(t *testing.T) {
			doTestBasicFlow(t, service.BackendTypeTemporal, nil)
		})
		t.Run(fmt.Sprintf("continue-as-new-%d", i), func(t *testing.T) {
			doTestBasicFlow(
				t,
				service.BackendTypeTemporal,
				minimumContinueAsNewAsyncDurabilityConfig(),
			)
		})
		t.Run(fmt.Sprintf("active-step-search-disabled-%d", i), func(t *testing.T) {
			doTestBasicFlow(t, service.BackendTypeTemporal, &dexpb.FlowConfig{
				ActiveStepSearchMode: ptr.Any(
					dexpb.ActiveStepSearchMode_ACTIVE_STEP_SEARCH_MODE_DISABLED,
				),
			})
		})
	}
}

func TestBasicFlowCadence(t *testing.T) {
	if !*cadenceIntegTest {
		t.Skip()
	}
	for i := 0; i < *repeatIntegTest; i++ {
		t.Run(fmt.Sprintf("default-%d", i), func(t *testing.T) {
			doTestBasicFlow(t, service.BackendTypeCadence, nil)
		})
		t.Run(fmt.Sprintf("continue-as-new-%d", i), func(t *testing.T) {
			doTestBasicFlow(
				t,
				service.BackendTypeCadence,
				minimumContinueAsNewAsyncDurabilityConfig(),
			)
		})
		t.Run(fmt.Sprintf("active-step-search-disabled-%d", i), func(t *testing.T) {
			doTestBasicFlow(t, service.BackendTypeCadence, &dexpb.FlowConfig{
				ActiveStepSearchMode: ptr.Any(
					dexpb.ActiveStepSearchMode_ACTIVE_STEP_SEARCH_MODE_DISABLED,
				),
			})
		})
	}
}

func doTestBasicFlow(
	t *testing.T,
	backendType service.BackendType,
	flowConfig *dexpb.FlowConfig,
	configureTest ...func(*DexServiceTestConfig),
) {
	workerHandler := basic.NewHandler()
	workerTarget := startWorker(t, workerHandler)
	testConfig := DexServiceTestConfig{
		BackendType: backendType,
	}
	for _, configure := range configureTest {
		configure(&testConfig)
	}
	runtime := startDexService(t, testConfig)
	flowClient := runtime.FlowClient

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	flowId := basic.FlowType + "-" + uuid.NewString()
	flowInput := &dexpb.Value{
		Kind: &dexpb.Value_ObjValue{
			ObjValue: &dexpb.EncodedObject{
				Encoding: "json",
				Payload:  []byte("test data"),
			},
		},
	}
	startRequest := &dexpb.StartFlowRequest{
		RequestId:          newRequestID(),
		FlowId:             flowId,
		FlowType:           basic.FlowType,
		FlowTimeoutSeconds: 100,

		StartStepType: basic.Step1,
		StepInput:     flowInput,
		FlowStartOptions: withWorkerTarget(&dexpb.FlowStartOptions{
			FlowConfigOverride: flowConfig,
			RetryPolicy: &dexpb.FlowRetryPolicy{
				InitialIntervalSeconds: 11,
				BackoffCoefficient:     11,
				MaximumAttempts:        11,
				MaximumIntervalSeconds: 11,
			},
		}, workerTarget),

		StepOptions: &dexpb.StepOptions{
			WaitForTimeoutSeconds: 12,
			ExecuteTimeoutSeconds: 13,
			WaitForRetryPolicy: &dexpb.RetryPolicy{
				InitialIntervalSeconds: 12,
				BackoffCoefficient:     12,
				MaximumAttempts:        12,
				MaximumIntervalSeconds: 12,
			},
			ExecuteRetryPolicy: &dexpb.RetryPolicy{
				InitialIntervalSeconds: 13,
				BackoffCoefficient:     13,
				MaximumAttempts:        13,
				MaximumIntervalSeconds: 13,
			},
		},
	}
	if isServerZDBinaryMode() {
		// C20: server-zd rejects Flow retry policies; this scenario never retries the Flow.
		startRequest.FlowStartOptions.RetryPolicy = nil
	}
	startResponse, err := flowClient.StartFlow(ctx, startRequest)
	require.NoError(t, err)
	require.NotEmpty(t, startResponse.GetRunId())

	_, err = flowClient.StartFlow(ctx, startRequest)
	require.Equal(t, codes.AlreadyExists, status.Code(err))
	require.Equal(
		t,
		dexpb.ErrorSubStatus_ERROR_SUB_STATUS_FLOW_ALREADY_STARTED,
		grpcServiceErrorResponse(t, err).GetSubStatus(),
	)

	response, err := flowClient.WaitForFlow(ctx, &dexpb.WaitForFlowRequest{
		FlowId:          flowId,
		NeedsResults:    true,
		WaitTimeSeconds: 20,
	})
	require.NoError(t, err)
	requireCurrentRunSearchResult(t, flowClient, flowId, backendType)

	startRequest.RequestId = newRequestID()
	_, err = flowClient.StartFlow(ctx, startRequest)
	require.Equal(t, codes.AlreadyExists, status.Code(err))
	require.Equal(
		t,
		dexpb.ErrorSubStatus_ERROR_SUB_STATUS_FLOW_ALREADY_STARTED,
		grpcServiceErrorResponse(t, err).GetSubStatus(),
	)

	_, err = flowClient.WaitForFlow(ctx, &dexpb.WaitForFlowRequest{
		FlowId:          "a-wrong-flow-id-" + uuid.NewString(),
		WaitTimeSeconds: 1,
	})
	require.Equal(t, codes.NotFound, status.Code(err))
	require.Equal(
		t,
		dexpb.ErrorSubStatus_ERROR_SUB_STATUS_FLOW_NOT_EXISTS,
		grpcServiceErrorResponse(t, err).GetSubStatus(),
	)

	history := workerHandler.GetTestResult().InvokeHistory
	require.Equal(t, map[string]int64{
		"S1_waitFor": 1,
		"S1_execute": 1,
		"S2_waitFor": 1,
		"S2_execute": 1,
	}, history)

	require.Equal(t, dexpb.FlowStatus_FLOW_STATUS_COMPLETED, response.GetFlowStatus())
	require.Len(t, response.GetResults(), 1)
	result := response.GetResults()[0]
	require.Equal(t, basic.Step2, result.GetCompletedStepType())
	require.Equal(t, basic.Step2+"-1", result.GetCompletedStepExecutionId())
	require.True(t, proto.Equal(flowInput, result.GetCompletedStepOutput()))
}

func requireCurrentRunSearchResult(
	t *testing.T,
	flowClient dexpb.FlowServiceClient,
	flowID string,
	backendType service.BackendType,
) {
	t.Helper()
	workflowIDField := "WorkflowId"
	if backendType == service.BackendTypeCadence {
		workflowIDField = "WorkflowID"
	}
	require.Eventually(t, func() bool {
		response, err := flowClient.SearchFlows(context.Background(), &dexpb.SearchFlowsRequest{
			Query:    workflowIDField + " = '" + flowID + "'",
			PageSize: 1000,
		})
		if err != nil {
			return false
		}
		currentRunCount := 0
		for _, flowRun := range response.GetFlowRuns() {
			if flowRun.GetFlowId() == flowID {
				currentRunCount++
				if flowRun.GetFlowStatus() == dexpb.FlowStatus_FLOW_STATUS_CONTINUED_AS_NEW {
					return false
				}
			}
		}
		return currentRunCount == 1
	}, 10*time.Second, 100*time.Millisecond)
}
