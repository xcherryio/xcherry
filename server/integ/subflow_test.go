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
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/superdurable/dex/gen/dexpb"
	"github.com/superdurable/dex/integ/workflow/common"
	"github.com/superdurable/dex/service"
	"github.com/superdurable/dex/service/common/ptr"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	subFlowParentType = "sub-flow-parent"
	subFlowChildType  = "sub-flow-child"
	subFlowParentStep = "Parent"
	subFlowChildStep  = "Child"
	subFlowInput      = "child-output"
	subFlowAttribute  = "child-initial-attribute"
)

type subFlowObservation struct {
	status dexpb.FlowStatus
}

type subFlowHandler struct {
	dexpb.UnimplementedWorkerServiceServer
	mu           sync.Mutex
	observations []subFlowObservation
	reusePolicy  dexpb.SubFlowReusePolicy
}

func TestSubFlowConditionTemporal(t *testing.T) {
	if !*temporalIntegTest {
		t.Skip()
	}
	testSubFlowReuseAfterReset(t, service.BackendTypeTemporal)
}

func TestSubFlowConditionCadence(t *testing.T) {
	if !*cadenceIntegTest {
		t.Skip()
	}
	testSubFlowReuseAfterReset(t, service.BackendTypeCadence)
}

func testSubFlowReuseAfterReset(t *testing.T, backendType service.BackendType) {
	for i := 0; i < *repeatIntegTest; i++ {
		t.Run(fmt.Sprintf("terminal-default-after-reset-%d", i), func(t *testing.T) {
			doTestSubFlowCondition(
				t,
				backendType,
				dexpb.SubFlowReusePolicy_SUB_FLOW_REUSE_POLICY_RESTART_IF_PREVIOUS_EXITS_ABNORMALLY,
				false,
			)
		})
		t.Run(fmt.Sprintf("terminal-always-restart-after-reset-%d", i), func(t *testing.T) {
			doTestSubFlowCondition(
				t,
				backendType,
				dexpb.SubFlowReusePolicy_SUB_FLOW_REUSE_POLICY_ALWAYS_RESTART,
				true,
			)
		})
	}
}

func doTestSubFlowCondition(
	t *testing.T,
	backendType service.BackendType,
	reusePolicy dexpb.SubFlowReusePolicy,
	shouldRestart bool,
	configureTest ...func(*DexServiceTestConfig),
) {
	requireTemporalOnly(t)
	handler := &subFlowHandler{reusePolicy: reusePolicy}
	workerTarget := startWorker(t, handler)
	testConfig := DexServiceTestConfig{
		BackendType:        backendType,
		LazyLoading:        ptr.Any(false),
		LocalBlobDirectory: t.TempDir(),
		LocalBlobThreshold: 10,
	}
	for _, configure := range configureTest {
		configure(&testConfig)
	}
	runtime := startDexService(t, testConfig)
	flowClient := runtime.FlowClient
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	parentFlowID := subFlowParentType + "-" + uuid.NewString()
	childFlowID := "SubFlow:" + parentFlowID + "-" + subFlowParentStep + "-1-0"
	input := stringValue(subFlowInput)
	parentRequestID := newRequestID()
	for _, reservedCharacter := range []string{"/", "$", ":"} {
		_, err := flowClient.StartFlow(ctx, &dexpb.StartFlowRequest{
			RequestId:          newRequestID(),
			FlowId:             parentFlowID,
			FlowType:           subFlowParentType,
			FlowTimeoutSeconds: 30,
			StartStepType:      subFlowParentStep + reservedCharacter + "Invalid",
			StepInput:          input,
			FlowStartOptions:   withWorkerTarget(&dexpb.FlowStartOptions{}, workerTarget),
		})
		require.Equal(t, codes.InvalidArgument, status.Code(err))
		require.ErrorContains(
			t,
			err,
			fmt.Sprintf("step type contains reserved character %q", reservedCharacter),
		)
	}
	_, err := flowClient.StartFlow(ctx, &dexpb.StartFlowRequest{
		RequestId:          newRequestID(),
		FlowId:             childFlowID,
		FlowType:           subFlowChildType,
		FlowTimeoutSeconds: 30,
		StartStepType:      subFlowChildStep,
		StepInput:          input,
		FlowStartOptions:   withWorkerTarget(&dexpb.FlowStartOptions{}, workerTarget),
	})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.ErrorContains(t, err, `flow ID contains reserved character ":"`)

	startResponse, err := flowClient.StartFlow(ctx, &dexpb.StartFlowRequest{
		RequestId:          parentRequestID,
		FlowId:             parentFlowID,
		FlowType:           subFlowParentType,
		FlowTimeoutSeconds: 30,
		StartStepType:      subFlowParentStep,
		StepInput:          input,
		FlowStartOptions: withWorkerTarget(&dexpb.FlowStartOptions{
			IdReusePolicy: dexpb.IdReusePolicy_ID_REUSE_POLICY_DISALLOW_REUSE,
		}, workerTarget),
	})
	require.NoError(t, err)
	parentDescription, err := runtime.UnifiedClient.DescribeWorkflowExecution(
		ctx,
		parentFlowID,
		startResponse.GetRunId(),
		nil,
	)
	require.NoError(t, err)
	require.Len(t, parentDescription.Memos, 1)
	require.Equal(t, parentRequestID, string(parentDescription.Memos["ReqId"].GetObjValue().GetPayload()))

	firstResult, err := flowClient.WaitForFlow(ctx, &dexpb.WaitForFlowRequest{
		FlowId:          parentFlowID,
		NeedsResults:    true,
		WaitTimeSeconds: 30,
	})
	require.NoError(t, err)
	require.Equal(t, dexpb.FlowStatus_FLOW_STATUS_COMPLETED, firstResult.GetFlowStatus())
	require.Len(t, firstResult.GetResults(), 1)
	completedOutput, err := common.LoadBlobsValue(
		ctx,
		flowClient,
		parentFlowID,
		firstResult.GetResults()[0].GetCompletedStepOutput(),
	)
	require.NoError(t, err)
	require.Equal(t, subFlowInput, completedOutput.GetStringValue())

	firstChild, err := flowClient.GetFlowSummary(ctx, &dexpb.GetFlowSummaryRequest{
		FlowId: childFlowID,
	})
	require.NoError(t, err)
	require.Equal(t, dexpb.FlowStatus_FLOW_STATUS_COMPLETED, firstChild.GetFlowStatus())
	require.Equal(t, startResponse.GetRunId()+subFlowParentStep+"-1", firstChild.GetRequestId())
	childDescription, err := runtime.UnifiedClient.DescribeWorkflowExecution(
		ctx,
		childFlowID,
		firstChild.GetFlowExecutionId().GetRunId(),
		map[string]dexpb.IndexType{
			service.SearchAttributeDexParentFlowID: dexpb.IndexType_INDEX_TYPE_KEYWORD,
		},
	)
	require.NoError(t, err)
	require.Equal(
		t,
		parentFlowID,
		childDescription.IndexedAttributes[service.SearchAttributeDexParentFlowID].GetStringValue(),
	)
	require.Len(t, childDescription.Memos, 1)
	require.Equal(
		t,
		startResponse.GetRunId()+subFlowParentStep+"-1",
		string(childDescription.Memos["ReqId"].GetObjValue().GetPayload()),
	)
	objectCount, err := globalBlobStore.CountWorkflowObjectsForTesting(ctx, childFlowID)
	require.NoError(t, err)
	require.Equal(t, int64(2), objectCount)
	if backendType == service.BackendTypeTemporal {
		requireTemporalHistoryStoresBlobIdsNotPayloads(
			t,
			ctx,
			runtime.UnifiedClient,
			parentFlowID,
			[]string{"local-store-id|"},
			[]string{subFlowInput, subFlowAttribute},
		)
	}

	resetResponse, err := flowClient.ResetFlow(ctx, &dexpb.ResetFlowRequest{
		FlowId:    parentFlowID,
		RunId:     startResponse.GetRunId(),
		ResetType: dexpb.FlowResetType_FLOW_RESET_TYPE_BEGINNING,
	})
	require.NoError(t, err)
	secondResult, err := flowClient.WaitForFlow(ctx, &dexpb.WaitForFlowRequest{
		FlowId:          parentFlowID,
		NeedsResults:    true,
		WaitTimeSeconds: 30,
	})
	require.NoError(t, err)
	require.Equal(t, dexpb.FlowStatus_FLOW_STATUS_COMPLETED, secondResult.GetFlowStatus())

	secondChild, err := flowClient.GetFlowSummary(ctx, &dexpb.GetFlowSummaryRequest{
		FlowId: childFlowID,
	})
	require.NoError(t, err)
	if shouldRestart {
		require.NotEqual(t, firstChild.GetFlowExecutionId().GetRunId(), secondChild.GetFlowExecutionId().GetRunId())
		require.Equal(t, resetResponse.GetRunId()+subFlowParentStep+"-1", secondChild.GetRequestId())
	} else {
		require.Equal(t, firstChild.GetFlowExecutionId().GetRunId(), secondChild.GetFlowExecutionId().GetRunId())
		require.Equal(t, firstChild.GetRequestId(), secondChild.GetRequestId())
	}
	require.Equal(t, []subFlowObservation{
		{status: dexpb.FlowStatus_FLOW_STATUS_COMPLETED},
		{status: dexpb.FlowStatus_FLOW_STATUS_COMPLETED},
	}, handler.results())
}

func (h *subFlowHandler) InvokeWaitForMethod(
	_ context.Context,
	request *dexpb.InvokeWaitForMethodRequest,
) (*dexpb.InvokeWaitForMethodResponse, error) {
	common.LogRequest("received SubFlow waitFor request, ", request)
	if request.GetFlowType() != subFlowParentType || request.GetStepType() != subFlowParentStep {
		return nil, status.Error(codes.InvalidArgument, "unexpected waitFor invocation")
	}
	return &dexpb.InvokeWaitForMethodResponse{
		WaitingCondition: &dexpb.WaitingCondition{
			WaitingConditionType: dexpb.WaitingConditionType_WAITING_CONDITION_TYPE_ALL_COMPLETED,
			SubFlowConditions: []*dexpb.SubFlowCondition{{
				SubFlowType:   subFlowChildType,
				StartStepType: subFlowChildStep,
				StepInput:     request.GetStepInput(),
				StepOptions:   &dexpb.StepOptions{SkipWaitFor: true},
				Options: &dexpb.SubFlowOptions{
					ReusePolicy: h.reusePolicy,
					Attributes: []*dexpb.AttributeWrite{{
						Key:   "child-attribute",
						Value: stringValue(subFlowAttribute),
					}},
				},
			}},
		},
	}, nil
}

func (h *subFlowHandler) InvokeExecuteMethod(
	_ context.Context,
	request *dexpb.InvokeExecuteMethodRequest,
) (*dexpb.InvokeExecuteMethodResponse, error) {
	common.LogRequest("received SubFlow execute request, ", request)
	switch request.GetFlowType() {
	case subFlowChildType:
		if request.GetStepType() != subFlowChildStep {
			return nil, status.Error(codes.InvalidArgument, "unexpected child Step")
		}
		attributes := request.GetAttributes()
		if len(attributes) != 1 || attributes[0].GetKey() != "child-attribute" ||
			attributes[0].GetValue().GetStringValue() != subFlowAttribute {
			return nil, status.Error(codes.InvalidArgument, "unexpected child Attributes")
		}
		return &dexpb.InvokeExecuteMethodResponse{
			StepDecision: &dexpb.StepDecision{
				CloseDecision: common.GracefulCompleteDecision(request.GetStepInput()),
			},
		}, nil
	case subFlowParentType:
		if request.GetStepType() != subFlowParentStep ||
			request.GetConditionResults().GetSubFlowResults() == nil {
			return nil, status.Error(codes.InvalidArgument, "unexpected parent execute invocation")
		}
		results := request.GetConditionResults().GetSubFlowResults()
		if len(results) != 1 || results[0].GetFlowStatus() != dexpb.FlowStatus_FLOW_STATUS_COMPLETED ||
			len(results[0].GetResults()) != 1 {
			return nil, status.Error(codes.InvalidArgument, "invalid SubFlow result")
		}
		h.mu.Lock()
		h.observations = append(h.observations, subFlowObservation{
			status: results[0].GetFlowStatus(),
		})
		h.mu.Unlock()
		return &dexpb.InvokeExecuteMethodResponse{
			StepDecision: &dexpb.StepDecision{
				CloseDecision: common.GracefulCompleteDecision(
					results[0].GetResults()[0].GetCompletedStepOutput(),
				),
			},
		}, nil
	default:
		return nil, status.Error(codes.InvalidArgument, "unexpected Flow type")
	}
}

func (h *subFlowHandler) results() []subFlowObservation {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]subFlowObservation(nil), h.observations...)
}
