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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/superdurable/dex/config"
	"github.com/superdurable/dex/gen/dexpb"
	"github.com/superdurable/dex/integ/workflow/common"
	"github.com/superdurable/dex/service/tenancy"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

const (
	tenantProbeFlowType    = "TenantProbeFlow"
	tenantProbeStep        = "ProbeStep"
	tenantProbeChannel     = "go"
	tenantProbeRPC         = "describeFlow"
	tenantProbeNestingFlow = "TenantNestingFlow"
	tenantProbeNestingStep = "NestingStep"
)

// tenantProbeHandler waits on a Channel, completes with its Flow ID, and nests SubFlows to the requested depth.
type tenantProbeHandler struct {
	mu              sync.Mutex
	observedFlowIDs []string
}

func (h *tenantProbeHandler) observe(flowID string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.observedFlowIDs = append(h.observedFlowIDs, flowID)
}

func (h *tenantProbeHandler) observed() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.observedFlowIDs...)
}

func (h *tenantProbeHandler) InvokeWaitForMethod(
	_ context.Context,
	request *dexpb.InvokeWaitForMethodRequest,
) (*dexpb.InvokeWaitForMethodResponse, error) {
	h.observe(request.GetContext().GetFlowId())
	if request.GetFlowType() == tenantProbeNestingFlow {
		depth := request.GetStepInput().GetIntValue()
		if depth == 0 {
			return &dexpb.InvokeWaitForMethodResponse{}, nil
		}
		return &dexpb.InvokeWaitForMethodResponse{WaitingCondition: &dexpb.WaitingCondition{
			WaitingConditionType: dexpb.WaitingConditionType_WAITING_CONDITION_TYPE_ALL_COMPLETED,
			SubFlowConditions: []*dexpb.SubFlowCondition{{
				SubFlowType:   tenantProbeNestingFlow,
				StartStepType: tenantProbeNestingStep,
				StepInput:     intValue(depth - 1),
			}},
		}}, nil
	}
	return &dexpb.InvokeWaitForMethodResponse{WaitingCondition: &dexpb.WaitingCondition{
		WaitingConditionType: dexpb.WaitingConditionType_WAITING_CONDITION_TYPE_ALL_COMPLETED,
		ChannelConditions:    []*dexpb.ChannelCondition{{ChannelName: tenantProbeChannel}},
	}}, nil
}

func (h *tenantProbeHandler) InvokeExecuteMethod(
	_ context.Context,
	request *dexpb.InvokeExecuteMethodRequest,
) (*dexpb.InvokeExecuteMethodResponse, error) {
	h.observe(request.GetContext().GetFlowId())
	return &dexpb.InvokeExecuteMethodResponse{StepDecision: &dexpb.StepDecision{
		CloseDecision: common.GracefulCompleteDecision(stringValue(request.GetContext().GetFlowId())),
	}}, nil
}

func (h *tenantProbeHandler) InvokeWorkerRPC(
	_ context.Context,
	request *dexpb.InvokeWorkerRPCRequest,
) (*dexpb.InvokeWorkerRPCResponse, error) {
	if request.GetRpcName() != tenantProbeRPC {
		return nil, status.Error(codes.InvalidArgument, "unknown RPC")
	}
	h.observe(request.GetContext().GetFlowId())
	return &dexpb.InvokeWorkerRPCResponse{Output: stringValue(request.GetContext().GetFlowId())}, nil
}

// tenantPair is one multi-tenant server with a client and Worker for each of two tenants.
type tenantPair struct {
	runtime                 *integRuntime
	environment             *tenancyTestEnvironment
	alpha, beta             dexpb.FlowServiceClient
	alphaWorker, betaWorker *tenantProbeHandler
	alphaTarget, betaTarget *dexpb.WorkerTarget
}

func startTenantPair(t *testing.T, configure ...func(*DexServiceTestConfig)) *tenantPair {
	t.Helper()
	environment := newTenancyTestEnvironment(t)
	pair := &tenantPair{
		environment: environment,
		alphaWorker: &tenantProbeHandler{},
		betaWorker:  &tenantProbeHandler{},
	}
	pair.alphaTarget = startWorker(t, pair.alphaWorker)
	pair.betaTarget = startWorker(t, pair.betaWorker)
	pair.runtime = environment.startTenantDexService(t, "alpha", configure...)
	pair.alpha = pair.runtime.FlowClient
	pair.beta = newFlowClientWithCredential(t, pair.runtime.PublicAddress, environment.TenantJWT(t, "beta"))
	return pair
}

func startTenantProbeFlow(
	t *testing.T,
	ctx context.Context,
	client dexpb.FlowServiceClient,
	workerTarget *dexpb.WorkerTarget,
	flowID string,
	flowType string,
	input *dexpb.Value,
) string {
	t.Helper()
	startStep := tenantProbeStep
	if flowType == tenantProbeNestingFlow {
		startStep = tenantProbeNestingStep
	}
	response, err := client.StartFlow(ctx, &dexpb.StartFlowRequest{
		RequestId:          newRequestID(),
		FlowId:             flowID,
		FlowType:           flowType,
		FlowTimeoutSeconds: 120,
		StartStepType:      startStep,
		StepInput:          input,
		FlowStartOptions:   withWorkerTarget(nil, workerTarget),
	})
	require.NoError(t, err)
	return response.GetRunId()
}

func TestTenantIdentityMappingTemporal(t *testing.T) {
	if !*temporalIntegTest || *dexServerAddress != "" {
		t.Skip()
	}
	pair := startTenantPair(t, func(testConfig *DexServiceTestConfig) {
		testConfig.StreamStore = config.StreamStoreConfig{Backend: config.StreamStoreBackendMemory}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	sharedFlowID := "shared-" + uuid.NewString()
	alphaRunID := startTenantProbeFlow(t, ctx, pair.alpha, pair.alphaTarget, sharedFlowID, tenantProbeFlowType, stringValue("a"))
	startTenantProbeFlow(t, ctx, pair.beta, pair.betaTarget, sharedFlowID, tenantProbeFlowType, stringValue("b"))
	alphaOnlyFlowID := "alpha-only-" + uuid.NewString()
	alphaOnlyRunID := startTenantProbeFlow(t, ctx, pair.alpha, pair.alphaTarget, alphaOnlyFlowID, tenantProbeFlowType, stringValue("a"))

	t.Run("the stored Flow ID carries exactly one tenant prefix", func(t *testing.T) {
		for _, tenantID := range []string{"alpha", "beta"} {
			description, err := pair.runtime.UnifiedClient.DescribeWorkflowExecution(
				ctx, tenancy.StoredFlowID(tenantID, sharedFlowID), "", nil,
			)
			require.NoError(t, err, tenantID)
			require.NotNil(t, description)
		}
		_, err := pair.runtime.UnifiedClient.DescribeWorkflowExecution(ctx, sharedFlowID, "", nil)
		require.Error(t, err, "no Flow is stored under the bare application Flow ID")
	})

	t.Run("another tenant's Flow ID is NotFound", func(t *testing.T) {
		calls := map[string]func() error{
			"PublishToChannel": func() error {
				_, err := pair.beta.PublishToChannel(ctx, &dexpb.PublishToChannelRequest{
					FlowId:   alphaOnlyFlowID,
					Messages: []*dexpb.ChannelMessage{{ChannelName: tenantProbeChannel}},
				})
				return err
			},
			"GetChannelMessages": func() error {
				_, err := pair.beta.GetChannelMessages(ctx, &dexpb.GetChannelMessagesRequest{
					FlowId: alphaOnlyFlowID, ChannelName: tenantProbeChannel,
				})
				return err
			},
			"DeleteChannelMessage": func() error {
				_, err := pair.beta.DeleteChannelMessage(ctx, &dexpb.DeleteChannelMessageRequest{
					FlowId: alphaOnlyFlowID, ChannelName: tenantProbeChannel, MessageId: uuid.NewString(),
					RequestId: uuid.NewString(),
				})
				return err
			},
			"StopFlow": func() error {
				_, err := pair.beta.StopFlow(ctx, &dexpb.StopFlowRequest{FlowId: alphaOnlyFlowID})
				return err
			},
			"ResetFlow": func() error {
				_, err := pair.beta.ResetFlow(ctx, &dexpb.ResetFlowRequest{
					FlowId: alphaOnlyFlowID, ResetType: dexpb.FlowResetType_FLOW_RESET_TYPE_BEGINNING,
				})
				return err
			},
			"SkipTimer": func() error {
				_, err := pair.beta.SkipTimer(ctx, &dexpb.SkipTimerRequest{
					FlowId: alphaOnlyFlowID, StepExecutionId: tenantProbeStep + "-1", TimerConditionId: "timer",
				})
				return err
			},
			"TriggerContinueAsNew": func() error {
				_, err := pair.beta.TriggerContinueAsNew(ctx, &dexpb.TriggerContinueAsNewRequest{FlowId: alphaOnlyFlowID})
				return err
			},
			"GetAttributes": func() error {
				_, err := pair.beta.GetAttributes(ctx, &dexpb.GetAttributesRequest{FlowId: alphaOnlyFlowID, AllKeys: true})
				return err
			},
			"SetAttributes": func() error {
				_, err := pair.beta.SetAttributes(ctx, &dexpb.SetAttributesRequest{
					FlowId:     alphaOnlyFlowID,
					Attributes: []*dexpb.AttributeWrite{{Key: "key", Value: stringValue("value")}},
					RequestId:  uuid.NewString(),
				})
				return err
			},
			"WaitForAttribute": func() error {
				_, err := pair.beta.WaitForAttribute(ctx, &dexpb.WaitForAttributeRequest{
					FlowId: alphaOnlyFlowID,
					Match: &dexpb.AttributeMatch{
						Key:      "key",
						Operator: dexpb.AttributeMatchOperator_ATTRIBUTE_MATCH_OPERATOR_EQUAL,
						Operand:  stringValue("value"),
					},
					RequestTimeoutSeconds: 2,
				})
				return err
			},
			"InvokeRPC": func() error {
				_, err := pair.beta.InvokeRPC(ctx, &dexpb.InvokeRPCRequest{
					FlowId: alphaOnlyFlowID, RpcName: tenantProbeRPC, RequestId: uuid.NewString(),
				})
				return err
			},
			"UpdateFlowConfig": func() error {
				_, err := pair.beta.UpdateFlowConfig(ctx, &dexpb.UpdateFlowConfigRequest{
					FlowId: alphaOnlyFlowID, FlowConfig: &dexpb.FlowConfig{WorkerTarget: pair.betaTarget},
				})
				return err
			},
			"WaitForFlow": func() error {
				_, err := pair.beta.WaitForFlow(ctx, &dexpb.WaitForFlowRequest{FlowId: alphaOnlyFlowID, WaitTimeSeconds: 1})
				return err
			},
			"WaitForStepCompletion": func() error {
				_, err := pair.beta.WaitForStepCompletion(ctx, &dexpb.WaitForStepCompletionRequest{
					FlowId: alphaOnlyFlowID, StepType: tenantProbeStep, StepExecutionNumber: "1", RequestTimeoutSeconds: 2,
				})
				return err
			},
			"GetFlowSummary": func() error {
				_, err := pair.beta.GetFlowSummary(ctx, &dexpb.GetFlowSummaryRequest{FlowId: alphaOnlyFlowID})
				return err
			},
			"GetFlowState": func() error {
				_, err := pair.beta.GetFlowState(ctx, &dexpb.GetFlowStateRequest{FlowId: alphaOnlyFlowID})
				return err
			},
			"GetHistoryEvents": func() error {
				_, err := pair.beta.GetHistoryEvents(ctx, &dexpb.GetHistoryEventsRequest{
					FlowId: alphaOnlyFlowID, RunId: alphaOnlyRunID,
				})
				return err
			},
			"WaitForHistoryEvent": func() error {
				_, err := pair.beta.WaitForHistoryEvent(ctx, &dexpb.WaitForHistoryEventRequest{
					FlowId: alphaOnlyFlowID, RunId: alphaOnlyRunID, NextInternalEventId: 1,
				})
				return err
			},
		}
		for name, call := range calls {
			t.Run(name, func(t *testing.T) {
				err := call()
				require.Equal(t, codes.NotFound, status.Code(err), "error: %v", err)
				require.NotContains(t, err.Error(), "T:beta:", "errors name only application Flow IDs")
			})
		}
	})

	t.Run("Streams of equal Flow IDs are separate per tenant", func(t *testing.T) {
		for _, write := range []struct {
			client dexpb.FlowServiceClient
			value  string
		}{{pair.alpha, "alpha-message"}, {pair.beta, "beta-message"}} {
			_, err := write.client.WriteStream(ctx, &dexpb.WriteStreamRequest{
				FlowId: sharedFlowID, FlowType: tenantProbeFlowType, StreamName: "events",
				StreamCapacityBytes: 64 * 1024, Value: stringValue(write.value), Source: "test",
			})
			require.NoError(t, err)
		}
		for _, read := range []struct {
			client dexpb.FlowServiceClient
			value  string
		}{{pair.alpha, "alpha-message"}, {pair.beta, "beta-message"}} {
			response, err := read.client.ListStreamMessages(ctx, &dexpb.ListStreamMessagesRequest{
				FlowId: sharedFlowID, FlowType: tenantProbeFlowType, StreamName: "events", PageSize: 10,
			})
			require.NoError(t, err)
			require.Len(t, response.GetMessages(), 1)
			require.Equal(t, read.value, response.GetMessages()[0].GetValue().GetStringValue())
		}
	})

	t.Run("a blob locator of another tenant is refused", func(t *testing.T) {
		_, err := pair.alpha.SetAttributes(ctx, &dexpb.SetAttributesRequest{
			FlowId:     sharedFlowID,
			Attributes: []*dexpb.AttributeWrite{{Key: "large", Value: stringValue(strings.Repeat("z", 4096))}},
			RequestId:  uuid.NewString(),
		})
		require.NoError(t, err)
		attributes, err := pair.alpha.GetAttributes(ctx, &dexpb.GetAttributesRequest{
			FlowId: sharedFlowID, Keys: []string{"large"},
		})
		require.NoError(t, err)
		require.Len(t, attributes.GetAttributes(), 1)
		blobValue := attributes.GetAttributes()[0].GetValue()
		require.NotEmpty(t, blobValue.GetInternalBlobIdForStringValue(), "the large value must be offloaded")
		loadRequest := &dexpb.LoadBlobsRequest{Entries: []*dexpb.LoadBlobRequestEntry{{
			FlowId: sharedFlowID, BlobValue: blobValue,
		}}}
		loaded, err := pair.alpha.LoadBlobs(ctx, loadRequest)
		require.NoError(t, err)
		require.Equal(
			t,
			strings.Repeat("z", 4096),
			loaded.GetValues()[blobValue.GetInternalBlobIdForStringValue()].GetStringValue(),
		)
		betaLoaded, err := pair.beta.LoadBlobs(ctx, loadRequest)
		require.NoError(t, err)
		require.Empty(t, betaLoaded.GetValues(), "the locator must not resolve under the other tenant's equal Flow ID")
	})

	t.Run("the Worker sees application Flow IDs in Steps and RPCs", func(t *testing.T) {
		response, err := pair.alpha.InvokeRPC(ctx, &dexpb.InvokeRPCRequest{
			FlowId: sharedFlowID, RpcName: tenantProbeRPC, RequestId: uuid.NewString(), TimeoutSeconds: 10,
		})
		require.NoError(t, err)
		require.Equal(t, sharedFlowID, response.GetOutput().GetStringValue())
		_, err = pair.alpha.PublishToChannel(ctx, &dexpb.PublishToChannelRequest{
			FlowId:   sharedFlowID,
			Messages: []*dexpb.ChannelMessage{{ChannelName: tenantProbeChannel}},
		})
		require.NoError(t, err)
		result, err := pair.alpha.WaitForFlow(ctx, &dexpb.WaitForFlowRequest{
			FlowId: sharedFlowID, NeedsResults: true, WaitTimeSeconds: 30,
		})
		require.NoError(t, err)
		require.Equal(t, dexpb.FlowStatus_FLOW_STATUS_COMPLETED, result.GetFlowStatus())
		require.Equal(t, sharedFlowID, result.GetResults()[0].GetCompletedStepOutput().GetStringValue())
		for _, observed := range pair.alphaWorker.observed() {
			require.False(t, strings.HasPrefix(observed, "T:"), observed)
		}

		betaState, err := pair.beta.GetFlowSummary(ctx, &dexpb.GetFlowSummaryRequest{FlowId: sharedFlowID})
		require.NoError(t, err)
		require.Equal(t, dexpb.FlowStatus_FLOW_STATUS_RUNNING, betaState.GetFlowStatus(),
			"the other tenant's Flow with the same Flow ID is unaffected")
	})

	t.Run("summaries and history show only application Flow IDs", func(t *testing.T) {
		summary, err := pair.alpha.GetFlowSummary(ctx, &dexpb.GetFlowSummaryRequest{FlowId: sharedFlowID})
		require.NoError(t, err)
		require.Equal(t, sharedFlowID, summary.GetFlowExecutionId().GetFlowId())
		history, err := pair.alpha.GetHistoryEvents(ctx, &dexpb.GetHistoryEventsRequest{
			FlowId: sharedFlowID, RunId: alphaRunID,
		})
		require.NoError(t, err)
		encoded, err := protojson.Marshal(history)
		require.NoError(t, err)
		require.Contains(t, string(encoded), sharedFlowID)
		require.NotContains(t, string(encoded), "T:alpha:")
	})

	t.Run("a Flow ID longer than the tenant budget is refused", func(t *testing.T) {
		budget := pair.environment.Config.MaxStoredFlowIDBytes - len(tenancy.StoredFlowID("alpha", ""))
		uniquePrefix := uuid.NewString()
		_, err := pair.alpha.StartFlow(ctx, &dexpb.StartFlowRequest{
			RequestId: newRequestID(), FlowId: uniquePrefix + strings.Repeat("x", budget+1-len(uniquePrefix)),
			FlowType:      tenantProbeFlowType,
			StartStepType: tenantProbeStep, FlowStartOptions: withWorkerTarget(nil, pair.alphaTarget),
		})
		require.Equal(t, codes.InvalidArgument, status.Code(err), "error: %v", err)
		maximumFlowID := uniquePrefix + strings.Repeat("y", budget-len(uniquePrefix))
		startTenantProbeFlow(t, ctx, pair.alpha, pair.alphaTarget, maximumFlowID, tenantProbeFlowType, nil)
		_, err = pair.alpha.StopFlow(ctx, &dexpb.StopFlowRequest{FlowId: maximumFlowID})
		require.NoError(t, err)
	})
}

func TestTenantSubFlowIdentityTemporal(t *testing.T) {
	if !*temporalIntegTest || *dexServerAddress != "" {
		t.Skip()
	}
	pair := startTenantPair(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	t.Run("nested SubFlows show application IDs and store one prefix", func(t *testing.T) {
		parentFlowID := "parent-" + uuid.NewString()
		startTenantProbeFlow(t, ctx, pair.alpha, pair.alphaTarget, parentFlowID, tenantProbeNestingFlow, intValue(2))
		result, err := pair.alpha.WaitForFlow(ctx, &dexpb.WaitForFlowRequest{
			FlowId: parentFlowID, NeedsResults: true, WaitTimeSeconds: 60,
		})
		require.NoError(t, err)
		require.Equal(t, dexpb.FlowStatus_FLOW_STATUS_COMPLETED, result.GetFlowStatus())

		childFlowID := "SubFlow:" + parentFlowID + "-" + tenantProbeNestingStep + "-1-0"
		grandchildFlowID := "SubFlow:" + childFlowID + "-" + tenantProbeNestingStep + "-1-0"
		require.Subset(t, pair.alphaWorker.observed(), []string{parentFlowID, childFlowID, grandchildFlowID})
		for _, observed := range pair.alphaWorker.observed() {
			require.False(t, strings.Contains(observed, "T:"), observed)
		}
		for _, subFlowID := range []string{childFlowID, grandchildFlowID} {
			summary, err := pair.alpha.GetFlowSummary(ctx, &dexpb.GetFlowSummaryRequest{FlowId: subFlowID})
			require.NoError(t, err)
			require.Equal(t, subFlowID, summary.GetFlowExecutionId().GetFlowId())
			_, err = pair.beta.GetFlowSummary(ctx, &dexpb.GetFlowSummaryRequest{FlowId: subFlowID})
			require.Equal(t, codes.NotFound, status.Code(err), "error: %v", err)
			_, err = pair.runtime.UnifiedClient.DescribeWorkflowExecution(
				ctx, tenancy.StoredFlowID("alpha", subFlowID), "", nil,
			)
			require.NoError(t, err)
		}
	})

	t.Run("a SubFlow over the length limit fails its Step before any start", func(t *testing.T) {
		budget := pair.environment.Config.MaxStoredFlowIDBytes - len(tenancy.StoredFlowID("alpha", ""))
		uniquePrefix := uuid.NewString()
		parentFlowID := uniquePrefix + strings.Repeat("p", budget-10-len(uniquePrefix))
		startTenantProbeFlow(t, ctx, pair.alpha, pair.alphaTarget, parentFlowID, tenantProbeNestingFlow, intValue(1))
		result, err := pair.alpha.WaitForFlow(ctx, &dexpb.WaitForFlowRequest{
			FlowId: parentFlowID, NeedsResults: true, WaitTimeSeconds: 60,
		})
		require.NoError(t, err)
		require.Equal(t, dexpb.FlowStatus_FLOW_STATUS_FAILED, result.GetFlowStatus())
		childFlowID := "SubFlow:" + parentFlowID + "-" + tenantProbeNestingStep + "-1-0"
		_, err = pair.runtime.UnifiedClient.DescribeWorkflowExecution(
			ctx, tenancy.StoredFlowID("alpha", childFlowID), "", nil,
		)
		require.True(t, pair.runtime.UnifiedClient.IsNotFoundError(err), "error: %v", err)
	})
}
