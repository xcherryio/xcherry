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
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/superdurable/dex/gen/dexpb"
	"github.com/superdurable/dex/integ/workflow/signal"
	"github.com/superdurable/dex/service"
	"github.com/superdurable/dex/service/common/ptr"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	waitForAttributeBlobKey = "wait-for-attribute-blob-key"
	waitForAttributeKey     = "wait-for-attribute-key"
)

func TestWaitForAttributeBlobBackedTemporal(t *testing.T) {
	if !*temporalIntegTest {
		t.Skip()
	}
	for i := 0; i < *repeatIntegTest; i++ {
		doTestWaitForAttributeBlobBacked(t)
		smallWaitForFastTest()
	}
}

func TestWaitForAttributeTemporal(t *testing.T) {
	if !*temporalIntegTest {
		t.Skip()
	}
	for i := 0; i < *repeatIntegTest; i++ {
		doTestWaitForAttributeSuccess(t)
		smallWaitForFastTest()
		doTestWaitForAttributeOperators(t)
		smallWaitForFastTest()
		doTestWaitForAttributeTimeout(t)
		smallWaitForFastTest()
		doTestWaitForAttributeInternalHandlerTimeout(t)
		smallWaitForFastTest()
		doTestWaitForAttributeTransportTimeoutAndReattach(t)
		smallWaitForFastTest()
		doTestWaitForAttributeCancel(t)
		smallWaitForFastTest()
		doTestWaitForAttributeNotFound(t)
		smallWaitForFastTest()
		doTestWaitForAttributeClosed(t)
		smallWaitForFastTest()
		doTestWaitForAttributeConcurrent(t)
		smallWaitForFastTest()
		doTestWaitForAttributeAcrossContinueAsNew(t)
		smallWaitForFastTest()
		doTestWaitForAttributeCounterSuccess(t)
		smallWaitForFastTest()
		doTestWaitForAttributeCounterFailure(t)
		smallWaitForFastTest()
	}
}

func TestWaitForAttributeCadence(t *testing.T) {
	if !*cadenceIntegTest {
		t.Skip()
	}
	for i := 0; i < *repeatIntegTest; i++ {
		doTestWaitForAttributeCadenceUnimplemented(t)
		smallWaitForFastTest()
	}
}

func doTestWaitForAttributeBlobBacked(t *testing.T) {
	workerTarget := startWorker(t, signal.NewHandler())
	runtime := startDexService(t, DexServiceTestConfig{
		BackendType:     service.BackendTypeTemporal,
		S3TestThreshold: 50,
		LazyLoading:     ptr.Any(true),
	})
	flowClient := runtime.FlowClient

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	flowId := "wait-for-attribute-" + uuid.NewString()
	_, err := flowClient.StartFlow(ctx, &dexpb.StartFlowRequest{
		RequestId:          newRequestID(),
		FlowId:             flowId,
		FlowType:           signal.WorkflowType,
		FlowTimeoutSeconds: 30,

		StartStepType:    signal.State1,
		FlowStartOptions: withWorkerTarget(nil, workerTarget),
	})
	require.NoError(t, err)

	largeValue := stringValue(strings.Repeat("x", 120))
	_, err = flowClient.SetAttributes(ctx, &dexpb.SetAttributesRequest{
		RequestId: newRequestID(),
		FlowId:    flowId,
		Attributes: []*dexpb.AttributeWrite{
			{Key: waitForAttributeBlobKey, Value: largeValue},
		},
	})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		response, getErr := flowClient.GetAttributes(ctx, &dexpb.GetAttributesRequest{
			FlowId: flowId,
			Keys:   []string{waitForAttributeBlobKey},
		})
		return getErr == nil && len(response.GetAttributes()) == 1
	}, 5*time.Second, 50*time.Millisecond)

	_, err = flowClient.WaitForAttribute(ctx, &dexpb.WaitForAttributeRequest{
		FlowId: flowId,
		Match: equalAttributeMatch(
			waitForAttributeBlobKey,
			stringValue("anything"),
		),
		RequestTimeoutSeconds: 0,
		RequestId:             uuid.NewString(),
	})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	errResp := grpcServiceErrorResponse(t, err)
	require.Equal(
		t,
		dexpb.ErrorSubStatus_ERROR_SUB_STATUS_UNCATEGORIZED,
		errResp.GetSubStatus(),
	)
	require.Contains(t, errResp.GetDetail(), "blob-backed")
	require.Contains(t, errResp.GetDetail(), waitForAttributeBlobKey)

	_, err = flowClient.StopFlow(ctx, &dexpb.StopFlowRequest{
		FlowId:   flowId,
		StopType: dexpb.StopType_STOP_TYPE_TERMINATE,
	})
	require.NoError(t, err)
}

func doTestWaitForAttributeSuccess(t *testing.T) {
	workerTarget := startWorker(t, signal.NewHandler())
	runtime := startDexService(t, DexServiceTestConfig{BackendType: service.BackendTypeTemporal})
	flowClient := runtime.FlowClient

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	flowId := startParkedWaitForAttributeFlow(t, ctx, flowClient, workerTarget, nil)
	expectedValue := stringValue("wait-for-attribute-success")

	_, err := flowClient.SetAttributes(ctx, &dexpb.SetAttributesRequest{
		RequestId: newRequestID(),
		FlowId:    flowId,
		Attributes: []*dexpb.AttributeWrite{
			{Key: waitForAttributeKey, Value: expectedValue},
		},
	})
	require.NoError(t, err)

	response, err := flowClient.WaitForAttribute(ctx, &dexpb.WaitForAttributeRequest{
		FlowId: flowId,
		Match: equalAttributeMatch(
			waitForAttributeKey,
			expectedValue,
		),
		RequestTimeoutSeconds: 0,
	})
	require.NoError(t, err)
	require.True(t, proto.Equal(expectedValue, response.GetMatchedValue()))

	_, err = flowClient.SetAttributes(ctx, &dexpb.SetAttributesRequest{
		RequestId: newRequestID(),
		FlowId:    flowId,
		Attributes: []*dexpb.AttributeWrite{
			{Key: waitForAttributeKey, Value: intValue(1)},
		},
	})
	require.NoError(t, err)
	description, err := runtime.UnifiedClient.DescribeWorkflowExecution(ctx, flowId, "", nil)
	require.NoError(t, err)
	nonEqualRequestID := "wait-for-attribute:" + waitForAttributeKey + ">5"
	nonEqualResponse := make(chan *dexpb.WaitForAttributeResponse, 1)
	nonEqualError := make(chan error, 1)
	go func() {
		matchedResponse, waitErr := flowClient.WaitForAttribute(ctx, &dexpb.WaitForAttributeRequest{
			FlowId: flowId,
			Match: waitForAttributeMatch(
				waitForAttributeKey,
				dexpb.AttributeMatchOperator_ATTRIBUTE_MATCH_OPERATOR_GREATER_THAN,
				intValue(5),
			),
			RequestTimeoutSeconds: 0,
		})
		nonEqualResponse <- matchedResponse
		nonEqualError <- waitErr
	}()
	require.Eventually(t, func() bool {
		accepted, _ := countTemporalUpdateEvents(
			t,
			ctx,
			runtime,
			flowId,
			description.RunId,
			nonEqualRequestID,
		)
		return accepted == 1
	}, 5*time.Second, 50*time.Millisecond)
	_, err = flowClient.SetAttributes(ctx, &dexpb.SetAttributesRequest{
		RequestId: newRequestID(),
		FlowId:    flowId,
		Attributes: []*dexpb.AttributeWrite{
			{Key: waitForAttributeKey, Value: intValue(7)},
		},
	})
	require.NoError(t, err)
	require.NoError(t, <-nonEqualError)
	require.True(t, proto.Equal(intValue(7), (<-nonEqualResponse).GetMatchedValue()))

	stopParkedWaitForAttributeFlow(t, ctx, flowClient, flowId)
}

func doTestWaitForAttributeOperators(t *testing.T) {
	workerTarget := startWorker(t, signal.NewHandler())
	runtime := startDexService(t, DexServiceTestConfig{BackendType: service.BackendTypeTemporal})
	flowClient := runtime.FlowClient

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	flowId := startParkedWaitForAttributeFlow(t, ctx, flowClient, workerTarget, nil)
	valuesByKey := map[string]*dexpb.Value{
		"match-string":    stringValue("ready"),
		"match-bool":      boolValue(true),
		"match-int":       intValue(3),
		"match-double":    doubleValue(3.5),
		"match-object":    jsonObjValue(map[string]string{"state": "ready"}),
		"match-nonfinite": doubleValue(math.Inf(1)),
	}
	writes := make([]*dexpb.AttributeWrite, 0, len(valuesByKey))
	for key, value := range valuesByKey {
		writes = append(writes, &dexpb.AttributeWrite{Key: key, Value: value})
	}
	_, err := flowClient.SetAttributes(ctx, &dexpb.SetAttributesRequest{
		RequestId:  newRequestID(),
		FlowId:     flowId,
		Attributes: writes,
	})
	require.NoError(t, err)

	testCases := []struct {
		name     string
		key      string
		operator dexpb.AttributeMatchOperator
		operand  *dexpb.Value
	}{
		{name: "string equal", key: "match-string", operator: dexpb.AttributeMatchOperator_ATTRIBUTE_MATCH_OPERATOR_EQUAL, operand: stringValue("ready")},
		{name: "string not equal", key: "match-string", operator: dexpb.AttributeMatchOperator_ATTRIBUTE_MATCH_OPERATOR_NOT_EQUAL, operand: stringValue("pending")},
		{name: "bool equal", key: "match-bool", operator: dexpb.AttributeMatchOperator_ATTRIBUTE_MATCH_OPERATOR_EQUAL, operand: boolValue(true)},
		{name: "bool not equal", key: "match-bool", operator: dexpb.AttributeMatchOperator_ATTRIBUTE_MATCH_OPERATOR_NOT_EQUAL, operand: boolValue(false)},
		{name: "int equal", key: "match-int", operator: dexpb.AttributeMatchOperator_ATTRIBUTE_MATCH_OPERATOR_EQUAL, operand: intValue(3)},
		{name: "int not equal", key: "match-int", operator: dexpb.AttributeMatchOperator_ATTRIBUTE_MATCH_OPERATOR_NOT_EQUAL, operand: intValue(2)},
		{name: "int greater", key: "match-int", operator: dexpb.AttributeMatchOperator_ATTRIBUTE_MATCH_OPERATOR_GREATER_THAN, operand: intValue(0)},
		{name: "int greater equal", key: "match-int", operator: dexpb.AttributeMatchOperator_ATTRIBUTE_MATCH_OPERATOR_GREATER_THAN_OR_EQUAL, operand: intValue(3)},
		{name: "int less", key: "match-int", operator: dexpb.AttributeMatchOperator_ATTRIBUTE_MATCH_OPERATOR_LESS_THAN, operand: intValue(4)},
		{name: "int less equal", key: "match-int", operator: dexpb.AttributeMatchOperator_ATTRIBUTE_MATCH_OPERATOR_LESS_THAN_OR_EQUAL, operand: intValue(3)},
		{name: "double equal", key: "match-double", operator: dexpb.AttributeMatchOperator_ATTRIBUTE_MATCH_OPERATOR_EQUAL, operand: doubleValue(3.5)},
		{name: "double not equal", key: "match-double", operator: dexpb.AttributeMatchOperator_ATTRIBUTE_MATCH_OPERATOR_NOT_EQUAL, operand: doubleValue(2.5)},
		{name: "double greater", key: "match-double", operator: dexpb.AttributeMatchOperator_ATTRIBUTE_MATCH_OPERATOR_GREATER_THAN, operand: doubleValue(3.0)},
		{name: "double greater equal", key: "match-double", operator: dexpb.AttributeMatchOperator_ATTRIBUTE_MATCH_OPERATOR_GREATER_THAN_OR_EQUAL, operand: doubleValue(3.5)},
		{name: "double less", key: "match-double", operator: dexpb.AttributeMatchOperator_ATTRIBUTE_MATCH_OPERATOR_LESS_THAN, operand: doubleValue(4.0)},
		{name: "double less equal", key: "match-double", operator: dexpb.AttributeMatchOperator_ATTRIBUTE_MATCH_OPERATOR_LESS_THAN_OR_EQUAL, operand: doubleValue(3.5)},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			response, waitErr := flowClient.WaitForAttribute(ctx, &dexpb.WaitForAttributeRequest{
				FlowId:                flowId,
				Match:                 waitForAttributeMatch(testCase.key, testCase.operator, testCase.operand),
				RequestTimeoutSeconds: 0,
				RequestId:             uuid.NewString(),
			})
			require.NoError(t, waitErr)
			require.True(t, proto.Equal(valuesByKey[testCase.key], response.GetMatchedValue()))
		})
	}

	invalidMatches := []struct {
		name  string
		match *dexpb.AttributeMatch
	}{
		{name: "unspecified", match: waitForAttributeMatch("match-int", dexpb.AttributeMatchOperator_ATTRIBUTE_MATCH_OPERATOR_UNSPECIFIED, intValue(0))},
		{name: "string ordering", match: waitForAttributeMatch("match-string", dexpb.AttributeMatchOperator_ATTRIBUTE_MATCH_OPERATOR_GREATER_THAN, stringValue("a"))},
		{name: "object", match: equalAttributeMatch("match-string", &dexpb.Value{Kind: &dexpb.Value_ObjValue{ObjValue: &dexpb.EncodedObject{Encoding: "json", Payload: []byte("{}")}}})},
		{name: "null", match: equalAttributeMatch("match-string", &dexpb.Value{Kind: &dexpb.Value_NullValue{}})},
		{name: "non-finite", match: equalAttributeMatch("match-double", doubleValue(math.Inf(1)))},
	}
	for _, testCase := range invalidMatches {
		t.Run(testCase.name, func(t *testing.T) {
			_, waitErr := flowClient.WaitForAttribute(ctx, &dexpb.WaitForAttributeRequest{
				FlowId:                flowId,
				Match:                 testCase.match,
				RequestTimeoutSeconds: 0,
				RequestId:             uuid.NewString(),
			})
			require.Equal(t, codes.InvalidArgument, status.Code(waitErr))
		})
	}

	for _, waitRequest := range []*dexpb.WaitForAttributeRequest{
		{
			FlowId:                flowId,
			Match:                 equalAttributeMatch("match-string", stringValue("ready")),
			RequestTimeoutSeconds: -1,
		},
		{
			FlowId:                        flowId,
			Match:                         equalAttributeMatch("match-string", stringValue("ready")),
			InternalHandlerTimeoutSeconds: -1,
		},
	} {
		_, waitErr := flowClient.WaitForAttribute(ctx, waitRequest)
		require.Equal(t, codes.InvalidArgument, status.Code(waitErr))
	}

	invalidStoredValues := []struct {
		name    string
		key     string
		operand *dexpb.Value
	}{
		{name: "stored object", key: "match-object", operand: stringValue("ready")},
		{name: "stored non-finite", key: "match-nonfinite", operand: doubleValue(0)},
	}
	for _, testCase := range invalidStoredValues {
		t.Run(testCase.name, func(t *testing.T) {
			_, waitErr := flowClient.WaitForAttribute(ctx, &dexpb.WaitForAttributeRequest{
				FlowId:                flowId,
				Match:                 equalAttributeMatch(testCase.key, testCase.operand),
				RequestTimeoutSeconds: 0,
				RequestId:             uuid.NewString(),
			})
			require.Equal(t, codes.FailedPrecondition, status.Code(waitErr))
		})
	}

	_, err = flowClient.WaitForAttribute(ctx, &dexpb.WaitForAttributeRequest{
		FlowId: flowId,
		Match: waitForAttributeMatch(
			"match-string",
			dexpb.AttributeMatchOperator_ATTRIBUTE_MATCH_OPERATOR_GREATER_THAN,
			intValue(0),
		),
		RequestTimeoutSeconds: 1,
		RequestId:             uuid.NewString(),
	})
	require.Equal(t, codes.DeadlineExceeded, status.Code(err))
	require.Equal(
		t,
		dexpb.ErrorSubStatus_ERROR_SUB_STATUS_REQUEST_TIMEOUT,
		grpcServiceErrorResponse(t, err).GetSubStatus(),
	)

	stopParkedWaitForAttributeFlow(t, ctx, flowClient, flowId)
}

func doTestWaitForAttributeTimeout(t *testing.T) {
	workerTarget := startWorker(t, signal.NewHandler())
	runtime := startDexService(t, DexServiceTestConfig{BackendType: service.BackendTypeTemporal})
	flowClient := runtime.FlowClient

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	flowId := startParkedWaitForAttributeFlow(t, ctx, flowClient, workerTarget, nil)
	description, err := runtime.UnifiedClient.DescribeWorkflowExecution(ctx, flowId, "", nil)
	require.NoError(t, err)
	defaultUpdateID := "wait-for-attribute:" + waitForAttributeKey + "==\"never-set\""

	waitRequest := &dexpb.WaitForAttributeRequest{
		FlowId: flowId,
		Match: equalAttributeMatch(
			waitForAttributeKey,
			stringValue("never-set"),
		),
		RequestTimeoutSeconds: 1,
	}
	_, err = flowClient.WaitForAttribute(ctx, waitRequest)
	require.Error(t, err)
	require.Equal(t, codes.DeadlineExceeded, status.Code(err))
	errResp := grpcServiceErrorResponse(t, err)
	require.Equal(
		t,
		dexpb.ErrorSubStatus_ERROR_SUB_STATUS_REQUEST_TIMEOUT,
		errResp.GetSubStatus(),
	)
	require.Equal(t, "attribute request timed out", errResp.GetDetail())
	counts := inspectTemporalUpdateHistory(
		t,
		ctx,
		runtime,
		flowId,
		description.RunId,
		defaultUpdateID,
	)
	require.Equal(t, 1, counts.accepted)
	require.Zero(t, counts.completed)
	require.Zero(t, counts.oneSecondTimerStarted)
	require.Zero(t, counts.oneSecondTimerCanceled)
	require.Eventually(t, func() bool {
		return !time.Now().Before(counts.acceptedAt.Add(time.Second))
	}, 2*time.Second, 10*time.Millisecond)

	waitRequest.RequestTimeoutSeconds = 10
	responseChannel := make(chan *dexpb.WaitForAttributeResponse, 1)
	errorChannel := make(chan error, 1)
	go func() {
		response, waitErr := flowClient.WaitForAttribute(ctx, waitRequest)
		responseChannel <- response
		errorChannel <- waitErr
	}()
	_, err = flowClient.SetAttributes(ctx, &dexpb.SetAttributesRequest{
		RequestId: newRequestID(),
		FlowId:    flowId,
		Attributes: []*dexpb.AttributeWrite{
			{Key: waitForAttributeKey, Value: stringValue("never-set")},
		},
	})
	require.NoError(t, err)
	require.NoError(t, <-errorChannel)
	require.True(t, proto.Equal(stringValue("never-set"), (<-responseChannel).GetMatchedValue()))
	counts = inspectTemporalUpdateHistory(
		t,
		ctx,
		runtime,
		flowId,
		description.RunId,
		defaultUpdateID,
	)
	require.Equal(t, 1, counts.accepted)
	require.Equal(t, 1, counts.completed)
	require.Zero(t, counts.oneSecondTimerStarted)
	require.Zero(t, counts.oneSecondTimerCanceled)

	stopParkedWaitForAttributeFlow(t, ctx, flowClient, flowId)
}

func doTestWaitForAttributeTransportTimeoutAndReattach(t *testing.T) {
	workerTarget := startWorker(t, signal.NewHandler())
	runtime := startDexService(t, DexServiceTestConfig{
		BackendType:    service.BackendTypeTemporal,
		MaxWaitSeconds: 1,
	})
	flowClient := runtime.FlowClient
	// The service above caps each wait at one second. The reattach goes through a service with the 12-second test
	// default, so the cap cannot expire before the workflow task that applies the write has run.
	reattachClient := startDexService(t, DexServiceTestConfig{BackendType: service.BackendTypeTemporal}).FlowClient
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	flowId := startParkedWaitForAttributeFlow(t, ctx, flowClient, workerTarget, nil)
	description, err := runtime.UnifiedClient.DescribeWorkflowExecution(ctx, flowId, "", nil)
	require.NoError(t, err)
	requestID := uuid.NewString()
	expectedValue := stringValue("reattached")
	waitRequest := &dexpb.WaitForAttributeRequest{
		FlowId: flowId,
		Match: equalAttributeMatch(
			waitForAttributeKey,
			expectedValue,
		),
		RequestTimeoutSeconds: 5,
		RequestId:             requestID,
	}
	_, err = flowClient.WaitForAttribute(ctx, waitRequest)
	require.Equal(t, codes.DeadlineExceeded, status.Code(err))
	require.Equal(
		t,
		dexpb.ErrorSubStatus_ERROR_SUB_STATUS_LONG_POLL_TIME_OUT,
		grpcServiceErrorResponse(t, err).GetSubStatus(),
	)
	// The call returned at the transport cap, which can precede the workflow task that accepts the Update.
	require.Eventually(t, func() bool {
		accepted, _ := countTemporalUpdateEvents(t, ctx, runtime, flowId, description.RunId, requestID)
		return accepted == 1
	}, 20*time.Second, 50*time.Millisecond)
	accepted, completed := countTemporalUpdateEvents(
		t,
		ctx,
		runtime,
		flowId,
		description.RunId,
		requestID,
	)
	require.Equal(t, 1, accepted)
	require.Zero(t, completed)
	_, err = flowClient.SetAttributes(ctx, &dexpb.SetAttributesRequest{
		RequestId: newRequestID(),
		FlowId:    flowId,
		Attributes: []*dexpb.AttributeWrite{
			{Key: waitForAttributeKey, Value: expectedValue},
		},
	})
	require.NoError(t, err)
	response, err := reattachClient.WaitForAttribute(ctx, waitRequest)
	require.NoError(t, err)
	require.True(t, proto.Equal(expectedValue, response.GetMatchedValue()))
	accepted, completed = countTemporalUpdateEvents(
		t,
		ctx,
		runtime,
		flowId,
		description.RunId,
		requestID,
	)
	require.Equal(t, 1, accepted)
	require.Equal(t, 1, completed)
	stopParkedWaitForAttributeFlow(t, ctx, flowClient, flowId)
}

func doTestWaitForAttributeInternalHandlerTimeout(t *testing.T) {
	workerTarget := startWorker(t, signal.NewHandler())
	runtime := startDexService(t, DexServiceTestConfig{BackendType: service.BackendTypeTemporal})
	flowClient := runtime.FlowClient
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	flowID := startParkedWaitForAttributeFlow(t, ctx, flowClient, workerTarget, nil)
	description, err := runtime.UnifiedClient.DescribeWorkflowExecution(ctx, flowID, "", nil)
	require.NoError(t, err)
	requestID := uuid.NewString()
	expectedValue := stringValue("matched-after-handler-rollover")
	responseChannel := make(chan *dexpb.WaitForAttributeResponse, 1)
	errorChannel := make(chan error, 1)
	go func() {
		response, waitErr := flowClient.WaitForAttribute(ctx, &dexpb.WaitForAttributeRequest{
			FlowId: flowID,
			Match: equalAttributeMatch(
				waitForAttributeKey,
				expectedValue,
			),
			RequestTimeoutSeconds:         10,
			InternalHandlerTimeoutSeconds: 1,
			RequestId:                     requestID,
		})
		responseChannel <- response
		errorChannel <- waitErr
	}()
	require.Eventually(t, func() bool {
		counts := inspectTemporalUpdateHistory(
			t,
			ctx,
			runtime,
			flowID,
			description.RunId,
			requestID,
		)
		return counts.accepted == 1 && !time.Now().Before(counts.acceptedAt.Add(time.Second))
	}, 10*time.Second, 20*time.Millisecond)
	_, err = flowClient.SetAttributes(ctx, &dexpb.SetAttributesRequest{
		RequestId: newRequestID(),
		FlowId:    flowID,
		Attributes: []*dexpb.AttributeWrite{
			{Key: waitForAttributeKey, Value: stringValue("wake-handler-timeout")},
		},
	})
	require.NoError(t, err)
	// The final write must not reach the base handler. Once the base Update has completed with its handler
	// deadline, the API moves to generation 1, which sees the final write on acceptance or while parked.
	require.Eventually(t, func() bool {
		_, completed := countTemporalUpdateEvents(
			t,
			ctx,
			runtime,
			flowID,
			description.RunId,
			requestID,
		)
		return completed == 1
	}, 10*time.Second, 50*time.Millisecond)
	_, err = flowClient.SetAttributes(ctx, &dexpb.SetAttributesRequest{
		RequestId: newRequestID(),
		FlowId:    flowID,
		Attributes: []*dexpb.AttributeWrite{
			{Key: waitForAttributeKey, Value: expectedValue},
		},
	})
	require.NoError(t, err)
	require.NoError(t, <-errorChannel)
	require.True(t, proto.Equal(expectedValue, (<-responseChannel).GetMatchedValue()))
	baseAccepted, baseCompleted := countTemporalUpdateEvents(
		t,
		ctx,
		runtime,
		flowID,
		description.RunId,
		requestID,
	)
	nextAccepted, nextCompleted := countTemporalUpdateEvents(
		t,
		ctx,
		runtime,
		flowID,
		description.RunId,
		requestID+"-1",
	)
	require.Equal(t, 1, baseAccepted)
	require.Equal(t, 1, baseCompleted)
	require.Equal(t, 1, nextAccepted)
	require.Equal(t, 1, nextCompleted)
	stopParkedWaitForAttributeFlow(t, ctx, flowClient, flowID)
}

func doTestWaitForAttributeCancel(t *testing.T) {
	workerTarget := startWorker(t, signal.NewHandler())
	runtime := startDexService(t, DexServiceTestConfig{BackendType: service.BackendTypeTemporal})
	flowClient := runtime.FlowClient

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	flowId := startParkedWaitForAttributeFlow(t, ctx, flowClient, workerTarget, nil)
	description, err := runtime.UnifiedClient.DescribeWorkflowExecution(ctx, flowId, "", nil)
	require.NoError(t, err)
	requestID := uuid.NewString()

	done := make(chan error, 1)
	go func() {
		_, waitErr := flowClient.WaitForAttribute(ctx, &dexpb.WaitForAttributeRequest{
			FlowId: flowId,
			Match: equalAttributeMatch(
				waitForAttributeKey,
				stringValue("never-set"),
			),
			RequestTimeoutSeconds: 30,
			RequestId:             requestID,
		})
		done <- waitErr
	}()

	require.Eventually(t, func() bool {
		accepted, _ := countTemporalUpdateEvents(
			t,
			ctx,
			runtime,
			flowId,
			description.RunId,
			requestID,
		)
		return accepted == 1
	}, 5*time.Second, 50*time.Millisecond)
	cancel()

	select {
	case err := <-done:
		require.Equal(t, codes.Canceled, status.Code(err))
	case <-time.After(10 * time.Second):
		t.Fatal("WaitForAttribute did not return after cancel")
	}

	stopParkedWaitForAttributeFlow(t, context.Background(), flowClient, flowId)
}

func doTestWaitForAttributeNotFound(t *testing.T) {
	runtime := startDexService(t, DexServiceTestConfig{BackendType: service.BackendTypeTemporal})
	flowClient := runtime.FlowClient

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_, err := flowClient.WaitForAttribute(ctx, &dexpb.WaitForAttributeRequest{
		FlowId: "wait-for-attribute-missing-" + uuid.NewString(),
		Match: equalAttributeMatch(
			waitForAttributeKey,
			stringValue("anything"),
		),
		RequestTimeoutSeconds: 0,
		RequestId:             uuid.NewString(),
	})
	require.Equal(t, codes.NotFound, status.Code(err))
	require.Equal(
		t,
		dexpb.ErrorSubStatus_ERROR_SUB_STATUS_FLOW_NOT_EXISTS,
		grpcServiceErrorResponse(t, err).GetSubStatus(),
	)
}

func doTestWaitForAttributeClosed(t *testing.T) {
	workerTarget := startWorker(t, signal.NewHandler())
	runtime := startDexService(t, DexServiceTestConfig{BackendType: service.BackendTypeTemporal})
	flowClient := runtime.FlowClient

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	flowId := startParkedWaitForAttributeFlow(t, ctx, flowClient, workerTarget, nil)

	_, err := flowClient.StopFlow(ctx, &dexpb.StopFlowRequest{
		FlowId:   flowId,
		StopType: dexpb.StopType_STOP_TYPE_TERMINATE,
	})
	require.NoError(t, err)

	_, err = flowClient.WaitForAttribute(ctx, &dexpb.WaitForAttributeRequest{
		FlowId: flowId,
		Match: equalAttributeMatch(
			waitForAttributeKey,
			stringValue("anything"),
		),
		RequestTimeoutSeconds: 0,
		RequestId:             uuid.NewString(),
	})
	require.Error(t, err)
	require.Equal(t, codes.NotFound, status.Code(err))
	require.Equal(
		t,
		dexpb.ErrorSubStatus_ERROR_SUB_STATUS_FLOW_NOT_EXISTS,
		grpcServiceErrorResponse(t, err).GetSubStatus(),
	)
}

func doTestWaitForAttributeConcurrent(t *testing.T) {
	workerTarget := startWorker(t, signal.NewHandler())
	runtime := startDexService(t, DexServiceTestConfig{BackendType: service.BackendTypeTemporal})
	flowClient := runtime.FlowClient

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	flowId := startParkedWaitForAttributeFlow(t, ctx, flowClient, workerTarget, nil)
	expectedValue := stringValue("wait-for-attribute-concurrent")
	requestId := uuid.NewString()
	description, err := runtime.UnifiedClient.DescribeWorkflowExecution(ctx, flowId, "", nil)
	require.NoError(t, err)

	var waitGroup sync.WaitGroup
	errors := make([]error, 2)
	responses := make([]*dexpb.WaitForAttributeResponse, 2)
	for index := range errors {
		waitGroup.Add(1)
		go func(resultIndex int) {
			defer waitGroup.Done()
			response, waitErr := flowClient.WaitForAttribute(ctx, &dexpb.WaitForAttributeRequest{
				FlowId: flowId,
				Match: equalAttributeMatch(
					waitForAttributeKey,
					expectedValue,
				),
				RequestTimeoutSeconds: 30,
				RequestId:             requestId,
			})
			responses[resultIndex] = response
			errors[resultIndex] = waitErr
		}(index)
	}

	require.Eventually(t, func() bool {
		accepted, _ := countTemporalUpdateEvents(
			t,
			ctx,
			runtime,
			flowId,
			description.RunId,
			requestId,
		)
		return accepted == 1
	}, 5*time.Second, 50*time.Millisecond)

	_, err = flowClient.SetAttributes(ctx, &dexpb.SetAttributesRequest{
		RequestId: newRequestID(),
		FlowId:    flowId,
		Attributes: []*dexpb.AttributeWrite{
			{Key: waitForAttributeKey, Value: expectedValue},
		},
	})
	require.NoError(t, err)

	waitGroup.Wait()
	for _, waitErr := range errors {
		require.NoError(t, waitErr)
	}
	for _, response := range responses {
		require.True(t, proto.Equal(expectedValue, response.GetMatchedValue()))
	}
	accepted, completed := countTemporalUpdateEvents(
		t,
		ctx,
		runtime,
		flowId,
		description.RunId,
		requestId,
	)
	require.Equal(t, 1, accepted)
	require.Equal(t, 1, completed)

	stopParkedWaitForAttributeFlow(t, ctx, flowClient, flowId)
}

func doTestWaitForAttributeAcrossContinueAsNew(t *testing.T) {
	workerTarget := startWorker(t, signal.NewHandler())
	runtime := startDexService(t, DexServiceTestConfig{BackendType: service.BackendTypeTemporal})
	flowClient := runtime.FlowClient

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	flowId := startParkedWaitForAttributeFlow(
		t,
		ctx,
		flowClient,
		workerTarget,
		minimumContinueAsNewSyncDurabilityConfig(),
	)
	expectedValue := stringValue("wait-for-attribute-can")
	description, err := runtime.UnifiedClient.DescribeWorkflowExecution(ctx, flowId, "", nil)
	require.NoError(t, err)
	requestID := uuid.NewString()
	responseChannel := make(chan *dexpb.WaitForAttributeResponse, 1)
	errorChannel := make(chan error, 1)
	go func() {
		response, waitErr := flowClient.WaitForAttribute(ctx, &dexpb.WaitForAttributeRequest{
			FlowId: flowId,
			Match: equalAttributeMatch(
				waitForAttributeKey,
				expectedValue,
			),
			RequestTimeoutSeconds: 30,
			RequestId:             requestID,
		})
		responseChannel <- response
		errorChannel <- waitErr
	}()
	require.Eventually(t, func() bool {
		accepted, _ := countTemporalUpdateEvents(
			t,
			ctx,
			runtime,
			flowId,
			description.RunId,
			requestID,
		)
		return accepted == 1
	}, 5*time.Second, 50*time.Millisecond)
	_, err = flowClient.SetAttributes(ctx, &dexpb.SetAttributesRequest{
		RequestId: newRequestID(),
		FlowId:    flowId,
		Attributes: []*dexpb.AttributeWrite{
			{Key: waitForAttributeKey, Value: expectedValue},
		},
	})
	require.NoError(t, err)
	require.NoError(t, <-errorChannel)
	response := <-responseChannel
	require.True(t, proto.Equal(expectedValue, response.GetMatchedValue()))

	stopParkedWaitForAttributeFlow(t, ctx, flowClient, flowId)
}

func doTestWaitForAttributeCounterSuccess(t *testing.T) {
	workerTarget := startWorker(t, signal.NewHandler())
	runtime := startDexService(t, DexServiceTestConfig{BackendType: service.BackendTypeTemporal})
	flowClient := runtime.FlowClient
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	expectedValue := stringValue("counter-match")
	flowId := "wait-for-attribute-counter-success-" + uuid.NewString()
	startResponse, err := flowClient.StartFlow(ctx, &dexpb.StartFlowRequest{
		RequestId:          newRequestID(),
		FlowId:             flowId,
		FlowType:           signal.WorkflowType,
		FlowTimeoutSeconds: 30,
		StartStepType:      signal.State1,
		FlowStartOptions: withWorkerTarget(&dexpb.FlowStartOptions{
			Attributes: []*dexpb.AttributeWrite{
				{Key: waitForAttributeKey, Value: expectedValue},
			},
			FlowConfigOverride: &dexpb.FlowConfig{
				ContinueAsNewThreshold: ptr.Any(int32(2)),
			},
		}, workerTarget),
	})
	require.NoError(t, err)

	requestID := uuid.NewString()
	waitRequest := &dexpb.WaitForAttributeRequest{
		FlowId: flowId,
		Match: equalAttributeMatch(
			waitForAttributeKey,
			expectedValue,
		),
		RequestId: requestID,
	}
	for range 2 {
		response, waitErr := flowClient.WaitForAttribute(ctx, waitRequest)
		require.NoError(t, waitErr)
		require.True(t, proto.Equal(expectedValue, response.GetMatchedValue()))
	}
	accepted, completed := countTemporalUpdateEvents(
		t,
		ctx,
		runtime,
		flowId,
		startResponse.GetRunId(),
		requestID,
	)
	require.Equal(t, 1, accepted)
	require.Equal(t, 1, completed)

	validationRequestID := uuid.NewString()
	var rejectedResponse dexpb.WaitForAttributeResponse
	err = runtime.UnifiedClient.SynchronousUpdateWorkflow(
		ctx,
		&rejectedResponse,
		flowId,
		"",
		validationRequestID,
		service.WaitForAttributeUpdateType,
		&dexpb.WaitForAttributeRequest{FlowId: flowId},
	)
	require.Error(t, err)
	accepted, completed = countTemporalUpdateEvents(
		t,
		ctx,
		runtime,
		flowId,
		startResponse.GetRunId(),
		validationRequestID,
	)
	require.Zero(t, accepted)
	require.Zero(t, completed)
	require.Never(t, func() bool {
		description, describeErr := runtime.UnifiedClient.DescribeWorkflowExecution(ctx, flowId, "", nil)
		return describeErr == nil && description.RunId != startResponse.GetRunId()
	}, time.Second, 50*time.Millisecond)

	waitRequest.RequestId = uuid.NewString()
	_, err = flowClient.WaitForAttribute(ctx, waitRequest)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		description, describeErr := runtime.UnifiedClient.DescribeWorkflowExecution(ctx, flowId, "", nil)
		return describeErr == nil && description.RunId != startResponse.GetRunId()
	}, 5*time.Second, 50*time.Millisecond)
	stopParkedWaitForAttributeFlow(t, ctx, flowClient, flowId)
}

func doTestWaitForAttributeCounterFailure(t *testing.T) {
	workerTarget := startWorker(t, signal.NewHandler())
	runtime := startDexService(t, DexServiceTestConfig{
		BackendType:     service.BackendTypeTemporal,
		S3TestThreshold: 50,
		LazyLoading:     ptr.Any(true),
	})
	flowClient := runtime.FlowClient
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	flowId := "wait-for-attribute-counter-failure-" + uuid.NewString()
	startResponse, err := flowClient.StartFlow(ctx, &dexpb.StartFlowRequest{
		RequestId:          newRequestID(),
		FlowId:             flowId,
		FlowType:           signal.WorkflowType,
		FlowTimeoutSeconds: 30,
		StartStepType:      signal.State1,
		FlowStartOptions: withWorkerTarget(&dexpb.FlowStartOptions{
			Attributes: []*dexpb.AttributeWrite{
				{Key: waitForAttributeBlobKey, Value: stringValue(strings.Repeat("x", 120))},
			},
			FlowConfigOverride: minimumContinueAsNewSyncDurabilityConfig(),
		}, workerTarget),
	})
	require.NoError(t, err)

	_, err = flowClient.WaitForAttribute(ctx, &dexpb.WaitForAttributeRequest{
		FlowId: flowId,
		Match: equalAttributeMatch(
			waitForAttributeBlobKey,
			stringValue("anything"),
		),
		RequestId: uuid.NewString(),
	})
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Eventually(t, func() bool {
		description, describeErr := runtime.UnifiedClient.DescribeWorkflowExecution(ctx, flowId, "", nil)
		return describeErr == nil && description.RunId != startResponse.GetRunId()
	}, 5*time.Second, 50*time.Millisecond)
	stopParkedWaitForAttributeFlow(t, ctx, flowClient, flowId)
}

func doTestWaitForAttributeCadenceUnimplemented(t *testing.T) {
	workerTarget := startWorker(t, signal.NewHandler())
	runtime := startDexService(t, DexServiceTestConfig{BackendType: service.BackendTypeCadence})
	flowClient := runtime.FlowClient

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	flowId := "wait-for-attribute-cadence-" + uuid.NewString()
	_, err := flowClient.StartFlow(ctx, &dexpb.StartFlowRequest{
		RequestId:          newRequestID(),
		FlowId:             flowId,
		FlowType:           signal.WorkflowType,
		FlowTimeoutSeconds: 20,

		StartStepType:    signal.State1,
		FlowStartOptions: withWorkerTarget(nil, workerTarget),
	})
	require.NoError(t, err)

	_, err = flowClient.WaitForAttribute(ctx, &dexpb.WaitForAttributeRequest{
		FlowId: flowId,
		Match: equalAttributeMatch(
			waitForAttributeBlobKey,
			stringValue("anything"),
		),
		RequestTimeoutSeconds: 1,
	})
	require.Equal(t, codes.Unimplemented, status.Code(err))

	_, err = flowClient.StopFlow(ctx, &dexpb.StopFlowRequest{
		FlowId:   flowId,
		StopType: dexpb.StopType_STOP_TYPE_TERMINATE,
	})
	require.NoError(t, err)
}

func equalAttributeMatch(
	key string,
	value *dexpb.Value,
) *dexpb.AttributeMatch {
	return waitForAttributeMatch(
		key,
		dexpb.AttributeMatchOperator_ATTRIBUTE_MATCH_OPERATOR_EQUAL,
		value,
	)
}

func waitForAttributeMatch(
	key string,
	operator dexpb.AttributeMatchOperator,
	value *dexpb.Value,
) *dexpb.AttributeMatch {
	return &dexpb.AttributeMatch{
		Key:      key,
		Operator: operator,
		Operand:  value,
	}
}

func startParkedWaitForAttributeFlow(
	t *testing.T,
	ctx context.Context,
	flowClient dexpb.FlowServiceClient,
	workerTarget *dexpb.WorkerTarget,
	flowConfig *dexpb.FlowConfig,
) string {
	t.Helper()

	flowId := "wait-for-attribute-" + uuid.NewString()
	startRequest := &dexpb.StartFlowRequest{
		RequestId:          newRequestID(),
		FlowId:             flowId,
		FlowType:           signal.WorkflowType,
		FlowTimeoutSeconds: 30,

		StartStepType:    signal.State1,
		FlowStartOptions: withWorkerTarget(nil, workerTarget),
	}
	if flowConfig != nil {
		startRequest.FlowStartOptions = withWorkerTarget(&dexpb.FlowStartOptions{
			FlowConfigOverride: flowConfig,
		}, workerTarget)
	}
	_, err := flowClient.StartFlow(ctx, startRequest)
	require.NoError(t, err)
	return flowId
}

func stopParkedWaitForAttributeFlow(
	t *testing.T,
	ctx context.Context,
	flowClient dexpb.FlowServiceClient,
	flowId string,
) {
	t.Helper()

	_, err := flowClient.StopFlow(ctx, &dexpb.StopFlowRequest{
		FlowId:   flowId,
		StopType: dexpb.StopType_STOP_TYPE_TERMINATE,
	})
	require.NoError(t, err)
}
