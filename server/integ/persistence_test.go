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
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/superdurable/dex/gen/dexpb"
	"github.com/superdurable/dex/integ/workflow/persistence"
	"github.com/superdurable/dex/service"
	"github.com/superdurable/dex/service/common/timeparser"
	"google.golang.org/protobuf/proto"
)

var persistenceSearchTimeSequence atomic.Int64

func TestPersistenceWorkflowTemporal(t *testing.T) {
	if !*temporalIntegTest {
		t.Skip()
	}
	for i := 0; i < *repeatIntegTest; i++ {
		doTestPersistenceWorkflow(t, service.BackendTypeTemporal, false, nil)
		smallWaitForFastTest()
	}
}

func TestPersistenceWorkflowTemporalWithEncryption(t *testing.T) {
	if !*temporalIntegTest {
		t.Skip()
	}
	for i := 0; i < *repeatIntegTest; i++ {
		doTestPersistenceWorkflow(t, service.BackendTypeTemporal, true, nil)
		smallWaitForFastTest()
	}
}

func TestPersistenceWorkflowTemporalContinueAsNew(t *testing.T) {
	if !*temporalIntegTest {
		t.Skip()
	}
	for i := 0; i < *repeatIntegTest; i++ {
		doTestPersistenceWorkflow(
			t,
			service.BackendTypeTemporal,
			false,
			minimumContinueAsNewAsyncDurabilityConfig(),
		)
		smallWaitForFastTest()
	}
}

func TestPersistenceWorkflowTemporalContinueAsNewWithEncryption(t *testing.T) {
	if !*temporalIntegTest {
		t.Skip()
	}
	for i := 0; i < *repeatIntegTest; i++ {
		doTestPersistenceWorkflow(
			t,
			service.BackendTypeTemporal,
			true,
			minimumContinueAsNewAsyncDurabilityConfig(),
		)
		smallWaitForFastTest()
	}
}

func TestPersistenceWorkflowCadence(t *testing.T) {
	if !*cadenceIntegTest {
		t.Skip()
	}
	for i := 0; i < *repeatIntegTest; i++ {
		doTestPersistenceWorkflow(t, service.BackendTypeCadence, false, nil)
		smallWaitForFastTest()
	}
}

func TestPersistenceWorkflowCadenceContinueAsNew(t *testing.T) {
	if !*cadenceIntegTest {
		t.Skip()
	}
	for i := 0; i < *repeatIntegTest; i++ {
		doTestPersistenceWorkflow(
			t,
			service.BackendTypeCadence,
			false,
			minimumContinueAsNewSyncDurabilityConfig(),
		)
		smallWaitForFastTest()
	}
}

func doTestPersistenceWorkflow(
	t *testing.T,
	backendType service.BackendType,
	memoEncryption bool,
	flowConfig *dexpb.FlowConfig,
) {
	workerHandler := persistence.NewHandler()
	workerTarget := startWorker(t, workerHandler)
	runtime := startDexService(t, DexServiceTestConfig{
		BackendType:    backendType,
		MemoEncryption: memoEncryption,
	})
	flowClient := runtime.FlowClient

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	flowId := persistence.WorkflowType + uuid.NewString()
	nowTime := time.Now().Add(
		time.Duration(persistenceSearchTimeSequence.Add(1)) * time.Hour,
	)
	nowTimeStr := nowTime.Format(timeparser.DateTimeFormat)

	expectedDataAttribute := dataObjectAttribute("TestKey", `"TestValue"`)
	expectedDatetimeSearchAttribute := indexedDatetimeAttribute("CustomDatetimeField", nowTimeStr)

	startRequest := &dexpb.StartFlowRequest{
		RequestId:          newRequestID(),
		FlowId:             flowId,
		FlowType:           persistence.WorkflowType,
		FlowTimeoutSeconds: 20,

		StartStepType: persistence.State1,
		FlowStartOptions: withWorkerTarget(&dexpb.FlowStartOptions{
			Attributes: []*dexpb.AttributeWrite{
				expectedDatetimeSearchAttribute,
				expectedDataAttribute,
			},
			FlowConfigOverride: flowConfig,
		}, workerTarget),
	}
	startResp, err := flowClient.StartFlow(ctx, startRequest)
	require.NoError(t, err)

	queryResult, err := getFlowAttributes(ctx, flowClient, flowId, []string{
		persistence.TestDataAttributeKey,
		expectedDataAttribute.GetKey(),
	})

	retryCount := 0
	if flowConfig != nil {
		for err != nil && retryCount < 5 {
			time.Sleep(time.Second)
			retryCount++
			queryResult, err = getFlowAttributes(ctx, flowClient, flowId, []string{
				persistence.TestDataAttributeKey,
				expectedDataAttribute.GetKey(),
			})
		}
	}
	require.NoError(t, err)
	requireAttributePresent(t, queryResult.GetAttributes(), expectedDataAttribute)

	_, err = flowClient.WaitForFlow(ctx, &dexpb.WaitForFlowRequest{
		FlowId: flowId,
	})
	require.NoError(t, err)
	runId := startResp.GetRunId()

	queryResult1, err := getFlowAttributes(ctx, flowClient, flowId, []string{
		persistence.TestDataAttributeKey,
		expectedDataAttribute.GetKey(),
	})
	require.NoError(t, err)

	queryResult2, err := flowClient.GetAttributes(ctx, &dexpb.GetAttributesRequest{
		FlowId: flowId,
		Keys: []string{
			persistence.TestDataAttributeKey,
			persistence.TestDataAttributeKey2,
			expectedDataAttribute.GetKey(),
		},
	})
	require.NoError(t, err)

	searchResult1, err := getFlowAttributes(ctx, flowClient, flowId, []string{
		persistence.TestSearchAttributeKeywordKey,
	})
	require.NoError(t, err)

	searchResult2, err := getFlowAttributes(ctx, flowClient, flowId, []string{
		persistence.TestSearchAttributeIntKey,
	})
	require.NoError(t, err)

	result := workerHandler.GetTestResult()
	history := result.InvokeHistory
	data := result.InvokeData
	require.Equal(t, map[string]int64{
		"S1_waitFor": 1,
		"S1_execute": 1,
		"S2_waitFor": 1,
		"S2_execute": 1,
		"S3_waitFor": 1,
		"S3_execute": 1,
	}, history)

	require.Equal(t, map[string]interface{}{
		"S1_decide_intSaFounds": 1,
		"S1_decide_kwSaFounds":  1,
		"S2_decide_intSaFounds": 1,
		"S2_decide_kwSaFounds":  1,
		"S2_start_intSaFounds":  1,
		"S2_start_kwSaFounds":   1,

		"S1_decide_localAttFound": true,
		"S1_decide_queryAttFound": 2,
		"S2_decide_queryAttFound": true,
		"S2_start_queryAttFound":  true,
	}, data)

	expectedVal1 := dataObjectAttribute(persistence.TestDataAttributeKey, "test-data-attribute-value2")

	requireAttributesMatch(t, []*dexpb.AttributeWrite{
		expectedVal1,
		expectedDataAttribute,
	}, queryResult1.GetAttributes())
	requireAttributesMatch(t, []*dexpb.AttributeWrite{
		expectedVal1,
		expectedDataAttribute,
	}, queryResult2.GetAttributes())
	requireAttributeAbsent(t, queryResult2.GetAttributes(), persistence.TestDataAttributeKey2)

	expectedSearchKeyword := indexedKeywordAttribute(
		persistence.TestSearchAttributeKeywordKey,
		persistence.TestSearchAttributeKeywordValue2,
	)
	expectedSearchInt := indexedIntAttribute(
		persistence.TestSearchAttributeIntKey,
		persistence.TestSearchAttributeIntValue2,
	)
	expectedSearchBool := indexedBoolAttribute(persistence.TestSearchAttributeBoolKey, false)

	requireAttributesMatch(t, []*dexpb.AttributeWrite{expectedSearchKeyword}, searchResult1.GetAttributes())
	requireAttributesMatch(t, []*dexpb.AttributeWrite{expectedSearchInt}, searchResult2.GetAttributes())

	allIndexed, err := flowClient.GetAttributes(ctx, &dexpb.GetAttributesRequest{
		FlowId: flowId,
		RunId:  runId,
		Keys: []string{
			"CustomDatetimeField",
			persistence.TestSearchAttributeKeywordKey,
			persistence.TestSearchAttributeIntKey,
			persistence.TestSearchAttributeBoolKey,
		},
	})
	require.NoError(t, err)
	expectedIndexedAttributes := []*dexpb.AttributeWrite{
		expectedDatetimeSearchAttribute,
		expectedSearchKeyword,
		expectedSearchInt,
		expectedSearchBool,
	}
	requireAttributesMatch(t, expectedIndexedAttributes, allIndexed.GetAttributes())
	expectedSearchIndexedAttributes := expectedIndexedAttributes
	if isServerZDBinaryMode() {
		// server-zd returns indexed datetimes in UTC; the Go Server uses its local time zone.
		expectedSearchIndexedAttributes = []*dexpb.AttributeWrite{
			indexedDatetimeAttribute("CustomDatetimeField", nowTime.UTC().Format(timeparser.DateTimeFormat)),
			expectedSearchKeyword,
			expectedSearchInt,
			expectedSearchBool,
		}
	}

	if *testSearchIntegTest {
		var searchFlow *dexpb.SearchFlowsResponseEntry
		expectedFlowType := &dexpb.KV{
			Key: service.SearchAttributeDexWorkflowType,
			Value: &dexpb.Value{
				Kind: &dexpb.Value_StringValue{StringValue: persistence.WorkflowType},
			},
		}
		require.Eventually(t, func() bool {
			searchResponse, searchErr := flowClient.SearchFlows(ctx, &dexpb.SearchFlowsRequest{
				IncludeContinuedAsNew: flowConfig != nil,
				Query:                 fmt.Sprintf("CustomDatetimeField='%v'", nowTimeStr),
				PageSize:              100,
			})
			if searchErr != nil {
				return false
			}
			for _, flowRun := range searchResponse.GetFlowRuns() {
				if flowRun.GetRunId() == runId &&
					searchAttributeWritesPresent(expectedSearchIndexedAttributes, flowRun.GetIndexedAttributes()) &&
					searchAttributePresent(flowRun.GetIndexedAttributes(), expectedFlowType) {
					searchFlow = flowRun
					return true
				}
			}
			return false
		}, 30*time.Second, 100*time.Millisecond)
		require.Equal(t, flowId, searchFlow.GetFlowId())
		require.Equal(t, persistence.WorkflowType, searchFlow.GetFlowType())
		require.NotEqual(t, dexpb.FlowStatus_FLOW_STATUS_UNSPECIFIED, searchFlow.GetFlowStatus())
		require.NotNil(t, searchFlow.GetStartTime())
		requireSearchAttributesMatch(t, expectedSearchIndexedAttributes, searchFlow.GetIndexedAttributes())
		requireSearchAttributePresent(t, searchFlow.GetIndexedAttributes(), expectedFlowType)

		firstFlowId := flowId
		startedRunIds := make(map[string]string)
		startMore := func(id string, attrs []*dexpb.AttributeWrite) {
			startResponse, startErr := flowClient.StartFlow(ctx, &dexpb.StartFlowRequest{
				RequestId:          newRequestID(),
				FlowId:             id,
				FlowType:           persistence.WorkflowType,
				FlowTimeoutSeconds: 20,

				StartStepType: persistence.State1,
				FlowStartOptions: withWorkerTarget(&dexpb.FlowStartOptions{
					Attributes:         attrs,
					FlowConfigOverride: flowConfig,
				}, workerTarget),
			})
			require.NoError(t, startErr)
			startedRunIds[id] = startResponse.GetRunId()
		}

		startMore(firstFlowId+"-1", []*dexpb.AttributeWrite{
			indexedBoolAttribute("CustomBoolField", true),
			indexedDatetimeAttribute("CustomDatetimeField", nowTimeStr),
		})
		startMore(firstFlowId+"-2", []*dexpb.AttributeWrite{
			indexedBoolAttribute("CustomBoolField", true),
			indexedDatetimeAttribute("CustomDatetimeField", nowTimeStr),
			indexedDoubleAttribute("CustomDoubleField", 0.01),
		})
		attrs3 := []*dexpb.AttributeWrite{
			indexedBoolAttribute("CustomBoolField", true),
			indexedDatetimeAttribute("CustomDatetimeField", nowTimeStr),
			indexedDoubleAttribute("CustomDoubleField", 0.01),
		}
		attrs4 := []*dexpb.AttributeWrite{
			indexedBoolAttribute("CustomBoolField", true),
			indexedDatetimeAttribute("CustomDatetimeField", nowTimeStr),
			indexedDoubleAttribute("CustomDoubleField", 0.01),
		}
		expectedExtraSearchAttributes := []*dexpb.AttributeWrite{
			indexedDoubleAttribute("CustomDoubleField", 0.01),
			indexedTextAttribute("CustomTextField", "My name is Quanzheng Long"),
		}
		// Cadence has no KeywordList search attribute registration.
		if backendType == service.BackendTypeTemporal {
			keywordArray := indexedKeywordArrayAttribute(
				persistence.TestSearchAttributeKeywordArrayKey,
				"keyword1",
				"keyword2",
			)
			attrs3 = append(attrs3, keywordArray)
			attrs4 = append(attrs4, keywordArray)
			expectedExtraSearchAttributes = append(expectedExtraSearchAttributes, keywordArray)
		}
		attrs4 = append(attrs4, indexedTextAttribute("CustomTextField", "My name is Quanzheng Long"))
		startMore(firstFlowId+"-3", attrs3)
		startMore(firstFlowId+"-4", attrs4)

		for _, suffix := range []string{"-1", "-2", "-3", "-4"} {
			resp, waitErr := flowClient.WaitForFlow(ctx, &dexpb.WaitForFlowRequest{
				FlowId: firstFlowId + suffix,
			})
			require.NoError(t, waitErr)
			require.Equal(t, dexpb.FlowStatus_FLOW_STATUS_COMPLETED, resp.GetFlowStatus())
		}

		extraFlowId := firstFlowId + "-4"
		var extraSearchFlow *dexpb.SearchFlowsResponseEntry
		require.Eventually(t, func() bool {
			searchResponse, searchErr := flowClient.SearchFlows(ctx, &dexpb.SearchFlowsRequest{
				IncludeContinuedAsNew: flowConfig != nil,
				Query:                 fmt.Sprintf("CustomDatetimeField='%v'", nowTimeStr),
				PageSize:              100,
			})
			if searchErr != nil {
				return false
			}
			for _, flowRun := range searchResponse.GetFlowRuns() {
				if flowRun.GetRunId() == startedRunIds[extraFlowId] &&
					searchAttributeWritesPresent(expectedExtraSearchAttributes, flowRun.GetIndexedAttributes()) &&
					searchAttributePresent(flowRun.GetIndexedAttributes(), expectedFlowType) {
					extraSearchFlow = flowRun
					return true
				}
			}
			return false
		}, 30*time.Second, 100*time.Millisecond)
		requireSearchAttributesMatch(t, expectedExtraSearchAttributes, extraSearchFlow.GetIndexedAttributes())
		requireSearchAttributePresent(t, extraSearchFlow.GetIndexedAttributes(), expectedFlowType)

		time.Sleep(time.Duration(*searchWaitTimeIntegTest) * time.Millisecond)

		boolQuery := fmt.Sprintf("CustomDatetimeField='%v' AND CustomBoolField=%v", nowTimeStr, true)
		if backendType == service.BackendTypeCadence {
			// Cadence ES visibility expects a quoted bool literal.
			boolQuery = fmt.Sprintf("CustomDatetimeField='%v' AND CustomBoolField='%v'", nowTimeStr, "true")
		}

		// C3: server-zd never continues a run as new, so it has no earlier runs to count.
		if flowConfig != nil && !isServerZDBinaryMode() {
			assertSearchFlows(t, flowClient, fmt.Sprintf("CustomDatetimeField='%v'", nowTimeStr), 15, flowConfig != nil)
			assertSearchFlows(t, flowClient, fmt.Sprintf("CustomDatetimeField='%v' AND CustomTextField='%v'", nowTimeStr, "Quanzheng"), 3, flowConfig != nil)
			assertSearchFlows(t, flowClient, fmt.Sprintf("CustomDatetimeField='%v' AND CustomDoubleField='%v'", nowTimeStr, "0.01"), 9, flowConfig != nil)
			assertSearchFlows(t, flowClient, boolQuery, 0, flowConfig != nil)
		} else {
			assertSearchFlows(t, flowClient, fmt.Sprintf("CustomDatetimeField='%v'", nowTimeStr), 5, flowConfig != nil)
			assertSearchFlows(t, flowClient, fmt.Sprintf("CustomDatetimeField='%v' AND CustomTextField='%v'", nowTimeStr, "Quanzheng"), 1, flowConfig != nil)
			assertSearchFlows(t, flowClient, fmt.Sprintf("CustomDatetimeField='%v' AND CustomDoubleField='%v'", nowTimeStr, "0.01"), 3, flowConfig != nil)
			assertSearchFlows(t, flowClient, boolQuery, 0, flowConfig != nil)
		}
	}
}

func getFlowAttributes(
	ctx context.Context,
	flowClient dexpb.FlowServiceClient,
	flowId string,
	keys []string,
) (*dexpb.GetAttributesResponse, error) {
	return flowClient.GetAttributes(ctx, &dexpb.GetAttributesRequest{
		FlowId: flowId,
		Keys:   keys,
	})
}

func requireAttributePresent(t *testing.T, attributes []*dexpb.KV, expected *dexpb.AttributeWrite) {
	t.Helper()
	for _, attribute := range attributes {
		if attribute.GetKey() == expected.GetKey() &&
			proto.Equal(attribute.GetValue(), expected.GetValue()) {
			return
		}
	}
	require.Fail(t, "expected attribute not found", expected.GetKey())
}

func requireAttributeAbsent(t *testing.T, attributes []*dexpb.KV, key string) {
	t.Helper()
	for _, attribute := range attributes {
		require.NotEqual(t, key, attribute.GetKey(), "unexpected attribute found")
	}
}

func requireSearchAttributesMatch(
	t *testing.T,
	expected []*dexpb.AttributeWrite,
	actual []*dexpb.KV,
) {
	t.Helper()
	keys := make([]string, 0, len(actual))
	for _, attribute := range actual {
		keys = append(keys, attribute.GetKey())
	}
	sortedKeys := append([]string(nil), keys...)
	sort.Strings(sortedKeys)
	require.Equal(t, sortedKeys, keys)

	for _, attribute := range expected {
		requireSearchAttributePresent(t, actual, &dexpb.KV{
			Key:   attribute.GetKey(),
			Value: attribute.GetValue(),
		})
	}
}

func searchAttributeWritesPresent(expected []*dexpb.AttributeWrite, actual []*dexpb.KV) bool {
	for _, attribute := range expected {
		if !searchAttributePresent(actual, &dexpb.KV{
			Key:   attribute.GetKey(),
			Value: attribute.GetValue(),
		}) {
			return false
		}
	}
	return true
}

func requireSearchAttributePresent(t *testing.T, attributes []*dexpb.KV, expected *dexpb.KV) {
	t.Helper()
	if searchAttributePresent(attributes, expected) {
		return
	}
	require.Fail(t, "expected search attribute not found", expected.GetKey())
}

func searchAttributePresent(attributes []*dexpb.KV, expected *dexpb.KV) bool {
	for _, attribute := range attributes {
		if proto.Equal(attribute, expected) {
			return true
		}
	}
	return false
}

func requireAttributesMatch(t *testing.T, expected []*dexpb.AttributeWrite, actual []*dexpb.KV) {
	t.Helper()
	require.Len(t, actual, len(expected))
	for _, want := range expected {
		found := false
		for _, got := range actual {
			if got.GetKey() == want.GetKey() && proto.Equal(got.GetValue(), want.GetValue()) {
				found = true
				break
			}
		}
		require.True(t, found, "missing attribute %s", want.GetKey())
	}
}
