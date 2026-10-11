// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package web_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/superdurable/dex/gen/dexpb"
	dexweb "github.com/superdurable/dex/web"
	"github.com/superdurable/dex/web/assets"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type flowService struct {
	dexpb.UnimplementedFlowServiceServer
	waitStarted        chan struct{}
	waitCanceled       chan struct{}
	waitDeadline       chan time.Time
	searchRequests     chan *dexpb.SearchFlowsRequest
	loadBlobsRequests  chan *dexpb.LoadBlobsRequest
	loadBlobsError     error
	streamRequests     chan *dexpb.ReadStreamRequest
	stopRequests       chan *dexpb.StopFlowRequest
	timeTravelRequests chan *dexpb.ResetFlowRequest
	publishRequests    chan *dexpb.PublishToChannelRequest
	setRequests        chan *dexpb.SetAttributesRequest
}

func TestWebServerWritesLiveFlowState(t *testing.T) {
	service := &flowService{
		publishRequests: make(chan *dexpb.PublishToChannelRequest, 1),
		setRequests:     make(chan *dexpb.SetAttributesRequest, 1),
	}
	harness := newHarness(t, service)

	publishResponse := postJSON(
		t,
		harness.http.URL+"/api/flows/channels/messages",
		`{"flowId":"checkout-1","runId":"run-1","channelName":"approvals","valueType":"json","value":{"approved":true,"count":3}}`,
	)
	publishBody := readBody(t, publishResponse)
	publishResponse.Body.Close()
	if publishResponse.StatusCode != http.StatusOK {
		t.Fatalf("publish status = %d body=%q", publishResponse.StatusCode, publishBody)
	}
	published := <-service.publishRequests
	if published.GetFlowId() != "checkout-1" || published.GetRunId() != "run-1" ||
		len(published.GetMessages()) != 1 || published.GetMessages()[0].GetChannelName() != "approvals" {
		t.Fatalf("unexpected publish request: %+v", published)
	}
	publishedObject := published.GetMessages()[0].GetValue().GetObjValue()
	if publishedObject.GetEncoding() != "json" ||
		string(publishedObject.GetPayload()) != `{"approved":true,"count":3}` {
		t.Fatalf("unexpected published value: %+v", published.GetMessages()[0].GetValue())
	}

	setResponse := sendJSON(
		t,
		http.MethodPut,
		harness.http.URL+"/api/flows/attributes",
		`{"flowId":"checkout-1","runId":"run-1","key":"retries","valueType":"int64","value":"9007199254740993"}`,
	)
	setBody := readBody(t, setResponse)
	setResponse.Body.Close()
	if setResponse.StatusCode != http.StatusOK {
		t.Fatalf("set status = %d body=%q", setResponse.StatusCode, setBody)
	}
	set := <-service.setRequests
	if set.GetFlowId() != "checkout-1" || set.GetRunId() != "run-1" || set.GetRequestId() == "" ||
		len(set.GetAttributes()) != 1 || set.GetAttributes()[0].GetKey() != "retries" ||
		set.GetAttributes()[0].GetValue().GetIntValue() != 9007199254740993 {
		t.Fatalf("unexpected set request: %+v", set)
	}

	for _, invalid := range []struct {
		method string
		path   string
		body   string
	}{
		{http.MethodPost, "/api/flows/channels/messages", `{"flowId":"checkout-1","channelName":"approvals","valueType":"raw","value":"x"}`},
		{http.MethodPost, "/api/flows/channels/messages", `{"flowId":"checkout-1","valueType":"string","value":"x"}`},
		{http.MethodPut, "/api/flows/attributes", `{"flowId":"checkout-1","key":"retries","valueType":"bool","value":"yes"}`},
	} {
		response := sendJSON(t, invalid.method, harness.http.URL+invalid.path, invalid.body)
		body := readBody(t, response)
		response.Body.Close()
		if response.StatusCode != http.StatusBadRequest {
			t.Fatalf("%s %s %s: status=%d body=%q", invalid.method, invalid.path, invalid.body, response.StatusCode, body)
		}
	}
}

func TestWebServerTimeTravelsToStepMethod(t *testing.T) {
	service := &flowService{timeTravelRequests: make(chan *dexpb.ResetFlowRequest, 1)}
	harness := newHarness(t, service)
	response := postJSON(
		t,
		harness.http.URL+"/api/flows/time-travel",
		`{"flowId":"checkout-1","runId":"run-1","timeTravelType":4,"stepExecutionId":"ChargeOrder-2","stepMethod":2,"reason":"retry fixed code"}`,
	)
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("time travel status = %d body=%q", response.StatusCode, readBody(t, response))
	}
	request := <-service.timeTravelRequests
	if request.GetFlowId() != "checkout-1" || request.GetRunId() != "run-1" {
		t.Fatalf("unexpected execution: %+v", request)
	}
	if request.GetResetType() != dexpb.FlowResetType_FLOW_RESET_TYPE_STEP_EXECUTION_ID ||
		request.GetStepExecutionId() != "ChargeOrder-2" ||
		request.GetStepMethod() != dexpb.FlowResetStepMethod_FLOW_RESET_STEP_METHOD_EXECUTE {
		t.Fatalf("unexpected time travel point: %+v", request)
	}
}

func TestWebServerBridgesDexAndServesSPA(t *testing.T) {
	harness := newHarness(t, &flowService{})

	searchResponse := postJSON(t, harness.http.URL+"/api/flows/search", `{"pageSize":10}`)
	defer searchResponse.Body.Close()
	if searchResponse.StatusCode != http.StatusOK {
		t.Fatalf("search status = %d", searchResponse.StatusCode)
	}
	var searchResult struct {
		Flows []struct {
			FlowID   string `json:"flowId"`
			FlowType string `json:"flowType"`
		} `json:"flows"`
	}
	decodeResponse(t, searchResponse, &searchResult)
	if len(searchResult.Flows) != 1 || searchResult.Flows[0].FlowID != "checkout-1" {
		t.Fatalf("unexpected search result: %+v", searchResult)
	}

	definitionResponse := get(t, harness.http.URL+"/api/flow-definitions")
	defer definitionResponse.Body.Close()
	var definitionCatalog struct {
		Configured  bool              `json:"configured"`
		Definitions []json.RawMessage `json:"definitions"`
	}
	decodeResponse(t, definitionResponse, &definitionCatalog)
	if definitionCatalog.Configured || len(definitionCatalog.Definitions) != 0 {
		t.Fatalf("unexpected unconfigured Flow definitions: %+v", definitionCatalog)
	}

	for _, path := range []string{"/", "/v1/rendering", "/v1/flows/checkout-1/run-1", "/v2"} {
		response := get(t, harness.http.URL+path)
		body := readBody(t, response)
		response.Body.Close()
		if response.StatusCode != http.StatusOK || !strings.Contains(body, `<div id="root"></div>`) {
			t.Fatalf("SPA %s: status=%d body=%q", path, response.StatusCode, body)
		}
	}

	missingAPI := get(t, harness.http.URL+"/api/unknown")
	missingBody := readBody(t, missingAPI)
	missingAPI.Body.Close()
	if missingAPI.StatusCode != http.StatusNotFound || !strings.Contains(missingBody, "API route not found") {
		t.Fatalf("unknown API: status=%d body=%q", missingAPI.StatusCode, missingBody)
	}
}

func TestWebServerLoadsFlowDefinitionsAtStartup(t *testing.T) {
	directory := t.TempDir()
	nestedDirectory := filepath.Join(directory, "nested")
	if err := os.MkdirAll(nestedDirectory, 0o755); err != nil {
		t.Fatal(err)
	}
	definitions := map[string]string{
		"orders.flow.json":     `{"schemaVersion":"1.0","valid":true,"source":{"language":"go","path":"orders.go"},"flow":{"name":"Orders"},"nodes":[],"edges":[],"diagnostics":[]}`,
		"nested/billing.json":  `{"schemaVersion":"1.0","valid":false,"source":{"language":"python","path":"billing.py"},"flow":{"name":"Billing"},"nodes":[],"edges":[],"diagnostics":[{"severity":"error","code":"unknown","message":"dynamic"}]}`,
		"nested/multiple.json": `{"schemaVersion":"1.0","valid":false,"source":{"language":"go","path":"flows.go"},"flow":{"name":""},"nodes":[],"edges":[],"diagnostics":[{"severity":"error","code":"multiple_flows","message":"source must define exactly one Flow"}]}`,
	}
	for name, contents := range definitions {
		if err := os.WriteFile(filepath.Join(directory, filepath.FromSlash(name)), []byte(contents), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	harness := newHarnessWithConfig(t, &flowService{}, &dexweb.Config{
		BindAddress:            "127.0.0.1",
		Port:                   dexweb.DefaultPort,
		FlowRenderingDirectory: directory,
	})

	response := get(t, harness.http.URL+"/api/flow-definitions")
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("Flow definitions status = %d body=%q", response.StatusCode, readBody(t, response))
	}
	var catalog struct {
		Configured  bool   `json:"configured"`
		Directory   string `json:"directory"`
		Definitions []struct {
			ID       string          `json:"id"`
			FlowName string          `json:"flowName"`
			Valid    bool            `json:"valid"`
			Graph    json.RawMessage `json:"graph"`
		} `json:"definitions"`
	}
	decodeResponse(t, response, &catalog)
	if !catalog.Configured || catalog.Directory != directory {
		t.Fatalf("unexpected Flow definition catalog: %+v", catalog)
	}
	if len(catalog.Definitions) != 3 || catalog.Definitions[0].ID != "nested/billing.json" || catalog.Definitions[1].ID != "nested/multiple.json" || catalog.Definitions[2].ID != "orders.flow.json" {
		t.Fatalf("unexpected Flow definitions: %+v", catalog.Definitions)
	}
	if catalog.Definitions[0].FlowName != "Billing" || catalog.Definitions[0].Valid || len(catalog.Definitions[0].Graph) == 0 {
		t.Fatalf("unexpected Billing definition: %+v", catalog.Definitions[0])
	}
	if catalog.Definitions[1].FlowName != "nested/multiple" || catalog.Definitions[1].Valid {
		t.Fatalf("unexpected multiple-Flow definition: %+v", catalog.Definitions[1])
	}
}

func TestWebServerMapsGRPCErrors(t *testing.T) {
	harness := newHarness(t, &flowService{})
	response := get(t, harness.http.URL+"/api/flows/summary?flowId=invalid")
	defer response.Body.Close()
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("summary status = %d", response.StatusCode)
	}
	var result struct {
		Error    string `json:"error"`
		GRPCCode int32  `json:"grpcCode"`
	}
	decodeResponse(t, response, &result)
	if result.Error != "invalid flow" || result.GRPCCode != int32(codes.InvalidArgument) {
		t.Fatalf("unexpected error response: %+v", result)
	}

	missing := get(
		t,
		harness.http.URL+"/api/flows/summary?flowId=missing-flow&runId=missing-run",
	)
	defer missing.Body.Close()
	if missing.StatusCode != http.StatusNotFound {
		t.Fatalf("missing summary status = %d", missing.StatusCode)
	}
	decodeResponse(t, missing, &result)
	if result.Error != `Flow run is not found for flow ID "missing-flow" and run ID "missing-run"` {
		t.Fatalf("missing summary error = %q", result.Error)
	}
	if result.GRPCCode != int32(codes.NotFound) {
		t.Fatalf("missing summary gRPC code = %d", result.GRPCCode)
	}
}

func TestWebServerReadsStream(t *testing.T) {
	service := &flowService{streamRequests: make(chan *dexpb.ReadStreamRequest, 2)}
	harness := newHarness(t, service)

	response := get(
		t,
		harness.http.URL+"/api/flows/stream?flowId=checkout-1&flowType=CheckoutFlow&streamName=thinking&resumeToken=previous-token",
	)
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("read stream status = %d body=%q", response.StatusCode, readBody(t, response))
	}
	var result struct {
		Value       string `json:"value"`
		ResumeToken string `json:"resumeToken"`
		CreatedTime string `json:"createdTime"`
		Source      string `json:"source"`
	}
	decodeResponse(t, response, &result)
	if result.Value != "reasoning token" ||
		result.ResumeToken != "next-token" ||
		result.CreatedTime != "2026-08-27T12:34:56Z" ||
		result.Source != "#Step-1" {
		t.Fatalf("unexpected Stream message: %+v", result)
	}
	streamRequest := <-service.streamRequests
	if streamRequest.GetFlowId() != "checkout-1" ||
		streamRequest.GetFlowType() != "CheckoutFlow" ||
		streamRequest.GetStreamName() != "thinking" ||
		streamRequest.GetResumeToken() != "previous-token" ||
		streamRequest.GetWaitTimeSeconds() != 0 {
		t.Fatalf("unexpected ReadStream request: %+v", streamRequest)
	}

	timeoutResponse := get(
		t,
		harness.http.URL+"/api/flows/stream?flowId=checkout-1&flowType=CheckoutFlow&streamName=timeout",
	)
	defer timeoutResponse.Body.Close()
	if timeoutResponse.StatusCode != http.StatusRequestTimeout {
		t.Fatalf("stream timeout status = %d body=%q", timeoutResponse.StatusCode, readBody(t, timeoutResponse))
	}

	missingInput := get(t, harness.http.URL+"/api/flows/stream?flowId=checkout-1&flowType=CheckoutFlow")
	defer missingInput.Body.Close()
	if missingInput.StatusCode != http.StatusBadRequest {
		t.Fatalf("missing Stream name status = %d", missingInput.StatusCode)
	}
}

func TestWebServerScopesSearchesToEngineWorkflows(t *testing.T) {
	service := &flowService{
		searchRequests: make(chan *dexpb.SearchFlowsRequest, 2),
	}
	harness := newHarness(t, service)

	testCases := []struct {
		name                  string
		body                  string
		wantQuery             string
		includeContinuedAsNew bool
	}{
		{
			name:      "empty query",
			body:      `{"pageSize":10}`,
			wantQuery: `WorkflowType = "Engine"`,
		},
		{
			name:      "user query",
			body:      `{"query":"ExecutionStatus = \"Running\"","pageSize":10}`,
			wantQuery: `(ExecutionStatus = "Running") AND (WorkflowType = "Engine")`,
		},
		{
			name:                  "include earlier runs",
			body:                  `{"query":"FlowType = 'CheckoutFlow' OR FlowType = 'RefundFlow'","includeContinuedAsNew":true,"pageSize":1,"nextPageToken":"next"}`,
			wantQuery:             `(FlowType = 'CheckoutFlow' OR FlowType = 'RefundFlow') AND (WorkflowType = "Engine")`,
			includeContinuedAsNew: true,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			response := postJSON(t, harness.http.URL+"/api/flows/search", testCase.body)
			defer response.Body.Close()
			if response.StatusCode != http.StatusOK {
				t.Fatalf("search status = %d", response.StatusCode)
			}
			request := <-service.searchRequests
			if request.GetIncludeContinuedAsNew() != testCase.includeContinuedAsNew {
				t.Fatal("run inclusion option was not forwarded")
			}
			if testCase.includeContinuedAsNew && request.GetNextPageToken() != "next" {
				t.Fatal("page token was not forwarded")
			}
			if request.GetQuery() != testCase.wantQuery {
				t.Fatalf("query = %q, want %q", request.GetQuery(), testCase.wantQuery)
			}
		})
	}
}

func TestWebServerLoadsBlobs(t *testing.T) {
	service := &flowService{
		loadBlobsRequests: make(chan *dexpb.LoadBlobsRequest, 1),
	}
	harness := newHarness(t, service)

	blobResponse := postJSON(t, harness.http.URL+"/api/blobs/load", `{
        "flowId": "flow-1",
        "values": [
          {"id":"string-id","kind":"string"},
          {"id":"object-id","kind":"object"},
          {"id":"string-id","kind":"string"}
        ]
      }`)
	defer blobResponse.Body.Close()
	if blobResponse.StatusCode != http.StatusOK {
		t.Fatalf("blob status = %d", blobResponse.StatusCode)
	}
	var blobResult struct {
		Values map[string]interface{} `json:"values"`
	}
	decodeResponse(t, blobResponse, &blobResult)
	if blobResult.Values["string:string-id"] != "loaded string" {
		t.Fatalf("unexpected string blob: %+v", blobResult.Values)
	}
	object, ok := blobResult.Values["object:object-id"].(map[string]interface{})
	if !ok || object["answer"] != float64(42) {
		t.Fatalf("unexpected object blob: %+v", blobResult.Values)
	}
	loadRequest := <-service.loadBlobsRequests
	if len(loadRequest.GetEntries()) != 2 ||
		loadRequest.GetEntries()[0].GetFlowId() != "flow-1" ||
		loadRequest.GetEntries()[0].GetBlobValue().GetInternalBlobIdForStringValue() != "string-id" ||
		loadRequest.GetEntries()[1].GetFlowId() != "flow-1" ||
		loadRequest.GetEntries()[1].GetBlobValue().GetInternalBlobIdForObjValue() != "object-id" {
		t.Fatalf("unexpected LoadBlobs request: %+v", loadRequest)
	}

	errorHarness := newHarness(t, &flowService{
		loadBlobsError: status.Error(codes.Unavailable, "blob store offline"),
	})
	errorResponse := postJSON(
		t,
		errorHarness.http.URL+"/api/blobs/load",
		`{"flowId":"flow-1","values":[{"id":"string-id","kind":"string"}]}`,
	)
	defer errorResponse.Body.Close()
	if errorResponse.StatusCode != http.StatusBadGateway {
		t.Fatalf("blob error status = %d", errorResponse.StatusCode)
	}
	var mappedError struct {
		Error    string `json:"error"`
		GRPCCode int32  `json:"grpcCode"`
	}
	decodeResponse(t, errorResponse, &mappedError)
	if mappedError.Error != "Value blob unavailable" || mappedError.GRPCCode != int32(codes.Unavailable) {
		t.Fatalf("unexpected blob error response: %+v", mappedError)
	}
}

func TestWaitRequestPropagatesCancellation(t *testing.T) {
	service := &flowService{
		waitStarted:  make(chan struct{}),
		waitCanceled: make(chan struct{}),
	}
	harness := newHarness(t, service)
	requestCtx, cancel := context.WithCancel(context.Background())
	request, err := http.NewRequestWithContext(
		requestCtx,
		http.MethodGet,
		harness.http.URL+"/api/flows/wait?flowId=flow&runId=run&nextInternalEventId=1",
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	requestFinished := make(chan error, 1)
	go func() {
		response, requestErr := http.DefaultClient.Do(request)
		if response != nil {
			if closeErr := response.Body.Close(); closeErr != nil {
				requestErr = errors.Join(requestErr, closeErr)
			}
		}
		requestFinished <- requestErr
	}()
	select {
	case <-service.waitStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("gRPC wait did not start")
	}
	cancel()
	select {
	case <-service.waitCanceled:
	case <-time.After(5 * time.Second):
		t.Fatal("gRPC wait context was not canceled")
	}
	select {
	case requestErr := <-requestFinished:
		if requestErr == nil {
			t.Fatal("canceled HTTP request unexpectedly succeeded")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("canceled HTTP request did not return")
	}
}

func TestWaitRequestUsesApplicationDeadline(t *testing.T) {
	service := &flowService{waitDeadline: make(chan time.Time, 1)}
	harness := newHarness(t, service)
	requestStartedAt := time.Now()
	response := get(
		t,
		harness.http.URL+"/api/flows/wait?flowId=flow&runId=run&nextInternalEventId=1",
	)
	defer response.Body.Close()
	if response.StatusCode != http.StatusRequestTimeout {
		t.Fatalf("wait status = %d body=%q", response.StatusCode, readBody(t, response))
	}
	deadline := <-service.waitDeadline
	requestTimeout := deadline.Sub(requestStartedAt)
	if requestTimeout < 45*time.Second || requestTimeout > 55*time.Second {
		t.Fatalf("wait request timeout = %s, want approximately 50s", requestTimeout)
	}
	var mappedError struct {
		Error    string `json:"error"`
		GRPCCode int32  `json:"grpcCode"`
	}
	decodeResponse(t, response, &mappedError)
	if mappedError.GRPCCode != int32(codes.DeadlineExceeded) {
		t.Fatalf("unexpected wait error response: %+v", mappedError)
	}
}

func TestWebServerStopsFlow(t *testing.T) {
	service := &flowService{
		stopRequests: make(chan *dexpb.StopFlowRequest, 1),
	}
	harness := newHarness(t, service)

	response := postJSON(t, harness.http.URL+"/api/flows/stop", `{
		"flowId":"checkout-1",
		"runId":"run-1",
		"stopType":3,
		"reason":"operator fail"
	}`)
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("stop status = %d", response.StatusCode)
	}
	stopRequest := <-service.stopRequests
	if stopRequest.GetFlowId() != "checkout-1" ||
		stopRequest.GetRunId() != "run-1" ||
		stopRequest.GetStopType() != dexpb.StopType_STOP_TYPE_FAIL ||
		stopRequest.GetReason() != "operator fail" {
		t.Fatalf("unexpected StopFlow request: %+v", stopRequest)
	}

	missingReason := postJSON(t, harness.http.URL+"/api/flows/stop", `{
		"flowId":"checkout-1",
		"runId":"run-1",
		"stopType":2,
		"reason":" "
	}`)
	defer missingReason.Body.Close()
	if missingReason.StatusCode != http.StatusBadRequest {
		t.Fatalf("missing reason status = %d", missingReason.StatusCode)
	}
}

func (s *flowService) SearchFlows(
	_ context.Context,
	request *dexpb.SearchFlowsRequest,
) (*dexpb.SearchFlowsResponse, error) {
	if s.searchRequests != nil {
		s.searchRequests <- request
	}
	return &dexpb.SearchFlowsResponse{
		FlowRuns: []*dexpb.SearchFlowsResponseEntry{{
			FlowId:   "checkout-1",
			RunId:    "run-1",
			FlowType: "checkout",
		}},
	}, nil
}

func (s *flowService) GetFlowSummary(
	_ context.Context,
	request *dexpb.GetFlowSummaryRequest,
) (*dexpb.GetFlowSummaryResponse, error) {
	if request.GetFlowId() == "missing-flow" {
		return nil, status.Errorf(
			codes.NotFound,
			`workflow execution not found for workflow ID %q and run ID %q`,
			request.GetFlowId(),
			request.GetRunId(),
		)
	}
	return nil, status.Error(codes.InvalidArgument, "invalid flow")
}

func (s *flowService) ReadStream(
	_ context.Context,
	request *dexpb.ReadStreamRequest,
) (*dexpb.ReadStreamResponse, error) {
	if s.streamRequests != nil {
		s.streamRequests <- request
	}
	if request.GetStreamName() == "timeout" {
		return nil, status.Error(codes.DeadlineExceeded, "stream read timed out")
	}
	return &dexpb.ReadStreamResponse{Message: &dexpb.StreamMessage{
		Value:       &dexpb.Value{Kind: &dexpb.Value_StringValue{StringValue: "reasoning token"}},
		ResumeToken: "next-token",
		CreatedTime: timestamppb.New(time.Date(2026, time.August, 27, 12, 34, 56, 0, time.UTC)),
		Source:      "#Step-1",
	}}, nil
}

func (s *flowService) LoadBlobs(
	_ context.Context,
	request *dexpb.LoadBlobsRequest,
) (*dexpb.LoadBlobsResponse, error) {
	if s.loadBlobsError != nil {
		return nil, s.loadBlobsError
	}
	s.loadBlobsRequests <- request
	return &dexpb.LoadBlobsResponse{Values: map[string]*dexpb.Value{
		"string-id": {Kind: &dexpb.Value_StringValue{StringValue: "loaded string"}},
		"object-id": {Kind: &dexpb.Value_ObjValue{ObjValue: &dexpb.EncodedObject{
			Encoding: "json",
			Payload:  []byte(`{"answer":42}`),
		}}},
	}}, nil
}

func (s *flowService) WaitForHistoryEvent(
	ctx context.Context,
	request *dexpb.WaitForHistoryEventRequest,
) (*dexpb.WaitForHistoryEventResponse, error) {
	if s.waitDeadline != nil {
		deadline, hasDeadline := ctx.Deadline()
		if !hasDeadline {
			return nil, status.Error(codes.Internal, "history wait has no deadline")
		}
		s.waitDeadline <- deadline
		return nil, status.Error(codes.DeadlineExceeded, "history wait timed out")
	}
	close(s.waitStarted)
	<-ctx.Done()
	close(s.waitCanceled)
	return nil, ctx.Err()
}

func (s *flowService) StopFlow(
	_ context.Context,
	request *dexpb.StopFlowRequest,
) (*emptypb.Empty, error) {
	s.stopRequests <- request
	return &emptypb.Empty{}, nil
}

func (s *flowService) PublishToChannel(
	_ context.Context,
	request *dexpb.PublishToChannelRequest,
) (*emptypb.Empty, error) {
	s.publishRequests <- request
	return &emptypb.Empty{}, nil
}

func (s *flowService) SetAttributes(
	_ context.Context,
	request *dexpb.SetAttributesRequest,
) (*emptypb.Empty, error) {
	s.setRequests <- request
	return &emptypb.Empty{}, nil
}

func (s *flowService) ResetFlow(
	_ context.Context,
	request *dexpb.ResetFlowRequest,
) (*dexpb.ResetFlowResponse, error) {
	s.timeTravelRequests <- request
	return &dexpb.ResetFlowResponse{RunId: "run-2"}, nil
}

type harness struct {
	http *httptest.Server
}

func newHarness(t *testing.T, service dexpb.FlowServiceServer) *harness {
	t.Helper()
	return newHarnessWithConfig(t, service, &dexweb.Config{
		BindAddress: "127.0.0.1",
		Port:        dexweb.DefaultPort,
	})
}

func newHarnessWithConfig(t *testing.T, service dexpb.FlowServiceServer, cfg *dexweb.Config) *harness {
	t.Helper()
	grpcListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	grpcServer := grpc.NewServer()
	dexpb.RegisterFlowServiceServer(grpcServer, service)
	serveErrors := make(chan error, 1)
	go func() {
		serveErrors <- grpcServer.Serve(grpcListener)
	}()
	connection, err := grpc.NewClient(
		grpcListener.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		grpcServer.Stop()
		t.Fatal(err)
	}
	webServer, err := dexweb.NewServer(
		cfg,
		dexpb.NewFlowServiceClient(connection),
		assets.Files,
	)
	if err != nil {
		if closeErr := connection.Close(); closeErr != nil {
			t.Errorf("close gRPC connection: %v", closeErr)
		}
		grpcServer.Stop()
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(webServer.Handler())
	t.Cleanup(func() {
		httpServer.Close()
		if err := connection.Close(); err != nil {
			t.Errorf("close gRPC connection: %v", err)
		}
		grpcServer.Stop()
		if err := <-serveErrors; err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			t.Errorf("gRPC server: %v", err)
		}
	})
	return &harness{http: httpServer}
}

func postJSON(t *testing.T, url string, body string) *http.Response {
	t.Helper()
	response, err := http.Post(url, "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func sendJSON(t *testing.T, method string, url string, body string) *http.Response {
	t.Helper()
	request, err := http.NewRequest(method, url, bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func get(t *testing.T, url string) *http.Response {
	t.Helper()
	response, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func decodeResponse(t *testing.T, response *http.Response, value interface{}) {
	t.Helper()
	if err := json.NewDecoder(response.Body).Decode(value); err != nil {
		t.Fatal(err)
	}
}

func readBody(t *testing.T, response *http.Response) string {
	t.Helper()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}
