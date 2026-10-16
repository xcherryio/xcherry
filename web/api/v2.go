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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/superdurable/dex/gen/dexpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

const (
	v2DefaultPageSize           = 50
	v2RPCConcurrency            = 8
	v2RPCTimeout                = 5 * time.Second
	v2WorkQueuePermissionsIndex = "DexWorkQueuePermissions"
	V2DefinitionRevisionHeader  = "X-Dex-Flow-Definition-Revision"
)

var v2PermissionPattern = regexp.MustCompile(`^[a-z][a-z0-9]*(?:[.-][a-z0-9]+)*$`)

// V2Definition describes one Flow type's Dex Web v2 contract.
type V2Definition struct {
	IndexedAttributes        []V2IndexedAttribute        `json:"indexedAttributes"`
	Summary                  V2RPCView                   `json:"summary"`
	Display                  V2RPCView                   `json:"display"`
	Actions                  []V2Action                  `json:"actions"`
	Start                    *V2StartDefinition          `json:"start,omitempty"`
	ConnectorTriggerBindings []V2ConnectorTriggerBinding `json:"connectorTriggerBindings,omitempty"`
}

// V2ConnectorTriggerBinding describes one configurable external Trigger binding.
type V2ConnectorTriggerBinding struct {
	ConnectorID          string                     `json:"connectorId"`
	TriggerName          string                     `json:"triggerName"`
	ConnectionName       string                     `json:"connectionName"`
	BindingName          string                     `json:"bindingName"`
	ModulePath           string                     `json:"modulePath"`
	ModuleVersion        string                     `json:"moduleVersion"`
	ConfigurationEnabled bool                       `json:"configurationEnabled"`
	ConfigurationUI      V2ConnectorConfigurationUI `json:"configurationUI"`
}

// V2ConnectorConfigurationUI declares a Flow-owned composition of Connector UI units.
type V2ConnectorConfigurationUI struct {
	Units []V2ConnectorUIUnit `json:"units"`
}

// V2ConnectorUIUnit describes one reusable Connector UI unit instance.
type V2ConnectorUIUnit struct {
	ID          string                 `json:"id"`
	UnitID      string                 `json:"unitId"`
	Label       string                 `json:"label"`
	Description string                 `json:"description,omitempty"`
	Required    bool                   `json:"required"`
	Bindings    []V2ConnectorUIBinding `json:"bindings"`
}

// V2ConnectorUIBinding maps a unit port into the use configuration object.
type V2ConnectorUIBinding struct {
	Port        string `json:"port"`
	JSONPointer string `json:"jsonPointer"`
}

// V2StartDefinition describes the Start Step and its JSON input.
type V2StartDefinition struct {
	StepType string             `json:"stepType"`
	Input    V2StartInputSchema `json:"input"`
	// SkipWaitFor is derived from the validated graph's Start Step phase and is never browser input.
	SkipWaitFor bool `json:"-"`
}

// V2StartInputSchema describes one recursive JSON input value.
type V2StartInputSchema struct {
	Kind        string                  `json:"kind"`
	Nullable    bool                    `json:"nullable,omitempty"`
	Format      string                  `json:"format,omitempty"`
	EnumValues  []V2StartInputEnumValue `json:"enumValues,omitempty"`
	Minimum     string                  `json:"minimum,omitempty"`
	Maximum     string                  `json:"maximum,omitempty"`
	Fields      []V2StartInputField     `json:"fields,omitempty"`
	Items       *V2StartInputSchema     `json:"items,omitempty"`
	Values      *V2StartInputSchema     `json:"values,omitempty"`
	FixedLength *int64                  `json:"fixedLength,omitempty"`
}

// V2StartInputEnumValue describes one source-named enum option.
type V2StartInputEnumValue struct {
	Name  string      `json:"name"`
	Value interface{} `json:"value"`
}

// V2StartInputField describes one JSON object field.
type V2StartInputField struct {
	Name     string             `json:"name"`
	Required bool               `json:"required"`
	Schema   V2StartInputSchema `json:"schema"`
}

// V2IndexedAttribute describes one searchable Attribute.
type V2IndexedAttribute struct {
	AttributeKey string `json:"attributeKey"`
	IndexKey     string `json:"indexKey"`
	IndexType    string `json:"indexType"`
	ValueType    string `json:"valueType"`
	Description  string `json:"description"`
}

// V2RPCView describes Summary or Display fields.
type V2RPCView struct {
	RPCName string        `json:"rpcName"`
	Fields  []V2ViewField `json:"fields"`
}

// V2ViewField describes one ordered RPC output field.
type V2ViewField struct {
	AttributeKey string `json:"attributeKey"`
	ValueType    string `json:"valueType"`
	Editable     bool   `json:"editable"`
	Description  string `json:"description"`
	// UISlot is the named position this field takes in a row or drawer. Empty means the detail list.
	UISlot string `json:"uiSlot,omitempty"`
}

// V2Action describes one operator RPC.
type V2Action struct {
	RPCName            string            `json:"rpcName"`
	Label              string            `json:"label"`
	RequiredPermission string            `json:"requiredPermission"`
	Condition          V2ActionCondition `json:"condition"`
	Input              V2ActionInput     `json:"input"`
}

// V2ActionCondition describes an Action's visibility predicate.
type V2ActionCondition struct {
	AttributeKey string        `json:"attributeKey"`
	Operator     string        `json:"operator"`
	Values       []interface{} `json:"values"`
}

// V2ActionInput describes a none or object RPC input.
type V2ActionInput struct {
	Kind   string               `json:"kind"`
	Fields []V2ActionInputField `json:"fields"`
}

// V2ActionInputField describes one ordered Action input.
type V2ActionInputField struct {
	FieldName    string `json:"fieldName"`
	ValueType    string `json:"valueType"`
	Source       string `json:"source"`
	AttributeKey string `json:"attributeKey,omitempty"`
	Capture      string `json:"capture,omitempty"`
	Required     bool   `json:"required"`
	Description  string `json:"description"`
}

type v2Handler struct {
	client                          dexpb.FlowServiceClient
	loadDefinitions                 V2DefinitionLoader
	isStartFlowWorkerTargetHeadless bool
	isStartFlowWorkerTargetPull     bool
	checkStartFlowWorkerHealth      V2WorkerHealthChecker
}

// V2DefinitionSnapshot is one request's immutable catalog revision.
type V2DefinitionSnapshot struct {
	Definitions map[string]V2Definition
	Revision    string
}

// V2DefinitionLoader loads one validated definition snapshot.
type V2DefinitionLoader func(context.Context) (V2DefinitionSnapshot, error)

// V2WorkerHealthChecker checks whether Dex Web can reach a Worker target.
type V2WorkerHealthChecker func(context.Context, string) error

// V2HandlerConfig controls server-side v2 behavior.
type V2HandlerConfig struct {
	IsStartFlowWorkerTargetHeadless bool
	IsStartFlowWorkerTargetPull     bool
	WorkerHealthChecker             V2WorkerHealthChecker
}

type v2CatalogEntry struct {
	FlowType   string       `json:"flowType"`
	Definition V2Definition `json:"definition"`
}

type v2Filter struct {
	Field    string        `json:"field"`
	Operator string        `json:"operator"`
	Values   []interface{} `json:"values"`
}

type v2SearchRequest struct {
	FlowType             string     `json:"flowType"`
	WorkQueuePermissions []string   `json:"workQueuePermissions"`
	Filters              []v2Filter `json:"filters"`
	PageSize             int32      `json:"pageSize"`
	NextPageToken        string     `json:"nextPageToken"`
}

type v2SearchResponse struct {
	Flows         []v2Flow `json:"flows"`
	NextPageToken string   `json:"nextPageToken"`
}

type v2Flow struct {
	FlowID            string                 `json:"flowId"`
	FlowType          string                 `json:"flowType"`
	FlowStatus        string                 `json:"flowStatus"`
	FlowStatusCode    int32                  `json:"flowStatusCode"`
	StartTime         *string                `json:"startTime"`
	CloseTime         *string                `json:"closeTime"`
	IndexedAttributes map[string]interface{} `json:"indexedAttributes"`
	Summary           map[string]interface{} `json:"summary,omitempty"`
	SummaryError      string                 `json:"summaryError,omitempty"`
}

type v2DisplayResponse struct {
	FlowID            string                 `json:"flowId"`
	FlowType          string                 `json:"flowType"`
	FlowStatus        string                 `json:"flowStatus"`
	FlowStatusCode    int32                  `json:"flowStatusCode"`
	IsActive          bool                   `json:"isActive"`
	Display           map[string]interface{} `json:"display"`
	AttributeSnapshot map[string]interface{} `json:"attributeSnapshot"`
	EligibleActions   []string               `json:"eligibleActions"`
}

type v2EditRequest struct {
	FlowType     string      `json:"flowType"`
	FlowID       string      `json:"flowId"`
	AttributeKey string      `json:"attributeKey"`
	Value        interface{} `json:"value"`
}

type v2ActionRequest struct {
	FlowType             string                 `json:"flowType"`
	FlowID               string                 `json:"flowId"`
	RPCName              string                 `json:"rpcName"`
	WorkQueuePermissions []string               `json:"workQueuePermissions"`
	Input                map[string]interface{} `json:"input"`
	AttributeSnapshot    map[string]interface{} `json:"attributeSnapshot"`
}

func RegisterV2Handlers(
	mux *http.ServeMux,
	client dexpb.FlowServiceClient,
	definitions map[string]V2Definition,
) {
	RegisterDynamicV2Handlers(
		mux,
		client,
		func(context.Context) (V2DefinitionSnapshot, error) {
			return V2DefinitionSnapshot{Definitions: definitions}, nil
		},
		V2HandlerConfig{},
	)
}

// RegisterDynamicV2Handlers registers v2 routes backed by a per-request definition loader.
func RegisterDynamicV2Handlers(
	mux *http.ServeMux,
	client dexpb.FlowServiceClient,
	loader V2DefinitionLoader,
	config V2HandlerConfig,
) {
	if mux == nil {
		panic("HTTP mux must not be nil")
	}
	if client == nil {
		panic("Dex FlowService client must not be nil")
	}
	handler := &v2Handler{
		client: client, loadDefinitions: loader,
		isStartFlowWorkerTargetHeadless: config.IsStartFlowWorkerTargetHeadless,
		isStartFlowWorkerTargetPull:     config.IsStartFlowWorkerTargetPull,
		checkStartFlowWorkerHealth:      config.WorkerHealthChecker,
	}
	if handler.checkStartFlowWorkerHealth == nil {
		handler.checkStartFlowWorkerHealth = checkV2WorkerPortHealth
	}
	mux.HandleFunc("GET /api/v2/catalog", handler.catalog)
	mux.HandleFunc("POST /api/v2/worker-health", handler.checkWorkerHealth)
	mux.HandleFunc("POST /api/v2/start", handler.startFlow)
	mux.HandleFunc("POST /api/v2/search", handler.search)
	mux.HandleFunc("GET /api/v2/display", handler.display)
	mux.HandleFunc("PATCH /api/v2/display", handler.editDisplay)
	mux.HandleFunc("POST /api/v2/actions", handler.invokeAction)
}

func (h *v2Handler) catalog(response http.ResponseWriter, request *http.Request) {
	snapshot, ok := h.loadSnapshot(response, request, false)
	if !ok {
		return
	}
	flowTypes := make([]string, 0, len(snapshot.Definitions))
	for flowType := range snapshot.Definitions {
		flowTypes = append(flowTypes, flowType)
	}
	sort.Strings(flowTypes)
	entries := make([]v2CatalogEntry, 0, len(flowTypes))
	for _, flowType := range flowTypes {
		entries = append(entries, v2CatalogEntry{
			FlowType: flowType, Definition: snapshot.Definitions[flowType],
		})
	}
	setDefinitionETag(response, snapshot.Revision)
	writeJSON(response, http.StatusOK, map[string]interface{}{
		"enabled":            len(entries) > 0,
		"flows":              entries,
		"definitionRevision": snapshot.Revision,
	})
}

func (h *v2Handler) search(response http.ResponseWriter, request *http.Request) {
	snapshot, ok := h.loadSnapshot(response, request, true)
	if !ok {
		return
	}
	var body v2SearchRequest
	if err := decodeJSON(response, request, &body); err != nil {
		WriteError(response, http.StatusBadRequest, err.Error(), nil)
		return
	}
	permissions, permitted := h.resolvePermissions(response, body.WorkQueuePermissions)
	if !permitted {
		return
	}
	definition, ok := snapshot.Definitions[body.FlowType]
	if !ok {
		WriteError(response, http.StatusBadRequest, "flowType has no valid Flow Definition Graph 2.0 contract", nil)
		return
	}
	if body.PageSize < 0 {
		WriteError(response, http.StatusBadRequest, "pageSize must be non-negative", nil)
		return
	}
	if body.PageSize == 0 || body.PageSize > v2DefaultPageSize {
		body.PageSize = v2DefaultPageSize
	}
	query, err := compileV2Query(body.FlowType, permissions, body.Filters, definition)
	if err != nil {
		WriteError(response, http.StatusBadRequest, err.Error(), nil)
		return
	}
	result, err := h.client.SearchFlows(request.Context(), &dexpb.SearchFlowsRequest{
		Query:         engineWorkflowSearchQuery(query),
		PageSize:      body.PageSize,
		NextPageToken: body.NextPageToken,
	})
	if err != nil {
		writeGRPCError(response, err, "SearchFlows")
		return
	}
	flowRuns := append([]*dexpb.SearchFlowsResponseEntry(nil), result.GetFlowRuns()...)
	sort.SliceStable(flowRuns, func(left int, right int) bool {
		return flowRunStartTime(flowRuns[left]).After(flowRunStartTime(flowRuns[right]))
	})
	flows := make([]v2Flow, 0, len(flowRuns))
	seenFlowIDs := make(map[string]struct{}, len(result.GetFlowRuns()))
	for _, entry := range flowRuns {
		if _, seen := seenFlowIDs[entry.GetFlowId()]; seen {
			continue
		}
		seenFlowIDs[entry.GetFlowId()] = struct{}{}
		indexedValues := make(map[string]interface{}, len(definition.IndexedAttributes))
		physicalValues := keyValueMap(entry.GetIndexedAttributes())
		for _, attribute := range definition.IndexedAttributes {
			indexedValues[attribute.AttributeKey] = v2ResponseValue(
				physicalValues[attribute.IndexKey],
				attribute.ValueType,
			)
		}
		flows = append(flows, v2Flow{
			FlowID: entry.GetFlowId(), FlowType: entry.GetFlowType(),
			FlowStatus: flowStatusLabel(entry.GetFlowStatus()), FlowStatusCode: int32(entry.GetFlowStatus()),
			StartTime: timestamp(entry.GetStartTime()), CloseTime: timestamp(entry.GetCloseTime()),
			IndexedAttributes: indexedValues,
		})
	}
	h.loadSummaries(request.Context(), definition.Summary, flows)
	writeJSON(response, http.StatusOK, v2SearchResponse{
		Flows: flows, NextPageToken: result.GetNextPageToken(),
	})
}

func (h *v2Handler) loadSummaries(
	ctx context.Context,
	view V2RPCView,
	flows []v2Flow,
) {
	semaphore := make(chan struct{}, v2RPCConcurrency)
	var waitGroup sync.WaitGroup
	for index := range flows {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			semaphore <- struct{}{}
			defer func() { <-semaphore }()
			callContext, cancelCall := context.WithTimeout(ctx, v2RPCTimeout)
			defer cancelCall()
			values, err := h.invokeView(callContext, flows[index].FlowID, view)
			if err != nil {
				flows[index].SummaryError = err.Error()
				return
			}
			flows[index].Summary = values
		}()
	}
	waitGroup.Wait()
}

func (h *v2Handler) display(response http.ResponseWriter, request *http.Request) {
	snapshot, ok := h.loadSnapshot(response, request, true)
	if !ok {
		return
	}
	for parameter, values := range request.URL.Query() {
		if (parameter != "flowType" && parameter != "flowId") || len(values) != 1 {
			WriteError(response, http.StatusBadRequest, "display accepts only one flowType and flowId", nil)
			return
		}
	}
	flowType := request.URL.Query().Get("flowType")
	flowID := request.URL.Query().Get("flowId")
	definition, ok := snapshot.Definitions[flowType]
	if !ok || flowID == "" {
		WriteError(response, http.StatusBadRequest, "flowType and flowId are required", nil)
		return
	}
	summary, err := h.client.GetFlowSummary(request.Context(), &dexpb.GetFlowSummaryRequest{FlowId: flowID})
	if err != nil {
		writeGRPCError(response, err, "GetFlowSummary")
		return
	}
	if summary.GetFlowType() != flowType {
		WriteError(response, http.StatusNotFound, "Flow does not match the requested flowType", nil)
		return
	}
	displayValues, err := h.invokeView(request.Context(), flowID, definition.Display)
	if err != nil {
		writeGRPCError(response, err, definition.Display.RPCName)
		return
	}
	snapshotKeys := v2SnapshotKeys(definition)
	attributeSnapshot := make(map[string]interface{}, len(snapshotKeys))
	if len(snapshotKeys) > 0 {
		attributes, attributeErr := h.client.GetAttributes(request.Context(), &dexpb.GetAttributesRequest{
			FlowId: flowID, Keys: snapshotKeys,
		})
		if attributeErr != nil {
			writeGRPCError(response, attributeErr, "GetAttributes")
			return
		}
		attributeSnapshot = keyValueMap(attributes.GetAttributes())
	}
	eligibleActions := make([]string, 0, len(definition.Actions))
	for _, action := range definition.Actions {
		if actionConditionMatches(action.Condition, attributeSnapshot[action.Condition.AttributeKey]) {
			eligibleActions = append(eligibleActions, action.RPCName)
		}
	}
	isActive := summary.GetFlowStatus() == dexpb.FlowStatus_FLOW_STATUS_RUNNING
	writeJSON(response, http.StatusOK, v2DisplayResponse{
		FlowID: flowID, FlowType: flowType,
		FlowStatus: flowStatusLabel(summary.GetFlowStatus()), FlowStatusCode: int32(summary.GetFlowStatus()),
		IsActive: isActive, Display: displayValues, AttributeSnapshot: v2ResponseSnapshot(attributeSnapshot),
		EligibleActions: eligibleActions,
	})
}

func (h *v2Handler) editDisplay(response http.ResponseWriter, request *http.Request) {
	snapshot, ok := h.loadSnapshot(response, request, true)
	if !ok {
		return
	}
	var body v2EditRequest
	if err := decodeJSON(response, request, &body); err != nil {
		WriteError(response, http.StatusBadRequest, err.Error(), nil)
		return
	}
	definition, ok := snapshot.Definitions[body.FlowType]
	if !ok || body.FlowID == "" || body.AttributeKey == "" {
		WriteError(response, http.StatusBadRequest, "flowType, flowId, and attributeKey are required", nil)
		return
	}
	field, ok := editableDisplayField(definition.Display.Fields, body.AttributeKey)
	if !ok {
		WriteError(response, http.StatusBadRequest, "attribute is not editable", nil)
		return
	}
	if err := h.requireActiveFlow(request.Context(), body.FlowID, body.FlowType); err != nil {
		writeGRPCError(response, err, "GetFlowSummary")
		return
	}
	value, err := encodeV2Value(body.Value, field.ValueType)
	if err != nil {
		WriteError(response, http.StatusBadRequest, err.Error(), nil)
		return
	}
	write := &dexpb.AttributeWrite{Key: body.AttributeKey, Value: value}
	if indexedAttribute, indexed := findIndexedAttribute(definition, body.AttributeKey); indexed {
		indexType, mapErr := v2IndexType(indexedAttribute.IndexType)
		if mapErr != nil {
			WriteError(response, http.StatusInternalServerError, mapErr.Error(), nil)
			return
		}
		write.IndexConfig = &dexpb.IndexConfig{
			Enable: true, Type: indexType, IndexKey: indexedAttribute.IndexKey,
		}
	}
	actionPermissionMappings, err := actionPermissionMappingsForAttributeWrite(definition, body.AttributeKey)
	if err != nil {
		WriteError(response, http.StatusInternalServerError, err.Error(), nil)
		return
	}
	_, err = h.client.SetAttributes(request.Context(), &dexpb.SetAttributesRequest{
		FlowId: body.FlowID, Attributes: []*dexpb.AttributeWrite{write}, RequestId: uuid.NewString(),
		ActionPermissionMappings: actionPermissionMappings,
	})
	if err != nil {
		writeGRPCError(response, err, "SetAttributes")
		return
	}
	writeJSON(response, http.StatusOK, map[string]interface{}{
		"attributeKey": body.AttributeKey, "value": body.Value,
	})
}

func actionPermissionMappingsForAttributeWrite(
	definition V2Definition,
	attributeKey string,
) (*dexpb.ActionPermissionMappings, error) {
	isActionSource := false
	for _, action := range definition.Actions {
		if action.Condition.AttributeKey == attributeKey {
			isActionSource = true
			break
		}
	}
	if !isActionSource {
		return nil, nil
	}

	mappings := make([]*dexpb.ActionPermissionMapping, 0, len(definition.Actions))
	for _, action := range definition.Actions {
		equalValues := make([]*dexpb.Value, 0, len(action.Condition.Values))
		for _, conditionValue := range action.Condition.Values {
			value, err := encodeActionPermissionConditionValue(conditionValue)
			if err != nil {
				return nil, fmt.Errorf("Action %q condition: %w", action.RPCName, err)
			}
			equalValues = append(equalValues, value)
		}
		mappings = append(mappings, &dexpb.ActionPermissionMapping{
			AttributeKey:       action.Condition.AttributeKey,
			EqualValues:        equalValues,
			RequiredPermission: action.RequiredPermission,
		})
	}
	return &dexpb.ActionPermissionMappings{Mappings: mappings}, nil
}

func encodeActionPermissionConditionValue(value interface{}) (*dexpb.Value, error) {
	switch typed := value.(type) {
	case string:
		return &dexpb.Value{Kind: &dexpb.Value_StringValue{StringValue: typed}}, nil
	case bool:
		return &dexpb.Value{Kind: &dexpb.Value_BoolValue{BoolValue: typed}}, nil
	case int, int32, int64, json.Number:
		if integer, ok := jsonNumberInt64(value); ok {
			return &dexpb.Value{Kind: &dexpb.Value_IntValue{IntValue: integer}}, nil
		}
		if number, ok := jsonNumberFloat64(value); ok {
			return &dexpb.Value{Kind: &dexpb.Value_DoubleValue{DoubleValue: number}}, nil
		}
	case float32, float64:
		if number, ok := jsonNumberFloat64(value); ok {
			return &dexpb.Value{Kind: &dexpb.Value_DoubleValue{DoubleValue: number}}, nil
		}
	}
	return nil, fmt.Errorf("value must be a string, integer, finite double, or boolean")
}

func (h *v2Handler) invokeAction(response http.ResponseWriter, request *http.Request) {
	snapshot, ok := h.loadSnapshot(response, request, true)
	if !ok {
		return
	}
	var body v2ActionRequest
	if err := decodeJSON(response, request, &body); err != nil {
		WriteError(response, http.StatusBadRequest, err.Error(), nil)
		return
	}
	permissions, permitted := h.resolvePermissions(response, body.WorkQueuePermissions)
	if !permitted {
		return
	}
	definition, ok := snapshot.Definitions[body.FlowType]
	if !ok || body.FlowID == "" || body.RPCName == "" {
		WriteError(response, http.StatusBadRequest, "flowType, flowId, and rpcName are required", nil)
		return
	}
	action, ok := findV2Action(definition.Actions, body.RPCName)
	if !ok {
		WriteError(response, http.StatusBadRequest, "rpcName is not a declared Action", nil)
		return
	}
	if snapshot.Revision != "" {
		if !containsPermission(permissions, action.RequiredPermission) {
			WriteError(response, http.StatusForbidden, "Action permission denied", nil)
			return
		}
	}
	if err := h.requireActiveFlow(request.Context(), body.FlowID, body.FlowType); err != nil {
		writeGRPCError(response, err, "GetFlowSummary")
		return
	}
	conditionAttributes, err := h.client.GetAttributes(request.Context(), &dexpb.GetAttributesRequest{
		FlowId: body.FlowID, Keys: []string{action.Condition.AttributeKey},
	})
	if err != nil {
		writeGRPCError(response, err, "GetAttributes")
		return
	}
	currentValues := keyValueMap(conditionAttributes.GetAttributes())
	if !actionConditionMatches(action.Condition, currentValues[action.Condition.AttributeKey]) {
		WriteError(response, http.StatusConflict, "Action condition no longer matches the current Flow state", nil)
		return
	}
	input, err := buildActionInput(action, body.Input, body.AttributeSnapshot)
	if err != nil {
		WriteError(response, http.StatusBadRequest, err.Error(), nil)
		return
	}
	result, err := h.client.InvokeRPC(request.Context(), &dexpb.InvokeRPCRequest{
		FlowId: body.FlowID, RpcName: action.RPCName, Input: input,
		TimeoutSeconds: int32(v2RPCTimeout.Seconds()), RequestId: uuid.NewString(),
	})
	if err != nil {
		writeGRPCError(response, err, action.RPCName)
		return
	}
	if dexValue(result.GetOutput()) != nil {
		WriteError(response, http.StatusBadGateway, "Action RPC returned a non-null value", nil)
		return
	}
	writeJSON(response, http.StatusOK, map[string]bool{"invoked": true})
}

func (h *v2Handler) loadSnapshot(
	response http.ResponseWriter,
	request *http.Request,
	requireRevision bool,
) (V2DefinitionSnapshot, bool) {
	snapshot, err := h.loadDefinitions(request.Context())
	if err != nil {
		code := "FLOW_DEFINITION_SOURCE_UNAVAILABLE"
		message := "Flow Definition source is unavailable"
		var coded interface{ DefinitionErrorCode() string }
		if errors.As(err, &coded) {
			code = coded.DefinitionErrorCode()
			if code == "FLOW_DEFINITION_INVALID" {
				message = "Flow Definition source is invalid"
			}
		}
		WriteCodedError(response, http.StatusServiceUnavailable, code, message)
		return V2DefinitionSnapshot{}, false
	}
	if requireRevision && snapshot.Revision != "" && request.Header.Get(V2DefinitionRevisionHeader) != snapshot.Revision {
		WriteCodedError(
			response,
			http.StatusConflict,
			"FLOW_DEFINITION_CHANGED",
			"Flow Definition updated; reload and confirm the operation again",
		)
		return V2DefinitionSnapshot{}, false
	}
	return snapshot, true
}

func (h *v2Handler) resolvePermissions(response http.ResponseWriter, permissions []string) ([]string, bool) {
	if _, err := compileWorkQueuePermissions(permissions); err != nil {
		WriteError(response, http.StatusBadRequest, err.Error(), nil)
		return nil, false
	}
	return permissions, true
}

func containsPermission(permissions []string, required string) bool {
	for _, permission := range permissions {
		if permission == required {
			return true
		}
	}
	return false
}

func setDefinitionETag(response http.ResponseWriter, revision string) {
	if revision != "" {
		response.Header().Set("ETag", strconv.Quote(revision))
	}
}

func (h *v2Handler) invokeView(
	ctx context.Context,
	flowID string,
	view V2RPCView,
) (map[string]interface{}, error) {
	result, err := h.client.InvokeRPC(ctx, &dexpb.InvokeRPCRequest{
		FlowId: flowID, RpcName: view.RPCName,
		Input:          &dexpb.Value{Kind: &dexpb.Value_NullValue{NullValue: structpb.NullValue_NULL_VALUE}},
		TimeoutSeconds: int32(v2RPCTimeout.Seconds()), RequestId: uuid.NewString(),
	})
	if err != nil {
		return nil, err
	}
	output, err := h.hydrateV2Value(ctx, flowID, result.GetOutput())
	if err != nil {
		return nil, err
	}
	values, ok := v2DexValue(output).(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("%s returned a non-object value", view.RPCName)
	}
	if err := validateViewOutput(view, values); err != nil {
		return nil, err
	}
	for _, field := range view.Fields {
		values[field.AttributeKey] = v2ResponseValue(values[field.AttributeKey], field.ValueType)
	}
	return values, nil
}

// InvokeRPC leaves large outputs as blob IDs when lazy loading is enabled.
func (h *v2Handler) hydrateV2Value(
	ctx context.Context,
	flowID string,
	value *dexpb.Value,
) (*dexpb.Value, error) {
	blobID := v2BlobID(value)
	if blobID == "" {
		return value, nil
	}
	result, err := h.client.LoadBlobs(ctx, &dexpb.LoadBlobsRequest{
		Entries: []*dexpb.LoadBlobRequestEntry{{FlowId: flowID, BlobValue: value}},
	})
	if err != nil {
		return nil, err
	}
	hydrated, ok := result.GetValues()[blobID]
	if !ok {
		return nil, fmt.Errorf("value blob unavailable")
	}
	return hydrated, nil
}

func v2BlobID(value *dexpb.Value) string {
	if value == nil {
		return ""
	}
	switch kind := value.GetKind().(type) {
	case *dexpb.Value_InternalBlobIdForStringValue:
		return kind.InternalBlobIdForStringValue
	case *dexpb.Value_InternalBlobIdForObjValue:
		return kind.InternalBlobIdForObjValue
	default:
		return ""
	}
}

func v2DexValue(value *dexpb.Value) interface{} {
	object, ok := value.GetKind().(*dexpb.Value_ObjValue)
	if !ok || object.ObjValue.GetEncoding() != "json" {
		return dexValue(value)
	}
	decoder := json.NewDecoder(strings.NewReader(string(object.ObjValue.GetPayload())))
	decoder.UseNumber()
	var decoded interface{}
	if err := decoder.Decode(&decoded); err != nil {
		return dexValue(value)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return dexValue(value)
	}
	return decoded
}

func (h *v2Handler) requireActiveFlow(ctx context.Context, flowID string, flowType string) error {
	summary, err := h.client.GetFlowSummary(ctx, &dexpb.GetFlowSummaryRequest{FlowId: flowID})
	if err != nil {
		return err
	}
	if summary.GetFlowType() != flowType {
		return status.Error(codes.NotFound, "Flow does not match the requested flowType")
	}
	if summary.GetFlowStatus() != dexpb.FlowStatus_FLOW_STATUS_RUNNING {
		return status.Error(codes.FailedPrecondition, "Flow is not active")
	}
	return nil
}

func compileV2Query(
	flowType string,
	workQueuePermissions []string,
	filters []v2Filter,
	definition V2Definition,
) (string, error) {
	conditions := []string{"FlowType = " + quoteVisibilityString(flowType)}
	permissionCondition, err := compileWorkQueuePermissions(workQueuePermissions)
	if err != nil {
		return "", err
	}
	if permissionCondition != "" {
		conditions = append(conditions, permissionCondition)
	}
	for _, filter := range filters {
		if len(filter.Values) == 0 {
			return "", fmt.Errorf("filter %q must contain at least one value", filter.Field)
		}
		physicalKey, valueType, indexType, ok := v2FilterField(filter.Field, definition)
		if !ok {
			return "", fmt.Errorf("unknown v2 filter field %q", filter.Field)
		}
		if !isV2IndexKey(physicalKey) {
			return "", fmt.Errorf("filter %q maps to an invalid index key", filter.Field)
		}
		condition, err := compileV2Filter(physicalKey, valueType, indexType, filter)
		if err != nil {
			return "", err
		}
		conditions = append(conditions, condition)
	}
	return strings.Join(conditions, " AND "), nil
}

func compileWorkQueuePermissions(permissions []string) (string, error) {
	permissionSet := make(map[string]struct{}, len(permissions))
	for _, permission := range permissions {
		if !v2PermissionPattern.MatchString(permission) {
			return "", fmt.Errorf("invalid Work Queue permission %q", permission)
		}
		permissionSet[permission] = struct{}{}
	}
	if len(permissionSet) == 0 {
		return "", nil
	}
	orderedPermissions := make([]string, 0, len(permissionSet))
	for permission := range permissionSet {
		orderedPermissions = append(orderedPermissions, permission)
	}
	sort.Strings(orderedPermissions)
	fieldExpression := quoteVisibilityField(v2WorkQueuePermissionsIndex)
	parts := make([]string, 0, len(orderedPermissions))
	for _, permission := range orderedPermissions {
		parts = append(parts, fieldExpression+" = "+quoteVisibilityString(permission))
	}
	if len(parts) == 1 {
		return parts[0], nil
	}
	return "(" + strings.Join(parts, " OR ") + ")", nil
}

func v2FilterField(
	field string,
	definition V2Definition,
) (string, string, string, bool) {
	switch field {
	case "flowId":
		return "WorkflowId", "string", "keyword", true
	case "executionStatus":
		return "ExecutionStatus", "string", "keyword", true
	case "startTime":
		return "StartTime", "datetime", "datetime", true
	case "closeTime":
		return "CloseTime", "datetime", "datetime", true
	default:
		for _, attribute := range definition.IndexedAttributes {
			if attribute.AttributeKey == field {
				return attribute.IndexKey, attribute.ValueType, attribute.IndexType, true
			}
		}
		return "", "", "", false
	}
}

func compileV2Filter(
	physicalKey string,
	valueType string,
	indexType string,
	filter v2Filter,
) (string, error) {
	fieldExpression := quoteVisibilityField(physicalKey)
	operator := strings.ToLower(filter.Operator)
	if operator == "" {
		operator = "eq"
	}
	if operator == "in" || len(filter.Values) > 1 {
		if operator != "in" && operator != "eq" {
			return "", fmt.Errorf("filter %q does not support multiple values with %q", filter.Field, operator)
		}
		parts := make([]string, 0, len(filter.Values))
		for _, value := range filter.Values {
			literal, err := visibilityLiteral(value, valueType)
			if err != nil {
				return "", fmt.Errorf("filter %q: %w", filter.Field, err)
			}
			parts = append(parts, fieldExpression+" = "+literal)
		}
		return "(" + strings.Join(parts, " OR ") + ")", nil
	}
	literal, err := visibilityLiteral(filter.Values[0], valueType)
	if err != nil {
		return "", fmt.Errorf("filter %q: %w", filter.Field, err)
	}
	switch operator {
	case "eq":
		return fieldExpression + " = " + literal, nil
	case "gt", "gte", "lt", "lte":
		if indexType != "int" && indexType != "double" && indexType != "datetime" {
			return "", fmt.Errorf("filter %q does not support ordered comparison", filter.Field)
		}
		operators := map[string]string{"gt": ">", "gte": ">=", "lt": "<", "lte": "<="}
		return fieldExpression + " " + operators[operator] + " " + literal, nil
	case "contains":
		if indexType != "fulltext" {
			return "", fmt.Errorf("contains requires a fulltext index")
		}
		value, ok := filter.Values[0].(string)
		if !ok {
			return "", fmt.Errorf("contains requires a string value")
		}
		return fieldExpression + " = " + quoteVisibilityString(value), nil
	default:
		return "", fmt.Errorf("unsupported filter operator %q", filter.Operator)
	}
}

func visibilityLiteral(value interface{}, valueType string) (string, error) {
	switch valueType {
	case "string", "datetime":
		text, ok := value.(string)
		if !ok {
			return "", fmt.Errorf("value must be a string")
		}
		if valueType == "datetime" {
			if _, err := time.Parse(time.RFC3339Nano, text); err != nil {
				return "", fmt.Errorf("value must be RFC3339 datetime")
			}
		}
		return quoteVisibilityString(text), nil
	case "int64":
		integer, ok := v2RequestInt64(value)
		if !ok {
			return "", fmt.Errorf("value must be an int64")
		}
		return strconv.FormatInt(integer, 10), nil
	case "double":
		number, ok := jsonNumberFloat64(value)
		if !ok {
			return "", fmt.Errorf("value must be a finite number")
		}
		return strconv.FormatFloat(number, 'g', -1, 64), nil
	case "bool":
		boolean, ok := value.(bool)
		if !ok {
			return "", fmt.Errorf("value must be a boolean")
		}
		return strconv.FormatBool(boolean), nil
	case "string-array":
		text, ok := value.(string)
		if !ok {
			return "", fmt.Errorf("value must be a string")
		}
		return quoteVisibilityString(text), nil
	default:
		return "", fmt.Errorf("unsupported value type %q", valueType)
	}
}

func quoteVisibilityString(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

func quoteVisibilityField(value string) string {
	for _, character := range value {
		if character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' ||
			character >= '0' && character <= '9' {
			continue
		}
		return "`" + value + "`"
	}
	return value
}

func isV2IndexKey(value string) bool {
	if value == "" {
		return false
	}
	for index, character := range value {
		isLetter := character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z'
		isLaterCharacter := index > 0 && (character >= '0' && character <= '9' || character == '-' || character == '.')
		if isLetter || character == '_' || isLaterCharacter {
			continue
		}
		return false
	}
	return true
}

func flowRunStartTime(entry *dexpb.SearchFlowsResponseEntry) time.Time {
	if entry == nil || entry.GetStartTime() == nil {
		return time.Time{}
	}
	return entry.GetStartTime().AsTime()
}

func validateViewOutput(view V2RPCView, values map[string]interface{}) error {
	if len(values) != len(view.Fields) {
		return fmt.Errorf("%s returned %d fields; contract declares %d", view.RPCName, len(values), len(view.Fields))
	}
	for _, field := range view.Fields {
		value, exists := values[field.AttributeKey]
		if !exists {
			return fmt.Errorf("%s omitted declared field %q", view.RPCName, field.AttributeKey)
		}
		if value != nil && !isV2ValueType(value, field.ValueType) {
			return fmt.Errorf("%s field %q does not match %s", view.RPCName, field.AttributeKey, field.ValueType)
		}
	}
	return nil
}

func v2ResponseValue(value interface{}, valueType string) interface{} {
	if value == nil || valueType != "int64" {
		return value
	}
	integer, ok := jsonNumberInt64(value)
	if !ok {
		return value
	}
	return strconv.FormatInt(integer, 10)
}

func v2ResponseSnapshot(snapshot map[string]interface{}) map[string]interface{} {
	response := make(map[string]interface{}, len(snapshot))
	for key, value := range snapshot {
		if integer, ok := value.(int64); ok {
			response[key] = strconv.FormatInt(integer, 10)
			continue
		}
		response[key] = value
	}
	return response
}

func isV2ValueType(value interface{}, valueType string) bool {
	switch valueType {
	case "string", "datetime":
		text, ok := value.(string)
		if !ok {
			return false
		}
		if valueType == "datetime" {
			_, err := time.Parse(time.RFC3339Nano, text)
			return err == nil
		}
		return true
	case "int64":
		_, ok := jsonNumberInt64(value)
		return ok
	case "double":
		_, ok := jsonNumberFloat64(value)
		return ok
	case "bool":
		_, ok := value.(bool)
		return ok
	case "string-array":
		values, ok := value.([]interface{})
		if !ok {
			return false
		}
		for _, item := range values {
			if _, ok := item.(string); !ok {
				return false
			}
		}
		return true
	case "object", "attribute-map":
		_, ok := value.(map[string]interface{})
		return ok
	case "array":
		_, ok := value.([]interface{})
		return ok
	case "json":
		return true
	default:
		return false
	}
}

func encodeV2Value(value interface{}, valueType string) (*dexpb.Value, error) {
	if valueType == "int64" {
		integer, ok := v2RequestInt64(value)
		if !ok {
			return nil, fmt.Errorf("value does not match %s", valueType)
		}
		return &dexpb.Value{Kind: &dexpb.Value_IntValue{IntValue: integer}}, nil
	}
	if !isV2ValueType(value, valueType) {
		return nil, fmt.Errorf("value does not match %s", valueType)
	}
	switch valueType {
	case "string", "datetime":
		return &dexpb.Value{Kind: &dexpb.Value_StringValue{StringValue: value.(string)}}, nil
	case "double":
		number, _ := jsonNumberFloat64(value)
		return &dexpb.Value{Kind: &dexpb.Value_DoubleValue{DoubleValue: number}}, nil
	case "bool":
		return &dexpb.Value{Kind: &dexpb.Value_BoolValue{BoolValue: value.(bool)}}, nil
	default:
		payload, err := json.Marshal(value)
		if err != nil {
			return nil, err
		}
		return &dexpb.Value{Kind: &dexpb.Value_ObjValue{ObjValue: &dexpb.EncodedObject{
			Encoding: "json", Payload: payload,
		}}}, nil
	}
}

func buildActionInput(
	action V2Action,
	userInput map[string]interface{},
	attributeSnapshot map[string]interface{},
) (*dexpb.Value, error) {
	if action.Input.Kind == "none" {
		if len(userInput) != 0 || len(action.Input.Fields) != 0 {
			return nil, fmt.Errorf("no-input Action must not receive input")
		}
		return &dexpb.Value{Kind: &dexpb.Value_NullValue{NullValue: structpb.NullValue_NULL_VALUE}}, nil
	}
	input := make(map[string]interface{}, len(action.Input.Fields))
	userFields := make(map[string]struct{}, len(action.Input.Fields))
	for _, field := range action.Input.Fields {
		var value interface{}
		var exists bool
		if field.Source == "attribute" {
			value, exists = attributeSnapshot[field.AttributeKey]
		} else {
			userFields[field.FieldName] = struct{}{}
			value, exists = userInput[field.FieldName]
		}
		if !exists || value == nil {
			if field.Required {
				return nil, fmt.Errorf("Action input %q is required", field.FieldName)
			}
			input[field.FieldName] = nil
			continue
		}
		if field.ValueType == "int64" {
			integer, validInteger := v2RequestInt64(value)
			if !validInteger {
				return nil, fmt.Errorf("Action input %q does not match %s", field.FieldName, field.ValueType)
			}
			input[field.FieldName] = integer
			continue
		}
		if !isV2ValueType(value, field.ValueType) {
			return nil, fmt.Errorf("Action input %q does not match %s", field.FieldName, field.ValueType)
		}
		input[field.FieldName] = value
	}
	for fieldName := range userInput {
		if _, ok := userFields[fieldName]; !ok {
			return nil, fmt.Errorf("unknown Action input %q", fieldName)
		}
	}
	payload, err := json.Marshal(input)
	if err != nil {
		return nil, err
	}
	return &dexpb.Value{Kind: &dexpb.Value_ObjValue{ObjValue: &dexpb.EncodedObject{
		Encoding: "json", Payload: payload,
	}}}, nil
}

func actionConditionMatches(condition V2ActionCondition, currentValue interface{}) bool {
	if condition.Operator != "in" {
		return false
	}
	currentJSON, err := json.Marshal(currentValue)
	if err != nil {
		return false
	}
	for _, expected := range condition.Values {
		expectedJSON, marshalErr := json.Marshal(expected)
		if marshalErr == nil && string(currentJSON) == string(expectedJSON) {
			return true
		}
	}
	return false
}

func v2SnapshotKeys(definition V2Definition) []string {
	seen := make(map[string]struct{})
	keys := make([]string, 0)
	for _, action := range definition.Actions {
		for _, key := range []string{action.Condition.AttributeKey} {
			if _, exists := seen[key]; !exists {
				seen[key] = struct{}{}
				keys = append(keys, key)
			}
		}
		for _, field := range action.Input.Fields {
			if field.Source == "attribute" {
				if _, exists := seen[field.AttributeKey]; !exists {
					seen[field.AttributeKey] = struct{}{}
					keys = append(keys, field.AttributeKey)
				}
			}
		}
	}
	return keys
}

func keyValueMap(values []*dexpb.KV) map[string]interface{} {
	mapped := make(map[string]interface{}, len(values))
	for _, value := range values {
		mapped[value.GetKey()] = dexValue(value.GetValue())
	}
	return mapped
}

func editableDisplayField(fields []V2ViewField, attributeKey string) (V2ViewField, bool) {
	for _, field := range fields {
		if field.AttributeKey == attributeKey && field.Editable {
			return field, true
		}
	}
	return V2ViewField{}, false
}

func findIndexedAttribute(
	definition V2Definition,
	attributeKey string,
) (V2IndexedAttribute, bool) {
	for _, attribute := range definition.IndexedAttributes {
		if attribute.AttributeKey == attributeKey {
			return attribute, true
		}
	}
	return V2IndexedAttribute{}, false
}

func findV2Action(actions []V2Action, rpcName string) (V2Action, bool) {
	for _, action := range actions {
		if action.RPCName == rpcName {
			return action, true
		}
	}
	return V2Action{}, false
}

func v2IndexType(indexType string) (dexpb.IndexType, error) {
	switch indexType {
	case "keyword":
		return dexpb.IndexType_INDEX_TYPE_KEYWORD, nil
	case "fulltext":
		return dexpb.IndexType_INDEX_TYPE_TEXT, nil
	case "keyword-array":
		return dexpb.IndexType_INDEX_TYPE_KEYWORD_ARRAY, nil
	case "int":
		return dexpb.IndexType_INDEX_TYPE_INT, nil
	case "double":
		return dexpb.IndexType_INDEX_TYPE_DOUBLE, nil
	case "bool":
		return dexpb.IndexType_INDEX_TYPE_BOOL, nil
	case "datetime":
		return dexpb.IndexType_INDEX_TYPE_DATETIME, nil
	default:
		return dexpb.IndexType_INDEX_TYPE_UNSPECIFIED, fmt.Errorf("unsupported index type %q", indexType)
	}
}

func jsonNumberInt64(value interface{}) (int64, bool) {
	switch number := value.(type) {
	case int:
		return int64(number), true
	case int32:
		return int64(number), true
	case int64:
		return number, true
	case json.Number:
		integer, err := number.Int64()
		return integer, err == nil
	case float64:
		if math.IsNaN(number) || math.IsInf(number, 0) || math.Trunc(number) != number ||
			number < math.MinInt64 || number > math.MaxInt64 {
			return 0, false
		}
		return int64(number), true
	default:
		return 0, false
	}
}

func v2RequestInt64(value interface{}) (int64, bool) {
	if text, ok := value.(string); ok {
		integer, err := strconv.ParseInt(text, 10, 64)
		return integer, err == nil
	}
	return jsonNumberInt64(value)
}

func jsonNumberFloat64(value interface{}) (float64, bool) {
	var number float64
	switch typed := value.(type) {
	case int:
		number = float64(typed)
	case int32:
		number = float64(typed)
	case int64:
		number = float64(typed)
	case float32:
		number = float64(typed)
	case float64:
		number = typed
	case json.Number:
		parsed, err := typed.Float64()
		if err != nil {
			return 0, false
		}
		number = parsed
	default:
		return 0, false
	}
	if math.IsNaN(number) || math.IsInf(number, 0) {
		return 0, false
	}
	return number, true
}
