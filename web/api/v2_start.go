// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/big"
	"net"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/superdurable/dex/gen/dexpb"
	"github.com/superdurable/dex/service"
	"github.com/superdurable/dex/service/common/grpctarget"
	"google.golang.org/protobuf/types/known/structpb"
)

const (
	maximumV2StartInputDepth      = 32
	v2WorkerHealthCheckTimeout    = 2 * time.Second
	v2WorkerUnhealthyResponseCode = "WORKER_UNHEALTHY"
)

var integerJSONPattern = regexp.MustCompile(`^-?(?:0|[1-9][0-9]*)$`)

type v2StartRequest struct {
	FlowType                string          `json:"flowType"`
	FlowID                  string          `json:"flowId"`
	WorkerTargetAddress     *string         `json:"workerTargetAddress"`
	WorkerTargetPull        *bool           `json:"workerTargetPull"`
	BypassWorkerHealthCheck bool            `json:"bypassWorkerHealthCheck"`
	Input                   json.RawMessage `json:"input"`
}

type v2StartResponse struct {
	FlowID string `json:"flowId"`
	RunID  string `json:"runId"`
}

type v2WorkerHealthRequest struct {
	WorkerTargetAddress string `json:"workerTargetAddress"`
}

type v2WorkerHealthResponse struct {
	Healthy bool   `json:"healthy"`
	Warning string `json:"warning,omitempty"`
}

func (h *v2Handler) startFlow(response http.ResponseWriter, request *http.Request) {
	var body v2StartRequest
	if err := decodeJSON(response, request, &body); err != nil {
		WriteError(response, http.StatusBadRequest, err.Error(), nil)
		return
	}
	snapshot, ok := h.loadSnapshot(response, request, true)
	if !ok {
		return
	}
	definition, exists := snapshot.Definitions[body.FlowType]
	if !exists || definition.Start == nil {
		WriteError(response, http.StatusBadRequest, "Flow type does not support starting from Dex Web", nil)
		return
	}
	if body.FlowID == "" {
		WriteError(response, http.StatusBadRequest, "flow ID is required", nil)
		return
	}
	if err := service.ValidateFlowID(body.FlowID); err != nil {
		WriteError(response, http.StatusBadRequest, err.Error(), nil)
		return
	}
	workerTarget, err := h.validateStartWorkerTarget(body.WorkerTargetAddress, body.WorkerTargetPull)
	if err != nil {
		WriteError(response, http.StatusBadRequest, err.Error(), nil)
		return
	}
	stepInput, err := v2StartStepInput(definition.Start.Input, body.Input)
	if err != nil {
		WriteError(response, http.StatusBadRequest, err.Error(), nil)
		return
	}
	if !workerTarget.GetIsPullMode() && !body.BypassWorkerHealthCheck {
		if err := h.checkWorkerTargetHealth(request.Context(), workerTarget.GetAddress()); err != nil {
			warning := workerHealthWarning(workerTarget.GetAddress(), err)
			WriteCodedError(response, http.StatusPreconditionFailed, v2WorkerUnhealthyResponseCode, warning)
			return
		}
	}
	requestID := uuid.NewString()
	result, err := h.client.StartFlow(request.Context(), &dexpb.StartFlowRequest{
		FlowId: body.FlowID, FlowType: body.FlowType, StartStepType: definition.Start.StepType,
		StepInput: stepInput, RequestId: requestID,
		StepOptions: &dexpb.StepOptions{SkipWaitFor: definition.Start.SkipWaitFor},
		FlowStartOptions: &dexpb.FlowStartOptions{
			FlowConfigOverride: &dexpb.FlowConfig{WorkerTarget: workerTarget},
		},
	})
	if err != nil {
		writeGRPCError(response, err, "StartFlow")
		return
	}
	accepted := v2StartResponse{FlowID: body.FlowID}
	accepted.RunID = result.GetRunId()
	writeJSON(response, http.StatusOK, accepted)
}

func (h *v2Handler) checkWorkerHealth(response http.ResponseWriter, request *http.Request) {
	var body v2WorkerHealthRequest
	if err := decodeJSON(response, request, &body); err != nil {
		WriteError(response, http.StatusBadRequest, err.Error(), nil)
		return
	}
	workerTarget, err := h.validateWorkerTarget(body.WorkerTargetAddress)
	if err != nil {
		WriteError(response, http.StatusBadRequest, err.Error(), nil)
		return
	}
	if err := h.checkWorkerTargetHealth(request.Context(), workerTarget.GetAddress()); err != nil {
		writeJSON(response, http.StatusOK, v2WorkerHealthResponse{
			Healthy: false,
			Warning: workerHealthWarning(workerTarget.GetAddress(), err),
		})
		return
	}
	writeJSON(response, http.StatusOK, v2WorkerHealthResponse{Healthy: true})
}

func (h *v2Handler) validateStartWorkerTarget(address *string, isPull *bool) (*dexpb.WorkerTarget, error) {
	isPullTarget := isPull != nil && *isPull
	if isPullTarget && address != nil {
		return nil, fmt.Errorf("workerTargetPull and workerTargetAddress cannot both be set")
	}
	if isPullTarget || (address == nil && isPull == nil && h.isStartFlowWorkerTargetPull) {
		return grpctarget.NormalizeWorkerTarget(&dexpb.WorkerTarget{IsPullMode: true})
	}
	if address == nil {
		return nil, fmt.Errorf("workerTargetAddress or workerTargetPull is required")
	}
	return h.validateWorkerTarget(*address)
}

func (h *v2Handler) validateWorkerTarget(address string) (*dexpb.WorkerTarget, error) {
	workerTarget, err := grpctarget.NormalizeWorkerTarget(&dexpb.WorkerTarget{
		Address: address, IsHeadlessAddress: h.isStartFlowWorkerTargetHeadless,
	})
	if err != nil {
		return nil, err
	}
	if _, _, err := net.SplitHostPort(workerTarget.GetAddress()); err != nil {
		return nil, fmt.Errorf("worker_target %q must use host:port: %w", workerTarget.GetAddress(), err)
	}
	return workerTarget, nil
}

func (h *v2Handler) checkWorkerTargetHealth(ctx context.Context, address string) error {
	checkContext, cancel := context.WithTimeout(ctx, v2WorkerHealthCheckTimeout)
	defer cancel()
	return h.checkStartFlowWorkerHealth(checkContext, address)
}

func checkV2WorkerPortHealth(ctx context.Context, address string) error {
	connection, err := (&net.Dialer{}).DialContext(ctx, "tcp", address)
	if err != nil {
		return err
	}
	return connection.Close()
}

func workerHealthWarning(address string, err error) string {
	return fmt.Sprintf("Dex Web cannot reach Worker %s: %v", address, err)
}

// ValidateV2StartDefinition validates a recursive Start input contract.
func ValidateV2StartDefinition(definition V2StartDefinition) error {
	if strings.TrimSpace(definition.StepType) == "" {
		return fmt.Errorf("Start Step type must be non-empty")
	}
	return validateV2StartInputSchema(definition.Input, 0)
}

func validateV2StartInputSchema(schema V2StartInputSchema, depth int) error {
	if depth > maximumV2StartInputDepth {
		return fmt.Errorf("Start input nesting exceeds %d levels", maximumV2StartInputDepth)
	}
	switch schema.Kind {
	case "null":
		if schema.Nullable {
			return fmt.Errorf("null schema must not declare nullable")
		}
	case "string":
		if schema.Format != "" && schema.Format != "date-time" {
			return fmt.Errorf("string schema has invalid format %q", schema.Format)
		}
		if err := validateV2StartInputEnum(schema.EnumValues, "string", nil, nil); err != nil {
			return err
		}
	case "integer":
		minimum, minimumOK := new(big.Int).SetString(schema.Minimum, 10)
		maximum, maximumOK := new(big.Int).SetString(schema.Maximum, 10)
		if !minimumOK || !maximumOK || minimum.Cmp(maximum) > 0 {
			return fmt.Errorf("integer schema has invalid range")
		}
		if err := validateV2StartInputEnum(schema.EnumValues, "integer", minimum, maximum); err != nil {
			return err
		}
	case "number", "boolean":
		if len(schema.EnumValues) != 0 {
			return fmt.Errorf("%s schema must not declare enum values", schema.Kind)
		}
	case "object":
		fieldNames := make(map[string]bool, len(schema.Fields))
		for _, field := range schema.Fields {
			if field.Name == "" || fieldNames[field.Name] {
				return fmt.Errorf("object schema has an empty or repeated field name")
			}
			fieldNames[field.Name] = true
			if err := validateV2StartInputSchema(field.Schema, depth+1); err != nil {
				return fmt.Errorf("field %q: %w", field.Name, err)
			}
		}
	case "array":
		if schema.Items == nil {
			return fmt.Errorf("array schema requires items")
		}
		if schema.FixedLength != nil && *schema.FixedLength < 0 {
			return fmt.Errorf("array fixed length must be non-negative")
		}
		if err := validateV2StartInputSchema(*schema.Items, depth+1); err != nil {
			return fmt.Errorf("array items: %w", err)
		}
	case "map":
		if schema.Values == nil {
			return fmt.Errorf("map schema requires values")
		}
		if err := validateV2StartInputSchema(*schema.Values, depth+1); err != nil {
			return fmt.Errorf("map values: %w", err)
		}
	default:
		return fmt.Errorf("Start input has unknown kind %q", schema.Kind)
	}
	return nil
}

func validateV2StartInputEnum(values []V2StartInputEnumValue, kind string, minimum *big.Int, maximum *big.Int) error {
	seenNames := make(map[string]bool, len(values))
	seenValues := make(map[string]bool, len(values))
	for _, option := range values {
		if option.Name == "" || seenNames[option.Name] {
			return fmt.Errorf("%s enum has an empty or repeated name", kind)
		}
		var valueKey string
		switch kind {
		case "string":
			value, ok := option.Value.(string)
			if !ok {
				return fmt.Errorf("string enum value %q must be a string", option.Name)
			}
			valueKey = value
		case "integer":
			number, ok := option.Value.(string)
			if !ok || !integerJSONPattern.MatchString(number) {
				return fmt.Errorf("integer enum value %q must be an integer", option.Name)
			}
			value, parsed := new(big.Int).SetString(number, 10)
			if !parsed || value.Cmp(minimum) < 0 || value.Cmp(maximum) > 0 {
				return fmt.Errorf("integer enum value %q is outside its range", option.Name)
			}
			valueKey = value.String()
		}
		if seenValues[valueKey] {
			return fmt.Errorf("%s enum repeats value %q", kind, valueKey)
		}
		seenNames[option.Name] = true
		seenValues[valueKey] = true
	}
	return nil
}

func v2StartStepInput(schema V2StartInputSchema, rawInput json.RawMessage) (*dexpb.Value, error) {
	if len(rawInput) == 0 {
		return nil, fmt.Errorf("input is required")
	}
	decoder := json.NewDecoder(bytes.NewReader(rawInput))
	decoder.UseNumber()
	var value interface{}
	if err := decoder.Decode(&value); err != nil {
		return nil, fmt.Errorf("input is invalid JSON: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return nil, fmt.Errorf("input contains trailing data")
	}
	if err := validateV2StartInputValue(schema, value, "$", 0); err != nil {
		return nil, err
	}
	switch schema.Kind {
	case "null":
		return &dexpb.Value{Kind: &dexpb.Value_NullValue{NullValue: structpb.NullValue_NULL_VALUE}}, nil
	case "string":
		if value == nil {
			return &dexpb.Value{Kind: &dexpb.Value_NullValue{NullValue: structpb.NullValue_NULL_VALUE}}, nil
		}
		return &dexpb.Value{Kind: &dexpb.Value_StringValue{StringValue: value.(string)}}, nil
	case "integer":
		if value == nil {
			return &dexpb.Value{Kind: &dexpb.Value_NullValue{NullValue: structpb.NullValue_NULL_VALUE}}, nil
		}
		integerValue, err := strconv.ParseInt(string(value.(json.Number)), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("$ must fit the Dex int64 value type")
		}
		return &dexpb.Value{Kind: &dexpb.Value_IntValue{IntValue: integerValue}}, nil
	case "number":
		if value == nil {
			return &dexpb.Value{Kind: &dexpb.Value_NullValue{NullValue: structpb.NullValue_NULL_VALUE}}, nil
		}
		numberValue, err := strconv.ParseFloat(string(value.(json.Number)), 64)
		if err != nil || math.IsInf(numberValue, 0) || math.IsNaN(numberValue) {
			return nil, fmt.Errorf("$ must fit the Dex double value type")
		}
		return &dexpb.Value{Kind: &dexpb.Value_DoubleValue{DoubleValue: numberValue}}, nil
	case "boolean":
		if value == nil {
			return &dexpb.Value{Kind: &dexpb.Value_NullValue{NullValue: structpb.NullValue_NULL_VALUE}}, nil
		}
		return &dexpb.Value{Kind: &dexpb.Value_BoolValue{BoolValue: value.(bool)}}, nil
	default:
		return &dexpb.Value{Kind: &dexpb.Value_ObjValue{ObjValue: &dexpb.EncodedObject{
			Encoding: "json", Payload: bytes.TrimSpace(rawInput),
		}}}, nil
	}
}

func validateV2StartInputValue(schema V2StartInputSchema, value interface{}, path string, depth int) error {
	if depth > maximumV2StartInputDepth {
		return fmt.Errorf("%s exceeds the maximum nesting depth", path)
	}
	if value == nil {
		if schema.Kind == "null" || schema.Nullable {
			return nil
		}
		return fmt.Errorf("%s must not be null", path)
	}
	switch schema.Kind {
	case "null":
		return fmt.Errorf("%s must be null", path)
	case "string":
		text, ok := value.(string)
		if !ok {
			return fmt.Errorf("%s must be a string", path)
		}
		if schema.Format == "date-time" {
			if _, err := time.Parse(time.RFC3339, text); err != nil {
				return fmt.Errorf("%s must be an RFC3339 datetime", path)
			}
		}
		if !matchesV2StartStringEnum(text, schema.EnumValues) {
			return fmt.Errorf("%s must be one of the declared enum values", path)
		}
	case "integer":
		number, ok := value.(json.Number)
		if !ok || !integerJSONPattern.MatchString(string(number)) {
			return fmt.Errorf("%s must be an integer", path)
		}
		integerValue, parsed := new(big.Int).SetString(string(number), 10)
		minimum, minimumOK := new(big.Int).SetString(schema.Minimum, 10)
		maximum, maximumOK := new(big.Int).SetString(schema.Maximum, 10)
		if !parsed || !minimumOK || !maximumOK || integerValue.Cmp(minimum) < 0 || integerValue.Cmp(maximum) > 0 {
			return fmt.Errorf("%s is outside the allowed integer range", path)
		}
		if !matchesV2StartIntegerEnum(integerValue, schema.EnumValues) {
			return fmt.Errorf("%s must be one of the declared enum values", path)
		}
	case "number":
		number, ok := value.(json.Number)
		if !ok {
			return fmt.Errorf("%s must be a number", path)
		}
		parsed, err := strconv.ParseFloat(string(number), 64)
		if err != nil || math.IsInf(parsed, 0) || math.IsNaN(parsed) {
			return fmt.Errorf("%s must be a finite number", path)
		}
	case "boolean":
		if _, ok := value.(bool); !ok {
			return fmt.Errorf("%s must be a boolean", path)
		}
	case "object":
		object, ok := value.(map[string]interface{})
		if !ok {
			return fmt.Errorf("%s must be an object", path)
		}
		fields := make(map[string]V2StartInputField, len(schema.Fields))
		for _, field := range schema.Fields {
			fields[field.Name] = field
			fieldValue, exists := object[field.Name]
			if field.Required && !exists {
				return fmt.Errorf("%s.%s is required", path, field.Name)
			}
			if exists {
				if err := validateV2StartInputValue(field.Schema, fieldValue, path+"."+field.Name, depth+1); err != nil {
					return err
				}
			}
		}
		for fieldName := range object {
			if _, exists := fields[fieldName]; !exists {
				return fmt.Errorf("%s.%s is unknown", path, fieldName)
			}
		}
	case "array":
		items, ok := value.([]interface{})
		if !ok {
			return fmt.Errorf("%s must be an array", path)
		}
		if schema.FixedLength != nil && int64(len(items)) != *schema.FixedLength {
			return fmt.Errorf("%s must contain exactly %d items", path, *schema.FixedLength)
		}
		for index, item := range items {
			if err := validateV2StartInputValue(*schema.Items, item, fmt.Sprintf("%s[%d]", path, index), depth+1); err != nil {
				return err
			}
		}
	case "map":
		entries, ok := value.(map[string]interface{})
		if !ok {
			return fmt.Errorf("%s must be an object map", path)
		}
		for key, entry := range entries {
			if key == "" {
				return fmt.Errorf("%s map keys must not be empty", path)
			}
			if err := validateV2StartInputValue(*schema.Values, entry, path+"["+strconv.Quote(key)+"]", depth+1); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("%s uses unsupported schema kind %q", path, schema.Kind)
	}
	return nil
}

func matchesV2StartStringEnum(value string, options []V2StartInputEnumValue) bool {
	if len(options) == 0 {
		return true
	}
	for _, option := range options {
		if option.Value == value {
			return true
		}
	}
	return false
}

func matchesV2StartIntegerEnum(value *big.Int, options []V2StartInputEnumValue) bool {
	if len(options) == 0 {
		return true
	}
	for _, option := range options {
		number, ok := option.Value.(string)
		if !ok {
			continue
		}
		optionValue, parsed := new(big.Int).SetString(number, 10)
		if parsed && value.Cmp(optionValue) == 0 {
			return true
		}
	}
	return false
}
