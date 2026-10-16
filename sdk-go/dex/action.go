// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package dex

import (
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strings"

	"github.com/superdurable/dex/sdk-go/gen/dexpb"
)

// WorkQueuePermissionsIndexKey is the Temporal KeywordList Search Attribute
// containing permissions for Actions that have been available in the current
// Flow execution.
//
// The index is a Work Queue discovery projection, not an authorization result.
// An Action gateway must still authorize the selected RPC against its required
// permission.
const WorkQueuePermissionsIndexKey = "DexWorkQueuePermissions"

var actionPermissionPattern = regexp.MustCompile(
	`^[a-z][a-z0-9]*(?:[.-][a-z0-9]+)*$`,
)

var workQueuePermissionsAttributeIndex = &AttributeIndex{
	Type:     IndexKeywordArray,
	IndexKey: WorkQueuePermissionsIndexKey,
}

// ActionDef is the schema-erased registration definition for an Action RPC.
//
// Applications create an Action with [DefineAction] and assign it to
// [RPCOptions.Action]. This interface is sealed so only SDK definitions can be
// registered.
type ActionDef interface {
	actionDefinition() *actionImpl
}

// ActionConditionDef is the schema-erased availability condition for an Action.
//
// Applications create conditions with [WhenAttributeMatches]. This interface is
// sealed so only SDK conditions can be registered.
type ActionConditionDef interface {
	compileActionCondition() (*compiledActionCondition, error)
}

// ActionOption configures an Action definition.
//
// Use [ActionRequiresPermission] to declare the permission required by the
// Action. Custom implementations are not supported.
type ActionOption interface {
	applyAction(*actionImpl)
}

type actionImpl struct {
	label                   string
	condition               ActionConditionDef
	requiredPermission      string
	requiredPermissionCount int
}

// DefineAction creates the immutable contract for one Action RPC.
//
// label is the user-facing Action label. condition determines whether the
// Action is available from the post-invocation Attribute state. Exactly one
// ActionRequiresPermission option is required.
//
// Example:
//
//	action := dex.DefineAction(
//		"Approve",
//		dex.WhenAttributeMatches(
//			caseStatus,
//			dex.AttributeMatchEqual("awaiting-manager"),
//		),
//		dex.ActionRequiresPermission("refund.approve"),
//	)
func DefineAction(
	label string,
	condition ActionConditionDef,
	options ...ActionOption,
) ActionDef {
	action := &actionImpl{label: label, condition: condition}
	for _, option := range options {
		if option != nil {
			option.applyAction(action)
		}
	}
	return action
}

func (action *actionImpl) actionDefinition() *actionImpl {
	return action
}

type actionRequiredPermissionOption struct {
	permission string
}

// ActionRequiresPermission declares the single permission required by an Action.
//
// The permission uses lowercase letter and number segments separated by dots or
// hyphens, such as "refund.approve" or "permission-x". A Flow may register
// multiple Actions with the same permission; the Work Queue projection removes
// duplicates.
func ActionRequiresPermission(permission string) ActionOption {
	return actionRequiredPermissionOption{permission: permission}
}

func (option actionRequiredPermissionOption) applyAction(action *actionImpl) {
	action.requiredPermission = option.permission
	action.requiredPermissionCount++
}

type attributeActionCondition[T any] struct {
	attribute Attribute[T]
	matches   []AttributeMatch[T]
}

// WhenAttributeMatches creates an Action condition for one scalar Attribute.
//
// At least one match is required. Every match must be created with
// [AttributeMatchEqual]. Multiple matches use OR semantics. A missing or deleted
// Attribute does not match. Supported Attribute types are strings, booleans,
// integers, unsigned integers that fit int64, and finite floating-point values.
//
// The Attribute must be registered in the same Flow as the Action RPC.
func WhenAttributeMatches[T any](
	attribute Attribute[T],
	matches ...AttributeMatch[T],
) ActionConditionDef {
	return attributeActionCondition[T]{attribute: attribute, matches: matches}
}

func (condition attributeActionCondition[T]) compileActionCondition() (
	*compiledActionCondition,
	error,
) {
	valueType := reflect.TypeFor[T]()
	if !supportsActionAttributeType(valueType) {
		return nil, fmt.Errorf(
			"dex: Action condition Attribute %q has unsupported type %s",
			condition.attribute.name,
			valueType,
		)
	}
	if len(condition.matches) == 0 {
		return nil, fmt.Errorf(
			"dex: Action condition Attribute %q requires at least one match",
			condition.attribute.name,
		)
	}
	operands := make([]*dexpb.Value, 0, len(condition.matches))
	for _, match := range condition.matches {
		if match.attributeMatchOperator() !=
			dexpb.AttributeMatchOperator_ATTRIBUTE_MATCH_OPERATOR_EQUAL {
			return nil, fmt.Errorf(
				"dex: Action condition Attribute %q supports only AttributeMatchEqual",
				condition.attribute.name,
			)
		}
		operand, err := encodeValue(match.attributeMatchOperand())
		if err != nil {
			return nil, fmt.Errorf(
				"dex: Action condition Attribute %q: %w",
				condition.attribute.name,
				err,
			)
		}
		if err := validateEncodedAttributeMatch(
			match.attributeMatchOperator(),
			operand,
		); err != nil {
			return nil, fmt.Errorf(
				"dex: Action condition Attribute %q: %w",
				condition.attribute.name,
				err,
			)
		}
		operands = append(operands, operand)
	}
	return &compiledActionCondition{
		attribute: condition.attribute,
		valueType: valueType,
		operands:  operands,
	}, nil
}

type registeredAction struct {
	label              string
	requiredPermission string
	condition          *compiledActionCondition
}

type compiledActionCondition struct {
	attribute AttributeDef
	valueType reflect.Type
	operands  []*dexpb.Value
}

func (condition *compiledActionCondition) matches(value *dexpb.Value) bool {
	if value == nil {
		return false
	}
	if _, deleted := value.Kind.(*dexpb.Value_NullValue); deleted {
		return false
	}
	for _, operand := range condition.operands {
		if equalActionScalarValues(value, operand) {
			return true
		}
	}
	return false
}

func (flow *registeredFlow) compileAction(definition ActionDef) (*registeredAction, error) {
	if nilInterface(definition) {
		return nil, fmt.Errorf("Action definition is nil")
	}
	action := definition.actionDefinition()
	if action == nil {
		return nil, fmt.Errorf("Action definition is nil")
	}
	if strings.TrimSpace(action.label) == "" {
		return nil, fmt.Errorf("Action label must not be empty")
	}
	if action.requiredPermissionCount != 1 {
		return nil, fmt.Errorf("Action requires exactly one permission")
	}
	if !actionPermissionPattern.MatchString(action.requiredPermission) {
		return nil, fmt.Errorf(
			"Action permission %q must use lowercase letter and number segments separated by dots or hyphens",
			action.requiredPermission,
		)
	}
	if nilInterface(action.condition) {
		return nil, fmt.Errorf("Action condition is nil")
	}
	condition, err := action.condition.compileActionCondition()
	if err != nil {
		return nil, err
	}
	registered, found := flow.attributes[condition.attribute.attributeName()]
	if !found || registered.isMap {
		return nil, fmt.Errorf(
			"Action condition Attribute %q is not declared by Flow %q",
			condition.attribute.attributeName(),
			flow.flowType,
		)
	}
	if registered.isHardDeleteOnNull {
		return nil, fmt.Errorf(
			"Action condition Attribute %q uses HardDeleteOnNull, which conditions cannot read",
			condition.attribute.attributeName(),
		)
	}
	if registered.valueType != condition.valueType ||
		!reflect.DeepEqual(registered.index, condition.attribute.attributeIndex()) ||
		registered.syncToAttributeStore != condition.attribute.attributeSyncToAttributeStore() ||
		registered.isHardDeleteOnNull != condition.attribute.attributeIsHardDeleteOnNull() {
		return nil, fmt.Errorf(
			"Action condition Attribute %q does not match its registered definition",
			condition.attribute.attributeName(),
		)
	}
	return &registeredAction{
		label:              action.label,
		requiredPermission: action.requiredPermission,
		condition:          condition,
	}, nil
}

func (flow *registeredFlow) projectWorkQueuePermissions(
	currentAttributes map[string]*dexpb.Value,
	writes []*dexpb.AttributeWrite,
) (*dexpb.AttributeWrite, error) {
	if len(flow.actions) == 0 {
		return nil, nil
	}
	postInvocationValues := make(map[string]*dexpb.Value, len(writes))
	for _, write := range writes {
		if write != nil {
			postInvocationValues[write.GetKey()] = write.GetValue()
		}
	}
	permissionSet := make(map[string]struct{}, len(flow.actions))
	for _, action := range flow.actions {
		attributeName := action.condition.attribute.attributeName()
		value, found := postInvocationValues[attributeName]
		if !found {
			value = currentAttributes[attributeName]
		}
		if action.condition.matches(value) {
			permissionSet[action.requiredPermission] = struct{}{}
		}
	}
	permissions := sortedStringSet(permissionSet)
	currentPermissions, isCurrentValueValid := decodeWorkQueuePermissions(
		currentAttributes[WorkQueuePermissionsIndexKey],
	)
	if isCurrentValueValid && reflect.DeepEqual(currentPermissions, permissions) {
		return nil, nil
	}
	if len(permissions) == 0 {
		return mapAttributeDelete(
			WorkQueuePermissionsIndexKey,
			workQueuePermissionsAttributeIndex,
			false,
			false,
		)
	}
	value, indexConfig, err := encodeAttributeValue(
		permissions,
		workQueuePermissionsAttributeIndex,
	)
	if err != nil {
		return nil, err
	}
	return &dexpb.AttributeWrite{
		Key:         WorkQueuePermissionsIndexKey,
		Value:       value,
		IndexConfig: indexConfig,
	}, nil
}

func (flow *registeredFlow) appendInitialWorkQueuePermissions(
	writes []*dexpb.AttributeWrite,
) ([]*dexpb.AttributeWrite, error) {
	projection, err := flow.projectWorkQueuePermissions(nil, writes)
	if err != nil {
		return nil, err
	}
	if projection != nil {
		writes = append(writes, projection)
	}
	return writes, nil
}

func (flow *registeredFlow) actionPermissionMappingsForWrites(
	writes []*dexpb.AttributeWrite,
) *dexpb.ActionPermissionMappings {
	if len(flow.actions) == 0 {
		return nil
	}
	actionSourceKeys := make(map[string]struct{}, len(flow.actions))
	for _, action := range flow.actions {
		actionSourceKeys[action.condition.attribute.attributeName()] = struct{}{}
	}
	for _, write := range writes {
		if write == nil {
			continue
		}
		if _, found := actionSourceKeys[write.GetKey()]; found {
			return flow.actionPermissionMappings()
		}
	}
	return nil
}

func (flow *registeredFlow) actionPermissionMappings() *dexpb.ActionPermissionMappings {
	mappings := make([]*dexpb.ActionPermissionMapping, 0, len(flow.actions))
	for _, action := range flow.actions {
		mappings = append(mappings, &dexpb.ActionPermissionMapping{
			AttributeKey:       action.condition.attribute.attributeName(),
			EqualValues:        action.condition.operands,
			RequiredPermission: action.requiredPermission,
		})
	}
	return &dexpb.ActionPermissionMappings{Mappings: mappings}
}

func supportsActionAttributeType(valueType reflect.Type) bool {
	if valueType == nil {
		return false
	}
	switch valueType.Kind() {
	case reflect.String, reflect.Bool,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Uintptr,
		reflect.Float32, reflect.Float64:
		return true
	default:
		return false
	}
}

func equalActionScalarValues(left *dexpb.Value, right *dexpb.Value) bool {
	switch leftValue := left.GetKind().(type) {
	case *dexpb.Value_StringValue:
		rightValue, ok := right.GetKind().(*dexpb.Value_StringValue)
		return ok && leftValue.StringValue == rightValue.StringValue
	case *dexpb.Value_BoolValue:
		rightValue, ok := right.GetKind().(*dexpb.Value_BoolValue)
		return ok && leftValue.BoolValue == rightValue.BoolValue
	case *dexpb.Value_IntValue:
		rightValue, ok := right.GetKind().(*dexpb.Value_IntValue)
		return ok && leftValue.IntValue == rightValue.IntValue
	case *dexpb.Value_DoubleValue:
		rightValue, ok := right.GetKind().(*dexpb.Value_DoubleValue)
		return ok && leftValue.DoubleValue == rightValue.DoubleValue
	default:
		return false
	}
}

func decodeWorkQueuePermissions(value *dexpb.Value) ([]string, bool) {
	if value == nil {
		return []string{}, true
	}
	if _, deleted := value.Kind.(*dexpb.Value_NullValue); deleted {
		return []string{}, true
	}
	var permissions []string
	if err := decodeValue(value, &permissions); err != nil {
		return nil, false
	}
	permissionSet := make(map[string]struct{}, len(permissions))
	for _, permission := range permissions {
		permissionSet[permission] = struct{}{}
	}
	return sortedStringSet(permissionSet), true
}

func sortedStringSet(values map[string]struct{}) []string {
	result := make([]string, 0, len(values))
	for value := range values {
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}
