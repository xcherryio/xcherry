// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package flowviz

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/constant"
	"go/token"
	"go/types"
	"math"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"
)

var v2GroupIDPattern = regexp.MustCompile(`^[a-z][a-z0-9]*(?:-[a-z0-9]+)*$`)
var actionPermissionPattern = regexp.MustCompile(`^[a-z][a-z0-9]*(?:[.-][a-z0-9]+)*$`)

type v2DirectiveArgument struct {
	text  string
	array bool
}

type v2Directive struct {
	name      string
	arguments map[string]v2DirectiveArgument
	comment   *ast.Comment
}

type v2AttributeDeclaration struct {
	variableName string
	key          string
	valueType    string
	isMap        bool
	isIndexed    bool
	indexKey     string
	indexType    string
	directives   []v2Directive
	span         *Span
}

type v2RPCRegistration struct {
	name             string
	actionExpression ast.Expr
}

type v2StructField struct {
	jsonName  string
	valueType string
	required  bool
	span      *Span
}

func (analyzer *goAnalyzer) analyzeVisualizationV2(flowType string) {
	attributes := analyzer.collectV2Attributes()
	analyzer.graph.Groups = analyzer.collectV2Groups()
	analyzer.diagnoseV2GroupShapes()
	analyzer.applyV2Explanations()
	registeredRPCs := analyzer.registeredV2RPCs(flowType)
	registeredRPCNames := make(map[string]bool, len(registeredRPCs))
	for _, rpc := range registeredRPCs {
		registeredRPCNames[rpc.name] = true
	}
	indexedAttributes := analyzer.collectV2IndexedAttributes(attributes)
	summary := analyzer.collectV2View(
		flowType,
		"GetDexSummary",
		attributes,
		registeredRPCNames,
		indexedAttributes,
		false,
	)
	display := analyzer.collectV2View(
		flowType,
		"GetDexDisplay",
		attributes,
		registeredRPCNames,
		indexedAttributes,
		true,
	)
	actions := analyzer.collectV2Actions(flowType, attributes, registeredRPCs)
	start := analyzer.collectV2StartDefinition()
	connectorTriggerBindings := analyzer.collectConnectorTriggerBindings(flowType)
	analyzer.graph.V2 = &V2Definition{
		IndexedAttributes:        indexedAttributes,
		Summary:                  summary,
		Display:                  display,
		Actions:                  actions,
		Start:                    start,
		ConnectorTriggerBindings: connectorTriggerBindings,
	}
}

func (analyzer *goAnalyzer) collectV2Groups() []StepGroup {
	groups := make([]StepGroup, 0)
	groupIndexes := make(map[string]int)
	typeDirectives := analyzer.v2TypeDirectives()
	for _, stepType := range analyzer.registeredSteps {
		if factory, ok := analyzer.connectorFactories[stepType]; ok {
			groupID := factory.annotations.groupID
			groupLabel := factory.annotations.groupLabel
			if !v2GroupIDPattern.MatchString(groupID) {
				analyzer.graph.AddDiagnostic("error", "v2_step_group", fmt.Sprintf("Connector factory Step %s group-id %q must be kebab-case", stepType, groupID), nil)
				continue
			}
			if strings.TrimSpace(groupLabel) == "" {
				analyzer.graph.AddDiagnostic("error", "v2_step_group", fmt.Sprintf("Connector factory Step %s group-label must not be empty", stepType), nil)
				continue
			}
			groupIndex, found := groupIndexes[groupID]
			if !found {
				groupIndexes[groupID] = len(groups)
				groups = append(groups, StepGroup{ID: groupID, Label: groupLabel, StepIDs: make([]string, 0)})
				groupIndex = len(groups) - 1
			} else if groups[groupIndex].Label != groupLabel {
				analyzer.graph.AddDiagnostic("error", "v2_step_group", fmt.Sprintf("group %q uses conflicting labels", groupID), nil)
				continue
			}
			groups[groupIndex].StepIDs = append(groups[groupIndex].StepIDs, "step:"+stepType)
			continue
		}
		directives := directivesNamed(typeDirectives[stepType], "group")
		if len(directives) != 1 {
			analyzer.graph.AddDiagnostic(
				"error",
				"v2_step_group",
				fmt.Sprintf("Step %s must declare exactly one dex:group directive", stepType),
				nil,
			)
			continue
		}
		directive := directives[0]
		if !analyzer.validateV2Directive(directive, []string{"group-id", "group-label"}, []string{"group-id", "group-label"}) {
			continue
		}
		groupID := directive.arguments["group-id"].text
		groupLabel := directive.arguments["group-label"].text
		if !v2GroupIDPattern.MatchString(groupID) {
			analyzer.addV2DirectiveError(directive, fmt.Sprintf("group-id %q must be kebab-case", groupID))
			continue
		}
		if strings.TrimSpace(groupLabel) == "" {
			analyzer.addV2DirectiveError(directive, "group-label must not be empty")
			continue
		}
		groupIndex, found := groupIndexes[groupID]
		if !found {
			groupIndexes[groupID] = len(groups)
			groups = append(groups, StepGroup{ID: groupID, Label: groupLabel, StepIDs: make([]string, 0)})
			groupIndex = len(groups) - 1
		} else if groups[groupIndex].Label != groupLabel {
			analyzer.addV2DirectiveError(directive, fmt.Sprintf("group %q uses conflicting labels", groupID))
			continue
		}
		groups[groupIndex].StepIDs = append(groups[groupIndex].StepIDs, "step:"+stepType)
	}
	return groups
}

func (analyzer *goAnalyzer) applyV2Explanations() {
	typeDirectives := analyzer.v2TypeDirectives()
	for _, stepType := range analyzer.registeredSteps {
		if factory, ok := analyzer.connectorFactories[stepType]; ok {
			explanation := strings.TrimSpace(factory.annotations.explanation)
			if explanation == "" {
				analyzer.graph.AddDiagnostic("error", "v2_step_explanation", fmt.Sprintf("Connector factory Step %s explanation must not be empty", stepType), nil)
				continue
			}
			nodeID := "step:" + stepType
			for index := range analyzer.graph.Nodes {
				if analyzer.graph.Nodes[index].ID != nodeID {
					continue
				}
				if analyzer.graph.Nodes[index].Metadata == nil {
					analyzer.graph.Nodes[index].Metadata = make(map[string]any)
				}
				analyzer.graph.Nodes[index].Metadata["explanation"] = explanation
				break
			}
			continue
		}
		directives := directivesNamed(typeDirectives[stepType], "explanation")
		if len(directives) != 1 {
			analyzer.graph.AddDiagnostic(
				"error",
				"v2_step_explanation",
				fmt.Sprintf("Step %s must declare exactly one dex:explanation directive", stepType),
				nil,
			)
			continue
		}
		directive := directives[0]
		if !analyzer.validateV2Directive(directive, []string{"text"}, []string{"text"}) {
			continue
		}
		explanation := strings.TrimSpace(directive.arguments["text"].text)
		if explanation == "" {
			analyzer.addV2DirectiveError(directive, "text must not be empty")
			continue
		}
		nodeID := "step:" + stepType
		for index := range analyzer.graph.Nodes {
			if analyzer.graph.Nodes[index].ID != nodeID {
				continue
			}
			if analyzer.graph.Nodes[index].Metadata == nil {
				analyzer.graph.Nodes[index].Metadata = make(map[string]any)
			}
			analyzer.graph.Nodes[index].Metadata["explanation"] = explanation
			break
		}
	}
}

func (analyzer *goAnalyzer) v2TypeDirectives() map[string][]v2Directive {
	directives := make(map[string][]v2Directive)
	for _, declaration := range analyzer.file.Decls {
		general, ok := declaration.(*ast.GenDecl)
		if !ok || general.Tok != token.TYPE {
			continue
		}
		for _, specification := range general.Specs {
			typeSpec, typeOK := specification.(*ast.TypeSpec)
			if !typeOK {
				continue
			}
			comments := typeSpec.Doc
			if comments == nil && len(general.Specs) == 1 {
				comments = general.Doc
			}
			directives[typeSpec.Name.Name] = analyzer.parseV2Directives(comments)
		}
	}
	return directives
}

func (analyzer *goAnalyzer) collectV2Attributes() map[string]v2AttributeDeclaration {
	attributes := make(map[string]v2AttributeDeclaration)
	for _, declaration := range analyzer.file.Decls {
		general, ok := declaration.(*ast.GenDecl)
		if !ok || general.Tok != token.VAR {
			continue
		}
		for _, specification := range general.Specs {
			valueSpec, valueOK := specification.(*ast.ValueSpec)
			if !valueOK {
				continue
			}
			comments := valueSpec.Doc
			if comments == nil && len(general.Specs) == 1 {
				comments = general.Doc
			}
			directives := analyzer.parseV2Directives(comments)
			for index, name := range valueSpec.Names {
				if index >= len(valueSpec.Values) {
					continue
				}
				call, callOK := valueSpec.Values[index].(*ast.CallExpr)
				if !callOK {
					continue
				}
				resourceKind := goResourceKind(analyzer.callName(call))
				if resourceKind != "attribute" {
					continue
				}
				attributeKey := name.Name
				if len(call.Args) > 0 {
					if staticKey, static := analyzer.staticString(call.Args[0]); static {
						attributeKey = staticKey
					}
				}
				resource := analyzer.resourceDetails(call)
				valueType := analyzer.v2ValueTypeForGenericCall(call)
				if valueType == "" {
					valueType = normalizeV2TypeName(resource.ValueType)
				}
				if resource.Map {
					valueType = "attribute-map"
				}
				isIndexed, indexKey, indexType := analyzer.v2IndexConfiguration(call, attributeKey, resource.Map)
				attributeDirectives := directives
				if len(valueSpec.Names) > 1 && len(directivesNamed(directives, "indexed-attribute")) > 0 {
					analyzer.graph.AddDiagnostic(
						"error",
						"v2_indexed_attribute",
						"dex:indexed-attribute must annotate a single variable declaration",
						analyzer.span(valueSpec),
					)
					attributeDirectives = nil
				}
				attributes[attributeKey] = v2AttributeDeclaration{
					variableName: name.Name,
					key:          attributeKey,
					valueType:    valueType,
					isMap:        resource.Map,
					isIndexed:    isIndexed,
					indexKey:     indexKey,
					indexType:    indexType,
					directives:   attributeDirectives,
					span:         analyzer.span(valueSpec),
				}
			}
		}
	}
	return attributes
}

func (analyzer *goAnalyzer) collectV2IndexedAttributes(
	attributes map[string]v2AttributeDeclaration,
) []IndexedAttribute {
	indexedAttributes := make([]IndexedAttribute, 0)
	for _, declaration := range analyzer.v2AttributeDeclarationsInSourceOrder(attributes) {
		directives := directivesNamed(declaration.directives, "indexed-attribute")
		if declaration.isIndexed && len(directives) != 1 {
			analyzer.graph.AddDiagnostic(
				"error",
				"v2_indexed_attribute",
				fmt.Sprintf("indexed Attribute %q must declare exactly one dex:indexed-attribute directive", declaration.key),
				declaration.span,
			)
			continue
		}
		if !declaration.isIndexed && len(directives) > 0 {
			analyzer.addV2DirectiveError(directives[0], fmt.Sprintf("Attribute %q is not indexed", declaration.key))
			continue
		}
		if !declaration.isIndexed {
			continue
		}
		directive := directives[0]
		allowed := []string{"attribute-key", "index-key", "index-type", "value-type", "description"}
		if !analyzer.validateV2Directive(directive, allowed, allowed) {
			continue
		}
		comparisons := []struct {
			argument string
			actual   string
		}{
			{argument: "attribute-key", actual: declaration.key},
			{argument: "index-key", actual: declaration.indexKey},
			{argument: "index-type", actual: declaration.indexType},
			{argument: "value-type", actual: declaration.valueType},
		}
		isValid := true
		for _, comparison := range comparisons {
			declared := directive.arguments[comparison.argument].text
			if declared == comparison.actual {
				continue
			}
			analyzer.addV2DirectiveError(
				directive,
				fmt.Sprintf("%s %q does not match the Go declaration %q", comparison.argument, declared, comparison.actual),
			)
			isValid = false
		}
		if !isV2IndexCompatible(declaration.indexType, declaration.valueType) {
			analyzer.addV2DirectiveError(
				directive,
				fmt.Sprintf("index-type %q does not support value-type %q", declaration.indexType, declaration.valueType),
			)
			isValid = false
		}
		if !isValid {
			continue
		}
		indexedAttributes = append(indexedAttributes, IndexedAttribute{
			AttributeKey: declaration.key,
			IndexKey:     declaration.indexKey,
			IndexType:    declaration.indexType,
			ValueType:    declaration.valueType,
			Description:  directive.arguments["description"].text,
		})
	}
	return indexedAttributes
}

func (analyzer *goAnalyzer) v2AttributeDeclarationsInSourceOrder(
	attributes map[string]v2AttributeDeclaration,
) []v2AttributeDeclaration {
	ordered := make([]v2AttributeDeclaration, 0, len(attributes))
	for _, declaration := range analyzer.file.Decls {
		general, ok := declaration.(*ast.GenDecl)
		if !ok || general.Tok != token.VAR {
			continue
		}
		for _, specification := range general.Specs {
			valueSpec, valueOK := specification.(*ast.ValueSpec)
			if !valueOK {
				continue
			}
			for index := range valueSpec.Values {
				call, callOK := valueSpec.Values[index].(*ast.CallExpr)
				if !callOK || goResourceKind(analyzer.callName(call)) != "attribute" || len(call.Args) == 0 {
					continue
				}
				attributeKey, static := analyzer.staticString(call.Args[0])
				if !static {
					continue
				}
				if attribute, found := attributes[attributeKey]; found {
					ordered = append(ordered, attribute)
				}
			}
		}
	}
	return ordered
}

func (analyzer *goAnalyzer) collectV2View(
	flowType string,
	rpcName string,
	attributes map[string]v2AttributeDeclaration,
	registeredRPCs map[string]bool,
	indexedAttributes []IndexedAttribute,
	canEdit bool,
) RPCView {
	view := RPCView{RPCName: rpcName, Fields: make([]ViewField, 0)}
	method := analyzer.methods[flowType][rpcName]
	if method == nil {
		analyzer.graph.AddDiagnostic("error", "v2_view_rpc", fmt.Sprintf("Flow must define %s", rpcName), nil)
		return view
	}
	if !registeredRPCs[rpcName] {
		analyzer.graph.AddDiagnostic("error", "v2_view_rpc", fmt.Sprintf("%s must be registered in GetRPCs", rpcName), analyzer.span(method))
	}
	analyzer.validateV2RPCSignature(method, "none", "map")
	analyzer.validateV2ReadOnlyRPC(method)
	indexedKeys := make(map[string]bool, len(indexedAttributes))
	for _, attribute := range indexedAttributes {
		indexedKeys[attribute.AttributeKey] = true
	}
	seen := make(map[string]bool)
	claimedUISlots := make(map[string]string)
	for _, directive := range directivesNamed(analyzer.parseV2Directives(method.Doc), "field") {
		required := []string{"attribute-key", "value-type", "editable", "description"}
		allowed := append(append([]string{}, required...), "ui-slot")
		if !analyzer.validateV2Directive(directive, allowed, required) {
			continue
		}
		attributeKey := directive.arguments["attribute-key"].text
		attribute, found := attributes[attributeKey]
		if !found {
			analyzer.addV2DirectiveError(directive, fmt.Sprintf("field Attribute %q must be declared in this file", attributeKey))
			continue
		}
		if seen[attributeKey] {
			analyzer.addV2DirectiveError(directive, fmt.Sprintf("field Attribute %q is duplicated", attributeKey))
			continue
		}
		seen[attributeKey] = true
		valueType := directive.arguments["value-type"].text
		if valueType != attribute.valueType {
			analyzer.addV2DirectiveError(directive, fmt.Sprintf("value-type %q does not match Attribute %q type %q", valueType, attributeKey, attribute.valueType))
			continue
		}
		isEditable, err := strconv.ParseBool(directive.arguments["editable"].text)
		if err != nil {
			analyzer.addV2DirectiveError(directive, "editable must be true or false")
			continue
		}
		if isEditable && !canEdit {
			analyzer.addV2DirectiveError(directive, "GetDexSummary fields cannot be editable")
			continue
		}
		if isEditable && !isV2EditableType(valueType) {
			analyzer.addV2DirectiveError(directive, fmt.Sprintf("value-type %q is not editable", valueType))
			continue
		}
		if !canEdit && indexedKeys[attributeKey] {
			analyzer.addV2DirectiveError(directive, fmt.Sprintf("Summary field %q duplicates an indexed Attribute", attributeKey))
			continue
		}
		uiSlot, uiSlotOK := analyzer.v2FieldUISlot(directive, claimedUISlots, attributeKey)
		if !uiSlotOK {
			continue
		}
		view.Fields = append(view.Fields, ViewField{
			AttributeKey: attributeKey,
			ValueType:    valueType,
			Editable:     isEditable,
			Description:  directive.arguments["description"].text,
			UISlot:       uiSlot,
		})
	}
	analyzer.validateV2ViewOutputKeys(method, view.Fields)
	return view
}

// UI slots a field may claim, mapped to whether only one field may claim each.
//
// Closed because the renderer is written against it. An unknown value would be ignored silently and
// the author would never learn the field did not land where they meant it to.
var v2FieldUISlots = map[string]bool{
	"title":          true,
	"subtitle":       true,
	"status":         true,
	"recommendation": true,
	"reason":         false,
}

// Reads and checks `ui-slot`, which is optional. Returns false when already reported.
func (analyzer *goAnalyzer) v2FieldUISlot(
	directive v2Directive,
	claimedUISlots map[string]string,
	attributeKey string,
) (string, bool) {
	argument, declared := directive.arguments["ui-slot"]
	if !declared {
		return "", true
	}
	uiSlot := argument.text
	unique, known := v2FieldUISlots[uiSlot]
	if !known {
		analyzer.addV2DirectiveError(directive, fmt.Sprintf("ui-slot %q is not a UI slot this view has", uiSlot))
		return "", false
	}
	if unique {
		if holder, taken := claimedUISlots[uiSlot]; taken {
			analyzer.addV2DirectiveError(directive, fmt.Sprintf("ui-slot %q is already taken by Attribute %q", uiSlot, holder))
			return "", false
		}
		claimedUISlots[uiSlot] = attributeKey
	}
	return uiSlot, true
}

func (analyzer *goAnalyzer) collectV2Actions(
	flowType string,
	attributes map[string]v2AttributeDeclaration,
	registeredRPCs []v2RPCRegistration,
) []Action {
	actions := make([]Action, 0)
	for _, rpc := range registeredRPCs {
		method := analyzer.methods[flowType][rpc.name]
		if method == nil {
			continue
		}
		directives := analyzer.parseV2Directives(method.Doc)
		if len(directivesNamed(directives, "action")) > 0 || len(directivesNamed(directives, "when")) > 0 {
			analyzer.graph.AddDiagnostic("error", "v2_action", "dex:action and dex:when are replaced by RPCOptions.Action", analyzer.span(method))
		}
		if rpc.actionExpression == nil {
			continue
		}
		action, actionOK := analyzer.v2ActionFromExpression(rpc.name, rpc.actionExpression, attributes)
		if !actionOK {
			continue
		}
		inputDirectives := directivesNamed(directives, "input")
		input, inputOK := analyzer.v2ActionInput(method, inputDirectives, attributes)
		if !inputOK {
			continue
		}
		action.Input = input
		actions = append(actions, action)
	}
	return actions
}

func (analyzer *goAnalyzer) v2ActionFromExpression(
	rpcName string,
	expression ast.Expr,
	attributes map[string]v2AttributeDeclaration,
) (Action, bool) {
	call, ok := expression.(*ast.CallExpr)
	if !ok || analyzer.callName(call) != "DefineAction" || len(call.Args) < 3 {
		analyzer.graph.AddDiagnostic("error", "v2_action", fmt.Sprintf("RPC %s Action must directly call dex.DefineAction", rpcName), analyzer.span(expression))
		return Action{}, false
	}
	label, isStatic := analyzer.staticString(call.Args[0])
	if !isStatic || strings.TrimSpace(label) == "" {
		analyzer.graph.AddDiagnostic("error", "v2_action", fmt.Sprintf("RPC %s Action label must be a non-empty compile-time string", rpcName), analyzer.span(call.Args[0]))
		return Action{}, false
	}
	condition, conditionOK := analyzer.v2ActionConditionFromExpression(call.Args[1], attributes)
	if !conditionOK {
		return Action{}, false
	}
	permission := ""
	permissionCount := 0
	for _, optionExpression := range call.Args[2:] {
		optionCall, optionOK := optionExpression.(*ast.CallExpr)
		if !optionOK || analyzer.callName(optionCall) != "ActionRequiresPermission" || len(optionCall.Args) != 1 {
			analyzer.graph.AddDiagnostic("error", "v2_action", fmt.Sprintf("RPC %s Action options must directly call dex.ActionRequiresPermission", rpcName), analyzer.span(optionExpression))
			return Action{}, false
		}
		staticPermission, permissionOK := analyzer.staticString(optionCall.Args[0])
		if !permissionOK {
			analyzer.graph.AddDiagnostic("error", "v2_action", fmt.Sprintf("RPC %s Action permission must be a compile-time string", rpcName), analyzer.span(optionCall.Args[0]))
			return Action{}, false
		}
		permission = staticPermission
		permissionCount++
	}
	if permissionCount != 1 || !actionPermissionPattern.MatchString(permission) {
		analyzer.graph.AddDiagnostic("error", "v2_action", fmt.Sprintf("RPC %s Action requires exactly one valid permission", rpcName), analyzer.span(call))
		return Action{}, false
	}
	return Action{
		RPCName:            rpcName,
		Label:              label,
		RequiredPermission: permission,
		Condition:          condition,
	}, true
}

func (analyzer *goAnalyzer) v2ActionConditionFromExpression(
	expression ast.Expr,
	attributes map[string]v2AttributeDeclaration,
) (ActionCondition, bool) {
	call, ok := expression.(*ast.CallExpr)
	if !ok || analyzer.callName(call) != "WhenAttributeMatches" || len(call.Args) < 2 {
		analyzer.graph.AddDiagnostic("error", "v2_action", "Action condition must directly call dex.WhenAttributeMatches", analyzer.span(expression))
		return ActionCondition{}, false
	}
	resourceID := analyzer.resourceForExpression(call.Args[0])
	variableName := strings.TrimPrefix(resourceID, "resource:attribute:")
	var attribute v2AttributeDeclaration
	found := false
	for _, candidate := range attributes {
		if candidate.variableName == variableName {
			attribute = candidate
			found = true
			break
		}
	}
	if !found || attribute.isMap {
		analyzer.graph.AddDiagnostic("error", "v2_action", "Action condition must reference a scalar Attribute declared in this file", analyzer.span(call.Args[0]))
		return ActionCondition{}, false
	}
	values := make([]any, 0, len(call.Args)-1)
	for _, matchExpression := range call.Args[1:] {
		matchCall, matchOK := matchExpression.(*ast.CallExpr)
		if !matchOK || analyzer.callName(matchCall) != "AttributeMatchEqual" || len(matchCall.Args) != 1 {
			analyzer.graph.AddDiagnostic("error", "v2_action", "Action condition matches must directly call dex.AttributeMatchEqual", analyzer.span(matchExpression))
			return ActionCondition{}, false
		}
		value, valueOK := analyzer.staticV2Scalar(matchCall.Args[0])
		if !valueOK || !v2JSONValueMatchesType(value, attribute.valueType) {
			analyzer.graph.AddDiagnostic("error", "v2_action", fmt.Sprintf("Action condition value must be a matching compile-time %s", attribute.valueType), analyzer.span(matchCall.Args[0]))
			return ActionCondition{}, false
		}
		values = append(values, value)
	}
	return ActionCondition{AttributeKey: attribute.key, Operator: "in", Values: values}, true
}

func (analyzer *goAnalyzer) staticV2Scalar(expression ast.Expr) (any, bool) {
	typeAndValue, ok := analyzer.typeInfo.Types[expression]
	if !ok || typeAndValue.Value == nil {
		return nil, false
	}
	switch typeAndValue.Value.Kind() {
	case constant.String:
		return constant.StringVal(typeAndValue.Value), true
	case constant.Bool:
		return constant.BoolVal(typeAndValue.Value), true
	case constant.Int:
		value, exact := constant.Int64Val(typeAndValue.Value)
		return value, exact
	case constant.Float:
		value, exact := constant.Float64Val(typeAndValue.Value)
		return value, exact && !math.IsInf(value, 0) && !math.IsNaN(value)
	default:
		return nil, false
	}
}

func (analyzer *goAnalyzer) v2ActionInput(
	method *ast.FuncDecl,
	directives []v2Directive,
	attributes map[string]v2AttributeDeclaration,
) (ActionInput, bool) {
	if len(directives) == 0 {
		return ActionInput{Kind: "none"}, analyzer.validateV2RPCSignature(method, "none", "none")
	}
	if !analyzer.validateV2RPCSignature(method, "object", "none") {
		return ActionInput{}, false
	}
	structFields, found := analyzer.v2RPCInputStruct(method)
	if !found {
		return ActionInput{}, false
	}
	fieldsByName := make(map[string]v2StructField, len(structFields))
	for _, field := range structFields {
		fieldsByName[field.jsonName] = field
	}
	input := ActionInput{Kind: "object", Fields: make([]ActionInputField, 0, len(directives))}
	seen := make(map[string]bool)
	for _, directive := range directives {
		allowed := []string{"field-name", "value-type", "source", "attribute-key", "capture", "required", "description"}
		required := []string{"field-name", "value-type", "source", "required", "description"}
		if !analyzer.validateV2Directive(directive, allowed, required) {
			continue
		}
		fieldName := directive.arguments["field-name"].text
		structField, fieldFound := fieldsByName[fieldName]
		if !fieldFound {
			analyzer.addV2DirectiveError(directive, fmt.Sprintf("input field %q is not in the RPC input struct", fieldName))
			continue
		}
		if seen[fieldName] {
			analyzer.addV2DirectiveError(directive, fmt.Sprintf("input field %q is duplicated", fieldName))
			continue
		}
		seen[fieldName] = true
		valueType := directive.arguments["value-type"].text
		if valueType != structField.valueType || !isV2EditableType(valueType) {
			analyzer.addV2DirectiveError(directive, fmt.Sprintf("input field %q type %q does not match Go type %q", fieldName, valueType, structField.valueType))
			continue
		}
		isRequired, err := strconv.ParseBool(directive.arguments["required"].text)
		if err != nil || isRequired != structField.required {
			analyzer.addV2DirectiveError(directive, fmt.Sprintf("input field %q required must match Go pointer semantics", fieldName))
			continue
		}
		source := directive.arguments["source"].text
		attributeKey := ""
		capture := ""
		if captureArgument, exists := directive.arguments["capture"]; exists {
			capture = captureArgument.text
			if capture != "qr-code" {
				analyzer.addV2DirectiveError(directive, "capture must be qr-code")
				continue
			}
			if source != "user" || valueType != "string" {
				analyzer.addV2DirectiveError(directive, "capture requires a source:user string input field")
				continue
			}
		}
		switch source {
		case "user":
			if _, exists := directive.arguments["attribute-key"]; exists {
				analyzer.addV2DirectiveError(directive, "source:user forbids attribute-key")
				continue
			}
		case "attribute":
			argument, exists := directive.arguments["attribute-key"]
			if !exists {
				analyzer.addV2DirectiveError(directive, "source:attribute requires attribute-key")
				continue
			}
			attributeKey = argument.text
			attribute, attributeFound := attributes[attributeKey]
			if !attributeFound || attribute.isMap || attribute.valueType != valueType {
				analyzer.addV2DirectiveError(directive, fmt.Sprintf("source Attribute %q must be a matching scalar Attribute", attributeKey))
				continue
			}
		default:
			analyzer.addV2DirectiveError(directive, "source must be user or attribute")
			continue
		}
		input.Fields = append(input.Fields, ActionInputField{
			FieldName:    fieldName,
			ValueType:    valueType,
			Source:       source,
			AttributeKey: attributeKey,
			Capture:      capture,
			Required:     isRequired,
			Description:  directive.arguments["description"].text,
		})
	}
	if len(seen) != len(structFields) {
		analyzer.graph.AddDiagnostic("error", "v2_action_input", "every RPC input struct field must have one dex:input directive", analyzer.span(method))
		return input, false
	}
	return input, len(input.Fields) == len(structFields)
}

func (analyzer *goAnalyzer) v2RPCInputStruct(method *ast.FuncDecl) ([]v2StructField, bool) {
	inputType := analyzer.v2RPCInputType(method)
	inputTypeName := namedTypeName(inputType)
	if inputTypeName == "" {
		analyzer.graph.AddDiagnostic("error", "v2_action_input", "Action RPC input must be a named struct in the Flow file", analyzer.span(method))
		return nil, false
	}
	for _, declaration := range analyzer.file.Decls {
		general, ok := declaration.(*ast.GenDecl)
		if !ok || general.Tok != token.TYPE {
			continue
		}
		for _, specification := range general.Specs {
			typeSpec, typeOK := specification.(*ast.TypeSpec)
			if !typeOK || typeSpec.Name.Name != inputTypeName {
				continue
			}
			structType, structOK := typeSpec.Type.(*ast.StructType)
			if !structOK {
				analyzer.graph.AddDiagnostic("error", "v2_action_input", "Action RPC input must be a struct", analyzer.span(typeSpec))
				return nil, false
			}
			fields := make([]v2StructField, 0)
			for _, field := range structType.Fields.List {
				if len(field.Names) != 1 || !field.Names[0].IsExported() || field.Tag == nil {
					analyzer.graph.AddDiagnostic("error", "v2_action_input", "Action RPC input fields must be exported and have JSON tags", analyzer.span(field))
					continue
				}
				tagText, err := strconv.Unquote(field.Tag.Value)
				if err != nil {
					analyzer.graph.AddDiagnostic("error", "v2_action_input", "Action RPC input has an invalid struct tag", analyzer.span(field))
					continue
				}
				jsonName := strings.Split(reflect.StructTag(tagText).Get("json"), ",")[0]
				if jsonName == "" || jsonName == "-" {
					analyzer.graph.AddDiagnostic("error", "v2_action_input", "Action RPC input fields require explicit JSON names", analyzer.span(field))
					continue
				}
				valueExpression := field.Type
				required := true
				if pointer, isPointer := field.Type.(*ast.StarExpr); isPointer {
					valueExpression = pointer.X
					required = false
				}
				fields = append(fields, v2StructField{
					jsonName:  jsonName,
					valueType: analyzer.v2ValueTypeForExpression(valueExpression),
					required:  required,
					span:      analyzer.span(field),
				})
			}
			return fields, true
		}
	}
	analyzer.graph.AddDiagnostic("error", "v2_action_input", fmt.Sprintf("Action RPC input type %s must be declared in the Flow file", inputTypeName), analyzer.span(method))
	return nil, false
}

func (analyzer *goAnalyzer) registeredV2RPCs(flowType string) []v2RPCRegistration {
	method := analyzer.methods[flowType]["GetRPCs"]
	if method == nil || method.Body == nil {
		analyzer.graph.AddDiagnostic("error", "v2_rpc_registration", "Flow must define GetRPCs in the Flow file", nil)
		return nil
	}
	registrations := make([]v2RPCRegistration, 0)
	ast.Inspect(method.Body, func(current ast.Node) bool {
		call, ok := current.(*ast.CallExpr)
		if !ok || analyzer.callName(call) != "DefineRPC" || len(call.Args) == 0 {
			return true
		}
		selector, selectorOK := call.Args[0].(*ast.SelectorExpr)
		if selectorOK {
			registration := v2RPCRegistration{name: selector.Sel.Name}
			if len(call.Args) > 1 {
				registration.actionExpression = analyzer.v2RPCActionExpression(call.Args[1])
			}
			registrations = append(registrations, registration)
		}
		return false
	})
	return registrations
}

func (analyzer *goAnalyzer) v2RPCActionExpression(optionsExpression ast.Expr) ast.Expr {
	if unary, ok := optionsExpression.(*ast.UnaryExpr); ok && unary.Op == token.AND {
		optionsExpression = unary.X
	}
	options, ok := optionsExpression.(*ast.CompositeLit)
	if !ok {
		return nil
	}
	for _, element := range options.Elts {
		keyValue, keyValueOK := element.(*ast.KeyValueExpr)
		if keyValueOK && analyzer.expressionString(keyValue.Key) == "Action" {
			return keyValue.Value
		}
	}
	return nil
}

func (analyzer *goAnalyzer) validateV2RPCSignature(method *ast.FuncDecl, inputKind string, outputKind string) bool {
	object := analyzer.typeInfo.Defs[method.Name]
	if object == nil {
		return false
	}
	signature, ok := object.Type().(*types.Signature)
	if !ok || signature.Params().Len() != 2 || signature.Results().Len() != 2 {
		analyzer.graph.AddDiagnostic("error", "v2_rpc_signature", fmt.Sprintf("RPC %s has an invalid signature", method.Name.Name), analyzer.span(method))
		return false
	}
	inputType := signature.Params().At(1).Type()
	inputText := types.TypeString(inputType, nil)
	if inputKind == "none" && !strings.HasSuffix(inputText, "/dex.None") && inputText != "dex.None" {
		analyzer.graph.AddDiagnostic("error", "v2_rpc_signature", fmt.Sprintf("RPC %s input must be dex.None", method.Name.Name), analyzer.span(method))
		return false
	}
	if inputKind == "object" {
		underlying := inputType.Underlying()
		if _, isStruct := underlying.(*types.Struct); !isStruct {
			analyzer.graph.AddDiagnostic("error", "v2_rpc_signature", fmt.Sprintf("RPC %s input must be a named struct", method.Name.Name), analyzer.span(method))
			return false
		}
	}
	resultText := types.TypeString(signature.Results().At(0).Type(), nil)
	expectedOutput := "map[string]any"
	if outputKind == "none" {
		expectedOutput = "dex.None"
	}
	if outputKind == "map" && !strings.Contains(resultText, "RPCResult[map[string]any]") {
		analyzer.graph.AddDiagnostic("error", "v2_rpc_signature", fmt.Sprintf("RPC %s output must be map[string]any", method.Name.Name), analyzer.span(method))
		return false
	}
	if outputKind == "none" && (!strings.Contains(resultText, "RPCResult[") || !strings.Contains(resultText, ".None]")) {
		analyzer.graph.AddDiagnostic("error", "v2_rpc_signature", fmt.Sprintf("RPC %s output must be %s", method.Name.Name, expectedOutput), analyzer.span(method))
		return false
	}
	return true
}

func (analyzer *goAnalyzer) v2RPCInputType(method *ast.FuncDecl) types.Type {
	object := analyzer.typeInfo.Defs[method.Name]
	if object == nil {
		return nil
	}
	signature, ok := object.Type().(*types.Signature)
	if !ok || signature.Params().Len() != 2 {
		return nil
	}
	return signature.Params().At(1).Type()
}

func (analyzer *goAnalyzer) validateV2ReadOnlyRPC(method *ast.FuncDecl) {
	declarations := make(map[types.Object]*ast.FuncDecl)
	for _, declaration := range analyzer.file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok {
			continue
		}
		object := analyzer.typeInfo.Defs[function.Name]
		if object != nil {
			declarations[object] = function
		}
	}
	analyzer.doValidateV2ReadOnlyRPC(method, method.Name.Name, declarations, make(map[*ast.FuncDecl]bool))
}

func (analyzer *goAnalyzer) doValidateV2ReadOnlyRPC(
	method *ast.FuncDecl,
	rpcName string,
	declarations map[types.Object]*ast.FuncDecl,
	visited map[*ast.FuncDecl]bool,
) {
	if method == nil || method.Body == nil || visited[method] {
		return
	}
	visited[method] = true
	forbidden := map[string]bool{
		"Set": true, "Delete": true, "Publish": true, "DeleteChannelMessage": true,
		"GoTo": true, "GoToMany": true, "CancelSteps": true,
	}
	ast.Inspect(method.Body, func(current ast.Node) bool {
		call, ok := current.(*ast.CallExpr)
		if ok {
			if forbidden[analyzer.callName(call)] {
				analyzer.graph.AddDiagnostic("error", "v2_view_rpc_side_effect", fmt.Sprintf("%s must be read-only", rpcName), analyzer.span(call))
			}
			if called := analyzer.v2LocalFunctionDeclaration(call, declarations); called != nil {
				analyzer.doValidateV2ReadOnlyRPC(called, rpcName, declarations, visited)
			}
		}
		composite, ok := current.(*ast.CompositeLit)
		if !ok || !strings.Contains(analyzer.expressionString(composite.Type), "RPCResult") {
			return true
		}
		for _, element := range composite.Elts {
			keyValue, keyValueOK := element.(*ast.KeyValueExpr)
			if !keyValueOK {
				continue
			}
			fieldName := analyzer.expressionString(keyValue.Key)
			if fieldName == "NextSteps" || fieldName == "CancelingSteps" {
				analyzer.graph.AddDiagnostic("error", "v2_view_rpc_side_effect", fmt.Sprintf("%s must be read-only", rpcName), analyzer.span(keyValue))
			}
		}
		return true
	})
}

func (analyzer *goAnalyzer) v2LocalFunctionDeclaration(
	call *ast.CallExpr,
	declarations map[types.Object]*ast.FuncDecl,
) *ast.FuncDecl {
	var object types.Object
	switch function := call.Fun.(type) {
	case *ast.Ident:
		object = analyzer.typeInfo.Uses[function]
	case *ast.SelectorExpr:
		if selection := analyzer.typeInfo.Selections[function]; selection != nil {
			object = selection.Obj()
		} else {
			object = analyzer.typeInfo.Uses[function.Sel]
		}
	}
	return declarations[object]
}

func (analyzer *goAnalyzer) validateV2ViewOutputKeys(method *ast.FuncDecl, fields []ViewField) {
	declared := make(map[string]bool, len(fields))
	for _, field := range fields {
		declared[field.AttributeKey] = true
	}
	var outputKeys map[string]bool
	ast.Inspect(method.Body, func(current ast.Node) bool {
		composite, ok := current.(*ast.CompositeLit)
		if !ok || analyzer.expressionString(composite.Type) != "map[string]any" {
			return true
		}
		keys := make(map[string]bool, len(composite.Elts))
		for _, element := range composite.Elts {
			keyValue, keyValueOK := element.(*ast.KeyValueExpr)
			if !keyValueOK {
				return true
			}
			key, static := analyzer.staticString(keyValue.Key)
			if !static {
				return true
			}
			keys[key] = true
		}
		if outputKeys == nil {
			outputKeys = keys
		}
		return true
	})
	if outputKeys == nil {
		analyzer.graph.AddDiagnostic(
			"error",
			"v2_view_rpc_output",
			fmt.Sprintf("%s must return its declared fields from a map[string]any literal", method.Name.Name),
			analyzer.span(method),
		)
		return
	}
	for key := range declared {
		if !outputKeys[key] {
			analyzer.graph.AddDiagnostic("error", "v2_view_rpc_output", fmt.Sprintf("%s omits declared field %q", method.Name.Name, key), analyzer.span(method))
		}
	}
	for key := range outputKeys {
		if !declared[key] {
			analyzer.graph.AddDiagnostic("error", "v2_view_rpc_output", fmt.Sprintf("%s returns undeclared field %q", method.Name.Name, key), analyzer.span(method))
		}
	}
}

func (analyzer *goAnalyzer) v2IndexConfiguration(
	attributeCall *ast.CallExpr,
	attributeKey string,
	isMap bool,
) (bool, string, string) {
	for _, option := range attributeCall.Args[1:] {
		indexedCall, ok := option.(*ast.CallExpr)
		if !ok || analyzer.callName(indexedCall) != "Indexed" || len(indexedCall.Args) != 1 {
			continue
		}
		composite, compositeOK := indexedCall.Args[0].(*ast.CompositeLit)
		if !compositeOK {
			return true, "", "unknown"
		}
		indexKey := ""
		indexType := "unknown"
		for _, element := range composite.Elts {
			keyValue, keyValueOK := element.(*ast.KeyValueExpr)
			if !keyValueOK {
				continue
			}
			key := analyzer.expressionString(keyValue.Key)
			switch key {
			case "Type":
				indexType = normalizeV2IndexType(analyzer.expressionString(keyValue.Value))
			case "IndexKey":
				if staticValue, static := analyzer.staticString(keyValue.Value); static {
					indexKey = staticValue
				}
			}
		}
		if indexKey == "" && !isMap {
			indexKey = attributeKey
		}
		return true, indexKey, indexType
	}
	return false, "", ""
}

func (analyzer *goAnalyzer) v2ValueTypeForGenericCall(call *ast.CallExpr) string {
	switch function := call.Fun.(type) {
	case *ast.IndexExpr:
		return analyzer.v2ValueTypeForExpression(function.Index)
	case *ast.IndexListExpr:
		if len(function.Indices) > 0 {
			return analyzer.v2ValueTypeForExpression(function.Indices[0])
		}
	}
	return ""
}

func (analyzer *goAnalyzer) v2ValueTypeForExpression(expression ast.Expr) string {
	if typeAndValue, found := analyzer.typeInfo.Types[expression]; found && typeAndValue.Type != nil {
		return normalizeV2GoType(typeAndValue.Type)
	}
	return normalizeV2TypeName(analyzer.expressionString(expression))
}

func normalizeV2GoType(valueType types.Type) string {
	if valueType == nil {
		return "json"
	}
	if named, ok := valueType.(*types.Named); ok && named.Obj() != nil && named.Obj().Pkg() != nil && named.Obj().Pkg().Path() == "time" && named.Obj().Name() == "Time" {
		return "datetime"
	}
	if _, ok := valueType.(*types.Named); ok {
		return "json"
	}
	switch underlying := valueType.Underlying().(type) {
	case *types.Basic:
		switch underlying.Kind() {
		case types.String:
			return "string"
		case types.Bool:
			return "bool"
		case types.Int64:
			return "int64"
		case types.Float64:
			return "double"
		}
	case *types.Slice:
		if basic, ok := underlying.Elem().Underlying().(*types.Basic); ok && basic.Kind() == types.String {
			return "string-array"
		}
		return "array"
	case *types.Array:
		return "array"
	case *types.Struct, *types.Map:
		return "object"
	}
	return "json"
}

func normalizeV2TypeName(typeName string) string {
	trimmed := strings.TrimSpace(typeName)
	switch trimmed {
	case "string":
		return "string"
	case "bool":
		return "bool"
	case "int64":
		return "int64"
	case "float64":
		return "double"
	case "[]string":
		return "string-array"
	case "time.Time":
		return "datetime"
	default:
		return "json"
	}
}

func normalizeV2IndexType(typeName string) string {
	trimmed := strings.TrimPrefix(strings.TrimSpace(typeName), "dex.")
	switch trimmed {
	case "IndexKeyword":
		return "keyword"
	case "IndexFullText":
		return "fulltext"
	case "IndexKeywordArray":
		return "keyword-array"
	case "IndexInt":
		return "int"
	case "IndexDouble":
		return "double"
	case "IndexBool":
		return "bool"
	case "IndexDatetime":
		return "datetime"
	default:
		return "unknown"
	}
}

func isV2EditableType(valueType string) bool {
	switch valueType {
	case "string", "int64", "double", "bool", "datetime":
		return true
	default:
		return false
	}
}

func isV2IndexCompatible(indexType string, valueType string) bool {
	compatibleValueTypes := map[string]string{
		"keyword":       "string",
		"fulltext":      "string",
		"keyword-array": "string-array",
		"int":           "int64",
		"double":        "double",
		"bool":          "bool",
		"datetime":      "datetime",
	}
	return compatibleValueTypes[indexType] == valueType
}

func v2JSONValueMatchesType(value any, valueType string) bool {
	switch valueType {
	case "string":
		_, ok := value.(string)
		return ok
	case "datetime":
		text, ok := value.(string)
		if !ok {
			return false
		}
		_, err := time.Parse(time.RFC3339Nano, text)
		return err == nil
	case "bool":
		_, ok := value.(bool)
		return ok
	case "int64":
		number, ok := value.(json.Number)
		if !ok {
			return false
		}
		_, err := number.Int64()
		return err == nil
	case "double":
		number, ok := value.(json.Number)
		if !ok {
			return false
		}
		parsed, err := number.Float64()
		return err == nil && !math.IsInf(parsed, 0) && !math.IsNaN(parsed)
	default:
		return false
	}
}

func (analyzer *goAnalyzer) parseV2Directives(comments *ast.CommentGroup) []v2Directive {
	if comments == nil {
		return nil
	}
	directives := make([]v2Directive, 0)
	for _, comment := range comments.List {
		text := strings.TrimSpace(strings.TrimPrefix(comment.Text, "//"))
		if !strings.HasPrefix(text, "dex:") {
			continue
		}
		directive, err := parseV2Directive(text, comment)
		if err != nil {
			analyzer.graph.AddDiagnostic("error", "v2_directive", err.Error(), analyzer.span(comment))
			continue
		}
		directives = append(directives, directive)
	}
	return directives
}

func parseV2Directive(text string, comment *ast.Comment) (v2Directive, error) {
	remaining := strings.TrimPrefix(text, "dex:")
	nameEnd := strings.IndexFunc(remaining, unicode.IsSpace)
	name := remaining
	if nameEnd >= 0 {
		name = remaining[:nameEnd]
		remaining = strings.TrimSpace(remaining[nameEnd:])
	} else {
		remaining = ""
	}
	if name == "" {
		return v2Directive{}, fmt.Errorf("dex directive name is required")
	}
	directive := v2Directive{name: name, arguments: make(map[string]v2DirectiveArgument), comment: comment}
	for remaining != "" {
		colon := strings.IndexByte(remaining, ':')
		if colon <= 0 {
			return v2Directive{}, fmt.Errorf("dex:%s arguments must use name:value", name)
		}
		argumentName := remaining[:colon]
		if strings.IndexFunc(argumentName, unicode.IsSpace) >= 0 {
			return v2Directive{}, fmt.Errorf("dex:%s has an invalid argument name", name)
		}
		if _, duplicated := directive.arguments[argumentName]; duplicated {
			return v2Directive{}, fmt.Errorf("dex:%s repeats argument %s", name, argumentName)
		}
		remaining = remaining[colon+1:]
		argument, tail, err := parseV2DirectiveArgument(remaining)
		if err != nil {
			return v2Directive{}, fmt.Errorf("dex:%s %s: %w", name, argumentName, err)
		}
		directive.arguments[argumentName] = argument
		remaining = strings.TrimSpace(tail)
	}
	return directive, nil
}

func parseV2DirectiveArgument(input string) (v2DirectiveArgument, string, error) {
	if input == "" {
		return v2DirectiveArgument{}, "", fmt.Errorf("value is required")
	}
	switch input[0] {
	case '"':
		end, err := scanV2JSONString(input)
		if err != nil {
			return v2DirectiveArgument{}, "", err
		}
		var value string
		if err := json.Unmarshal([]byte(input[:end]), &value); err != nil {
			return v2DirectiveArgument{}, "", fmt.Errorf("invalid quoted string")
		}
		return v2DirectiveArgument{text: value}, input[end:], nil
	case '[':
		end, err := scanV2JSONArray(input)
		if err != nil {
			return v2DirectiveArgument{}, "", err
		}
		if _, err := decodeV2JSONArray(input[:end]); err != nil {
			return v2DirectiveArgument{}, "", fmt.Errorf("invalid JSON array")
		}
		return v2DirectiveArgument{text: input[:end], array: true}, input[end:], nil
	default:
		end := strings.IndexFunc(input, unicode.IsSpace)
		if end < 0 {
			end = len(input)
		}
		if end == 0 {
			return v2DirectiveArgument{}, "", fmt.Errorf("value is required")
		}
		return v2DirectiveArgument{text: input[:end]}, input[end:], nil
	}
}

func decodeV2JSONArray(value string) ([]any, error) {
	decoder := json.NewDecoder(strings.NewReader(value))
	decoder.UseNumber()
	var values []any
	if err := decoder.Decode(&values); err != nil {
		return nil, err
	}
	return values, nil
}

func scanV2JSONString(input string) (int, error) {
	escaped := false
	for index := 1; index < len(input); index++ {
		switch {
		case escaped:
			escaped = false
		case input[index] == '\\':
			escaped = true
		case input[index] == '"':
			return index + 1, nil
		}
	}
	return 0, fmt.Errorf("unterminated quoted string")
}

func scanV2JSONArray(input string) (int, error) {
	depth := 0
	inString := false
	escaped := false
	for index := 0; index < len(input); index++ {
		character := input[index]
		if inString {
			switch {
			case escaped:
				escaped = false
			case character == '\\':
				escaped = true
			case character == '"':
				inString = false
			}
			continue
		}
		switch character {
		case '"':
			inString = true
		case '[':
			depth++
		case ']':
			depth--
			if depth == 0 {
				return index + 1, nil
			}
		}
	}
	return 0, fmt.Errorf("unterminated JSON array")
}

func (analyzer *goAnalyzer) validateV2Directive(
	directive v2Directive,
	allowed []string,
	required []string,
) bool {
	allowedNames := make(map[string]bool, len(allowed))
	for _, name := range allowed {
		allowedNames[name] = true
	}
	isValid := true
	for name := range directive.arguments {
		if allowedNames[name] {
			argument := directive.arguments[name]
			if argument.array && !(directive.name == "when" && name == "values") {
				analyzer.addV2DirectiveError(directive, fmt.Sprintf("argument %s must not be a JSON array", name))
				isValid = false
			}
			continue
		}
		analyzer.addV2DirectiveError(directive, fmt.Sprintf("unknown argument %s", name))
		isValid = false
	}
	for _, name := range required {
		if _, found := directive.arguments[name]; found {
			continue
		}
		analyzer.addV2DirectiveError(directive, fmt.Sprintf("missing required argument %s", name))
		isValid = false
	}
	return isValid
}

func (analyzer *goAnalyzer) addV2DirectiveError(directive v2Directive, message string) {
	analyzer.graph.AddDiagnostic("error", "v2_directive", fmt.Sprintf("dex:%s %s", directive.name, message), analyzer.span(directive.comment))
}

func directivesNamed(directives []v2Directive, name string) []v2Directive {
	matched := make([]v2Directive, 0)
	for _, directive := range directives {
		if directive.name == name {
			matched = append(matched, directive)
		}
	}
	return matched
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
