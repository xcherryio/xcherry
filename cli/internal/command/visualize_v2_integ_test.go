// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//go:build integration

package command

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/superdurable/dex/cli/internal/flowviz"
)

func TestVisualizeV2RefundFlows(t *testing.T) {
	repositoryRoot := visualizerRepositoryRoot(t)
	tests := []struct {
		name               string
		source             string
		wantFlowType       string
		wantGroupIDs       []string
		wantSummaryFields  []string
		wantActionRPCNames []string
		wantPermissions    []string
		wantIndexTypes     map[string]string
		wantIndexKeys      map[string]string
	}{
		{
			name: "deterministic",
			source: filepath.Join(repositoryRoot,
				"examples/go/products/customer-refund/deterministic/workflow.go"),
			wantFlowType:       "CustomerRefundFlow",
			wantGroupIDs:       []string{"intake", "evidence", "control", "resolution", "failure", "close"},
			wantSummaryFields:  []string{"charge-reference", "refund-amount", "recommended-action"},
			wantActionRPCNames: []string{},
			wantPermissions:    []string{},
			wantIndexTypes:     map[string]string{"case-status": "keyword"},
			wantIndexKeys:      map[string]string{"case-status": "keyword2"},
		},
		{
			name: "agentic",
			source: filepath.Join(repositoryRoot,
				"examples/go/products/customer-refund/agentic/workflow.go"),
			wantFlowType:      "AgenticCustomerRefundFlow",
			wantGroupIDs:      []string{"intake", "reasoning", "evidence", "control", "resolution", "failure", "close"},
			wantSummaryFields: []string{"in-charge-ref", "ev-payment-amount", "recommended-action", "guardrail-rule"},
			wantActionRPCNames: []string{
				"ApproveRefund", "RejectRefund", "ConfirmCustomerMessage", "EditCustomerMessage",
			},
			wantPermissions: []string{"refund.manage", "refund.manage", "refund.message", "refund.message"},
			wantIndexTypes: map[string]string{
				"case-status":    "keyword",
				"customer-email": "keyword",
				"refund-amount":  "double",
			},
			wantIndexKeys: map[string]string{
				"case-status":    "keyword2",
				"customer-email": "keyword1",
				"refund-amount":  "double1",
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			graph, err := flowviz.Analyze(context.Background(), test.source, flowviz.AnalyzeOptions{
				SchemaVersion: flowviz.SchemaVersionV2,
			})
			require.NoError(t, err)
			require.True(t, graph.Valid, "%+v", graph.Diagnostics)
			require.Equal(t, flowviz.SchemaVersionV2, graph.SchemaVersion)
			require.Equal(t, test.wantFlowType, graph.Flow.Name)
			require.NotNil(t, graph.V2)
			require.Equal(t, test.wantGroupIDs, v2GroupIDs(graph.Groups))
			require.Equal(t, test.wantSummaryFields, v2ViewFieldKeys(graph.V2.Summary.Fields))
			require.Equal(t, test.wantActionRPCNames, v2ActionRPCNames(graph.V2.Actions))
			require.Equal(t, test.wantPermissions, v2ActionPermissions(graph.V2.Actions))
			// Keyed rather than positional: declaring another Indexed Attribute must not move this.
			indexTypes := make(map[string]string, len(graph.V2.IndexedAttributes))
			for _, attribute := range graph.V2.IndexedAttributes {
				require.Equal(t, test.wantIndexKeys[attribute.AttributeKey], attribute.IndexKey)
				indexTypes[attribute.AttributeKey] = attribute.IndexType
			}
			require.Equal(t, test.wantIndexTypes, indexTypes)
			if test.name == "agentic" {
				connectorNode := graphNodeByID(t, graph, "step:GenerateCustomerMessageStep")
				require.Equal(t, true, connectorNode.Metadata["connectorFactory"])
				require.Equal(t, "mutation", connectorNode.Metadata["connectorOperationKind"])
				require.True(t, hasTransitionFromStepToTarget(
					graph,
					"step:agenticPrepareCustomerMessage",
					"step:GenerateCustomerMessageStep",
				), "concrete StepRef transition into Connector factory is missing")
			}
			for _, node := range graph.Nodes {
				if node.Kind != "step" {
					continue
				}
				require.NotNil(t, node.Metadata, node.ID)
				explanation, ok := node.Metadata["explanation"].(string)
				require.True(t, ok, node.ID)
				require.NotEmpty(t, explanation, node.ID)
			}

			firstJSON, err := flowviz.MarshalJSON(graph)
			require.NoError(t, err)
			secondGraph, err := flowviz.Analyze(context.Background(), test.source, flowviz.AnalyzeOptions{
				SchemaVersion: flowviz.SchemaVersionV2,
			})
			require.NoError(t, err)
			secondJSON, err := flowviz.MarshalJSON(secondGraph)
			require.NoError(t, err)
			require.Equal(t, firstJSON, secondJSON)
			require.NotContains(t, string(firstJSON), `"order"`)
		})
	}
}

func TestVisualizeV2BuildsRecursiveStartInputSchema(t *testing.T) {
	repositoryRoot := visualizerRepositoryRoot(t)
	graph, err := flowviz.Analyze(
		context.Background(),
		filepath.Join(repositoryRoot, "cli/internal/command/testfixtures/visualization-v2-start/workflow.go"),
		flowviz.AnalyzeOptions{SchemaVersion: flowviz.SchemaVersionV2},
	)
	require.NoError(t, err)
	require.True(t, graph.Valid, "%+v", graph.Diagnostics)
	require.NotNil(t, graph.V2.Start)
	require.Equal(t, "StartSchema", graph.V2.Start.StepType)
	require.Equal(t, "object", graph.V2.Start.Input.Kind)
	fields := make(map[string]flowviz.StartInputField, len(graph.V2.Start.Input.Fields))
	for _, field := range graph.V2.Start.Input.Fields {
		fields[field.Name] = field
	}
	require.True(t, fields["name"].Required)
	require.True(t, fields["correlationId"].Required)
	require.False(t, fields["optional"].Required)
	require.True(t, fields["optional"].Schema.Nullable)
	require.False(t, fields["omitted"].Required)
	require.Equal(t, "date-time", fields["nested"].Schema.Fields[1].Schema.Format)
	require.Equal(t, []flowviz.StartInputEnumValue{
		{Name: "PriorityNormal", Value: "1"},
		{Name: "PriorityUrgent", Value: "9223372036854775807"},
	}, fields["priority"].Schema.EnumValues)
	require.Equal(t, "-32768", fields["scores"].Schema.Items.Minimum)
	require.Equal(t, int64(2), *fields["decisions"].Schema.FixedLength)
	require.Equal(t, "18446744073709551615", fields["counters"].Schema.Values.Maximum)
	require.Len(t, graph.V2.Actions, 1)
	require.Equal(t, "qr-code", graph.V2.Actions[0].Input.Fields[0].Capture)
}

func TestVisualizeV2BuildsTopLevelScalarAndNullStartSchemas(t *testing.T) {
	repositoryRoot := visualizerRepositoryRoot(t)
	for _, testCase := range []struct {
		name       string
		source     string
		wantedKind string
	}{
		{name: "scalar", source: "examples/go/primitives/stream/workflow.go", wantedKind: "string"},
		{name: "null", source: "examples/go/patterns/inactiveness-tracker-timer/workflow.go", wantedKind: "null"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			graph, err := flowviz.Analyze(
				context.Background(), filepath.Join(repositoryRoot, testCase.source),
				flowviz.AnalyzeOptions{SchemaVersion: flowviz.SchemaVersionV2},
			)
			require.NoError(t, err)
			require.NotNil(t, graph.V2.Start)
			require.Equal(t, testCase.wantedKind, graph.V2.Start.Input.Kind)
		})
	}
}

func hasTransitionFromStepToTarget(graph *flowviz.Graph, sourceStepID string, targetStepID string) bool {
	for _, edge := range graph.Edges {
		if edge.Kind != "transition" || edge.To != targetStepID {
			continue
		}
		for _, node := range graph.Nodes {
			if node.ID == edge.From && node.ParentID == sourceStepID {
				return true
			}
		}
	}
	return false
}

func TestVisualizeV2ConnectorFactoryExample(t *testing.T) {
	repositoryRoot := visualizerRepositoryRoot(t)
	source := filepath.Join(repositoryRoot, "examples/go/products/connector-factory/workflow.go")
	graph, err := flowviz.Analyze(context.Background(), source, flowviz.AnalyzeOptions{
		SchemaVersion: flowviz.SchemaVersionV2,
	})
	require.NoError(t, err)
	require.True(t, graph.Valid, "%+v", graph.Diagnostics)
	require.Equal(t, "CustomerSummaryConnectorFlow", graph.Flow.Name)
	require.Equal(t, "step:InitializeCustomerSummary", graph.Flow.StartStepID)
	initializeNode := graphNodeByID(t, graph, "step:InitializeCustomerSummary")
	require.Equal(t, "InitializeCustomerSummary", initializeNode.Name)
	require.NotContains(t, initializeNode.Metadata, "displayName")
	require.Equal(t, "InitializeCustomerSummary", graph.V2.Start.StepType)
	completedNode := graphNodeByID(t, graph, "step:CustomerSummaryCompleted")
	require.Equal(t, "CustomerSummaryCompleted", completedNode.Name)
	require.NotContains(t, completedNode.Metadata, "displayName")
	require.Equal(t, []string{"generation", "recovery", "failure", "execute-failure"}, v2GroupIDs(graph.Groups))

	generateNode := graphNodeByID(t, graph, "step:GenerateCustomerSummary")
	require.Equal(t, "execute", generateNode.Phase)
	require.Equal(t, true, generateNode.Metadata["connectorFactory"])
	require.Equal(t, "mutation", generateNode.Metadata["connectorOperationKind"])
	require.Equal(t, "Generate a customer summary with streamed model progress.", generateNode.Metadata["explanation"])

	reconcileNode := graphNodeByID(t, graph, "step:ReconcileCustomerSummary")
	require.Equal(t, true, reconcileNode.Metadata["connectorFactory"])
	require.Equal(t, "query", reconcileNode.Metadata["connectorOperationKind"])

	require.Equal(t, map[string]string{
		"completed": "step:CustomerSummaryCompleted",
		"failed":    "step:CustomerSummaryFailed",
		"uncertain": "step:ValidateCustomerSummaryReconciliation",
		"defect":    "step:CustomerSummaryFailed",
	}, connectorBranchTargets(graph, "step:GenerateCustomerSummary"))
	require.Equal(t, map[string]string{
		"found":  "step:CustomerSummaryReconciled",
		"failed": "step:CustomerSummaryReconcileFailed",
		"defect": "step:CustomerSummaryReconcileFailed",
	}, connectorBranchTargets(graph, "step:ReconcileCustomerSummary"))

	resultEdge := graphEdge(t, graph, "resource_write", "step:GenerateCustomerSummary", "resource:attribute:generatedCustomerSummary", "Set")
	require.Equal(t, "execute", resultEdge.Metadata["phase"])
	reconciledResultEdge := graphEdge(t, graph, "resource_write", "step:ReconcileCustomerSummary", "resource:attribute:reconciledCustomerSummary", "Set")
	require.Equal(t, "execute", reconciledResultEdge.Metadata["phase"])
	structuredEdge := graphEdge(t, graph, "resource_write", "step:GenerateCustomerSummary", "resource:stream:customerSummaryProgress", "Write")
	require.Equal(t, true, structuredEdge.Metadata["bestEffort"])
	require.Equal(t, true, structuredEdge.Metadata["repeatable"])
	require.Equal(t, "progress", structuredEdge.Metadata["role"])
	require.Equal(t, "structured", structuredEdge.Metadata["format"])
	textEdge := graphEdge(t, graph, "resource_write", "step:GenerateCustomerSummary", "resource:stream:customerSummaryText", "Write")
	require.Equal(t, true, textEdge.Metadata["bestEffort"])
	require.Equal(t, true, textEdge.Metadata["repeatable"])
	require.Equal(t, "progress", textEdge.Metadata["role"])
	require.Equal(t, "text", textEdge.Metadata["format"])
	failureEdge := graphEdge(t, graph, "failure_transition", "step:GenerateCustomerSummary", "step:CustomerSummaryExecuteFailed", "Execute failure")
	require.Equal(t, true, failureEdge.Metadata["skipWaitFor"])

	graph.Source.Path = "examples/go/products/connector-factory/workflow.go"
	firstJSON, err := flowviz.MarshalJSON(graph)
	require.NoError(t, err)
	golden, err := os.ReadFile(filepath.Join(repositoryRoot, "docs/src/data/flow-definitions/connector-factory.json"))
	require.NoError(t, err)
	require.Equal(t, string(golden), string(firstJSON))
	secondGraph, err := flowviz.Analyze(context.Background(), source, flowviz.AnalyzeOptions{
		SchemaVersion: flowviz.SchemaVersionV2,
	})
	require.NoError(t, err)
	secondGraph.Source.Path = graph.Source.Path
	secondJSON, err := flowviz.MarshalJSON(secondGraph)
	require.NoError(t, err)
	require.Equal(t, firstJSON, secondJSON)
}

func TestVisualizeV2RejectsDynamicConnectorFactoryFields(t *testing.T) {
	repositoryRoot := visualizerRepositoryRoot(t)
	graph, err := flowviz.Analyze(
		context.Background(),
		filepath.Join(repositoryRoot, "cli/internal/command/testfixtures/connector-factory/dynamic/workflow.go"),
		flowviz.AnalyzeOptions{SchemaVersion: flowviz.SchemaVersionV2},
	)
	require.NoError(t, err)
	require.False(t, graph.Valid)
	codes := diagnosticCodes(graph.Diagnostics)
	require.Contains(t, codes, "connector_factory_config")
	require.Contains(t, codes, "connector_factory_branch")
	require.Contains(t, codes, "connector_factory_step_type")
	require.Contains(t, codes, "connector_factory_target")
}

func TestVisualizeV2PreservesGenericConnectorFactoryEscapeHatch(t *testing.T) {
	repositoryRoot := visualizerRepositoryRoot(t)
	graph, err := flowviz.Analyze(
		context.Background(),
		filepath.Join(repositoryRoot, "cli/internal/command/testfixtures/connector-factory/generic/workflow.go"),
		flowviz.AnalyzeOptions{SchemaVersion: flowviz.SchemaVersionV2},
	)
	require.NoError(t, err)
	require.True(t, graph.Valid, "%+v", graph.Diagnostics)
	require.Equal(t, map[string]string{
		"found":  "step:finishStep",
		"failed": "step:finishStep",
		"defect": "step:finishStep",
	}, connectorBranchTargets(graph, "step:GenericRetrieveResponse"))
}

func TestVisualizeDoesNotRecognizeSpoofedConnectorFactory(t *testing.T) {
	repositoryRoot := visualizerRepositoryRoot(t)
	graph, err := flowviz.Analyze(
		context.Background(),
		filepath.Join(repositoryRoot, "cli/internal/command/testfixtures/connector-factory/spoof/workflow.go"),
		flowviz.AnalyzeOptions{SchemaVersion: flowviz.SchemaVersionV1},
	)
	require.NoError(t, err)
	require.True(t, graph.Valid, "%+v", graph.Diagnostics)
	node := graphNodeByID(t, graph, "step:spoofStep")
	require.NotEqual(t, true, node.Metadata["connectorFactory"])
	for _, code := range diagnosticCodes(graph.Diagnostics) {
		require.NotContains(t, code, "connector_factory")
	}
}

func TestVisualizeV2PreservesDirectiveLineOrderWithoutParameterOrder(t *testing.T) {
	repositoryRoot := visualizerRepositoryRoot(t)
	source := filepath.Join(repositoryRoot, "examples/go/products/customer-refund/agentic/workflow.go")
	graph, err := flowviz.Analyze(context.Background(), source, flowviz.AnalyzeOptions{
		SchemaVersion: flowviz.SchemaVersionV2,
	})
	require.NoError(t, err)
	require.True(t, graph.Valid, "%+v", graph.Diagnostics)

	require.Equal(t, "intake", graph.Groups[0].ID)
	require.Equal(t, "Intake", graph.Groups[0].Label)
	require.Equal(t, []string{"reason", "gateRequestKey"}, v2ActionInputNames(graph.V2.Actions[1].Input.Fields))
	require.Equal(t, []string{"user", "attribute"}, v2ActionInputSources(graph.V2.Actions[1].Input.Fields))
}

func TestVisualizeV2WarnsAboutGroupsThatAreNeitherRunNorChoice(t *testing.T) {
	repositoryRoot := visualizerRepositoryRoot(t)
	graph, err := flowviz.Analyze(context.Background(), filepath.Join(
		repositoryRoot,
		"cli/internal/command/testfixtures/visualization-v2-group-shape/workflow.go",
	), flowviz.AnalyzeOptions{SchemaVersion: flowviz.SchemaVersionV2})
	require.NoError(t, err)
	// A warning never makes the definition invalid.
	require.True(t, graph.Valid, "%+v", graph.Diagnostics)
	require.Equal(t, []string{"begin", "decide", "tools", "reply", "wrap-up", "sheet", "failure"}, v2GroupIDs(graph.Groups))
	warnings := groupShapeWarnings(t, graph.Diagnostics)
	// The sheet's reads join only through hidden failure handling; a group of handlers is never judged.
	require.Equal(t, []string{
		`group "sheet" is neither one run of Steps that lead to each other nor one choice that a single Step makes; ` +
			"it falls into 2 parts: ReadSheetStep; RereadSheetStep. Split it so that each group does one thing",
		`group "wrap-up" is neither one run of Steps that lead to each other nor one choice that a single Step makes; ` +
			"it falls into 2 parts: NotifyOwnerStep; CleanUpStep. Split it so that each group does one thing",
	}, warnings)
}

func TestVisualizeV2WarnsAboutTheAgenticRefundFailureGroupOnly(t *testing.T) {
	repositoryRoot := visualizerRepositoryRoot(t)
	graph, err := flowviz.Analyze(context.Background(), filepath.Join(
		repositoryRoot,
		"examples/go/products/customer-refund/agentic/workflow.go",
	), flowviz.AnalyzeOptions{SchemaVersion: flowviz.SchemaVersionV2})
	require.NoError(t, err)
	warnings := groupShapeWarnings(t, graph.Diagnostics)
	// Evidence is a choice the agent's decision Step makes, so only the failure handlers warn.
	require.Len(t, warnings, 1)
	require.Contains(t, warnings[0], `group "failure"`)
}

func groupShapeWarnings(t *testing.T, diagnostics []flowviz.Diagnostic) []string {
	t.Helper()
	warnings := make([]string, 0)
	for _, diagnostic := range diagnostics {
		if diagnostic.Code == "v2_step_group_shape" {
			require.Equal(t, "warning", diagnostic.Severity)
			warnings = append(warnings, diagnostic.Message)
		}
	}
	return warnings
}

func TestVisualizeV2RejectsPython(t *testing.T) {
	repositoryRoot := visualizerRepositoryRoot(t)
	_, err := flowviz.Analyze(
		context.Background(),
		filepath.Join(repositoryRoot, "examples/python/dex_examples/patterns/recovery/failure_recovery_flow.py"),
		flowviz.AnalyzeOptions{SchemaVersion: flowviz.SchemaVersionV2},
	)
	require.EqualError(t, err, "schema version 2.0 supports Go source only")
}

func TestVisualizeV2ReportsMalformedNamedDirectives(t *testing.T) {
	repositoryRoot := visualizerRepositoryRoot(t)
	graph, err := flowviz.Analyze(
		context.Background(),
		filepath.Join(repositoryRoot, "cli/internal/command/testfixtures/visualization-v2-invalid/workflow.go"),
		flowviz.AnalyzeOptions{SchemaVersion: flowviz.SchemaVersionV2},
	)
	require.NoError(t, err)
	require.False(t, graph.Valid)
	messages := make([]string, 0, len(graph.Diagnostics))
	for _, diagnostic := range graph.Diagnostics {
		messages = append(messages, diagnostic.Message)
	}
	require.Contains(t, messages, "dex:group unknown argument unexpected")
	require.Contains(t, messages, "dex:indexed-attribute repeats argument attribute-key")
	require.Contains(t, messages, "dex:field missing required argument description")
	require.Contains(t, messages, "dex:field description: unterminated quoted string")
	require.Contains(t, messages, "Step missingGroupStep must declare exactly one dex:group directive")
	require.Contains(t, messages, "Step missingGroupStep must declare exactly one dex:explanation directive")
	require.Contains(t, messages, "Step invalidV2Step must declare exactly one dex:explanation directive")
	require.Contains(t, messages, `dex:indexed-attribute attribute-key "declared-indexed" does not match the Go declaration "actual-indexed"`)
	require.Contains(t, messages, `dex:field value-type "string" does not match Attribute "flag" type "bool"`)
	require.Contains(t, messages, "GetDexDisplay must be read-only")
	require.Contains(t, messages, `dex:input input field "missing" is not in the RPC input struct`)
	require.Contains(t, messages, "dex:input capture must be qr-code")
	require.Contains(t, messages, "dex:input capture requires a source:user string input field")
	require.Contains(t, messages, "RPC RejectBadPermission Action requires exactly one valid permission")
	require.Contains(t, messages, `dex:field ui-slot "headline" is not a UI slot this view has`)
	require.Contains(t, messages, `dex:field ui-slot "title" is already taken by Attribute "state"`)
	require.Contains(t, diagnosticCodes(graph.Diagnostics), "v2_start_input")
	require.Nil(t, graph.V2.Start)
	encoded, err := flowviz.MarshalJSON(graph)
	require.NoError(t, err)
	require.Contains(t, string(encoded), `"groups": []`)
	require.Contains(t, string(encoded), `"v2": {`)
}

func v2GroupIDs(groups []flowviz.StepGroup) []string {
	ids := make([]string, 0, len(groups))
	for _, group := range groups {
		ids = append(ids, group.ID)
	}
	return ids
}

func v2ViewFieldKeys(fields []flowviz.ViewField) []string {
	keys := make([]string, 0, len(fields))
	for _, field := range fields {
		keys = append(keys, field.AttributeKey)
	}
	return keys
}

func v2ActionRPCNames(actions []flowviz.Action) []string {
	names := make([]string, 0, len(actions))
	for _, action := range actions {
		names = append(names, action.RPCName)
	}
	return names
}

func v2ActionPermissions(actions []flowviz.Action) []string {
	permissions := make([]string, 0, len(actions))
	for _, action := range actions {
		permissions = append(permissions, action.RequiredPermission)
	}
	return permissions
}

func graphNodeByID(t *testing.T, graph *flowviz.Graph, nodeID string) flowviz.Node {
	t.Helper()
	for _, node := range graph.Nodes {
		if node.ID == nodeID {
			return node
		}
	}
	require.FailNow(t, "graph node not found", nodeID)
	return flowviz.Node{}
}

func connectorBranchTargets(graph *flowviz.Graph, stepID string) map[string]string {
	targets := make(map[string]string)
	for _, edge := range graph.Edges {
		if edge.Kind == "transition" && edge.From == stepID && edge.Metadata["connectorBranch"] == true {
			targets[edge.Label] = edge.To
		}
	}
	return targets
}

func graphEdge(t *testing.T, graph *flowviz.Graph, kind string, from string, to string, label string) flowviz.Edge {
	t.Helper()
	for _, edge := range graph.Edges {
		if edge.Kind == kind && edge.From == from && edge.To == to && edge.Label == label {
			return edge
		}
	}
	require.FailNow(t, "graph edge not found", "%s %s -> %s (%s)", kind, from, to, label)
	return flowviz.Edge{}
}

func v2ActionInputNames(fields []flowviz.ActionInputField) []string {
	names := make([]string, 0, len(fields))
	for _, field := range fields {
		names = append(names, field.FieldName)
	}
	return names
}

func v2ActionInputSources(fields []flowviz.ActionInputField) []string {
	sources := make([]string, 0, len(fields))
	for _, field := range fields {
		sources = append(sources, field.Source)
	}
	return sources
}
