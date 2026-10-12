// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package flowviz

import (
	"fmt"
	"strings"
)

type stepLink struct {
	from string
	to   string
}

// diagnoseV2GroupShapes warns about a group that is neither a run of linked Steps nor one choice.
func (analyzer *goAnalyzer) diagnoseV2GroupShapes() {
	links := stepLinks(analyzer.graph)
	for _, group := range analyzer.graph.Groups {
		parts := stepGroupParts(group.StepIDs, links)
		if len(parts) < 2 || isOneChoice(parts, links) {
			continue
		}
		names := make([]string, 0, len(parts))
		for _, part := range parts {
			stepTypes := make([]string, 0, len(part))
			for _, stepID := range part {
				stepTypes = append(stepTypes, strings.TrimPrefix(stepID, "step:"))
			}
			names = append(names, strings.Join(stepTypes, ", "))
		}
		analyzer.graph.AddDiagnostic(
			"warning",
			"v2_step_group_shape",
			fmt.Sprintf(
				"group %q is neither one run of Steps that lead to each other nor one choice that a single Step makes; "+
					"it falls into %d parts: %s. Split it so that each group does one thing",
				group.ID,
				len(parts),
				strings.Join(names, "; "),
			),
			nil,
		)
	}
}

// stepLinks returns every transition and failure transition between two different Steps.
func stepLinks(graph *Graph) []stepLink {
	parents := make(map[string]string, len(graph.Nodes))
	isStep := make(map[string]bool, len(graph.Nodes))
	for _, node := range graph.Nodes {
		parents[node.ID] = node.ParentID
		if node.Kind == "step" {
			isStep[node.ID] = true
		}
	}
	owningStep := func(nodeID string) string {
		for hops := 0; nodeID != "" && hops <= len(parents); hops++ {
			if isStep[nodeID] {
				return nodeID
			}
			nodeID = parents[nodeID]
		}
		return ""
	}
	links := make([]stepLink, 0)
	for _, edge := range graph.Edges {
		if edge.Kind != "transition" && edge.Kind != "failure_transition" {
			continue
		}
		from := owningStep(edge.From)
		to := owningStep(edge.To)
		if from == "" || to == "" || from == to {
			continue
		}
		links = append(links, stepLink{from: from, to: to})
	}
	return links
}

// stepGroupParts returns the group's Steps split into the parts its inner transitions join, in group order.
func stepGroupParts(stepIDs []string, links []stepLink) [][]string {
	root := make(map[string]string, len(stepIDs))
	for _, stepID := range stepIDs {
		root[stepID] = stepID
	}
	find := func(stepID string) string {
		for root[stepID] != stepID {
			stepID = root[stepID]
		}
		return stepID
	}
	for _, link := range links {
		if _, ok := root[link.from]; !ok {
			continue
		}
		if _, ok := root[link.to]; !ok {
			continue
		}
		root[find(link.from)] = find(link.to)
	}
	order := make([]string, 0)
	parts := make(map[string][]string)
	for _, stepID := range stepIDs {
		key := find(stepID)
		if _, ok := parts[key]; !ok {
			order = append(order, key)
		}
		parts[key] = append(parts[key], stepID)
	}
	result := make([][]string, 0, len(order))
	for _, key := range order {
		result = append(result, parts[key])
	}
	return result
}

// isOneChoice reports whether one Step outside the group leads into every part.
func isOneChoice(parts [][]string, links []stepLink) bool {
	member := make(map[string]bool)
	for _, part := range parts {
		for _, stepID := range part {
			member[stepID] = true
		}
	}
	var common map[string]bool
	for _, part := range parts {
		inPart := make(map[string]bool, len(part))
		for _, stepID := range part {
			inPart[stepID] = true
		}
		sources := make(map[string]bool)
		for _, link := range links {
			if inPart[link.to] && !member[link.from] {
				sources[link.from] = true
			}
		}
		if common == nil {
			common = sources
			continue
		}
		for source := range common {
			if !sources[source] {
				delete(common, source)
			}
		}
	}
	return len(common) > 0
}
