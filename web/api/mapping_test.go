// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package api

import (
	"testing"

	"github.com/superdurable/dex/gen/dexpb"
)

func TestMapPullRouting(t *testing.T) {
	routing := &dexpb.PullRouting{StickyAppWorkerId: "worker-first", ServedByAppWorkerId: "worker-second", Source: dexpb.PullRoutingSource_PULL_ROUTING_SOURCE_FLOW_TYPE_QUEUE}
	state, err := mapFlowState(&dexpb.GetFlowStateResponse{
		StickyAppWorkerId:    "worker-second",
		ActiveStepExecutions: []*dexpb.ActiveStepExecutionState{{StepExecutionId: "CheckStep-1", WaitForPullRouting: routing}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if state.StickyAppWorkerID != "worker-second" {
		t.Fatalf("sticky Worker: %q", state.StickyAppWorkerID)
	}
	mapped := state.ActiveStepExecutions[0].WaitForPullRouting
	if mapped["servedByAppWorkerId"] != "worker-second" || mapped["stickyAppWorkerId"] != "worker-first" || mapped["source"] != float64(2) {
		t.Fatalf("routing: %#v", mapped)
	}
	history, err := mapHistoryEvent(&dexpb.FlowHistoryEvent{Payload: &dexpb.FlowHistoryEvent_StepExecuteCompleted{StepExecuteCompleted: &dexpb.StepExecuteCompletedEvent{Context: &dexpb.StepMethodEventContext{PullRouting: routing}}}})
	if err != nil {
		t.Fatal(err)
	}
	if history.Payload["context"].(map[string]interface{})["pullRouting"].(map[string]interface{})["servedByAppWorkerId"] != "worker-second" {
		t.Fatalf("history: %#v", history)
	}
	rpc, err := mapHistoryEvent(&dexpb.FlowHistoryEvent{Payload: &dexpb.FlowHistoryEvent_RpcExecutionCompleted{RpcExecutionCompleted: &dexpb.RpcExecutionCompletedEvent{ServedByAppWorkerId: "rpc-worker"}}})
	if err != nil {
		t.Fatal(err)
	}
	if rpc.Payload["servedByAppWorkerId"] != "rpc-worker" {
		t.Fatalf("RPC: %#v", rpc)
	}
}

func TestMapLegacyRouting(t *testing.T) {
	state, err := mapFlowState(&dexpb.GetFlowStateResponse{ActiveStepExecutions: []*dexpb.ActiveStepExecutionState{{}}})
	if err != nil {
		t.Fatal(err)
	}
	if state.StickyAppWorkerID != "" || state.ActiveStepExecutions[0].WaitForPullRouting != nil || state.ActiveStepExecutions[0].ExecutePullRouting != nil {
		t.Fatalf("legacy routing: %#v", state)
	}
}
