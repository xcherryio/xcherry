// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package interpreter

import (
	"github.com/superdurable/dex/gen/dexpb"
	"github.com/superdurable/dex/service/tenancy"
)

type SubFlowTracker struct {
	parentFlowID         string
	tenantID             string
	waitStateByStepExeId map[string]*subFlowWaitState
	subFlowIdLookupMap   map[string]*subFlowIdLookupEntry
}

type subFlowWaitState struct {
	waitingConditionState *dexpb.WaitingConditionState
	// keyed by condition index
	completedResults map[int32]*dexpb.FlowResult
}

type subFlowIdLookupEntry struct {
	waitState *subFlowWaitState
	// index of the condition in the WaitingCondition's subFlowConditions
	index int32
}

func NewSubFlowTracker(
	parentFlowID string,
	tenantID string,
	resumeInfos []*dexpb.StepExecutionResumeInfo,
) *SubFlowTracker {
	if parentFlowID == "" {
		panic("SubFlow tracker requires a parent Flow ID")
	}
	tracker := &SubFlowTracker{
		parentFlowID:         parentFlowID,
		tenantID:             tenantID,
		waitStateByStepExeId: map[string]*subFlowWaitState{},
		subFlowIdLookupMap:   map[string]*subFlowIdLookupEntry{},
	}
	for _, resumeInfo := range resumeInfos {
		if resumeInfo == nil || len(resumeInfo.GetWaitingCondition().GetSubFlowConditions()) == 0 {
			continue
		}
		completedResults := resumeInfo.GetCompletedConditions().GetCompletedSubFlowResults()
		if completedResults == nil {
			completedResults = map[int32]*dexpb.FlowResult{}
			if resumeInfo.CompletedConditions == nil {
				resumeInfo.CompletedConditions = &dexpb.StepExecutionCompletedConditions{}
			}
			resumeInfo.CompletedConditions.CompletedSubFlowResults = completedResults
		}
		tracker.Register(resumeInfo.GetStepExecutionId(), resumeInfo.GetWaitingCondition(), completedResults)
	}
	return tracker
}

func (t *SubFlowTracker) Register(
	stepExecutionID string,
	conditionState *dexpb.WaitingConditionState,
	completedResults map[int32]*dexpb.FlowResult,
) {
	if completedResults == nil {
		panic("SubFlow completed result map is nil")
	}
	waitState := &subFlowWaitState{
		waitingConditionState: conditionState,
		completedResults:      completedResults,
	}
	t.waitStateByStepExeId[stepExecutionID] = waitState
	for index := range conditionState.GetSubFlowConditions() {
		subFlowID := t.subFlowID(stepExecutionID, int32(index))
		t.subFlowIdLookupMap[subFlowID] = &subFlowIdLookupEntry{waitState: waitState, index: int32(index)}
	}
}

func (t *SubFlowTracker) ApplyImmediateFlowResultFromStart(
	stepExecutionID string, index int32, activityOutput *dexpb.StartSubFlowActivityOutput,
) {
	waitState := t.waitStateByStepExeId[stepExecutionID]
	if waitState == nil || activityOutput == nil {
		return
	}
	if index < 0 || int(index) >= len(waitState.waitingConditionState.GetSubFlowConditions()) {
		return
	}
	if activityOutput.GetImmediateFlowResult() != nil {
		if _, exists := waitState.completedResults[index]; !exists {
			waitState.completedResults[index] = activityOutput.GetImmediateFlowResult()
		}
	}
}

func (t *SubFlowTracker) HandleSubFlowCompletion(signal *dexpb.SubFlowCompletionSignalRequest) {
	result := signal.GetFlowResult()
	if result == nil {
		return
	}
	lookupEntry := t.subFlowIdLookupMap[signal.GetSubFlowId()]
	if lookupEntry == nil {
		return
	}
	if _, exists := lookupEntry.waitState.completedResults[lookupEntry.index]; !exists {
		lookupEntry.waitState.completedResults[lookupEntry.index] = result
	}
}

func (t *SubFlowTracker) MustGetCompletedResults(stepExecutionID string) map[int32]*dexpb.FlowResult {
	wait := t.waitStateByStepExeId[stepExecutionID]
	if wait == nil {
		panic("SubFlow wait state is missing for resumed Step " + stepExecutionID)
	}
	return wait.completedResults
}

func (t *SubFlowTracker) Unregister(stepExecutionID string) {
	waitState := t.waitStateByStepExeId[stepExecutionID]
	if waitState == nil {
		return
	}
	for index := range waitState.waitingConditionState.GetSubFlowConditions() {
		delete(t.subFlowIdLookupMap, t.subFlowID(stepExecutionID, int32(index)))
	}
	delete(t.waitStateByStepExeId, stepExecutionID)
}

// subFlowID panics on a tenant mismatch, because the workflow's own ID and input tenant are trusted.
func (t *SubFlowTracker) subFlowID(stepExecutionID string, index int32) string {
	subFlowID, err := tenancy.SubFlowID(t.tenantID, t.parentFlowID, stepExecutionID, index)
	if err != nil {
		panic(err)
	}
	return subFlowID
}
