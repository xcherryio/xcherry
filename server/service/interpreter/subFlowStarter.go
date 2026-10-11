// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package interpreter

import (
	"fmt"

	"github.com/superdurable/dex/config"
	"github.com/superdurable/dex/gen/dexpb"
	"github.com/superdurable/dex/service/interpreter/interfaces"
)

type SubFlowStarter struct {
	provider         interfaces.WorkflowProvider
	activities       *Activities
	tracker          *SubFlowTracker
	stepExecutionID  string
	parentFlowConfig *dexpb.FlowConfig
	condition        *dexpb.WaitingCondition
	activityConfig   *config.InterpreterActivityConfig
	doneByIdx        []bool
	startErrByIdx    map[int32]error
}

func NewSubFlowStarter(
	provider interfaces.WorkflowProvider,
	activities *Activities,
	tracker *SubFlowTracker,
	stepExecutionID string,
	parentFlowConfig *dexpb.FlowConfig,
	condition *dexpb.WaitingCondition,
	activityConfig *config.InterpreterActivityConfig,
) *SubFlowStarter {
	if activityConfig == nil {
		panic("SubFlow starter requires non-nil configuration")
	}
	return &SubFlowStarter{
		provider:         provider,
		activities:       activities,
		tracker:          tracker,
		stepExecutionID:  stepExecutionID,
		parentFlowConfig: parentFlowConfig,
		condition:        condition,
		activityConfig:   activityConfig,
		doneByIdx:        make([]bool, len(condition.GetSubFlowConditions())),
		startErrByIdx:    map[int32]error{},
	}
}

// StartAll starts every SubFlow and waits for each start attempt to finish.
//
// Returns the Await error when the wait itself fails. Otherwise returns the
// lowest-index start failure, or nil when every SubFlow started. StartFailureResults
// reports each failed start.
func (s *SubFlowStarter) StartAll(ctx interfaces.UnifiedContext) error {
	ctx = s.provider.WithActivityOptions(ctx, s.activityOptions())
	for index := range s.condition.GetSubFlowConditions() {
		startCtx := s.provider.ExtendContextWithValue(ctx, "subFlowIndex", index)
		s.provider.GoNamed(startCtx, fmt.Sprintf("start-sub-flow-%d", index), s.startOne)
	}
	if err := s.provider.Await(ctx, s.isAllDone); err != nil {
		return err
	}
	for index := range s.condition.GetSubFlowConditions() {
		if err := s.startErrByIdx[int32(index)]; err != nil {
			return err
		}
	}
	return nil
}

// StartFailureResults returns a failed SubFlow result for each SubFlow that could not start, keyed by condition index.
func (s *SubFlowStarter) StartFailureResults() map[int32]*dexpb.FlowResult {
	results := make(map[int32]*dexpb.FlowResult, len(s.startErrByIdx))
	for index, err := range s.startErrByIdx {
		results[index] = &dexpb.FlowResult{
			FlowStatus:   dexpb.FlowStatus_FLOW_STATUS_FAILED,
			ErrorType:    dexpb.FlowErrorType_FLOW_ERROR_TYPE_SUB_FLOW_START_FAILED,
			ErrorMessage: err.Error(),
		}
	}
	return results
}

func (s *SubFlowStarter) startOne(ctx interfaces.UnifiedContext) {
	index, ok := s.provider.GetContextValue(ctx, "subFlowIndex").(int)
	if !ok {
		panic("cannot read SubFlow index from workflow context")
	}
	condition := s.condition.GetSubFlowConditions()[index]
	var output dexpb.StartSubFlowActivityOutput
	startInput := &dexpb.StartSubFlowActivityInput{
		Condition:             condition,
		ParentFlowConfig:      s.parentFlowConfig,
		ParentStepExecutionId: s.stepExecutionID,
	}
	err := s.provider.ExecuteLocalActivity(&output, ctx, s.activities.StartSubFlow, startInput)
	if err != nil {
		s.startErrByIdx[int32(index)] = err
	}
	if err == nil {
		s.tracker.ApplyImmediateFlowResultFromStart(s.stepExecutionID, int32(index), &output)
	}
	s.doneByIdx[index] = true
}

func (s *SubFlowStarter) activityOptions() interfaces.ActivityOptions {
	activityConfig := s.activityConfig.EffectiveSubFlowStartActivityConfig()
	return interfaces.ActivityOptions{
		LocalActivityScheduleToCloseTimeout: activityConfig.ScheduleToCloseTimeout,
		RetryPolicy:                         activityConfig.RetryPolicy,
	}
}

func (s *SubFlowStarter) isAllDone() bool {
	for _, done := range s.doneByIdx {
		if !done {
			return false
		}
	}
	return true
}
