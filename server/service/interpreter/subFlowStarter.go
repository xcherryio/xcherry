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
	"github.com/superdurable/dex/service"
	"github.com/superdurable/dex/service/interpreter/interfaces"
)

type SubFlowStarter struct {
	provider         interfaces.WorkflowProvider
	globalVersioner  *GlobalVersioner
	activities       *Activities
	tracker          *SubFlowTracker
	stepExecutionID  string
	parentFlowConfig *dexpb.FlowConfig
	condition        *dexpb.WaitingCondition
	activityConfig   *config.InterpreterActivityConfig
	doneByIdx        []bool
	startErr         error
}

func NewSubFlowStarter(
	provider interfaces.WorkflowProvider,
	globalVersioner *GlobalVersioner,
	activities *Activities,
	tracker *SubFlowTracker,
	stepExecutionID string,
	parentInfo service.BasicInfo,
	parentFlowConfig *dexpb.FlowConfig,
	condition *dexpb.WaitingCondition,
	activityConfig *config.InterpreterActivityConfig,
) *SubFlowStarter {
	if activityConfig == nil || globalVersioner == nil {
		panic("SubFlow starter requires non-nil configuration and versioner")
	}
	return &SubFlowStarter{
		provider:         provider,
		globalVersioner:  globalVersioner,
		activities:       activities,
		tracker:          tracker,
		stepExecutionID:  stepExecutionID,
		parentFlowConfig: parentFlowConfig,
		condition:        condition,
		activityConfig:   activityConfig,
		doneByIdx:        make([]bool, len(condition.GetSubFlowConditions())),
	}
}

func (s *SubFlowStarter) StartAll(ctx interfaces.UnifiedContext) error {
	ctx = s.provider.WithActivityOptions(ctx, s.activityOptions())
	for index := range s.condition.GetSubFlowConditions() {
		startCtx := s.provider.ExtendContextWithValue(ctx, "subFlowIndex", index)
		s.provider.GoNamed(startCtx, fmt.Sprintf("start-sub-flow-%d", index), s.startOne)
	}
	if err := s.provider.Await(ctx, s.isAllDone); err != nil {
		return err
	}
	return s.startErr
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
		TypedRejectionEnabled: s.globalVersioner.UsesTypedSubFlowStartFailures(),
	}
	err := s.provider.ExecuteLocalActivity(&output, ctx, s.activities.StartSubFlow, startInput)
	if err != nil {
		if s.startErr == nil {
			s.startErr = err
		}
	}
	if err == nil {
		s.tracker.ApplyImmediateFlowResultFromStart(s.stepExecutionID, int32(index), &output)
	}
	s.doneByIdx[index] = true
}

func (s *SubFlowStarter) activityOptions() interfaces.ActivityOptions {
	activityConfig := s.activityConfig.EffectiveSubFlowStartActivityConfig()
	options := interfaces.ActivityOptions{
		LocalActivityScheduleToCloseTimeout: activityConfig.ScheduleToCloseTimeout,
		RetryPolicy:                         activityConfig.RetryPolicy,
	}
	if s.globalVersioner.UsesTypedSubFlowStartFailures() {
		options.NonRetryableErrorTypes = []string{interfaces.SubFlowStartRejectionErrorType}
	}
	return options
}

func (s *SubFlowStarter) isAllDone() bool {
	for _, done := range s.doneByIdx {
		if !done {
			return false
		}
	}
	return true
}
