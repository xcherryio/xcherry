// Legacy Materials in this file remain under their original licenses.
// See LEGACY_NOTICES.md.

// Modifications Copyright (c) 2026 Super Durable, Inc.
//
// Modifications after the Legacy Cutoff are licensed under the
// Sustainable Use License 1.0.
// Legacy Materials remain under their original licenses.
// See LICENSE and LEGACY_NOTICES.md.

package service

import (
	"github.com/superdurable/dex/gen/dexpb"
)

// BasicInfo contains non-serialized flow identity.
type BasicInfo struct {
	FlowType            string
	RunStartedTimestamp int64
}

// StepExecutionStatus is the interpreter's internal per-step-execution outcome.
// It is not serialized to history and has no proto equivalent.
type StepExecutionStatus string

const StepExecutionStatusCompleted StepExecutionStatus = "Completed"
const StepExecutionStatusFailedNoProceed StepExecutionStatus = "Failure"
const StepExecutionStatusInternalError StepExecutionStatus = "InternalError"
const StepExecutionStatusWaitingAborted StepExecutionStatus = "WaitingConditions"
const StepExecutionStatusFailedAndProceed StepExecutionStatus = "ExecuteMethodFailedAndProceed"
const StepExecutionStatusFlowTimeout StepExecutionStatus = "FlowTimeout"

// ValidateTimerSkipRequest validates a pending timer by condition ID or index.
func ValidateTimerSkipRequest(
	timerInfos []*dexpb.TimerInfo,
	timerConditionId string,
	timerConditionIndex int,
) (*dexpb.TimerInfo, bool) {
	if timerConditionId != "" {
		for _, timerInfo := range timerInfos {
			if timerInfo.GetConditionId() == timerConditionId &&
				timerInfo.GetStatus() == dexpb.InternalTimerStatus_INTERNAL_TIMER_STATUS_PENDING {
				return timerInfo, true
			}
		}
		return nil, false
	}
	if timerConditionIndex >= 0 && timerConditionIndex < len(timerInfos) {
		timerInfo := timerInfos[timerConditionIndex]
		if timerInfo.GetStatus() == dexpb.InternalTimerStatus_INTERNAL_TIMER_STATUS_PENDING {
			return timerInfo, true
		}
	}
	return nil, false
}
