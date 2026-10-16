// Legacy Materials in this file remain under their original licenses.
// See LEGACY_NOTICES.md.

// Modifications Copyright (c) 2026 Super Durable, Inc.
//
// Modifications after the Legacy Cutoff are licensed under the
// Sustainable Use License 1.0.
// Legacy Materials remain under their original licenses.
// See LICENSE and LEGACY_NOTICES.md.

package interpreter

import (
	"github.com/superdurable/dex/service/interpreter/interfaces"
)

const globalChangeId = "global"

// StartingVersionV1 is the reset baseline for the rewritten interpreter. All
// historical version branches were removed; running flows always execute the
// latest behavior. Future determinism-affecting changes add a new constant here
// and gate on it via GlobalVersioner.
const StartingVersionV1 = 1

const DeterministicStepActivityIDVersion = 2

const TimerlessWaitUpdatesVersion = 3

const SplitWaitForAttributeTimeoutSemanticsVersion = 4

const TypedSubFlowStartFailureVersion = 5

const MaxOfAllVersions = TypedSubFlowStartFailureVersion

// GlobalVersioner is the forward hook for determinism-safe interpreter changes.
// See https://stackoverflow.com/questions/73941723 for the pattern.
type GlobalVersioner struct {
	version int
}

func NewGlobalVersioner(
	workflowProvider interfaces.WorkflowProvider, ctx interfaces.UnifiedContext,
) *GlobalVersioner {
	version := workflowProvider.GetVersion(ctx, globalChangeId, 0, MaxOfAllVersions)
	return &GlobalVersioner{version: version}
}

func (v *GlobalVersioner) UsesDeterministicStepActivityIDs() bool {
	return v.version >= DeterministicStepActivityIDVersion
}

func (v *GlobalVersioner) UsesTimerlessWaitUpdates() bool {
	return v.version >= TimerlessWaitUpdatesVersion
}

func (v *GlobalVersioner) UsesSplitWaitForAttributeTimeoutSemantics() bool {
	return v.version >= SplitWaitForAttributeTimeoutSemanticsVersion
}

func (v *GlobalVersioner) UsesTypedSubFlowStartFailures() bool {
	return v.version >= TypedSubFlowStartFailureVersion
}
