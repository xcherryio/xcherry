// Legacy Materials in this file remain under their original licenses.
// See LEGACY_NOTICES.md.

// Modifications Copyright (c) 2026 Super Durable, Inc.
//
// Modifications after the Legacy Cutoff are licensed under the
// Sustainable Use License 1.0.
// Legacy Materials remain under their original licenses.
// See LICENSE and LEGACY_NOTICES.md.

package replayTests

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	dexconverter "github.com/superdurable/dex/service/common/converter"
	"github.com/superdurable/dex/service/interpreter/temporal"

	"go.temporal.io/sdk/worker"
)

// gRPC-era Temporal histories recaptured after the interpreter rewrite.
// Global versioning restarted at v1; do not keep pre-rewrite version baselines.
var jsonHistoryFiles = []string{
	"v1-pull-sticky-continue-as-new-CAN1.json",
	"v1-pull-sticky-continue-as-new-wf-finish.json",
	"rt-pr1-single-basic-CAN1.json",
	"rt-pr1-single-basic-finish.json",
	"rt-pr1-single-subflow-parent-reset.json",
	"rt-pr1-single-subflow-child.json",
	"v1-persistence.json",
	"v1-persistence-continue-as-new-CAN1.json",
	"v1-persistence-continue-as-new-CAN2.json",
	"v1-persistence-continue-as-new-wf-finish.json",
	"v1-basic.json",
	"v1-basic-disable-system-searchattributes.json",
	"v1-basic-continue-as-new-CAN1.json",
	"v1-basic-continue-as-new-wf-finish.json",
	"v1-any-timer-signal.json",
	"v1-any-timer-signal-continue-as-new-CAN1.json",
	"v1-any-timer-signal-continue-as-new-CAN2.json",
	"v1-any-timer-signal-continue-as-new-wf-finish.json",
	"v1-skip-start.json",
	"v1-bug-no-state-stuck.json",
	"v1-continue-as-new-on-no-state.json",
	"v1-yield-on-conditional-complete.json",
	"v1-yield-on-conditional-complete-continue-as-new-CAN1.json",
	"v1-yield-on-conditional-complete-continue-as-new-CAN2.json",
	"v1-yield-on-conditional-complete-continue-as-new-wf-finish.json",
	"v1-activity-for-sync-updates-rpcs.json",
	"v1-activity-for-sync-updates-rpcs-continue-as-new-CAN1.json",
	"v1-activity-for-sync-updates-rpcs-continue-as-new-CAN2.json",
	"v1-activity-for-sync-updates-rpcs-continue-as-new-CAN3.json",
	"v1-activity-for-sync-updates-rpcs-continue-as-new-CAN4.json",
	"v1-activity-for-sync-updates-rpcs-continue-as-new-wf-finish.json",
	"v1-locking-sync-update.json",
	"v1-command-thread-completion.json",
	"v1-command-thread-completion-CAN1.json",
	"v1-command-thread-completion-CAN2.json",
	"v1-command-thread-completion-CAN3.json",
	"v1-command-thread-completion-wf-finish.json",
	"v1-any-command-thread-completion-CAN1.json",
	"v1-any-command-thread-completion-wf-finish.json",
	"v1-wait-for-step-completion.json",
	"v1-wait-for-step-completion-by-step-type.json",
	"v1-wait-for-attribute.json",
	"v1-wait-for-attribute-timeout.json",
	"v1-signal.json",
	"v1-signal-continue-as-new-CAN1.json",
	"v1-signal-continue-as-new-CAN2.json",
	"v1-signal-continue-as-new-CAN3.json",
	"v1-signal-continue-as-new-CAN4.json",
	"v1-signal-continue-as-new-CAN5.json",
	"v1-signal-continue-as-new-CAN6.json",
	"v1-signal-continue-as-new-CAN7.json",
	"v1-signal-continue-as-new-CAN8.json",
	"v1-signal-continue-as-new-CAN9.json",
	"v1-signal-continue-as-new-CAN10.json",
	"v1-signal-continue-as-new-CAN11.json",
	"v1-signal-continue-as-new-CAN12.json",
	"v1-signal-continue-as-new-CAN13.json",
	"v1-signal-continue-as-new-CAN14.json",
	"v1-signal-continue-as-new-wf-finish.json",
}

func TestTemporalReplay(t *testing.T) {
	worker.EnableVerboseLogging(true)

	replayer, err := worker.NewWorkflowReplayerWithOptions(
		worker.WorkflowReplayerOptions{
			EnableLoggingInReplay: true,
			DataConverter:         dexconverter.NewTemporalDataConverter(),
		})
	require.NoError(t, err)

	replayer.RegisterWorkflow(temporal.NewWorkerForReplay().Engine)

	for _, f := range jsonHistoryFiles {
		err := replayer.ReplayWorkflowHistoryFromJSONFile(nil, "history/"+f)
		assert.Nil(t, err, "fail at replay history for: "+f)
	}
}
