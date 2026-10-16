// Legacy Materials in this file remain under their original licenses.
// See LEGACY_NOTICES.md.

// Modifications Copyright (c) 2026 Super Durable, Inc.
//
// Modifications after the Legacy Cutoff are licensed under the
// Sustainable Use License 1.0.
// Legacy Materials remain under their original licenses.
// See LICENSE and LEGACY_NOTICES.md.

package timers

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/superdurable/dex/gen/dexpb"
	"github.com/superdurable/dex/service/interpreter/interfaces"
)

type fakeWorkflowProvider struct {
	nowUnixSeconds int64
	awaitErr       error
}

func (p *fakeWorkflowProvider) NewFlowError(dexpb.FlowErrorType, string) error {
	return nil
}

func (p *fakeWorkflowProvider) NewFlowErrorFromActivityError(error) error {
	return nil
}

func (p *fakeWorkflowProvider) NewCanceledError(string) error { return nil }

func (p *fakeWorkflowProvider) NewUpdateError(dexpb.UpdateErrorType, string) error {
	return nil
}

func (p *fakeWorkflowProvider) IsApplicationError(error) bool {
	return false
}

func (p *fakeWorkflowProvider) IsSubFlowStartRejection(error) (string, bool, error) {
	return "", false, nil
}

func (p *fakeWorkflowProvider) MapToRecoveryError(error) (*dexpb.RecoveryErrorInfo, error) {
	return nil, nil
}

func (p *fakeWorkflowProvider) MapToFlowResultError(error) (dexpb.FlowErrorType, *dexpb.RecoveryErrorInfo, error) {
	return dexpb.FlowErrorType_FLOW_ERROR_TYPE_UNSPECIFIED, nil, nil
}

func (p *fakeWorkflowProvider) IsCanceledError(error) bool {
	return false
}

func (p *fakeWorkflowProvider) IsContinueAsNewError(error) bool {
	return false
}

func (p *fakeWorkflowProvider) NewDisconnectedContext(ctx interfaces.UnifiedContext) interfaces.UnifiedContext {
	return ctx
}

func (p *fakeWorkflowProvider) WithCancel(
	ctx interfaces.UnifiedContext,
) (interfaces.UnifiedContext, func()) {
	return ctx, func() {}
}

func (p *fakeWorkflowProvider) GetWorkflowInfo(interfaces.UnifiedContext) interfaces.WorkflowInfo {
	return interfaces.WorkflowInfo{}
}

func (p *fakeWorkflowProvider) GetSearchAttributeKeyword(
	interfaces.UnifiedContext,
	string,
) (string, error) {
	return "", nil
}

func (p *fakeWorkflowProvider) GetSearchAttributeKeywordArray(
	interfaces.UnifiedContext,
	string,
) ([]string, error) {
	return nil, nil
}

func (p *fakeWorkflowProvider) UpsertSearchAttributes(
	interfaces.UnifiedContext,
	map[string]interface{},
) error {
	return nil
}

func (p *fakeWorkflowProvider) SetQueryHandler(
	interfaces.UnifiedContext,
	string,
	interface{},
) error {
	return nil
}

func (p *fakeWorkflowProvider) SetInvokeRPCUpdateHandler(
	interfaces.UnifiedContext,
	interfaces.InvokeRPCUpdateValidator,
	interfaces.InvokeRPCUpdateHandler,
) error {
	return nil
}

func (p *fakeWorkflowProvider) SetDeleteChannelMessageUpdateHandler(
	interfaces.UnifiedContext,
	interfaces.DeleteChannelMessageUpdateValidator,
	interfaces.DeleteChannelMessageUpdateHandler,
) error {
	return nil
}

func (p *fakeWorkflowProvider) SetWaitForStepCompletionUpdateHandler(
	interfaces.UnifiedContext,
	interfaces.WaitForStepCompletionUpdateValidator,
	interfaces.WaitForStepCompletionUpdateHandler,
) error {
	return nil
}

func (p *fakeWorkflowProvider) SetWaitForAttributeUpdateHandler(
	interfaces.UnifiedContext,
	interfaces.WaitForAttributeUpdateValidator,
	interfaces.WaitForAttributeUpdateHandler,
) error {
	return nil
}

func (p *fakeWorkflowProvider) ExtendContextWithValue(
	ctx interfaces.UnifiedContext,
	_ string,
	_ interface{},
) interfaces.UnifiedContext {
	return ctx
}

func (p *fakeWorkflowProvider) GoNamed(
	_ interfaces.UnifiedContext,
	_ string,
	run func(interfaces.UnifiedContext),
) {
	run(nil)
}

func (p *fakeWorkflowProvider) GetThreadCount() int {
	return 0
}

func (p *fakeWorkflowProvider) GetPendingThreadNames() map[string]int {
	return nil
}

func (p *fakeWorkflowProvider) Await(
	_ interfaces.UnifiedContext,
	_ func() bool,
) error {
	return p.awaitErr
}

func (p *fakeWorkflowProvider) WithActivityOptions(
	ctx interfaces.UnifiedContext,
	_ interfaces.ActivityOptions,
) interfaces.UnifiedContext {
	return ctx
}

func (p *fakeWorkflowProvider) ExecuteActivity(
	interface{},
	dexpb.StepDurability,
	interfaces.UnifiedContext,
	interface{},
	interface{},
	interface{},
) error {
	return nil
}

func (p *fakeWorkflowProvider) ExecuteLocalActivity(
	interface{},
	interfaces.UnifiedContext,
	interface{},
	...interface{},
) error {
	return nil
}

func (p *fakeWorkflowProvider) Now(interfaces.UnifiedContext) time.Time {
	return time.Unix(p.nowUnixSeconds, 0)
}

func (p *fakeWorkflowProvider) IsReplaying(interfaces.UnifiedContext) bool {
	return false
}

func (p *fakeWorkflowProvider) Sleep(interfaces.UnifiedContext, time.Duration) error {
	return nil
}

func (p *fakeWorkflowProvider) NewTimer(
	interfaces.UnifiedContext,
	time.Duration,
) interfaces.Future {
	return nil
}

func (p *fakeWorkflowProvider) GetSignalChannel(
	interfaces.UnifiedContext,
	string,
) interfaces.ReceiveChannel {
	return nil
}

func (p *fakeWorkflowProvider) GetContextValue(
	interfaces.UnifiedContext,
	string,
) interface{} {
	return nil
}

func (p *fakeWorkflowProvider) GetVersion(
	interfaces.UnifiedContext,
	string,
	int,
	int,
) int {
	return 0
}

func (p *fakeWorkflowProvider) GetLogger(
	interfaces.UnifiedContext,
) interfaces.UnifiedLogger {
	return fakeLogger{}
}

func (p *fakeWorkflowProvider) NewInterpreterContinueAsNewError(
	interfaces.UnifiedContext,
	*dexpb.InterpreterWorkflowInput,
) error {
	return nil
}

type fakeLogger struct{}

func (fakeLogger) Debug(string, ...interface{}) {}
func (fakeLogger) Info(string, ...interface{})  {}
func (fakeLogger) Warn(string, ...interface{})  {}
func (fakeLogger) Error(string, ...interface{}) {}

func TestPruneToNextTimer_PrunesCorrectly_WithTwoScheduled(t *testing.T) {
	timerScheduler := &timerScheduler{
		pendingScheduling: []*dexpb.TimerInfo{
			{FiringUnixTimestampSeconds: 1751395615, Status: dexpb.InternalTimerStatus_INTERNAL_TIMER_STATUS_PENDING},
			{FiringUnixTimestampSeconds: 1751395955, Status: dexpb.InternalTimerStatus_INTERNAL_TIMER_STATUS_PENDING},
			{FiringUnixTimestampSeconds: 1751395755, Status: dexpb.InternalTimerStatus_INTERNAL_TIMER_STATUS_PENDING},
			{FiringUnixTimestampSeconds: 1751395555, Status: dexpb.InternalTimerStatus_INTERNAL_TIMER_STATUS_PENDING},
		},
		providerScheduledTimerUnixTs: []int64{1751395955, 1751395555},
	}

	pruned := timerScheduler.pruneToNextTimer(1751395755)
	assert.NotNil(t, pruned)
	assert.Equal(t, int64(1751395955), pruned.GetFiringUnixTimestampSeconds())
	assert.Equal(t, []int64{1751395955}, timerScheduler.providerScheduledTimerUnixTs)
	if assert.Equal(t, 2, len(timerScheduler.pendingScheduling)) {
		assert.Equal(t, int64(1751395615), timerScheduler.pendingScheduling[0].GetFiringUnixTimestampSeconds())
		assert.Equal(t, int64(1751395955), timerScheduler.pendingScheduling[1].GetFiringUnixTimestampSeconds())
	}
}

func TestPruneToNextTimer_PrunesCorrectly_WithOneScheduled(t *testing.T) {
	timerScheduler := &timerScheduler{
		pendingScheduling: []*dexpb.TimerInfo{
			{FiringUnixTimestampSeconds: 1751395615, Status: dexpb.InternalTimerStatus_INTERNAL_TIMER_STATUS_PENDING},
			{FiringUnixTimestampSeconds: 1751395955, Status: dexpb.InternalTimerStatus_INTERNAL_TIMER_STATUS_PENDING},
			{FiringUnixTimestampSeconds: 1751395755, Status: dexpb.InternalTimerStatus_INTERNAL_TIMER_STATUS_PENDING},
			{FiringUnixTimestampSeconds: 1751395555, Status: dexpb.InternalTimerStatus_INTERNAL_TIMER_STATUS_PENDING},
		},
		providerScheduledTimerUnixTs: []int64{1751395555},
	}

	pruned := timerScheduler.pruneToNextTimer(1751395755)
	assert.NotNil(t, pruned)
	assert.Equal(t, int64(1751395955), pruned.GetFiringUnixTimestampSeconds())
	assert.Equal(t, []int64(nil), timerScheduler.providerScheduledTimerUnixTs)
	if assert.Equal(t, 2, len(timerScheduler.pendingScheduling)) {
		assert.Equal(t, int64(1751395615), timerScheduler.pendingScheduling[0].GetFiringUnixTimestampSeconds())
		assert.Equal(t, int64(1751395955), timerScheduler.pendingScheduling[1].GetFiringUnixTimestampSeconds())
	}
}

func TestStartGreedyTimerScheduler_AwaitErrorBreaksLoop(t *testing.T) {
	provider := &fakeWorkflowProvider{
		awaitErr:       errors.New("test error"),
		nowUnixSeconds: 100,
	}
	_ = startGreedyTimerScheduler(nil, provider, nil)
}
