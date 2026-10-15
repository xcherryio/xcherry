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
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/superdurable/dex/config"
	"github.com/superdurable/dex/gen/dexpb"
	"github.com/superdurable/dex/service"
	commonlog "github.com/superdurable/dex/service/common/log"
	"github.com/superdurable/dex/service/common/ptr"
	"github.com/superdurable/dex/service/common/streamstore"
	"github.com/superdurable/dex/service/interpreter/interfaces"
	"go.temporal.io/sdk/client"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type waitForOutputSequence struct {
	outputs []*dexpb.InvokeWaitForMethodOutput
}

func (sequence *waitForOutputSequence) Recv() (*dexpb.InvokeWaitForMethodOutput, error) {
	if len(sequence.outputs) == 0 {
		return nil, io.EOF
	}
	output := sequence.outputs[0]
	sequence.outputs = sequence.outputs[1:]
	return output, nil
}

type executeOutputSequence struct {
	outputs []*dexpb.InvokeExecuteMethodOutput
}

func (sequence *executeOutputSequence) Recv() (*dexpb.InvokeExecuteMethodOutput, error) {
	if len(sequence.outputs) == 0 {
		return nil, io.EOF
	}
	output := sequence.outputs[0]
	sequence.outputs = sequence.outputs[1:]
	return output, nil
}

type discardUnifiedLogger struct{}

func (discardUnifiedLogger) Debug(string, ...interface{}) {}
func (discardUnifiedLogger) Info(string, ...interface{})  {}
func (discardUnifiedLogger) Warn(string, ...interface{})  {}
func (discardUnifiedLogger) Error(string, ...interface{}) {}

type capturedMetric struct {
	name  string
	tags  map[string]string
	value int64
}

type capturingMetricsHandler struct {
	tags    map[string]string
	metrics *[]capturedMetric
}

func newCapturingMetricsHandler() (*capturingMetricsHandler, *[]capturedMetric) {
	metrics := &[]capturedMetric{}
	return &capturingMetricsHandler{metrics: metrics}, metrics
}

func (handler *capturingMetricsHandler) WithTags(tags map[string]string) client.MetricsHandler {
	combined := make(map[string]string, len(handler.tags)+len(tags))
	for key, value := range handler.tags {
		combined[key] = value
	}
	for key, value := range tags {
		combined[key] = value
	}
	return &capturingMetricsHandler{tags: combined, metrics: handler.metrics}
}

func (handler *capturingMetricsHandler) Counter(name string) client.MetricsCounter {
	return &capturingCounter{name: name, tags: handler.tags, metrics: handler.metrics}
}

func (handler *capturingMetricsHandler) Gauge(string) client.MetricsGauge {
	return discardGauge{}
}

func (handler *capturingMetricsHandler) Timer(string) client.MetricsTimer {
	return discardTimer{}
}

type capturingCounter struct {
	name    string
	tags    map[string]string
	metrics *[]capturedMetric
}

func (counter *capturingCounter) Inc(value int64) {
	*counter.metrics = append(*counter.metrics, capturedMetric{
		name:  counter.name,
		tags:  counter.tags,
		value: value,
	})
}

type discardGauge struct{}

func (discardGauge) Update(float64) {}

type discardTimer struct{}

func (discardTimer) Record(time.Duration) {}

func TestReceiveWaitForMethodClearsHeartbeatValueAndWritesStream(t *testing.T) {
	provider := interfaces.NewMockActivityProvider(gomock.NewController(t))
	heartbeatValue := stringValue("checkpoint")
	provider.EXPECT().RecordHeartbeat(gomock.Any(), heartbeatValue)
	provider.EXPECT().RecordHeartbeat(gomock.Any())
	provider.EXPECT().RecordHeartbeat(gomock.Any())
	store := newActivityStreamStore(t, config.StreamStoreBackendMemory)
	activities := &Activities{
		activityProvider: provider,
		streamStore:      store,
		metrics:          client.MetricsNopHandler,
	}
	request := &dexpb.InvokeWaitForMethodRequest{
		Context: &dexpb.Context{
			FlowId:          "flow",
			RunId:           "run",
			StepExecutionId: "step-1",
		},
		FlowType: "flow-type",
		StepType: "step-type",
	}
	response, err := activities.receiveWaitForMethodResponse(
		context.Background(),
		&waitForOutputSequence{outputs: []*dexpb.InvokeWaitForMethodOutput{
			{Output: &dexpb.InvokeWaitForMethodOutput_Heartbeat{
				Heartbeat: &dexpb.StepMethodHeartbeat{Value: heartbeatValue},
			}},
			{Output: &dexpb.InvokeWaitForMethodOutput_Heartbeat{
				Heartbeat: &dexpb.StepMethodHeartbeat{},
			}},
			{Output: &dexpb.InvokeWaitForMethodOutput_StreamWrite{
				StreamWrite: &dexpb.StepStreamWrite{
					StreamName:          "progress",
					StreamCapacityBytes: 1024,
					Value:               stringValue("message"),
				},
			}},
			{Output: &dexpb.InvokeWaitForMethodOutput_Result{
				Result: &dexpb.InvokeWaitForMethodResponse{},
			}},
		}},
		request,
		interfaces.ActivityInfo{},
		discardUnifiedLogger{},
	)
	require.NoError(t, err)
	require.NotNil(t, response)
	readCtx, cancelRead := context.WithTimeout(context.Background(), time.Second)
	defer cancelRead()
	message, err := store.Read(readCtx, "flow-type", "flow", "progress", "")
	require.NoError(t, err)
	require.Equal(t, "message", message.Value.GetStringValue())
	require.Equal(t, "#step-1", message.Source)
}

func TestReceiveExecuteMethodLocalIgnoresHeartbeatAndWritesStream(t *testing.T) {
	provider := interfaces.NewMockActivityProvider(gomock.NewController(t))
	store := newActivityStreamStore(t, config.StreamStoreBackendMemory)
	activities := &Activities{
		activityProvider: provider,
		streamStore:      store,
		metrics:          client.MetricsNopHandler,
	}
	request := &dexpb.InvokeExecuteMethodRequest{
		Context: &dexpb.Context{
			FlowId:          "flow",
			RunId:           "run",
			StepExecutionId: "step-1",
		},
		FlowType: "flow-type",
		StepType: "step-type",
	}
	response, err := activities.receiveExecuteMethodResponse(
		context.Background(),
		&executeOutputSequence{outputs: []*dexpb.InvokeExecuteMethodOutput{
			{Output: &dexpb.InvokeExecuteMethodOutput_Heartbeat{
				Heartbeat: &dexpb.StepMethodHeartbeat{Value: stringValue("ignored")},
			}},
			{Output: &dexpb.InvokeExecuteMethodOutput_StreamWrite{
				StreamWrite: &dexpb.StepStreamWrite{
					StreamName:          "progress",
					StreamCapacityBytes: 1024,
					Value:               stringValue("local-message"),
				},
			}},
			{Output: &dexpb.InvokeExecuteMethodOutput_Result{
				Result: &dexpb.InvokeExecuteMethodResponse{},
			}},
		}},
		request,
		interfaces.ActivityInfo{IsLocalActivity: true},
		discardUnifiedLogger{},
	)
	require.NoError(t, err)
	require.NotNil(t, response)
	readCtx, cancelRead := context.WithTimeout(context.Background(), time.Second)
	defer cancelRead()
	message, err := store.Read(readCtx, "flow-type", "flow", "progress", "")
	require.NoError(t, err)
	require.Equal(t, "local-message", message.Value.GetStringValue())
}

func TestReceiveExecuteMethodRejectsInvalidResultProtocol(t *testing.T) {
	result := &dexpb.InvokeExecuteMethodOutput{
		Output: &dexpb.InvokeExecuteMethodOutput_Result{
			Result: &dexpb.InvokeExecuteMethodResponse{},
		},
	}
	testCases := []struct {
		name          string
		outputs       []*dexpb.InvokeExecuteMethodOutput
		errorContains string
	}{
		{name: "missing_result", errorContains: "without a result"},
		{name: "duplicate_result", outputs: []*dexpb.InvokeExecuteMethodOutput{result, result}, errorContains: "after its result"},
		{name: "frame_after_result", outputs: []*dexpb.InvokeExecuteMethodOutput{
			result,
			{Output: &dexpb.InvokeExecuteMethodOutput_Heartbeat{Heartbeat: &dexpb.StepMethodHeartbeat{}}},
		}, errorContains: "after its result"},
		{name: "empty_output", outputs: []*dexpb.InvokeExecuteMethodOutput{{}}, errorContains: "empty output"},
	}
	activities := &Activities{}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := activities.receiveExecuteMethodResponse(
				context.Background(),
				&executeOutputSequence{outputs: testCase.outputs},
				&dexpb.InvokeExecuteMethodRequest{Context: &dexpb.Context{}},
				interfaces.ActivityInfo{IsLocalActivity: true},
				discardUnifiedLogger{},
			)
			require.ErrorContains(t, err, testCase.errorContains)
		})
	}
}

func TestReceiveExecuteMethodDropsDisabledStreamWrite(t *testing.T) {
	store := newActivityStreamStore(t, config.StreamStoreBackendDisabled)
	metricsHandler, capturedMetrics := newCapturingMetricsHandler()
	activities := &Activities{
		streamStore: store,
		metrics:     metricsHandler,
	}
	response, err := activities.receiveExecuteMethodResponse(
		context.Background(),
		&executeOutputSequence{outputs: []*dexpb.InvokeExecuteMethodOutput{
			{Output: &dexpb.InvokeExecuteMethodOutput_StreamWrite{
				StreamWrite: &dexpb.StepStreamWrite{
					StreamName:          "progress",
					StreamCapacityBytes: 1024,
					Value:               stringValue("dropped"),
				},
			}},
			{Output: &dexpb.InvokeExecuteMethodOutput_Result{
				Result: &dexpb.InvokeExecuteMethodResponse{},
			}},
		}},
		&dexpb.InvokeExecuteMethodRequest{
			Context:  &dexpb.Context{FlowId: "flow", RunId: "run", StepExecutionId: "step"},
			FlowType: "flow-type",
			StepType: "step-type",
		},
		interfaces.ActivityInfo{IsLocalActivity: true},
		discardUnifiedLogger{},
	)
	require.NoError(t, err)
	require.NotNil(t, response)
	require.Equal(t, &[]capturedMetric{{
		name: "dex_step_stream_write_failure",
		tags: map[string]string{
			"flow_type":   "flow-type",
			"step_type":   "step-type",
			"step_method": "execute",
			"reason":      codes.FailedPrecondition.String(),
		},
		value: 1,
	}}, capturedMetrics)
}

func newActivityStreamStore(t *testing.T, backend config.StreamStoreBackend) *streamstore.Store {
	t.Helper()
	store, err := streamstore.New(&config.StreamStoreConfig{Backend: backend}, commonlog.NewNoop())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	return store
}

func TestActivityInvocationIdStableAcrossRetries(t *testing.T) {
	activityInfo := interfaces.ActivityInfo{
		ActivityID: "activity-456",
		Attempt:    1,
		WorkflowExecution: interfaces.WorkflowExecution{
			ID:    "flow-123",
			RunID: "run-123",
		},
	}
	firstInvocationId := activityInvocationId(activityInfo)
	activityInfo.Attempt = 7
	retryInvocationId := activityInvocationId(activityInfo)

	require.Equal(t, firstInvocationId, retryInvocationId)
}

func TestStepActivityOptionsDefaultsAndOverrides(t *testing.T) {
	defaults := stepActivityOptions(nil, 0, 0)
	require.Equal(t, 2*time.Hour, defaults.StartToCloseTimeout)
	require.Equal(t, time.Minute, defaults.HeartbeatTimeout)
	require.Equal(t, 4*time.Hour, defaults.RetryPolicy.TotalDuration)

	overrides := stepActivityOptions(
		&dexpb.RetryPolicy{TotalDurationSeconds: 8 * 60 * 60},
		3*60*60,
		10,
	)
	require.Equal(t, 3*time.Hour, overrides.StartToCloseTimeout)
	require.Equal(t, 10*time.Second, overrides.HeartbeatTimeout)
	require.Equal(t, 8*time.Hour, overrides.RetryPolicy.TotalDuration)
}

func TestInvalidAnyConditionCombination(t *testing.T) {
	timers, channels := createConditions()
	waiting := &dexpb.WaitingCondition{
		WaitingConditionType: dexpb.WaitingConditionType_WAITING_CONDITION_TYPE_ANY_COMBINATION_COMPLETED,
		TimerConditions:      timers,
		ChannelConditions:    channels,
		ConditionCombinations: []*dexpb.ConditionCombination{
			{ConditionIds: []string{"timer-cmd1", "signal-cmd1"}},
			{ConditionIds: []string{"timer-cmd1", "invalid"}},
		},
	}

	err := validateWaitingCondition(waiting, config.DefaultMinimumStepHeartbeatTimeout)
	require.Error(t, err)
	require.ErrorContains(t, err, `references undeclared condition_id "invalid"`)
}

func TestValidAnyConditionCombination(t *testing.T) {
	timers, channels := createConditions()
	waiting := &dexpb.WaitingCondition{
		WaitingConditionType: dexpb.WaitingConditionType_WAITING_CONDITION_TYPE_ANY_COMBINATION_COMPLETED,
		TimerConditions:      timers,
		ChannelConditions:    channels,
		ConditionCombinations: []*dexpb.ConditionCombination{
			{ConditionIds: []string{"timer-cmd1", "signal-cmd1"}},
			{ConditionIds: []string{"timer-cmd1", "internal-cmd1"}},
		},
	}

	require.NoError(t, validateWaitingCondition(waiting, config.DefaultMinimumStepHeartbeatTimeout))
}

func TestValidateWaitingConditionChannelBounds(t *testing.T) {
	waiting := &dexpb.WaitingCondition{
		WaitingConditionType: dexpb.WaitingConditionType_WAITING_CONDITION_TYPE_ALL_COMPLETED,
		ChannelConditions: []*dexpb.ChannelCondition{
			{ConditionId: "c1", ChannelName: "ch", AtLeast: ptr.Any(int32(2)), AtMost: ptr.Any(int32(1))},
		},
	}
	require.Error(t, validateWaitingCondition(waiting, config.DefaultMinimumStepHeartbeatTimeout))
}

func TestValidateWaitingConditionRejections(t *testing.T) {
	duplicateId := newValidWaitingCondition()
	duplicateId.TimerConditions = []*dexpb.TimerCondition{{ConditionId: "condition"}}

	negativeTimer := newValidWaitingCondition()
	negativeTimer.TimerConditions = []*dexpb.TimerCondition{{
		ConditionId:     "timer",
		DurationSeconds: -1,
	}}

	absoluteTimer := newValidWaitingCondition()
	absoluteTimer.TimerConditions = []*dexpb.TimerCondition{{
		ConditionId:                "timer",
		FiringUnixTimestampSeconds: 10,
	}}

	combinationsOnAll := newValidWaitingCondition()
	combinationsOnAll.WaitingConditionType =
		dexpb.WaitingConditionType_WAITING_CONDITION_TYPE_ALL_COMPLETED
	combinationsOnAll.ConditionCombinations = []*dexpb.ConditionCombination{{
		ConditionIds: []string{"condition"},
	}}

	missingCombination := newValidWaitingCondition()
	missingCombination.WaitingConditionType =
		dexpb.WaitingConditionType_WAITING_CONDITION_TYPE_ANY_COMBINATION_COMPLETED

	unknownType := newValidWaitingCondition()
	unknownType.WaitingConditionType =
		dexpb.WaitingConditionType_WAITING_CONDITION_TYPE_UNSPECIFIED

	emptyConditionId := waitingConditionWithChannel(newChannelCondition("", "channel", nil, nil))
	emptyConditionId.WaitingConditionType =
		dexpb.WaitingConditionType_WAITING_CONDITION_TYPE_ANY_COMBINATION_COMPLETED
	emptyConditionId.ConditionCombinations = []*dexpb.ConditionCombination{{
		ConditionIds: []string{"condition"},
	}}

	testCases := []struct {
		name             string
		waitingCondition *dexpb.WaitingCondition
		errorContains    string
	}{
		{"nil_timer_entry", waitingConditionWithTimer(nil), "timer condition at index 0 is nil"},
		{"nil_channel_entry", waitingConditionWithChannel(nil), "channel condition at index 0 is nil"},
		{"empty_condition_id_for_combination", emptyConditionId, "empty condition_id"},
		{"duplicate_condition_id", duplicateId, `duplicate condition_id "condition"`},
		{"empty_channel_name", waitingConditionWithChannel(newChannelCondition("condition", "", nil, nil)), "empty channel_name"},
		{
			"negative_at_least",
			waitingConditionWithChannel(newChannelCondition("condition", "channel", ptr.Any(int32(-1)), nil)),
			"negative at_least",
		},
		{
			"negative_at_most",
			waitingConditionWithChannel(newChannelCondition("condition", "channel", nil, ptr.Any(int32(-1)))),
			"negative at_most",
		},
		{
			"at_most_less_than_at_least",
			waitingConditionWithChannel(
				newChannelCondition("condition", "channel", ptr.Any(int32(3)), ptr.Any(int32(2))),
			),
			"at_most 2 < at_least 3",
		},
		{
			"explicit_zero_at_most_less_than_at_least",
			waitingConditionWithChannel(
				newChannelCondition("condition", "channel", ptr.Any(int32(1)), ptr.Any(int32(0))),
			),
			"at_most 0 < at_least 1",
		},
		{"negative_timer_duration", negativeTimer, "negative duration_seconds"},
		{"worker_sets_absolute_timer", absoluteTimer, "server-owned firing_unix_timestamp_seconds"},
		{"combinations_on_all", combinationsOnAll, "only valid for ANY_COMBINATION_COMPLETED"},
		{"any_combination_requires_combination", missingCombination, "requires at least one condition_combination"},
		{
			"empty_combination",
			&dexpb.WaitingCondition{
				WaitingConditionType: dexpb.WaitingConditionType_WAITING_CONDITION_TYPE_ANY_COMBINATION_COMPLETED,
				ChannelConditions:    []*dexpb.ChannelCondition{newChannelCondition("condition", "channel", nil, nil)},
				ConditionCombinations: []*dexpb.ConditionCombination{
					{},
				},
			},
			"condition_combination at index 0 is empty",
		},
		{
			"combination_references_undeclared_id",
			&dexpb.WaitingCondition{
				WaitingConditionType: dexpb.WaitingConditionType_WAITING_CONDITION_TYPE_ANY_COMBINATION_COMPLETED,
				ChannelConditions:    []*dexpb.ChannelCondition{newChannelCondition("condition", "channel", nil, nil)},
				ConditionCombinations: []*dexpb.ConditionCombination{
					{ConditionIds: []string{"undeclared"}},
				},
			},
			`references undeclared condition_id "undeclared"`,
		},
		{
			"combination_duplicate_id",
			&dexpb.WaitingCondition{
				WaitingConditionType: dexpb.WaitingConditionType_WAITING_CONDITION_TYPE_ANY_COMBINATION_COMPLETED,
				ChannelConditions:    []*dexpb.ChannelCondition{newChannelCondition("condition", "channel", nil, nil)},
				ConditionCombinations: []*dexpb.ConditionCombination{
					{ConditionIds: []string{"condition", "condition"}},
				},
			},
			`duplicate condition_id "condition"`,
		},
		{"unknown_type", unknownType, "unknown waiting_condition_type"},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			err := validateWaitingCondition(
				testCase.waitingCondition,
				config.DefaultMinimumStepHeartbeatTimeout,
			)
			require.Error(t, err)
			require.ErrorContains(t, err, testCase.errorContains)
		})
	}
	require.NoError(t, validateWaitingCondition(nil, config.DefaultMinimumStepHeartbeatTimeout))
}

func TestValidateStepDecision(t *testing.T) {
	validConditional := decisionWithClose(
		dexpb.CloseDecisionType_CLOSE_DECISION_TYPE_FORCE_COMPLETE_ON_CHANNELS_EMPTY,
	)
	validConditional.NextSteps = []*dexpb.StepMovement{{StepType: "next"}}
	validConditional.CloseDecision.ConditionalChannelNames = []string{"channel"}

	conditionalWithoutNext := decisionWithClose(
		dexpb.CloseDecisionType_CLOSE_DECISION_TYPE_FORCE_COMPLETE_ON_CHANNELS_EMPTY,
	)
	conditionalWithoutNext.CloseDecision.ConditionalChannelNames = []string{"channel"}

	conditionalWithoutChannels := decisionWithClose(
		dexpb.CloseDecisionType_CLOSE_DECISION_TYPE_FORCE_COMPLETE_ON_CHANNELS_EMPTY,
	)
	conditionalWithoutChannels.NextSteps = []*dexpb.StepMovement{{StepType: "next"}}

	conditionalWithDuplicateChannels := decisionWithClose(
		dexpb.CloseDecisionType_CLOSE_DECISION_TYPE_FORCE_COMPLETE_ON_CHANNELS_EMPTY,
	)
	conditionalWithDuplicateChannels.NextSteps = []*dexpb.StepMovement{{StepType: "next"}}
	conditionalWithDuplicateChannels.CloseDecision.ConditionalChannelNames = []string{"channel", "channel"}

	conditionalWithEmptyChannel := decisionWithClose(
		dexpb.CloseDecisionType_CLOSE_DECISION_TYPE_FORCE_COMPLETE_ON_CHANNELS_EMPTY,
	)
	conditionalWithEmptyChannel.NextSteps = []*dexpb.StepMovement{{StepType: "next"}}
	conditionalWithEmptyChannel.CloseDecision.ConditionalChannelNames = []string{""}

	gracefulWithNext := decisionWithClose(
		dexpb.CloseDecisionType_CLOSE_DECISION_TYPE_GRACEFUL_COMPLETE,
	)
	gracefulWithNext.NextSteps = []*dexpb.StepMovement{{StepType: "next"}}

	gracefulWithChannels := decisionWithClose(
		dexpb.CloseDecisionType_CLOSE_DECISION_TYPE_GRACEFUL_COMPLETE,
	)
	gracefulWithChannels.CloseDecision.ConditionalChannelNames = []string{"channel"}

	forceFailWithString := decisionWithClose(
		dexpb.CloseDecisionType_CLOSE_DECISION_TYPE_FORCE_FAIL,
	)
	forceFailWithString.CloseDecision.CloseInput = &dexpb.Value{
		Kind: &dexpb.Value_StringValue{StringValue: "detail"},
	}

	forceFailWithInt := decisionWithClose(
		dexpb.CloseDecisionType_CLOSE_DECISION_TYPE_FORCE_FAIL,
	)
	forceFailWithInt.CloseDecision.CloseInput = &dexpb.Value{
		Kind: &dexpb.Value_IntValue{IntValue: 1},
	}

	deadEndWithInput := decisionWithClose(
		dexpb.CloseDecisionType_CLOSE_DECISION_TYPE_DEAD_END,
	)
	deadEndWithInput.CloseDecision.CloseInput = &dexpb.Value{
		Kind: &dexpb.Value_StringValue{StringValue: "input"},
	}

	testCases := []struct {
		name          string
		decision      *dexpb.StepDecision
		errorContains string
	}{
		{"nil", nil, "step decision is nil"},
		{"empty", &dexpb.StepDecision{}, "empty step decision"},
		{"nil_next_step", &dexpb.StepDecision{
			NextSteps: []*dexpb.StepMovement{nil},
		}, "next step at index 0 is invalid"},
		{"empty_next_step_type", &dexpb.StepDecision{
			NextSteps: []*dexpb.StepMovement{{}},
		}, "next step at index 0 is invalid"},
		{"next_step", &dexpb.StepDecision{
			NextSteps: []*dexpb.StepMovement{{StepType: "next"}},
		}, ""},
		{"conditional", validConditional, ""},
		{"conditional_without_next", conditionalWithoutNext, "requires at least one next step"},
		{"conditional_without_channels", conditionalWithoutChannels, "requires at least one channel"},
		{"conditional_with_duplicate_channels", conditionalWithDuplicateChannels, "duplicate conditional close channel"},
		{"conditional_with_empty_channel", conditionalWithEmptyChannel, "conditional close channel name is empty"},
		{"graceful", decisionWithClose(
			dexpb.CloseDecisionType_CLOSE_DECISION_TYPE_GRACEFUL_COMPLETE,
		), ""},
		{"graceful_with_next", gracefulWithNext, "cannot be combined with next steps"},
		{"graceful_with_channels", gracefulWithChannels, "require a conditional close decision"},
		{"force_complete", decisionWithClose(
			dexpb.CloseDecisionType_CLOSE_DECISION_TYPE_FORCE_COMPLETE,
		), ""},
		{"force_fail", decisionWithClose(
			dexpb.CloseDecisionType_CLOSE_DECISION_TYPE_FORCE_FAIL,
		), ""},
		{"force_fail_with_string", forceFailWithString, ""},
		{"force_fail_with_int", forceFailWithInt, "must be a string"},
		{"dead_end", decisionWithClose(
			dexpb.CloseDecisionType_CLOSE_DECISION_TYPE_DEAD_END,
		), ""},
		{"dead_end_with_input", deadEndWithInput, "cannot have close input"},
		{"unspecified", decisionWithClose(
			dexpb.CloseDecisionType_CLOSE_DECISION_TYPE_UNSPECIFIED,
		), "close decision type is unspecified"},
		{"unknown", decisionWithClose(
			dexpb.CloseDecisionType(100),
		), "close decision type is unspecified"},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			err := validateStepDecision(
				testCase.decision,
				config.DefaultMinimumStepHeartbeatTimeout,
			)
			if testCase.errorContains == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, testCase.errorContains)
		})
	}
}

func decisionWithClose(closeType dexpb.CloseDecisionType) *dexpb.StepDecision {
	return &dexpb.StepDecision{
		CloseDecision: &dexpb.CloseDecision{
			CloseDecisionType: closeType,
		},
	}
}

func createConditions() ([]*dexpb.TimerCondition, []*dexpb.ChannelCondition) {
	timers := []*dexpb.TimerCondition{
		{ConditionId: "timer-cmd1", DurationSeconds: 86400 * 365},
	}
	channels := []*dexpb.ChannelCondition{
		{ConditionId: "signal-cmd1", ChannelName: "test-signal-name1"},
		{ConditionId: "internal-cmd1", ChannelName: "test-internal-name1"},
	}
	return timers, channels
}

func newValidWaitingCondition() *dexpb.WaitingCondition {
	return waitingConditionWithChannel(newChannelCondition("condition", "channel", nil, nil))
}

func waitingConditionWithTimer(timerCondition *dexpb.TimerCondition) *dexpb.WaitingCondition {
	return &dexpb.WaitingCondition{
		WaitingConditionType: dexpb.WaitingConditionType_WAITING_CONDITION_TYPE_ANY_COMPLETED,
		TimerConditions:      []*dexpb.TimerCondition{timerCondition},
	}
}

func TestValidateWaitingConditionAllowsEmptyIDsForAllAndAny(t *testing.T) {
	waitingConditionTypes := []dexpb.WaitingConditionType{
		dexpb.WaitingConditionType_WAITING_CONDITION_TYPE_ALL_COMPLETED,
		dexpb.WaitingConditionType_WAITING_CONDITION_TYPE_ANY_COMPLETED,
	}
	for _, waitingConditionType := range waitingConditionTypes {
		waitingCondition := &dexpb.WaitingCondition{
			WaitingConditionType: waitingConditionType,
			TimerConditions: []*dexpb.TimerCondition{
				{DurationSeconds: 1},
				{DurationSeconds: 2},
			},
			ChannelConditions: []*dexpb.ChannelCondition{
				newChannelCondition("", "first", nil, nil),
				newChannelCondition("", "second", nil, nil),
			},
		}
		require.NoError(t, validateWaitingCondition(
			waitingCondition,
			config.DefaultMinimumStepHeartbeatTimeout,
		))
	}
}

func waitingConditionWithChannel(channelCondition *dexpb.ChannelCondition) *dexpb.WaitingCondition {
	return &dexpb.WaitingCondition{
		WaitingConditionType: dexpb.WaitingConditionType_WAITING_CONDITION_TYPE_ANY_COMPLETED,
		ChannelConditions:    []*dexpb.ChannelCondition{channelCondition},
	}
}

func newChannelCondition(
	conditionId string,
	channelName string,
	atLeast *int32,
	atMost *int32,
) *dexpb.ChannelCondition {
	return &dexpb.ChannelCondition{
		ConditionId: conditionId,
		ChannelName: channelName,
		AtLeast:     atLeast,
		AtMost:      atMost,
	}
}

func TestNewWorkerActivityFailureUsesInternalForNonGRPCError(t *testing.T) {
	provider := interfaces.NewMockActivityProvider(gomock.NewController(t))
	inputError := errors.New("dial failed")
	expectedError := errors.New("activity error")
	var internalActivityError *dexpb.InternalActivityError
	provider.EXPECT().
		NewActivityError(
			dexpb.FlowErrorType_FLOW_ERROR_TYPE_INTERNAL,
			gomock.Any(),
			int32(0),
		).
		DoAndReturn(func(
			_ dexpb.FlowErrorType,
			activityError *dexpb.InternalActivityError,
			_ int32,
		) error {
			internalActivityError = activityError
			return expectedError
		})

	require.ErrorIs(
		t,
		newWorkerSideActivityError(context.Background(), provider, service.BackendTypeTemporal, inputError, nil),
		expectedError,
	)
	require.Equal(t, "dial failed", internalActivityError.GetServerDetail())
}

func TestNewWorkerActivityFailurePreservesWorkerDetails(t *testing.T) {
	provider := interfaces.NewMockActivityProvider(gomock.NewController(t))
	grpcStatus, err := status.New(codes.Internal, "worker failure").WithDetails(
		&dexpb.WorkerErrorResponse{
			Detail:            "worker detail",
			ErrorType:         "worker type",
			StackTrace:        "worker stack",
			RetryAfterSeconds: 17,
		},
	)
	require.NoError(t, err)

	expectedError := errors.New("activity error")
	var internalActivityError *dexpb.InternalActivityError
	var retryAfterSeconds int32
	provider.EXPECT().
		NewActivityError(
			dexpb.FlowErrorType_FLOW_ERROR_TYPE_WORKER_API_FAIL,
			gomock.Any(),
			int32(17),
		).
		DoAndReturn(func(
			_ dexpb.FlowErrorType,
			activityError *dexpb.InternalActivityError,
			retryAfter int32,
		) error {
			internalActivityError = activityError
			retryAfterSeconds = retryAfter
			return expectedError
		})

	require.ErrorIs(
		t,
		newWorkerSideActivityError(context.Background(), provider, service.BackendTypeTemporal, grpcStatus.Err(), nil),
		expectedError,
	)
	require.Empty(t, internalActivityError.GetServerDetail())
	require.Equal(t, int32(codes.Internal), internalActivityError.GetWorkerGrpcStatus())
	require.Equal(t, "worker detail", internalActivityError.GetWorkerError().GetDetail())
	require.Equal(t, "worker type", internalActivityError.GetWorkerError().GetErrorType())
	require.Equal(t, "worker stack", internalActivityError.GetWorkerError().GetStackTrace())
	require.Equal(t, int32(17), retryAfterSeconds)
}

func TestNewWorkerActivityFailureRejectsRetryAfterOnCadence(t *testing.T) {
	provider := interfaces.NewMockActivityProvider(gomock.NewController(t))
	grpcStatus, err := status.New(codes.Internal, "worker failure").WithDetails(
		&dexpb.WorkerErrorResponse{RetryAfterSeconds: 17},
	)
	require.NoError(t, err)

	expectedError := errors.New("activity error")
	var internalActivityError *dexpb.InternalActivityError
	provider.EXPECT().
		NewActivityError(
			dexpb.FlowErrorType_FLOW_ERROR_TYPE_INVALID_USER_FLOW_CODE,
			gomock.Any(),
			int32(0),
		).
		DoAndReturn(func(
			_ dexpb.FlowErrorType,
			activityError *dexpb.InternalActivityError,
			_ int32,
		) error {
			internalActivityError = activityError
			return expectedError
		})

	require.ErrorIs(
		t,
		newWorkerSideActivityError(context.Background(), provider, service.BackendTypeCadence, grpcStatus.Err(), nil),
		expectedError,
	)
	require.Equal(
		t,
		"WorkerErrorResponse.retry_after_seconds requires the Temporal backend",
		internalActivityError.GetServerDetail(),
	)
}

func TestNewWorkerActivityFailureFallsBackWhenMessageEmpty(t *testing.T) {
	provider := interfaces.NewMockActivityProvider(gomock.NewController(t))
	inputError := status.Error(codes.Canceled, "")
	expectedError := errors.New("activity error")
	var internalActivityError *dexpb.InternalActivityError
	provider.EXPECT().
		NewActivityError(
			dexpb.FlowErrorType_FLOW_ERROR_TYPE_WORKER_API_FAIL,
			gomock.Any(),
			int32(0),
		).
		DoAndReturn(func(
			_ dexpb.FlowErrorType,
			activityError *dexpb.InternalActivityError,
			_ int32,
		) error {
			internalActivityError = activityError
			return expectedError
		})

	require.ErrorIs(
		t,
		newWorkerSideActivityError(context.Background(), provider, service.BackendTypeTemporal, inputError, nil),
		expectedError,
	)
	require.Equal(t, inputError.Error(), internalActivityError.GetServerDetail())
	require.Equal(t, int32(codes.Canceled), internalActivityError.GetWorkerGrpcStatus())
}

func TestNewServerActivityFailureKeepsGRPCErrorInternal(t *testing.T) {
	provider := interfaces.NewMockActivityProvider(gomock.NewController(t))
	inputError := status.Error(codes.Unavailable, "internal service unavailable")
	expectedError := errors.New("activity error")
	var internalActivityError *dexpb.InternalActivityError
	provider.EXPECT().
		NewActivityError(
			dexpb.FlowErrorType_FLOW_ERROR_TYPE_INTERNAL,
			gomock.Any(),
			int32(0),
		).
		DoAndReturn(func(
			_ dexpb.FlowErrorType,
			activityError *dexpb.InternalActivityError,
			_ int32,
		) error {
			internalActivityError = activityError
			return expectedError
		})

	require.ErrorIs(
		t,
		newServerSideActivityError(context.Background(), provider, inputError, nil),
		expectedError,
	)
	require.Contains(t, internalActivityError.GetServerDetail(), "internal service unavailable")
}
