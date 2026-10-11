// Legacy Materials in this file remain under their original licenses.
// See LEGACY_NOTICES.md.

// Modifications Copyright (c) 2026 Super Durable, Inc.
//
// Modifications after the Legacy Cutoff are licensed under the
// Sustainable Use License 1.0.
// Legacy Materials remain under their original licenses.
// See LICENSE and LEGACY_NOTICES.md.

package dex

import (
	"fmt"
	"time"
)

// Wait describes when Dex may invoke a Step's Execute method.
//
// Build waits from durable Timer and Channel Conditions. Condition order determines timer indexes
// used by TimerID and HasTimerFiredByIndex.
//
// Example:
//
//	return dex.AnyOf(
//		approvals.ForOne(),
//		dex.Timer(5*time.Minute),
//	), nil
type Wait struct {
	kind         waitKind
	conditions   []Condition
	combinations []ConditionCombination
}

// SkipWaitImmediately asks Dex to invoke Execute without evaluating WaitFor conditions.
func SkipWaitImmediately() *Wait {
	return &Wait{kind: skipWaitImmediately}
}

// Until waits until one condition is satisfied. The Condition does not need an ID.
func Until(condition Condition) *Wait {
	return AllOf(condition)
}

// AllOf waits until every condition is satisfied. Conditions do not need IDs.
func AllOf(conditions ...Condition) *Wait {
	return &Wait{kind: waitAllOf, conditions: conditions}
}

// AnyOf waits until at least one condition is satisfied.
// Conditions do not need IDs.
// Among ready conditions, Dex selects the first in canonical order: Timers, Channels, then
// SubFlows, preserving declaration order within each kind. An unready condition does not block
// a later ready condition. Return only the higher-priority condition to wait for strict priority.
// It consumes messages only from the selected Channel condition, not other ready alternatives.
func AnyOf(conditions ...Condition) *Wait {
	return &Wait{kind: waitAnyOf, conditions: conditions}
}

// AnyComboOf waits until every Condition in at least one combination is satisfied.
// It consumes messages only from Channel conditions in the selected combination.
// Every Condition requires a non-empty user-provided ID.
func AnyComboOf(combinations ...ConditionCombination) *Wait {
	return &Wait{kind: waitAnyComboOf, combinations: combinations}
}

// Condition represents one durable Timer, Channel, or SubFlow predicate.
// Create values through Timer, SubFlow, Channel, or ChannelMap; custom implementations are not supported.
type Condition interface {
	condition()
}

// Timer returns a Condition satisfied after duration of durable workflow time.
// The timer survives Worker restarts.
func Timer(duration time.Duration, options ...ConditionOption) Condition {
	condition := &conditionImpl{
		kind:     conditionTimer,
		duration: duration,
	}
	applyConditionOptions(condition, options)
	return condition
}

// SubFlow returns a Condition satisfied when target reaches a terminal state.
//
// target must be registered by the Worker and define a starting Step whose input accepts input.
// Dex generates the SubFlow Flow ID. Omit options to use the default abnormal-restart policy, or
// pass exactly one value to configure reuse, timing, Attributes, Flow configuration, and ID.
//
// The server starts the SubFlow after WaitFor returns and retries the start within its
// configured retry budget. When the start still fails, the waiting Step's WaitForFailure
// policy decides the outcome. The default FailFlowOnWaitForFailure fails the parent Flow.
// ProceedOnWaitForFailure runs Execute, and SubFlowResult reports FlowErrorSubFlowStartFailed.
//
//	return dex.Until(dex.SubFlow(ChargeFlow{}, chargeInput)), nil
func SubFlow(target Flow, input any, options ...SubFlowOptions) Condition {
	condition := &conditionImpl{
		kind:           conditionSubFlow,
		subFlow:        target,
		subFlowInput:   input,
		subFlowOptions: defaultSubFlowOptions(),
	}
	if len(options) > 1 {
		condition.err = fmt.Errorf("dex: SubFlow accepts at most one options value")
		return condition
	}
	if len(options) == 1 {
		condition.subFlowOptions = options[0]
	}
	condition.conditionID = condition.subFlowOptions.ConditionID
	condition.idSet = condition.conditionID != ""
	return condition
}

// SubFlowResult returns one SubFlow result from the current Execute invocation.
//
// Omit index to read the first SubFlow Condition. Indexes are zero-based in the stable SubFlow
// order within the surrounding Wait. AnyOf losers return a nonterminal running snapshot.
//
// A SubFlow that the server could not start returns Status FlowFailed with ErrorType
// FlowErrorSubFlowStartFailed and the start error in ErrorMessage. Execute sees this result
// only when the Step uses ProceedOnWaitForFailure. Check the error type to tell a start
// failure apart from a SubFlow that started and then failed.
//
//	result, err := dex.SubFlowResult(ctx)
//	if err != nil {
//		return nil, err
//	}
//	if result.ErrorType == dex.FlowErrorSubFlowStartFailed {
//		return dex.ForceFail("SubFlow did not start: " + result.ErrorMessage), nil
//	}
func SubFlowResult(ctx Context, index ...int) (FlowResult, error) {
	invocation, ok := ctx.(*invocationContext)
	if !ok || invocation == nil {
		return FlowResult{}, fmt.Errorf("dex: SubFlow results require a Dex invocation Context")
	}
	resolvedIndex, err := resolveSubFlowIndex(index)
	if err != nil {
		return FlowResult{}, err
	}
	return invocation.subFlowResult(resolvedIndex)
}

// SubFlowID returns one server-generated SubFlow Flow ID from the current Execute invocation.
//
// Omit index for the first SubFlow. The ID remains addressable after AnyOf completes and can be
// passed to Client.StopFlow to stop a still-running loser.
func SubFlowID(ctx Context, index ...int) (string, error) {
	invocation, ok := ctx.(*invocationContext)
	if !ok || invocation == nil {
		return "", fmt.Errorf("dex: SubFlow IDs require a Dex invocation Context")
	}
	resolvedIndex, err := resolveSubFlowIndex(index)
	if err != nil {
		return "", err
	}
	if _, err := invocation.subFlowResult(resolvedIndex); err != nil {
		return "", err
	}
	return fmt.Sprintf(
		"SubFlow:%s-%s-%d",
		invocation.flowID,
		invocation.stepExecutionID,
		resolvedIndex,
	), nil
}

func resolveSubFlowIndex(indexes []int) (int, error) {
	if len(indexes) > 1 {
		return 0, fmt.Errorf("dex: SubFlow accepts at most one result index")
	}
	if len(indexes) == 0 {
		return 0, nil
	}
	if indexes[0] < 0 {
		return 0, fmt.Errorf("dex: SubFlow result index must not be negative")
	}
	return indexes[0], nil
}

// ConditionOption configures a Condition. Use WithConditionID to create an option.
type ConditionOption interface {
	applyCondition(*conditionImpl)
}

// WithConditionID assigns a stable timer or Channel Condition ID for later targeting.
func WithConditionID(conditionID string) ConditionOption {
	return conditionIDOption{conditionID: conditionID}
}

// ConditionCombination groups Conditions that must all succeed as one AnyComboOf branch.
type ConditionCombination struct {
	conditions []Condition
}

// Combo creates one branch satisfied only when all conditions are satisfied.
func Combo(conditions ...Condition) ConditionCombination {
	return ConditionCombination{conditions: conditions}
}

type waitKind uint8

const (
	skipWaitImmediately waitKind = iota + 1
	waitAllOf
	waitAnyOf
	waitAnyComboOf
)

type conditionKind uint8

const (
	conditionChannel conditionKind = iota + 1
	conditionTimer
	conditionSubFlow
)

type conditionImpl struct {
	kind           conditionKind
	conditionID    string
	idSet          bool
	channelName    string
	instance       string
	isMap          bool
	atLeast        *int
	atMost         *int
	duration       time.Duration
	subFlow        Flow
	subFlowInput   any
	subFlowOptions SubFlowOptions
	err            error
}

func newChannelCondition(
	name string,
	instance string,
	isMap bool,
	atLeast *int,
	atMost *int,
	options []ConditionOption,
) Condition {
	condition := &conditionImpl{
		kind:        conditionChannel,
		channelName: name,
		instance:    instance,
		isMap:       isMap,
		atLeast:     atLeast,
		atMost:      atMost,
	}
	condition.err = validateChannelBounds(atLeast, atMost)
	applyConditionOptions(condition, options)
	return condition
}

func applyConditionOptions(
	condition *conditionImpl,
	options []ConditionOption,
) {
	for _, option := range options {
		option.applyCondition(condition)
	}
}

func validateChannelBounds(atLeast *int, atMost *int) error {
	if atLeast != nil && *atLeast < 0 {
		return fmt.Errorf("dex: at_least must be non-negative")
	}
	if atMost != nil && *atMost < 0 {
		return fmt.Errorf("dex: at_most must be non-negative")
	}
	if atLeast != nil && atMost != nil && *atMost < *atLeast {
		return fmt.Errorf("dex: at_most must not be below at_least")
	}
	return nil
}

func (*conditionImpl) condition() {}

type conditionIDOption struct {
	conditionID string
}

func (option conditionIDOption) applyCondition(condition *conditionImpl) {
	condition.conditionID = option.conditionID
	condition.idSet = true
}
