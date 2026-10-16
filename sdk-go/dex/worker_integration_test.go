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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/superdurable/dex/blob-cache-go/blobcache"
	"github.com/superdurable/dex/sdk-go/gen/dexpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"
)

var (
	workerTestStatus   = DefineAttribute[string]("status", SyncToAttributeStore())
	workerTestItems    = DefineAttributeMap[int]("items", SyncToAttributeStore())
	workerTestCommands = DefineChannel[string]("commands")
	workerTestByOrder  = DefineChannelMap[string]("commands-by-order")
	workerTestThinking = DefineStream[string]("thinking", 1<<20)
	workerActionStatus = DefineAttribute[string]("action-status")
	workerActionRegion = DefineAttribute[string]("action-region")
	workerActionNote   = DefineAttribute[string]("action-note")
)

type workerTestInput struct {
	OrderID string
	Mode    string
}

type workerTestOutput struct {
	Before int
	After  int
}

type concreteValueHydrator struct{}

func (concreteValueHydrator) HydrateValuesInPlace(
	_ context.Context,
	flowValuePointers []flowValuePointer,
) error {
	for index, target := range flowValuePointers {
		valuePointer := target.valuePointer
		if valuePointer == nil {
			return newWorkerFailure(
				codes.InvalidArgument,
				fmt.Errorf("dex: value pointer at index %d is nil", index),
			)
		}
		if err := validateConcreteValue(*valuePointer); err != nil {
			return newWorkerFailure(codes.InvalidArgument, err)
		}
	}
	return nil
}

type workerActionProjectionInput struct {
	Mode string
}

type workerActionProjectionStep struct {
	StepDefaults
}

func (workerActionProjectionStep) WaitFor(
	ctx Context,
	input workerActionProjectionInput,
) (*Wait, error) {
	if err := applyWorkerActionProjectionWrites(ctx, input.Mode); err != nil {
		return nil, err
	}
	return SkipWaitImmediately(), nil
}

func (workerActionProjectionStep) Execute(
	ctx Context,
	input workerActionProjectionInput,
) (*StepDecision, error) {
	if err := applyWorkerActionProjectionWrites(ctx, input.Mode); err != nil {
		return nil, err
	}
	return DeadEnd(), nil
}

var workerActionProjectionStart = workerActionProjectionStep{}

type workerActionProjectionFlow struct {
	FlowDefaults
}

func (workerActionProjectionFlow) GetSteps() []StepDef {
	return []StepDef{DefineStartStep(workerActionProjectionStart)}
}

func (flow workerActionProjectionFlow) GetRPCs() []RPCDef {
	return []RPCDef{
		DefineRPC(flow.UpdateProjection, &RPCOptions{Action: DefineAction(
			"Update projection",
			WhenAttributeMatches(
				workerActionStatus,
				AttributeMatchEqual("A"),
				AttributeMatchEqual("B"),
			),
			ActionRequiresPermission("permission-z"),
		)}),
		DefineRPC(flow.ApproveRegion, &RPCOptions{Action: DefineAction(
			"Approve region",
			WhenAttributeMatches(workerActionRegion, AttributeMatchEqual("C")),
			ActionRequiresPermission("permission-x"),
		)}),
		DefineRPC(flow.ReviewRegion, &RPCOptions{Action: DefineAction(
			"Review region",
			WhenAttributeMatches(workerActionRegion, AttributeMatchEqual("C")),
			ActionRequiresPermission("permission-x"),
		)}),
	}
}

func (workerActionProjectionFlow) GetPersistenceSchema() PersistenceSchema {
	return PersistenceSchema{Attributes: []AttributeDef{
		workerActionStatus,
		workerActionRegion,
		workerActionNote,
	}}
}

func (workerActionProjectionFlow) HandleTimeout(ctx Context) (*StepDecision, error) {
	if err := workerActionStatus.Set(ctx, "A"); err != nil {
		return nil, err
	}
	return DeadEnd(), nil
}

func (workerActionProjectionFlow) UpdateProjection(
	ctx Context,
	input workerActionProjectionInput,
) (*RPCResult[None], error) {
	if err := applyWorkerActionProjectionWrites(ctx, input.Mode); err != nil {
		return nil, err
	}
	return &RPCResult[None]{}, nil
}

func (workerActionProjectionFlow) ApproveRegion(
	Context,
	None,
) (*RPCResult[None], error) {
	return &RPCResult[None]{}, nil
}

func (workerActionProjectionFlow) ReviewRegion(
	Context,
	None,
) (*RPCResult[None], error) {
	return &RPCResult[None]{}, nil
}

func applyWorkerActionProjectionWrites(ctx Context, mode string) error {
	switch mode {
	case "none":
		return nil
	case "status-a":
		return workerActionStatus.Set(ctx, "A")
	case "status-d":
		return workerActionStatus.Set(ctx, "D")
	case "both":
		if err := workerActionStatus.Set(ctx, "A"); err != nil {
			return err
		}
		return workerActionRegion.Set(ctx, "C")
	case "last-status-wins":
		if err := workerActionStatus.Set(ctx, "B"); err != nil {
			return err
		}
		return workerActionStatus.Set(ctx, "D")
	case "delete-status":
		return workerActionStatus.Delete(ctx)
	case "unrelated":
		return workerActionNote.Set(ctx, "note")
	default:
		return fmt.Errorf("unknown projection test mode %q", mode)
	}
}

var workerActionFlow = workerActionProjectionFlow{}

type workerWaitingStep struct {
	StepDefaults
}

func (workerWaitingStep) WaitFor(
	ctx Context,
	input workerTestInput,
) (*Wait, error) {
	if input.Mode == "channel-size" &&
		(workerTestCommands.Size(ctx) != 2 || workerTestByOrder.Size(ctx, input.OrderID) != 1) {
		return nil, errors.New("WaitFor channel size snapshot is invalid")
	}
	if err := workerTestStatus.Set(ctx, "waiting"); err != nil {
		return nil, err
	}
	statusValue, err := workerTestStatus.Get(ctx)
	if err != nil || statusValue != "waiting" {
		return nil, errors.New("buffered attribute write was not visible")
	}
	_, err = workerTestItems.Get(ctx, "missing")
	var missingItem *AttributeNotFoundError
	var notLoaded *AttributeMapNotLoadedError
	if !errors.As(err, &notLoaded) && (!errors.As(err, &missingItem) ||
		missingItem.AttributeName != workerTestItems.AttributeName() ||
		missingItem.Instance != "missing") {
		return nil, errors.New("missing attribute map value returned the wrong error")
	}
	if err := ctx.SetStepExecutionLocal("input", input); err != nil {
		return nil, err
	}
	if err := ctx.RecordEvent("waiting", input); err != nil {
		return nil, err
	}
	if err := workerTestByOrder.Publish(ctx, input.OrderID, "published"); err != nil {
		return nil, err
	}
	if input.Mode == "stream" || input.Mode == "stream-twice" {
		if err := workerTestThinking.Write(ctx, "checking inventory"); err != nil {
			return nil, err
		}
	}
	if input.Mode == "stream-twice" {
		if err := workerTestThinking.Write(ctx, "duplicate"); err != nil {
			return nil, err
		}
	}
	if input.Mode == "progress" || input.Mode == "progress-error" || input.Mode == "progress-panic" {
		if err := ctx.RecordHeartbeat("checkpoint-1"); err != nil {
			return nil, err
		}
		if err := workerTestThinking.Write(ctx, "checking inventory"); err != nil {
			return nil, err
		}
		if err := ctx.RecordHeartbeat(nil); err != nil {
			return nil, err
		}
		if input.Mode == "progress-error" {
			return nil, errors.New("wait failed after progress")
		}
		if input.Mode == "progress-panic" {
			panic("wait panicked after progress")
		}
	}
	if input.Mode == "restore-heartbeat" {
		lastHeartbeat := "unchanged"
		found, err := ctx.GetLastHeartbeatValue(&lastHeartbeat)
		if err != nil || !found || lastHeartbeat != "checkpoint-restored" {
			return nil, fmt.Errorf("last heartbeat was not restored: %q: %w", lastHeartbeat, err)
		}
		var typedNil *string
		if err := ctx.RecordHeartbeat(typedNil); err != nil {
			return nil, err
		}
	}
	if input.Mode == "missing-heartbeat" {
		lastHeartbeat := "unchanged"
		found, err := ctx.GetLastHeartbeatValue(&lastHeartbeat)
		if err != nil || found || lastHeartbeat != "unchanged" {
			return nil, fmt.Errorf("missing heartbeat changed target: %q: %w", lastHeartbeat, err)
		}
	}
	if input.Mode == "nil-wait" {
		return nil, nil
	}
	if input.Mode == "immediate" {
		return SkipWaitImmediately(), nil
	}
	if input.Mode == "combo" {
		return AnyComboOf(Combo(
			workerTestCommands.ForOne(WithConditionID("command")),
			Timer(time.Second, WithConditionID("timer")),
		)), nil
	}
	return AnyOf(
		workerTestCommands.ForOne(WithConditionID("command")),
		Timer(time.Second, WithConditionID("timer")),
	), nil
}

func (workerWaitingStep) Execute(
	ctx Context,
	input workerTestInput,
) (*StepDecision, error) {
	if input.Mode == "channel-size" &&
		(workerTestCommands.Size(ctx) != 2 || workerTestByOrder.Size(ctx, input.OrderID) != 1) {
		return nil, errors.New("Execute channel size snapshot is invalid")
	}
	if input.Mode == "empty" {
		return nil, nil
	}
	if input.Mode == "error" {
		if err := workerTestStatus.Set(ctx, "discarded"); err != nil {
			return nil, err
		}
		return nil, errors.New("execute failed")
	}
	if input.Mode == "execute-progress" {
		if err := ctx.RecordHeartbeat("execute-checkpoint"); err != nil {
			return nil, err
		}
		if err := workerTestThinking.Write(ctx, "execute-progress"); err != nil {
			return nil, err
		}
	}
	if input.Mode == "missing-local-target" {
		found, err := ctx.GetStepExecutionLocal("missing", nil)
		if err == nil || found {
			return nil, errors.New("missing local accepted a nil target")
		}
		return DeadEnd(), nil
	}
	values, err := workerTestCommands.GetConditionResults(ctx)
	if err != nil {
		return nil, err
	}
	if len(values) != 2 || values[0] != "first" || values[1] != "second" {
		return nil, errors.New("channel results are out of order")
	}
	mapValues, err := workerTestByOrder.GetConditionResults(ctx, input.OrderID)
	if err != nil {
		return nil, err
	}
	if len(mapValues) != 1 || mapValues[0] != "mapped" {
		return nil, errors.New("map channel results are invalid")
	}
	var local workerTestInput
	found, err := ctx.GetStepExecutionLocal("input", &local)
	if err != nil || !found || local != input {
		return nil, errors.New("step-execution local is invalid")
	}
	timerFired := input.Mode == "timer"
	if !ctx.WaitForMethodFailed() || ctx.HasTimerFired() != timerFired ||
		ctx.HasTimerFiredByIndex(0) != timerFired {
		return nil, errors.New("condition helpers are invalid")
	}
	if err := workerTestStatus.Delete(ctx); err != nil {
		return nil, err
	}
	_, err = workerTestStatus.Get(ctx)
	var notFound *AttributeNotFoundError
	if !errors.As(err, &notFound) ||
		notFound.AttributeName != workerTestStatus.AttributeName() {
		return nil, errors.New("buffered attribute delete was not visible")
	}
	return GoTo(workerTestFinish, input), nil
}

var workerTestWait = workerWaitingStep{}

type workerFinishStep struct {
	StepDefaultsNoWaitFor[workerTestInput]
}

func (workerFinishStep) Execute(
	Context,
	workerTestInput,
) (*StepDecision, error) {
	return DeadEnd(), nil
}

var workerTestFinish = workerFinishStep{}

type workerTestFlow struct {
	FlowDefaults
}

func (workerTestFlow) HandleTimeout(ctx Context) (*StepDecision, error) {
	if err := ctx.RecordHeartbeat("timeout-checkpoint"); err != nil {
		return nil, err
	}
	if err := workerTestThinking.Write(ctx, "timeout-progress"); err != nil {
		return nil, err
	}
	return GracefulComplete("timeout"), nil
}

func (workerTestFlow) GetSteps() []StepDef {
	return []StepDef{
		DefineStartStep(workerTestWait),
		DefineStep(workerTestFinish),
	}
}

func (flow workerTestFlow) GetRPCs() []RPCDef {
	return []RPCDef{DefineRPC(flow.Update, nil)}
}

func (workerTestFlow) GetPersistenceSchema() PersistenceSchema {
	return PersistenceSchema{
		Attributes: []AttributeDef{workerTestStatus, workerTestItems},
		Channels:   []ChannelDef{workerTestCommands, workerTestByOrder},
		Streams:    []StreamDef{workerTestThinking},
	}
}

func (workerTestFlow) Update(
	ctx Context,
	input workerTestInput,
) (*RPCResult[workerTestOutput], error) {
	if input.Mode == "panic" {
		panic("RPC panic")
	}
	if input.Mode == "nil-result" {
		return nil, nil
	}
	if input.Mode == "status" {
		return nil, status.Error(codes.PermissionDenied, "denied")
	}
	if input.Mode == "stream" {
		if err := workerTestThinking.Write(ctx, "rpc"); err != nil {
			return nil, err
		}
	}
	if input.Mode == "heartbeat" {
		if err := ctx.RecordHeartbeat("rpc"); err != nil {
			return nil, err
		}
	}
	if input.Mode == "pending" {
		pending, err := workerTestCommands.PendingMessages(ctx)
		if err != nil {
			return nil, err
		}
		return &RPCResult[workerTestOutput]{
			Output: workerTestOutput{Before: len(pending), After: len(pending)},
		}, nil
	}
	before := workerTestCommands.Size(ctx)
	if err := workerTestCommands.Publish(ctx, "local"); err != nil {
		return nil, err
	}
	after := workerTestCommands.Size(ctx)
	if err := workerTestItems.Set(ctx, input.OrderID, after); err != nil {
		return nil, err
	}
	if input.Mode == "introspection" {
		pending, err := workerTestCommands.PendingMessages(ctx)
		if err != nil || !reflect.DeepEqual(
			pending,
			[]ChannelMessage[string]{{MessageID: "message-1", Value: "queued"}},
		) {
			return nil, fmt.Errorf("unexpected pending Channel messages %v: %w", pending, err)
		}
		mapped, found, err := workerTestByOrder.FindPendingMessage(
			ctx,
			"selected",
			"message-2",
		)
		if err != nil || !found || mapped.Value != "mapped" {
			return nil, fmt.Errorf("unexpected pending ChannelMap message %v: %w", mapped, err)
		}
		if keys := workerTestItems.AllInstanceKeys(ctx); !reflect.DeepEqual(
			keys,
			[]string{"initial % key", input.OrderID},
		) || workerTestItems.MapSize(ctx) != len(keys) {
			return nil, fmt.Errorf("unexpected AttributeMap keys %v", keys)
		}
		if err := workerTestItems.Delete(ctx, "initial % key"); err != nil {
			return nil, err
		}
		if keys := workerTestItems.AllInstanceKeys(ctx); !reflect.DeepEqual(
			keys,
			[]string{input.OrderID},
		) || workerTestItems.MapSize(ctx) != len(keys) {
			return nil, fmt.Errorf("unexpected updated AttributeMap keys %v", keys)
		}
		if keys := workerTestByOrder.AllInstanceKeys(ctx); !reflect.DeepEqual(
			keys,
			[]string{"initial % key"},
		) || workerTestByOrder.MapSize(ctx) != len(keys) {
			return nil, fmt.Errorf("unexpected ChannelMap keys %v", keys)
		}
		if err := workerTestByOrder.Publish(ctx, input.OrderID, "mapped"); err != nil {
			return nil, err
		}
		if keys := workerTestByOrder.AllInstanceKeys(ctx); !reflect.DeepEqual(
			keys,
			[]string{"initial % key", input.OrderID},
		) || workerTestByOrder.MapSize(ctx) != len(keys) {
			return nil, fmt.Errorf("unexpected updated ChannelMap keys %v", keys)
		}
	}
	if err := ctx.RecordEvent("updated", input); err != nil {
		return nil, err
	}
	return &RPCResult[workerTestOutput]{
		Output: workerTestOutput{Before: before, After: after},
		NextSteps: []StepMovement{
			MovementOf(workerTestFinish, input),
		},
	}, nil
}

var workerFlow = workerTestFlow{}

type workerBlockingFlow struct {
	FlowDefaults
	entered     chan struct{}
	release     chan struct{}
	enteredOnce sync.Once
}

func (*workerBlockingFlow) GetSteps() []StepDef {
	return nil
}

func (flow *workerBlockingFlow) GetRPCs() []RPCDef {
	return []RPCDef{DefineRPC(flow.Block, nil)}
}

func (*workerBlockingFlow) GetPersistenceSchema() PersistenceSchema {
	return PersistenceSchema{}
}

func (flow *workerBlockingFlow) Block(
	ctx Context,
	_ struct{},
) (*RPCResult[bool], error) {
	flow.enteredOnce.Do(func() { close(flow.entered) })
	select {
	case <-flow.release:
		return &RPCResult[bool]{Output: true}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func TestWorkerServiceDispatchesWaitExecuteAndRPC(t *testing.T) {
	client, closeService := newWorkerTestClient(t, nil)
	defer closeService()

	waitInput := workerTestInput{OrderID: "order-1"}
	waitResponse := invokeWaitForResult(t, client,
		&dexpb.InvokeWaitForMethodRequest{
			Context:   workerStepContext(),
			FlowType:  GetFinalFlowType(workerFlow),
			StepType:  GetFinalStepType(workerTestWait),
			StepInput: mustEncodeWorkerTestValue(t, waitInput),
		},
	)
	require.Len(t, waitResponse.UpsertAttributes, 1)
	require.Equal(t, "waiting", waitResponse.UpsertAttributes[0].Value.GetStringValue())
	require.True(t, waitResponse.UpsertAttributes[0].GetSyncConfig().GetEnabled())
	require.Len(t, waitResponse.UpsertStepExeLocals, 1)
	require.Len(t, waitResponse.RecordEvents, 1)
	require.Len(t, waitResponse.PublishToChannel, 1)
	require.Equal(t, "commands-by-order/order-1", waitResponse.PublishToChannel[0].ChannelName)
	require.Len(t, waitResponse.WaitingCondition.ChannelConditions, 1)
	require.Len(t, waitResponse.WaitingCondition.TimerConditions, 1)

	immediateRequest := &dexpb.InvokeWaitForMethodRequest{
		Context:  workerStepContext(),
		FlowType: GetFinalFlowType(workerFlow),
		StepType: GetFinalStepType(workerTestWait),
		StepInput: mustEncodeWorkerTestValue(t, workerTestInput{
			OrderID: "order-1",
			Mode:    "immediate",
		}),
	}
	immediateResponse := invokeWaitForResult(t, client, immediateRequest)
	require.Nil(t, immediateResponse.WaitingCondition)

	comboRequest := &dexpb.InvokeWaitForMethodRequest{
		Context:  workerStepContext(),
		FlowType: GetFinalFlowType(workerFlow),
		StepType: GetFinalStepType(workerTestWait),
		StepInput: mustEncodeWorkerTestValue(t, workerTestInput{
			OrderID: "order-1",
			Mode:    "combo",
		}),
	}
	comboResponse := invokeWaitForResult(t, client, comboRequest)
	require.Len(t, comboResponse.WaitingCondition.ConditionCombinations, 1)

	channelSizeInput := workerTestInput{OrderID: "order-1", Mode: "channel-size"}
	channelSizeWaitRequest := &dexpb.InvokeWaitForMethodRequest{
		Context:   workerStepContext(),
		FlowType:  GetFinalFlowType(workerFlow),
		StepType:  GetFinalStepType(workerTestWait),
		StepInput: mustEncodeWorkerTestValue(t, channelSizeInput),
		ChannelInfos: map[string]*dexpb.ChannelInfo{
			"commands":                  {Size: 2},
			"commands-by-order/order-1": {Size: 1},
		},
	}
	invokeWaitForResult(t, client, channelSizeWaitRequest)
	channelSizeExecuteRequest := workerExecuteRequest(t, channelSizeInput)
	channelSizeExecuteRequest.ChannelInfos = channelSizeWaitRequest.ChannelInfos
	invokeExecuteResult(t, client, channelSizeExecuteRequest)

	executeResponse := invokeExecuteResult(t, client,
		workerExecuteRequest(t, waitInput),
	)
	require.Len(t, executeResponse.StepDecision.NextSteps, 1)
	require.Equal(t, GetFinalStepType(workerTestFinish), executeResponse.StepDecision.NextSteps[0].StepType)
	require.Len(t, executeResponse.UpsertAttributes, 1)
	_, deleted := executeResponse.UpsertAttributes[0].Value.Kind.(*dexpb.Value_NullValue)
	require.True(t, deleted)
	require.True(t, executeResponse.UpsertAttributes[0].GetSyncConfig().GetEnabled())

	timerRequest := workerExecuteRequest(t, workerTestInput{
		OrderID: "order-1",
		Mode:    "timer",
	})
	timerRequest.ConditionResults.TimerResults[0].ConditionStatus =
		dexpb.ConditionStatus_CONDITION_STATUS_COMPLETED
	invokeExecuteResult(t, client, timerRequest)

	invokeExecuteResult(t, client,
		workerExecuteRequest(t, workerTestInput{
			OrderID: "order-1",
			Mode:    "missing-local-target",
		}),
	)

	rpcInput := workerTestInput{OrderID: "order-1", Mode: "introspection"}
	rpcResponse, err := client.InvokeWorkerRPC(
		context.Background(),
		&dexpb.InvokeWorkerRPCRequest{
			Context:  workerRPCContext(),
			FlowType: GetFinalFlowType(workerFlow),
			RpcName:  "Update",
			Input:    mustEncodeWorkerTestValue(t, rpcInput),
			Attributes: []*dexpb.KV{{
				Key:   "items/initial%20%25%20key",
				Value: mustEncodeWorkerTestValue(t, 1),
			}},
			ChannelInfos: map[string]*dexpb.ChannelInfo{
				"commands":                              {Size: 2},
				"commands-by-order/empty":               {Size: 0},
				"commands-by-order/initial%20%25%20key": {Size: 1},
			},
			LoadedAttributeMapInstances: []string{"items/"},
			LoadedChannelNames:          []string{"commands"},
			LoadedChannelMapInstances: []string{
				"commands-by-order/selected",
			},
			LoadedChannelMessages: map[string]*dexpb.ChannelValues{
				"commands": {Messages: []*dexpb.ChannelMessage{{
					MessageId: "message-1",
					Value:     mustEncodeWorkerTestValue(t, "queued"),
				}}},
				"commands-by-order/selected": {
					Messages: []*dexpb.ChannelMessage{{
						MessageId: "message-2",
						Value:     mustEncodeWorkerTestValue(t, "mapped"),
					}},
				},
			},
		},
	)
	require.NoError(t, err)
	var output workerTestOutput
	require.NoError(t, decodeValue(rpcResponse.Output, &output))
	require.Equal(t, workerTestOutput{Before: 2, After: 3}, output)
	require.Len(t, rpcResponse.PublishToChannel, 2)
	require.Len(t, rpcResponse.UpsertAttributes, 2)
	require.Equal(t, "items/order-1", rpcResponse.UpsertAttributes[0].Key)
	require.True(t, rpcResponse.UpsertAttributes[0].GetSyncConfig().GetEnabled())
	require.Len(t, rpcResponse.RecordEvents, 1)
	require.Len(t, rpcResponse.StepDecision.NextSteps, 1)
}

func TestWorkerActionPermissionMappingEmission(t *testing.T) {
	client, closeService := newWorkerClientForFlows(t, []Flow{workerActionFlow}, nil)
	defer closeService()

	tests := []struct {
		name              string
		method            string
		mode              string
		attributes        []*dexpb.KV
		expectedWriteKeys []string
		expectedValues    []any
		expectsMappings   bool
	}{
		{
			name:   "WaitFor missing to empty",
			method: "wait-for",
			mode:   "none",
		},
		{
			name:   "Execute unchanged",
			method: "execute",
			mode:   "none",
			attributes: workerActionProjectionAttributes(t,
				map[string]any{
					workerActionStatus.AttributeName(): "A",
					WorkQueuePermissionsIndexKey:       []string{"permission-z"},
				},
			),
		},
		{
			name:   "RPC logical set unchanged",
			method: "rpc",
			mode:   "none",
			attributes: workerActionProjectionAttributes(t,
				map[string]any{
					workerActionStatus.AttributeName(): "A",
					workerActionRegion.AttributeName(): "C",
					WorkQueuePermissionsIndexKey: []string{
						"permission-z",
						"permission-x",
						"permission-x",
					},
				},
			),
		},
		{
			name:   "WaitFor nonempty replacement",
			method: "wait-for",
			mode:   "both",
			expectedWriteKeys: []string{
				workerActionStatus.AttributeName(),
				workerActionRegion.AttributeName(),
			},
			expectedValues:  []any{"A", "C"},
			expectsMappings: true,
		},
		{
			name:              "timeout Execute source write",
			method:            "timeout",
			expectedWriteKeys: []string{workerActionStatus.AttributeName()},
			expectedValues:    []any{"A"},
			expectsMappings:   true,
		},
		{
			name:   "Execute nonempty to empty",
			method: "execute",
			mode:   "last-status-wins",
			attributes: workerActionProjectionAttributes(t,
				map[string]any{
					workerActionStatus.AttributeName(): "A",
					WorkQueuePermissionsIndexKey:       []string{"permission-z"},
				},
			),
			expectedWriteKeys: []string{
				workerActionStatus.AttributeName(),
			},
			expectedValues:  []any{"D"},
			expectsMappings: true,
		},
		{
			name:   "RPC unrelated business write",
			method: "rpc",
			mode:   "unrelated",
			attributes: workerActionProjectionAttributes(t,
				map[string]any{
					workerActionStatus.AttributeName(): "A",
					WorkQueuePermissionsIndexKey:       []string{"permission-z"},
				},
			),
			expectedWriteKeys: []string{workerActionNote.AttributeName()},
			expectedValues:    []any{"note"},
		},
		{
			name:   "RPC source written to same value",
			method: "rpc",
			mode:   "status-a",
			attributes: workerActionProjectionAttributes(t,
				map[string]any{
					workerActionStatus.AttributeName(): "A",
					WorkQueuePermissionsIndexKey:       []string{"permission-z"},
				},
			),
			expectedWriteKeys: []string{workerActionStatus.AttributeName()},
			expectedValues:    []any{"A"},
			expectsMappings:   true,
		},
		{
			name:   "RPC deleted source",
			method: "rpc",
			mode:   "delete-status",
			attributes: workerActionProjectionAttributes(t,
				map[string]any{
					workerActionStatus.AttributeName(): "A",
					WorkQueuePermissionsIndexKey:       []string{"permission-z"},
				},
			),
			expectedWriteKeys: []string{
				workerActionStatus.AttributeName(),
			},
			expectedValues:  []any{nil},
			expectsMappings: true,
		},
		{
			name:   "RPC without source write ignores malformed projection",
			method: "rpc",
			mode:   "none",
			attributes: workerActionProjectionAttributes(t,
				map[string]any{
					workerActionStatus.AttributeName(): "A",
					WorkQueuePermissionsIndexKey:       "malformed",
				},
			),
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			result := invokeWorkerActionProjection(
				t,
				client,
				testCase.method,
				testCase.mode,
				testCase.attributes,
			)
			writes := result.writes
			require.Len(t, writes, len(testCase.expectedWriteKeys))
			for index, expectedKey := range testCase.expectedWriteKeys {
				require.Equal(t, expectedKey, writes[index].GetKey())
				expectedValue := testCase.expectedValues[index]
				if expectedValue == nil {
					_, deleted := writes[index].GetValue().GetKind().(*dexpb.Value_NullValue)
					require.True(t, deleted)
					continue
				}
				encoded := mustEncodeWorkerTestValue(t, expectedValue)
				require.True(t, reflect.DeepEqual(encoded, writes[index].GetValue()))
			}
			if !testCase.expectsMappings {
				require.Nil(t, result.mappings)
				return
			}
			require.Len(t, result.mappings.GetMappings(), 3)
			require.Equal(t, workerActionStatus.AttributeName(), result.mappings.GetMappings()[0].GetAttributeKey())
			require.Equal(t, "permission-z", result.mappings.GetMappings()[0].GetRequiredPermission())
			require.Len(t, result.mappings.GetMappings()[0].GetEqualValues(), 2)
			require.Equal(t, workerActionRegion.AttributeName(), result.mappings.GetMappings()[1].GetAttributeKey())
			require.Equal(t, "permission-x", result.mappings.GetMappings()[1].GetRequiredPermission())
		})
	}
}

type workerActionProjectionResult struct {
	writes   []*dexpb.AttributeWrite
	mappings *dexpb.ActionPermissionMappings
}

func invokeWorkerActionProjection(
	t *testing.T,
	client dexpb.WorkerServiceClient,
	method string,
	mode string,
	attributes []*dexpb.KV,
) workerActionProjectionResult {
	input := workerActionProjectionInput{Mode: mode}
	switch method {
	case "wait-for":
		response := invokeWaitForResult(t, client, &dexpb.InvokeWaitForMethodRequest{
			Context:    workerStepContext(),
			FlowType:   GetFinalFlowType(workerActionFlow),
			StepType:   GetFinalStepType(workerActionProjectionStart),
			StepInput:  mustEncodeWorkerTestValue(t, input),
			Attributes: attributes,
		})
		return workerActionProjectionResult{
			writes: response.UpsertAttributes, mappings: response.ActionPermissionMappings,
		}
	case "execute":
		response := invokeExecuteResult(t, client, &dexpb.InvokeExecuteMethodRequest{
			Context:          workerStepContext(),
			FlowType:         GetFinalFlowType(workerActionFlow),
			StepType:         GetFinalStepType(workerActionProjectionStart),
			StepInput:        mustEncodeWorkerTestValue(t, input),
			Attributes:       attributes,
			ConditionResults: &dexpb.ConditionResults{},
		})
		return workerActionProjectionResult{
			writes: response.UpsertAttributes, mappings: response.ActionPermissionMappings,
		}
	case "timeout":
		response := invokeExecuteResult(t, client, &dexpb.InvokeExecuteMethodRequest{
			Context:          workerStepContext(),
			FlowType:         GetFinalFlowType(workerActionFlow),
			StepType:         timeoutHandlerStepType,
			ConditionResults: &dexpb.ConditionResults{},
		})
		return workerActionProjectionResult{
			writes: response.UpsertAttributes, mappings: response.ActionPermissionMappings,
		}
	case "rpc":
		response, err := client.InvokeWorkerRPC(context.Background(), &dexpb.InvokeWorkerRPCRequest{
			Context:    workerRPCContext(),
			FlowType:   GetFinalFlowType(workerActionFlow),
			RpcName:    "UpdateProjection",
			Input:      mustEncodeWorkerTestValue(t, input),
			Attributes: attributes,
		})
		require.NoError(t, err)
		return workerActionProjectionResult{
			writes: response.UpsertAttributes, mappings: response.ActionPermissionMappings,
		}
	default:
		t.Fatalf("unknown Worker method %q", method)
		return workerActionProjectionResult{}
	}
}

func workerActionProjectionAttributes(
	t *testing.T,
	values map[string]any,
) []*dexpb.KV {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	attributes := make([]*dexpb.KV, 0, len(keys))
	for _, key := range keys {
		attributes = append(attributes, &dexpb.KV{
			Key:   key,
			Value: mustEncodeWorkerTestValue(t, values[key]),
		})
	}
	return attributes
}

func TestWorkerStepProgressFrames(t *testing.T) {
	client, closeService := newWorkerTestClient(t, nil)
	defer closeService()

	request := &dexpb.InvokeWaitForMethodRequest{
		Context:   workerStepContext(),
		FlowType:  GetFinalFlowType(workerFlow),
		StepType:  GetFinalStepType(workerTestWait),
		StepInput: mustEncodeWorkerTestValue(t, workerTestInput{OrderID: "order-1", Mode: "progress"}),
	}
	outputs, err := collectWaitForOutputs(client, request)
	require.NoError(t, err)
	require.Len(t, outputs, 4)
	require.Equal(t, "checkpoint-1", outputs[0].GetHeartbeat().GetValue().GetStringValue())
	require.Equal(t, "thinking", outputs[1].GetStreamWrite().GetStreamName())
	require.Equal(t, int64(1<<20), outputs[1].GetStreamWrite().GetStreamCapacityBytes())
	require.Equal(t, "checking inventory", outputs[1].GetStreamWrite().GetValue().GetStringValue())
	require.NotNil(t, outputs[2].GetHeartbeat())
	require.Nil(t, outputs[2].GetHeartbeat().GetValue())
	require.NotNil(t, outputs[3].GetResult())

	for _, mode := range []string{"progress-error", "progress-panic"} {
		request.StepInput = mustEncodeWorkerTestValue(t, workerTestInput{OrderID: "order-1", Mode: mode})
		outputs, err = collectWaitForOutputs(client, request)
		require.Error(t, err)
		require.Len(t, outputs, 3)
		for _, output := range outputs {
			require.Nil(t, output.GetResult())
		}
	}

	request.StepInput = mustEncodeWorkerTestValue(t, workerTestInput{OrderID: "order-1", Mode: "stream-twice"})
	outputs, err = collectWaitForOutputs(client, request)
	require.NoError(t, err)
	require.Len(t, outputs, 3)
	require.Equal(t, "checking inventory", outputs[0].GetStreamWrite().GetValue().GetStringValue())
	require.Equal(t, "duplicate", outputs[1].GetStreamWrite().GetValue().GetStringValue())
	require.NotNil(t, outputs[2].GetResult())

	restoreRequest := &dexpb.InvokeWaitForMethodRequest{
		Context:   workerStepContext(),
		FlowType:  GetFinalFlowType(workerFlow),
		StepType:  GetFinalStepType(workerTestWait),
		StepInput: mustEncodeWorkerTestValue(t, workerTestInput{OrderID: "order-1", Mode: "restore-heartbeat"}),
	}
	restoreRequest.Context.LastHeartbeatValue = mustEncodeWorkerTestValue(t, "checkpoint-restored")
	outputs, err = collectWaitForOutputs(client, restoreRequest)
	require.NoError(t, err)
	require.Len(t, outputs, 2)
	require.NotNil(t, outputs[0].GetHeartbeat())
	require.Nil(t, outputs[0].GetHeartbeat().GetValue())
	require.NotNil(t, outputs[1].GetResult())

	missingRequest := &dexpb.InvokeWaitForMethodRequest{
		Context:   workerStepContext(),
		FlowType:  GetFinalFlowType(workerFlow),
		StepType:  GetFinalStepType(workerTestWait),
		StepInput: mustEncodeWorkerTestValue(t, workerTestInput{OrderID: "order-1", Mode: "missing-heartbeat"}),
	}
	invokeWaitForResult(t, client, missingRequest)

	executeOutputs, err := collectExecuteOutputs(
		client,
		workerExecuteRequest(t, workerTestInput{OrderID: "order-1", Mode: "execute-progress"}),
	)
	require.NoError(t, err)
	require.Len(t, executeOutputs, 3)
	require.Equal(t, "execute-checkpoint", executeOutputs[0].GetHeartbeat().GetValue().GetStringValue())
	require.Equal(t, "execute-progress", executeOutputs[1].GetStreamWrite().GetValue().GetStringValue())
	require.NotNil(t, executeOutputs[2].GetResult())

	timeoutRequest := &dexpb.InvokeExecuteMethodRequest{
		Context:  workerStepContext(),
		FlowType: GetFinalFlowType(workerFlow),
		StepType: timeoutHandlerStepType,
	}
	timeoutOutputs, err := collectExecuteOutputs(client, timeoutRequest)
	require.NoError(t, err)
	require.Len(t, timeoutOutputs, 3)
	require.Equal(t, "timeout-checkpoint", timeoutOutputs[0].GetHeartbeat().GetValue().GetStringValue())
	require.Equal(t, "timeout-progress", timeoutOutputs[1].GetStreamWrite().GetValue().GetStringValue())
	require.NotNil(t, timeoutOutputs[2].GetResult())

	_, err = client.InvokeWorkerRPC(context.Background(), workerRPCRequest(
		t,
		workerTestInput{OrderID: "order-1", Mode: "stream"},
	))
	require.ErrorContains(t, err, "invalid invocation context")
	_, err = client.InvokeWorkerRPC(context.Background(), workerRPCRequest(
		t,
		workerTestInput{OrderID: "order-1", Mode: "heartbeat"},
	))
	require.ErrorContains(t, err, "invalid invocation context")
}

func TestWorkerStepOutputEmitterSerializesAndRetainsSendFailure(t *testing.T) {
	stream := &workerTestProgressStream{}
	handlerContext, emitter := newStepOutputEmitter(context.Background(), stream)
	var waitGroup sync.WaitGroup
	for index := range 20 {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			if index%2 == 0 {
				require.NoError(t, emitter.sendHeartbeat(&dexpb.StepMethodHeartbeat{}))
				return
			}
			require.NoError(t, emitter.sendStreamWrite(&dexpb.StepStreamWrite{}))
		}()
	}
	waitGroup.Wait()
	require.Len(t, stream.frames, 20)
	require.NoError(t, emitter.close())
	require.ErrorIs(t, emitter.sendHeartbeat(&dexpb.StepMethodHeartbeat{}), errInvalidInvocationContext)
	require.ErrorIs(t, handlerContext.Err(), context.Canceled)

	sendFailure := errors.New("send failed")
	failingStream := &workerTestProgressStream{failure: sendFailure, failAfter: 1}
	failingContext, failingEmitter := newStepOutputEmitter(context.Background(), failingStream)
	require.NoError(t, failingEmitter.sendHeartbeat(&dexpb.StepMethodHeartbeat{}))
	require.ErrorIs(t, failingEmitter.sendStreamWrite(&dexpb.StepStreamWrite{}), sendFailure)
	require.ErrorIs(t, failingEmitter.sendHeartbeat(&dexpb.StepMethodHeartbeat{}), sendFailure)
	require.ErrorIs(t, failingEmitter.close(), sendFailure)
	require.ErrorIs(t, failingContext.Err(), context.Canceled)
	require.Len(t, failingStream.frames, 1)
}

func TestBufferedTextStreamFlushesOnTimerAndInvocationFinish(t *testing.T) {
	progressStream := &bufferedTestProgressStream{}
	registry, err := NewRegistry([]Flow{workerFlow})
	require.NoError(t, err)
	registeredFlow, found := registry.lookupFlow(GetFinalFlowType(workerFlow))
	require.True(t, found)
	invocation, err := newInvocationContext(
		context.Background(),
		invocationExecute,
		registeredFlow,
		progressStream,
		workerStepContext(),
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
	)
	require.NoError(t, err)
	writer, err := NewBufferedTextStream(
		invocation,
		workerTestThinking,
		BufferedTextStreamFlushInterval(5*time.Millisecond),
	)
	require.NoError(t, err)
	require.NoError(t, writer.Write("hello"))
	require.Eventually(t, func() bool {
		return len(progressStream.values()) == 1
	}, time.Second, time.Millisecond)
	require.NoError(t, writer.Write(" 世界"))
	require.NoError(t, invocation.finish())
	require.Equal(t, []string{"hello", " 世界"}, progressStream.values())
	require.ErrorIs(t, writer.Write("late"), errInvalidInvocationContext)
}

func TestInvocationContextExposesRecoveryError(t *testing.T) {
	registry, err := NewRegistry([]Flow{workerFlow})
	require.NoError(t, err)
	registeredFlow, found := registry.lookupFlow(GetFinalFlowType(workerFlow))
	require.True(t, found)
	metadata := workerStepContext()
	metadata.RecoveryError = &dexpb.RecoveryErrorInfo{
		Detail:    "handler exhausted retries",
		ErrorType: "TimeoutError",
	}
	invocation, err := newInvocationContext(
		context.Background(),
		invocationExecute,
		registeredFlow,
		nil,
		metadata,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, &RecoveryErrorInfo{
		Detail:    "handler exhausted retries",
		ErrorType: "TimeoutError",
	}, invocation.RecoveryError())
}

func TestWorkerServiceMapsErrorsAndDiscardsResponses(t *testing.T) {
	client, closeService := newWorkerTestClient(t, nil)
	defer closeService()

	tests := []struct {
		name      string
		call      func() error
		code      codes.Code
		detail    string
		errorType string
	}{
		{
			name: "unknown flow",
			call: func() error {
				_, err := client.InvokeWorkerRPC(context.Background(), &dexpb.InvokeWorkerRPCRequest{
					Context: workerRPCContext(), FlowType: "missing", RpcName: "Update",
					Input: mustEncodeWorkerTestValue(t, workerTestInput{}),
				})
				return err
			},
			code: codes.NotFound,
		},
		{
			name: "execute-only WaitFor",
			call: func() error {
				return invokeWaitForError(client, &dexpb.InvokeWaitForMethodRequest{
					Context: workerStepContext(), FlowType: GetFinalFlowType(workerFlow),
					StepType:  GetFinalStepType(workerTestFinish),
					StepInput: mustEncodeWorkerTestValue(t, workerTestInput{}),
				})
			},
			code: codes.FailedPrecondition,
		},
		{
			name: "unknown step",
			call: func() error {
				return invokeExecuteError(client, &dexpb.InvokeExecuteMethodRequest{
					Context: workerStepContext(), FlowType: GetFinalFlowType(workerFlow),
					StepType: "missing", StepInput: mustEncodeWorkerTestValue(t, workerTestInput{}),
				})
			},
			code: codes.NotFound,
		},
		{
			name: "unknown RPC",
			call: func() error {
				_, err := client.InvokeWorkerRPC(context.Background(), &dexpb.InvokeWorkerRPCRequest{
					Context: workerRPCContext(), FlowType: GetFinalFlowType(workerFlow),
					RpcName: "Missing", Input: mustEncodeWorkerTestValue(t, workerTestInput{}),
				})
				return err
			},
			code: codes.NotFound,
		},
		{
			name: "nil wait",
			call: func() error {
				return invokeWaitForError(client,
					&dexpb.InvokeWaitForMethodRequest{
						Context:  workerStepContext(),
						FlowType: GetFinalFlowType(workerFlow),
						StepType: GetFinalStepType(workerTestWait),
						StepInput: mustEncodeWorkerTestValue(
							t,
							workerTestInput{OrderID: "order-1", Mode: "nil-wait"},
						),
					},
				)
			},
			code:   codes.InvalidArgument,
			detail: "WaitFor returned nil",
			errorType: fmt.Sprintf(
				"%T",
				&InvalidStepResultError{},
			),
		},
		{
			name: "nil decision",
			call: func() error {
				return invokeExecuteError(client,
					workerExecuteRequest(t, workerTestInput{OrderID: "order-1", Mode: "empty"}),
				)
			},
			code:   codes.InvalidArgument,
			detail: "Execute returned nil",
			errorType: fmt.Sprintf(
				"%T",
				&InvalidStepResultError{},
			),
		},
		{
			name: "nil RPC result",
			call: func() error {
				_, err := client.InvokeWorkerRPC(context.Background(), workerRPCRequest(
					t, workerTestInput{Mode: "nil-result"},
				))
				return err
			},
			code:   codes.InvalidArgument,
			detail: "RPC returned nil",
			errorType: fmt.Sprintf(
				"%T",
				&InvalidStepResultError{},
			),
		},
		{
			name: "application error",
			call: func() error {
				return invokeExecuteError(client,
					workerExecuteRequest(t, workerTestInput{OrderID: "order-1", Mode: "error"}),
				)
			},
			code: codes.Unknown,
		},
		{
			name: "application status",
			call: func() error {
				_, err := client.InvokeWorkerRPC(context.Background(), workerRPCRequest(
					t, workerTestInput{Mode: "status"},
				))
				return err
			},
			code: codes.PermissionDenied,
		},
		{
			name: "panic",
			call: func() error {
				_, err := client.InvokeWorkerRPC(context.Background(), workerRPCRequest(
					t, workerTestInput{Mode: "panic"},
				))
				return err
			},
			code: codes.Internal,
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			err := testCase.call()
			require.Error(t, err)
			rpcStatus := status.Convert(err)
			require.Equal(t, testCase.code, rpcStatus.Code())
			if testCase.detail != "" {
				require.Contains(t, rpcStatus.Message(), testCase.detail)
			}
			details := requireWorkerErrorDetail(t, rpcStatus)
			if testCase.errorType != "" {
				require.Equal(t, testCase.errorType, details.ErrorType)
				require.Contains(t, details.Detail, GetFinalFlowType(workerFlow))
			}
		})
	}
}

func TestWorkerLifecycleAndTargetDefaults(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))
	concrete, err := newWorkerForTest(t, []Flow{workerFlow}, WorkerOptions{
		BindAddress: "127.0.0.1:9900",
		Logger:      logger,
	})
	require.NoError(t, err)
	require.Same(t, logger, concrete.logger)
	require.Equal(t, "127.0.0.1:9900", concrete.WorkerTarget().Address)
	require.NoError(t, concrete.Stop(context.Background()))

	wildcard, err := newWorkerForTest(t, []Flow{workerFlow}, WorkerOptions{BindAddress: ":8804"})
	require.NoError(t, err)
	require.Equal(t, "localhost:8804", wildcard.WorkerTarget().Address)
	require.NoError(t, wildcard.Stop(context.Background()))
	require.Error(t, wildcard.Start())

	address := unusedWorkerAddress(t)
	worker, err := newWorkerForTest(t, []Flow{workerFlow}, WorkerOptions{
		BindAddress: address,
		WorkerTarget: WorkerTarget{
			Address:  "worker.example:8803",
			Headless: true,
		},
	})
	require.NoError(t, err)
	require.Equal(t, &WorkerTarget{Address: "worker.example:8803", Headless: true}, worker.WorkerTarget())
	startResult := make(chan error, 1)
	go func() {
		startResult <- worker.Start()
	}()

	conn, err := grpc.NewClient(
		address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, conn.Close()) }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = dexpb.NewWorkerServiceClient(conn).InvokeWorkerRPC(
		ctx,
		workerRPCRequest(t, workerTestInput{OrderID: "order-1"}),
		grpc.WaitForReady(true),
	)
	require.NoError(t, err)
	require.NoError(t, worker.Stop(context.Background()))
	require.NoError(t, <-startResult)
	require.NoError(t, worker.Stop(context.Background()))
	stoppedCtx, cancelStopped := context.WithCancel(context.Background())
	cancelStopped()
	require.NoError(t, worker.Stop(stoppedCtx))

	_, err = newWorkerForTest(t, []Flow{workerFlow}, WorkerOptions{WorkerTarget: WorkerTarget{Address: "https://worker"}})
	require.ErrorContains(t, err, "plaintext")
	_, err = newWorkerForTest(t, []Flow{workerFlow}, WorkerOptions{BindAddress: ":0"})
	require.ErrorContains(t, err, "1-65535")
	_, err = newWorkerForTest(t, []Flow{workerFlow}, WorkerOptions{AttributeIndexSyncTimeout: -time.Second})
	require.ErrorContains(t, err, "sync timeout must be positive")
}

func TestWorkerSynchronizesAttributeIndexesBeforeListening(t *testing.T) {
	indexedStatus := DefineAttribute[string](
		"status",
		Indexed(AttributeIndex{Type: IndexKeyword, IndexKey: "WorkerTestStatus"}),
	)
	flow := &registrationFlow{
		flowType: "worker-index-sync",
		schema: PersistenceSchema{Attributes: []AttributeDef{
			indexedStatus,
		}},
	}
	address := unusedWorkerAddress(t)
	flowService := &workerSyncFlowService{
		probeAddress: address,
		received:     make(chan *dexpb.SyncAttributeIndexRequest, 1),
		wasListening: make(chan bool, 1),
		serverInfo: &dexpb.ServerInfo{
			ServerVersion:                   "test",
			MinimumSupportedProtocolVersion: 1,
			CurrentProtocolVersion:          2,
		},
	}
	flowServiceAddress := startWorkerFlowService(t, flowService)
	worker, err := newWorkerForTest(t, []Flow{flow}, WorkerOptions{
		BindAddress:        address,
		FlowServiceAddress: flowServiceAddress,
	})
	require.NoError(t, err)
	startResult := make(chan error, 1)
	go func() { startResult <- worker.Start() }()

	request := <-flowService.received
	require.Equal(t, map[string]dexpb.IndexType{
		"WorkerTestStatus": dexpb.IndexType_INDEX_TYPE_KEYWORD,
	}, request.AttributeIndexes)
	require.False(t, <-flowService.wasListening)
	require.Equal(t, uint32(2), worker.serverProtocol.version.Load())
	require.Equal(t, "dev", worker.sdkVersion)
	require.Equal(t, []string{"GetServerInfo", "SyncAttributeIndexes"}, flowService.recordedCalls())
	require.Eventually(t, func() bool {
		connection, dialErr := net.DialTimeout("tcp", address, 50*time.Millisecond)
		if dialErr != nil {
			return false
		}
		require.NoError(t, connection.Close())
		return true
	}, 5*time.Second, 10*time.Millisecond)
	require.NoError(t, worker.Stop(context.Background()))
	require.NoError(t, <-startResult)
}

func TestWorkerProtocolFailureKeepsIndexesUnsynchronizedAndPortClosed(t *testing.T) {
	testCases := []struct {
		name              string
		serverInfo        *dexpb.ServerInfo
		serverInfoFailure error
		errorDetail       string
	}{
		{
			name: "Server minimum exceeds SDK maximum",
			serverInfo: &dexpb.ServerInfo{
				ServerVersion:                   "future",
				MinimumSupportedProtocolVersion: maximumSupportedServerProtocolVersion + 1,
				CurrentProtocolVersion:          maximumSupportedServerProtocolVersion + 1,
			},
			errorDetail: "do not overlap",
		},
		{
			name: "Server current is below SDK minimum",
			serverInfo: &dexpb.ServerInfo{
				ServerVersion:                   "old",
				MinimumSupportedProtocolVersion: 1,
				CurrentProtocolVersion:          1,
			},
			errorDetail: "do not overlap",
		},
		{
			name: "zero interval",
			serverInfo: &dexpb.ServerInfo{
				ServerVersion: "invalid",
			},
			errorDetail: "interval is invalid",
		},
		{
			name: "reversed interval",
			serverInfo: &dexpb.ServerInfo{
				ServerVersion:                   "invalid",
				MinimumSupportedProtocolVersion: 2,
				CurrentProtocolVersion:          1,
			},
			errorDetail: "interval is invalid",
		},
		{
			name:              "unimplemented",
			serverInfoFailure: status.Error(codes.Unimplemented, "old Server"),
			errorDetail:       "GetServerInfo failed",
		},
		{
			name:              "request failure",
			serverInfoFailure: status.Error(codes.Unavailable, "offline"),
			errorDetail:       "GetServerInfo failed",
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			address := unusedWorkerAddress(t)
			flowService := &workerSyncFlowService{
				serverInfo:        testCase.serverInfo,
				serverInfoFailure: testCase.serverInfoFailure,
			}
			flowServiceAddress := startWorkerFlowService(t, flowService)
			worker, err := newWorkerForTest(t, []Flow{workerFlow}, WorkerOptions{
				BindAddress:        address,
				FlowServiceAddress: flowServiceAddress,
			})
			require.NoError(t, err)
			err = worker.Start()
			require.ErrorContains(t, err, testCase.errorDetail)
			require.Contains(t, err.Error(), "Go SDK version")
			require.Equal(t, []string{"GetServerInfo"}, flowService.recordedCalls())
			connection, dialErr := net.DialTimeout("tcp", address, 50*time.Millisecond)
			if connection != nil {
				require.NoError(t, connection.Close())
			}
			require.Error(t, dialErr)
		})
	}
}

func TestWorkerAttributeIndexSyncFailureKeepsPortClosed(t *testing.T) {
	address := unusedWorkerAddress(t)
	flowServiceAddress := startWorkerFlowService(t, &workerSyncFlowService{
		attributeFailure: status.Error(codes.PermissionDenied, "denied"),
	})
	worker, err := newWorkerForTest(t, []Flow{workerFlow}, WorkerOptions{
		BindAddress:        address,
		FlowServiceAddress: flowServiceAddress,
	})
	require.NoError(t, err)
	err = worker.Start()
	require.ErrorContains(t, err, "sync attribute indexes")
	require.Equal(t, codes.PermissionDenied, status.Code(err))

	connection, dialErr := net.DialTimeout("tcp", address, 50*time.Millisecond)
	if dialErr == nil {
		require.NoError(t, connection.Close())
	}
	require.Error(t, dialErr)
}

func TestWorkerHydrationUsesLoadBlobsAndDiskCache(t *testing.T) {
	flowService := &workerBlobFlowService{}
	client, closeFlowService := newFlowServiceTestClient(t, flowService)
	defer closeFlowService()
	cache, err := blobcache.New(&blobcache.Config{
		Dir:      t.TempDir(),
		MaxBytes: 1 << 20,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, cache.Close()) }()
	hydrator := newValueHydrator(client, cache, nil, "flow-test")
	request := &dexpb.Value{Kind: &dexpb.Value_InternalBlobIdForStringValue{
		InternalBlobIdForStringValue: "blob-1",
	}}

	first := request
	err = hydrator.HydrateValuesInPlace(context.Background(), valuePointersForFlow("flow-1", []**dexpb.Value{&first}))
	require.NoError(t, err)
	require.Equal(t, "loaded-blob-1", first.GetStringValue())
	second := request
	err = hydrator.HydrateValuesInPlace(context.Background(), valuePointersForFlow("flow-1", []**dexpb.Value{&second}))
	require.NoError(t, err)
	require.Equal(t, "loaded-blob-1", second.GetStringValue())
	require.Equal(t, 1, flowService.callCount())

	cached, err := cache.Put(flowBlobCacheKey("flow-1", "corrupt"), []byte{0xff})
	require.NoError(t, err)
	require.True(t, cached)
	corruptRequest := &dexpb.Value{Kind: &dexpb.Value_InternalBlobIdForStringValue{
		InternalBlobIdForStringValue: "corrupt",
	}}
	loaded := corruptRequest
	err = hydrator.HydrateValuesInPlace(context.Background(), valuePointersForFlow("flow-1", []**dexpb.Value{&loaded}))
	require.NoError(t, err)
	require.Equal(t, "loaded-corrupt", loaded.GetStringValue())
	require.Equal(t, 2, flowService.callCount())

	encodedInput := mustEncodeWorkerTestValue(t, workerTestInput{OrderID: "order-1"})
	flowService.setValue("input-blob", encodedInput)
	workerClient, closeWorker := newWorkerTestClient(
		t,
		newValueHydrator(client, cache, nil, "flow-test"),
	)
	defer closeWorker()
	rpcRequest := workerRPCRequest(t, workerTestInput{})
	rpcRequest.Input = &dexpb.Value{Kind: &dexpb.Value_InternalBlobIdForObjValue{
		InternalBlobIdForObjValue: "input-blob",
	}}
	response, err := workerClient.InvokeWorkerRPC(context.Background(), rpcRequest)
	require.NoError(t, err)
	var output workerTestOutput
	require.NoError(t, decodeValue(response.Output, &output))
	require.Equal(t, workerTestOutput{Before: 0, After: 1}, output)

	flowService.setValue("channel-blob", mustEncodeWorkerTestValue(t, "hydrated-message"))
	pendingRequest := workerRPCRequest(t, workerTestInput{Mode: "pending"})
	pendingRequest.LoadedChannelNames = []string{"commands"}
	pendingRequest.LoadedChannelMessages = map[string]*dexpb.ChannelValues{
		"commands": {Messages: []*dexpb.ChannelMessage{{
			MessageId: "message-1",
			Value: &dexpb.Value{Kind: &dexpb.Value_InternalBlobIdForStringValue{
				InternalBlobIdForStringValue: "channel-blob",
			}},
		}}},
	}
	pendingResponse, err := workerClient.InvokeWorkerRPC(context.Background(), pendingRequest)
	require.NoError(t, err)
	require.NoError(t, decodeValue(pendingResponse.Output, &output))
	require.Equal(t, workerTestOutput{Before: 1, After: 1}, output)

	wrongID := "wrong-kind"
	flowService.setValue(wrongID, encodedInput)
	wrongRequest := &dexpb.Value{Kind: &dexpb.Value_InternalBlobIdForStringValue{
		InternalBlobIdForStringValue: wrongID,
	}}
	wrongValue := wrongRequest
	err = hydrator.HydrateValuesInPlace(context.Background(), valuePointersForFlow("flow-1", []**dexpb.Value{&wrongValue}))
	require.ErrorContains(t, err, "hydrated to")
	require.Same(t, wrongRequest, wrongValue)

	omittedID := "omitted"
	flowService.omitValue(omittedID)
	omittedRequest := &dexpb.Value{Kind: &dexpb.Value_InternalBlobIdForStringValue{
		InternalBlobIdForStringValue: omittedID,
	}}
	omittedValue := omittedRequest
	err = hydrator.HydrateValuesInPlace(context.Background(), valuePointersForFlow("flow-1", []**dexpb.Value{&omittedValue}))
	require.ErrorContains(t, err, "omitted blob")
	require.Same(t, omittedRequest, omittedValue)

	var logOutput bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logOutput, nil))
	closedCache, err := blobcache.New(&blobcache.Config{
		Dir:      t.TempDir(),
		MaxBytes: 1 << 20,
		Logger:   logger,
	})
	require.NoError(t, err)
	require.NoError(t, closedCache.Close())
	uncachedHydrator := newValueHydrator(client, closedCache, nil, "flow-test")
	uncached := request
	err = uncachedHydrator.HydrateValuesInPlace(
		context.Background(),
		valuePointersForFlow("flow-1", []**dexpb.Value{&uncached}),
	)
	require.NoError(t, err)
	require.Equal(t, "loaded-blob-1", uncached.GetStringValue())
	require.Contains(t, logOutput.String(), "read blob cache")
	require.Contains(t, logOutput.String(), "write blob cache")
}

func TestWorkerHydrationReportsEveryOmittedBlobAndValue(t *testing.T) {
	var logOutput bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logOutput, nil))
	flowService := &workerBlobFlowService{}
	for _, blobID := range []string{"input", "live", "selected", "mapped", "heartbeat", "local", "condition", "completion"} {
		flowService.omitValue(blobID)
	}
	flowServiceAddress := startWorkerFlowService(t, flowService)
	address := unusedWorkerAddress(t)
	worker, err := newWorkerForTest(t, []Flow{workerFlow}, WorkerOptions{
		BindAddress: address, FlowServiceAddress: "  " + flowServiceAddress + "  ", Logger: logger,
	})
	require.NoError(t, err)
	startResult := make(chan error, 1)
	go func() { startResult <- worker.Start() }()
	t.Cleanup(func() {
		require.NoError(t, worker.Stop(context.Background()))
		require.NoError(t, <-startResult)
	})
	connection, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer func() { require.NoError(t, connection.Close()) }()
	client := dexpb.NewWorkerServiceClient(connection)
	require.Eventually(t, func() bool {
		worker.lifecycleMu.Lock()
		defer worker.lifecycleMu.Unlock()
		return worker.state == workerRunning
	}, 5*time.Second, 10*time.Millisecond)

	for _, method := range []string{"WaitFor", "Execute", "RPC", "Timeout"} {
		t.Run(method, func(t *testing.T) {
			flowID := "flow-" + t.Name()
			attributes := []*dexpb.KV{
				{Key: "LiveExecution", Value: workerTestStringBlob("live")},
				{Key: "SelectedExecution", Value: workerTestStringBlob("selected")},
				{Key: "items/order-1", Value: workerTestStringBlob("mapped")},
				{Key: "present", Value: workerTestStringBlob("available")},
			}
			loadedMessages := map[string]*dexpb.ChannelValues{
				"commands":                  {Messages: []*dexpb.ChannelMessage{{MessageId: "message-1", Value: workerTestStringBlob("live")}}},
				"commands-by-order/order-1": {Messages: []*dexpb.ChannelMessage{{MessageId: "message-2", Value: workerTestStringBlob("selected")}}},
			}
			expected := []string{
				`blob "live"`, `Attribute "LiveExecution"`, `blob "selected"`, `Attribute "SelectedExecution"`,
				`blob "mapped"`, `Attribute "items/order-1"`, `Channel "commands" message "message-1"`,
				`Channel "commands-by-order/order-1" message "message-2"`,
				fmt.Sprintf("Flow ID %q", flowID), fmt.Sprintf("FlowServiceAddress %q", flowServiceAddress),
				"Worker's Flow service may differ from the dispatching Server",
			}
			var invocationError error
			switch method {
			case "RPC":
				request := workerRPCRequest(t, workerTestInput{})
				request.Context.FlowId = flowID
				request.Input = &dexpb.Value{Kind: &dexpb.Value_InternalBlobIdForObjValue{InternalBlobIdForObjValue: "input"}}
				request.Attributes = attributes
				request.LoadedChannelMessages = loadedMessages
				_, invocationError = client.InvokeWorkerRPC(context.Background(), request)
				expected = append(expected, `blob "input"`, "RPC input")
			case "WaitFor":
				request := &dexpb.InvokeWaitForMethodRequest{
					Context: workerStepContext(), FlowType: GetFinalFlowType(workerFlow),
					StepType: GetFinalStepType(workerTestWait), StepInput: &dexpb.Value{Kind: &dexpb.Value_InternalBlobIdForObjValue{InternalBlobIdForObjValue: "input"}},
					Attributes: attributes, LoadedChannelMessages: loadedMessages,
				}
				request.Context.FlowId = flowID
				request.Context.LastHeartbeatValue = workerTestStringBlob("heartbeat")
				_, invocationError = collectWaitForOutputs(client, request)
				expected = append(expected, `blob "input"`, "Step input", `blob "heartbeat"`, "last heartbeat")
			case "Execute", "Timeout":
				request := workerExecuteRequest(t, workerTestInput{})
				request.Context.FlowId = flowID
				request.StepInput = &dexpb.Value{Kind: &dexpb.Value_InternalBlobIdForObjValue{InternalBlobIdForObjValue: "input"}}
				request.Context.LastHeartbeatValue = workerTestStringBlob("heartbeat")
				request.Attributes = attributes
				request.StepExeLocals = []*dexpb.KV{{Key: "execution", Value: workerTestStringBlob("local")}}
				request.LoadedChannelMessages = loadedMessages
				request.ConditionResults.ChannelResults[0].Values = []*dexpb.Value{workerTestStringBlob("condition")}
				request.ConditionResults.SubFlowResults = []*dexpb.FlowResult{{Results: []*dexpb.StepCompletionOutput{{
					CompletedStepOutput: workerTestStringBlob("completion"),
				}}}}
				if method == "Timeout" {
					request.StepType = timeoutHandlerStepType
					request.StepInput = nil
				} else {
					expected = append(expected, `blob "input"`, "Step input")
				}
				_, invocationError = collectExecuteOutputs(client, request)
				expected = append(expected, `blob "heartbeat"`, "last heartbeat", `blob "local"`, `Step-execution local "execution"`,
					`blob "condition"`, `Channel "commands" condition "command-1" value at index 0`,
					`blob "completion"`, "SubFlow result at index 0 completion at index 0 Step output")
			}
			require.Equal(t, codes.Internal, status.Code(invocationError))
			for _, description := range expected {
				require.ErrorContains(t, invocationError, description)
			}
			require.Equal(t, 1, strings.Count(invocationError.Error(), `blob "live"`))
			require.Equal(t, 1, strings.Count(invocationError.Error(), `blob "selected"`))
			require.NotContains(t, invocationError.Error(), `blob "available"`)
			details := requireWorkerErrorDetail(t, status.Convert(invocationError))
			require.Equal(t, status.Convert(invocationError).Message(), details.Detail)
			require.NotContains(t, details.Detail, string(errorDetailTruncationMarker))
		})
	}
	t.Run("large diagnostic logs every missing value", func(t *testing.T) {
		request := workerRPCRequest(t, workerTestInput{})
		request.Context.FlowId = "flow-" + t.Name()
		for index := range 30 {
			blobID := fmt.Sprintf("omitted-%d", index)
			flowService.omitValue(blobID)
			request.Attributes = append(request.Attributes, &dexpb.KV{
				Key: "attribute-" + blobID, Value: workerTestStringBlob(blobID),
			})
		}
		_, invocationError := client.InvokeWorkerRPC(context.Background(), request)
		require.Equal(t, codes.Internal, status.Code(invocationError))
		require.ErrorContains(t, invocationError, fmt.Sprintf("FlowServiceAddress %q", flowServiceAddress))
		require.ErrorContains(t, invocationError, "Worker's Flow service may differ from the dispatching Server")
		require.ErrorContains(t, invocationError, string(errorDetailTruncationMarker))
		require.Contains(t, logOutput.String(), request.Context.FlowId)
		for _, attribute := range request.Attributes {
			require.Contains(t, logOutput.String(), attribute.Key)
			require.Contains(t, logOutput.String(), attribute.Value.GetInternalBlobIdForStringValue())
		}
	})
}

func TestWorkerHydrationReportsMissingBlobsAcrossFlows(t *testing.T) {
	flowService := &workerBlobFlowService{}
	client, closeFlowService := newFlowServiceTestClient(t, flowService)
	defer closeFlowService()
	hydrator := newValueHydrator(client, newHydrationCache(t), nil, "flow-test")
	cached := workerTestStringBlob("shared")
	require.NoError(t, hydrator.HydrateValuesInPlace(context.Background(), []flowValuePointer{
		{flowID: "cached-flow", valuePointer: &cached},
	}))
	flowService.omitValue("shared")
	flowService.omitValue("missing")
	values := []*dexpb.Value{
		workerTestStringBlob("shared"), workerTestStringBlob("shared"),
		workerTestStringBlob("shared"), workerTestStringBlob("missing"),
	}
	originalValues := append([]*dexpb.Value(nil), values...)
	err := hydrator.HydrateValuesInPlace(context.Background(), []flowValuePointer{
		{flowID: "cached-flow", valueDescription: `Attribute "cached"`, valuePointer: &values[0]},
		{flowID: "first-flow", valueDescription: `Attribute "first"`, valuePointer: &values[1]},
		{flowID: "second-flow", valueDescription: `Attribute "second"`, valuePointer: &values[2]},
		{flowID: "first-flow", valueDescription: `Attribute "other"`, valuePointer: &values[3]},
	})
	require.ErrorContains(t, err, `Flow ID "first-flow": blob "shared" (Attribute "first"); blob "missing" (Attribute "other")`)
	require.ErrorContains(t, err, `Flow ID "second-flow": blob "shared" (Attribute "second")`)
	require.NotContains(t, err.Error(), "cached-flow")
	for index, originalValue := range originalValues {
		require.Same(t, originalValue, values[index])
	}
}

func workerTestStringBlob(blobID string) *dexpb.Value {
	return &dexpb.Value{Kind: &dexpb.Value_InternalBlobIdForStringValue{InternalBlobIdForStringValue: blobID}}
}

type workerBlobFlowService struct {
	workerSyncFlowService
	mu      sync.Mutex
	calls   int
	values  map[string]*dexpb.Value
	omitted map[string]struct{}
}

type workerSyncFlowService struct {
	dexpb.UnimplementedFlowServiceServer
	probeAddress      string
	received          chan *dexpb.SyncAttributeIndexRequest
	wasListening      chan bool
	serverInfo        *dexpb.ServerInfo
	serverInfoFailure error
	attributeFailure  error
	calls             []string
	callsMu           sync.Mutex
}

func (service *workerSyncFlowService) GetServerInfo(
	_ context.Context,
	_ *emptypb.Empty,
) (*dexpb.ServerInfo, error) {
	service.callsMu.Lock()
	service.calls = append(service.calls, "GetServerInfo")
	service.callsMu.Unlock()
	if service.serverInfoFailure != nil {
		return nil, service.serverInfoFailure
	}
	if service.serverInfo != nil {
		return service.serverInfo, nil
	}
	return &dexpb.ServerInfo{
		ServerVersion:                   "test",
		MinimumSupportedProtocolVersion: 2,
		CurrentProtocolVersion:          2,
	}, nil
}

func (service *workerSyncFlowService) SyncAttributeIndexes(
	_ context.Context,
	request *dexpb.SyncAttributeIndexRequest,
) (*dexpb.SyncAttributeIndexResponse, error) {
	service.callsMu.Lock()
	service.calls = append(service.calls, "SyncAttributeIndexes")
	service.callsMu.Unlock()
	if service.probeAddress != "" {
		connection, dialErr := net.DialTimeout("tcp", service.probeAddress, 50*time.Millisecond)
		listening := dialErr == nil
		if connection != nil {
			if err := connection.Close(); err != nil {
				return nil, err
			}
		}
		service.wasListening <- listening
	}
	if service.received != nil {
		service.received <- request
	}
	if service.attributeFailure != nil {
		return nil, service.attributeFailure
	}
	return &dexpb.SyncAttributeIndexResponse{}, nil
}

func (service *workerSyncFlowService) recordedCalls() []string {
	service.callsMu.Lock()
	defer service.callsMu.Unlock()
	return append([]string(nil), service.calls...)
}

func (service *workerBlobFlowService) LoadBlobs(
	_ context.Context,
	request *dexpb.LoadBlobsRequest,
) (*dexpb.LoadBlobsResponse, error) {
	service.mu.Lock()
	service.calls++
	service.mu.Unlock()
	values := make(map[string]*dexpb.Value, len(request.Entries))
	for _, entry := range request.Entries {
		value := entry.GetBlobValue()
		id := value.GetInternalBlobIdForStringValue()
		if id == "" {
			id = value.GetInternalBlobIdForObjValue()
		}
		service.mu.Lock()
		configured := service.values[id]
		_, omitted := service.omitted[id]
		service.mu.Unlock()
		if omitted {
			continue
		}
		if configured != nil {
			values[id] = configured
			continue
		}
		values[id] = &dexpb.Value{Kind: &dexpb.Value_StringValue{StringValue: "loaded-" + id}}
	}
	return &dexpb.LoadBlobsResponse{Values: values}, nil
}

func (service *workerBlobFlowService) setValue(id string, value *dexpb.Value) {
	service.mu.Lock()
	defer service.mu.Unlock()
	if service.values == nil {
		service.values = make(map[string]*dexpb.Value)
	}
	service.values[id] = value
}

func (service *workerBlobFlowService) omitValue(id string) {
	service.mu.Lock()
	defer service.mu.Unlock()
	if service.omitted == nil {
		service.omitted = make(map[string]struct{})
	}
	service.omitted[id] = struct{}{}
}

func (service *workerBlobFlowService) callCount() int {
	service.mu.Lock()
	defer service.mu.Unlock()
	return service.calls
}

func TestWorkerConcurrentInvocationsAreIsolated(t *testing.T) {
	client, closeService := newWorkerTestClient(t, nil)
	defer closeService()

	const callCount = 24
	requests := make([]*dexpb.InvokeWorkerRPCRequest, callCount)
	for index := range requests {
		request := workerRPCRequest(t, workerTestInput{OrderID: fmt.Sprintf("order-%d", index)})
		request.ChannelInfos = map[string]*dexpb.ChannelInfo{
			"commands": {Size: int32(index)},
		}
		requests[index] = request
	}

	errorsByCall := make(chan error, callCount)
	var waitGroup sync.WaitGroup
	for index := range requests {
		waitGroup.Add(1)
		go func(request *dexpb.InvokeWorkerRPCRequest, expected int) {
			defer waitGroup.Done()
			response, err := client.InvokeWorkerRPC(context.Background(), request)
			if err == nil {
				var output workerTestOutput
				err = decodeValue(response.Output, &output)
				if err == nil && output != (workerTestOutput{Before: expected, After: expected + 1}) {
					err = fmt.Errorf("unexpected output: %+v", output)
				}
			}
			errorsByCall <- err
		}(requests[index], index)
	}
	waitGroup.Wait()
	close(errorsByCall)
	for err := range errorsByCall {
		require.NoError(t, err)
	}
}

func TestWorkerStopDrainsAndForceCancels(t *testing.T) {
	t.Run("graceful drain", func(t *testing.T) {
		flow := &workerBlockingFlow{entered: make(chan struct{}), release: make(chan struct{})}
		worker, client, startResult, closeConn := startBlockingWorker(t, flow)
		defer closeConn()
		rpcResult := invokeBlockingRPC(client)
		<-flow.entered
		stopResult := make(chan error, 1)
		go func() { stopResult <- worker.Stop(context.Background()) }()
		close(flow.release)
		require.NoError(t, <-rpcResult)
		require.NoError(t, <-stopResult)
		require.NoError(t, <-startResult)
	})

	t.Run("force cancel", func(t *testing.T) {
		flow := &workerBlockingFlow{entered: make(chan struct{}), release: make(chan struct{})}
		worker, client, startResult, closeConn := startBlockingWorker(t, flow)
		defer closeConn()
		rpcResult := invokeBlockingRPC(client)
		<-flow.entered
		stopCtx, cancel := context.WithCancel(context.Background())
		cancel()
		require.ErrorIs(t, worker.Stop(stopCtx), context.Canceled)
		require.Error(t, <-rpcResult)
		require.NoError(t, <-startResult)
	})
}

func TestWorkerRegisteredDecisionMapping(t *testing.T) {
	registered, err := NewRegistry([]Flow{workerFlow})
	require.NoError(t, err)
	flow, found := registered.lookupFlow(GetFinalFlowType(workerFlow))
	require.True(t, found)

	tests := []struct {
		name     string
		decision *StepDecision
		close    dexpb.CloseDecisionType
	}{
		{name: "graceful", decision: GracefulComplete("done"), close: dexpb.CloseDecisionType_CLOSE_DECISION_TYPE_GRACEFUL_COMPLETE},
		{name: "force complete", decision: ForceComplete("done"), close: dexpb.CloseDecisionType_CLOSE_DECISION_TYPE_FORCE_COMPLETE},
		{name: "force fail", decision: ForceFail("failed"), close: dexpb.CloseDecisionType_CLOSE_DECISION_TYPE_FORCE_FAIL},
		{name: "dead end", decision: DeadEnd(), close: dexpb.CloseDecisionType_CLOSE_DECISION_TYPE_DEAD_END},
		{
			name: "conditional",
			decision: ForceCompleteIfChannelsEmpty(
				"done",
				[]ChannelDef{workerTestCommands},
				MovementOf(workerTestFinish, workerTestInput{}),
			),
			close: dexpb.CloseDecisionType_CLOSE_DECISION_TYPE_FORCE_COMPLETE_ON_CHANNELS_EMPTY,
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			mapped, err := mapRegisteredDecision(flow, testCase.decision)
			require.NoError(t, err)
			require.Equal(t, testCase.close, mapped.CloseDecision.CloseDecisionType)
		})
	}

	mapped, err := mapRegisteredDecision(
		flow,
		GoTo(workerTestFinish, workerTestInput{}),
	)
	require.NoError(t, err)
	require.Len(t, mapped.NextSteps, 1)
}

func newWorkerForTest(
	t *testing.T,
	flows []Flow,
	options WorkerOptions,
) (*Worker, error) {
	t.Helper()
	if options.FlowServiceAddress == "" {
		options.FlowServiceAddress = startWorkerFlowService(t, &workerSyncFlowService{})
	}
	registry, err := NewRegistry(flows)
	if err != nil {
		return nil, err
	}
	cache, err := blobcache.New(&blobcache.Config{
		Dir:      t.TempDir(),
		MaxBytes: 1 << 20,
	})
	if err != nil {
		return nil, err
	}
	t.Cleanup(func() {
		require.NoError(t, cache.Close())
	})
	return NewWorker(registry, cache, options)
}

func startWorkerFlowService(
	t *testing.T,
	service dexpb.FlowServiceServer,
) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	grpcServer := grpc.NewServer()
	dexpb.RegisterFlowServiceServer(grpcServer, service)
	serveResult := make(chan error, 1)
	go func() {
		serveResult <- grpcServer.Serve(listener)
	}()
	t.Cleanup(func() {
		grpcServer.Stop()
		if err := listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			require.NoError(t, err)
		}
		requireServeStopped(t, <-serveResult)
	})
	return listener.Addr().String()
}

func startBlockingWorker(
	t *testing.T,
	flow Flow,
) (*Worker, dexpb.WorkerServiceClient, <-chan error, func()) {
	address := unusedWorkerAddress(t)
	worker, err := newWorkerForTest(t, []Flow{flow}, WorkerOptions{BindAddress: address})
	require.NoError(t, err)
	startResult := make(chan error, 1)
	go func() { startResult <- worker.Start() }()
	conn, err := grpc.NewClient(
		address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	return worker, dexpb.NewWorkerServiceClient(conn), startResult, func() {
		require.NoError(t, conn.Close())
	}
}

func invokeBlockingRPC(
	client dexpb.WorkerServiceClient,
) <-chan error {
	result := make(chan error, 1)
	input, err := encodeValue(struct{}{})
	if err != nil {
		result <- err
		return result
	}
	go func() {
		_, err := client.InvokeWorkerRPC(
			context.Background(),
			&dexpb.InvokeWorkerRPCRequest{
				Context:  workerRPCContext(),
				FlowType: "workerBlockingFlow",
				RpcName:  "Block",
				Input:    input,
			},
			grpc.WaitForReady(true),
		)
		result <- err
	}()
	return result
}

func newWorkerTestClient(
	t *testing.T,
	hydrator valueHydrator,
) (dexpb.WorkerServiceClient, func()) {
	return newWorkerClientForFlows(t, []Flow{workerFlow}, hydrator)
}

func newWorkerClientForFlows(
	t *testing.T,
	flows []Flow,
	hydrator valueHydrator,
) (dexpb.WorkerServiceClient, func()) {
	return newWorkerClientForServerProtocol(t, flows, hydrator, maximumSupportedServerProtocolVersion)
}

func newWorkerClientForServerProtocol(
	t *testing.T,
	flows []Flow,
	hydrator valueHydrator,
	serverProtocolVersion uint32,
) (dexpb.WorkerServiceClient, func()) {
	serverProtocol := &negotiatedServerProtocol{}
	serverProtocol.version.Store(serverProtocolVersion)
	if hydrator == nil {
		hydrator = concreteValueHydrator{}
	}
	registered, err := NewRegistry(flows)
	require.NoError(t, err)
	listener := bufconn.Listen(1 << 20)
	grpcServer := grpc.NewServer()
	dexpb.RegisterWorkerServiceServer(
		grpcServer,
		newWorkerService(registered, hydrator, nil, serverProtocol),
	)
	serveResult := make(chan error, 1)
	go func() {
		serveResult <- grpcServer.Serve(listener)
	}()
	conn, err := grpc.NewClient(
		"passthrough:///worker-test",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	return dexpb.NewWorkerServiceClient(conn), func() {
		grpcServer.Stop()
		require.NoError(t, conn.Close())
		requireServeStopped(t, <-serveResult)
	}
}

func invokeWaitForResult(
	t *testing.T,
	client dexpb.WorkerServiceClient,
	request *dexpb.InvokeWaitForMethodRequest,
) *dexpb.InvokeWaitForMethodResponse {
	outputs, err := collectWaitForOutputs(client, request)
	require.NoError(t, err)
	var result *dexpb.InvokeWaitForMethodResponse
	for _, output := range outputs {
		if output.GetResult() == nil {
			continue
		}
		require.Nil(t, result)
		result = output.GetResult()
	}
	require.NotNil(t, result)
	require.NotNil(t, outputs[len(outputs)-1].GetResult())
	return result
}

func invokeExecuteResult(
	t *testing.T,
	client dexpb.WorkerServiceClient,
	request *dexpb.InvokeExecuteMethodRequest,
) *dexpb.InvokeExecuteMethodResponse {
	outputs, err := collectExecuteOutputs(client, request)
	require.NoError(t, err)
	var result *dexpb.InvokeExecuteMethodResponse
	for _, output := range outputs {
		if output.GetResult() == nil {
			continue
		}
		require.Nil(t, result)
		result = output.GetResult()
	}
	require.NotNil(t, result)
	require.NotNil(t, outputs[len(outputs)-1].GetResult())
	return result
}

func invokeWaitForError(
	client dexpb.WorkerServiceClient,
	request *dexpb.InvokeWaitForMethodRequest,
) error {
	_, err := collectWaitForOutputs(client, request)
	return err
}

func invokeExecuteError(
	client dexpb.WorkerServiceClient,
	request *dexpb.InvokeExecuteMethodRequest,
) error {
	_, err := collectExecuteOutputs(client, request)
	return err
}

func collectWaitForOutputs(
	client dexpb.WorkerServiceClient,
	request *dexpb.InvokeWaitForMethodRequest,
) ([]*dexpb.InvokeWaitForMethodOutput, error) {
	stream, err := client.InvokeWaitForMethod(context.Background(), request)
	if err != nil {
		return nil, err
	}
	var outputs []*dexpb.InvokeWaitForMethodOutput
	for {
		output, receiveErr := stream.Recv()
		if errors.Is(receiveErr, io.EOF) {
			return outputs, nil
		}
		if receiveErr != nil {
			return outputs, receiveErr
		}
		outputs = append(outputs, output)
	}
}

func collectExecuteOutputs(
	client dexpb.WorkerServiceClient,
	request *dexpb.InvokeExecuteMethodRequest,
) ([]*dexpb.InvokeExecuteMethodOutput, error) {
	stream, err := client.InvokeExecuteMethod(context.Background(), request)
	if err != nil {
		return nil, err
	}
	var outputs []*dexpb.InvokeExecuteMethodOutput
	for {
		output, receiveErr := stream.Recv()
		if errors.Is(receiveErr, io.EOF) {
			return outputs, nil
		}
		if receiveErr != nil {
			return outputs, receiveErr
		}
		outputs = append(outputs, output)
	}
}

type workerTestProgressStream struct {
	frames    []string
	failure   error
	failAfter int
}

type bufferedTestProgressStream struct {
	mu      sync.Mutex
	written []string
}

func (stream *bufferedTestProgressStream) sendHeartbeat(*dexpb.StepMethodHeartbeat) error {
	return nil
}

func (stream *bufferedTestProgressStream) sendStreamWrite(write *dexpb.StepStreamWrite) error {
	stream.mu.Lock()
	defer stream.mu.Unlock()
	stream.written = append(stream.written, write.GetValue().GetStringValue())
	return nil
}

func (stream *bufferedTestProgressStream) values() []string {
	stream.mu.Lock()
	defer stream.mu.Unlock()
	return append([]string(nil), stream.written...)
}

func (stream *workerTestProgressStream) sendHeartbeat(*dexpb.StepMethodHeartbeat) error {
	if err := stream.sendError(); err != nil {
		return err
	}
	stream.frames = append(stream.frames, "heartbeat")
	return nil
}

func (stream *workerTestProgressStream) sendStreamWrite(*dexpb.StepStreamWrite) error {
	if err := stream.sendError(); err != nil {
		return err
	}
	stream.frames = append(stream.frames, "stream")
	return nil
}

func (stream *workerTestProgressStream) sendError() error {
	if stream.failure != nil && len(stream.frames) >= stream.failAfter {
		return stream.failure
	}
	return nil
}

func newFlowServiceTestClient(
	t *testing.T,
	service dexpb.FlowServiceServer,
) (dexpb.FlowServiceClient, func()) {
	listener := bufconn.Listen(1 << 20)
	grpcServer := grpc.NewServer()
	dexpb.RegisterFlowServiceServer(grpcServer, service)
	serveResult := make(chan error, 1)
	go func() {
		serveResult <- grpcServer.Serve(listener)
	}()
	conn, err := grpc.NewClient(
		"passthrough:///flow-test",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	return dexpb.NewFlowServiceClient(conn), func() {
		grpcServer.Stop()
		require.NoError(t, conn.Close())
		requireServeStopped(t, <-serveResult)
	}
}

func requireServeStopped(t *testing.T, err error) {
	if err != nil {
		require.ErrorIs(t, err, grpc.ErrServerStopped)
	}
}

func workerStepContext() *dexpb.Context {
	return &dexpb.Context{
		FlowId:                "flow-1",
		RunId:                 "run-1",
		FlowStartedTimestamp:  1,
		StepExecutionId:       "waiting-1",
		FromStepExecutionId:   "source-1",
		FirstAttemptTimestamp: 2,
		Attempt:               1,
	}
}

func workerRPCContext() *dexpb.Context {
	return &dexpb.Context{
		FlowId:               "flow-1",
		RunId:                "run-1",
		FlowStartedTimestamp: 1,
	}
}

func workerExecuteRequest(
	t *testing.T,
	input workerTestInput,
) *dexpb.InvokeExecuteMethodRequest {
	return &dexpb.InvokeExecuteMethodRequest{
		Context:   workerStepContext(),
		FlowType:  GetFinalFlowType(workerFlow),
		StepType:  GetFinalStepType(workerTestWait),
		StepInput: mustEncodeWorkerTestValue(t, input),
		StepExeLocals: []*dexpb.KV{{
			Key: "input", Value: mustEncodeWorkerTestValue(t, input),
		}},
		ConditionResults: &dexpb.ConditionResults{
			WaitForFailed: true,
			TimerResults: []*dexpb.TimerResult{{
				ConditionId:     "timer",
				ConditionStatus: dexpb.ConditionStatus_CONDITION_STATUS_WAITING,
			}},
			ChannelResults: []*dexpb.ChannelResult{
				{
					ConditionId: "command-1", ChannelName: "commands",
					ConditionStatus: dexpb.ConditionStatus_CONDITION_STATUS_COMPLETED,
					Values:          []*dexpb.Value{mustEncodeWorkerTestValue(t, "first")},
				},
				{
					ConditionId: "command-2", ChannelName: "commands",
					ConditionStatus: dexpb.ConditionStatus_CONDITION_STATUS_COMPLETED,
					Values:          []*dexpb.Value{mustEncodeWorkerTestValue(t, "second")},
				},
				{
					ConditionId: "map-command", ChannelName: "commands-by-order/" + input.OrderID,
					ConditionStatus: dexpb.ConditionStatus_CONDITION_STATUS_COMPLETED,
					Values:          []*dexpb.Value{mustEncodeWorkerTestValue(t, "mapped")},
				},
			},
		},
	}
}

func workerRPCRequest(
	t *testing.T,
	input workerTestInput,
) *dexpb.InvokeWorkerRPCRequest {
	return &dexpb.InvokeWorkerRPCRequest{
		Context:  workerRPCContext(),
		FlowType: GetFinalFlowType(workerFlow),
		RpcName:  "Update",
		Input:    mustEncodeWorkerTestValue(t, input),
	}
}

func mustEncodeWorkerTestValue(t *testing.T, value any) *dexpb.Value {
	encoded, err := encodeValue(value)
	require.NoError(t, err)
	return encoded
}

func requireWorkerErrorDetail(
	t *testing.T,
	rpcStatus *status.Status,
) *dexpb.WorkerErrorResponse {
	require.Len(t, rpcStatus.Details(), 1)
	details, ok := rpcStatus.Details()[0].(*dexpb.WorkerErrorResponse)
	require.True(t, ok)
	return details
}

func unusedWorkerAddress(t *testing.T) string {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	address := listener.Addr().String()
	require.NoError(t, listener.Close())
	return address
}
