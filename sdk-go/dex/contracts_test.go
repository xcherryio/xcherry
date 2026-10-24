// Legacy Materials in this file remain under their original licenses.
// See LEGACY_NOTICES.md.

// Modifications Copyright (c) 2026 Super Durable, Inc.
//
// Modifications after the Legacy Cutoff are licensed under the
// Sustainable Use License 1.0.
// Legacy Materials remain under their original licenses.
// See LICENSE and LEGACY_NOTICES.md.

package dex_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"testing"
	"time"

	"github.com/superdurable/dex/blob-cache-go/blobcache"
	"github.com/superdurable/dex/sdk-go/dex"
	"github.com/superdurable/dex/sdk-go/dex/ptr"
	"google.golang.org/grpc/codes"
)

var (
	statusAttribute = dex.DefineAttribute[string](
		"status",
		dex.Indexed(dex.AttributeIndex{Type: dex.IndexKeyword}),
	)
	itemsAttribute   = dex.DefineAttributeMap[int]("items")
	commandChannel   = dex.DefineChannel[command]("commands")
	commandByOrder   = dex.DefineChannelMap[command]("commands-by-order")
	noPayloadChannel = dex.DefineChannel[dex.None]("no-payload")
	progressStream   = dex.DefineStream[string]("progress", 10<<20)
)

type command struct {
	Name string
}

type stepInput struct {
	OrderID string
}

type noPayloadStep struct {
	dex.StepDefaultsNoWaitFor[dex.None]
}

func (noPayloadStep) Execute(
	dex.Context,
	dex.None,
) (*dex.StepDecision, error) {
	return dex.DeadEnd(), nil
}

var noPayload = noPayloadStep{}
var _ dex.Step[dex.None] = noPayload

type waitingStep struct {
	dex.StepDefaults
}

func (waitingStep) WaitFor(
	ctx dex.Context,
	input stepInput,
) (*dex.Wait, error) {
	var checkpoint string
	if _, err := ctx.GetLastHeartbeatValue(&checkpoint); err != nil {
		return nil, err
	}
	if err := ctx.RecordHeartbeat(input.OrderID); err != nil {
		return nil, err
	}
	if err := progressStream.Write(ctx, "waiting"); err != nil {
		return nil, err
	}
	if err := progressStream.Write(ctx, "still waiting"); err != nil {
		return nil, err
	}
	if err := statusAttribute.Set(ctx, input.OrderID); err != nil {
		return nil, err
	}
	return dex.AnyComboOf(
		dex.Combo(
			commandChannel.ForOne(dex.WithConditionID("command")),
			dex.Timer(
				time.Minute,
				dex.WithConditionID("timeout"),
			),
		),
	), nil
}

func (waitingStep) Execute(
	ctx dex.Context,
	input stepInput,
) (*dex.StepDecision, error) {
	if ctx.WaitForMethodFailed() || ctx.HasTimerFiredByIndex(0) {
		return dex.ForceFail("wait failed"), nil
	}
	results, err := commandChannel.GetConditionResults(ctx)
	if err != nil {
		return nil, err
	}
	if len(results) == 0 {
		return dex.DeadEnd(), nil
	}
	return dex.GoTo(executeOnly, input), nil
}

var waitForCommand = waitingStep{}
var _ dex.Step[stepInput] = waitForCommand

type executeOnlyStep struct {
	dex.StepDefaultsNoWaitFor[stepInput]
}

func (executeOnlyStep) Execute(
	ctx dex.Context,
	input stepInput,
) (*dex.StepDecision, error) {
	first := dex.MovementOf(waitForCommand, input)
	second := dex.MovementOf(executeOnly, input)
	return dex.GoToMany(first, second), nil
}

var executeOnly = executeOnlyStep{}
var _ dex.Step[stepInput] = executeOnly

type contractFlow struct {
	dex.FlowDefaults
}

type unregisteredContractFlow struct {
	dex.FlowDefaults
}

func (unregisteredContractFlow) GetSteps() []dex.StepDef {
	return nil
}

func (unregisteredContractFlow) GetPersistenceSchema() dex.PersistenceSchema {
	return dex.PersistenceSchema{}
}

func (contractFlow) GetSteps() []dex.StepDef {
	return []dex.StepDef{
		dex.DefineStartStep(waitForCommand),
		dex.DefineStep(executeOnly),
	}
}

func (flow contractFlow) GetRPCs() []dex.RPCDef {
	return []dex.RPCDef{
		dex.DefineRPC(flow.Update, nil),
		dex.DefineRPC(flow.Describe, &dex.RPCOptions{
			LockAttributes: []dex.AttributeLock{dex.LockAttribute(statusAttribute)},
			Action: dex.DefineAction(
				"Describe",
				dex.WhenAttributeMatches(
					statusAttribute,
					dex.AttributeMatchEqual("ready"),
					dex.AttributeMatchEqual("pending"),
				),
				dex.ActionRequiresPermission("order.describe"),
			),
		}),
	}
}

func (contractFlow) GetPersistenceSchema() dex.PersistenceSchema {
	return dex.PersistenceSchema{
		Attributes: []dex.AttributeDef{
			statusAttribute,
			itemsAttribute,
		},
		Channels: []dex.ChannelDef{
			commandChannel,
			commandByOrder,
			noPayloadChannel,
		},
		Streams: []dex.StreamDef{progressStream},
	}
}

func (contractFlow) Update(
	ctx dex.Context,
	input stepInput,
) (*dex.RPCResult[command], error) {
	return &dex.RPCResult[command]{
		Output:    command{Name: "updated"},
		NextSteps: []dex.StepMovement{dex.MovementOf(executeOnly, input)},
	}, nil
}

func (contractFlow) Describe(
	dex.Context,
	dex.None,
) (*dex.RPCResult[command], error) {
	return &dex.RPCResult[command]{Output: command{Name: "described"}}, nil
}

var flow = contractFlow{}
var _ dex.Flow = flow
var _ dex.RPC[stepInput, command] = flow.Update
var _ dex.RPC[dex.None, command] = flow.Describe

func compileBufferedTextStream(context dex.Context) error {
	progress, err := dex.NewBufferedTextStream(
		context,
		progressStream,
		dex.BufferedTextStreamFlushInterval(500*time.Millisecond),
		dex.BufferedTextStreamMaxBytes(8<<10),
	)
	if err != nil {
		return err
	}
	if err := progress.Write("thinking "); err != nil {
		return err
	}
	return nil
}

var _ = compileBufferedTextStream

var (
	_ func(*dex.Worker, context.Context) error    = (*dex.Worker).WaitPullRegistered
	_ func(*dex.Worker) dex.PullRegistrationState = (*dex.Worker).PullRegistrationState
	_ error                                       = (*dex.PullRegistrationError)(nil)
	_ interface{ Unwrap() error }                 = (*dex.PullRegistrationError)(nil)
)

var (
	_ func(string, string, string, ...dex.APIKeyCredentialOption) (dex.Credential, error) = dex.APIKeyCredential
	_ func(time.Duration) dex.APIKeyCredentialOption                                      = dex.WithAPIKeyExchangeTimeout
	_ func(*http.Client) dex.APIKeyCredentialOption                                       = dex.WithAPIKeyHTTPClient
	_ dex.Credential                                                                      = applicationCredential{}
)

// applicationCredential proves an application Credential still needs only Token.
type applicationCredential struct{}

func (applicationCredential) Token(context.Context) (string, error) {
	return "application-token", nil
}

func TestPublicContractsCompile(t *testing.T) {
	_ = dex.PullWorkerOptions{MaxConcurrentTasks: 4, ReservedRPCTasks: ptr.Any(0)}
	var reserveDeclines func(*dex.Worker) uint64 = (*dex.Worker).PullReservedRPCDeclines
	_ = reserveDeclines
	registrationState := dex.PullRegistrationState{
		IsReady: true,
		Queues:  []dex.PullQueueRegistrationState{{Queue: "OrderFlow", HasRegisteredStream: true, LastPermanentError: nil}},
	}
	if !registrationState.IsReady || !registrationState.Queues[0].HasRegisteredStream {
		t.Fatal("unexpected pull registration snapshot")
	}
	if dex.WorkQueuePermissionsIndexKey != "DexWorkQueuePermissions" {
		t.Fatal("unexpected Work Queue permissions index key")
	}
	_ = dex.MovementOf(noPayload, nil)
	_ = noPayloadChannel.ForOne()

	initial, err := dex.InitialAttribute(statusAttribute, "new")
	if err != nil {
		t.Fatal(err)
	}
	mapInitial, err := dex.InitialAttributeMapValue(itemsAttribute, "order-1", 1)
	if err != nil {
		t.Fatal(err)
	}

	mode := dex.SearchAllActiveSteps
	durability := dex.StepDurabilitySync
	config := dex.FlowConfig{
		ActiveStepSearchMode:   &mode,
		ContinueAsNewThreshold: ptr.Any(int32(100)),
		StepDurability:         &durability,
		AttributeStoreNames:    []string{"profiles"},
	}
	options := dex.StartFlowOptions{
		Timeout:        ptr.Any(time.Minute),
		StartDelay:     ptr.Any(time.Second),
		Attributes:     []dex.InitialAttributeDef{initial, mapInitial},
		ConfigOverride: &config,
	}
	if options.Timeout == nil ||
		options.StartDelay == nil ||
		len(options.Attributes) != 2 {
		t.Fatal("start flow options are missing")
	}
	if config.AttributeStoreNames == nil || len(config.AttributeStoreNames) != 1 {
		t.Fatal("attribute store names are missing")
	}
	disabledConfig := dex.FlowConfig{AttributeStoreNames: []string{}}
	if disabledConfig.AttributeStoreNames == nil || len(disabledConfig.AttributeStoreNames) != 0 {
		t.Fatal("empty attribute store names must preserve an explicit empty override")
	}

	registry, err := dex.NewRegistry([]dex.Flow{flow})
	if err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cache, err := blobcache.New(&blobcache.Config{
		Dir:      t.TempDir(),
		MaxBytes: 1 << 20,
		Logger:   logger,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := cache.Close(); err != nil {
			t.Fatal(err)
		}
	}()
	worker, err := dex.NewWorker(registry, cache, dex.WorkerOptions{
		Logger:              logger,
		GrpcMaxMessageBytes: dex.DefaultGrpcMaxMessageBytes,
	})
	if err != nil {
		t.Fatal(err)
	}
	if worker.WorkerTarget().Address != "localhost:8803" {
		t.Fatal("worker target was not derived from the bind address")
	}
	client, err := dex.NewClient(registry, cache, dex.ClientOptions{
		WorkerTarget:        worker.WorkerTarget(),
		Logger:              logger,
		GrpcMaxMessageBytes: dex.DefaultGrpcMaxMessageBytes,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := dex.NewClient(registry, cache, dex.ClientOptions{GrpcMaxMessageBytes: -1}); err == nil {
		t.Fatal("negative GrpcMaxMessageBytes was accepted")
	}
	apiKeyCredential, err := dex.APIKeyCredential(
		"https://auth.example.com/oauth/token",
		"orders-production",
		"contract-api-key",
		dex.WithAPIKeyExchangeTimeout(5*time.Second),
		dex.WithAPIKeyHTTPClient(&http.Client{}),
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, credential := range []dex.Credential{apiKeyCredential, applicationCredential{}} {
		credentialClient, err := dex.NewClient(registry, cache, dex.ClientOptions{Credential: credential})
		if err != nil {
			t.Fatal(err)
		}
		if err := credentialClient.Close(); err != nil {
			t.Fatal(err)
		}
	}
	_, err = client.StartFlow(
		context.Background(),
		unregisteredContractFlow{},
		"unregistered",
		nil,
		dex.StartFlowOptions{},
	)
	var definition *dex.FlowDefinitionError
	if !errors.As(err, &definition) {
		t.Fatalf("unregistered Flow error is not FlowDefinitionError: %v", err)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if err := worker.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	pullTarget := dex.WorkerTarget{Pull: true}
	pullOptions := dex.PullWorkerOptions{
		MaxConcurrentTasks:             2,
		IdleStreamsPerQueue:            1,
		AppWorkerID:                    "contract-pull-12345678",
		StopOnPermanentStreamRejection: true,
	}
	pullWorker, err := dex.NewWorker(registry, cache, dex.WorkerOptions{Pull: &pullOptions})
	if err != nil {
		t.Fatal(err)
	}
	if !pullWorker.WorkerTarget().Pull {
		t.Fatal("pull Worker target was not advertised")
	}
	pullClient, err := dex.NewClient(registry, cache, dex.ClientOptions{WorkerTarget: &pullTarget})
	if err != nil {
		t.Fatal(err)
	}
	if err := pullClient.Close(); err != nil {
		t.Fatal(err)
	}
	if err := pullWorker.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestServiceErrorsSupportErrorsAs(t *testing.T) {
	serviceError := &dex.ServiceError{
		Op:        "WaitForFlow",
		FlowID:    "flow-id",
		Code:      codes.NotFound,
		SubStatus: dex.ErrorSubStatusFlowNotFound,
		Detail:    "flow is missing",
	}
	sdkError := error(&dex.FlowNotFoundError{ServiceError: serviceError})
	var missing *dex.FlowNotFoundError
	if !errors.As(sdkError, &missing) {
		t.Fatal("specific SDK error does not support errors.As")
	}
	var target *dex.ServiceError
	if !errors.As(sdkError, &target) {
		t.Fatal("SDK error does not unwrap to ServiceError")
	}
	if target.Code != codes.NotFound || target.FlowID != "flow-id" {
		t.Fatalf("unexpected service error: %#v", target)
	}
}

func TestLocalErrorContracts(t *testing.T) {
	_, err := dex.NewRegistry([]dex.Flow{nil})
	var definition *dex.FlowDefinitionError
	if !errors.As(err, &definition) {
		t.Fatalf("registry error is not FlowDefinitionError: %v", err)
	}

	indexed := dex.DefineAttribute[float64](
		"score",
		dex.Indexed(dex.AttributeIndex{Type: dex.IndexDouble}),
	)
	_, err = dex.InitialAttribute(indexed, math.NaN())
	var mapping *dex.ValueMappingError
	if !errors.As(err, &mapping) {
		t.Fatalf("mapping error is not ValueMappingError: %v", err)
	}

	if (dex.FlowResult{Status: dex.FlowRunning}).IsTerminal() {
		t.Fatal("running Flow result must not be terminal")
	}
}

func TestAttributeNotFoundErrorSupportsErrorsAs(t *testing.T) {
	missing := error(&dex.AttributeNotFoundError{
		AttributeName: "items",
		Instance:      "order-1",
	})
	var target *dex.AttributeNotFoundError
	if !errors.As(missing, &target) {
		t.Fatal("attribute not-found error does not support errors.As")
	}
	if target.AttributeName != "items" || target.Instance != "order-1" {
		t.Fatalf("unexpected attribute identity: %#v", target)
	}
}

func compileAttributeOperations(ctx dex.Context) error {
	if _, err := statusAttribute.Get(ctx); err != nil {
		return err
	}
	if err := statusAttribute.Set(ctx, "ready"); err != nil {
		return err
	}
	if err := statusAttribute.Delete(ctx); err != nil {
		return err
	}
	if _, err := itemsAttribute.Get(ctx, "order-1"); err != nil {
		return err
	}
	if err := itemsAttribute.Set(ctx, "order-1", 1); err != nil {
		return err
	}
	return itemsAttribute.Delete(ctx, "order-1")
}

func compileChannelOperations(ctx dex.Context) error {
	if err := commandChannel.Publish(ctx, command{}); err != nil {
		return err
	}
	if err := commandByOrder.Publish(ctx, "order-1", command{}); err != nil {
		return err
	}
	_ = commandChannel.ForOne()
	_ = commandChannel.ForN(2)
	_ = commandChannel.AtLeast(1)
	_ = commandChannel.AtMost(2)
	_ = commandChannel.AtLeastAtMost(1, 2)
	_ = commandByOrder.ForOne("order-1")
	_ = commandByOrder.ForN("order-1", 2)
	_ = commandByOrder.AtLeast("order-1", 1)
	_ = commandByOrder.AtMost("order-1", 2)
	_ = commandByOrder.AtLeastAtMost("order-1", 1, 2)
	_ = commandChannel.Size(ctx)
	_ = commandByOrder.Size(ctx, "order-1")
	if _, err := commandChannel.GetConditionResults(ctx); err != nil {
		return err
	}
	_, err := commandByOrder.GetConditionResults(ctx, "order-1")
	return err
}

func compileContextOperations(ctx dex.Context) error {
	if err := ctx.SetStepExecutionLocal("snapshot", command{}); err != nil {
		return err
	}
	var snapshot command
	if _, err := ctx.GetStepExecutionLocal("snapshot", &snapshot); err != nil {
		return err
	}
	if err := ctx.RecordEvent("snapshot", snapshot); err != nil {
		return err
	}
	_ = ctx.HasTimerFired()
	_ = ctx.HasTimerFiredByIndex(0)
	_ = ctx.WaitForMethodFailed()
	_ = ctx.RecoveryError()
	_ = itemsAttribute.MapSize(ctx)
	_ = itemsAttribute.AllInstanceKeys(ctx)
	_ = commandByOrder.MapSize(ctx)
	_ = commandByOrder.AllInstanceKeys(ctx)
	return nil
}

var _ = dex.AllOf(
	commandChannel.ForOne(),
	dex.Timer(time.Minute, dex.WithConditionID("all")),
)
var _ = dex.Until(commandChannel.ForOne())
var _ = dex.AnyOf(
	commandChannel.ForOne(),
	dex.Timer(time.Minute, dex.WithConditionID("any")),
)
var _ = dex.ForceComplete("done")
var _ = dex.GracefulComplete("done")
var _ = dex.ForceFail("failed")
var _ = dex.DeadEnd()
var _ = dex.DefineAttribute[string]("contract-secret", dex.HardDeleteOnNull())
var _ error = &dex.HardDeleteAttributeUnsupportedError{
	AttributeName:                 "contract-secret",
	NegotiatedServerProtocol:      2,
	RequiredServerProtocolVersion: 3,
}
var _ = dex.ForceCompleteIfChannelsEmpty(
	"done",
	[]dex.ChannelDef{commandChannel},
	dex.MovementOf(executeOnly, stepInput{}),
)

var _ func(
	*dex.Client,
	context.Context,
	dex.Flow,
	string,
	any,
	dex.StartFlowOptions,
) (string, error) = (*dex.Client).StartFlow

var _ func(
	*dex.Client,
	context.Context,
	string,
	dex.AttributeDef,
	dex.AttributeMatchDef,
	any,
	dex.WaitForAttributeOptions,
) error = (*dex.Client).WaitForAttributeMatch

var _ func(
	*dex.Client,
	context.Context,
	string,
	dex.AttributeDef,
	string,
	dex.AttributeMatchDef,
	any,
	dex.WaitForAttributeOptions,
) error = (*dex.Client).WaitForAttributeMapInstanceMatch

var _ func(
	*dex.Client,
	context.Context,
	string,
	dex.StepExecutionID,
	dex.WaitForStepCompletionOptions,
) error = (*dex.Client).WaitForStepCompletion

var _ func(
	*dex.Client,
	context.Context,
	string,
	dex.StreamDef,
	string,
	any,
) error = (*dex.Client).WriteStream

var _ func(
	*dex.Client,
	context.Context,
	string,
	dex.StreamDef,
	string,
	any,
) (dex.StreamMessage, error) = (*dex.Client).ReadStream

var _ func(
	*dex.Client,
	context.Context,
	string,
	dex.StreamDef,
	int32,
	string,
	any,
) error = (*dex.Client).ListStreamMessages

var _ func(
	*dex.Client,
	context.Context,
	string,
	any,
	any,
	any,
) error = (*dex.Client).InvokeRPC

var _ func(
	*dex.Client,
	context.Context,
	string,
	int32,
	string,
	dex.SearchFlowsOptions,
) (dex.SearchFlowsPage, error) = (*dex.Client).SearchFlowsWithOptions

func TestBlobMessageTooLargeErrorSupportsErrorsAs(t *testing.T) {
	tooLarge := error(&dex.ServiceError{Op: "LoadBlobs", Code: codes.ResourceExhausted})
	tooLarge = fmt.Errorf("hydrate: %w", &dex.BlobMessageTooLargeError{
		FlowID: "flow-id",
		BlobID: "blob-id",
		Err:    tooLarge,
	})
	var target *dex.BlobMessageTooLargeError
	if !errors.As(tooLarge, &target) {
		t.Fatal("blob message size error does not support errors.As")
	}
	if target.FlowID != "flow-id" || target.BlobID != "blob-id" {
		t.Fatalf("unexpected blob identity: %#v", target)
	}
}
