// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package dex

import (
	"context"
	"errors"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/superdurable/dex/blob-cache-go/blobcache"
	"github.com/superdurable/dex/sdk-go/gen/dexpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"
)

var (
	hardDeleteTestSecret = DefineAttribute[string]("hard-delete-test-secret", HardDeleteOnNull())
	hardDeleteTestAny    = DefineAttribute[any]("hard-delete-test-any", HardDeleteOnNull())
)

type hardDeleteTestFlow struct {
	FlowDefaults
}

func (hardDeleteTestFlow) GetSteps() []StepDef {
	return []StepDef{DefineStartStep(hardDeleteTestStep{})}
}

func (flow hardDeleteTestFlow) GetRPCs() []RPCDef {
	return []RPCDef{
		DefineRPC(flow.SetSecret, nil),
		DefineRPC(flow.DeleteSecret, nil),
		DefineRPC(flow.SetNumber, nil),
	}
}

func (hardDeleteTestFlow) GetPersistenceSchema() PersistenceSchema {
	return PersistenceSchema{Attributes: []AttributeDef{hardDeleteTestSecret, hardDeleteTestAny}}
}

func (hardDeleteTestFlow) SetSecret(ctx Context, secret string) (*RPCResult[None], error) {
	return &RPCResult[None]{}, hardDeleteTestSecret.Set(ctx, secret)
}

func (hardDeleteTestFlow) DeleteSecret(ctx Context, _ None) (*RPCResult[None], error) {
	return &RPCResult[None]{}, hardDeleteTestSecret.Delete(ctx)
}

func (hardDeleteTestFlow) SetNumber(ctx Context, number int) (*RPCResult[None], error) {
	return &RPCResult[None]{}, hardDeleteTestAny.Set(ctx, number)
}

type hardDeleteTestStep struct {
	StepDefaultsNoWaitFor[None]
}

func (hardDeleteTestStep) Execute(Context, None) (*StepDecision, error) {
	return DeadEnd(), nil
}

func TestHardDeleteAttributeWritesCarryTheHardDeleteMarker(t *testing.T) {
	client, closeService := newWorkerClientForServerProtocol(
		t, []Flow{hardDeleteTestFlow{}}, nil, hardDeleteAttributeProtocolVersion,
	)
	defer closeService()

	setResponse, err := client.InvokeWorkerRPC(context.Background(), hardDeleteTestRPCRequest(t, "SetSecret", "secret"))
	require.NoError(t, err)
	require.Len(t, setResponse.UpsertAttributes, 1)
	require.True(t, setResponse.UpsertAttributes[0].IsHardDeleteOnNull)
	require.Equal(t, "secret", setResponse.UpsertAttributes[0].Value.GetStringValue())

	deleteResponse, err := client.InvokeWorkerRPC(context.Background(), hardDeleteTestRPCRequest(t, "DeleteSecret", nil))
	require.NoError(t, err)
	require.Len(t, deleteResponse.UpsertAttributes, 1)
	require.True(t, deleteResponse.UpsertAttributes[0].IsHardDeleteOnNull)
	require.NotNil(t, deleteResponse.UpsertAttributes[0].Value.GetKind().(*dexpb.Value_NullValue))

	_, err = client.InvokeWorkerRPC(context.Background(), hardDeleteTestRPCRequest(t, "SetNumber", 7))
	require.ErrorContains(t, err, "supports only string and object values")
}

func TestHardDeleteAttributeWorkerRefusesOlderServerProtocol(t *testing.T) {
	client, closeService := newWorkerClientForServerProtocol(t, []Flow{hardDeleteTestFlow{}}, nil, 2)
	defer closeService()

	_, err := client.InvokeWorkerRPC(context.Background(), hardDeleteTestRPCRequest(t, "SetSecret", "secret"))
	require.ErrorContains(t, err, "requires Server protocol 3 or later")
}

func TestHardDeleteAttributeClientNegotiatesBeforeStartFlow(t *testing.T) {
	for _, testCase := range []struct {
		name                  string
		serverProtocolVersion uint32
		isAccepted            bool
	}{
		{name: "older Server", serverProtocolVersion: 2},
		{name: "current Server", serverProtocolVersion: 3, isAccepted: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			client, service := newHardDeleteTestClient(t, testCase.serverProtocolVersion)
			initialSecret, err := InitialAttribute(hardDeleteTestSecret, "secret")
			require.NoError(t, err)
			_, err = client.StartFlow(context.Background(), hardDeleteTestFlow{}, "flow-1", nil, StartFlowOptions{
				Attributes: []InitialAttributeDef{initialSecret},
			})
			if !testCase.isAccepted {
				var unsupported *HardDeleteAttributeUnsupportedError
				require.ErrorAs(t, err, &unsupported)
				require.Nil(t, service.startRequest)
				return
			}
			require.NoError(t, err)
			attributes := service.startRequest.GetFlowStartOptions().GetAttributes()
			require.Len(t, attributes, 1)
			require.True(t, attributes[0].IsHardDeleteOnNull)
		})
	}
}

type hardDeleteTestFlowService struct {
	dexpb.UnimplementedFlowServiceServer
	serverProtocolVersion uint32
	startRequest          *dexpb.StartFlowRequest
}

func (service *hardDeleteTestFlowService) GetServerInfo(
	context.Context,
	*emptypb.Empty,
) (*dexpb.ServerInfo, error) {
	return &dexpb.ServerInfo{
		ServerVersion:                   "test",
		MinimumSupportedProtocolVersion: 1,
		CurrentProtocolVersion:          service.serverProtocolVersion,
	}, nil
}

func (service *hardDeleteTestFlowService) StartFlow(
	_ context.Context,
	request *dexpb.StartFlowRequest,
) (*dexpb.StartFlowResponse, error) {
	service.startRequest = request
	return &dexpb.StartFlowResponse{RunId: "run-1"}, nil
}

func newHardDeleteTestClient(t *testing.T, serverProtocolVersion uint32) (*Client, *hardDeleteTestFlowService) {
	t.Helper()
	registry, err := NewRegistry([]Flow{hardDeleteTestFlow{}})
	require.NoError(t, err)
	cache, err := blobcache.New(&blobcache.Config{Dir: t.TempDir(), MaxBytes: 1 << 20})
	require.NoError(t, err)
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	service := &hardDeleteTestFlowService{serverProtocolVersion: serverProtocolVersion}
	dexpb.RegisterFlowServiceServer(server, service)
	serveResult := make(chan error, 1)
	go func() { serveResult <- server.Serve(listener) }()
	connection, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
	)
	require.NoError(t, err)
	client := newClient(registry, cache, connection, &WorkerTarget{Address: "worker.test:8803"}, nil)
	t.Cleanup(func() {
		require.NoError(t, client.Close())
		server.Stop()
		if serveErr := <-serveResult; serveErr != nil && !errors.Is(serveErr, grpc.ErrServerStopped) {
			t.Errorf("serve FlowService: %v", serveErr)
		}
		require.NoError(t, cache.Close())
	})
	return client, service
}

func hardDeleteTestRPCRequest(t *testing.T, rpcName string, input any) *dexpb.InvokeWorkerRPCRequest {
	t.Helper()
	return &dexpb.InvokeWorkerRPCRequest{
		Context:  workerRPCContext(),
		FlowType: GetFinalFlowType(hardDeleteTestFlow{}),
		RpcName:  rpcName,
		Input:    mustEncodeWorkerTestValue(t, input),
	}
}

// hardDeleteSecretWithoutMarker shares the hard-delete Attribute name but omits HardDeleteOnNull.
var hardDeleteSecretWithoutMarker = DefineAttribute[string]("hard-delete-test-secret")

type hardDeleteParentTestFlow struct {
	FlowDefaults
}

func (hardDeleteParentTestFlow) GetSteps() []StepDef {
	return []StepDef{DefineStartStep(hardDeleteParentTestStep{})}
}

func (hardDeleteParentTestFlow) GetPersistenceSchema() PersistenceSchema {
	return PersistenceSchema{}
}

type hardDeleteParentTestStep struct {
	StepDefaults
}

func (hardDeleteParentTestStep) WaitFor(Context, None) (*Wait, error) {
	secret, err := InitialAttribute(hardDeleteSecretWithoutMarker, "secret")
	if err != nil {
		return nil, err
	}
	return Until(SubFlow(hardDeleteTestFlow{}, nil, SubFlowOptions{
		Attributes: []InitialAttributeDef{secret},
	})), nil
}

func (hardDeleteParentTestStep) Execute(Context, None) (*StepDecision, error) {
	return DeadEnd(), nil
}

func TestHardDeleteAttributeSubFlowRejectsMismatchedDefinition(t *testing.T) {
	client, closeService := newWorkerClientForServerProtocol(
		t,
		[]Flow{hardDeleteParentTestFlow{}, hardDeleteTestFlow{}},
		nil,
		hardDeleteAttributeProtocolVersion,
	)
	defer closeService()

	_, err := collectWaitForOutputs(client, &dexpb.InvokeWaitForMethodRequest{
		Context:   workerStepContext(),
		FlowType:  GetFinalFlowType(hardDeleteParentTestFlow{}),
		StepType:  GetFinalStepType(hardDeleteParentTestStep{}),
		StepInput: mustEncodeWorkerTestValue(t, nil),
	})
	require.ErrorContains(t, err, "hard-delete setting does not match")
}
