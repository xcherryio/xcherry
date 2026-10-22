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
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/superdurable/dex/sdk-go/dex/ptr"
	"github.com/superdurable/dex/sdk-go/gen/dexpb"
)

type reservedRPCFlow struct {
	FlowDefaults
	entered chan string
	release chan struct{}
}

func TestWorkerPullReservedRPCValidation(t *testing.T) {
	for _, reserve := range []int{-1, 2, 3} {
		_, err := newWorkerForTest(t, nil, WorkerOptions{Pull: &PullWorkerOptions{MaxConcurrentTasks: 2, ReservedRPCTasks: ptr.Any(reserve)}})
		require.ErrorContains(t, err, "ReservedRPCTasks")
	}
}

func TestWorkerPullReservedRPCAdmission(t *testing.T) {
	for _, reserve := range []int{0, 1} {
		for _, kind := range []string{"waitFor", "execute", "rpc"} {
			t.Run(fmt.Sprintf("reserve=%d/first=%s", reserve, kind), func(t *testing.T) {
				address, server := startPullTestServer(t)
				server.supportsUnknownNameRejection = true
				flow := &reservedRPCFlow{entered: make(chan string, 20), release: make(chan struct{})}
				worker, err := newWorkerForTest(t, []Flow{flow}, WorkerOptions{FlowServiceAddress: address, Pull: &PullWorkerOptions{MaxConcurrentTasks: 2, ReservedRPCTasks: ptr.Any(reserve)}})
				require.NoError(t, err)
				started := make(chan error, 1)
				go func() { started <- worker.Start() }()
				defer func() {
					close(flow.release)
					require.NoError(t, worker.Stop(context.Background()))
					require.NoError(t, <-started)
				}()
				first := awaitReservedRPCStream(t, server, GetFinalFlowType(flow))
				require.NoError(t, first.send(reservedRPCTestTask(kind, "first")))
				require.Equal(t, kind, awaitReservedRPCEntry(t, flow))
				second := awaitReservedRPCStream(t, server, GetFinalFlowType(flow))
				unknownStep := reservedRPCTestTask("execute", "unknown-step")
				unknownStep.GetTask().GetExecute().StepType = "MissingStep"
				require.NoError(t, second.send(unknownStep))
				require.Equal(t, dexpb.PollWorkerTaskRejectReason_POLL_WORKER_TASK_REJECT_REASON_UNKNOWN_STEP, awaitPullFrame(t, second).GetReject().GetReason())
				unknownRPC := reservedRPCTestTask("rpc", "unknown-rpc")
				unknownRPC.GetTask().GetRpc().RpcName = "MissingRPC"
				require.NoError(t, second.send(unknownRPC))
				require.Equal(t, dexpb.PollWorkerTaskRejectReason_POLL_WORKER_TASK_REJECT_REASON_UNKNOWN_RPC, awaitPullFrame(t, second).GetReject().GetReason())
				require.NoError(t, second.send(reservedRPCTestTask("execute", "second-step")))
				if reserve > 0 {
					require.Equal(t, dexpb.PollWorkerTaskRejectReason_POLL_WORKER_TASK_REJECT_REASON_NO_CAPACITY, awaitPullFrame(t, second).GetReject().GetReason())
					require.NoError(t, second.send(reservedRPCTestTask("rpc", "second-rpc")))
					require.Equal(t, "rpc", awaitReservedRPCEntry(t, flow))
				} else {
					require.Equal(t, "execute", awaitReservedRPCEntry(t, flow))
				}
				idle := awaitReservedRPCStream(t, server, GetFinalFlowType(flow))
				for _, overflow := range []string{"waitFor", "execute", "rpc"} {
					require.NoError(t, idle.send(reservedRPCTestTask(overflow, "overflow-"+overflow)))
					require.Equal(t, dexpb.PollWorkerTaskRejectReason_POLL_WORKER_TASK_REJECT_REASON_NO_CAPACITY, awaitPullFrame(t, idle).GetReject().GetReason())
				}
				require.Equal(t, uint64(reserve), worker.PullReservedRPCDeclines())
				require.NoError(t, idle.send(&dexpb.PollWorkerTaskResponse{Frame: &dexpb.PollWorkerTaskResponse_Ping{Ping: &dexpb.PollWorkerTaskPing{}}}))
				require.NotNil(t, awaitPullFrame(t, idle).GetPong())
				require.True(t, worker.PullRegistrationState().IsReady)
			})
		}
	}
}

func TestWorkerPullReservedRPCConcurrentAdmission(t *testing.T) {
	address, server := startPullTestServer(t)
	flow := &reservedRPCFlow{entered: make(chan string, 20), release: make(chan struct{})}
	worker, err := newWorkerForTest(t, []Flow{flow}, WorkerOptions{FlowServiceAddress: address, Pull: &PullWorkerOptions{MaxConcurrentTasks: 2, ReservedRPCTasks: ptr.Any(1), IdleStreamsPerQueue: 20}})
	require.NoError(t, err)
	started := make(chan error, 1)
	go func() { started <- worker.Start() }()
	defer func() {
		close(flow.release)
		require.NoError(t, worker.Stop(context.Background()))
		require.NoError(t, <-started)
	}()
	streams := make([]*pullTestStream, 0, 20)
	for len(streams) < 20 {
		candidate := awaitPullRegistration(t, server)
		if candidate.registration.Queue == GetFinalFlowType(flow) {
			streams = append(streams, candidate)
		}
	}
	var wg sync.WaitGroup
	for index, stream := range streams {
		wg.Add(1)
		go func() {
			defer wg.Done()
			require.NoError(t, stream.send(reservedRPCTestTask("execute", fmt.Sprint(index))))
		}()
	}
	wg.Wait()
	require.Equal(t, "execute", awaitReservedRPCEntry(t, flow))
	rejected := 0
	for _, stream := range streams {
		require.NoError(t, stream.send(&dexpb.PollWorkerTaskResponse{Frame: &dexpb.PollWorkerTaskResponse_Ping{Ping: &dexpb.PollWorkerTaskPing{}}}))
		frame := awaitPullFrame(t, stream)
		if frame.GetReject() != nil {
			require.Equal(t, dexpb.PollWorkerTaskRejectReason_POLL_WORKER_TASK_REJECT_REASON_NO_CAPACITY, frame.GetReject().GetReason())
			rejected++
			frame = awaitPullFrame(t, stream)
		}
		require.NotNil(t, frame.GetPong())
	}
	require.Equal(t, 19, rejected)
	worker.pull.mu.Lock()
	require.Equal(t, 1, worker.pull.busyTasks)
	worker.pull.mu.Unlock()
}

func (flow *reservedRPCFlow) GetSteps() []StepDef {
	return []StepDef{DefineStartStep(reservedRPCStep{flow: flow})}
}

func (flow *reservedRPCFlow) GetRPCs() []RPCDef {
	return []RPCDef{DefineRPC(flow.GetResult, nil)}
}

func (*reservedRPCFlow) GetPersistenceSchema() PersistenceSchema { return PersistenceSchema{} }

func (flow *reservedRPCFlow) GetResult(ctx Context, _ None) (*RPCResult[bool], error) {
	if err := flow.block(ctx, "rpc"); err != nil {
		return nil, err
	}
	return &RPCResult[bool]{Output: true}, nil
}

func (flow *reservedRPCFlow) block(ctx Context, kind string) error {
	flow.entered <- kind
	select {
	case <-flow.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type reservedRPCStep struct {
	StepDefaults
	flow *reservedRPCFlow
}

func (step reservedRPCStep) WaitFor(ctx Context, _ None) (*Wait, error) {
	if err := step.flow.block(ctx, "waitFor"); err != nil {
		return nil, err
	}
	return SkipWaitImmediately(), nil
}

func (step reservedRPCStep) Execute(ctx Context, _ None) (*StepDecision, error) {
	if err := step.flow.block(ctx, "execute"); err != nil {
		return nil, err
	}
	return GracefulComplete(true), nil
}

func awaitReservedRPCEntry(t *testing.T, flow *reservedRPCFlow) string {
	t.Helper()
	select {
	case kind := <-flow.entered:
		return kind
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not start")
		return ""
	}
}

func reservedRPCTestTask(kind, taskID string) *dexpb.PollWorkerTaskResponse {
	task := &dexpb.PollWorkerTask{TaskId: taskID}
	switch kind {
	case "waitFor":
		task.Request = &dexpb.PollWorkerTask_WaitFor{WaitFor: &dexpb.InvokeWaitForMethodRequest{Context: workerStepContext(), FlowType: "reservedRPCFlow", StepType: "reservedRPCStep", StepInput: newNullValue()}}
	case "execute":
		task.Request = &dexpb.PollWorkerTask_Execute{Execute: &dexpb.InvokeExecuteMethodRequest{Context: workerStepContext(), FlowType: "reservedRPCFlow", StepType: "reservedRPCStep", StepInput: newNullValue()}}
	case "rpc":
		task.Request = &dexpb.PollWorkerTask_Rpc{Rpc: &dexpb.InvokeWorkerRPCRequest{Context: workerRPCContext(), FlowType: "reservedRPCFlow", RpcName: "GetResult", Input: newNullValue()}}
	}
	return &dexpb.PollWorkerTaskResponse{Frame: &dexpb.PollWorkerTaskResponse_Task{Task: task}}
}

func awaitReservedRPCStream(t *testing.T, server *pullTestServer, queue string) *pullTestStream {
	t.Helper()
	for {
		candidate := awaitPullRegistration(t, server)
		if candidate.registration.Queue == queue {
			return candidate
		}
	}
}

type reservedRPCWarningLogger struct {
	Logger
	warnings atomic.Int32
}

func TestWorkerPullReservedRPCDefaults(t *testing.T) {
	for _, capacity := range []int{1, 4} {
		address, server := startPullTestServer(t)
		logger := &reservedRPCWarningLogger{Logger: slog.Default()}
		worker, err := newWorkerForTest(t, nil, WorkerOptions{FlowServiceAddress: address, Logger: logger, Pull: &PullWorkerOptions{MaxConcurrentTasks: capacity}})
		require.NoError(t, err)
		expectedReserve := 1
		expectedWarnings := int32(0)
		if capacity == 1 {
			expectedReserve = 0
			expectedWarnings = 1
		}
		require.Equal(t, expectedReserve, worker.pull.reservedRPCTasks)
		require.Zero(t, worker.PullReservedRPCDeclines())
		started := make(chan error, 1)
		go func() { started <- worker.Start() }()
		awaitPullRegistration(t, server)
		require.NoError(t, worker.Stop(context.Background()))
		require.NoError(t, <-started)
		require.Equal(t, expectedWarnings, logger.warnings.Load())
	}
}

func (logger *reservedRPCWarningLogger) Warn(message string, keyvals ...interface{}) {
	if strings.Contains(message, "RPCs may wait") {
		logger.warnings.Add(1)
	}
	logger.Logger.Warn(message, keyvals...)
}
