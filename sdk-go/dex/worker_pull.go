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
	"crypto/rand"
	"encoding/base32"
	"errors"
	"fmt"
	"io"
	"math/big"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/superdurable/dex/sdk-go/gen/dexpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

type pullWorker struct {
	service             *workerService
	client              dexpb.WorkerTaskServiceClient
	appWorkerID         string
	flowTypes           []string
	queues              []string
	maxConcurrentTasks  int
	idleStreamsPerQueue int
	sdkVersion          string
	ctx                 context.Context
	cancel              context.CancelFunc
	mu                  sync.Mutex
	streams             map[*pullStream]struct{}
	busyTasks           int
	isStopping          bool
	wake                chan struct{}
	done                chan struct{}
}

type pullStream struct {
	worker      *pullWorker
	queue       string
	ctx         context.Context
	cancel      context.CancelFunc
	mu          sync.Mutex
	sendMu      sync.Mutex
	stream      dexpb.WorkerTaskService_PollWorkerTaskClient
	taskID      string
	taskCancel  context.CancelFunc
	taskDone    chan struct{}
	isBusy      bool
	isDraining  bool
	isPreferred bool
}

func newPullWorker(registry *Registry, service *workerService, conn *grpc.ClientConn, options PullWorkerOptions, sdkVersion string) (*pullWorker, error) {
	maxConcurrentTasks := options.MaxConcurrentTasks
	if maxConcurrentTasks < 0 {
		return nil, fmt.Errorf("dex: MaxConcurrentTasks must not be negative")
	}
	if maxConcurrentTasks == 0 {
		maxConcurrentTasks = runtime.NumCPU()
	}
	idleStreamsPerQueue := options.IdleStreamsPerQueue
	if idleStreamsPerQueue < 0 {
		return nil, fmt.Errorf("dex: IdleStreamsPerQueue must not be negative")
	}
	if idleStreamsPerQueue == 0 {
		idleStreamsPerQueue = 1
	}
	appWorkerID := options.AppWorkerID
	if appWorkerID == "" {
		var err error
		appWorkerID, err = generateAppWorkerID()
		if err != nil {
			return nil, err
		}
	}
	if err := validateAppWorkerID(appWorkerID); err != nil {
		return nil, err
	}
	flowTypes := make([]string, 0, len(registry.flows))
	for flowType := range registry.flows {
		flowTypes = append(flowTypes, flowType)
	}
	sort.Strings(flowTypes)
	for _, flowType := range flowTypes {
		if flowType == appWorkerID {
			return nil, fmt.Errorf("dex: app Worker ID conflicts with Flow type %q", flowType)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &pullWorker{
		service: service, client: dexpb.NewWorkerTaskServiceClient(conn), appWorkerID: appWorkerID,
		flowTypes: flowTypes, queues: append([]string{appWorkerID}, flowTypes...),
		maxConcurrentTasks: maxConcurrentTasks, idleStreamsPerQueue: idleStreamsPerQueue, sdkVersion: sdkVersion,
		ctx: ctx, cancel: cancel, streams: make(map[*pullStream]struct{}), wake: make(chan struct{}, 1), done: make(chan struct{}),
	}, nil
}

func generateAppWorkerID() (string, error) {
	identity := ""
	for _, candidate := range []string{os.Getenv("DEX_WORKER_INSTANCE"), os.Getenv("POD_NAME"), os.Getenv("HOSTNAME")} {
		if strings.TrimSpace(candidate) != "" {
			identity = candidate
			break
		}
	}
	if identity == "" {
		hostname, err := os.Hostname()
		if err != nil {
			return "", fmt.Errorf("dex: read Worker hostname: %w", err)
		}
		identity = hostname
	}
	var sanitized strings.Builder
	for _, character := range strings.ToLower(identity) {
		if character >= 'a' && character <= 'z' || character >= '0' && character <= '9' || character == '-' {
			sanitized.WriteRune(character)
		} else if sanitized.Len() > 0 {
			sanitized.WriteByte('-')
		}
		if sanitized.Len() >= 54 {
			break
		}
	}
	cleaned := strings.Trim(sanitized.String(), "-")
	if cleaned == "" {
		cleaned = "worker"
	}
	randomBytes := make([]byte, 5)
	if _, err := rand.Read(randomBytes); err != nil {
		return "", fmt.Errorf("dex: generate Worker ID: %w", err)
	}
	return cleaned + "-" + strings.ToLower(base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(randomBytes)), nil
}

func validateAppWorkerID(appWorkerID string) error {
	if len(appWorkerID) == 0 || len(appWorkerID) > 63 {
		return fmt.Errorf("dex: AppWorkerID must contain 1-63 characters")
	}
	for _, character := range appWorkerID {
		if character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' || character >= '0' && character <= '9' || character == '-' || character == '_' || character == '.' {
			continue
		}
		return fmt.Errorf("dex: AppWorkerID contains invalid character %q", character)
	}
	return nil
}

func (worker *pullWorker) run() {
	defer close(worker.done)
	worker.reconcile()
	for {
		select {
		case <-worker.ctx.Done():
			return
		case <-worker.wake:
			worker.reconcile()
		}
	}
}

func (worker *pullWorker) stop() {
	worker.mu.Lock()
	worker.isStopping = true
	for stream := range worker.streams {
		stream.mu.Lock()
		stream.isDraining = true
		if !stream.isBusy {
			stream.cancel()
		}
		stream.mu.Unlock()
	}
	if len(worker.streams) == 0 {
		worker.cancel()
	}
	worker.mu.Unlock()
}

func (worker *pullWorker) signal() {
	select {
	case worker.wake <- struct{}{}:
	default:
	}
}

func (worker *pullWorker) reconcile() {
	worker.mu.Lock()
	defer worker.mu.Unlock()
	if worker.isStopping {
		return
	}
	for _, queue := range worker.queues {
		idle := 0
		for stream := range worker.streams {
			stream.mu.Lock()
			isIdle := !stream.isBusy && !stream.isDraining
			stream.mu.Unlock()
			if stream.queue == queue && isIdle {
				idle++
			}
		}
		desired := worker.idleStreamsPerQueue
		if worker.busyTasks >= worker.maxConcurrentTasks {
			desired = 0
		}
		for idle < desired {
			streamCtx, cancel := context.WithCancel(worker.ctx)
			stream := &pullStream{worker: worker, queue: queue, ctx: streamCtx, cancel: cancel}
			worker.streams[stream] = struct{}{}
			idle++
			go stream.run()
		}
		for pass := 0; pass < 2 && idle > desired; pass++ {
			for stream := range worker.streams {
				if idle <= desired {
					break
				}
				stream.mu.Lock()
				if stream.queue == queue && !stream.isBusy && !stream.isDraining && (pass == 1 || !stream.isPreferred) {
					stream.isDraining = true
					stream.cancel()
					idle--
				}
				stream.mu.Unlock()
			}
		}
	}
}

func (stream *pullStream) run() {
	defer stream.remove()
	backoff := 100 * time.Millisecond
	for stream.ctx.Err() == nil {
		err := stream.poll()
		if stream.ctx.Err() != nil {
			return
		}
		stream.worker.mu.Lock()
		isStopping := stream.worker.isStopping
		stream.worker.mu.Unlock()
		if isStopping {
			return
		}
		if stream.isReconnecting() {
			backoff = 100 * time.Millisecond
			continue
		}
		if err != nil && !errors.Is(err, io.EOF) {
			stream.worker.service.logger.Warn("Worker task stream disconnected", "queue", stream.queue, "error", err)
		}
		jitter, jitterErr := rand.Int(rand.Reader, big.NewInt(int64(backoff/2)))
		if jitterErr != nil {
			stream.worker.service.logger.Error("generate Worker reconnect jitter", "error", jitterErr)
			return
		}
		wait := backoff + time.Duration(jitter.Int64())
		select {
		case <-stream.ctx.Done():
			return
		case <-time.After(wait):
		}
		backoff *= 2
		if backoff > 5*time.Second {
			backoff = 5 * time.Second
		}
	}
}

func (stream *pullStream) poll() error {
	pollCtx, cancel := context.WithCancel(stream.ctx)
	defer cancel()
	clientStream, err := stream.worker.client.PollWorkerTask(pollCtx)
	if err != nil {
		return err
	}
	stream.mu.Lock()
	stream.stream = clientStream
	stream.mu.Unlock()
	defer func() {
		if closeErr := clientStream.CloseSend(); closeErr != nil {
			stream.worker.service.logger.Debug("close Worker poll send", "error", closeErr)
		}
	}()
	register := &dexpb.PollWorkerTaskRequest{Frame: &dexpb.PollWorkerTaskRequest_Register{Register: &dexpb.PollWorkerTaskRegister{
		Queue: stream.queue, AppWorkerId: stream.worker.appWorkerID, FlowTypes: stream.worker.flowTypes, SdkName: "go", SdkVersion: stream.worker.sdkVersion,
	}}}
	if err := stream.send(register); err != nil {
		return err
	}
	registered := false
	for {
		response, err := clientStream.Recv()
		if err != nil {
			stream.cancelTask()
			return err
		}
		if !registered {
			if response.GetRegistered() == nil {
				return fmt.Errorf("dex: Worker task stream did not register")
			}
			registered = true
			stream.worker.signal()
			continue
		}
		switch frame := response.Frame.(type) {
		case *dexpb.PollWorkerTaskResponse_Task:
			stream.startTask(frame.Task)
		case *dexpb.PollWorkerTaskResponse_CancelTask:
			stream.mu.Lock()
			if stream.taskID == frame.CancelTask.TaskId && stream.taskCancel != nil {
				stream.taskCancel()
			}
			stream.mu.Unlock()
		case *dexpb.PollWorkerTaskResponse_Ping:
			if err := stream.send(&dexpb.PollWorkerTaskRequest{Frame: &dexpb.PollWorkerTaskRequest_Pong{Pong: &dexpb.PollWorkerTaskPong{}}}); err != nil {
				return err
			}
		case *dexpb.PollWorkerTaskResponse_Reconnect:
			stream.mu.Lock()
			stream.isDraining = true
			isBusy := stream.isBusy
			stream.mu.Unlock()
			stream.worker.signal()
			if !isBusy {
				return nil
			}
		default:
			return fmt.Errorf("dex: unrecognized Worker task frame %T", frame)
		}
	}
}

func (stream *pullStream) startTask(task *dexpb.PollWorkerTask) {
	stream.worker.mu.Lock()
	stream.mu.Lock()
	if stream.isBusy || stream.isDraining || stream.worker.isStopping || stream.worker.busyTasks >= stream.worker.maxConcurrentTasks {
		stream.mu.Unlock()
		stream.worker.mu.Unlock()
		if err := stream.send(&dexpb.PollWorkerTaskRequest{Frame: &dexpb.PollWorkerTaskRequest_Reject{Reject: &dexpb.PollWorkerTaskReject{TaskId: task.GetTaskId(), Reason: dexpb.PollWorkerTaskRejectReason_POLL_WORKER_TASK_REJECT_REASON_NO_CAPACITY}}}); err != nil {
			stream.worker.service.logger.Error("reject Worker task", "error", err)
		}
		return
	}
	taskCtx := stream.ctx
	var cancel context.CancelFunc
	if task.GetDeadline() != nil {
		taskCtx, cancel = context.WithDeadline(taskCtx, task.GetDeadline().AsTime())
	} else {
		taskCtx, cancel = context.WithCancel(taskCtx)
	}
	stream.taskCancel = cancel
	stream.taskID = task.GetTaskId()
	stream.taskDone = make(chan struct{})
	stream.isBusy = true
	stream.worker.busyTasks++
	stream.mu.Unlock()
	stream.worker.mu.Unlock()
	stream.worker.signal()
	go stream.executeTask(taskCtx, task)
}

func (stream *pullStream) executeTask(ctx context.Context, task *dexpb.PollWorkerTask) {
	defer stream.finishTask()
	var err error
	defer func() {
		if finalErr := finishWorkerCall(stream.worker.service.logger, recover(), err); finalErr != nil {
			workerError := &dexpb.WorkerErrorResponse{Detail: finalErr.Error()}
			if rpcStatus, ok := status.FromError(finalErr); ok {
				for _, detail := range rpcStatus.Details() {
					if typed, ok := detail.(*dexpb.WorkerErrorResponse); ok {
						workerError = typed
						break
					}
				}
			}
			if sendErr := stream.sendTaskFrame(&dexpb.PollWorkerTaskFrame{TaskId: task.GetTaskId(), Output: &dexpb.PollWorkerTaskFrame_WorkerError{WorkerError: workerError}}); sendErr != nil {
				stream.worker.service.logger.Error("send Worker error", "error", sendErr)
			}
		}
	}()
	switch request := task.Request.(type) {
	case *dexpb.PollWorkerTask_WaitFor:
		err = stream.worker.service.invokeWaitForMethod(request.WaitFor, newWaitForOutputStream(&pullWaitForSender{stream: stream, ctx: ctx, taskID: task.GetTaskId()}))
	case *dexpb.PollWorkerTask_Execute:
		err = stream.worker.service.invokeExecuteMethod(request.Execute, newExecuteOutputStream(&pullExecuteSender{stream: stream, ctx: ctx, taskID: task.GetTaskId()}))
	case *dexpb.PollWorkerTask_Rpc:
		var response *dexpb.InvokeWorkerRPCResponse
		response, err = stream.worker.service.invokeWorkerRPC(ctx, request.Rpc)
		if err == nil {
			err = stream.sendTaskFrame(&dexpb.PollWorkerTaskFrame{TaskId: task.GetTaskId(), Output: &dexpb.PollWorkerTaskFrame_Rpc{Rpc: response}})
		}
	default:
		err = fmt.Errorf("dex: unsupported Worker task request %T", request)
	}
}

func (stream *pullStream) finishTask() {
	stream.worker.mu.Lock()
	stream.mu.Lock()
	stream.taskCancel()
	stream.taskCancel = nil
	stream.taskID = ""
	stream.isBusy = false
	close(stream.taskDone)
	stream.worker.busyTasks--
	isDraining := stream.isDraining
	stream.mu.Unlock()
	stream.worker.mu.Unlock()
	if isDraining {
		stream.sendMu.Lock()
		if err := stream.stream.CloseSend(); err != nil {
			stream.worker.service.logger.Error("close drained Worker stream", "error", err)
		}
		stream.sendMu.Unlock()
	}
	stream.worker.signal()
}

func (stream *pullStream) cancelTask() {
	stream.mu.Lock()
	if stream.taskCancel != nil {
		stream.taskCancel()
	}
	taskDone := stream.taskDone
	stream.mu.Unlock()
	if taskDone != nil {
		<-taskDone
	}
}

func (stream *pullStream) isReconnecting() bool {
	stream.mu.Lock()
	defer stream.mu.Unlock()
	if !stream.isDraining {
		return false
	}
	stream.isDraining = false
	stream.isPreferred = true
	return true
}

func (stream *pullStream) remove() {
	stream.cancelTask()
	stream.worker.mu.Lock()
	delete(stream.worker.streams, stream)
	if stream.worker.isStopping && len(stream.worker.streams) == 0 {
		stream.worker.cancel()
	}
	stream.worker.mu.Unlock()
	stream.worker.signal()
}

func (stream *pullStream) send(request *dexpb.PollWorkerTaskRequest) error {
	stream.sendMu.Lock()
	defer stream.sendMu.Unlock()
	stream.mu.Lock()
	clientStream := stream.stream
	stream.mu.Unlock()
	return clientStream.Send(request)
}

func (stream *pullStream) sendTaskFrame(frame *dexpb.PollWorkerTaskFrame) error {
	return stream.send(&dexpb.PollWorkerTaskRequest{Frame: &dexpb.PollWorkerTaskRequest_TaskFrame{TaskFrame: frame}})
}

type pullWaitForSender struct {
	stream *pullStream
	ctx    context.Context
	taskID string
}

func (sender *pullWaitForSender) Context() context.Context { return sender.ctx }
func (sender *pullWaitForSender) Send(output *dexpb.InvokeWaitForMethodOutput) error {
	return sender.stream.sendTaskFrame(&dexpb.PollWorkerTaskFrame{TaskId: sender.taskID, Output: &dexpb.PollWorkerTaskFrame_WaitFor{WaitFor: output}})
}

type pullExecuteSender struct {
	stream *pullStream
	ctx    context.Context
	taskID string
}

func (sender *pullExecuteSender) Context() context.Context { return sender.ctx }
func (sender *pullExecuteSender) Send(output *dexpb.InvokeExecuteMethodOutput) error {
	return sender.stream.sendTaskFrame(&dexpb.PollWorkerTaskFrame{TaskId: sender.taskID, Output: &dexpb.PollWorkerTaskFrame_Execute{Execute: output}})
}
