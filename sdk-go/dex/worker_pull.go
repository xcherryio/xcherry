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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type pullWorker struct {
	service                        *workerService
	client                         dexpb.WorkerTaskServiceClient
	appWorkerID                    string
	flowTypes                      []string
	queues                         []string
	maxConcurrentTasks             int
	idleStreamsPerQueue            int
	stopDrainTimeout               time.Duration
	stopOnPermanentStreamRejection bool
	sdkVersion                     string
	ctx                            context.Context
	cancel                         context.CancelFunc
	mu                             sync.Mutex
	streams                        map[*pullStream]struct{}
	streamWG                       sync.WaitGroup
	unsupportedOnce                sync.Once
	busyTasks                      int
	isStopping                     bool
	wake                           chan struct{}
	done                           chan struct{}
	registrationChanged            chan struct{}
	queueRegistrations             map[string]*pullQueueRegistration
	hasRegisteredAllQueues         bool
	initialRegistrationError       error
	startError                     error
	hasStartReturned               bool
	permanentStreamError           *PullRegistrationError
	permanentStreamRejected        chan struct{}
}

type pullQueueRegistration struct {
	registeredStreams  int
	hasRegistered      bool
	lastPermanentError error
}

type pullStream struct {
	worker       *pullWorker
	queue        string
	ctx          context.Context
	cancel       context.CancelFunc
	mu           sync.Mutex
	sendMu       sync.Mutex
	stream       dexpb.WorkerTaskService_PollWorkerTaskClient
	isSendClosed bool
	// Registration state is guarded by worker.mu, independently of stream.mu and sendMu.
	isRegistered                 bool
	taskID                       string
	taskCancel                   context.CancelFunc
	taskFinished                 chan struct{}
	isBusy                       bool
	isDraining                   bool
	supportsUnknownNameRejection bool
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
	stopDrainTimeout := options.StopDrainTimeout
	if stopDrainTimeout < 0 {
		return nil, fmt.Errorf("dex: StopDrainTimeout must not be negative")
	}
	if stopDrainTimeout == 0 {
		stopDrainTimeout = 30 * time.Second
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
	queues := append([]string{appWorkerID}, flowTypes...)
	queueRegistrations := make(map[string]*pullQueueRegistration, len(queues))
	for _, queue := range queues {
		queueRegistrations[queue] = &pullQueueRegistration{}
	}
	return &pullWorker{
		service: service, client: dexpb.NewWorkerTaskServiceClient(conn), appWorkerID: appWorkerID,
		flowTypes: flowTypes, queues: queues,
		queueRegistrations: queueRegistrations, registrationChanged: make(chan struct{}), permanentStreamRejected: make(chan struct{}),
		stopOnPermanentStreamRejection: options.StopOnPermanentStreamRejection,
		maxConcurrentTasks:             maxConcurrentTasks, idleStreamsPerQueue: idleStreamsPerQueue, stopDrainTimeout: stopDrainTimeout, sdkVersion: sdkVersion,
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

func (worker *pullWorker) run() error {
	defer func() {
		worker.streamWG.Wait()
		close(worker.done)
	}()
	worker.reconcile()
	for {
		select {
		case <-worker.ctx.Done():
			worker.mu.Lock()
			err := worker.permanentStreamError
			worker.mu.Unlock()
			if err != nil && worker.stopOnPermanentStreamRejection {
				return err
			}
			return nil
		case <-worker.permanentStreamRejected:
			worker.stop()
			drainTimer := time.AfterFunc(worker.stopDrainTimeout, worker.cancel)
			<-worker.ctx.Done()
			drainTimer.Stop()
			worker.mu.Lock()
			err := worker.permanentStreamError
			worker.mu.Unlock()
			return err
		case <-worker.wake:
			worker.reconcile()
		}
	}
}

func (worker *pullWorker) stop() {
	worker.mu.Lock()
	worker.isStopping = true
	worker.notifyRegistrationChanged()
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
			isIdle := !stream.isBusy
			stream.mu.Unlock()
			if stream.queue == queue && isIdle {
				idle++
			}
		}
		desired := worker.idleStreamsPerQueue
		for idle < desired {
			streamCtx, cancel := context.WithCancel(worker.ctx)
			stream := &pullStream{worker: worker, queue: queue, ctx: streamCtx, cancel: cancel}
			worker.streams[stream] = struct{}{}
			worker.streamWG.Add(1)
			idle++
			go stream.run()
		}
	}
}

func (worker *pullWorker) waitRegistered(ctx context.Context) error {
	for {
		worker.mu.Lock()
		if worker.hasRegisteredAllQueues {
			worker.mu.Unlock()
			return nil
		}
		err := worker.initialRegistrationError
		if err == nil && worker.hasStartReturned {
			err = worker.startError
			if err == nil {
				err = fmt.Errorf("dex: pull Worker stopped before registration")
			}
		}
		if err == nil && worker.isStopping {
			err = fmt.Errorf("dex: pull Worker stopped before registration")
		}
		changed := worker.registrationChanged
		worker.mu.Unlock()
		if err != nil {
			return err
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (worker *pullWorker) registrationState() PullRegistrationState {
	worker.mu.Lock()
	defer worker.mu.Unlock()
	state := PullRegistrationState{IsReady: worker.hasRegisteredAllQueues, Queues: make([]PullQueueRegistrationState, 0, len(worker.queues))}
	for _, queue := range worker.queues {
		registration := worker.queueRegistrations[queue]
		hasRegisteredStream := registration.registeredStreams > 0
		state.Queues = append(state.Queues, PullQueueRegistrationState{
			Queue: queue, HasRegisteredStream: hasRegisteredStream, LastPermanentError: registration.lastPermanentError,
		})
		if !hasRegisteredStream && registration.lastPermanentError != nil {
			state.IsReady = false
		}
	}
	return state
}

func (worker *pullWorker) recordRegisteredStream(stream *pullStream) {
	worker.mu.Lock()
	defer worker.mu.Unlock()
	registration := worker.queueRegistrations[stream.queue]
	stream.isRegistered = true
	registration.registeredStreams++
	registration.hasRegistered = true
	registration.lastPermanentError = nil
	if !worker.hasRegisteredAllQueues {
		hasRegisteredAllQueues := true
		for _, queue := range worker.queues {
			if !worker.queueRegistrations[queue].hasRegistered {
				hasRegisteredAllQueues = false
				break
			}
		}
		worker.hasRegisteredAllQueues = hasRegisteredAllQueues
	}
	worker.notifyRegistrationChanged()
}

func (worker *pullWorker) recordStreamResult(stream *pullStream, err error) {
	worker.mu.Lock()
	defer worker.mu.Unlock()
	registration := worker.queueRegistrations[stream.queue]
	if stream.isRegistered {
		stream.isRegistered = false
		registration.registeredStreams--
	}
	if isPermanentPullStreamError(err) {
		registration.lastPermanentError = err
		rejection := &PullRegistrationError{Queue: stream.queue, Err: err}
		if registration.registeredStreams == 0 && !worker.hasRegisteredAllQueues && worker.initialRegistrationError == nil {
			worker.initialRegistrationError = rejection
		}
		if registration.registeredStreams == 0 && worker.stopOnPermanentStreamRejection && worker.permanentStreamError == nil {
			worker.permanentStreamError = rejection
			close(worker.permanentStreamRejected)
		}
	}
	worker.notifyRegistrationChanged()
}

func (worker *pullWorker) finishRegistration(err error) {
	worker.mu.Lock()
	defer worker.mu.Unlock()
	worker.startError = err
	worker.hasStartReturned = true
	worker.notifyRegistrationChanged()
}

func (worker *pullWorker) notifyRegistrationChanged() {
	close(worker.registrationChanged)
	worker.registrationChanged = make(chan struct{})
}

func (stream *pullStream) run() {
	defer stream.worker.streamWG.Done()
	defer stream.remove()
	backoff := 100 * time.Millisecond
	for stream.ctx.Err() == nil {
		wasRegistered, err := stream.poll()
		stream.worker.recordStreamResult(stream, err)
		stream.waitForTaskFinished()
		if wasRegistered {
			backoff = 100 * time.Millisecond
		}
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
		}
		isPermanent := isPermanentPullStreamError(err)
		if status.Code(err) == codes.Unimplemented {
			stream.worker.unsupportedOnce.Do(func() {
				stream.worker.service.logger.Error("pull Worker polling is unsupported by this Dex server", "error", err)
			})
			backoff = time.Minute
		} else if isPermanent {
			stream.worker.service.logger.Error("Worker task stream rejected", "queue", stream.queue, "error", err)
			backoff = time.Minute
		} else if err != nil && !errors.Is(err, io.EOF) {
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
		if !isPermanent {
			backoff *= 2
			if backoff > 5*time.Second {
				backoff = 5 * time.Second
			}
		}
	}
}

func isPermanentPullStreamError(err error) bool {
	switch status.Code(err) {
	case codes.Unauthenticated, codes.PermissionDenied, codes.InvalidArgument, codes.Unimplemented, codes.ResourceExhausted:
		return true
	default:
		return false
	}
}

func (stream *pullStream) poll() (bool, error) {
	stream.mu.Lock()
	stream.supportsUnknownNameRejection = false
	stream.mu.Unlock()
	pollCtx, cancel := context.WithCancel(stream.ctx)
	clientStream, err := stream.worker.client.PollWorkerTask(pollCtx)
	if err != nil {
		cancel()
		return false, err
	}
	stream.sendMu.Lock()
	stream.stream = clientStream
	stream.isSendClosed = false
	stream.sendMu.Unlock()
	register := &dexpb.PollWorkerTaskRequest{Frame: &dexpb.PollWorkerTaskRequest_Register{Register: &dexpb.PollWorkerTaskRegister{
		Queue: stream.queue, AppWorkerId: stream.worker.appWorkerID, FlowTypes: stream.worker.flowTypes, SdkName: "go", SdkVersion: stream.worker.sdkVersion,
	}}}
	if err := stream.send(register); err != nil {
		stream.closeSend()
		cancel()
		return false, err
	}
	responses := make(chan *dexpb.PollWorkerTaskResponse, 1)
	receiveErrors := make(chan error, 1)
	receiveDone := make(chan struct{})
	go receivePullFrames(pollCtx, clientStream, responses, receiveErrors, receiveDone)
	defer func() {
		cancel()
		<-receiveDone
		stream.closeSend()
	}()
	wasRegistered := false
	for {
		select {
		case response := <-responses:
			if !wasRegistered {
				if response.GetReconnect() != nil {
					stream.markDraining()
					return false, nil
				}
				if response.GetRegistered() == nil {
					return false, fmt.Errorf("dex: Worker task stream did not register")
				}
				stream.mu.Lock()
				stream.supportsUnknownNameRejection = response.GetRegistered().GetSupportsUnknownNameRejection()
				stream.mu.Unlock()
				wasRegistered = true
				stream.worker.recordRegisteredStream(stream)
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
					stream.cancelTaskContext()
					return wasRegistered, err
				}
			case *dexpb.PollWorkerTaskResponse_Reconnect:
				if !stream.markDraining() {
					return wasRegistered, nil
				}
				stream.worker.signal()
			default:
				stream.worker.service.logger.Debug("ignore unknown Worker task frame", "frame", fmt.Sprintf("%T", frame))
			}
		case <-stream.taskFinished:
			stream.completeTask()
		case err := <-receiveErrors:
			stream.cancelTaskContext()
			return wasRegistered, err
		case <-pollCtx.Done():
			stream.cancelTaskContext()
			return wasRegistered, pollCtx.Err()
		}
	}
}

func receivePullFrames(ctx context.Context, stream dexpb.WorkerTaskService_PollWorkerTaskClient, responses chan<- *dexpb.PollWorkerTaskResponse, receiveErrors chan<- error, done chan<- struct{}) {
	defer close(done)
	for {
		response, err := stream.Recv()
		if err != nil {
			receiveErrors <- err
			return
		}
		select {
		case responses <- response:
		case <-ctx.Done():
			return
		}
	}
}

func (stream *pullStream) startTask(task *dexpb.PollWorkerTask) {
	stream.mu.Lock()
	supportsUnknownNameRejection := stream.supportsUnknownNameRejection
	stream.mu.Unlock()
	reason := dexpb.PollWorkerTaskRejectReason_POLL_WORKER_TASK_REJECT_REASON_UNSPECIFIED
	if supportsUnknownNameRejection {
		reason = stream.worker.unknownTaskReason(task)
	}
	if reason != dexpb.PollWorkerTaskRejectReason_POLL_WORKER_TASK_REJECT_REASON_UNSPECIFIED {
		stream.rejectTask(task.GetTaskId(), reason)
		return
	}
	stream.worker.mu.Lock()
	stream.mu.Lock()
	if stream.isBusy || stream.isDraining || stream.worker.isStopping || stream.worker.busyTasks >= stream.worker.maxConcurrentTasks {
		stream.mu.Unlock()
		stream.worker.mu.Unlock()
		stream.rejectTask(task.GetTaskId(), dexpb.PollWorkerTaskRejectReason_POLL_WORKER_TASK_REJECT_REASON_NO_CAPACITY)
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
	stream.taskFinished = make(chan struct{}, 1)
	stream.isBusy = true
	stream.worker.busyTasks++
	stream.mu.Unlock()
	stream.worker.mu.Unlock()
	stream.worker.signal()
	go stream.executeTask(taskCtx, task)
}

func (worker *pullWorker) unknownTaskReason(task *dexpb.PollWorkerTask) dexpb.PollWorkerTaskRejectReason {
	var flowType, stepType, rpcName string
	isRPC := false
	switch request := task.Request.(type) {
	case *dexpb.PollWorkerTask_WaitFor:
		flowType, stepType = request.WaitFor.GetFlowType(), request.WaitFor.GetStepType()
	case *dexpb.PollWorkerTask_Execute:
		flowType, stepType = request.Execute.GetFlowType(), request.Execute.GetStepType()
	case *dexpb.PollWorkerTask_Rpc:
		flowType, rpcName = request.Rpc.GetFlowType(), request.Rpc.GetRpcName()
		isRPC = true
	default:
		return dexpb.PollWorkerTaskRejectReason_POLL_WORKER_TASK_REJECT_REASON_UNSPECIFIED
	}
	flow, found := worker.service.registry.lookupFlow(flowType)
	if !isRPC && stepType == timeoutHandlerStepType {
		if found && flow.timeoutHandler != nil {
			return dexpb.PollWorkerTaskRejectReason_POLL_WORKER_TASK_REJECT_REASON_UNSPECIFIED
		}
		return dexpb.PollWorkerTaskRejectReason_POLL_WORKER_TASK_REJECT_REASON_UNKNOWN_STEP
	}
	if isRPC {
		if !found {
			return dexpb.PollWorkerTaskRejectReason_POLL_WORKER_TASK_REJECT_REASON_UNKNOWN_RPC
		}
		if _, found = flow.lookupRPC(rpcName); !found {
			return dexpb.PollWorkerTaskRejectReason_POLL_WORKER_TASK_REJECT_REASON_UNKNOWN_RPC
		}
		return dexpb.PollWorkerTaskRejectReason_POLL_WORKER_TASK_REJECT_REASON_UNSPECIFIED
	}
	if !found {
		return dexpb.PollWorkerTaskRejectReason_POLL_WORKER_TASK_REJECT_REASON_UNKNOWN_STEP
	}
	if _, found = flow.lookupStep(stepType); !found {
		return dexpb.PollWorkerTaskRejectReason_POLL_WORKER_TASK_REJECT_REASON_UNKNOWN_STEP
	}
	return dexpb.PollWorkerTaskRejectReason_POLL_WORKER_TASK_REJECT_REASON_UNSPECIFIED
}

func (stream *pullStream) rejectTask(taskID string, reason dexpb.PollWorkerTaskRejectReason) {
	if err := stream.send(&dexpb.PollWorkerTaskRequest{Frame: &dexpb.PollWorkerTaskRequest_Reject{Reject: &dexpb.PollWorkerTaskReject{TaskId: taskID, Reason: reason}}}); err != nil {
		stream.worker.service.logger.Error("reject Worker task", "error", err)
	}
}

func (stream *pullStream) executeTask(ctx context.Context, task *dexpb.PollWorkerTask) {
	defer stream.finishTask()
	var err error
	defer func() {
		if finalErr := finishWorkerCall(stream.worker.service.logger, recover(), err); finalErr != nil {
			workerError := &dexpb.WorkerErrorResponse{Detail: finalErr.Error(), GrpcStatus: int32(status.Code(finalErr))}
			if rpcStatus, ok := status.FromError(finalErr); ok {
				workerError.Detail = rpcStatus.Message()
				for _, detail := range rpcStatus.Details() {
					if typed, ok := detail.(*dexpb.WorkerErrorResponse); ok {
						workerError = typed
						workerError.GrpcStatus = int32(rpcStatus.Code())
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
	stream.mu.Lock()
	taskFinished := stream.taskFinished
	stream.mu.Unlock()
	taskFinished <- struct{}{}
}

func (stream *pullStream) completeTask() {
	stream.worker.mu.Lock()
	stream.mu.Lock()
	stream.taskCancel()
	stream.taskCancel = nil
	stream.taskID = ""
	stream.isBusy = false
	stream.taskFinished = nil
	stream.worker.busyTasks--
	isDraining := stream.isDraining
	stream.mu.Unlock()
	stream.worker.mu.Unlock()
	if isDraining {
		stream.closeSend()
	}
	stream.worker.signal()
}

func (stream *pullStream) cancelTaskContext() {
	stream.mu.Lock()
	if stream.taskCancel != nil {
		stream.taskCancel()
	}
	stream.mu.Unlock()
}

func (stream *pullStream) waitForTaskFinished() {
	stream.mu.Lock()
	isBusy := stream.isBusy
	taskFinished := stream.taskFinished
	stream.mu.Unlock()
	if isBusy {
		<-taskFinished
		stream.completeTask()
	}
}

func (stream *pullStream) markDraining() bool {
	stream.mu.Lock()
	stream.isDraining = true
	isBusy := stream.isBusy
	stream.mu.Unlock()
	return isBusy
}

func (stream *pullStream) isReconnecting() bool {
	stream.mu.Lock()
	defer stream.mu.Unlock()
	if !stream.isDraining {
		return false
	}
	stream.isDraining = false
	return true
}

func (stream *pullStream) remove() {
	stream.cancelTaskContext()
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
	if stream.isSendClosed {
		return io.ErrClosedPipe
	}
	return stream.stream.Send(request)
}

func (stream *pullStream) closeSend() {
	stream.sendMu.Lock()
	defer stream.sendMu.Unlock()
	if stream.isSendClosed {
		return
	}
	stream.isSendClosed = true
	if err := stream.stream.CloseSend(); err != nil {
		stream.worker.service.logger.Debug("close Worker poll send", "error", err)
	}
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
