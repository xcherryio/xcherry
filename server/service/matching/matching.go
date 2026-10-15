// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

// Package matching assigns pull-mode tasks to registered Worker streams.
package matching

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/superdurable/dex/config"
	"github.com/superdurable/dex/gen/dexpb"
	"github.com/superdurable/dex/service/membership"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

const forwardedHeader = "dex-matching-forwarded"
const affinityDuration = 30 * time.Second

// Membership locates queue owners and reports changes to the matching ring.
type Membership interface {
	Lookup(membership.Role, string) (membership.HostInfo, error)
	Subscribe() (<-chan membership.Change, func())
}

// ClientFactory obtains a MatchingService client for an advertised internal RPC address.
type ClientFactory func(context.Context, string) (dexpb.MatchingServiceClient, error)

// Engine owns registered streams and dispatches tasks for its matching queues.
type Engine struct {
	dexpb.UnimplementedMatchingServiceServer
	cfg          *config.MatchingConfig
	logger       *zap.Logger
	membership   Membership
	clients      ClientFactory
	queues       sync.Map
	limitsMu     sync.Mutex
	streamCount  int
	workerCounts map[string]int
	changes      <-chan membership.Change
	unsubscribe  func()
	stop         chan struct{}
	stopped      chan struct{}
}

type queueState struct {
	mu       sync.Mutex
	idle     []*workerStream
	streams  map[*workerStream]struct{}
	affinity map[string]affinityEntry
	changed  chan struct{}
}

type affinityEntry struct {
	workerID string
	expires  time.Time
}

type workerStream struct {
	queue            *queueState
	registration     *dexpb.PollWorkerTaskRegister
	transport        pollTransport
	sendMu           sync.Mutex
	mu               sync.Mutex
	current          chan *dexpb.PollWorkerTaskRequest
	currentTaskID    string
	isBusy           bool
	isClosing        bool
	hasReconnectSent bool
	done             chan struct{}
	closeOnce        sync.Once
}

type pollTransport interface {
	Send(*dexpb.PollWorkerTaskResponse) error
	Recv() (*dexpb.PollWorkerTaskRequest, error)
	Context() context.Context
}

// NewEngine creates a matching owner with injected membership and internal clients.
func NewEngine(cfg *config.MatchingConfig, logger *zap.Logger, members Membership, clients ClientFactory) (*Engine, error) {
	if cfg == nil || logger == nil || members == nil || clients == nil {
		panic("matching dependencies are required")
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	effective := cfg.WithDefaults()
	changes, unsubscribe := members.Subscribe()
	engine := &Engine{cfg: &effective, logger: logger, membership: members, clients: clients, workerCounts: make(map[string]int), changes: changes, unsubscribe: unsubscribe, stop: make(chan struct{}), stopped: make(chan struct{})}
	go engine.watchMembership()
	return engine, nil
}

// Close stops membership observation and drains owned poll streams.
func (e *Engine) Close() {
	select {
	case <-e.stop:
		return
	default:
		close(e.stop)
	}
	<-e.stopped
	e.queues.Range(func(_, value any) bool {
		queue := value.(*queueState)
		queue.mu.Lock()
		streams := make([]*workerStream, 0, len(queue.streams))
		for stream := range queue.streams {
			streams = append(streams, stream)
		}
		queue.mu.Unlock()
		for _, stream := range streams {
			stream.requestReconnect("matching server is closing")
		}
		return true
	})
}

func (e *Engine) watchMembership() {
	defer close(e.stopped)
	defer e.unsubscribe()
	for {
		select {
		case <-e.stop:
			return
		case change, isOpen := <-e.changes:
			if !isOpen {
				return
			}
			if change.Role != membership.RoleMatching {
				continue
			}
			e.queues.Range(func(key, value any) bool {
				owner, err := e.membership.Lookup(membership.RoleMatching, key.(string))
				if err != nil {
					e.logger.Warn("matching owner lookup after membership change", zap.Error(err))
					return true
				}
				if owner.IsSelf {
					return true
				}
				queue := value.(*queueState)
				queue.mu.Lock()
				streams := make([]*workerStream, 0, len(queue.streams))
				for stream := range queue.streams {
					streams = append(streams, stream)
				}
				queue.mu.Unlock()
				for _, stream := range streams {
					stream.requestReconnect("queue ownership changed")
				}
				return true
			})
		}
	}
}

func (e *Engine) getQueue(name string) *queueState {
	value, isExistingQueue := e.queues.LoadOrStore(name, &queueState{streams: make(map[*workerStream]struct{}), affinity: make(map[string]affinityEntry), changed: make(chan struct{})})
	if !isExistingQueue {
		e.logger.Debug("matching queue created", zap.String("queue", name))
	}
	return value.(*queueState)
}

func (q *queueState) signal() { close(q.changed); q.changed = make(chan struct{}) }

func (e *Engine) reserveStream(workerID string) error {
	e.limitsMu.Lock()
	defer e.limitsMu.Unlock()
	if e.streamCount >= e.cfg.MaxStreams {
		return status.Error(codes.ResourceExhausted, "matching stream limit reached")
	}
	if e.workerCounts[workerID] >= e.cfg.MaxConcurrentTasksCap {
		return status.Error(codes.ResourceExhausted, "Worker stream limit reached")
	}
	e.streamCount++
	e.workerCounts[workerID]++
	return nil
}

func (e *Engine) releaseStream(workerID string) {
	e.limitsMu.Lock()
	e.streamCount--
	e.workerCounts[workerID]--
	if e.workerCounts[workerID] == 0 {
		delete(e.workerCounts, workerID)
	}
	e.limitsMu.Unlock()
}

// ProxyPollWorkerTask accepts a public Worker stream and routes it to its queue owner.
func (e *Engine) ProxyPollWorkerTask(stream dexpb.WorkerTaskService_PollWorkerTaskServer) error {
	return e.servePoll(stream)
}

// ForwardPollWorkerTask accepts a stream forwarded by another Dex server.
func (e *Engine) ForwardPollWorkerTask(stream dexpb.MatchingService_ForwardPollWorkerTaskServer) error {
	return e.servePoll(stream)
}

func (e *Engine) servePoll(transport pollTransport) error {
	first, err := transport.Recv()
	if err != nil {
		return err
	}
	registration := first.GetRegister()
	if err := validateRegistration(registration); err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	owner, err := e.membership.Lookup(membership.RoleMatching, registration.Queue)
	if err != nil {
		return status.Error(codes.Unavailable, err.Error())
	}
	if !owner.IsSelf {
		if isForwarded(transport.Context()) {
			return status.Error(codes.FailedPrecondition, "matching forwarding loop")
		}
		return e.proxyPoll(transport, first, owner.RPCAddress)
	}
	if err := e.reserveStream(registration.AppWorkerId); err != nil {
		return err
	}
	defer e.releaseStream(registration.AppWorkerId)
	queue := e.getQueue(registration.Queue)
	worker := &workerStream{queue: queue, registration: registration, transport: transport, done: make(chan struct{})}
	queue.mu.Lock()
	queue.streams[worker] = struct{}{}
	queue.mu.Unlock()
	defer worker.disconnect()
	if err := worker.send(&dexpb.PollWorkerTaskResponse{Frame: &dexpb.PollWorkerTaskResponse_Registered{Registered: &dexpb.PollWorkerTaskRegistered{}}}); err != nil {
		return err
	}
	worker.makeIdle()
	for {
		frame, err := transport.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if frame.GetRegister() != nil {
			return status.Error(codes.InvalidArgument, "duplicate Worker registration")
		}
		if frame.GetPong() != nil {
			continue
		}
		worker.mu.Lock()
		current, taskID := worker.current, worker.currentTaskID
		worker.mu.Unlock()
		if current == nil {
			return status.Error(codes.InvalidArgument, "Worker frame without assigned task")
		}
		if taskFrame := frame.GetTaskFrame(); taskFrame != nil {
			if taskFrame.TaskId != taskID {
				return status.Error(codes.InvalidArgument, "task ID does not match assignment")
			}
			if taskFrame.Output == nil {
				return status.Error(codes.InvalidArgument, "task output is required")
			}
			current <- frame
			if isFinal(taskFrame) {
				worker.finishTask()
			}
		} else if rejection := frame.GetReject(); rejection != nil {
			if rejection.TaskId != taskID || rejection.Reason != dexpb.PollWorkerTaskRejectReason_POLL_WORKER_TASK_REJECT_REASON_NO_CAPACITY {
				return status.Error(codes.InvalidArgument, "invalid task rejection")
			}
			current <- frame
			worker.finishTask()
		} else {
			return status.Error(codes.InvalidArgument, "unknown Worker frame")
		}
	}
}

func (e *Engine) proxyPoll(source pollTransport, first *dexpb.PollWorkerTaskRequest, address string) error {
	if address == "" {
		return status.Error(codes.Unavailable, "queue owner has no RPC address")
	}
	ctx, cancel := context.WithCancel(metadata.AppendToOutgoingContext(source.Context(), forwardedHeader, "1"))
	defer cancel()
	client, err := e.clients(ctx, address)
	if err != nil {
		return err
	}
	target, err := client.ForwardPollWorkerTask(ctx)
	if err != nil {
		return err
	}
	if err := target.Send(first); err != nil {
		return err
	}
	upstream := make(chan error, 1)
	downstream := make(chan error, 1)
	go func() {
		for {
			frame, recvErr := source.Recv()
			if recvErr != nil {
				if errors.Is(recvErr, io.EOF) {
					upstream <- target.CloseSend()
				} else {
					upstream <- recvErr
				}
				return
			}
			if sendErr := target.Send(frame); sendErr != nil {
				upstream <- sendErr
				return
			}
		}
	}()
	go func() {
		for {
			frame, recvErr := target.Recv()
			if recvErr != nil {
				downstream <- recvErr
				return
			}
			if sendErr := source.Send(frame); sendErr != nil {
				downstream <- sendErr
				return
			}
		}
	}()
	select {
	case err = <-upstream:
		if err != nil {
			return err
		}
		err = <-downstream
	case err = <-downstream:
	}
	if errors.Is(err, io.EOF) {
		return nil
	}
	return err
}

func (w *workerStream) send(frame *dexpb.PollWorkerTaskResponse) error {
	w.sendMu.Lock()
	defer w.sendMu.Unlock()
	return w.transport.Send(frame)
}

func (w *workerStream) makeIdle() {
	w.mu.Lock()
	if w.isClosing {
		w.mu.Unlock()
		return
	}
	w.isBusy = false
	w.current = nil
	w.currentTaskID = ""
	w.mu.Unlock()
	w.queue.mu.Lock()
	w.queue.idle = append(w.queue.idle, w)
	w.queue.signal()
	w.queue.mu.Unlock()
}

func (w *workerStream) finishTask() {
	w.mu.Lock()
	isClosing := w.isClosing
	w.isBusy = false
	w.current = nil
	w.currentTaskID = ""
	w.mu.Unlock()
	if isClosing {
		w.requestReconnect("queue ownership changed")
		return
	}
	w.makeIdle()
}

func (w *workerStream) requestReconnect(reason string) {
	w.mu.Lock()
	w.isClosing = true
	isBusy := w.isBusy
	shouldSendReconnect := !isBusy && !w.hasReconnectSent
	if shouldSendReconnect {
		w.hasReconnectSent = true
	}
	w.mu.Unlock()
	if shouldSendReconnect {
		if err := w.send(&dexpb.PollWorkerTaskResponse{Frame: &dexpb.PollWorkerTaskResponse_Reconnect{Reconnect: &dexpb.PollWorkerTaskReconnect{Reason: reason}}}); err != nil {
			w.disconnect()
		}
	}
}

func (w *workerStream) disconnect() {
	w.closeOnce.Do(func() {
		w.mu.Lock()
		w.isClosing = true
		w.mu.Unlock()
		close(w.done)
		w.queue.mu.Lock()
		delete(w.queue.streams, w)
		for index, idle := range w.queue.idle {
			if idle == w {
				w.queue.idle = append(w.queue.idle[:index], w.queue.idle[index+1:]...)
				break
			}
		}
		w.queue.signal()
		w.queue.mu.Unlock()
	})
}

func (q *queueState) take(flowID string, excluded map[*workerStream]struct{}) (*workerStream, <-chan struct{}) {
	q.mu.Lock()
	defer q.mu.Unlock()
	preferred := q.affinity[flowID]
	selected := -1
	for index, worker := range q.idle {
		if _, wasRejected := excluded[worker]; wasRejected {
			continue
		}
		if selected < 0 {
			selected = index
		}
		if flowID != "" && time.Now().Before(preferred.expires) && worker.registration.AppWorkerId == preferred.workerID {
			selected = index
			break
		}
	}
	if selected < 0 {
		return nil, q.changed
	}
	worker := q.idle[selected]
	q.idle = append(q.idle[:selected], q.idle[selected+1:]...)
	return worker, q.changed
}

func (q *queueState) remember(flowID, workerID string) {
	if flowID == "" {
		return
	}
	q.mu.Lock()
	q.affinity[flowID] = affinityEntry{workerID: workerID, expires: time.Now().Add(affinityDuration)}
	if len(q.affinity) > 10000 {
		for key, entry := range q.affinity {
			if time.Now().After(entry.expires) {
				delete(q.affinity, key)
			}
		}
	}
	q.mu.Unlock()
}

// DispatchTask routes a task to an idle Worker and relays its output frames.
func (e *Engine) DispatchTask(request *dexpb.DispatchTaskRequest, stream dexpb.MatchingService_DispatchTaskServer) error {
	if err := validateDispatch(request); err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	ctx := stream.Context()
	owner, err := e.membership.Lookup(membership.RoleMatching, request.Queue)
	if err != nil {
		return status.Error(codes.Unavailable, err.Error())
	}
	if !owner.IsSelf {
		if isForwarded(ctx) {
			return status.Error(codes.FailedPrecondition, "matching forwarding loop")
		}
		return e.forwardDispatch(ctx, request, owner.RPCAddress, stream.Send)
	}
	dispatchEnd := time.Now().Add(request.DispatchWait.AsDuration())
	if deadline := request.Deadline.AsTime(); deadline.Before(dispatchEnd) {
		dispatchEnd = deadline
	}
	if deadline, exists := ctx.Deadline(); exists && deadline.Before(dispatchEnd) {
		dispatchEnd = deadline
	}
	stickyEnd := dispatchEnd
	if request.FallbackQueue != "" && request.FallbackQueue != request.Queue {
		stickyEnd = time.Now().Add(request.StickyWait.AsDuration())
		if stickyEnd.After(dispatchEnd) {
			stickyEnd = dispatchEnd
		}
	}
	matched, err := e.dispatchQueue(ctx, request, request.Queue, stickyEnd, stream.Send)
	if err != nil {
		return err
	}
	if matched {
		return nil
	}
	if request.FallbackQueue != "" && request.FallbackQueue != request.Queue && time.Now().Before(dispatchEnd) {
		matched, err = e.dispatchQueue(ctx, request, request.FallbackQueue, dispatchEnd, stream.Send)
		if err != nil {
			return err
		}
		if matched {
			return nil
		}
	}
	return stream.Send(dispatchError(dexpb.DispatchTaskErrorReason_DISPATCH_TASK_ERROR_REASON_NO_WORKER_AVAILABLE, "no Worker accepted the task"))
}

func (e *Engine) forwardDispatch(ctx context.Context, request *dexpb.DispatchTaskRequest, address string, send func(*dexpb.DispatchTaskResponse) error) error {
	if address == "" {
		return status.Error(codes.Unavailable, "queue owner has no RPC address")
	}
	client, err := e.clients(ctx, address)
	if err != nil {
		return err
	}
	remote, err := client.DispatchTask(metadata.AppendToOutgoingContext(ctx, forwardedHeader, "1"), request)
	if err != nil {
		return err
	}
	for {
		frame, recvErr := remote.Recv()
		if errors.Is(recvErr, io.EOF) {
			return nil
		}
		if recvErr != nil {
			return recvErr
		}
		if err := send(frame); err != nil {
			return err
		}
	}
}

func (e *Engine) dispatchQueue(ctx context.Context, request *dexpb.DispatchTaskRequest, queueName string, waitEnd time.Time, send func(*dexpb.DispatchTaskResponse) error) (bool, error) {
	owner, err := e.membership.Lookup(membership.RoleMatching, queueName)
	if err != nil {
		return false, status.Error(codes.Unavailable, err.Error())
	}
	if !owner.IsSelf {
		remoteRequest := *request
		remoteRequest.Queue = queueName
		remoteRequest.FallbackQueue = ""
		remoteRequest.StickyWait = nil
		remoteRequest.DispatchWait = durationUntil(waitEnd)
		isMatched := false
		err := e.forwardDispatch(ctx, &remoteRequest, owner.RPCAddress, func(frame *dexpb.DispatchTaskResponse) error {
			if dispatchErr := frame.GetDispatchError(); dispatchErr != nil && dispatchErr.Reason == dexpb.DispatchTaskErrorReason_DISPATCH_TASK_ERROR_REASON_NO_WORKER_AVAILABLE {
				return nil
			}
			isMatched = true
			return send(frame)
		})
		return isMatched, err
	}
	queue := e.getQueue(queueName)
	excluded := make(map[*workerStream]struct{})
	for {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		if !time.Now().Before(waitEnd) {
			return false, nil
		}
		worker, changed := queue.take(request.FlowId, excluded)
		if worker == nil {
			timer := time.NewTimer(time.Until(waitEnd))
			select {
			case <-changed:
			case <-timer.C:
			case <-ctx.Done():
				if !timer.Stop() {
					<-timer.C
				}
				return false, ctx.Err()
			}
			timer.Stop()
			continue
		}
		frames := make(chan *dexpb.PollWorkerTaskRequest, 32)
		worker.mu.Lock()
		if worker.isClosing {
			worker.mu.Unlock()
			continue
		}
		worker.isBusy = true
		worker.current = frames
		worker.currentTaskID = request.TaskId
		worker.mu.Unlock()
		task := &dexpb.PollWorkerTask{TaskId: request.TaskId, Deadline: request.Deadline}
		switch value := request.Request.(type) {
		case *dexpb.DispatchTaskRequest_WaitFor:
			task.Request = &dexpb.PollWorkerTask_WaitFor{WaitFor: value.WaitFor}
		case *dexpb.DispatchTaskRequest_Execute:
			task.Request = &dexpb.PollWorkerTask_Execute{Execute: value.Execute}
		case *dexpb.DispatchTaskRequest_Rpc:
			task.Request = &dexpb.PollWorkerTask_Rpc{Rpc: value.Rpc}
		}
		if err := worker.send(&dexpb.PollWorkerTaskResponse{Frame: &dexpb.PollWorkerTaskResponse_Task{Task: task}}); err != nil {
			worker.disconnect()
			return true, send(dispatchError(dexpb.DispatchTaskErrorReason_DISPATCH_TASK_ERROR_REASON_WORKER_DISCONNECTED, err.Error()))
		}
		deadlineTimer := time.NewTimer(time.Until(request.Deadline.AsTime()))
		defer deadlineTimer.Stop()
		for {
			select {
			case frame := <-frames:
				if frame.GetReject() != nil {
					excluded[worker] = struct{}{}
					goto nextWorker
				}
				output := frame.GetTaskFrame()
				response := relayFrame(output, worker.registration.AppWorkerId)
				if err := send(response); err != nil {
					worker.cancelTask(request.TaskId)
					return true, err
				}
				if isFinal(output) {
					queue.remember(request.FlowId, worker.registration.AppWorkerId)
					return true, nil
				}
			case <-worker.done:
				return true, send(dispatchError(dexpb.DispatchTaskErrorReason_DISPATCH_TASK_ERROR_REASON_WORKER_DISCONNECTED, "Worker stream disconnected"))
			case <-ctx.Done():
				worker.cancelTask(request.TaskId)
				return true, ctx.Err()
			case <-deadlineTimer.C:
				worker.cancelTask(request.TaskId)
				return true, status.Error(codes.DeadlineExceeded, "task deadline elapsed")
			}
		}
	nextWorker:
	}
}

func (w *workerStream) cancelTask(taskID string) {
	if err := w.send(&dexpb.PollWorkerTaskResponse{Frame: &dexpb.PollWorkerTaskResponse_CancelTask{CancelTask: &dexpb.PollWorkerTaskCancelTask{TaskId: taskID}}}); err != nil {
		w.disconnect()
	}
}

func durationUntil(deadline time.Time) *durationpb.Duration {
	return durationpb.New(time.Until(deadline))
}

func dispatchError(reason dexpb.DispatchTaskErrorReason, detail string) *dexpb.DispatchTaskResponse {
	return &dexpb.DispatchTaskResponse{Output: &dexpb.DispatchTaskResponse_DispatchError{DispatchError: &dexpb.DispatchTaskError{Reason: reason, Detail: detail}}}
}

func relayFrame(frame *dexpb.PollWorkerTaskFrame, workerID string) *dexpb.DispatchTaskResponse {
	response := &dexpb.DispatchTaskResponse{}
	switch output := frame.Output.(type) {
	case *dexpb.PollWorkerTaskFrame_WaitFor:
		response.Output = &dexpb.DispatchTaskResponse_WaitFor{WaitFor: output.WaitFor}
	case *dexpb.PollWorkerTaskFrame_Execute:
		response.Output = &dexpb.DispatchTaskResponse_Execute{Execute: output.Execute}
	case *dexpb.PollWorkerTaskFrame_Rpc:
		response.Output = &dexpb.DispatchTaskResponse_Rpc{Rpc: output.Rpc}
	case *dexpb.PollWorkerTaskFrame_WorkerError:
		response.Output = &dexpb.DispatchTaskResponse_WorkerError{WorkerError: output.WorkerError}
	}
	if isFinal(frame) {
		response.ServedByAppWorkerId = workerID
	}
	return response
}

func isFinal(frame *dexpb.PollWorkerTaskFrame) bool {
	if frame.GetWorkerError() != nil || frame.GetRpc() != nil {
		return true
	}
	if output := frame.GetWaitFor(); output != nil {
		return output.GetResult() != nil
	}
	if output := frame.GetExecute(); output != nil {
		return output.GetResult() != nil
	}
	return false
}

func isForwarded(ctx context.Context) bool {
	incoming, exists := metadata.FromIncomingContext(ctx)
	return exists && len(incoming.Get(forwardedHeader)) > 0
}

func validateRegistration(registration *dexpb.PollWorkerTaskRegister) error {
	if registration == nil {
		return errors.New("first frame must register a Worker")
	}
	workerID := registration.AppWorkerId
	if len(workerID) < 1 || len(workerID) > 128 {
		return errors.New("app_worker_id must have 1 to 128 characters")
	}
	for _, character := range workerID {
		if !((character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') || (character >= '0' && character <= '9') || character == '-' || character == '_' || character == '.') {
			return errors.New("app_worker_id contains an invalid character")
		}
	}
	if registration.Queue == "" {
		return errors.New("queue is required")
	}
	if registration.Queue == workerID {
		return nil
	}
	for _, flowType := range registration.FlowTypes {
		if registration.Queue == flowType {
			return nil
		}
	}
	return errors.New("queue must be app_worker_id or a declared flow_type")
}

func validateDispatch(request *dexpb.DispatchTaskRequest) error {
	if request == nil || request.Queue == "" || request.Request == nil {
		return errors.New("queue and task request are required")
	}
	switch task := request.Request.(type) {
	case *dexpb.DispatchTaskRequest_WaitFor:
		if task.WaitFor == nil {
			return errors.New("wait_for request is required")
		}
	case *dexpb.DispatchTaskRequest_Execute:
		if task.Execute == nil {
			return errors.New("execute request is required")
		}
	case *dexpb.DispatchTaskRequest_Rpc:
		if task.Rpc == nil {
			return errors.New("rpc request is required")
		}
	default:
		return errors.New("unknown task request")
	}
	if request.Deadline == nil || request.Deadline.AsTime().Before(time.Now()) {
		return errors.New("future task deadline is required")
	}
	if request.DispatchWait == nil || request.DispatchWait.AsDuration() <= 0 {
		return errors.New("positive dispatch_wait is required")
	}
	if request.FallbackQueue != "" && request.FallbackQueue != request.Queue && (request.StickyWait == nil || request.StickyWait.AsDuration() < 0) {
		return errors.New("non-negative sticky_wait is required with fallback_queue")
	}
	if request.TaskId == "" {
		request.TaskId = uuid.NewString()
	}
	return nil
}

var _ dexpb.MatchingServiceServer = (*Engine)(nil)
