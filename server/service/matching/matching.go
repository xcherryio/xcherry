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
	"container/list"
	"context"
	"errors"
	"fmt"
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
	"google.golang.org/protobuf/types/known/emptypb"
)

const forwardedHeader = "dex-matching-forwarded"
const affinityDuration = 30 * time.Second

// Backoff bounds retries against full Workers while detecting freed capacity during dispatch waits.
const capacityRejectInitialBackoff = 300 * time.Millisecond
const capacityRejectMaxBackoff = 2 * time.Second

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
	lifecycleMu  sync.Mutex
	isClosing    bool
	activePoll   sync.WaitGroup
}

type forwardDispatchRelay struct {
	request            *dexpb.DispatchTaskRequest
	send               func(*dexpb.DispatchTaskResponse) error
	dispatchEnd        time.Time
	hasForwardedOutput bool
	isMatched          bool
	isAssigned         bool
	isSticky           bool
	excludedWorkerIDs  map[string]struct{}
}

type queueState struct {
	mu            sync.Mutex
	idle          []*workerStream
	streams       map[*workerStream]struct{}
	affinity      map[string]*list.Element
	affinityOrder *list.List
	changed       chan struct{}
	users         int
	isDeleted     bool
}

type affinityEntry struct {
	flowID   string
	workerID string
	expires  time.Time
}

type workerStream struct {
	queue            *queueState
	registration     *dexpb.PollWorkerTaskRegister
	transport        pollTransport
	sendMu           sync.Mutex
	mu               sync.Mutex
	current          *taskAssignment
	isBusy           bool
	isClosing        bool
	hasReconnectSent bool
	reconnectSent    chan struct{}
	done             chan struct{}
	closeOnce        sync.Once
}

type taskAssignment struct {
	frames    chan *dexpb.PollWorkerTaskRequest
	abandoned chan struct{}
	taskID    string
}

type dispatchAttemptOutcome uint8

const (
	dispatchAttemptMatched dispatchAttemptOutcome = iota
	dispatchAttemptRetryCapacity
	dispatchAttemptRetryDisconnected
	dispatchAttemptRetryUnknownName
	dispatchAttemptTerminal
)

// maxExcludedAppWorkerIDs bounds untrusted dispatch exclusions.
const maxExcludedAppWorkerIDs = 256

const maxAppWorkerIDLength = 128

type pollResult struct {
	frame *dexpb.PollWorkerTaskRequest
	err   error
}

type pollProxy struct {
	source        pollTransport
	target        dexpb.MatchingService_ForwardPollWorkerTaskClient
	stop          <-chan struct{}
	upstream      chan error
	downstream    chan error
	mu            sync.Mutex
	sendMu        sync.Mutex
	isBusy        bool
	isDraining    bool
	idle          chan struct{}
	reconnectOnce sync.Once
	reconnectErr  error
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
	e.lifecycleMu.Lock()
	if e.isClosing {
		e.lifecycleMu.Unlock()
		return
	}
	e.isClosing = true
	close(e.stop)
	e.lifecycleMu.Unlock()
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
	e.activePoll.Wait()
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
	for {
		value, isExistingQueue := e.queues.LoadOrStore(name, &queueState{streams: make(map[*workerStream]struct{}), affinity: make(map[string]*list.Element), affinityOrder: list.New(), changed: make(chan struct{})})
		queue := value.(*queueState)
		queue.mu.Lock()
		if queue.isDeleted {
			queue.mu.Unlock()
			continue
		}
		queue.users++
		queue.mu.Unlock()
		if !isExistingQueue {
			e.logger.Debug("matching queue created", zap.String("queue", name))
		}
		return queue
	}
}

func (e *Engine) releaseQueue(name string, queue *queueState) {
	queue.mu.Lock()
	queue.users--
	if queue.users == 0 {
		queue.isDeleted = true
		e.queues.CompareAndDelete(name, queue)
	}
	queue.mu.Unlock()
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
	if err := validateRegistration(registration, e.cfg); err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	e.lifecycleMu.Lock()
	if e.isClosing {
		e.lifecycleMu.Unlock()
		return status.Error(codes.Unavailable, "matching server is closing")
	}
	e.activePoll.Add(1)
	e.lifecycleMu.Unlock()
	defer e.activePoll.Done()
	select {
	case <-e.stop:
		return status.Error(codes.Unavailable, "matching server is closing")
	default:
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
	defer e.releaseQueue(registration.Queue, queue)
	worker := &workerStream{queue: queue, registration: registration, transport: transport, done: make(chan struct{})}
	queue.mu.Lock()
	queue.streams[worker] = struct{}{}
	queue.mu.Unlock()
	defer worker.disconnect()
	select {
	case <-e.stop:
		<-worker.requestReconnect("matching server is closing")
		return nil
	default:
	}
	owner, err = e.membership.Lookup(membership.RoleMatching, registration.Queue)
	if err != nil {
		return status.Error(codes.Unavailable, err.Error())
	}
	if !owner.IsSelf {
		<-worker.requestReconnect("queue ownership changed")
		return nil
	}
	if err := worker.send(&dexpb.PollWorkerTaskResponse{Frame: &dexpb.PollWorkerTaskResponse_Registered{Registered: &dexpb.PollWorkerTaskRegistered{SupportsUnknownNameRejection: true}}}); err != nil {
		return err
	}
	worker.makeIdle()
	received := make(chan pollResult, 1)
	go func() {
		for {
			frame, recvErr := transport.Recv()
			select {
			case received <- pollResult{frame: frame, err: recvErr}:
			case <-worker.done:
				return
			}
			if recvErr != nil {
				return
			}
		}
	}()
	pingTicker := time.NewTicker(e.cfg.PingInterval)
	defer pingTicker.Stop()
	for {
		var result pollResult
		select {
		case result = <-received:
		case <-pingTicker.C:
			worker.mu.Lock()
			isIdle := !worker.isBusy && !worker.isClosing
			worker.mu.Unlock()
			if isIdle {
				if err := worker.send(&dexpb.PollWorkerTaskResponse{Frame: &dexpb.PollWorkerTaskResponse_Ping{Ping: &dexpb.PollWorkerTaskPing{}}}); err != nil {
					return err
				}
			}
			continue
		case <-worker.done:
			return nil
		}
		if result.err != nil {
			if errors.Is(result.err, io.EOF) {
				return nil
			}
			return result.err
		}
		frame := result.frame
		if frame.GetRegister() != nil {
			return status.Error(codes.InvalidArgument, "duplicate Worker registration")
		}
		if frame.GetPong() != nil {
			continue
		}
		worker.mu.Lock()
		current := worker.current
		worker.mu.Unlock()
		if current == nil {
			e.logger.Debug("dropping Worker frame without assignment")
			continue
		}
		if taskFrame := frame.GetTaskFrame(); taskFrame != nil {
			if taskFrame.TaskId != current.taskID {
				e.logger.Debug("dropping Worker frame for another task", zap.String("task_id", taskFrame.TaskId))
				continue
			}
			if taskFrame.Output == nil {
				return status.Error(codes.InvalidArgument, "task output is required")
			}
			select {
			case current.frames <- frame:
			case <-current.abandoned:
			}
			if isFinal(taskFrame) {
				worker.finishTask()
			}
		} else if rejection := frame.GetReject(); rejection != nil {
			if rejection.TaskId != current.taskID {
				e.logger.Debug("dropping Worker rejection for another task", zap.String("task_id", rejection.TaskId))
				continue
			}
			if rejection.Reason != dexpb.PollWorkerTaskRejectReason_POLL_WORKER_TASK_REJECT_REASON_NO_CAPACITY &&
				rejection.Reason != dexpb.PollWorkerTaskRejectReason_POLL_WORKER_TASK_REJECT_REASON_UNKNOWN_STEP &&
				rejection.Reason != dexpb.PollWorkerTaskRejectReason_POLL_WORKER_TASK_REJECT_REASON_UNKNOWN_RPC {
				return status.Error(codes.InvalidArgument, "invalid task rejection")
			}
			select {
			case current.frames <- frame:
			case <-current.abandoned:
			}
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
	proxy := &pollProxy{source: source, target: target, stop: e.stop, upstream: make(chan error, 1), downstream: make(chan error, 1)}
	return proxy.run()
}

func (p *pollProxy) run() error {
	go p.relayWorkerFrames()
	go p.relayOwnerFrames()
	var err error
	select {
	case <-p.stop:
		idle := p.beginDrain()
		select {
		case <-idle:
			return p.finishDrain()
		case err = <-p.upstream:
		case err = <-p.downstream:
		case <-p.source.Context().Done():
			return p.source.Context().Err()
		}
	case err = <-p.upstream:
		if err != nil {
			return err
		}
		err = <-p.downstream
	case err = <-p.downstream:
	}
	if errors.Is(err, io.EOF) {
		return nil
	}
	return err
}

func (p *pollProxy) finishDrain() error {
	if err := p.target.CloseSend(); err != nil {
		return err
	}
	select {
	case err := <-p.downstream:
		if err != nil && !errors.Is(err, io.EOF) {
			return err
		}
	case <-p.source.Context().Done():
		return p.source.Context().Err()
	}
	return p.sendReconnect()
}

func (p *pollProxy) relayWorkerFrames() {
	for {
		frame, recvErr := p.source.Recv()
		if recvErr != nil {
			if errors.Is(recvErr, io.EOF) {
				p.upstream <- p.target.CloseSend()
			} else {
				p.upstream <- recvErr
			}
			return
		}
		if err := p.target.Send(frame); err != nil {
			p.upstream <- err
			return
		}
		if taskFrame := frame.GetTaskFrame(); taskFrame != nil && isFinal(taskFrame) {
			p.finishTask()
		} else if frame.GetReject() != nil {
			p.finishTask()
		}
	}
}

func (p *pollProxy) relayOwnerFrames() {
	for {
		frame, recvErr := p.target.Recv()
		if recvErr != nil {
			p.downstream <- recvErr
			return
		}
		if frame.GetTask() != nil && !p.beginTask() {
			p.downstream <- p.sendReconnect()
			return
		}
		if err := p.sendToWorker(frame); err != nil {
			p.downstream <- err
			return
		}
	}
}

func (p *pollProxy) beginTask() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.isDraining {
		return false
	}
	p.isBusy = true
	p.idle = make(chan struct{})
	return true
}

func (p *pollProxy) finishTask() {
	p.mu.Lock()
	if p.isBusy {
		p.isBusy = false
		close(p.idle)
	}
	p.mu.Unlock()
}

func (p *pollProxy) beginDrain() <-chan struct{} {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.isDraining = true
	if p.isBusy {
		return p.idle
	}
	idle := make(chan struct{})
	close(idle)
	return idle
}

func (p *pollProxy) sendToWorker(frame *dexpb.PollWorkerTaskResponse) error {
	p.sendMu.Lock()
	defer p.sendMu.Unlock()
	return p.source.Send(frame)
}

func (p *pollProxy) sendReconnect() error {
	p.reconnectOnce.Do(func() {
		p.reconnectErr = p.sendToWorker(&dexpb.PollWorkerTaskResponse{Frame: &dexpb.PollWorkerTaskResponse_Reconnect{Reconnect: &dexpb.PollWorkerTaskReconnect{Reason: "matching server is closing"}}})
	})
	return p.reconnectErr
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
	w.mu.Unlock()
	if isClosing {
		w.requestReconnect("queue ownership changed")
		return
	}
	w.makeIdle()
}

func (w *workerStream) requestReconnect(reason string) <-chan struct{} {
	w.mu.Lock()
	w.isClosing = true
	isBusy := w.isBusy
	shouldSendReconnect := !isBusy && !w.hasReconnectSent
	if shouldSendReconnect {
		w.hasReconnectSent = true
		w.reconnectSent = make(chan struct{})
	}
	reconnectSent := w.reconnectSent
	w.mu.Unlock()
	if shouldSendReconnect {
		go func() {
			defer close(reconnectSent)
			if err := w.send(&dexpb.PollWorkerTaskResponse{Frame: &dexpb.PollWorkerTaskResponse_Reconnect{Reconnect: &dexpb.PollWorkerTaskReconnect{Reason: reason}}}); err != nil {
				w.disconnect()
				return
			}
			w.disconnect()
		}()
	}
	return reconnectSent
}

func (w *workerStream) abandon(assignment *taskAssignment) {
	close(assignment.abandoned)
	w.mu.Lock()
	isCurrent := w.current == assignment
	w.mu.Unlock()
	if !isCurrent {
		return
	}
	go func() {
		timer := time.NewTimer(time.Second)
		defer timer.Stop()
		select {
		case <-timer.C:
			w.mu.Lock()
			isCurrent := w.current == assignment
			w.mu.Unlock()
			if isCurrent {
				w.disconnect()
			}
		case <-w.done:
		}
	}()
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

func (q *queueState) take(flowID string, excluded map[*workerStream]time.Time, excludedWorkerIDs map[string]struct{}) (*workerStream, <-chan struct{}, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	preferredElement := q.affinity[flowID]
	preferred := affinityEntry{}
	if preferredElement != nil {
		preferred = preferredElement.Value.(affinityEntry)
	}
	selected := -1
	for index, worker := range q.idle {
		if _, isRejected := excludedWorkerIDs[worker.registration.AppWorkerId]; isRejected {
			continue
		}
		if _, isRejected := excluded[worker]; isRejected {
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
		return nil, q.changed, len(q.streams) > 0
	}
	worker := q.idle[selected]
	q.idle = append(q.idle[:selected], q.idle[selected+1:]...)
	q.rememberLocked(flowID, worker.registration.AppWorkerId)
	return worker, q.changed, true
}

func (q *queueState) rememberLocked(flowID, workerID string) {
	if flowID == "" {
		return
	}
	if element := q.affinity[flowID]; element != nil {
		q.affinityOrder.Remove(element)
	}
	q.affinity[flowID] = q.affinityOrder.PushBack(affinityEntry{flowID: flowID, workerID: workerID, expires: time.Now().Add(affinityDuration)})
	if len(q.affinity) > 10000 {
		oldest := q.affinityOrder.Front()
		delete(q.affinity, oldest.Value.(affinityEntry).flowID)
		q.affinityOrder.Remove(oldest)
	}
}

// DispatchTask routes a task to an idle Worker and relays its output frames.
func (e *Engine) DispatchTask(request *dexpb.DispatchTaskRequest, stream dexpb.MatchingService_DispatchTaskServer) error {
	select {
	case <-e.stop:
		return status.Error(codes.Unavailable, "matching server is closing")
	default:
	}
	if err := validateDispatch(request, e.cfg); err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	if request.TaskId == "" {
		request.TaskId = uuid.NewString()
	}
	excludedWorkerIDs := make(map[string]struct{}, len(request.GetExcludedAppWorkerIds()))
	for _, workerID := range request.GetExcludedAppWorkerIds() {
		excludedWorkerIDs[workerID] = struct{}{}
	}
	ctx := stream.Context()
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
	hasUnavailableWorker := false
	isMatched, err := e.dispatchQueue(ctx, request, request.Queue, stickyEnd, dispatchEnd, excludedWorkerIDs, &hasUnavailableWorker, stream.Send)
	if err != nil {
		return err
	}
	if isMatched {
		return nil
	}
	if request.FallbackQueue != "" && request.FallbackQueue != request.Queue && time.Now().Before(dispatchEnd) {
		isMatched, err = e.dispatchQueue(ctx, request, request.FallbackQueue, dispatchEnd, dispatchEnd, excludedWorkerIDs, &hasUnavailableWorker, stream.Send)
		if err != nil {
			return err
		}
		if isMatched {
			return nil
		}
	}
	return stream.Send(unmatchedDispatchError(request, excludedWorkerIDs, hasUnavailableWorker))
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

func (e *Engine) dispatchQueue(ctx context.Context, request *dexpb.DispatchTaskRequest, queueName string, waitEnd time.Time, dispatchEnd time.Time, excludedWorkerIDs map[string]struct{}, hasUnavailableWorker *bool, send func(*dexpb.DispatchTaskResponse) error) (bool, error) {
	if !time.Now().Before(waitEnd) {
		return false, nil
	}
	owner, err := e.membership.Lookup(membership.RoleMatching, queueName)
	if err != nil {
		return false, status.Error(codes.Unavailable, err.Error())
	}
	if !owner.IsSelf {
		if isForwarded(ctx) {
			return false, status.Error(codes.FailedPrecondition, "matching forwarding loop")
		}
		remoteRequest := &dexpb.DispatchTaskRequest{
			Queue: queueName, FlowId: request.FlowId, DispatchWait: durationUntil(waitEnd),
			Deadline: request.Deadline, TaskId: request.TaskId, Request: request.Request,
			SupportsAssignmentState:   request.GetSupportsAssignmentState(),
			StopAfterUnknownName:      request.GetSupportsUnknownNameError() && queueName != request.FallbackQueue && request.FallbackQueue != "",
			StopWhenQueueHasNoStreams: queueName != request.FallbackQueue && request.FallbackQueue != "",
			SupportsUnknownNameError:  request.GetSupportsUnknownNameError(),
		}
		for workerID := range excludedWorkerIDs {
			remoteRequest.ExcludedAppWorkerIds = append(remoteRequest.ExcludedAppWorkerIds, workerID)
		}
		relay := &forwardDispatchRelay{request: request, send: send, dispatchEnd: dispatchEnd, isSticky: queueName != request.FallbackQueue && request.FallbackQueue != "", excludedWorkerIDs: excludedWorkerIDs}
		err := e.forwardDispatch(ctx, remoteRequest, owner.RPCAddress, relay.relay)
		return relay.isMatched, err
	}
	queue := e.getQueue(queueName)
	defer e.releaseQueue(queueName, queue)
	shouldStopWhenQueueHasNoStreams := request.GetStopWhenQueueHasNoStreams() || queueName != request.FallbackQueue && request.FallbackQueue != ""
	excludedUntil := make(map[*workerStream]time.Time)
	capacityBackoffs := make(map[*workerStream]time.Duration)
	deadlineTimer := time.NewTimer(time.Until(request.Deadline.AsTime()))
	defer deadlineTimer.Stop()
	for {
		select {
		case <-e.stop:
			return false, status.Error(codes.Unavailable, "matching server is closing")
		default:
		}
		if err := ctx.Err(); err != nil {
			return false, err
		}
		if !time.Now().Before(waitEnd) {
			return false, nil
		}
		nextRetry := clearExpiredExclusions(excludedUntil)
		worker, changed, hasStreams := queue.take(request.FlowId, excludedUntil, excludedWorkerIDs)
		if worker == nil {
			if shouldStopWhenQueueHasNoStreams && !hasStreams {
				return false, nil
			}
			wakeAt := waitEnd
			if !nextRetry.IsZero() && nextRetry.Before(wakeAt) {
				wakeAt = nextRetry
			}
			timer := time.NewTimer(time.Until(wakeAt))
			select {
			case <-changed:
			case <-timer.C:
			case <-ctx.Done():
				if !timer.Stop() {
					<-timer.C
				}
				return false, ctx.Err()
			case <-e.stop:
				if !timer.Stop() {
					<-timer.C
				}
				return false, status.Error(codes.Unavailable, "matching server is closing")
			}
			timer.Stop()
			continue
		}
		outcome, attemptErr := worker.dispatchTaskAssignment(ctx, request, dispatchEnd, deadlineTimer.C, send)
		if attemptErr != nil {
			return false, attemptErr
		}
		switch outcome {
		case dispatchAttemptRetryCapacity:
			*hasUnavailableWorker = true
			excludeCapacityReject(worker, excludedUntil, capacityBackoffs)
		case dispatchAttemptRetryDisconnected:
			*hasUnavailableWorker = true
			excludedUntil[worker] = time.Time{}
		case dispatchAttemptRetryUnknownName:
			excludedWorkerIDs[worker.registration.AppWorkerId] = struct{}{}
			if request.GetSupportsUnknownNameError() && queueName != request.FallbackQueue && request.FallbackQueue != "" {
				return false, nil
			}
			if request.GetStopAfterUnknownName() {
				response := unmatchedDispatchError(request, excludedWorkerIDs, *hasUnavailableWorker)
				response.RejectedByAppWorkerId = worker.registration.AppWorkerId
				return true, send(response)
			}
		case dispatchAttemptMatched, dispatchAttemptTerminal:
			return true, attemptErr
		default:
			panic("unknown matching dispatch outcome")
		}
	}
}

func (r *forwardDispatchRelay) relay(frame *dexpb.DispatchTaskResponse) error {
	if frame.GetAssigned() != nil {
		r.isAssigned = true
		return r.send(frame)
	}
	if frame.GetUnassigned() != nil {
		r.isAssigned = false
		return r.send(frame)
	}
	if dispatchErr := frame.GetDispatchError(); dispatchErr != nil {
		if dispatchErr.Reason == dexpb.DispatchTaskErrorReason_DISPATCH_TASK_ERROR_REASON_UNKNOWN_NAME && r.isSticky {
			if workerID := frame.GetRejectedByAppWorkerId(); workerID != "" {
				r.excludedWorkerIDs[workerID] = struct{}{}
			}
			if r.isAssigned {
				r.isAssigned = false
				return sendAssignmentState(r.request, false, r.send)
			}
			return nil
		}
		// Remote sticky waits can expire before the overall dispatch budget.
		if dispatchErr.Reason == dexpb.DispatchTaskErrorReason_DISPATCH_TASK_ERROR_REASON_NO_WORKER_AVAILABLE ||
			(dispatchErr.Reason == dexpb.DispatchTaskErrorReason_DISPATCH_TASK_ERROR_REASON_WORKER_DISCONNECTED && !r.hasForwardedOutput && time.Now().Before(r.dispatchEnd)) {
			if r.isAssigned {
				r.isAssigned = false
				return sendAssignmentState(r.request, false, r.send)
			}
			return nil
		}
	} else {
		r.hasForwardedOutput = true
	}
	r.isMatched = true
	return r.send(frame)
}

func (w *workerStream) dispatchTaskAssignment(
	ctx context.Context,
	request *dexpb.DispatchTaskRequest,
	dispatchEnd time.Time,
	deadline <-chan time.Time,
	send func(*dexpb.DispatchTaskResponse) error,
) (dispatchAttemptOutcome, error) {
	assignment := &taskAssignment{frames: make(chan *dexpb.PollWorkerTaskRequest, 32), abandoned: make(chan struct{}), taskID: request.TaskId}
	w.mu.Lock()
	if w.isClosing {
		w.mu.Unlock()
		return dispatchAttemptRetryDisconnected, nil
	}
	w.isBusy = true
	w.current = assignment
	w.mu.Unlock()
	defer w.abandon(assignment)
	task := &dexpb.PollWorkerTask{TaskId: request.TaskId, Deadline: request.Deadline}
	switch value := request.Request.(type) {
	case *dexpb.DispatchTaskRequest_WaitFor:
		task.Request = &dexpb.PollWorkerTask_WaitFor{WaitFor: value.WaitFor}
	case *dexpb.DispatchTaskRequest_Execute:
		task.Request = &dexpb.PollWorkerTask_Execute{Execute: value.Execute}
	case *dexpb.DispatchTaskRequest_Rpc:
		task.Request = &dexpb.PollWorkerTask_Rpc{Rpc: value.Rpc}
	}
	if err := w.send(&dexpb.PollWorkerTaskResponse{Frame: &dexpb.PollWorkerTaskResponse_Task{Task: task}}); err != nil {
		w.disconnect()
		if !time.Now().Before(dispatchEnd) {
			return dispatchAttemptTerminal, send(dispatchError(dexpb.DispatchTaskErrorReason_DISPATCH_TASK_ERROR_REASON_WORKER_DISCONNECTED, err.Error()))
		}
		return dispatchAttemptRetryDisconnected, nil
	}
	if err := sendAssignmentState(request, true, send); err != nil {
		w.cancelTask(request.TaskId)
		return dispatchAttemptTerminal, err
	}
	hasReceivedOutput := false
	for {
		select {
		case frame := <-assignment.frames:
			if rejection := frame.GetReject(); rejection != nil {
				if hasReceivedOutput {
					return dispatchAttemptTerminal, send(dispatchError(dexpb.DispatchTaskErrorReason_DISPATCH_TASK_ERROR_REASON_WORKER_DISCONNECTED, "Worker rejected task after output"))
				}
				return rejectionOutcome(request, rejection.Reason), sendAssignmentState(request, false, send)
			}
			output := frame.GetTaskFrame()
			hasReceivedOutput = true
			if err := send(relayFrame(output, w.registration.AppWorkerId)); err != nil {
				w.cancelTask(request.TaskId)
				return dispatchAttemptTerminal, err
			}
			if isFinal(output) {
				return dispatchAttemptMatched, nil
			}
		case <-w.done:
			for {
				select {
				case frame := <-assignment.frames:
					if rejection := frame.GetReject(); rejection != nil {
						if hasReceivedOutput {
							return dispatchAttemptTerminal, send(dispatchError(dexpb.DispatchTaskErrorReason_DISPATCH_TASK_ERROR_REASON_WORKER_DISCONNECTED, "Worker rejected task after output"))
						}
						return rejectionOutcome(request, rejection.Reason), sendAssignmentState(request, false, send)
					}
					if output := frame.GetTaskFrame(); output != nil {
						hasReceivedOutput = true
						if err := send(relayFrame(output, w.registration.AppWorkerId)); err != nil {
							return dispatchAttemptTerminal, err
						}
						if isFinal(output) {
							return dispatchAttemptMatched, nil
						}
					}
				default:
					if !hasReceivedOutput && time.Now().Before(dispatchEnd) {
						// Pre-output disconnects can replay tasks because delivery is at least once.
						return dispatchAttemptRetryDisconnected, sendAssignmentState(request, false, send)
					}
					return dispatchAttemptTerminal, send(dispatchError(dexpb.DispatchTaskErrorReason_DISPATCH_TASK_ERROR_REASON_WORKER_DISCONNECTED, "Worker stream disconnected"))
				}
			}
		case <-ctx.Done():
			w.cancelTask(request.TaskId)
			return dispatchAttemptTerminal, ctx.Err()
		case <-deadline:
			w.cancelTask(request.TaskId)
			return dispatchAttemptTerminal, status.Error(codes.DeadlineExceeded, "task deadline elapsed")
		}
	}
}

func rejectionOutcome(request *dexpb.DispatchTaskRequest, reason dexpb.PollWorkerTaskRejectReason) dispatchAttemptOutcome {
	if reason == dexpb.PollWorkerTaskRejectReason_POLL_WORKER_TASK_REJECT_REASON_NO_CAPACITY {
		return dispatchAttemptRetryCapacity
	}
	if (reason == dexpb.PollWorkerTaskRejectReason_POLL_WORKER_TASK_REJECT_REASON_UNKNOWN_STEP && request.GetRpc() == nil) ||
		(reason == dexpb.PollWorkerTaskRejectReason_POLL_WORKER_TASK_REJECT_REASON_UNKNOWN_RPC && request.GetRpc() != nil) {
		return dispatchAttemptRetryUnknownName
	}
	return dispatchAttemptRetryDisconnected
}

func excludeCapacityReject(worker *workerStream, excludedUntil map[*workerStream]time.Time, capacityBackoffs map[*workerStream]time.Duration) {
	backoff := capacityBackoffs[worker]
	if backoff == 0 {
		backoff = capacityRejectInitialBackoff
	} else if backoff < capacityRejectMaxBackoff {
		backoff *= 2
		if backoff > capacityRejectMaxBackoff {
			backoff = capacityRejectMaxBackoff
		}
	}
	capacityBackoffs[worker] = backoff
	excludedUntil[worker] = time.Now().Add(backoff)
}

func clearExpiredExclusions(excludedUntil map[*workerStream]time.Time) time.Time {
	now := time.Now()
	var nextRetry time.Time
	for worker, expiry := range excludedUntil {
		if expiry.IsZero() {
			continue
		}
		if !expiry.After(now) {
			delete(excludedUntil, worker)
			continue
		}
		if nextRetry.IsZero() || expiry.Before(nextRetry) {
			nextRetry = expiry
		}
	}
	return nextRetry
}

func (w *workerStream) cancelTask(taskID string) {
	if err := w.send(&dexpb.PollWorkerTaskResponse{Frame: &dexpb.PollWorkerTaskResponse_CancelTask{CancelTask: &dexpb.PollWorkerTaskCancelTask{TaskId: taskID}}}); err != nil {
		w.disconnect()
	}
}

func durationUntil(deadline time.Time) *durationpb.Duration {
	return durationpb.New(time.Until(deadline))
}

func unmatchedDispatchError(request *dexpb.DispatchTaskRequest, excludedWorkerIDs map[string]struct{}, hasUnavailableWorker bool) *dexpb.DispatchTaskResponse {
	if hasUnavailableWorker || !request.GetSupportsUnknownNameError() || len(excludedWorkerIDs) == 0 {
		return dispatchError(dexpb.DispatchTaskErrorReason_DISPATCH_TASK_ERROR_REASON_NO_WORKER_AVAILABLE, "no Worker accepted the task")
	}
	var detail string
	switch task := request.Request.(type) {
	case *dexpb.DispatchTaskRequest_WaitFor:
		detail = fmt.Sprintf("no pull Worker registered Step %q for Flow type %q", task.WaitFor.GetStepType(), task.WaitFor.GetFlowType())
	case *dexpb.DispatchTaskRequest_Execute:
		detail = fmt.Sprintf("no pull Worker registered Step %q for Flow type %q", task.Execute.GetStepType(), task.Execute.GetFlowType())
	case *dexpb.DispatchTaskRequest_Rpc:
		detail = fmt.Sprintf("no pull Worker registered RPC %q for Flow type %q", task.Rpc.GetRpcName(), task.Rpc.GetFlowType())
	}
	return dispatchError(dexpb.DispatchTaskErrorReason_DISPATCH_TASK_ERROR_REASON_UNKNOWN_NAME, detail)
}

func dispatchError(reason dexpb.DispatchTaskErrorReason, detail string) *dexpb.DispatchTaskResponse {
	return &dexpb.DispatchTaskResponse{Output: &dexpb.DispatchTaskResponse_DispatchError{DispatchError: &dexpb.DispatchTaskError{Reason: reason, Detail: detail}}}
}

func sendAssignmentState(request *dexpb.DispatchTaskRequest, isAssigned bool, send func(*dexpb.DispatchTaskResponse) error) error {
	if !request.GetSupportsAssignmentState() {
		return nil
	}
	if isAssigned {
		return send(&dexpb.DispatchTaskResponse{Output: &dexpb.DispatchTaskResponse_Assigned{Assigned: &emptypb.Empty{}}})
	}
	return send(&dexpb.DispatchTaskResponse{Output: &dexpb.DispatchTaskResponse_Unassigned{Unassigned: &emptypb.Empty{}}})
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

func validateRegistration(registration *dexpb.PollWorkerTaskRegister, cfg *config.MatchingConfig) error {
	if registration == nil {
		return errors.New("first frame must register a Worker")
	}
	workerID := registration.AppWorkerId
	if len(workerID) < 1 || len(workerID) > maxAppWorkerIDLength {
		return fmt.Errorf("app_worker_id must have 1 to %d characters", maxAppWorkerIDLength)
	}
	for _, character := range workerID {
		if !((character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') || (character >= '0' && character <= '9') || character == '-' || character == '_' || character == '.') {
			return errors.New("app_worker_id contains an invalid character")
		}
	}
	if registration.Queue == "" || len(registration.Queue) > cfg.MaxQueueLength {
		return errors.New("queue must have a valid length")
	}
	if len(registration.FlowTypes) > cfg.MaxFlowTypes {
		return errors.New("too many flow_types")
	}
	for _, flowType := range registration.FlowTypes {
		if len(flowType) > cfg.MaxFlowTypeLength {
			return errors.New("flow_type exceeds the length limit")
		}
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

func validateDispatch(request *dexpb.DispatchTaskRequest, cfg *config.MatchingConfig) error {
	if request == nil || request.Queue == "" || request.Request == nil {
		return errors.New("queue and task request are required")
	}
	if len(request.Queue) > cfg.MaxQueueLength || len(request.FallbackQueue) > cfg.MaxQueueLength {
		return errors.New("queue exceeds the length limit")
	}
	if len(request.ExcludedAppWorkerIds) > maxExcludedAppWorkerIDs {
		return fmt.Errorf("excluded_app_worker_ids exceeds the limit of %d", maxExcludedAppWorkerIDs)
	}
	for _, workerID := range request.ExcludedAppWorkerIds {
		if len(workerID) == 0 || len(workerID) > maxAppWorkerIDLength {
			return fmt.Errorf("excluded_app_worker_ids entries must have 1 to %d characters", maxAppWorkerIDLength)
		}
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
	return nil
}

var _ dexpb.MatchingServiceServer = (*Engine)(nil)
