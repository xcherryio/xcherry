// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package streamstore

import (
	"context"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/superdurable/dex/config"
	"github.com/superdurable/dex/gen/dexpb"
	"google.golang.org/protobuf/proto"
)

type memoryBackend struct {
	cfg           *config.StreamStoreConfig
	isMultiTenant bool

	mutex  sync.Mutex
	ready  *sync.Cond
	scopes map[streamScope]*memoryScope
	// tenantBytes are the charged bytes buffered per tenant in this process; the empty tenant is single-tenant.
	tenantBytes map[string]int64
	pending     map[streamScope]*memoryTrimState
	closed      bool
	stop        chan struct{}
	wait        sync.WaitGroup
}

type memoryScope struct {
	totalBytes       int64
	fifo             []*memoryEntry
	instances        map[string]*memoryInstance
	lastMilliseconds int64
	lastSequence     uint64
}

type memoryInstance struct {
	messages []*memoryEntry
	notify   chan struct{}
	waiters  int
}

type memoryEntry struct {
	messageID    string
	milliseconds int64
	sequence     uint64
	flowID       string
	source       string
	payload      []byte
	chargedBytes int64
}

type memoryTrimState struct {
	targetBytes int64
	generation  uint64
	isRunning   bool
}

type memoryTrimTask struct {
	scope      streamScope
	target     int64
	generation uint64
}

func newMemoryBackend(cfg *config.StreamStoreConfig, isMultiTenant bool) *memoryBackend {
	if cfg == nil {
		panic("Memory Stream Store config must not be nil")
	}
	backend := &memoryBackend{
		cfg:           cfg,
		isMultiTenant: isMultiTenant,
		scopes:        make(map[streamScope]*memoryScope),
		tenantBytes:   make(map[string]int64),
		pending:       make(map[streamScope]*memoryTrimState),
		stop:          make(chan struct{}),
	}
	backend.ready = sync.NewCond(&backend.mutex)
	for workerIndex := 0; workerIndex < cfg.EffectiveTrimWorkers(); workerIndex++ {
		backend.wait.Add(1)
		go backend.runTrimWorker()
	}
	return backend
}

func (b *memoryBackend) Close() error {
	b.mutex.Lock()
	if b.closed {
		b.mutex.Unlock()
		b.wait.Wait()
		return nil
	}
	b.closed = true
	for _, scope := range b.scopes {
		for _, instance := range scope.instances {
			close(instance.notify)
		}
	}
	b.ready.Broadcast()
	close(b.stop)
	b.mutex.Unlock()
	b.wait.Wait()
	return nil
}

func (b *memoryBackend) Write(ctx context.Context, input backendWriteInput) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	b.mutex.Lock()
	defer b.mutex.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if b.closed {
		return ErrUnavailable
	}
	scopeID := newStreamScope(b.isMultiTenant, input.input.FlowType, input.input.StreamName, input.input.FlowID)
	scope := b.scopeLocked(scopeID)
	if scope.totalBytes >= input.capacityBytes ||
		input.chargedBytes > input.capacityBytes-scope.totalBytes {
		b.scheduleTrimLocked(scopeID, input.baseTrimTargetBytes)
		return ErrCapacityExceeded
	}
	if scopeID.tenantID != "" && input.maxTenantBytes > 0 &&
		b.tenantBytes[scopeID.tenantID]+input.chargedBytes > input.maxTenantBytes {
		b.scheduleTrimLocked(scopeID, input.baseTrimTargetBytes)
		return ErrCapacityExceeded
	}
	b.tenantBytes[scopeID.tenantID] += input.chargedBytes
	entry := scope.append(
		input.input.FlowID,
		input.input.Source,
		input.payload,
		input.chargedBytes,
	)
	instance := scope.instance(input.input.FlowID)
	instance.messages = append(instance.messages, entry)
	close(instance.notify)
	instance.notify = make(chan struct{})
	if scope.totalBytes >= input.trimTriggerBytes {
		b.scheduleTrimLocked(scopeID, input.messageTrimTargetBytes)
	}
	return nil
}

func (b *memoryBackend) Read(
	ctx context.Context,
	flowType string,
	flowID string,
	streamName string,
	messageID string,
) (*Message, error) {
	cursorMilliseconds, cursorSequence, err := parseMessageID(messageID)
	if err != nil {
		panic("validated Stream message ID was not parseable")
	}
	for {
		b.mutex.Lock()
		if b.closed {
			b.mutex.Unlock()
			return nil, ErrUnavailable
		}
		scopeID := newStreamScope(b.isMultiTenant, flowType, streamName, flowID)
		scope := b.scopeLocked(scopeID)
		instance := scope.instance(flowID)
		entry := instance.after(cursorMilliseconds, cursorSequence)
		if entry != nil {
			b.mutex.Unlock()
			return entry.message()
		}
		notify := instance.notify
		instance.waiters++
		b.mutex.Unlock()
		select {
		case <-notify:
			b.releaseReader(scopeID, flowID, scope, instance)
		case <-ctx.Done():
			b.releaseReader(scopeID, flowID, scope, instance)
			if ctx.Err() == context.DeadlineExceeded {
				return nil, ErrWaitTimeout
			}
			return nil, ctx.Err()
		}
	}
}

func (b *memoryBackend) List(
	ctx context.Context,
	flowType string,
	flowID string,
	streamName string,
	beforeMessageID string,
	pageSize int32,
) ([]*Message, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	b.mutex.Lock()
	defer b.mutex.Unlock()
	if b.closed {
		return nil, false, ErrUnavailable
	}
	scope := b.scopes[newStreamScope(b.isMultiTenant, flowType, streamName, flowID)]
	if scope == nil {
		return nil, false, nil
	}
	instance := scope.instances[flowID]
	if instance == nil {
		return nil, false, nil
	}
	end := len(instance.messages)
	if beforeMessageID != "" {
		cursorMilliseconds, cursorSequence, err := parseMessageID(beforeMessageID)
		if err != nil {
			panic("validated Stream page message ID was not parseable")
		}
		end = instance.firstAtOrAfter(cursorMilliseconds, cursorSequence)
	}
	start := end - int(pageSize) - 1
	if start < 0 {
		start = 0
	}
	hasMore := end-start > int(pageSize)
	if hasMore {
		start++
	}
	messages := make([]*Message, 0, end-start)
	for index := end - 1; index >= start; index-- {
		message, err := instance.messages[index].message()
		if err != nil {
			return nil, false, err
		}
		messages = append(messages, message)
	}
	return messages, hasMore, nil
}

func (b *memoryBackend) runTrimWorker() {
	defer b.wait.Done()
	for {
		task, ok := b.nextTrimTask()
		if !ok {
			return
		}
		b.trim(task)
		b.finishTrimTask(task)
	}
}

func (b *memoryBackend) nextTrimTask() (memoryTrimTask, bool) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	for {
		if b.closed {
			return memoryTrimTask{}, false
		}
		for scope, state := range b.pending {
			if state.isRunning {
				continue
			}
			state.isRunning = true
			return memoryTrimTask{
				scope:      scope,
				target:     state.targetBytes,
				generation: state.generation,
			}, true
		}
		b.ready.Wait()
	}
}

func (b *memoryBackend) trim(task memoryTrimTask) {
	for {
		b.mutex.Lock()
		if b.closed {
			b.mutex.Unlock()
			return
		}
		scope := b.scopes[task.scope]
		isComplete := true
		if scope != nil {
			var trimmedBytes int64
			isComplete, trimmedBytes = scope.trim(task.target, b.cfg.EffectiveBackgroundTrimBatchSize())
			b.releaseTenantBytesLocked(task.scope.tenantID, trimmedBytes)
		}
		if scope != nil && scope.isEmpty() {
			delete(b.scopes, task.scope)
		}
		b.mutex.Unlock()
		if isComplete || !b.waitForTrimBatch() {
			return
		}
	}
}

func (b *memoryBackend) waitForTrimBatch() bool {
	timer := time.NewTimer(b.cfg.EffectiveTrimBatchYieldTime())
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-b.stop:
		return false
	}
}

func (b *memoryBackend) finishTrimTask(task memoryTrimTask) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	state := b.pending[task.scope]
	if state == nil {
		return
	}
	if state.generation == task.generation {
		delete(b.pending, task.scope)
		return
	}
	state.isRunning = false
	b.ready.Signal()
}

func (b *memoryBackend) releaseReader(
	scopeID streamScope,
	flowID string,
	scope *memoryScope,
	instance *memoryInstance,
) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	instance.waiters--
	if instance.waiters < 0 {
		panic("Memory Stream Store reader count became negative")
	}
	if len(instance.messages) == 0 && instance.waiters == 0 && scope.instances[flowID] == instance {
		delete(scope.instances, flowID)
	}
	if scope.isEmpty() && b.scopes[scopeID] == scope {
		delete(b.scopes, scopeID)
	}
}

func (b *memoryBackend) scopeLocked(scopeID streamScope) *memoryScope {
	scope := b.scopes[scopeID]
	if scope == nil {
		scope = &memoryScope{
			instances: make(map[string]*memoryInstance),
		}
		b.scopes[scopeID] = scope
	}
	return scope
}

func (b *memoryBackend) scheduleTrimLocked(scopeID streamScope, targetBytes int64) {
	state := b.pending[scopeID]
	if state == nil {
		state = &memoryTrimState{targetBytes: targetBytes}
		b.pending[scopeID] = state
	} else if targetBytes < state.targetBytes {
		state.targetBytes = targetBytes
	} else if !state.isRunning {
		return
	}
	state.generation++
	b.ready.Signal()
}

func (s *memoryScope) append(
	flowID string,
	source string,
	payload []byte,
	chargedBytes int64,
) *memoryEntry {
	milliseconds, sequence := s.nextMessageIdentity()
	entry := &memoryEntry{
		messageID:    fmt.Sprintf("%d-%d", milliseconds, sequence),
		milliseconds: milliseconds,
		sequence:     sequence,
		flowID:       flowID,
		source:       source,
		payload:      payload,
		chargedBytes: chargedBytes,
	}
	s.fifo = append(s.fifo, entry)
	s.totalBytes += chargedBytes
	return entry
}

func (s *memoryScope) nextMessageIdentity() (int64, uint64) {
	milliseconds := time.Now().UnixMilli()
	if milliseconds > s.lastMilliseconds {
		s.lastMilliseconds = milliseconds
		s.lastSequence = 0
		return milliseconds, 0
	}
	if s.lastSequence == math.MaxUint64 {
		s.lastMilliseconds++
		s.lastSequence = 0
		return s.lastMilliseconds, 0
	}
	s.lastSequence++
	return s.lastMilliseconds, s.lastSequence
}

func (s *memoryScope) instance(flowID string) *memoryInstance {
	instance := s.instances[flowID]
	if instance == nil {
		instance = &memoryInstance{notify: make(chan struct{})}
		s.instances[flowID] = instance
	}
	return instance
}

// trim reports whether the scope reached targetBytes, and the bytes it removed.
func (s *memoryScope) trim(targetBytes int64, batchSize int) (bool, int64) {
	trimmedMessages := 0
	var trimmedBytes int64
	for s.totalBytes > targetBytes && trimmedMessages < batchSize && len(s.fifo) > 0 {
		entry := s.fifo[0]
		s.fifo[0] = nil
		s.fifo = s.fifo[1:]
		instance := s.instances[entry.flowID]
		if instance == nil || len(instance.messages) == 0 || instance.messages[0] != entry {
			panic("Memory Stream Store instance FIFO is inconsistent")
		}
		instance.messages[0] = nil
		instance.messages = instance.messages[1:]
		if len(instance.messages) == 0 && instance.waiters == 0 {
			delete(s.instances, entry.flowID)
		}
		s.totalBytes -= entry.chargedBytes
		trimmedBytes += entry.chargedBytes
		if s.totalBytes < 0 {
			panic("Memory Stream Store charged bytes became negative")
		}
		trimmedMessages++
	}
	return s.totalBytes <= targetBytes || len(s.fifo) == 0, trimmedBytes
}

func (b *memoryBackend) releaseTenantBytesLocked(tenantID string, trimmedBytes int64) {
	b.tenantBytes[tenantID] -= trimmedBytes
	if b.tenantBytes[tenantID] <= 0 {
		delete(b.tenantBytes, tenantID)
	}
}

func (s *memoryScope) isEmpty() bool {
	return len(s.fifo) == 0 && len(s.instances) == 0
}

func (i *memoryInstance) after(milliseconds int64, sequence uint64) *memoryEntry {
	index := sort.Search(len(i.messages), func(index int) bool {
		entry := i.messages[index]
		return entry.milliseconds > milliseconds ||
			(entry.milliseconds == milliseconds && entry.sequence > sequence)
	})
	if index == len(i.messages) {
		return nil
	}
	return i.messages[index]
}

func (i *memoryInstance) firstAtOrAfter(milliseconds int64, sequence uint64) int {
	return sort.Search(len(i.messages), func(index int) bool {
		entry := i.messages[index]
		return entry.milliseconds > milliseconds ||
			(entry.milliseconds == milliseconds && entry.sequence >= sequence)
	})
}

func (e *memoryEntry) message() (*Message, error) {
	value := &dexpb.Value{}
	if err := proto.Unmarshal(e.payload, value); err != nil {
		return nil, fmt.Errorf("unmarshal retained Stream Value: %w", err)
	}
	return &Message{
		Value:       value,
		MessageID:   e.messageID,
		CreatedTime: time.UnixMilli(e.milliseconds),
		Source:      e.source,
	}, nil
}
