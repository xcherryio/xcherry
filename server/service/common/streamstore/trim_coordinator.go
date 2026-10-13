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
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/superdurable/dex/config"
	"github.com/superdurable/dex/service"
	"github.com/superdurable/dex/service/common/log"
	"github.com/superdurable/dex/service/common/log/tag"
)

type trimCoordinator struct {
	cfg     *config.StreamStoreConfig
	client  *redis.Client
	logger  log.Logger
	ownerID string
	ctx     context.Context
	cancel  context.CancelFunc

	mutex     sync.Mutex
	ready     *sync.Cond
	pending   map[streamScope]*trimState
	closed    bool
	wait      sync.WaitGroup
	stop      chan struct{}
	closeOnce sync.Once
}

type trimState struct {
	targetBytes int64
	generation  uint64
	isRunning   bool
}

// streamScope is the budget scope of a Stream: the Flow type and Stream name, within one tenant.
type streamScope struct {
	tenantID   string
	flowType   string
	streamName string
}

// newStreamScope derives the tenant from a server-minted stored Flow ID in multi-tenant mode only.
func newStreamScope(isMultiTenant bool, flowType string, streamName string, flowID string) streamScope {
	scope := streamScope{flowType: flowType, streamName: streamName}
	if isMultiTenant {
		scope.tenantID, _, _ = service.SplitTenantStoredFlowID(flowID)
	}
	return scope
}

type trimTask struct {
	scope      streamScope
	target     int64
	generation uint64
}

func newTrimCoordinator(
	cfg *config.StreamStoreConfig,
	client *redis.Client,
	logger log.Logger,
) *trimCoordinator {
	if cfg == nil || client == nil || logger == nil {
		panic("Trim Coordinator dependencies must not be nil")
	}
	coordinatorCtx, cancelCoordinator := context.WithCancel(context.Background())
	coordinator := &trimCoordinator{
		cfg:     cfg,
		client:  client,
		logger:  logger,
		ownerID: uuid.NewString(),
		ctx:     coordinatorCtx,
		cancel:  cancelCoordinator,
		pending: make(map[streamScope]*trimState),
		stop:    make(chan struct{}),
	}
	coordinator.ready = sync.NewCond(&coordinator.mutex)
	for workerIndex := 0; workerIndex < cfg.EffectiveTrimWorkers(); workerIndex++ {
		coordinator.wait.Add(1)
		go coordinator.runWorker()
	}
	return coordinator
}

func (c *trimCoordinator) Schedule(scope streamScope, targetBytes int64) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.closed {
		return
	}
	state := c.pending[scope]
	if state == nil {
		state = &trimState{targetBytes: targetBytes}
		c.pending[scope] = state
	} else {
		if targetBytes < state.targetBytes {
			state.targetBytes = targetBytes
		} else if !state.isRunning {
			return
		}
	}
	state.generation++
	c.ready.Signal()
}

func (c *trimCoordinator) Close() {
	c.closeOnce.Do(func() {
		c.mutex.Lock()
		c.closed = true
		c.ready.Broadcast()
		c.mutex.Unlock()
		close(c.stop)
		c.cancel()
	})
	c.wait.Wait()
}

func (c *trimCoordinator) runWorker() {
	defer c.wait.Done()
	for {
		task, ok := c.nextTask()
		if !ok {
			return
		}
		if err := c.trim(task.scope, task.target); err != nil && !c.isClosed() {
			c.logger.Error("background Stream trim failed", tag.Error(err))
		}
		c.finishTask(task)
	}
}

func (c *trimCoordinator) nextTask() (trimTask, bool) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	for {
		if c.closed {
			return trimTask{}, false
		}
		for scope, state := range c.pending {
			if state.isRunning {
				continue
			}
			state.isRunning = true
			return trimTask{
				scope:      scope,
				target:     state.targetBytes,
				generation: state.generation,
			}, true
		}
		c.ready.Wait()
	}
}

func (c *trimCoordinator) finishTask(task trimTask) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	state := c.pending[task.scope]
	if state == nil {
		return
	}
	if state.generation == task.generation {
		delete(c.pending, task.scope)
		return
	}
	state.isRunning = false
	c.ready.Signal()
}

func (c *trimCoordinator) trim(scope streamScope, targetBytes int64) error {
	keys := streamKeys(scope, "")
	leaseOwner := c.ownerID + ":" + uuid.NewString()
	for {
		if c.isClosed() {
			return nil
		}
		acquired, err := c.client.SetNX(c.ctx, keys.lease, leaseOwner, c.cfg.EffectiveTrimLeaseTTL()).Result()
		if err != nil {
			return fmt.Errorf("acquire Redis trim lease: %w", err)
		}
		if acquired {
			break
		}
		if !c.waitFor(c.cfg.EffectiveTrimLeaseRetry()) {
			return nil
		}
	}
	defer func() {
		if c.isClosed() {
			return
		}
		if err := runReleaseLeaseScript(c.ctx, c.client, releaseLeaseScriptInput{
			leaseKey:   keys.lease,
			leaseOwner: leaseOwner,
		}); err != nil {
			c.logger.Error("release Stream trim lease failed", tag.Error(err))
		}
	}()

	for {
		renewOutput, err := runRenewLeaseScript(
			c.ctx,
			c.client,
			renewLeaseScriptInput{
				leaseKey:   keys.lease,
				leaseOwner: leaseOwner,
				leaseTTL:   c.cfg.EffectiveTrimLeaseTTL(),
			},
		)
		if err != nil {
			return err
		}
		if !renewOutput.isRenewed {
			return nil
		}
		trimOutput, err := runTrimScript(c.ctx, c.client, trimScriptInput{
			keys:        keys,
			targetBytes: targetBytes,
			batchSize:   c.cfg.EffectiveBackgroundTrimBatchSize(),
			leaseOwner:  leaseOwner,
		})
		if err != nil {
			return err
		}
		if trimOutput.remainingBytes < 0 || trimOutput.remainingBytes <= targetBytes {
			return nil
		}
		if !c.waitFor(c.cfg.EffectiveTrimBatchYieldTime()) {
			return nil
		}
	}
}

func (c *trimCoordinator) waitFor(duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-timer.C:
		return !c.isClosed()
	case <-c.stop:
		return false
	}
}

func (c *trimCoordinator) isClosed() bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.closed
}
