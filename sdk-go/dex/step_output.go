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
	"sync"

	"github.com/superdurable/dex/sdk-go/gen/dexpb"
)

type stepProgressStream interface {
	sendHeartbeat(*dexpb.StepMethodHeartbeat) error
	sendStreamWrite(*dexpb.StepStreamWrite) error
}

type waitForOutputStream struct {
	stream waitForFrameSender
}

type executeOutputStream struct {
	stream executeFrameSender
}

type waitForFrameSender interface {
	Context() context.Context
	Send(*dexpb.InvokeWaitForMethodOutput) error
}

type executeFrameSender interface {
	Context() context.Context
	Send(*dexpb.InvokeExecuteMethodOutput) error
}

type stepOutputEmitter struct {
	mu        sync.Mutex
	stream    stepProgressStream
	cancel    context.CancelFunc
	isOpen    bool
	sendError error
}

func newWaitForOutputStream(
	stream waitForFrameSender,
) *waitForOutputStream {
	if stream == nil {
		panic("dex: WaitFor output stream is nil")
	}
	return &waitForOutputStream{stream: stream}
}

func newExecuteOutputStream(
	stream executeFrameSender,
) *executeOutputStream {
	if stream == nil {
		panic("dex: Execute output stream is nil")
	}
	return &executeOutputStream{stream: stream}
}

func newStepOutputEmitter(
	parent context.Context,
	stream stepProgressStream,
) (context.Context, *stepOutputEmitter) {
	if stream == nil {
		panic("dex: Step output stream is nil")
	}
	handlerContext, cancel := context.WithCancel(parent)
	return handlerContext, &stepOutputEmitter{
		stream: stream,
		cancel: cancel,
		isOpen: true,
	}
}

func (stream *waitForOutputStream) sendHeartbeat(
	heartbeat *dexpb.StepMethodHeartbeat,
) error {
	return stream.stream.Send(&dexpb.InvokeWaitForMethodOutput{
		Output: &dexpb.InvokeWaitForMethodOutput_Heartbeat{Heartbeat: heartbeat},
	})
}

func (stream *waitForOutputStream) sendStreamWrite(write *dexpb.StepStreamWrite) error {
	return stream.stream.Send(&dexpb.InvokeWaitForMethodOutput{
		Output: &dexpb.InvokeWaitForMethodOutput_StreamWrite{StreamWrite: write},
	})
}

func (stream *waitForOutputStream) sendResult(
	result *dexpb.InvokeWaitForMethodResponse,
) error {
	return stream.stream.Send(&dexpb.InvokeWaitForMethodOutput{
		Output: &dexpb.InvokeWaitForMethodOutput_Result{Result: result},
	})
}

func (stream *executeOutputStream) sendHeartbeat(
	heartbeat *dexpb.StepMethodHeartbeat,
) error {
	return stream.stream.Send(&dexpb.InvokeExecuteMethodOutput{
		Output: &dexpb.InvokeExecuteMethodOutput_Heartbeat{Heartbeat: heartbeat},
	})
}

func (stream *executeOutputStream) sendStreamWrite(write *dexpb.StepStreamWrite) error {
	return stream.stream.Send(&dexpb.InvokeExecuteMethodOutput{
		Output: &dexpb.InvokeExecuteMethodOutput_StreamWrite{StreamWrite: write},
	})
}

func (stream *executeOutputStream) sendResult(
	result *dexpb.InvokeExecuteMethodResponse,
) error {
	return stream.stream.Send(&dexpb.InvokeExecuteMethodOutput{
		Output: &dexpb.InvokeExecuteMethodOutput_Result{Result: result},
	})
}

func (emitter *stepOutputEmitter) sendHeartbeat(
	heartbeat *dexpb.StepMethodHeartbeat,
) error {
	return emitter.send(func() error {
		return emitter.stream.sendHeartbeat(heartbeat)
	})
}

func (emitter *stepOutputEmitter) sendStreamWrite(write *dexpb.StepStreamWrite) error {
	return emitter.send(func() error {
		return emitter.stream.sendStreamWrite(write)
	})
}

func (emitter *stepOutputEmitter) send(sendFrame func() error) error {
	emitter.mu.Lock()
	defer emitter.mu.Unlock()
	if !emitter.isOpen {
		if emitter.sendError != nil {
			return emitter.sendError
		}
		return errInvalidInvocationContext
	}
	if err := sendFrame(); err != nil {
		emitter.sendError = err
		emitter.isOpen = false
		emitter.cancel()
		return err
	}
	return nil
}

func (emitter *stepOutputEmitter) close() error {
	emitter.mu.Lock()
	defer emitter.mu.Unlock()
	if emitter.isOpen {
		emitter.isOpen = false
		emitter.cancel()
	}
	return emitter.sendError
}
