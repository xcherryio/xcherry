// Copyright (c) 2022-2026 Super Durable, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

// Package workermode selects whether the example Workers listen for push tasks
// or connect to Dex Server as pull Workers.
package workermode

import (
	"fmt"
	"os"

	"github.com/superdurable/dex/sdk-go/dex"
)

// WorkerModeEnvironmentVariable names the environment variable that selects
// the Worker mode. Valid values are push and pull. Empty selects push.
const WorkerModeEnvironmentVariable = "DEX_EXAMPLES_WORKER_MODE"

// WorkerMode is how an example Worker receives tasks from Dex Server.
type WorkerMode string

const (
	// PushWorkerMode listens on a Worker address that Dex Server calls.
	PushWorkerMode WorkerMode = "push"
	// PullWorkerMode opens outbound task streams to Dex Server.
	PullWorkerMode WorkerMode = "pull"
)

// ReadWorkerModeFromEnvironment returns the Worker mode selected by
// DEX_EXAMPLES_WORKER_MODE, or an error for an unknown value.
func ReadWorkerModeFromEnvironment() (WorkerMode, error) {
	switch value := os.Getenv(WorkerModeEnvironmentVariable); value {
	case "", string(PushWorkerMode):
		return PushWorkerMode, nil
	case string(PullWorkerMode):
		return PullWorkerMode, nil
	default:
		return "", fmt.Errorf("%s must be push or pull, got %q", WorkerModeEnvironmentVariable, value)
	}
}

// IsPull reports whether the Worker connects to Dex Server as a pull Worker.
func (mode WorkerMode) IsPull() bool {
	return mode == PullWorkerMode
}

// NewExamplePullWorkerOptions returns pull options sized for every sample Flow
// sharing one Worker process.
func NewExamplePullWorkerOptions() *dex.PullWorkerOptions {
	return &dex.PullWorkerOptions{MaxConcurrentTasks: 64}
}
