// Copyright 2023 XDBLab organization
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package persistence

import "github.com/xdblab/xdb-apis/goapi/xdbapi"

type ProcessExecutionStatus int32

const (
	ProcessExecutionStatusUndefined  ProcessExecutionStatus = 0
	ProcessExecutionStatusRunning    ProcessExecutionStatus = 1
	ProcessExecutionStatusCompleted  ProcessExecutionStatus = 2
	ProcessExecutionStatusFailed     ProcessExecutionStatus = 3
	ProcessExecutionStatusTimeout    ProcessExecutionStatus = 4
	ProcessExecutionStatusTerminated ProcessExecutionStatus = 5
)

func (e ProcessExecutionStatus) String() string {
	switch e {
	case ProcessExecutionStatusRunning:
		return string(xdbapi.RUNNING)
	case ProcessExecutionStatusCompleted:
		return string(xdbapi.COMPLETED)
	case ProcessExecutionStatusFailed:
		return string(xdbapi.FAILED)
	case ProcessExecutionStatusTimeout:
		return string(xdbapi.TIMEOUT)
	case ProcessExecutionStatusTerminated:
		return string(xdbapi.TERMINATED)
	case ProcessExecutionStatusUndefined:
		return "UNDEFINED"
	default:
		panic("this is not supported")
	}
}

type StateExecutionStatus int32

const (
	StateExecutionStatusSkipped   StateExecutionStatus = -1
	StateExecutionStatusUndefined StateExecutionStatus = 0
	StateExecutionStatusRunning   StateExecutionStatus = 1
	StateExecutionStatusCompleted StateExecutionStatus = 2
	StateExecutionStatusFailed    StateExecutionStatus = 3
	StateExecutionStatusTimeout   StateExecutionStatus = 4
	StateExecutionStatusAborted   StateExecutionStatus = 5
)

func (e StateExecutionStatus) String() string {
	switch e {
	case StateExecutionStatusSkipped:
		return "Skipped"
	case StateExecutionStatusRunning:
		return "Running"
	case StateExecutionStatusCompleted:
		return "Completed"
	case StateExecutionStatusFailed:
		return "Failed"
	case StateExecutionStatusTimeout:
		return "Timeout"
	case StateExecutionStatusAborted:
		return "Aborted"
	case StateExecutionStatusUndefined:
		return "Undefined"
	default:
		panic("this is not supported")
	}
}

type WorkerTaskType int32

const (
	WorkerTaskTypeWaitUntil WorkerTaskType = 1
	WorkerTaskTypeExecute   WorkerTaskType = 2
)

func (e WorkerTaskType) String() string {
	switch e {
	case WorkerTaskTypeWaitUntil:
		return "WaitUntil"
	case WorkerTaskTypeExecute:
		return "Execute"
	default:
		panic("this is not supported")
	}
}

type TimerTaskType int32

const (
	TimerTaskTypeProcessTimeout    TimerTaskType = 1
	TimerTaskTypeTimerCommand      TimerTaskType = 2
	TimerTaskTypeWorkerTaskBackoff TimerTaskType = 3
)
