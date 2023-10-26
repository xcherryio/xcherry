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
	StateExecutionStatusWaitUntilRunning StateExecutionStatus = 1
	StateExecutionStatusWaitUntilWaiting StateExecutionStatus = 2
	StateExecutionStatusExecuteRunning   StateExecutionStatus = 3
	StateExecutionStatusCompleted        StateExecutionStatus = 4
	StateExecutionStatusFailed           StateExecutionStatus = 5
	StateExecutionStatusTimeout          StateExecutionStatus = 6
	StateExecutionStatusAborted          StateExecutionStatus = 7
)

func (e StateExecutionStatus) String() string {
	switch e {
	case StateExecutionStatusWaitUntilRunning:
		return "WaitUntilRunning"
	case StateExecutionStatusWaitUntilWaiting:
		return "WaitUntilWaiting"
	case StateExecutionStatusExecuteRunning:
		return "ExecuteRunning"
	case StateExecutionStatusCompleted:
		return "Completed"
	case StateExecutionStatusFailed:
		return "Failed"
	case StateExecutionStatusTimeout:
		return "Timeout"
	case StateExecutionStatusAborted:
		return "Aborted"
	default:
		panic("this is not supported")
	}
}

type ImmediateTaskType int32

const (
	ImmediateTaskTypeWaitUntil             ImmediateTaskType = 1
	ImmediateTaskTypeExecute               ImmediateTaskType = 2
	ImmediateTaskTypeNewLocalQueueMessages ImmediateTaskType = 3
)

func (e ImmediateTaskType) String() string {
	switch e {
	case ImmediateTaskTypeWaitUntil:
		return "WaitUntil"
	case ImmediateTaskTypeExecute:
		return "Execute"
	case ImmediateTaskTypeNewLocalQueueMessages:
		return "NewLocalQueueMessages"
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
