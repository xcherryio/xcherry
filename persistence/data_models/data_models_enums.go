// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: BUSL-1.1

package data_models

import "github.com/xcherryio/apis/goapi/xcapi"

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
		return string(xcapi.RUNNING)
	case ProcessExecutionStatusCompleted:
		return string(xcapi.COMPLETED)
	case ProcessExecutionStatusFailed:
		return string(xcapi.FAILED)
	case ProcessExecutionStatusTimeout:
		return string(xcapi.TIMEOUT)
	case ProcessExecutionStatusTerminated:
		return string(xcapi.TERMINATED)
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
