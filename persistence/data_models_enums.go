package persistence

type ProcessExecutionStatus int32

const (
	ProcessExecutionStatusUndefined ProcessExecutionStatus = 0
	ProcessExecutionStatusRunning   ProcessExecutionStatus = 1
	ProcessExecutionStatusCompleted ProcessExecutionStatus = 2
	ProcessExecutionStatusFailed    ProcessExecutionStatus = 3
	ProcessExecutionStatusTimeout   ProcessExecutionStatus = 4
)

func (e ProcessExecutionStatus) String() string {
	switch e {
	case ProcessExecutionStatusRunning:
		return "Running"
	case ProcessExecutionStatusCompleted:
		return "Completed"
	case ProcessExecutionStatusFailed:
		return "Failed"
	case ProcessExecutionStatusTimeout:
		return "Timeout"
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
