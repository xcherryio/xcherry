package extensions

import (
	"github.com/jmoiron/sqlx/types"
	"time"
)

type ProcessExecutionStatus int

const (
	ExecutionStatusUndefined ProcessExecutionStatus = 0
	ExecutionStatusRunning   ProcessExecutionStatus = 1
	ExecutionStatusCompleted ProcessExecutionStatus = 2
	ExecutionStatusFailed    ProcessExecutionStatus = 3
	ExecutionStatusTimeout   ProcessExecutionStatus = 4
)

func (e ProcessExecutionStatus) String() string {
	switch e {
	case ExecutionStatusRunning:
		return "Running"
	case ExecutionStatusCompleted:
		return "Completed"
	case ExecutionStatusFailed:
		return "Failed"
	case ExecutionStatusTimeout:
		return "Timeout"
	default:
		panic("this is not supported")
	}
}

type StateExecutionStatus int

const (
	StateExecutionStatusSkipped   StateExecutionStatus = -1
	StateExecutionStatusUndefined StateExecutionStatus = 0
	StateExecutionStatusRunning   StateExecutionStatus = 1
	StateExecutionStatusCompleted StateExecutionStatus = 2
	StateExecutionStatusFailed    StateExecutionStatus = 3
	StateExecutionStatusTimeout   StateExecutionStatus = 4
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
	default:
		panic("this is not supported")
	}
}

type CurrentProcessExecutionRow struct {
	Namespace          string
	ProcessId          string
	ProcessExecutionId string
}
type ProcessExecutionRow struct {
	Namespace              string
	ProcessExecutionId     string
	ProcessId              string
	IsCurrent              bool
	Status                 string
	StartTime              time.Time
	TimeoutSeconds         int
	HistoryEventIdSequence int
	Info                   types.JSONText
}

type ProcessExecutionInfoJson struct {
	ProcessType string `json:"processType"`
	WorkerURL   string `json:"workerURL"`
}
