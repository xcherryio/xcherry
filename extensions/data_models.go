package extensions

import (
	"time"
)

type ExecutionStatus string

const (
	ExecutionStatusRunning   ExecutionStatus = "running"
	ExecutionStatusTimeout   ExecutionStatus = "timeout"
	ExecutionStatusCompleted ExecutionStatus = "completed"
	ExecutionStatusFailed    ExecutionStatus = "failed"
)

func (e ExecutionStatus) String() string {
	return string(e)
}

type ProcessExecutionRow struct {
	Id                     string
	ProcessId              string
	IsCurrent              bool
	Status                 string
	StartTime              time.Time
	TimeoutSeconds         int
	HistoryEventIdSequence int
	Info                   ProcessExecutionInfo
}

type ProcessExecutionInfo struct {
	ProcessType string
	WorkerURL   string
}
