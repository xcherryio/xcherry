package extensions

import (
	"github.com/jmoiron/sqlx/types"
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
	ProcessExecutionId     string
	ProcessId              string
	IsCurrent              bool
	Status                 string
	StartTime              time.Time
	TimeoutSeconds         int
	HistoryEventIdSequence int
	Info                   types.JSONText
}

type ProcessExecutionInfo struct {
	ProcessType string `json:"processType"`
	WorkerURL   string `json:"workerURL"`
}
