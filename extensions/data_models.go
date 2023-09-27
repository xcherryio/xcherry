package extensions

import "time"

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
	WorkerUrl    string
	StartStateId string
}
