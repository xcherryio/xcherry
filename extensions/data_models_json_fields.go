package extensions

// TODO add helper methods for conversion

type ProcessExecutionInfoJson struct {
	ProcessType string `json:"processType"`
	WorkerURL   string `json:"workerURL"`
}

type StateExecutionIdSequenceJson struct {
	SequenceMap map[string]int `json:"sequenceMap"`
}

type AsyncStateExecutionInfoJson struct {
	ProcessId   string `json:"processId"`
	ProcessType string `json:"processType"`
	WorkerURL   string `json:"workerURL"`
}
