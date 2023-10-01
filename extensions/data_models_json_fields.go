package extensions

type ProcessExecutionInfoJson struct {
	ProcessType string `json:"processType"`
	WorkerURL   string `json:"workerURL"`
}

type StateExecutionIdSequenceJson struct {
	SequenceMap map[string]int `json:"sequenceMap"`
}

type AsyncStateExecutionInfoJson struct {
}
