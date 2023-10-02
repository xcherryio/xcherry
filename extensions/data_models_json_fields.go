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

// EncodedDataJson represents xdbapi.EncodedObject
type EncodedDataJson struct {
	Encoding *string `json:"encoding"`
	Data     *string `json:"data"`
}
