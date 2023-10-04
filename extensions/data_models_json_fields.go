package extensions

import (
	"encoding/json"
	"github.com/xdblab/xdb-apis/goapi/xdbapi"
)

type ProcessExecutionInfoJson struct {
	ProcessType string `json:"processType"`
	WorkerURL   string `json:"workerURL"`
}

func FromStartRequestToProcessInfoBytes(req xdbapi.ProcessExecutionStartRequest) ([]byte, error) {
	return json.Marshal(ProcessExecutionInfoJson{
		ProcessType: req.GetProcessType(),
		WorkerURL:   req.GetWorkerUrl(),
	})
}

func BytesToProcessExecutionInfo(bytes []byte) (ProcessExecutionInfoJson, error) {
	var info ProcessExecutionInfoJson
	err := json.Unmarshal(bytes, &info)
	return info, err
}

type StateExecutionIdSequenceJson struct {
	SequenceMap map[string]int `json:"sequenceMap"`
}

func FromSequenceMapToBytes(sequenceMap map[string]int) ([]byte, error) {
	return json.Marshal(StateExecutionIdSequenceJson{
		SequenceMap: sequenceMap,
	})
}

type AsyncStateExecutionInfoJson struct {
	ProcessId   string `json:"processId"`
	ProcessType string `json:"processType"`
	WorkerURL   string `json:"workerURL"`
}

func FromStartRequestToStateInfoBytes(req xdbapi.ProcessExecutionStartRequest) ([]byte, error) {
	return json.Marshal(AsyncStateExecutionInfoJson{
		ProcessId:   req.ProcessId,
		ProcessType: req.GetProcessType(),
		WorkerURL:   req.GetWorkerUrl(),
	})
}
