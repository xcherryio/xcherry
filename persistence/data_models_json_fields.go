package persistence

import (
	"encoding/json"
	"fmt"
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

type StateExecutionSequenceMapsJson struct {
	SequenceMap map[string]int `json:"sequenceMap"`
	// store what state execution IDs are currently running
	// [stateId] -> [stateIdSequence] -> true
	PendingExecutionMap map[string]map[int]bool `json:"pendingExecutionMap"`
}

func NewStateExecutionSequenceMapsFromBytes(bytes []byte) (StateExecutionSequenceMapsJson, error) {
	var seqMaps StateExecutionSequenceMapsJson
	err := json.Unmarshal(bytes, &seqMaps)
	return seqMaps, err
}

func NewStateExecutionSequenceMaps() StateExecutionSequenceMapsJson {
	return StateExecutionSequenceMapsJson{
		SequenceMap:         map[string]int{},
		PendingExecutionMap: map[string]map[int]bool{},
	}
}

func (s *StateExecutionSequenceMapsJson) ToBytes() ([]byte, error) {
	return json.Marshal(s)
}

func (s *StateExecutionSequenceMapsJson) StartNewStateExecution(stateId string) int {
	s.SequenceMap[stateId]++
	seqId := s.SequenceMap[stateId]
	stateMap, ok := s.PendingExecutionMap[stateId]
	if ok {
		stateMap[seqId] = true
	} else {
		stateMap = map[int]bool{
			seqId: true,
		}
	}
	s.PendingExecutionMap[stateId] = stateMap
	return seqId
}

func (s *StateExecutionSequenceMapsJson) CompleteNewStateExecution(stateId string, stateSeq int) error {
	stateMap, ok := s.PendingExecutionMap[stateId]
	if !ok || !stateMap[stateSeq] {
		return fmt.Errorf("the state is not started, all current running states: %v", stateMap)
	}
	delete(stateMap, stateSeq)
	return nil
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

func FromAsyncStateExecutionInfoToBytes(info AsyncStateExecutionInfoJson) ([]byte, error) {
	return json.Marshal(info)
}

func BytesToAsyncStateExecutionInfo(bytes []byte) (AsyncStateExecutionInfoJson, error) {
	var info AsyncStateExecutionInfoJson
	err := json.Unmarshal(bytes, &info)
	return info, err
}

func FromEncodedObjectIntoBytes(obj *xdbapi.EncodedObject) ([]byte, error) {
	return json.Marshal(obj)
}

func BytesToEncodedObject(bytes []byte) (xdbapi.EncodedObject, error) {
	var obj xdbapi.EncodedObject
	err := json.Unmarshal(bytes, &obj)
	return obj, err
}
