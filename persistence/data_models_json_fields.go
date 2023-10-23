// Copyright 2023 XDBLab organization
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package persistence

import (
	"encoding/json"
	"fmt"
	"github.com/xdblab/xdb-apis/goapi/xdbapi"
	"github.com/xdblab/xdb/common/ptr"
	"github.com/xdblab/xdb/common/uuid"
	"sort"
	"time"
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
	pendingMap, ok := s.PendingExecutionMap[stateId]
	if !ok || !pendingMap[stateSeq] {
		return fmt.Errorf("the state is not started, all current running states: %v", pendingMap)
	}
	delete(pendingMap, stateSeq)
	if len(pendingMap) == 0 {
		delete(s.PendingExecutionMap, stateId)
	}
	return nil
}

type StateExecutionWaitingQueuesJson struct {
	// { queue_1: {(state_1, state_1_seq): waiting_count, (state_2, state_2_seq): waiting_count, ...} }
	QueueToStatesMap map[string]map[string]int `json:"queueToStatesMap"`
	// { (state_1, state_1_seq): total_waiting_count, (state_2, state_2_seq): total_waiting_count, ...} }
	StateToQueueCountMap map[string]int `json:"stateToQueueCountMap"`
}

func NewStateExecutionWaitingQueues() StateExecutionWaitingQueuesJson {
	return StateExecutionWaitingQueuesJson{
		QueueToStatesMap:     map[string]map[string]int{},
		StateToQueueCountMap: map[string]int{},
	}
}

func (s *StateExecutionWaitingQueuesJson) ToBytes() ([]byte, error) {
	return json.Marshal(s)
}

func NewStateExecutionWaitingQueuesFromBytes(bytes []byte) (StateExecutionWaitingQueuesJson, error) {
	var waitingQueuesJson StateExecutionWaitingQueuesJson
	err := json.Unmarshal(bytes, &waitingQueuesJson)
	return waitingQueuesJson, err
}

func (s *StateExecutionWaitingQueuesJson) AddNewLocalQueueCommandForStateExecution(
	stateExecutionId StateExecutionId, command xdbapi.LocalQueueCommand, anyOfCompletion bool) {
	count := command.GetCount()
	if count == 0 {
		count = 1
	}

	stateExecutionIdKey := stateExecutionId.GetStateExecutionId()

	m, ok := s.QueueToStatesMap[command.GetQueueName()]
	if !ok {
		m = map[string]int{}
	}

	if anyOfCompletion {
		m[stateExecutionIdKey] = 1
	} else {
		m[stateExecutionIdKey] += int(count)
	}
	s.QueueToStatesMap[command.GetQueueName()] = m

	if anyOfCompletion {
		s.StateToQueueCountMap[stateExecutionIdKey] = 1
	} else {
		s.StateToQueueCountMap[stateExecutionIdKey] += int(count)
	}
}

// Consume return assigned StateExecutionId string and a boolean indicating whether the assigned StateExecutionId has finished waiting
func (s *StateExecutionWaitingQueuesJson) Consume(message xdbapi.LocalQueueMessage) (*string, bool) {
	m, ok := s.QueueToStatesMap[message.GetQueueName()]
	if !ok {
		return nil, false
	}

	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}

	// E.g., given m as:
	//	state_1, 1: (q1, 1), (q2, 2)
	//	state_1, 2: (q1, 2)
	//	state_3, 1: (q1: 1)
	// If receiving the queue `q1`, then state_3, 1 will consume the queue `q1`, and the m becomes:
	//	state_1, 1: (q1, 1), (q2, 2)
	//	state_1, 2: (q1, 2)
	// If receiving the queue `q1` again, then state_1, 1 will consume the queue `q1`, and the m becomes:
	//	state_1, 1: (q2, 2)
	//	state_1, 2: (q1, 2)
	sort.Slice(keys, func(i, j int) bool {
		return m[keys[i]] < m[keys[j]] || (m[keys[i]] == m[keys[j]] && s.StateToQueueCountMap[keys[i]] < s.StateToQueueCountMap[keys[j]])
	})

	assignedStateExecutionIdString := keys[0]
	hasFinishedWaiting := false

	m[assignedStateExecutionIdString] -= 1
	if m[assignedStateExecutionIdString] == 0 {
		delete(m, assignedStateExecutionIdString)
	}
	s.QueueToStatesMap[message.GetQueueName()] = m
	if len(s.QueueToStatesMap[message.GetQueueName()]) == 0 {
		delete(s.QueueToStatesMap, message.GetQueueName())
	}

	s.StateToQueueCountMap[assignedStateExecutionIdString] -= 1
	hasFinishedWaiting = s.StateToQueueCountMap[assignedStateExecutionIdString] == 0

	if hasFinishedWaiting {
		s.CleanupForCompletion(assignedStateExecutionIdString)
	}

	return &assignedStateExecutionIdString, hasFinishedWaiting
}

func (s *StateExecutionWaitingQueuesJson) CleanupForCompletion(stateExecutionIdString string) {
	delete(s.StateToQueueCountMap, stateExecutionIdString)

	// if the completion type is ANY_OF_COMPLETION, delete the stateExecutionId from each queue map
	for k := range s.QueueToStatesMap {
		delete(s.QueueToStatesMap[k], stateExecutionIdString)
	}
}

type AsyncStateExecutionInfoJson struct {
	Namespace   string                   `json:"namespace"`
	ProcessId   string                   `json:"processId"`
	ProcessType string                   `json:"processType"`
	WorkerURL   string                   `json:"workerURL"`
	StateConfig *xdbapi.AsyncStateConfig `json:"stateConfig"`
}

func FromStartRequestToStateInfoBytes(req xdbapi.ProcessExecutionStartRequest) ([]byte, error) {
	return json.Marshal(AsyncStateExecutionInfoJson{
		Namespace:   req.Namespace,
		ProcessId:   req.ProcessId,
		ProcessType: req.GetProcessType(),
		WorkerURL:   req.GetWorkerUrl(),
		StateConfig: req.StartStateConfig,
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

func FromCommandRequestToBytes(request xdbapi.CommandRequest) ([]byte, error) {
	return json.Marshal(request)
}

func BytesToCommandRequest(bytes []byte) (xdbapi.CommandRequest, error) {
	var request xdbapi.CommandRequest
	err := json.Unmarshal(bytes, &request)
	return request, err
}

func FromCommandResultsToBytes(result xdbapi.CommandResults) ([]byte, error) {
	return json.Marshal(result)
}

func BytesToCommandResults(bytes []byte) (xdbapi.CommandResults, error) {
	var result xdbapi.CommandResults
	err := json.Unmarshal(bytes, &result)
	return result, err
}

type WorkerTaskBackoffInfoJson struct {
	// CompletedAttempts is the number of attempts that have been completed
	// for calculating next backoff interval
	CompletedAttempts int32 `json:"completedAttempts"`
	// FirstAttemptTimestampSeconds is the timestamp of the first attempt
	// for calculating next backoff interval
	FirstAttemptTimestampSeconds int64 `json:"firstAttemptTimestampSeconds"`
}

type LocalQueueMessageInfoJson struct {
	QueueName string               `json:"queueName"`
	DedupId   uuid.UUID            `json:"dedupId"`
	Payload   xdbapi.EncodedObject `json:"payload"`
}

type ImmediateTaskInfoJson struct {
	// used when the `task_type` is waitUntil or execute
	WorkerTaskBackoffInfo *WorkerTaskBackoffInfoJson `json:"workerTaskBackoffInfo"`
	// used when the `task_type` is localQueueMessage
	LocalQueueMessageInfo []LocalQueueMessageInfoJson `json:"localQueueMessageInfo"`
}

func BytesToImmediateTaskInfo(bytes []byte) (ImmediateTaskInfoJson, error) {
	var obj ImmediateTaskInfoJson
	err := json.Unmarshal(bytes, &obj)
	return obj, err
}

func FromImmediateTaskInfoIntoBytes(obj ImmediateTaskInfoJson) ([]byte, error) {
	return json.Marshal(obj)
}

type TimerTaskInfoJson struct {
	WorkerTaskBackoffInfo *WorkerTaskBackoffInfoJson `json:"workerTaskBackoffInfo"`
	WorkerTaskType        *ImmediateTaskType         `json:"workerTaskType"`
}

func BytesToTimerTaskInfo(bytes []byte) (TimerTaskInfoJson, error) {
	var obj TimerTaskInfoJson
	err := json.Unmarshal(bytes, &obj)
	return obj, err
}

func CreateTimerTaskInfoBytes(backoff *WorkerTaskBackoffInfoJson, taskType *ImmediateTaskType) ([]byte, error) {
	obj := TimerTaskInfoJson{
		WorkerTaskBackoffInfo: backoff,
		WorkerTaskType:        taskType,
	}
	return json.Marshal(obj)
}

type StateExecutionFailureJson struct {
	StatusCode           *int32  `json:"statusCode"`
	Details              *string `json:"details"`
	CompletedAttempts    *int32  `json:"completedAttempts"`
	LastAttemptTimestamp *int64  `json:"lastAttemptTimestamp"`
}

func BytesToStateExecutionFailure(bytes []byte) (StateExecutionFailureJson, error) {
	var obj StateExecutionFailureJson
	if len(bytes) == 0 {
		return obj, nil
	}

	err := json.Unmarshal(bytes, &obj)
	return obj, err
}

func CreateStateExecutionFailureBytesForBackoff(status int32, details string, completedAttempts int32) ([]byte, error) {
	obj := StateExecutionFailureJson{
		StatusCode:           &status,
		Details:              &details,
		CompletedAttempts:    &completedAttempts,
		LastAttemptTimestamp: ptr.Any(time.Now().Unix()),
	}
	return json.Marshal(obj)
}
