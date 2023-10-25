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
	"time"

	"github.com/xdblab/xdb-apis/goapi/xdbapi"
	"github.com/xdblab/xdb/common/ptr"
	"github.com/xdblab/xdb/common/uuid"
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

type InternalLocalQueueMessage struct {
	DedupId string
	IsFull  bool // only false for now until we support including payload
}

type StateExecutionWaitingQueuesJson struct {
	// { state_execution_id_1: [ (queue_name_1, count_1), (queue_name_2, count_2), ... ], ... }
	StateToLocalQueueCommandsMap map[string][]xdbapi.LocalQueueCommand `json:"stateToLocalQueueCommandsMap"`
	// { queue_name_1: [dedupId_1, dedupId_2, ...], queue_name_2: [dedup_id, ...], ... }
	UnconsumedLocalQueueMessages map[string][]InternalLocalQueueMessage `json:"unconsumedLocalQueueMessages"`
}

func NewStateExecutionWaitingQueues() StateExecutionWaitingQueuesJson {
	return StateExecutionWaitingQueuesJson{
		StateToLocalQueueCommandsMap: map[string][]xdbapi.LocalQueueCommand{},
		UnconsumedLocalQueueMessages: map[string][]InternalLocalQueueMessage{},
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

func (s *StateExecutionWaitingQueuesJson) AddNewLocalQueueCommand(
	stateExecutionId StateExecutionId, command xdbapi.LocalQueueCommand) {
	if command.GetCount() == 0 {
		command.Count = xdbapi.PtrInt32(1)
	}

	stateExecutionIdKey := stateExecutionId.GetStateExecutionId()

	s.StateToLocalQueueCommandsMap[stateExecutionIdKey] = append(s.StateToLocalQueueCommandsMap[stateExecutionIdKey], command)
}

// AddMessageAndTryConsume returns (StateExecutionId string, InternalLocalQueueMessages) where the StateExecutionId consumes these messages.
//
// E.g., given StateToLocalQueueCommandsMap as:
//
//	state_1, 1: (q1, 2),
//	state_1, 2: (q2, 3),
//	state_3, 1: (q1: 1), (q2, 2)
//
// If receiving the queue `q1`, then state_3, 1 will consume the queue `q1`, and StateToLocalQueueCommandsMap becomes:
//
//	state_1, 1: (q1, 2),
//	state_1, 2: (q2, 3),
//	state_3, 1: (q2, 2)
//
// If receiving the queue `q2` for twice, then state_3, 1 will consume the two queue `q2`, and StateToLocalQueueCommandsMap becomes:
//
//	state_1, 1: (q1, 2),
//	state_1, 2: (q2, 3),
func (s *StateExecutionWaitingQueuesJson) AddMessageAndTryConsume(message LocalQueueMessageInfoJson) (string, []InternalLocalQueueMessage) {
	s.UnconsumedLocalQueueMessages[message.QueueName] = append(
		s.UnconsumedLocalQueueMessages[message.QueueName], InternalLocalQueueMessage{
			DedupId: message.DedupId.String(), IsFull: false,
		})

	for stateExecutionIdKey, commands := range s.StateToLocalQueueCommandsMap {
		for i, command := range commands {
			if command.GetQueueName() != message.QueueName || int(command.GetCount()) > len(s.UnconsumedLocalQueueMessages[message.QueueName]) {
				continue
			}

			// the method will return results immediately, so handling the manipulation directly here
			consumedInternalLocalQueueMessages := s.UnconsumedLocalQueueMessages[message.QueueName][:int(command.GetCount())]

			s.UnconsumedLocalQueueMessages[message.QueueName] =
				s.UnconsumedLocalQueueMessages[message.QueueName][int(command.GetCount()):]
			if len(s.UnconsumedLocalQueueMessages[message.QueueName]) == 0 {
				delete(s.UnconsumedLocalQueueMessages, message.QueueName)
			}

			s.StateToLocalQueueCommandsMap[stateExecutionIdKey] = append(s.StateToLocalQueueCommandsMap[stateExecutionIdKey][:i],
				s.StateToLocalQueueCommandsMap[stateExecutionIdKey][i+1:]...)
			if len(s.StateToLocalQueueCommandsMap[stateExecutionIdKey]) == 0 {
				delete(s.StateToLocalQueueCommandsMap, stateExecutionIdKey)
			}

			return stateExecutionIdKey, consumedInternalLocalQueueMessages
		}
	}

	return "", nil
}

// ConsumeWithCheckingLocalQueueWaitingComplete returns a bool indicating if the stateExecutionId can complete the local queue commands waiting,
// and an array of all the consumed messages.
//
// E.g., given UnconsumedLocalQueueMessages as:
//
// q1: [id_1_1, id_1_2], q2: [id_2_1, id_2_2], q3: [id_3_1]
//
// and StateToLocalQueueCommandsMap[stateExecutionId] as:
//
// (q1, 1), (q2, 2)
//
// and xdbapi.CommandWaitingType is ALL_OF_COMPLETION. Then after ConsumeWithCheckingLocalQueueWaitingComplete,
// the UnconsumedLocalQueueMessages becomes:
//
// q1: [id_1_2], q3: [id_3_1]
//
// and returns:
//
// (true, [(id_1_1, false), (id_2_1, false), (id_2_2, false)])
func (s *StateExecutionWaitingQueuesJson) ConsumeWithCheckingLocalQueueWaitingComplete(stateExecutionId StateExecutionId,
	commandWaitingType xdbapi.CommandWaitingType) (bool, []InternalLocalQueueMessage) {
	stateExecutionIdKey := stateExecutionId.GetStateExecutionId()

	var remainingCommands []xdbapi.LocalQueueCommand
	var consumedMessages []InternalLocalQueueMessage

	idx := 0

	for i, command := range s.StateToLocalQueueCommandsMap[stateExecutionIdKey] {
		idx = i

		messages, ok := s.UnconsumedLocalQueueMessages[command.GetQueueName()]

		if !ok || int(command.GetCount()) > len(messages) {
			remainingCommands = append(remainingCommands, command)
			continue
		}

		consumedMessages = append(consumedMessages, s.UnconsumedLocalQueueMessages[command.GetQueueName()][:int(command.GetCount())]...)

		s.UnconsumedLocalQueueMessages[command.GetQueueName()] = s.UnconsumedLocalQueueMessages[command.GetQueueName()][int(command.GetCount()):]
		if len(s.UnconsumedLocalQueueMessages[command.GetQueueName()]) == 0 {
			delete(s.UnconsumedLocalQueueMessages, command.GetQueueName())
		}

		if xdbapi.ANY_OF_COMPLETION == commandWaitingType {
			break
		}
	}

	if idx < len(s.StateToLocalQueueCommandsMap[stateExecutionIdKey]) {
		remainingCommands = append(remainingCommands, s.StateToLocalQueueCommandsMap[stateExecutionIdKey][idx+1:]...)
	}

	if len(remainingCommands) == len(s.StateToLocalQueueCommandsMap[stateExecutionIdKey]) {
		return false, consumedMessages
	}

	s.StateToLocalQueueCommandsMap[stateExecutionIdKey] = remainingCommands
	if len(s.StateToLocalQueueCommandsMap[stateExecutionIdKey]) == 0 {
		delete(s.StateToLocalQueueCommandsMap, stateExecutionIdKey)
	}

	if xdbapi.ANY_OF_COMPLETION == commandWaitingType {
		s.CleanupFor(stateExecutionId)
		return true, consumedMessages
	}

	return len(remainingCommands) == 0, consumedMessages
}

func (s *StateExecutionWaitingQueuesJson) CleanupFor(stateExecutionId StateExecutionId) {
	stateExecutionIdKey := stateExecutionId.GetStateExecutionId()
	delete(s.StateToLocalQueueCommandsMap, stateExecutionIdKey)
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
	QueueName string    `json:"queueName"`
	DedupId   uuid.UUID `json:"dedupId"`
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

func CreateStateExecutionFailureBytesForBackoff(status int32, details string, completedAttempts int32) ([]byte, error) {
	obj := StateExecutionFailureJson{
		StatusCode:           &status,
		Details:              &details,
		CompletedAttempts:    &completedAttempts,
		LastAttemptTimestamp: ptr.Any(time.Now().Unix()),
	}
	return json.Marshal(obj)
}
