// Copyright (c) XDBLab
// SPDX-License-Identifier: BUSL-1.1

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
	ProcessType           string                         `json:"processType"`
	WorkerURL             string                         `json:"workerURL"`
	GlobalAttributeConfig *InternalGlobalAttributeConfig `json:"globalAttributeConfig"`
}

func FromStartRequestToProcessInfoBytes(req xdbapi.ProcessExecutionStartRequest) ([]byte, error) {
	info := ProcessExecutionInfoJson{
		ProcessType:           req.GetProcessType(),
		WorkerURL:             req.GetWorkerUrl(),
		GlobalAttributeConfig: getInternalGlobalAttributeConfig(req),
	}
	return json.Marshal(info)
}

func getInternalGlobalAttributeConfig(req xdbapi.ProcessExecutionStartRequest) *InternalGlobalAttributeConfig {
	if req.ProcessStartConfig != nil && req.ProcessStartConfig.GlobalAttributeConfig != nil {
		primaryKeys := map[string]xdbapi.TableColumnValue{}
		for _, cfg := range req.ProcessStartConfig.GlobalAttributeConfig.TableConfigs {
			primaryKeys[cfg.TableName] = cfg.PrimaryKey
		}
		return &InternalGlobalAttributeConfig{
			TablePrimaryKeys: primaryKeys,
		}
	}
	return nil
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

type StateExecutionLocalQueuesJson struct {
	// { state_execution_id_1: { 1: (queue_name_2, count_2), ... }, ... }
	StateToLocalQueueCommandsMap map[string]map[int]xdbapi.LocalQueueCommand `json:"stateToLocalQueueCommandsMap"`
	// { queue_name_1: [dedupId_1, dedupId_2, ...], queue_name_2: [dedup_id, ...], ... }
	UnconsumedLocalQueueMessages map[string][]InternalLocalQueueMessage `json:"unconsumedLocalQueueMessages"`
}

func NewStateExecutionLocalQueues() StateExecutionLocalQueuesJson {
	return StateExecutionLocalQueuesJson{
		StateToLocalQueueCommandsMap: map[string]map[int]xdbapi.LocalQueueCommand{},
		UnconsumedLocalQueueMessages: map[string][]InternalLocalQueueMessage{},
	}
}

func (s *StateExecutionLocalQueuesJson) ToBytes() ([]byte, error) {
	return json.Marshal(s)
}

func NewStateExecutionLocalQueuesFromBytes(bytes []byte) (StateExecutionLocalQueuesJson, error) {
	var localQueuesJson StateExecutionLocalQueuesJson
	err := json.Unmarshal(bytes, &localQueuesJson)
	return localQueuesJson, err
}

func (s *StateExecutionLocalQueuesJson) AddNewLocalQueueCommands(
	stateExecutionId StateExecutionId, localQueueCommands []xdbapi.LocalQueueCommand,
) {
	stateExecutionIdKey := stateExecutionId.GetStateExecutionId()
	_, ok := s.StateToLocalQueueCommandsMap[stateExecutionIdKey]
	if !ok {
		s.StateToLocalQueueCommandsMap[stateExecutionIdKey] = map[int]xdbapi.LocalQueueCommand{}
	}

	for idx, command := range localQueueCommands {
		if command.GetCount() == 0 {
			command.Count = xdbapi.PtrInt32(1)
		}

		s.StateToLocalQueueCommandsMap[stateExecutionIdKey][idx] = command
	}
}

// AddMessageAndTryConsume returns (StateExecutionId string, index of the command, InternalLocalQueueMessages)
// where the StateExecutionId consumes these messages for the command.
//
// E.g., given StateToLocalQueueCommandsMap as:
//
//	state_1, 1: 0: (q1, 2),
//	state_1, 2: 0: (q2, 3),
//	state_3, 1: 0: (q1: 1), 1: (q2, 2)
//
// If receiving the queue `q1`, then state_3, 1 will consume the queue `q1`, and StateToLocalQueueCommandsMap becomes:
//
//	state_1, 1: 0: (q1, 2),
//	state_1, 2: 0: (q2, 3),
//	state_3, 1: 1: (q2, 2)
//
// If receiving the queue `q2` for twice, then state_3, 1 will consume the two queue `q2`, and StateToLocalQueueCommandsMap becomes:
//
//	state_1, 1: 0: (q1, 2),
//	state_1, 2: 0: (q2, 3),
func (s *StateExecutionLocalQueuesJson) AddMessageAndTryConsume(message LocalQueueMessageInfoJson) (string, int, []InternalLocalQueueMessage) {
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

			delete(s.StateToLocalQueueCommandsMap[stateExecutionIdKey], i)
			if len(s.StateToLocalQueueCommandsMap[stateExecutionIdKey]) == 0 {
				delete(s.StateToLocalQueueCommandsMap, stateExecutionIdKey)
			}

			return stateExecutionIdKey, i, consumedInternalLocalQueueMessages
		}
	}

	return "", -1, nil
}

// TryConsumeForStateExecution returns a map with key as the command index, and value as an array of all the consumed messages of that command.
//
// E.g., given UnconsumedLocalQueueMessages as:
//
// q1: [id_1_1, id_1_2], q2: [id_2_1, id_2_2], q3: [id_3_1]
//
// and StateToLocalQueueCommandsMap[stateExecutionId] as:
//
// 0: (q1, 1), 1: (q2, 2)
//
// and xdbapi.CommandWaitingType is ALL_OF_COMPLETION. Then after TryConsumeForStateExecution,
// the UnconsumedLocalQueueMessages becomes:
//
// q1: [id_1_2], q3: [id_3_1]
//
// and returns:
//
// { 0: [(id_1_1, false)], 1: [(id_2_1, false), (id_2_2, false)] }
func (s *StateExecutionLocalQueuesJson) TryConsumeForStateExecution(
	stateExecutionId StateExecutionId,
	commandWaitingType xdbapi.CommandWaitingType,
) map[int][]InternalLocalQueueMessage {
	stateExecutionIdKey := stateExecutionId.GetStateExecutionId()

	// command_index: {...}
	remainingCommands := map[int]xdbapi.LocalQueueCommand{}
	consumedMessages := map[int][]InternalLocalQueueMessage{}

	idx := 0

	for i, command := range s.StateToLocalQueueCommandsMap[stateExecutionIdKey] {
		idx = i

		messages, ok := s.UnconsumedLocalQueueMessages[command.GetQueueName()]

		if !ok || int(command.GetCount()) > len(messages) {
			remainingCommands[i] = command
			continue
		}

		consumedMessages[i] = s.UnconsumedLocalQueueMessages[command.GetQueueName()][:int(command.GetCount())]

		s.UnconsumedLocalQueueMessages[command.GetQueueName()] = s.UnconsumedLocalQueueMessages[command.GetQueueName()][int(command.GetCount()):]
		if len(s.UnconsumedLocalQueueMessages[command.GetQueueName()]) == 0 {
			delete(s.UnconsumedLocalQueueMessages, command.GetQueueName())
		}

		if xdbapi.ANY_OF_COMPLETION == commandWaitingType {
			break
		}
	}

	for idx+1 < len(s.StateToLocalQueueCommandsMap[stateExecutionIdKey]) {
		idx += 1
		remainingCommands[idx] = s.StateToLocalQueueCommandsMap[stateExecutionIdKey][idx]
	}

	if len(remainingCommands) == len(s.StateToLocalQueueCommandsMap[stateExecutionIdKey]) {
		return consumedMessages
	}

	s.StateToLocalQueueCommandsMap[stateExecutionIdKey] = remainingCommands
	if len(s.StateToLocalQueueCommandsMap[stateExecutionIdKey]) == 0 {
		delete(s.StateToLocalQueueCommandsMap, stateExecutionIdKey)
	}

	if xdbapi.ANY_OF_COMPLETION == commandWaitingType {
		s.CleanupFor(stateExecutionId)
	}

	return consumedMessages
}

func (s *StateExecutionLocalQueuesJson) CleanupFor(stateExecutionId StateExecutionId) {
	stateExecutionIdKey := stateExecutionId.GetStateExecutionId()
	delete(s.StateToLocalQueueCommandsMap, stateExecutionIdKey)
}

type CommandResultsJson struct {
	// if value is true, the timer was fired. Otherwise, the timer was skipped.
	TimerResults      map[int]bool                             `json:"timerResults"`
	LocalQueueResults map[int][]xdbapi.LocalQueueMessageResult `json:"localQueueResults"`
}

func NewCommandResultsJson() CommandResultsJson {
	return CommandResultsJson{
		TimerResults:      map[int]bool{},
		LocalQueueResults: map[int][]xdbapi.LocalQueueMessageResult{},
	}
}

type AsyncStateExecutionInfoJson struct {
	Namespace                   string                         `json:"namespace"`
	ProcessId                   string                         `json:"processId"`
	ProcessType                 string                         `json:"processType"`
	WorkerURL                   string                         `json:"workerURL"`
	StateConfig                 *xdbapi.AsyncStateConfig       `json:"stateConfig"`
	RecoverFromStateExecutionId *string                        `json:"recoverFromStateExecutionId,omitempty"`
	RecoverFromApi              *xdbapi.StateApiType           `json:"recoverFromApi,omitempty"`
	GlobalAttributeConfig       *InternalGlobalAttributeConfig `json:"globalAttributeConfig"`
}

func FromStartRequestToStateInfoBytes(req xdbapi.ProcessExecutionStartRequest) ([]byte, error) {

	return json.Marshal(AsyncStateExecutionInfoJson{
		Namespace:             req.Namespace,
		ProcessId:             req.ProcessId,
		ProcessType:           req.GetProcessType(),
		WorkerURL:             req.GetWorkerUrl(),
		StateConfig:           req.StartStateConfig,
		GlobalAttributeConfig: getInternalGlobalAttributeConfig(req),
	})
}

func FromAsyncStateExecutionInfoToBytesForNextState(
	info AsyncStateExecutionInfoJson,
	nextStateConfig *xdbapi.AsyncStateConfig,
) ([]byte, error) {
	info.StateConfig = nextStateConfig
	return json.Marshal(info)
}

func FromAsyncStateExecutionInfoToBytesForStateRecovery(
	info AsyncStateExecutionInfoJson,
	stateExeId string,
	api xdbapi.StateApiType,
) ([]byte, error) {
	info.RecoverFromStateExecutionId = &stateExeId
	info.RecoverFromApi = &api
	// TODO we need to clean up for the next state execution otherwise it will be carried over forever
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
	if bytes == nil {
		return xdbapi.CommandRequest{}, nil
	}

	var request xdbapi.CommandRequest
	err := json.Unmarshal(bytes, &request)
	return request, err
}

func FromCommandResultsJsonToBytes(result CommandResultsJson) ([]byte, error) {
	return json.Marshal(result)
}

func BytesToCommandResultsJson(bytes []byte) (CommandResultsJson, error) {
	var result CommandResultsJson
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
	TimerCommandIndex     int                        `json:"timerCommandIndex"`
}

func (s *TimerTaskInfoJson) ToBytes() ([]byte, error) {
	return json.Marshal(s)
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
	return obj.ToBytes()
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
