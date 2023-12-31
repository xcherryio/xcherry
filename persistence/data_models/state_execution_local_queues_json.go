// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: Apache-2.0

package data_models

import (
	"encoding/json"
	"github.com/xcherryio/apis/goapi/xcapi"
)

type StateExecutionLocalQueuesJson struct {
	// { state_execution_id_1: { 1: (queue_name_2, count_2), ... }, ... }
	StateToLocalQueueCommandsMap map[string]map[int]xcapi.LocalQueueCommand `json:"stateToLocalQueueCommandsMap"`
	// { queue_name_1: [dedupId_1, dedupId_2, ...], queue_name_2: [dedup_id, ...], ... }
	UnconsumedLocalQueueMessages map[string][]InternalLocalQueueMessage `json:"unconsumedLocalQueueMessages"`
}

func NewStateExecutionLocalQueues() StateExecutionLocalQueuesJson {
	return StateExecutionLocalQueuesJson{
		StateToLocalQueueCommandsMap: map[string]map[int]xcapi.LocalQueueCommand{},
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
	stateExecutionId StateExecutionId, localQueueCommands []xcapi.LocalQueueCommand,
) {
	stateExecutionIdKey := stateExecutionId.GetStateExecutionId()
	_, ok := s.StateToLocalQueueCommandsMap[stateExecutionIdKey]
	if !ok {
		s.StateToLocalQueueCommandsMap[stateExecutionIdKey] = map[int]xcapi.LocalQueueCommand{}
	}

	for idx, command := range localQueueCommands {
		if command.GetCount() == 0 {
			command.Count = xcapi.PtrInt32(1)
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
// and xcapi.CommandWaitingType is ALL_OF_COMPLETION. Then after TryConsumeForStateExecution,
// the UnconsumedLocalQueueMessages becomes:
//
// q1: [id_1_2], q3: [id_3_1]
//
// and returns:
//
// { 0: [(id_1_1, false)], 1: [(id_2_1, false), (id_2_2, false)] }
func (s *StateExecutionLocalQueuesJson) TryConsumeForStateExecution(
	stateExecutionId StateExecutionId,
	commandWaitingType xcapi.CommandWaitingType,
) map[int][]InternalLocalQueueMessage {
	stateExecutionIdKey := stateExecutionId.GetStateExecutionId()

	// command_index: {...}
	remainingCommands := map[int]xcapi.LocalQueueCommand{}
	consumedMessages := map[int][]InternalLocalQueueMessage{}

	stopConsume := false

	for i, command := range s.StateToLocalQueueCommandsMap[stateExecutionIdKey] {
		messages, ok := s.UnconsumedLocalQueueMessages[command.GetQueueName()]

		if stopConsume || !ok || int(command.GetCount()) > len(messages) {
			remainingCommands[i] = command
			continue
		}

		consumedMessages[i] = s.UnconsumedLocalQueueMessages[command.GetQueueName()][:int(command.GetCount())]

		s.UnconsumedLocalQueueMessages[command.GetQueueName()] = s.UnconsumedLocalQueueMessages[command.GetQueueName()][int(command.GetCount()):]
		if len(s.UnconsumedLocalQueueMessages[command.GetQueueName()]) == 0 {
			delete(s.UnconsumedLocalQueueMessages, command.GetQueueName())
		}

		if xcapi.ANY_OF_COMPLETION == commandWaitingType {
			stopConsume = true
		}
	}

	if len(remainingCommands) == len(s.StateToLocalQueueCommandsMap[stateExecutionIdKey]) {
		return consumedMessages
	}

	s.StateToLocalQueueCommandsMap[stateExecutionIdKey] = remainingCommands
	if len(s.StateToLocalQueueCommandsMap[stateExecutionIdKey]) == 0 {
		delete(s.StateToLocalQueueCommandsMap, stateExecutionIdKey)
	}

	if xcapi.ANY_OF_COMPLETION == commandWaitingType {
		s.CleanupFor(stateExecutionId)
	}

	return consumedMessages
}

func (s *StateExecutionLocalQueuesJson) CleanupFor(stateExecutionId StateExecutionId) {
	stateExecutionIdKey := stateExecutionId.GetStateExecutionId()
	delete(s.StateToLocalQueueCommandsMap, stateExecutionIdKey)
}
