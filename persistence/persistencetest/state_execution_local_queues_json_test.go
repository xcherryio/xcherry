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

package tests

import (
	"github.com/stretchr/testify/assert"
	"github.com/xdblab/xdb-apis/goapi/xdbapi"
	"github.com/xdblab/xdb/common/ptr"
	"github.com/xdblab/xdb/common/uuid"
	"github.com/xdblab/xdb/persistence"
	"testing"
)

func TestStateExecutionLocalQueuesJsonConsume(t *testing.T) {
	uuid_q1_1 := uuid.MustNewUUID()
	uuid_q1_2 := uuid.MustNewUUID()
	uuid_q2_1 := uuid.MustNewUUID()
	uuid_q2_2 := uuid.MustNewUUID()

	stateExecutionLocalQueues := persistence.NewStateExecutionLocalQueues()
	prepareDataForConsume(stateExecutionLocalQueues)

	// Data:
	//	state_1, 1: (q1, 2),
	//	state_1, 2: (q2, 3),
	//	state_3, 1: (q1: 1), (q2, 2)

	completedStateExecutionIdString, idx, consumedMessages := stateExecutionLocalQueues.AddMessageAndTryConsume(persistence.LocalQueueMessageInfoJson{
		QueueName: "q1", DedupId: uuid_q1_1,
	})
	// The new data should be:
	//	state_1, 1: (q1, 2),
	//	state_1, 2: (q2, 3),
	//	state_3, 1: (q2, 2)
	assert.Equal(t, "state_3-1", completedStateExecutionIdString)
	assert.Equal(t, 0, idx)
	assert.Equal(t, []persistence.InternalLocalQueueMessage{{
		DedupId: uuid_q1_1.String(), IsFull: false,
	}}, consumedMessages)
	assert.Empty(t, stateExecutionLocalQueues.UnconsumedLocalQueueMessages)

	completedStateExecutionIdString, idx, consumedMessages = stateExecutionLocalQueues.AddMessageAndTryConsume(persistence.LocalQueueMessageInfoJson{
		QueueName: "q2", DedupId: uuid_q2_1,
	})
	// The data does not change:
	//	state_1, 1: (q1, 2),
	//	state_1, 2: (q2, 3),
	//	state_3, 1: (q2, 2)
	assert.Empty(t, completedStateExecutionIdString)
	assert.Equal(t, -1, idx)
	assert.Empty(t, consumedMessages)
	assert.Len(t, stateExecutionLocalQueues.UnconsumedLocalQueueMessages, 1)
	assert.Equal(t, []persistence.InternalLocalQueueMessage{{
		DedupId: uuid_q2_1.String(), IsFull: false,
	}}, stateExecutionLocalQueues.UnconsumedLocalQueueMessages["q2"])

	completedStateExecutionIdString, idx, consumedMessages = stateExecutionLocalQueues.AddMessageAndTryConsume(persistence.LocalQueueMessageInfoJson{
		QueueName: "q1", DedupId: uuid_q1_2,
	})
	// The data does not change:
	//	state_1, 1: (q1, 2),
	//	state_1, 2: (q2, 3),
	//	state_3, 1: (q2, 2)
	assert.Empty(t, completedStateExecutionIdString)
	assert.Equal(t, -1, idx)
	assert.Empty(t, consumedMessages)
	assert.Len(t, stateExecutionLocalQueues.UnconsumedLocalQueueMessages, 2)
	assert.Equal(t, []persistence.InternalLocalQueueMessage{{
		DedupId: uuid_q1_2.String(), IsFull: false,
	}}, stateExecutionLocalQueues.UnconsumedLocalQueueMessages["q1"])
	assert.Equal(t, []persistence.InternalLocalQueueMessage{{
		DedupId: uuid_q2_1.String(), IsFull: false,
	}}, stateExecutionLocalQueues.UnconsumedLocalQueueMessages["q2"])

	completedStateExecutionIdString, idx, consumedMessages = stateExecutionLocalQueues.AddMessageAndTryConsume(persistence.LocalQueueMessageInfoJson{
		QueueName: "q2", DedupId: uuid_q2_2,
	})
	// The new data should be:
	//	state_1, 1: (q1, 2),
	//	state_1, 2: (q2, 3),
	assert.Equal(t, "state_3-1", completedStateExecutionIdString)
	assert.Equal(t, 1, idx)
	assert.Equal(t, []persistence.InternalLocalQueueMessage{{
		DedupId: uuid_q2_1.String(), IsFull: false,
	}, {
		DedupId: uuid_q2_2.String(), IsFull: false,
	}}, consumedMessages)
	assert.Len(t, stateExecutionLocalQueues.UnconsumedLocalQueueMessages, 1)
	assert.Equal(t, []persistence.InternalLocalQueueMessage{{
		DedupId: uuid_q1_2.String(), IsFull: false,
	}}, stateExecutionLocalQueues.UnconsumedLocalQueueMessages["q1"])
}

func TestStateExecutionLocalQueuesJsonConsumeFor_All_consumed(t *testing.T) {
	uuids := []uuid.UUID{}
	uuid_q1_1 := uuid.MustNewUUID()
	uuid_q1_2 := uuid.MustNewUUID()
	uuid_q2_1 := uuid.MustNewUUID()
	uuid_q2_2 := uuid.MustNewUUID()
	uuid_q3_1 := uuid.MustNewUUID()
	uuids = append(uuids, uuid_q1_1)
	uuids = append(uuids, uuid_q1_2)
	uuids = append(uuids, uuid_q2_1)
	uuids = append(uuids, uuid_q2_2)
	uuids = append(uuids, uuid_q3_1)

	stateExecutionLocalQueues := persistence.NewStateExecutionLocalQueues()
	prepareDataForConsumeFor(stateExecutionLocalQueues, uuids)

	// Return UnconsumedMessageQueueCountMap as:
	//
	// (q1, 2), (q2, 2), (q3, 1)
	//
	// and StateToLocalQueueCommandsMap["state_1-1"] as:
	//
	// (q1, 1), (q2, 2)

	consumedMessages := stateExecutionLocalQueues.TryConsumeForStateExecution(persistence.StateExecutionId{
		StateId: "state_1", StateIdSequence: 1,
	}, xdbapi.ALL_OF_COMPLETION)

	// The new UnconsumedMessageQueueCountMap should be:
	//
	// (q1, 1) (q3, 1)
	//
	// and StateToLocalQueueCommandsMap["state_1-1"] was deleted.

	assert.Equal(t, map[int][]persistence.InternalLocalQueueMessage{
		0: {{
			DedupId: uuid_q1_1.String(), IsFull: false,
		}},
		1: {{
			DedupId: uuid_q2_1.String(), IsFull: false,
		}, {
			DedupId: uuid_q2_2.String(), IsFull: false,
		}}}, consumedMessages)
	assert.Len(t, stateExecutionLocalQueues.UnconsumedLocalQueueMessages, 2)
	assert.Equal(t, []persistence.InternalLocalQueueMessage{{
		DedupId: uuid_q1_2.String(), IsFull: false,
	}}, stateExecutionLocalQueues.UnconsumedLocalQueueMessages["q1"])
	assert.Equal(t, []persistence.InternalLocalQueueMessage{{
		DedupId: uuid_q3_1.String(), IsFull: false,
	}}, stateExecutionLocalQueues.UnconsumedLocalQueueMessages["q3"])
	assert.Empty(t, stateExecutionLocalQueues.StateToLocalQueueCommandsMap)
}

func TestStateExecutionLocalQueuesJsonConsumeFor_All_notAllConsumed(t *testing.T) {
	uuids := []uuid.UUID{}
	uuid_q1_1 := uuid.MustNewUUID()
	uuid_q1_2 := uuid.MustNewUUID()
	uuid_q2_1 := uuid.MustNewUUID()
	uuid_q2_2 := uuid.MustNewUUID()
	uuid_q3_1 := uuid.MustNewUUID()
	uuids = append(uuids, uuid_q1_1)
	uuids = append(uuids, uuid_q1_2)
	uuids = append(uuids, uuid_q2_1)
	uuids = append(uuids, uuid_q2_2)
	uuids = append(uuids, uuid_q3_1)

	stateExecutionLocalQueues := persistence.NewStateExecutionLocalQueues()
	prepareDataForConsumeFor(stateExecutionLocalQueues, uuids)

	stateExecutionLocalQueues.AddNewLocalQueueCommands(persistence.StateExecutionId{
		StateId: "state_1", StateIdSequence: 1,
	}, []xdbapi.LocalQueueCommand{
		{QueueName: "q3", Count: ptr.Any(int32(2))},
	})

	// Return UnconsumedMessageQueueCountMap as:
	//
	// (q1, 2), (q2, 2), (q3, 1)
	//
	// and StateToLocalQueueCommandsMap["state_1-1"] as:
	//
	// (q1, 1), (q2, 2), (q3, 2)

	consumedMessages := stateExecutionLocalQueues.TryConsumeForStateExecution(persistence.StateExecutionId{
		StateId: "state_1", StateIdSequence: 1,
	}, xdbapi.ALL_OF_COMPLETION)

	// The new UnconsumedMessageQueueCountMap should be:
	//
	// (q1, 1), (q3, 1)
	//
	// and StateToLocalQueueCommandsMap["state_1-1"] as:
	//
	// (q3, 2)

	assert.Equal(t, map[int][]persistence.InternalLocalQueueMessage{
		0: {{
			DedupId: uuid_q1_1.String(), IsFull: false,
		}},
		1: {{
			DedupId: uuid_q2_1.String(), IsFull: false,
		}, {
			DedupId: uuid_q2_2.String(), IsFull: false,
		}}}, consumedMessages)
	assert.Len(t, stateExecutionLocalQueues.UnconsumedLocalQueueMessages, 2)
	assert.Equal(t, []persistence.InternalLocalQueueMessage{{
		DedupId: uuid_q1_2.String(), IsFull: false,
	}}, stateExecutionLocalQueues.UnconsumedLocalQueueMessages["q1"])
	assert.Equal(t, []persistence.InternalLocalQueueMessage{{
		DedupId: uuid_q3_1.String(), IsFull: false,
	}}, stateExecutionLocalQueues.UnconsumedLocalQueueMessages["q3"])
	assert.Len(t, stateExecutionLocalQueues.StateToLocalQueueCommandsMap, 1)
	assert.Len(t, stateExecutionLocalQueues.StateToLocalQueueCommandsMap["state_1-1"], 1)
	assert.Equal(t, xdbapi.LocalQueueCommand{
		QueueName: "q3",
		Count:     xdbapi.PtrInt32(2),
	}, stateExecutionLocalQueues.StateToLocalQueueCommandsMap["state_1-1"][2])
}

func TestStateExecutionLocalQueuesJsonConsumeFor_Any_consumed(t *testing.T) {
	uuids := []uuid.UUID{}
	uuid_q1_1 := uuid.MustNewUUID()
	uuid_q1_2 := uuid.MustNewUUID()
	uuid_q2_1 := uuid.MustNewUUID()
	uuid_q2_2 := uuid.MustNewUUID()
	uuid_q3_1 := uuid.MustNewUUID()
	uuids = append(uuids, uuid_q1_1)
	uuids = append(uuids, uuid_q1_2)
	uuids = append(uuids, uuid_q2_1)
	uuids = append(uuids, uuid_q2_2)
	uuids = append(uuids, uuid_q3_1)

	stateExecutionLocalQueues := persistence.NewStateExecutionLocalQueues()
	prepareDataForConsumeFor(stateExecutionLocalQueues, uuids)

	// Return UnconsumedMessageQueueCountMap as:
	//
	// (q1, 2), (q2, 2), (q3, 1)
	//
	// and StateToLocalQueueCommandsMap["state_1-1"] as:
	//
	// (q1, 1), (q2, 2)

	consumedMessages := stateExecutionLocalQueues.TryConsumeForStateExecution(persistence.StateExecutionId{
		StateId: "state_1", StateIdSequence: 1,
	}, xdbapi.ANY_OF_COMPLETION)

	// The new UnconsumedMessageQueueCountMap should be:
	//
	// (q1, 1), (q2, 2), (q3, 1)
	//
	// and StateToLocalQueueCommandsMap["state_1-1"] as deleted.

	assert.Equal(t, map[int][]persistence.InternalLocalQueueMessage{
		0: {{
			DedupId: uuid_q1_1.String(), IsFull: false,
		}}}, consumedMessages)
	assert.Len(t, stateExecutionLocalQueues.UnconsumedLocalQueueMessages, 3)
	assert.Equal(t, []persistence.InternalLocalQueueMessage{{
		DedupId: uuid_q1_2.String(), IsFull: false,
	}}, stateExecutionLocalQueues.UnconsumedLocalQueueMessages["q1"])
	assert.Equal(t, []persistence.InternalLocalQueueMessage{{
		DedupId: uuid_q2_1.String(), IsFull: false,
	}, {
		DedupId: uuid_q2_2.String(), IsFull: false,
	}}, stateExecutionLocalQueues.UnconsumedLocalQueueMessages["q2"])
	assert.Equal(t, []persistence.InternalLocalQueueMessage{{
		DedupId: uuid_q3_1.String(), IsFull: false,
	}}, stateExecutionLocalQueues.UnconsumedLocalQueueMessages["q3"])
	assert.Empty(t, stateExecutionLocalQueues.StateToLocalQueueCommandsMap)
}

func TestStateExecutionLocalQueuesJsonConsumeFor_Any_notConsumed(t *testing.T) {
	uuid_q1_1 := uuid.MustNewUUID()

	stateExecutionLocalQueues := persistence.NewStateExecutionLocalQueues()

	completedStateExecutionIdString, idx, consumedMessages := stateExecutionLocalQueues.AddMessageAndTryConsume(persistence.LocalQueueMessageInfoJson{
		QueueName: "q1", DedupId: uuid_q1_1,
	})
	assert.Empty(t, completedStateExecutionIdString)
	assert.Equal(t, -1, idx)
	assert.Empty(t, consumedMessages)
	assert.Len(t, stateExecutionLocalQueues.UnconsumedLocalQueueMessages, 1)
	assert.Equal(t, []persistence.InternalLocalQueueMessage{{
		DedupId: uuid_q1_1.String(), IsFull: false,
	}}, stateExecutionLocalQueues.UnconsumedLocalQueueMessages["q1"])
	assert.Empty(t, stateExecutionLocalQueues.StateToLocalQueueCommandsMap)

	stateExecutionLocalQueues.AddNewLocalQueueCommands(persistence.StateExecutionId{
		StateId: "state_1", StateIdSequence: 1,
	}, []xdbapi.LocalQueueCommand{
		{QueueName: "q1", Count: ptr.Any(int32(2))}, {QueueName: "q2", Count: ptr.Any(int32(1))},
	})

	// Return UnconsumedMessageQueueCountMap as:
	//
	// (q1, 1)
	//
	// and StateToLocalQueueCommandsMap["state_1-1"] as:
	//
	// (q1, 2), (q2, 1)

	consumedMessagesMap := stateExecutionLocalQueues.TryConsumeForStateExecution(persistence.StateExecutionId{
		StateId: "state_1", StateIdSequence: 1,
	}, xdbapi.ANY_OF_COMPLETION)

	assert.Empty(t, consumedMessagesMap)
	assert.Len(t, stateExecutionLocalQueues.UnconsumedLocalQueueMessages, 1)
	assert.Equal(t, []persistence.InternalLocalQueueMessage{{
		DedupId: uuid_q1_1.String(), IsFull: false,
	}}, stateExecutionLocalQueues.UnconsumedLocalQueueMessages["q1"])
	assert.Len(t, stateExecutionLocalQueues.StateToLocalQueueCommandsMap, 1)
	assert.Len(t, stateExecutionLocalQueues.StateToLocalQueueCommandsMap["state_1-1"], 2)
	assert.Equal(t, xdbapi.LocalQueueCommand{
		QueueName: "q1",
		Count:     xdbapi.PtrInt32(2),
	}, stateExecutionLocalQueues.StateToLocalQueueCommandsMap["state_1-1"][0])
	assert.Equal(t, xdbapi.LocalQueueCommand{
		QueueName: "q2",
		Count:     xdbapi.PtrInt32(1),
	}, stateExecutionLocalQueues.StateToLocalQueueCommandsMap["state_1-1"][1])
}

// Return:
//
//	state_1, 1: (q1, 2),
//	state_1, 2: (q2, 3),
//	state_3, 1: (q1: 1), (q2, 2)
func prepareDataForConsume(stateExecutionLocalQueues persistence.StateExecutionLocalQueuesJson) {
	stateExecutionLocalQueues.AddNewLocalQueueCommands(persistence.StateExecutionId{
		StateId: "state_1", StateIdSequence: 1,
	}, []xdbapi.LocalQueueCommand{
		{QueueName: "q1", Count: ptr.Any(int32(2))},
	})

	stateExecutionLocalQueues.AddNewLocalQueueCommands(persistence.StateExecutionId{
		StateId: "state_1", StateIdSequence: 2,
	}, []xdbapi.LocalQueueCommand{
		{QueueName: "q2", Count: ptr.Any(int32(3))},
	})

	stateExecutionLocalQueues.AddNewLocalQueueCommands(persistence.StateExecutionId{
		StateId: "state_3", StateIdSequence: 1,
	}, []xdbapi.LocalQueueCommand{
		{QueueName: "q1"}, {QueueName: "q2", Count: ptr.Any(int32(2))},
	})
}

// Return UnconsumedMessageQueueCountMap as:
//
// (q1, 2), (q2, 2), (q3, 1)
//
// and StateToLocalQueueCommandsMap["state_1-1"] as:
//
// (q1, 1), (q2, 2)
func prepareDataForConsumeFor(stateExecutionLocalQueues persistence.StateExecutionLocalQueuesJson, dedupIds []uuid.UUID) {
	stateExecutionLocalQueues.AddMessageAndTryConsume(persistence.LocalQueueMessageInfoJson{
		QueueName: "q1", DedupId: dedupIds[0],
	})
	stateExecutionLocalQueues.AddMessageAndTryConsume(persistence.LocalQueueMessageInfoJson{
		QueueName: "q1", DedupId: dedupIds[1],
	})
	stateExecutionLocalQueues.AddMessageAndTryConsume(persistence.LocalQueueMessageInfoJson{
		QueueName: "q2", DedupId: dedupIds[2],
	})
	stateExecutionLocalQueues.AddMessageAndTryConsume(persistence.LocalQueueMessageInfoJson{
		QueueName: "q2", DedupId: dedupIds[3],
	})
	stateExecutionLocalQueues.AddMessageAndTryConsume(persistence.LocalQueueMessageInfoJson{
		QueueName: "q3", DedupId: dedupIds[4],
	})

	stateExecutionLocalQueues.AddNewLocalQueueCommands(persistence.StateExecutionId{
		StateId: "state_1", StateIdSequence: 1,
	}, []xdbapi.LocalQueueCommand{
		{QueueName: "q1", Count: ptr.Any(int32(1))}, {QueueName: "q2", Count: ptr.Any(int32(2))},
	})
}
