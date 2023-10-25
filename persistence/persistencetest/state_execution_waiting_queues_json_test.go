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

func TestStateExecutionWaitingQueuesJsonConsume(t *testing.T) {
	uuid_q1_1 := uuid.MustNewUUID()
	uuid_q1_2 := uuid.MustNewUUID()
	uuid_q2_1 := uuid.MustNewUUID()
	uuid_q2_2 := uuid.MustNewUUID()

	stateExecutionWaitingQueues := persistence.NewStateExecutionWaitingQueues()
	prepareDataForConsume(stateExecutionWaitingQueues)

	// Data:
	//	state_1, 1: (q1, 2),
	//	state_1, 2: (q2, 3),
	//	state_3, 1: (q1: 1), (q2, 2)

	completedStateExecutionIdString, consumedMessages := stateExecutionWaitingQueues.Consume(persistence.LocalQueueMessageInfoJson{
		QueueName: "q1", DedupId: uuid_q1_1,
	})
	// The new data should be:
	//	state_1, 1: (q1, 2),
	//	state_1, 2: (q2, 3),
	//	state_3, 1: (q2, 2)
	assert.Equal(t, "state_3-1", completedStateExecutionIdString)
	assert.Equal(t, []persistence.InternalLocalQueueMessage{{
		DedupId: uuid_q1_1.String(), IsFull: false,
	}}, consumedMessages)
	assert.Empty(t, stateExecutionWaitingQueues.UnconsumedLocalQueueMessages)

	completedStateExecutionIdString, consumedMessages = stateExecutionWaitingQueues.Consume(persistence.LocalQueueMessageInfoJson{
		QueueName: "q2", DedupId: uuid_q2_1,
	})
	// The data does not change:
	//	state_1, 1: (q1, 2),
	//	state_1, 2: (q2, 3),
	//	state_3, 1: (q2, 2)
	assert.Empty(t, completedStateExecutionIdString)
	assert.Empty(t, consumedMessages)
	assert.Len(t, stateExecutionWaitingQueues.UnconsumedLocalQueueMessages, 1)
	assert.Equal(t, []persistence.InternalLocalQueueMessage{{
		DedupId: uuid_q2_1.String(), IsFull: false,
	}}, stateExecutionWaitingQueues.UnconsumedLocalQueueMessages["q2"])

	completedStateExecutionIdString, consumedMessages = stateExecutionWaitingQueues.Consume(persistence.LocalQueueMessageInfoJson{
		QueueName: "q1", DedupId: uuid_q1_2,
	})
	// The data does not change:
	//	state_1, 1: (q1, 2),
	//	state_1, 2: (q2, 3),
	//	state_3, 1: (q2, 2)
	assert.Empty(t, completedStateExecutionIdString)
	assert.Empty(t, consumedMessages)
	assert.Len(t, stateExecutionWaitingQueues.UnconsumedLocalQueueMessages, 2)
	assert.Equal(t, []persistence.InternalLocalQueueMessage{{
		DedupId: uuid_q1_2.String(), IsFull: false,
	}}, stateExecutionWaitingQueues.UnconsumedLocalQueueMessages["q1"])
	assert.Equal(t, []persistence.InternalLocalQueueMessage{{
		DedupId: uuid_q2_1.String(), IsFull: false,
	}}, stateExecutionWaitingQueues.UnconsumedLocalQueueMessages["q2"])

	completedStateExecutionIdString, consumedMessages = stateExecutionWaitingQueues.Consume(persistence.LocalQueueMessageInfoJson{
		QueueName: "q2", DedupId: uuid_q2_2,
	})
	// The new data should be:
	//	state_1, 1: (q1, 2),
	//	state_1, 2: (q2, 3),
	assert.Equal(t, "state_3-1", completedStateExecutionIdString)
	assert.Equal(t, []persistence.InternalLocalQueueMessage{{
		DedupId: uuid_q2_1.String(), IsFull: false,
	}, {
		DedupId: uuid_q2_2.String(), IsFull: false,
	}}, consumedMessages)
	assert.Len(t, stateExecutionWaitingQueues.UnconsumedLocalQueueMessages, 1)
	assert.Equal(t, []persistence.InternalLocalQueueMessage{{
		DedupId: uuid_q1_2.String(), IsFull: false,
	}}, stateExecutionWaitingQueues.UnconsumedLocalQueueMessages["q1"])
}

func TestStateExecutionWaitingQueuesJsonConsumeFor_All_consumed(t *testing.T) {
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

	stateExecutionWaitingQueues := persistence.NewStateExecutionWaitingQueues()
	prepareDataForConsumeFor(stateExecutionWaitingQueues, uuids)

	// Return UnconsumedMessageQueueCountMap as:
	//
	// (q1, 2), (q2, 2), (q3, 1)
	//
	// and StateToLocalQueueCommandsMap["state_1-1"] as:
	//
	// (q1, 1), (q2, 2)

	canComplete, consumedMessages := stateExecutionWaitingQueues.CheckCanCompleteLocalQueueWaiting(persistence.StateExecutionId{
		StateId: "state_1", StateIdSequence: 1,
	}, xdbapi.ALL_OF_COMPLETION)

	// The new UnconsumedMessageQueueCountMap should be:
	//
	// (q1, 1) (q3, 1)
	//
	// and StateToLocalQueueCommandsMap["state_1-1"] was deleted.

	assert.True(t, canComplete)
	assert.Equal(t, []persistence.InternalLocalQueueMessage{{
		DedupId: uuid_q1_1.String(), IsFull: false,
	}, {
		DedupId: uuid_q2_1.String(), IsFull: false,
	}, {
		DedupId: uuid_q2_2.String(), IsFull: false,
	}}, consumedMessages)
	assert.Len(t, stateExecutionWaitingQueues.UnconsumedLocalQueueMessages, 2)
	assert.Equal(t, []persistence.InternalLocalQueueMessage{{
		DedupId: uuid_q1_2.String(), IsFull: false,
	}}, stateExecutionWaitingQueues.UnconsumedLocalQueueMessages["q1"])
	assert.Equal(t, []persistence.InternalLocalQueueMessage{{
		DedupId: uuid_q3_1.String(), IsFull: false,
	}}, stateExecutionWaitingQueues.UnconsumedLocalQueueMessages["q3"])
	assert.Empty(t, stateExecutionWaitingQueues.StateToLocalQueueCommandsMap)
}

func TestStateExecutionWaitingQueuesJsonConsumeFor_All_notAllConsumed(t *testing.T) {
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

	stateExecutionWaitingQueues := persistence.NewStateExecutionWaitingQueues()
	prepareDataForConsumeFor(stateExecutionWaitingQueues, uuids)

	stateExecutionWaitingQueues.AddNewLocalQueueCommandForStateExecution(persistence.StateExecutionId{
		StateId: "state_1", StateIdSequence: 1,
	}, xdbapi.LocalQueueCommand{
		QueueName: "q3", Count: ptr.Any(int32(2)),
	})

	// Return UnconsumedMessageQueueCountMap as:
	//
	// (q1, 2), (q2, 2), (q3, 1)
	//
	// and StateToLocalQueueCommandsMap["state_1-1"] as:
	//
	// (q1, 1), (q2, 2), (q3, 2)

	canComplete, consumedMessages := stateExecutionWaitingQueues.CheckCanCompleteLocalQueueWaiting(persistence.StateExecutionId{
		StateId: "state_1", StateIdSequence: 1,
	}, xdbapi.ALL_OF_COMPLETION)

	// The new UnconsumedMessageQueueCountMap should be:
	//
	// (q1, 1), (q3, 1)
	//
	// and StateToLocalQueueCommandsMap["state_1-1"] as:
	//
	// (q3, 2)

	assert.False(t, canComplete)
	assert.Equal(t, []persistence.InternalLocalQueueMessage{{
		DedupId: uuid_q1_1.String(), IsFull: false,
	}, {
		DedupId: uuid_q2_1.String(), IsFull: false,
	}, {
		DedupId: uuid_q2_2.String(), IsFull: false,
	}}, consumedMessages)
	assert.Len(t, stateExecutionWaitingQueues.UnconsumedLocalQueueMessages, 2)
	assert.Equal(t, []persistence.InternalLocalQueueMessage{{
		DedupId: uuid_q1_2.String(), IsFull: false,
	}}, stateExecutionWaitingQueues.UnconsumedLocalQueueMessages["q1"])
	assert.Equal(t, []persistence.InternalLocalQueueMessage{{
		DedupId: uuid_q3_1.String(), IsFull: false,
	}}, stateExecutionWaitingQueues.UnconsumedLocalQueueMessages["q3"])
	assert.Len(t, stateExecutionWaitingQueues.StateToLocalQueueCommandsMap, 1)
	assert.Len(t, stateExecutionWaitingQueues.StateToLocalQueueCommandsMap["state_1-1"], 1)
	assert.Equal(t, xdbapi.LocalQueueCommand{
		QueueName: "q3",
		Count:     xdbapi.PtrInt32(2),
	}, stateExecutionWaitingQueues.StateToLocalQueueCommandsMap["state_1-1"][0])
}

func TestStateExecutionWaitingQueuesJsonConsumeFor_Any_consumed(t *testing.T) {
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

	stateExecutionWaitingQueues := persistence.NewStateExecutionWaitingQueues()
	prepareDataForConsumeFor(stateExecutionWaitingQueues, uuids)

	// Return UnconsumedMessageQueueCountMap as:
	//
	// (q1, 2), (q2, 2), (q3, 1)
	//
	// and StateToLocalQueueCommandsMap["state_1-1"] as:
	//
	// (q1, 1), (q2, 2)

	canComplete, consumedMessages := stateExecutionWaitingQueues.CheckCanCompleteLocalQueueWaiting(persistence.StateExecutionId{
		StateId: "state_1", StateIdSequence: 1,
	}, xdbapi.ANY_OF_COMPLETION)

	// The new UnconsumedMessageQueueCountMap should be:
	//
	// (q1, 1), (q2, 2), (q3, 1)
	//
	// and StateToLocalQueueCommandsMap["state_1-1"] as deleted.

	assert.True(t, canComplete)
	assert.Equal(t, []persistence.InternalLocalQueueMessage{{
		DedupId: uuid_q1_1.String(), IsFull: false,
	}}, consumedMessages)
	assert.Len(t, stateExecutionWaitingQueues.UnconsumedLocalQueueMessages, 3)
	assert.Equal(t, []persistence.InternalLocalQueueMessage{{
		DedupId: uuid_q1_2.String(), IsFull: false,
	}}, stateExecutionWaitingQueues.UnconsumedLocalQueueMessages["q1"])
	assert.Equal(t, []persistence.InternalLocalQueueMessage{{
		DedupId: uuid_q2_1.String(), IsFull: false,
	}, {
		DedupId: uuid_q2_2.String(), IsFull: false,
	}}, stateExecutionWaitingQueues.UnconsumedLocalQueueMessages["q2"])
	assert.Equal(t, []persistence.InternalLocalQueueMessage{{
		DedupId: uuid_q3_1.String(), IsFull: false,
	}}, stateExecutionWaitingQueues.UnconsumedLocalQueueMessages["q3"])
	assert.Empty(t, stateExecutionWaitingQueues.StateToLocalQueueCommandsMap)
}

func TestStateExecutionWaitingQueuesJsonConsumeFor_Any_notConsumed(t *testing.T) {
	uuid_q1_1 := uuid.MustNewUUID()

	stateExecutionWaitingQueues := persistence.NewStateExecutionWaitingQueues()

	completedStateExecutionIdString, consumedMessages := stateExecutionWaitingQueues.Consume(persistence.LocalQueueMessageInfoJson{
		QueueName: "q1", DedupId: uuid_q1_1,
	})
	assert.Empty(t, completedStateExecutionIdString)
	assert.Empty(t, consumedMessages)
	assert.Len(t, stateExecutionWaitingQueues.UnconsumedLocalQueueMessages, 1)
	assert.Equal(t, []persistence.InternalLocalQueueMessage{{
		DedupId: uuid_q1_1.String(), IsFull: false,
	}}, stateExecutionWaitingQueues.UnconsumedLocalQueueMessages["q1"])
	assert.Empty(t, stateExecutionWaitingQueues.StateToLocalQueueCommandsMap)

	stateExecutionWaitingQueues.AddNewLocalQueueCommandForStateExecution(persistence.StateExecutionId{
		StateId: "state_1", StateIdSequence: 1,
	}, xdbapi.LocalQueueCommand{
		QueueName: "q1", Count: ptr.Any(int32(2)),
	})
	stateExecutionWaitingQueues.AddNewLocalQueueCommandForStateExecution(persistence.StateExecutionId{
		StateId: "state_1", StateIdSequence: 1,
	}, xdbapi.LocalQueueCommand{
		QueueName: "q2", Count: ptr.Any(int32(1)),
	})

	// Return UnconsumedMessageQueueCountMap as:
	//
	// (q1, 1)
	//
	// and StateToLocalQueueCommandsMap["state_1-1"] as:
	//
	// (q1, 2), (q2, 1)

	canComplete, consumedMessages := stateExecutionWaitingQueues.CheckCanCompleteLocalQueueWaiting(persistence.StateExecutionId{
		StateId: "state_1", StateIdSequence: 1,
	}, xdbapi.ANY_OF_COMPLETION)

	assert.False(t, canComplete)
	assert.Empty(t, consumedMessages)
	assert.Len(t, stateExecutionWaitingQueues.UnconsumedLocalQueueMessages, 1)
	assert.Equal(t, []persistence.InternalLocalQueueMessage{{
		DedupId: uuid_q1_1.String(), IsFull: false,
	}}, stateExecutionWaitingQueues.UnconsumedLocalQueueMessages["q1"])
	assert.Len(t, stateExecutionWaitingQueues.StateToLocalQueueCommandsMap, 1)
	assert.Len(t, stateExecutionWaitingQueues.StateToLocalQueueCommandsMap["state_1-1"], 2)
	assert.Equal(t, xdbapi.LocalQueueCommand{
		QueueName: "q1",
		Count:     xdbapi.PtrInt32(2),
	}, stateExecutionWaitingQueues.StateToLocalQueueCommandsMap["state_1-1"][0])
	assert.Equal(t, xdbapi.LocalQueueCommand{
		QueueName: "q2",
		Count:     xdbapi.PtrInt32(1),
	}, stateExecutionWaitingQueues.StateToLocalQueueCommandsMap["state_1-1"][1])
}

// Return:
//
//	state_1, 1: (q1, 2),
//	state_1, 2: (q2, 3),
//	state_3, 1: (q1: 1), (q2, 2)
func prepareDataForConsume(stateExecutionWaitingQueues persistence.StateExecutionWaitingQueuesJson) {
	stateExecutionWaitingQueues.AddNewLocalQueueCommandForStateExecution(persistence.StateExecutionId{
		StateId: "state_1", StateIdSequence: 1,
	}, xdbapi.LocalQueueCommand{
		QueueName: "q1", Count: ptr.Any(int32(2)),
	})

	stateExecutionWaitingQueues.AddNewLocalQueueCommandForStateExecution(persistence.StateExecutionId{
		StateId: "state_1", StateIdSequence: 2,
	}, xdbapi.LocalQueueCommand{
		QueueName: "q2", Count: ptr.Any(int32(3)),
	})

	stateExecutionWaitingQueues.AddNewLocalQueueCommandForStateExecution(persistence.StateExecutionId{
		StateId: "state_3", StateIdSequence: 1,
	}, xdbapi.LocalQueueCommand{
		QueueName: "q1",
	})

	stateExecutionWaitingQueues.AddNewLocalQueueCommandForStateExecution(persistence.StateExecutionId{
		StateId: "state_3", StateIdSequence: 1,
	}, xdbapi.LocalQueueCommand{
		QueueName: "q2", Count: ptr.Any(int32(2)),
	})
}

// Return UnconsumedMessageQueueCountMap as:
//
// (q1, 2), (q2, 2), (q3, 1)
//
// and StateToLocalQueueCommandsMap["state_1-1"] as:
//
// (q1, 1), (q2, 2)
func prepareDataForConsumeFor(stateExecutionWaitingQueues persistence.StateExecutionWaitingQueuesJson, dedupIds []uuid.UUID) {
	stateExecutionWaitingQueues.Consume(persistence.LocalQueueMessageInfoJson{
		QueueName: "q1", DedupId: dedupIds[0],
	})
	stateExecutionWaitingQueues.Consume(persistence.LocalQueueMessageInfoJson{
		QueueName: "q1", DedupId: dedupIds[1],
	})
	stateExecutionWaitingQueues.Consume(persistence.LocalQueueMessageInfoJson{
		QueueName: "q2", DedupId: dedupIds[2],
	})
	stateExecutionWaitingQueues.Consume(persistence.LocalQueueMessageInfoJson{
		QueueName: "q2", DedupId: dedupIds[3],
	})
	stateExecutionWaitingQueues.Consume(persistence.LocalQueueMessageInfoJson{
		QueueName: "q3", DedupId: dedupIds[4],
	})

	stateExecutionWaitingQueues.AddNewLocalQueueCommandForStateExecution(persistence.StateExecutionId{
		StateId: "state_1", StateIdSequence: 1,
	}, xdbapi.LocalQueueCommand{
		QueueName: "q1", Count: ptr.Any(int32(1)),
	})

	stateExecutionWaitingQueues.AddNewLocalQueueCommandForStateExecution(persistence.StateExecutionId{
		StateId: "state_1", StateIdSequence: 1,
	}, xdbapi.LocalQueueCommand{
		QueueName: "q2", Count: ptr.Any(int32(2)),
	})
}
