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
	"github.com/xdblab/xdb/persistence"
	"testing"
)

func TestStateExecutionWaitingQueuesJsonAnyOfCompletion(t *testing.T) {
	stateExecutionWaitingQueues := persistence.NewStateExecutionWaitingQueues()
	prepareDataForAnyOfCompletion(stateExecutionWaitingQueues)

	// Consume a non-existent queue
	completedStateExecutionId, hasFinishedWaiting := stateExecutionWaitingQueues.Consume(xdbapi.LocalQueueMessage{
		QueueName: "q3",
	})
	assert.Nil(t, completedStateExecutionId)
	assert.False(t, hasFinishedWaiting)

	completedStateExecutionId, hasFinishedWaiting = stateExecutionWaitingQueues.Consume(xdbapi.LocalQueueMessage{
		QueueName: "q1",
	})
	completedStateExecutionId2, hasFinishedWaiting2 := stateExecutionWaitingQueues.Consume(xdbapi.LocalQueueMessage{
		QueueName: "q1",
	})

	// The new data should be:
	//	state_3-a, 1: (q2: 1)
	assert.True(t, hasFinishedWaiting)
	assert.True(t, hasFinishedWaiting2)
	if completedStateExecutionId.StateIdSequence == 1 {
		assert.Equal(t, persistence.StateExecutionId{
			StateId: "state_1", StateIdSequence: 1,
		}, *completedStateExecutionId)
		assert.Equal(t, persistence.StateExecutionId{
			StateId: "state_1", StateIdSequence: 2,
		}, *completedStateExecutionId2)
	} else {
		assert.Equal(t, persistence.StateExecutionId{
			StateId: "state_1", StateIdSequence: 2,
		}, *completedStateExecutionId)
		assert.Equal(t, persistence.StateExecutionId{
			StateId: "state_1", StateIdSequence: 1,
		}, *completedStateExecutionId2)
	}

	completedStateExecutionId, hasFinishedWaiting = stateExecutionWaitingQueues.Consume(xdbapi.LocalQueueMessage{
		QueueName: "q2",
	})
	// The new data should be empty.
	// Return state_3-a, 1 as completed
	assert.Equal(t, persistence.StateExecutionId{
		StateId: "state_3-a", StateIdSequence: 1,
	}, *completedStateExecutionId)
	assert.True(t, hasFinishedWaiting)

}

func TestStateExecutionWaitingQueuesJsonAllOfCompletion(t *testing.T) {
	stateExecutionWaitingQueues := persistence.NewStateExecutionWaitingQueues()
	prepareDataForAllOfCompletion(stateExecutionWaitingQueues)

	// Consume a non-existent queue
	completedStateExecutionId, hasFinishedWaiting := stateExecutionWaitingQueues.Consume(xdbapi.LocalQueueMessage{
		QueueName: "q3",
	})
	assert.Nil(t, completedStateExecutionId)
	assert.False(t, hasFinishedWaiting)

	completedStateExecutionId, hasFinishedWaiting = stateExecutionWaitingQueues.Consume(xdbapi.LocalQueueMessage{
		QueueName: "q1",
	})
	// The new data should be:
	//	state_1, 1: (q1, 1), (q2, 2)
	//	state_1, 2: (q1, 2)
	// return (state_3, 1), true
	assert.Equal(t, persistence.StateExecutionId{
		StateId: "state_3", StateIdSequence: 1,
	}, *completedStateExecutionId)
	assert.True(t, hasFinishedWaiting)

	completedStateExecutionId, hasFinishedWaiting = stateExecutionWaitingQueues.Consume(xdbapi.LocalQueueMessage{
		QueueName: "q1",
	})
	// The new data should be:
	//	state_1, 1: (q2, 2)
	//	state_1, 2: (q1, 2)
	// return (state_1, 1), false
	assert.Equal(t, persistence.StateExecutionId{
		StateId: "state_1", StateIdSequence: 1,
	}, *completedStateExecutionId)
	assert.False(t, hasFinishedWaiting)

	completedStateExecutionId, hasFinishedWaiting = stateExecutionWaitingQueues.Consume(xdbapi.LocalQueueMessage{
		QueueName: "q1",
	})
	// The new data should be:
	//	state_1, 1: (q2, 2)
	//	state_1, 2: (q1, 1)
	// return (state_1, 2), false
	assert.Equal(t, persistence.StateExecutionId{
		StateId: "state_1", StateIdSequence: 2,
	}, *completedStateExecutionId)
	assert.False(t, hasFinishedWaiting)

	completedStateExecutionId, hasFinishedWaiting = stateExecutionWaitingQueues.Consume(xdbapi.LocalQueueMessage{
		QueueName: "q1",
	})
	// The new data should be:
	//	state_1, 1: (q2, 2)
	// return (state_1, 2), true
	assert.Equal(t, persistence.StateExecutionId{
		StateId: "state_1", StateIdSequence: 2,
	}, *completedStateExecutionId)
	assert.True(t, hasFinishedWaiting)
}

// Return:
//
//	state_1, 1: (q1, 1), (q2, 1)
//	state_1, 2: (q1, 1)
//	state_3-a, 1: (q2: 1)
func prepareDataForAnyOfCompletion(stateExecutionWaitingQueues persistence.StateExecutionWaitingQueuesJson) {
	stateExecutionWaitingQueues.Add(persistence.StateExecutionId{
		StateId: "state_1", StateIdSequence: 1,
	}, xdbapi.LocalQueueCommand{
		QueueName: "q1",
	}, true)

	stateExecutionWaitingQueues.Add(persistence.StateExecutionId{
		StateId: "state_1", StateIdSequence: 1,
	}, xdbapi.LocalQueueCommand{
		QueueName: "q2", Count: ptr.Any(int32(2)),
	}, true)

	stateExecutionWaitingQueues.Add(persistence.StateExecutionId{
		StateId: "state_1", StateIdSequence: 2,
	}, xdbapi.LocalQueueCommand{
		QueueName: "q1", Count: ptr.Any(int32(2)),
	}, true)

	stateExecutionWaitingQueues.Add(persistence.StateExecutionId{
		StateId: "state_3-a", StateIdSequence: 1,
	}, xdbapi.LocalQueueCommand{
		QueueName: "q2",
	}, true)
}

// Return:
//
//	state_1, 1: (q1, 1), (q2, 2)
//	state_1, 2: (q1, 2)
//	state_3, 1: (q1: 1)
func prepareDataForAllOfCompletion(stateExecutionWaitingQueues persistence.StateExecutionWaitingQueuesJson) {
	stateExecutionWaitingQueues.Add(persistence.StateExecutionId{
		StateId: "state_1", StateIdSequence: 1,
	}, xdbapi.LocalQueueCommand{
		QueueName: "q1",
	}, false)

	stateExecutionWaitingQueues.Add(persistence.StateExecutionId{
		StateId: "state_1", StateIdSequence: 1,
	}, xdbapi.LocalQueueCommand{
		QueueName: "q2", Count: ptr.Any(int32(2)),
	}, false)

	stateExecutionWaitingQueues.Add(persistence.StateExecutionId{
		StateId: "state_1", StateIdSequence: 2,
	}, xdbapi.LocalQueueCommand{
		QueueName: "q1", Count: ptr.Any(int32(2)),
	}, false)

	stateExecutionWaitingQueues.Add(persistence.StateExecutionId{
		StateId: "state_3", StateIdSequence: 1,
	}, xdbapi.LocalQueueCommand{
		QueueName: "q1",
	}, false)
}