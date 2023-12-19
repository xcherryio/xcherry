// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"container/heap"
	"github.com/stretchr/testify/assert"
	"github.com/xcherryio/xcherry/persistence/data_models"
	"testing"
)

func TestTimerTaskPriorityQueue(t *testing.T) {
	pq := NewTimerTaskPriorityQueue([]data_models.TimerTask{
		{FireTimestampSeconds: 6},
		{FireTimestampSeconds: 7},
		{FireTimestampSeconds: 5},
		{FireTimestampSeconds: 8},
	})

	heap.Init(&pq)

	heap.Push(&pq, &data_models.TimerTask{FireTimestampSeconds: 3})
	heap.Push(&pq, &data_models.TimerTask{FireTimestampSeconds: 1})
	heap.Push(&pq, &data_models.TimerTask{FireTimestampSeconds: 2})
	heap.Push(&pq, &data_models.TimerTask{FireTimestampSeconds: 4})

	for i := 0; i < 8; i++ {
		task0 := pq[0]
		task := heap.Pop(&pq)
		assert.Equal(t, task0, task)
		task1, ok := task.(*data_models.TimerTask)
		assert.Equal(t, true, ok)

		assert.Equal(t, int64(i+1), task1.FireTimestampSeconds)
	}
}
