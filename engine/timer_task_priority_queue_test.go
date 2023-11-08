// Copyright (c) 2023 XDBLab Organization
// SPDX-License-Identifier: BUSL-1.1

package engine

import (
	"container/heap"
	"github.com/stretchr/testify/assert"
	"github.com/xdblab/xdb/persistence"
	"testing"
)

func TestTimerTaskPriorityQueue(t *testing.T) {
	pq := NewTimerTaskPriorityQueue([]persistence.TimerTask{
		{FireTimestampSeconds: 6},
		{FireTimestampSeconds: 7},
		{FireTimestampSeconds: 5},
		{FireTimestampSeconds: 8},
	})

	heap.Init(&pq)

	heap.Push(&pq, &persistence.TimerTask{FireTimestampSeconds: 3})
	heap.Push(&pq, &persistence.TimerTask{FireTimestampSeconds: 1})
	heap.Push(&pq, &persistence.TimerTask{FireTimestampSeconds: 2})
	heap.Push(&pq, &persistence.TimerTask{FireTimestampSeconds: 4})

	for i := 0; i < 8; i++ {
		task0 := pq[0]
		task := heap.Pop(&pq)
		assert.Equal(t, task0, task)
		task1, ok := task.(*persistence.TimerTask)
		assert.Equal(t, true, ok)

		assert.Equal(t, int64(i+1), task1.FireTimestampSeconds)
	}
}
