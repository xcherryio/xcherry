// Copyright (c) 2023 XDBLab Organization
// SPDX-License-Identifier: BUSL-1.1

package engine

import (
	"container/heap"
	"github.com/xdblab/xdb/persistence/data_models"
)

// I know, it looks a lot to have a heap. This is the standard way of using heap in Golang
// See https://pkg.go.dev/container/heap for more details

func NewTimerTaskPriorityQueue(tasks []data_models.TimerTask) TimerTaskPriorityQueue {
	hq := make(TimerTaskPriorityQueue, 0, len(tasks))
	for _, task := range tasks {
		t := task
		hq = append(hq, &t)
	}
	heap.Init(&hq)
	return hq
}

// A TimerTaskPriorityQueue implements heap.Interface and holds Items.
type TimerTaskPriorityQueue []*data_models.TimerTask

func (pq *TimerTaskPriorityQueue) Len() int { return len(*pq) }

func (pq *TimerTaskPriorityQueue) Less(i, j int) bool {
	// We want Pop to give us the lowest, not lowest, priority so we use less than here.
	return (*pq)[i].FireTimestampSeconds < (*pq)[j].FireTimestampSeconds
}

func (pq *TimerTaskPriorityQueue) Swap(i, j int) {
	(*pq)[i], (*pq)[j] = (*pq)[j], (*pq)[i]
}

func (pq *TimerTaskPriorityQueue) Push(x any) {
	item, ok := x.(*data_models.TimerTask)
	if !ok {
		panic("Pushed item is not a TimerTask")
	}
	*pq = append(*pq, item)
}

func (pq *TimerTaskPriorityQueue) Pop() any {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil // avoid memory leak
	*pq = old[0 : n-1]
	return item
}
