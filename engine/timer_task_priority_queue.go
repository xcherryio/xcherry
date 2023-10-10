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

package engine

import (
	"container/heap"
	"github.com/xdblab/xdb/persistence"
)

// I know, it looks a lot to have a heap. This is the standard way of using heap in Golang
// See https://pkg.go.dev/container/heap for more details

func NewTimerTaskPriorityQueue(tasks []persistence.TimerTask) TimerTaskPriorityQueue {
	hq := make(TimerTaskPriorityQueue, 0, len(tasks))
	for _, task := range tasks {
		t := task
		hq = append(hq, &t)
	}
	heap.Init(&hq)
	return hq
}

// A TimerTaskPriorityQueue implements heap.Interface and holds Items.
type TimerTaskPriorityQueue []*persistence.TimerTask

func (pq *TimerTaskPriorityQueue) Len() int { return len(*pq) }

func (pq *TimerTaskPriorityQueue) Less(i, j int) bool {
	// We want Pop to give us the lowest, not lowest, priority so we use less than here.
	return (*pq)[i].FireTimestampSeconds < (*pq)[j].FireTimestampSeconds
}

func (pq *TimerTaskPriorityQueue) Swap(i, j int) {
	(*pq)[i], (*pq)[j] = (*pq)[j], (*pq)[i]
}

func (pq *TimerTaskPriorityQueue) Push(x any) {
	item := x.(*persistence.TimerTask)
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
