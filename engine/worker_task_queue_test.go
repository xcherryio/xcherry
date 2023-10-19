package engine

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestWorkerTaskPagesEmpty(t *testing.T) {
	var completedPages []*workerTaskPage
	pages := mergeWorkerTaskPages(completedPages)

	assert.Equal(t, 0, len(pages))
}

func TestWorkerTaskPages(t *testing.T) {
	var completedPages []*workerTaskPage
	completedPages = append(completedPages,
		&workerTaskPage{
			minTaskSequence: 1,
			maxTaskSequence: 2,
		},
		&workerTaskPage{
			minTaskSequence: 7,
			maxTaskSequence: 8,
		},
		&workerTaskPage{
			minTaskSequence: 3,
			maxTaskSequence: 4,
		})
	pages := mergeWorkerTaskPages(completedPages)

	assert.Equal(t, 2, len(pages))
	assert.Equal(t, &workerTaskPage{
		minTaskSequence: 1,
		maxTaskSequence: 4,
	}, pages[0])
	assert.Equal(t, &workerTaskPage{
		minTaskSequence: 7,
		maxTaskSequence: 8,
	}, pages[1])
}
