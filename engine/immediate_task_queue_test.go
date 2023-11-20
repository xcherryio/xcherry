// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: BUSL-1.1

package engine

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestImmediateTaskPagesEmpty(t *testing.T) {
	var completedPages []*immediateTaskPage
	pages := mergeImmediateTaskPages(completedPages)

	assert.Equal(t, 0, len(pages))
}

func TestImmediateTaskPages(t *testing.T) {
	var completedPages []*immediateTaskPage
	completedPages = append(completedPages,
		&immediateTaskPage{
			minTaskSequence: 1,
			maxTaskSequence: 2,
		},
		&immediateTaskPage{
			minTaskSequence: 7,
			maxTaskSequence: 8,
		},
		&immediateTaskPage{
			minTaskSequence: 3,
			maxTaskSequence: 4,
		})
	pages := mergeImmediateTaskPages(completedPages)

	assert.Equal(t, 2, len(pages))
	assert.Equal(t, &immediateTaskPage{
		minTaskSequence: 1,
		maxTaskSequence: 4,
	}, pages[0])
	assert.Equal(t, &immediateTaskPage{
		minTaskSequence: 7,
		maxTaskSequence: 8,
	}, pages[1])
}
