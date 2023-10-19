// Copyright 2023 XDBLab organization

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
