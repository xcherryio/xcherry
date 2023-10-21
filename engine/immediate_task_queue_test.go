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
