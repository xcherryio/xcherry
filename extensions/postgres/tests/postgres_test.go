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
	"github.com/xdblab/xdb/persistence/sql/sqltest"
	"testing"
)

func TestBasic(t *testing.T) {
	assert := assert.New(t)

	sqltest.SQLBasicTest(assert, store)
	sqltest.SQLGracefulCompleteTest(assert, store)
	sqltest.SQLForceFailTest(assert, store)

	sqltest.SQLProcessIdReusePolicyDisallowReuseTest(assert, store)
	sqltest.SQLProcessIdReusePolicyAllowIfNoRunning(assert, store)
	sqltest.SQLProcessIdReusePolicyTerminateIfRunning(assert, store)
	sqltest.SQLProcessIdReusePolicyAllowIfPreviousExitAbnormally(assert, store)
	sqltest.SQLProcessIdReusePolicyDefault(assert, store)
}

func TestBackoffTimer(t *testing.T) {
	ass := assert.New(t)

	sqltest.SQLBackoffTest(ass, store)
}
