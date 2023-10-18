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
	sqltest.SQLBasicTest(assert.New(t), store)
}

func TestGracefulComplete(t *testing.T) {
	sqltest.SQLGracefulCompleteTest(assert.New(t), store)
}

func TestForceFail(t *testing.T) {
	sqltest.SQLForceFailTest(assert.New(t), store)
}

func TestProcessIdReusePolicyDisallowReuse(t *testing.T) {
	sqltest.SQLProcessIdReusePolicyDisallowReuseTest(assert.New(t), store)
}

func TestProcessIdReusePolicyAllowIfNoRunning(t *testing.T) {
	sqltest.SQLProcessIdReusePolicyAllowIfNoRunning(assert.New(t), store)
}

func TestProcessIdReusePolicyTerminateIfRunning(t *testing.T) {
	sqltest.SQLProcessIdReusePolicyTerminateIfRunning(assert.New(t), store)
}

func TestProcessIdReusePolicyAllowIfPreviousExitAbnormally(t *testing.T) {
	sqltest.SQLProcessIdReusePolicyAllowIfPreviousExitAbnormally(assert.New(t), store)
}

func TestProcessIdReusePolicyDefault(t *testing.T) {
	sqltest.SQLProcessIdReusePolicyDefault(assert.New(t), store)
}

func TestBackoffTimer(t *testing.T) {
	sqltest.CleanupEnv(assert.New(t), store)
	sqltest.SQLBackoffTest(assert.New(t), store)
}
