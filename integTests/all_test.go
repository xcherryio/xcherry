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

package integTests

import (
	"github.com/xdblab/xdb-golang-sdk/integTests/basic"
	"github.com/xdblab/xdb-golang-sdk/integTests/multi_states"
	"github.com/xdblab/xdb-golang-sdk/integTests/state_decision"
	"github.com/xdblab/xdb-golang-sdk/integTests/stateretry"
	"testing"
)

func TestIOProcess(t *testing.T) {
	basic.TestStartIOProcess(t, client)
}

func TestStateBackoffRetry(t *testing.T) {
	stateretry.TestBackoff(t, client)
}

func TestTerminateProcess(t *testing.T) {
	multi_states.TestTerminateMultiStatesProcess(t, client)
}

func TestStopProcessByFail(t *testing.T) {
	multi_states.TestFailMultiStatesProcess(t, client)
}

func TestStateDecision(t *testing.T) {
	state_decision.TestGracefulCompleteProcess(t, client)
	state_decision.TestForceCompleteProcess(t, client)
	state_decision.TestForceFailProcess(t, client)
	state_decision.TestDeadEndProcess(t, client)
}

func TestProcessIdReusePolicyDisallowReuse(t *testing.T) {
	basic.TestProcessIdReusePolicyDisallowReuse(t, client)
}

func TestProcessIdReusePolicyAllowIfNoRunning(t *testing.T) {
	basic.TestProcessIdReusePolicyAllowIfNoRunning(t, client)
}

func TestProcessIdReusePolicyTerminateIfRunning(t *testing.T) {
	basic.TestProcessIdReusePolicyTerminateIfRunning(t, client)
}

func TestProcessIdReusePolicyAllowIfPreviousExitAbnormally(t *testing.T) {
	basic.TestProcessIdReusePolicyAllowIfPreviousExitAbnormally(t, client)
}
