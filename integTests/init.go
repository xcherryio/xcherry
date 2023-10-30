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
	"github.com/xdblab/xdb-golang-sdk/integTests/failure_recovery"
	"github.com/xdblab/xdb-golang-sdk/integTests/multi_states"
	"github.com/xdblab/xdb-golang-sdk/integTests/state_decision"
	"github.com/xdblab/xdb-golang-sdk/integTests/stateretry"
	"github.com/xdblab/xdb-golang-sdk/xdb"
)

var registry = xdb.NewRegistry()

var client = xdb.NewClient(registry, createTestDefaultClientOptions())

var workerService = xdb.NewWorkerService(registry, nil)

func init() {
	err := registry.AddProcesses(
		&basic.IOProcess{},
		&failure_recovery.StateFailureRecoveryTestExecuteProcess{},
		&failure_recovery.StateFailureRecoveryTestWaitUntilProcess{},
		&failure_recovery.StateFailureRecoveryTestExecuteNoWaitUntilProcess{},
		&multi_states.MultiStatesProcess{},
		&state_decision.GracefulCompleteProcess{},
		&state_decision.ForceCompleteProcess{},
		&state_decision.ForceFailProcess{},
		&state_decision.DeadEndProcess{},
		&stateretry.BackoffProcess{},
	)
	if err != nil {
		panic(err)
	}
}

func createTestDefaultClientOptions() *xdb.ClientOptions {
	opts := xdb.GetLocalDefaultClientOptions()
	opts.EnabledDebugLogging = true
	return opts
}
