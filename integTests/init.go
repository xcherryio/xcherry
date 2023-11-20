// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: BUSL-1.1

package integTests

import (
	"github.com/xcherryio/sdk-go/integTests/basic"
	"github.com/xcherryio/sdk-go/integTests/failure_recovery"
	"github.com/xcherryio/sdk-go/integTests/global_attribute"
	"github.com/xcherryio/sdk-go/integTests/multi_states"
	"github.com/xcherryio/sdk-go/integTests/process_timeout"
	"github.com/xcherryio/sdk-go/integTests/state_decision"
	"github.com/xcherryio/sdk-go/integTests/stateretry"
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
		&failure_recovery.StateFailureRecoveryTestExecuteFailedAtStartProcess{},
		&multi_states.MultiStatesProcess{},
		&state_decision.GracefulCompleteProcess{},
		&state_decision.ForceCompleteProcess{},
		&state_decision.ForceFailProcess{},
		&state_decision.DeadEndProcess{},
		&stateretry.BackoffProcess{},
		&global_attribute.SingleTableProcess{},
		&global_attribute.MultiTablesProcess{},
		&process_timeout.TimeoutProcess{},
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
