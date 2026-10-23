// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package integ

import (
	"strings"
	"testing"
)

var temporalOnlyScenarioTests = map[string]struct{}{
	"TestAnyCommandCombinationFlowCadence":                      {},
	"TestAnyCommandCombinationFlowCadenceContinueAsNew":         {},
	"TestAnyCommandCombinationFlowTemporal":                     {},
	"TestAnyCommandCombinationFlowTemporalContinueAsNew":        {},
	"TestAsyncRetryBudgetCadence":                               {},
	"TestAsyncRetryBudgetTemporal":                              {},
	"TestAttributeIndexSyncCadence":                             {},
	"TestAttributeIndexSyncTemporal":                            {},
	"TestAttributeSyncRetryExhaustionCadence":                   {},
	"TestAttributeSyncRetryExhaustionTemporal":                  {},
	"TestChannelMultivalueCadence":                              {},
	"TestChannelMultivalueTemporal":                             {},
	"TestCreateFlowCadence":                                     {},
	"TestCreateFlowCadenceContinueAsNew":                        {},
	"TestCreateFlowTemporal":                                    {},
	"TestCreateFlowTemporalContinueAsNew":                       {},
	"TestDeadEndFlowCadence":                                    {},
	"TestDeadEndFlowCadenceContinueAsNew":                       {},
	"TestDeadEndFlowTemporal":                                   {},
	"TestDeadEndFlowTemporalContinueAsNew":                      {},
	"TestFlowIDWithTenantPrefixPassesThroughTemporal":           {},
	"TestFlowTimeoutCadence":                                    {},
	"TestFlowTimeoutTemporal":                                   {},
	"TestGreedyTimerFlowBaseCadence":                            {},
	"TestGreedyTimerFlowBaseCadenceContinueAsNew":               {},
	"TestGreedyTimerFlowBaseTemporal":                           {},
	"TestGreedyTimerFlowBaseTemporalContinueAsNew":              {},
	"TestGreedyTimerFlowCadence":                                {},
	"TestGreedyTimerFlowCadenceContinueAsNew":                   {},
	"TestGreedyTimerFlowTemporal":                               {},
	"TestGreedyTimerFlowTemporalContinueAsNew":                  {},
	"TestHardDeleteAttributeBlobCacheTemporal":                  {},
	"TestHardDeleteAttributeCleanupTemporal":                    {},
	"TestHardDeleteAttributeStepWriteRequiresBlobStoreTemporal": {},
	"TestHardDeleteAttributeSubFlowCadence":                     {},
	"TestHardDeleteAttributeSubFlowTemporal":                    {},
	"TestHardDeleteAttributesCadence":                           {},
	"TestHardDeleteAttributesTemporal":                          {},
	"TestLargeDataAttributesTemporalContinueAsNew":              {},
	"TestLockingWorkflowCadence":                                {},
	"TestLockingWorkflowCadenceContinueAsNew":                   {},
	"TestLockingWorkflowTemporal":                               {},
	"TestLockingWorkflowTemporalContinueAsNew":                  {},
	"TestPullParallelLastWriteCadence":                          {},
	"TestPullParallelLastWriteTemporal":                         {},
	"TestPullRPCDispatchRespectsCallerDeadlineCadence":          {},
	"TestPullRPCDispatchRespectsCallerDeadlineTemporal":         {},
	"TestPullSubFlowStartsWithoutStickyQueueCadence":            {},
	"TestPullSubFlowStartsWithoutStickyQueueTemporal":           {},
	"TestPullUnknownNameRedispatchCadence":                      {},
	"TestPullUnknownNameRedispatchTemporal":                     {},
	"TestPullWorkerDispatchCadence":                             {},
	"TestPullWorkerDispatchTemporal":                            {},
	"TestPullWorkerQueueSurvivesContinueAsNewCadence":           {},
	"TestPullWorkerQueueSurvivesContinueAsNewTemporal":          {},
	"TestPullWorkerStickyCapacityRecoveryCadence":               {},
	"TestPullWorkerStickyCapacityRecoveryTemporal":              {},
	"TestPullWorkerStickyFallbackCadence":                       {},
	"TestPullWorkerStickyFallbackTemporal":                      {},
	"TestRpcExternalStorageLockingTemporal":                     {},
	"TestRpcExternalStorageNonLockingCadence":                   {},
	"TestRpcExternalStorageNonLockingTemporal":                  {},
	"TestRpcExternalStorageSynchronousUpdateTemporal":           {},
	"TestRpcInlineTransportCadence":                             {},
	"TestRpcInlineTransportTemporal":                            {},
	"TestRpcReadBlobStoreUnavailableCadence":                    {},
	"TestRpcReadBlobStoreUnavailableTemporal":                   {},
	"TestRpcSignalWireCompatibilityCadence":                     {},
	"TestRpcSignalWireCompatibilityTemporal":                    {},
	"TestRpcTransactionalRequestTooLargeTemporal":               {},
	"TestRpcWorkerRequestTooLargeCadence":                       {},
	"TestRpcWorkerRequestTooLargeTemporal":                      {},
	"TestS3CleanupCadence":                                      {},
	"TestS3CleanupTemporal":                                     {},
	"TestS3GetSetDataAttributesCadence":                         {},
	"TestS3GetSetDataAttributesTemporal":                        {},
	"TestS3GetSetDataAttributesWithInitialDataTemporal":         {},
	"TestS3KeyPrefixLayoutAndCleanupTemporal":                   {},
	"TestS3WorkflowInitDataAttributesCadence":                   {},
	"TestS3WorkflowInitDataAttributesTemporal":                  {},
	"TestS3WorkflowStartInputCadence":                           {},
	"TestS3WorkflowStartInputTemporal":                          {},
	"TestS3WorkflowStateInputOptimizationCadence":               {},
	"TestS3WorkflowStateInputOptimizationTemporal":              {},
	"TestS3WorkflowUpsertDataObjectsCadence":                    {},
	"TestS3WorkflowUpsertDataObjectsTemporal":                   {},
	"TestSetSearchAttributes":                                   {},
	"TestSignalWorkflowCadence":                                 {},
	"TestSignalWorkflowCadenceContinueAsNew":                    {},
	"TestSignalWorkflowTemporal":                                {},
	"TestSignalWorkflowTemporalContinueAsNew":                   {},
	"TestStartFlowNoOptionsCadence":                             {},
	"TestStartFlowNoOptionsTemporal":                            {},
	"TestStepLineageCadence":                                    {},
	"TestStepLineageTemporal":                                   {},
	"TestSubFlowConditionCadence":                               {},
	"TestSubFlowConditionTemporal":                              {},
	"TestSynchronousUpdateRequestIDTemporal":                    {},
	"TestTemporalMetrics":                                       {},
	"TestTimerFlowCadence":                                      {},
	"TestTimerFlowCadenceContinueAsNew":                         {},
	"TestTimerFlowTemporal":                                     {},
	"TestTimerFlowTemporalContinueAsNew":                        {},
	"TestUpdateWorkerTargetCadence":                             {},
	"TestUpdateWorkerTargetTemporal":                            {},
	"TestWaitForAttributeTemporal":                              {},
	"TestWaitForStateCompletionTemporal":                        {},
	"TestWaitUntilSearchAttributesOptimizationWorkflowTemporal": {},
	"TestWebAPICadence":                                         {},
	"TestWebAPITemporal":                                        {},
}

func requireTemporalOnlyScenario(t *testing.T) {
	t.Helper()
	testName, _, _ := strings.Cut(t.Name(), "/")
	requireTemporalOnlyScenarioName(t, testName)
}

func requireTemporalOnlyScenarioName(t *testing.T, testName string) {
	t.Helper()
	if *dexServerBinary == "" {
		return
	}
	if _, isTemporalOnly := temporalOnlyScenarioTests[testName]; isTemporalOnly {
		t.Skip("Temporal-only assertion")
	}
}
