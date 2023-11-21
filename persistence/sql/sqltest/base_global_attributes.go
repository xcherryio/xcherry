// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: BUSL-1.1

package sqltest

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/require"
	"github.com/xcherryio/apis/goapi/xcapi"
	"github.com/xcherryio/xcherry/persistence/data_models"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/xcherryio/xcherry/common/ptr"
	"github.com/xcherryio/xcherry/persistence"
)

func SQLGlobalAttributesTest(t *testing.T, ass *assert.Assertions, store persistence.ProcessStore) {
	ctx := context.Background()

	processId := fmt.Sprintf("test-prcid-%v", time.Now().String())
	input := createTestInput()

	gloAttCfg := &xcapi.GlobalAttributeConfig{
		TableConfigs: []xcapi.GlobalAttributeTableConfig{
			{
				TableName: "sample_user_table",
				PrimaryKey: xcapi.TableColumnValue{
					DbColumn:     "user_id",
					DbQueryValue: processId,
				},
				InitialWrite: []xcapi.TableColumnValue{
					{
						DbColumn:     "first_name",
						DbQueryValue: "Quanzheng",
					},
					{
						DbColumn:     "create_timestamp",
						DbQueryValue: "123",
					},
				},
				InitialWriteMode: xcapi.OVERRIDE_ON_CONFLICT.Ptr(),
			},
			{
				TableName: "sample_order_table",
				PrimaryKey: xcapi.TableColumnValue{
					DbColumn:     "order_id",
					DbQueryValue: "123",
				},
				InitialWrite: []xcapi.TableColumnValue{
					{
						DbColumn:     "item_name",
						DbQueryValue: "xcherry",
					},
				},
				InitialWriteMode: xcapi.IGNORE_CONFLICT.Ptr(),
			},
		},
	}

	loadReq1 := &xcapi.LoadGlobalAttributesRequest{
		TableRequests: []xcapi.TableReadRequest{
			{
				TableName:     ptr.Any("sample_user_table"),
				LockingPolicy: xcapi.NO_LOCKING.Ptr(),
				Columns: []xcapi.TableColumnDef{
					{
						DbColumn: "first_name",
					},
					{
						DbColumn: "create_timestamp",
					},
				},
			},
			{
				TableName:     ptr.Any("sample_order_table"),
				LockingPolicy: xcapi.NO_LOCKING.Ptr(),
				Columns: []xcapi.TableColumnDef{
					{
						DbColumn: "item_name",
					},
					{
						DbColumn: "sequence",
					},
				},
			},
		},
	}
	stateCfg := &xcapi.AsyncStateConfig{
		SkipWaitUntil:               ptr.Any(true),
		LoadGlobalAttributesRequest: loadReq1,
	}
	// Start the process and verify it started correctly.
	prcExeId := startProcessWithConfigs(ctx, t, ass, store, namespace, processId, input, gloAttCfg, stateCfg)

	// Check initial immediate tasks.
	minSeq, maxSeq, immediateTasks := checkAndGetImmediateTasks(ctx, t, ass, store, 1)
	task := immediateTasks[0]
	verifyImmediateTaskNoInfo(ass, task, data_models.ImmediateTaskTypeExecute, stateId1+"-1")

	// Delete and verify immediate tasks are deleted.
	deleteAndVerifyImmediateTasksDeleted(ctx, t, ass, store, minSeq, maxSeq)

	// Prepare state execution for first Execute API
	prep := prepareStateExecution(ctx, t, store, prcExeId, task.StateId, task.StateIdSequence)
	verifyStateExecution(ass, prep, processId, input, data_models.StateExecutionStatusExecuteRunning)

	gloAttResp, err := store.LoadGlobalAttributes(ctx, data_models.LoadGlobalAttributesRequest{
		TableConfig: *prep.Info.GlobalAttributeConfig,
		Request:     *prep.Info.StateConfig.LoadGlobalAttributesRequest,
	})
	require.NoError(t, err)
	expectedResp1 := xcapi.LoadGlobalAttributeResponse{
		TableResponses: []xcapi.TableReadResponse{
			{
				TableName: ptr.Any("sample_user_table"),
				Columns: []xcapi.TableColumnValue{
					{
						DbColumn:     "first_name",
						DbQueryValue: "Quanzheng",
					},
					{
						DbColumn:     "create_timestamp",
						DbQueryValue: "123",
					},
				},
			},
			{
				TableName: ptr.Any("sample_order_table"),
				Columns: []xcapi.TableColumnValue{
					{
						DbColumn:     "item_name",
						DbQueryValue: "xcherry",
					},
					{
						DbColumn:     "sequence",
						DbQueryValue: "1",
					},
				},
			},
		},
	}

	assertProbablyEqualForIgnoringOrderByJsonEncoder(t, ass, expectedResp1, gloAttResp.Response)

	loadReq2 := &xcapi.LoadGlobalAttributesRequest{
		TableRequests: []xcapi.TableReadRequest{
			{
				TableName:     ptr.Any("sample_user_table"),
				LockingPolicy: xcapi.NO_LOCKING.Ptr(),
				Columns: []xcapi.TableColumnDef{
					{
						DbColumn: "first_name",
					},
				},
			},
			{
				TableName:     ptr.Any("sample_order_table"),
				LockingPolicy: xcapi.NO_LOCKING.Ptr(),
				Columns: []xcapi.TableColumnDef{
					{
						DbColumn: "item_name",
					},
					{
						DbColumn: "create_timestamp",
					},
				},
			},
		},
	}
	decision1 := xcapi.StateDecision{
		NextStates: []xcapi.StateMovement{
			{
				StateId:    stateId2,
				StateInput: &input,
				// no input, skip waitUntil
				StateConfig: &xcapi.AsyncStateConfig{
					SkipWaitUntil:               ptr.Any(true),
					LoadGlobalAttributesRequest: loadReq2,
				},
			},
		},
	}

	updates := []xcapi.GlobalAttributeTableRowUpdate{
		{
			TableName: "sample_user_table",
			UpdateColumns: []xcapi.TableColumnValue{
				{
					DbColumn:     "first_name",
					DbQueryValue: "Long",
				},
			},
		},
		{
			TableName: "sample_order_table",
			UpdateColumns: []xcapi.TableColumnValue{
				{
					DbColumn:     "create_timestamp",
					DbQueryValue: "456",
				},
			},
		},
	}

	completeExecuteExecutionWithGlobalAttributes(
		ctx, t, ass, store, prcExeId, task, prep, decision1, true,
		prep.Info.GlobalAttributeConfig, updates)

	minSeq, maxSeq, immediateTasks = checkAndGetImmediateTasks(ctx, t, ass, store, 1)
	task = immediateTasks[0]
	verifyImmediateTaskNoInfo(ass, task, data_models.ImmediateTaskTypeExecute, stateId2+"-1")

	// Delete and verify immediate tasks are deleted.
	deleteAndVerifyImmediateTasksDeleted(ctx, t, ass, store, minSeq, maxSeq)

	prep = prepareStateExecution(ctx, t, store, prcExeId, task.StateId, task.StateIdSequence)
	verifyStateExecution(ass, prep, processId, input, data_models.StateExecutionStatusExecuteRunning)

	gloAttResp, err = store.LoadGlobalAttributes(ctx, data_models.LoadGlobalAttributesRequest{
		TableConfig: *prep.Info.GlobalAttributeConfig,
		Request:     *prep.Info.StateConfig.LoadGlobalAttributesRequest,
	})
	require.NoError(t, err)
	expectedResp2 := xcapi.LoadGlobalAttributeResponse{
		TableResponses: []xcapi.TableReadResponse{
			{
				TableName: ptr.Any("sample_user_table"),
				Columns: []xcapi.TableColumnValue{
					{
						DbColumn:     "first_name",
						DbQueryValue: "Long",
					},
				},
			},
			{
				TableName: ptr.Any("sample_order_table"),
				Columns: []xcapi.TableColumnValue{
					{
						DbColumn:     "item_name",
						DbQueryValue: "xcherry",
					},
					{
						DbColumn:     "create_timestamp",
						DbQueryValue: "456",
					},
				},
			},
		},
	}

	assertProbablyEqualForIgnoringOrderByJsonEncoder(t, ass, expectedResp2, gloAttResp.Response)

	decision2 := xcapi.StateDecision{
		ThreadCloseDecision: &xcapi.ThreadCloseDecision{
			CloseType: xcapi.FORCE_COMPLETE_PROCESS,
		},
	}
	completeExecuteExecution(
		ctx, t, ass, store, prcExeId, task, prep, decision2, false)
	checkAndGetImmediateTasks(ctx, t, ass, store, 0)
	describeProcess(ctx, t, ass, store, namespace, processId, xcapi.COMPLETED)
}
