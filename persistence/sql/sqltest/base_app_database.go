// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: Apache-2.0

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

// nolint: funlen
func SQLAppDatabaseTest(t *testing.T, ass *assert.Assertions, store persistence.ProcessStore) {
	ctx := context.Background()

	processId := fmt.Sprintf("test-app-database-%v", time.Now().String())
	input := createTestInput()

	appDatabaseConfig := &xcapi.AppDatabaseConfig{
		Tables: []xcapi.AppDatabaseTableConfig{
			{
				TableName: "sample_user_table",
				Rows: []xcapi.AppDatabaseTableRowSelector{
					{
						PrimaryKey: []xcapi.AppDatabaseColumnValue{
							{
								Column:     "user_id_1",
								QueryValue: "user_pk_1_1",
							},
							{
								Column:     "user_id_2",
								QueryValue: "user_pk_1_2",
							},
						},
						InitialWrite: []xcapi.AppDatabaseColumnValue{
							{
								Column:     "first_name",
								QueryValue: "Quanzheng",
							},
							{
								Column:     "create_timestamp",
								QueryValue: "123",
							},
						},
						ConflictMode: xcapi.OVERRIDE_ON_CONFLICT.Ptr(),
					},
					{
						PrimaryKey: []xcapi.AppDatabaseColumnValue{
							{
								Column:     "user_id_1",
								QueryValue: "user_pk_2_1",
							},
							{
								Column:     "user_id_2",
								QueryValue: "user_pk_2_2",
							},
						},
						InitialWrite: []xcapi.AppDatabaseColumnValue{
							{
								Column:     "first_name",
								QueryValue: "Kaili",
							},
							{
								Column:     "create_timestamp",
								QueryValue: "456",
							},
						},
						ConflictMode: xcapi.RETURN_ERROR_ON_CONFLICT.Ptr(),
					},
				},
			},
			{
				TableName: "sample_order_table",
				Rows: []xcapi.AppDatabaseTableRowSelector{
					{
						PrimaryKey: []xcapi.AppDatabaseColumnValue{
							{
								Column:     "order_id",
								QueryValue: "123",
							},
						},
						InitialWrite: []xcapi.AppDatabaseColumnValue{
							{
								Column:     "item_name",
								QueryValue: "xcherry",
							},
						},
						ConflictMode: xcapi.IGNORE_CONFLICT.Ptr(),
					},
				},
			},
		},
	}

	appDatabaseReadReq1 := &xcapi.AppDatabaseReadRequest{
		Tables: []xcapi.AppDatabaseTableReadRequest{
			{
				TableName: ptr.Any("sample_user_table"),
				LockType:  xcapi.NO_LOCKING.Ptr(),
				Columns:   []string{"first_name", "create_timestamp"},
			},
			{
				TableName: ptr.Any("sample_order_table"),
				LockType:  xcapi.NO_LOCKING.Ptr(),
				Columns:   []string{"item_name", "sequence"},
			},
		},
	}
	stateCfg := &xcapi.AsyncStateConfig{
		SkipWaitUntil:          ptr.Any(true),
		AppDatabaseReadRequest: appDatabaseReadReq1,
	}
	// Start the process and verify it started correctly.
	prcExeId := startProcessWithConfigs(ctx, t, ass, store, namespace, processId, input, appDatabaseConfig, stateCfg)

	// Check initial immediate tasks.
	minSeq, maxSeq, immediateTasks := checkAndGetImmediateTasks(ctx, t, ass, store, 2)
	task := immediateTasks[0]
	verifyImmediateTaskNoInfo(ass, task, data_models.ImmediateTaskTypeExecute, stateId1+"-1")
	visibilityTask := immediateTasks[1]
	ass.Equal(data_models.ImmediateTaskTypeVisibility, visibilityTask.TaskType)

	// Delete and verify immediate tasks are deleted.
	deleteAndVerifyImmediateTasksDeleted(ctx, t, ass, store, minSeq, maxSeq)

	// Prepare state execution for first Execute API
	prep := prepareStateExecution(ctx, t, store, prcExeId, task.StateId, task.StateIdSequence)
	verifyStateExecution(ass, prep, processId, input, data_models.StateExecutionStatusExecuteRunning)

	readAppDatabaseReq, err := store.ReadAppDatabase(ctx, data_models.AppDatabaseReadRequest{
		AppDatabaseConfig: *prep.Info.AppDatabaseConfig,
		Request:           *prep.Info.StateConfig.AppDatabaseReadRequest,
	})
	require.NoError(t, err)

	expectedResp1 := xcapi.AppDatabaseReadResponse{
		Tables: []xcapi.AppDatabaseTableReadResponse{
			{
				TableName: ptr.Any("sample_user_table"),
				Rows: []xcapi.AppDatabaseRowReadResponse{
					{
						Columns: []xcapi.AppDatabaseColumnValue{
							{
								Column:     "first_name",
								QueryValue: "Quanzheng",
							},
							{
								Column:     "create_timestamp",
								QueryValue: "123",
							},
						},
					},
					{
						Columns: []xcapi.AppDatabaseColumnValue{
							{
								Column:     "first_name",
								QueryValue: "Kaili",
							},
							{
								Column:     "create_timestamp",
								QueryValue: "456",
							},
						},
					},
				},
			},
			{
				TableName: ptr.Any("sample_order_table"),
				Rows: []xcapi.AppDatabaseRowReadResponse{
					{
						Columns: []xcapi.AppDatabaseColumnValue{
							{
								Column:     "item_name",
								QueryValue: "xcherry",
							},
							{
								Column:     "sequence",
								QueryValue: "1",
							},
						},
					},
				},
			},
		},
	}

	assertProbablyEqualForIgnoringOrderByJsonEncoder(t, ass, expectedResp1, readAppDatabaseReq.Response)

	appDatabaseReadReq2 := &xcapi.AppDatabaseReadRequest{
		Tables: []xcapi.AppDatabaseTableReadRequest{
			{
				TableName: ptr.Any("sample_user_table"),
				LockType:  xcapi.NO_LOCKING.Ptr(),
				Columns:   []string{"first_name", "create_timestamp"},
			},
			{
				TableName: ptr.Any("sample_order_table"),
				LockType:  xcapi.NO_LOCKING.Ptr(),
				Columns:   []string{"item_name", "sequence"},
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
					SkipWaitUntil:          ptr.Any(true),
					AppDatabaseReadRequest: appDatabaseReadReq2,
				},
			},
		},
	}

	appDatabaseWrite := &xcapi.AppDatabaseWrite{
		Tables: []xcapi.AppDatabaseTableWrite{
			{
				TableName: "sample_user_table",
				Rows: []xcapi.AppDatabaseRowWrite{
					{
						PrimaryKey: []xcapi.AppDatabaseColumnValue{
							{
								Column:     "user_id_2",
								QueryValue: "user_pk_1_2",
							},
							{
								Column:     "user_id_1",
								QueryValue: "user_pk_1_1",
							},
						},
						WriteColumns: []xcapi.AppDatabaseColumnValue{
							{
								Column:     "first_name",
								QueryValue: "Long",
							},
						},
					},
					{
						PrimaryKey: []xcapi.AppDatabaseColumnValue{
							{
								Column:     "user_id_1",
								QueryValue: "user_pk_2_1",
							},
							{
								Column:     "user_id_2",
								QueryValue: "user_pk_2_2",
							},
						},
						WriteColumns: []xcapi.AppDatabaseColumnValue{
							{
								Column:     "first_name",
								QueryValue: "Zhu",
							},
						},
					},
				},
			},
			{
				TableName: "sample_order_table",
				Rows: []xcapi.AppDatabaseRowWrite{
					{
						PrimaryKey: []xcapi.AppDatabaseColumnValue{
							{
								Column:     "order_id",
								QueryValue: "123",
							},
						},
						WriteColumns: []xcapi.AppDatabaseColumnValue{
							{
								Column:     "item_name",
								QueryValue: "xcherryxcherry",
							},
						},
					},
				},
			},
		},
	}

	completeExecuteExecutionWithAppDatabase(
		ctx, t, ass, store, prcExeId, task, prep, decision1, true,
		prep.Info.AppDatabaseConfig, appDatabaseWrite)

	minSeq, maxSeq, immediateTasks = checkAndGetImmediateTasks(ctx, t, ass, store, 1)
	task = immediateTasks[0]
	verifyImmediateTaskNoInfo(ass, task, data_models.ImmediateTaskTypeExecute, stateId2+"-1")

	// Delete and verify immediate tasks are deleted.
	deleteAndVerifyImmediateTasksDeleted(ctx, t, ass, store, minSeq, maxSeq)

	prep = prepareStateExecution(ctx, t, store, prcExeId, task.StateId, task.StateIdSequence)
	verifyStateExecution(ass, prep, processId, input, data_models.StateExecutionStatusExecuteRunning)

	readAppDatabaseResp, err := store.ReadAppDatabase(ctx, data_models.AppDatabaseReadRequest{
		AppDatabaseConfig: *prep.Info.AppDatabaseConfig,
		Request:           *prep.Info.StateConfig.AppDatabaseReadRequest,
	})
	require.NoError(t, err)

	expectedResp2 := xcapi.AppDatabaseReadResponse{
		Tables: []xcapi.AppDatabaseTableReadResponse{
			{
				TableName: ptr.Any("sample_user_table"),
				Rows: []xcapi.AppDatabaseRowReadResponse{
					{
						Columns: []xcapi.AppDatabaseColumnValue{
							{
								Column:     "first_name",
								QueryValue: "Long",
							},
							{
								Column:     "create_timestamp",
								QueryValue: "123",
							},
						},
					},
					{
						Columns: []xcapi.AppDatabaseColumnValue{
							{
								Column:     "first_name",
								QueryValue: "Zhu",
							},
							{
								Column:     "create_timestamp",
								QueryValue: "456",
							},
						},
					},
				},
			},
			{
				TableName: ptr.Any("sample_order_table"),
				Rows: []xcapi.AppDatabaseRowReadResponse{
					{
						Columns: []xcapi.AppDatabaseColumnValue{
							{
								Column:     "item_name",
								QueryValue: "xcherryxcherry",
							},
							{
								Column:     "sequence",
								QueryValue: "1",
							},
						},
					},
				},
			},
		},
	}

	assertProbablyEqualForIgnoringOrderByJsonEncoder(t, ass, expectedResp2, readAppDatabaseResp.Response)

	decision2 := xcapi.StateDecision{
		ThreadCloseDecision: &xcapi.ThreadCloseDecision{
			CloseType: xcapi.FORCE_COMPLETE_PROCESS,
		},
	}
	completeExecuteExecution(
		ctx, t, ass, store, prcExeId, task, prep, decision2, true)
	_, _, immediateTasks = checkAndGetImmediateTasks(ctx, t, ass, store, 1)
	visibilityTask = immediateTasks[0]
	ass.Equal(data_models.ImmediateTaskTypeVisibility, visibilityTask.TaskType)
	describeProcess(ctx, t, ass, store, namespace, processId, xcapi.COMPLETED)
}
