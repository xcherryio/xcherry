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

package sqltest

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/xdblab/xdb-apis/goapi/xdbapi"
	"github.com/xdblab/xdb/common/ptr"
	"github.com/xdblab/xdb/persistence"
)

func SQLGlobalAttributesTest(t *testing.T, ass *assert.Assertions, store persistence.ProcessStore) {
	ctx := context.Background()
	namespace := "test-ns"
	processId := fmt.Sprintf("test-prcid-%v", time.Now().String())
	input := createTestInput()

	gloAttCfg := &xdbapi.GlobalAttributeConfig{
		TableConfigs: []xdbapi.GlobalAttributeTableConfig{
			{
				TableName: "sample_user_table",
				PrimaryKey: xdbapi.TableColumnValue{
					DbColumn:     "user_id",
					DbQueryValue: processId,
				},
				InitialWrite: []xdbapi.TableColumnValue{
					{
						DbColumn:     "first_name",
						DbQueryValue: "Quanzheng",
					},
					{
						DbColumn:     "create_timestamp",
						DbQueryValue: "123",
					},
				},
				InitialWriteMode: xdbapi.OVERRIDE_ON_CONFLICT.Ptr(),
			},
			{
				TableName: "sample_order_table",
				PrimaryKey: xdbapi.TableColumnValue{
					DbColumn:     "order_id",
					DbQueryValue: "123",
				},
				InitialWrite: []xdbapi.TableColumnValue{
					{
						DbColumn:     "item_name",
						DbQueryValue: "xdb",
					},
				},
				InitialWriteMode: xdbapi.IGNORE_CONFLICT.Ptr(),
			},
		},
	}

	loadReq1 := &xdbapi.LoadGlobalAttributesRequest{
		TableRequests: []xdbapi.TableReadRequest{
			{
				TableName:     ptr.Any("sample_user_table"),
				LockingPolicy: xdbapi.NO_LOCKING.Ptr(),
				Columns: []xdbapi.TableColumnDef{
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
				LockingPolicy: xdbapi.NO_LOCKING.Ptr(),
				Columns: []xdbapi.TableColumnDef{
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
	stateCfg := &xdbapi.AsyncStateConfig{
		SkipWaitUntil:               ptr.Any(true),
		LoadGlobalAttributesRequest: loadReq1,
	}
	// Start the process and verify it started correctly.
	prcExeId := startProcessWithConfigs(ctx, t, ass, store, namespace, processId, input, gloAttCfg, stateCfg)

	// Check initial immediate tasks.
	minSeq, maxSeq, immediateTasks := checkAndGetImmediateTasks(ctx, t, ass, store, 1)
	task := immediateTasks[0]
	verifyImmediateTaskNoInfo(ass, task, persistence.ImmediateTaskTypeExecute, stateId1+"-1")

	// Delete and verify immediate tasks are deleted.
	deleteAndVerifyImmediateTasksDeleted(ctx, t, ass, store, minSeq, maxSeq)

	// Prepare state execution for first Execute API
	prep := prepareStateExecution(ctx, t, store, prcExeId, task.StateId, task.StateIdSequence)
	verifyStateExecution(ass, prep, processId, input, persistence.StateExecutionStatusExecuteRunning)

	gloAttResp, err := store.LoadGlobalAttributes(ctx, persistence.LoadGlobalAttributesRequest{
		TableConfig: *prep.Info.GlobalAttributeConfig,
		Request:     *prep.Info.StateConfig.LoadGlobalAttributesRequest,
	})
	require.NoError(t, err)
	expectedResp1 := xdbapi.LoadGlobalAttributeResponse{
		TableResponses: []xdbapi.TableReadResponse{
			{
				TableName: ptr.Any("sample_user_table"),
				Columns: []xdbapi.TableColumnValue{
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
				Columns: []xdbapi.TableColumnValue{
					{
						DbColumn:     "item_name",
						DbQueryValue: "xdb",
					},
					{
						DbColumn:     "sequence",
						DbQueryValue: "1",
					},
				},
			},
		},
	}
	ass.Equal(expectedResp1, gloAttResp.Response)

	loadReq2 := &xdbapi.LoadGlobalAttributesRequest{
		TableRequests: []xdbapi.TableReadRequest{
			{
				TableName:     ptr.Any("sample_user_table"),
				LockingPolicy: xdbapi.NO_LOCKING.Ptr(),
				Columns: []xdbapi.TableColumnDef{
					{
						DbColumn: "first_name",
					},
				},
			},
			{
				TableName:     ptr.Any("sample_order_table"),
				LockingPolicy: xdbapi.NO_LOCKING.Ptr(),
				Columns: []xdbapi.TableColumnDef{
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
	decision1 := xdbapi.StateDecision{
		NextStates: []xdbapi.StateMovement{
			{
				StateId:    stateId2,
				StateInput: &input,
				// no input, skip waitUntil
				StateConfig: &xdbapi.AsyncStateConfig{
					SkipWaitUntil:               ptr.Any(true),
					LoadGlobalAttributesRequest: loadReq2,
				},
			},
		},
	}

	updates := []xdbapi.GlobalAttributeTableRowUpdate{
		{
			TableName: "sample_user_table",
			UpdateColumns: []xdbapi.TableColumnValue{
				{
					DbColumn:     "first_name",
					DbQueryValue: "Long",
				},
			},
		},
		{
			TableName: "sample_order_table",
			UpdateColumns: []xdbapi.TableColumnValue{
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
	verifyImmediateTaskNoInfo(ass, task, persistence.ImmediateTaskTypeExecute, stateId2+"-1")

	// Delete and verify immediate tasks are deleted.
	deleteAndVerifyImmediateTasksDeleted(ctx, t, ass, store, minSeq, maxSeq)

	prep = prepareStateExecution(ctx, t, store, prcExeId, task.StateId, task.StateIdSequence)
	verifyStateExecution(ass, prep, processId, input, persistence.StateExecutionStatusExecuteRunning)

	gloAttResp, err = store.LoadGlobalAttributes(ctx, persistence.LoadGlobalAttributesRequest{
		TableConfig: *prep.Info.GlobalAttributeConfig,
		Request:     *prep.Info.StateConfig.LoadGlobalAttributesRequest,
	})
	require.NoError(t, err)
	expectedResp2 := xdbapi.LoadGlobalAttributeResponse{
		TableResponses: []xdbapi.TableReadResponse{
			{
				TableName: ptr.Any("sample_user_table"),
				Columns: []xdbapi.TableColumnValue{
					{
						DbColumn:     "first_name",
						DbQueryValue: "Long",
					},
				},
			},
			{
				TableName: ptr.Any("sample_order_table"),
				Columns: []xdbapi.TableColumnValue{
					{
						DbColumn:     "item_name",
						DbQueryValue: "xdb",
					},
					{
						DbColumn:     "create_timestamp",
						DbQueryValue: "456",
					},
				},
			},
		},
	}
	ass.Equal(expectedResp2, gloAttResp.Response)

	decision2 := xdbapi.StateDecision{
		ThreadCloseDecision: &xdbapi.ThreadCloseDecision{
			CloseType: xdbapi.FORCE_COMPLETE_PROCESS,
		},
	}
	completeExecuteExecution(
		ctx, t, ass, store, prcExeId, task, prep, decision2, false)
	checkAndGetImmediateTasks(ctx, t, ass, store, 0)
	describeProcess(ctx, t, ass, store, namespace, processId, xdbapi.COMPLETED)
}
