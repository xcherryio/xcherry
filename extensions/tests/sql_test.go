package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/jmoiron/sqlx/types"
	"github.com/stretchr/testify/assert"
	"github.com/xdblab/xdb/common/ptr"
	"github.com/xdblab/xdb/config"
	"github.com/xdblab/xdb/extensions"
	"github.com/xdblab/xdb/extensions/postgres"
	"github.com/xdblab/xdb/extensions/postgres/postgrestool"
	"github.com/xdblab/xdb/persistence/uuid"
	"testing"
	"time"
)

func TestPostgres(t *testing.T) {
	testDBName := fmt.Sprintf("test%v", time.Now().UnixNano())
	fmt.Println("using database name ", testDBName)

	sqlConfig := &config.SQL{
		ConnectAddr:     fmt.Sprintf("%v:%v", postgrestool.DefaultEndpoint, postgrestool.DefaultPort),
		User:            postgrestool.DefaultUserName,
		Password:        postgrestool.DefaultPassword,
		DBExtensionName: postgres.ExtensionName,
		DatabaseName:    testDBName,
	}
	ass := assert.New(t)

	err := extensions.CreateDatabase(*sqlConfig, testDBName)
	ass.Nil(err)

	err = extensions.SetupSchema(sqlConfig, "../../"+postgrestool.DefaultSchemaFilePath)
	ass.Nil(err)

	session, err := extensions.NewSQLSession(sqlConfig)
	if err != nil {
		panic(err)
	}
	testSQL(ass, session)
}

func testSQL(ass *assert.Assertions, session extensions.SQLDBSession) {
	ctx := context.Background()
	// start process transaction
	txn, err := session.StartTransaction(ctx)
	ass.Nil(err)
	namespace := "test-ns"
	processId := fmt.Sprintf("test-prcid-%v", time.Now().String())
	prcExeId := uuid.MustNewUUID()
	err = txn.InsertCurrentProcessExecution(ctx, extensions.CurrentProcessExecutionRow{
		Namespace:          namespace,
		ProcessId:          processId,
		ProcessExecutionId: prcExeId,
	})
	ass.Nil(err)

	info, err := json.Marshal(extensions.ProcessExecutionInfoJson{
		ProcessType: "test-type",
		WorkerURL:   "test-url",
	})
	ass.Nil(err)
	stateIdSequenceJson, err := json.Marshal(extensions.StateExecutionIdSequenceJson{
		SequenceMap: map[string]int{
			"start-state": 1,
		},
	})
	ass.Nil(err)
	prcExeRow := extensions.ProcessExecutionRow{
		ProcessExecutionRowForUpdate: extensions.ProcessExecutionRowForUpdate{
			ProcessExecutionId:     prcExeId,
			IsCurrent:              true,
			Status:                 extensions.ProcessExecutionStatusRunning,
			HistoryEventIdSequence: 1,
			StateIdSequence:        stateIdSequenceJson,
		},
		Namespace:      namespace,
		ProcessId:      processId,
		StartTime:      time.Now(),
		TimeoutSeconds: 10,
		Info:           info,
	}
	err = txn.InsertProcessExecution(ctx, prcExeRow)
	ass.Nil(err)

	inputJson, err := json.Marshal(extensions.EncodedDataJson{
		Encoding: ptr.Any("test-encoding"),
		Data:     ptr.Any("test-data"),
	})
	ass.Nil(err)
	stateExeInfo, err := json.Marshal(extensions.AsyncStateExecutionInfoJson{})
	ass.Nil(err)

	startStateId := "init-state"
	stateIdSequence := int32(0)
	stateRow := extensions.AsyncStateExecutionRow{
		AsyncStateExecutionRowForUpdate: extensions.AsyncStateExecutionRowForUpdate{
			AsyncStateExecutionSelectFilter: extensions.AsyncStateExecutionSelectFilter{
				ProcessExecutionId: prcExeId,
				StateId:            startStateId,
				StateIdSequence:    stateIdSequence,
			},
			WaitUntilStatus: extensions.StateExecutionStatusRunning,
			ExecuteStatus:   extensions.StateExecutionStatusUndefined,
			PreviousVersion: 0,
		},
		Info:  stateExeInfo,
		Input: inputJson,
	}
	err = txn.InsertAsyncStateExecution(ctx, stateRow)
	ass.Nil(err)

	workerTaskRow := extensions.WorkerTaskRowForInsert{
		ShardId:            extensions.DefaultShardId,
		TaskType:           extensions.WorkerTaskTypeWaitUntil,
		ProcessExecutionId: prcExeId,
		StateId:            startStateId,
		StateIdSequence:    stateIdSequence,
	}
	err = txn.InsertWorkerTask(ctx, workerTaskRow)
	ass.Nil(err)

	err = txn.Commit()
	ass.Nil(err)

	row, err := session.SelectCurrentProcessExecution(ctx, namespace, processId)
	ass.Nil(err)

	assertTimeEqual(ass, prcExeRow.StartTime, row.StartTime)
	assertJsonValueEqual(ass, prcExeRow.Info, row.Info, extensions.ProcessExecutionInfoJson{}, extensions.ProcessExecutionInfoJson{})
	assertJsonValueEqual(ass, prcExeRow.StateIdSequence, row.StateIdSequence, extensions.StateExecutionIdSequenceJson{}, extensions.StateExecutionIdSequenceJson{})
	row.StartTime = prcExeRow.StartTime
	row.Info = prcExeRow.Info
	row.StateIdSequence = prcExeRow.StateIdSequence
	ass.Equal(&prcExeRow, row)

	// test insert the same processId
	txn2, err := session.StartTransaction(ctx)
	ass.Nil(err)
	err = txn2.InsertCurrentProcessExecution(ctx, extensions.CurrentProcessExecutionRow{
		Namespace:          namespace,
		ProcessId:          processId,
		ProcessExecutionId: prcExeId,
	})
	ass.True(session.IsDupEntryError(err))
}

func assertTimeEqual(assertions *assert.Assertions, t1, t2 time.Time) {
	assertions.Equal(t1.UnixNano(), t2.UnixNano())
}

func assertJsonValueEqual(assertions *assert.Assertions, v1, v2 types.JSONText, t1, t2 interface{}) {
	err := json.Unmarshal(v1, &t1)
	assertions.Nil(err)
	err = json.Unmarshal(v2, &t2)
	assertions.Nil(err)
	assertions.Equal(t1, t2)
}
