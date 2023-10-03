package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/jmoiron/sqlx/types"
	"github.com/stretchr/testify/assert"
	"github.com/xdblab/xdb/common/ptr"
	"github.com/xdblab/xdb/common/uuid"
	"github.com/xdblab/xdb/config"
	"github.com/xdblab/xdb/extensions"
	"github.com/xdblab/xdb/extensions/postgres"
	"github.com/xdblab/xdb/extensions/postgres/postgrestool"
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
	// TODO this test need to be refactored to be easier to read!!!!
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

		ProcessExecutionId:     prcExeId,
		IsCurrent:              true,
		Status:                 extensions.ProcessExecutionStatusRunning,
		HistoryEventIdSequence: 1,
		StateIdSequence:        stateIdSequenceJson,
		Namespace:              namespace,
		ProcessId:              processId,
		StartTime:              time.Now(),
		TimeoutSeconds:         10,
		Info:                   info,
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
	previousVersion := int32(1)
	stateIdSequence := int32(0)
	stateRow := extensions.AsyncStateExecutionRow{
		ProcessExecutionId: prcExeId,
		StateId:            startStateId,
		StateIdSequence:    stateIdSequence,
		WaitUntilStatus:    extensions.StateExecutionStatusRunning,
		ExecuteStatus:      extensions.StateExecutionStatusUndefined,
		PreviousVersion:    previousVersion,
		Info:               stateExeInfo,
		Input:              inputJson,
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
	ass.Nil(txn2.Rollback())

	// test select wrong id
	_, err = session.SelectCurrentProcessExecution(ctx, namespace, "a wrong id")
	ass.True(session.IsNotFoundError(err))

	// test select worker tasks
	workerTasks, err := session.BatchSelectWorkerTasks(ctx, extensions.DefaultShardId, 0, 1000)
	ass.Nil(err)
	ass.Equal(1, len(workerTasks))
	// TODO assert equal

	err = session.BatchDeleteWorkerTask(ctx, extensions.WorkerTaskRangeDeleteFilter{
		ShardId:                  extensions.DefaultShardId,
		MaxTaskSequenceInclusive: workerTasks[0].TaskSequence,
	})
	ass.Nil(err)
	workerTasks, err = session.BatchSelectWorkerTasks(ctx, extensions.DefaultShardId, 0, 1000)
	ass.Nil(err)
	ass.Equal(0, len(workerTasks))

	// async state execution
	stateRowSelect, err := session.SelectAsyncStateExecutionForUpdate(ctx, extensions.AsyncStateExecutionSelectFilter{
		ProcessExecutionId: prcExeId,
		StateId:            startStateId,
		StateIdSequence:    stateIdSequence,
	})
	ass.Nil(err)
	ass.Equal(previousVersion, stateRowSelect.PreviousVersion)

	txn3, err := session.StartTransaction(ctx)
	ass.Nil(err)
	stateRowSelect.PreviousVersion = 0 // override for error
	err = txn3.UpdateAsyncStateExecution(ctx, *stateRowSelect)
	ass.True(session.IsConditionalUpdateFailure(err))
	ass.Nil(txn3.Rollback())

	txn4, err := session.StartTransaction(ctx)
	ass.Nil(err)
	stateRowSelect.PreviousVersion = previousVersion
	stateRowSelect.WaitUntilStatus = extensions.StateExecutionStatusCompleted
	stateRowSelect.ExecuteStatus = extensions.StateExecutionStatusRunning
	err = txn4.UpdateAsyncStateExecution(ctx, *stateRowSelect)
	ass.Nil(err)
	ass.Nil(txn4.Commit())

	previousVersion++
	stateRowSelect, err = session.SelectAsyncStateExecutionForUpdate(ctx, extensions.AsyncStateExecutionSelectFilter{
		ProcessExecutionId: prcExeId,
		StateId:            startStateId,
		StateIdSequence:    stateIdSequence,
	})
	ass.Nil(err)
	ass.Equal(previousVersion, stateRowSelect.PreviousVersion)

	txn5, err := session.StartTransaction(ctx)
	ass.Nil(err)
	stateRowSelect.WaitUntilStatus = extensions.StateExecutionStatusCompleted
	stateRowSelect.ExecuteStatus = extensions.StateExecutionStatusCompleted
	err = txn5.UpdateAsyncStateExecution(ctx, *stateRowSelect)
	ass.Nil(err)
	prcRow2, err := txn5.SelectProcessExecutionForUpdate(ctx, prcExeId)
	ass.Nil(err)
	prcRow2.Status = extensions.ProcessExecutionStatusCompleted
	err = txn5.UpdateProcessExecution(ctx, *prcRow2)
	ass.Nil(err)
	ass.Nil(txn5.Commit())

	prcRow3, err := session.SelectCurrentProcessExecution(ctx, namespace, processId)
	ass.Nil(err)
	ass.Equal(extensions.ProcessExecutionStatusCompleted, prcRow3.Status)
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
