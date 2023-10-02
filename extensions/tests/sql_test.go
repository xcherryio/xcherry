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
	sqlConfig := &config.SQL{
		ConnectAddr:     fmt.Sprintf("%v:%v", postgrestool.DefaultEndpoint, postgrestool.DefaultPort),
		User:            postgrestool.DefaultUserName,
		Password:        postgrestool.DefaultPassword,
		DBExtensionName: postgres.ExtensionName,
		DatabaseName:    postgrestool.DefaultDatabaseName,
	}
	session, err := extensions.NewSQLSession(sqlConfig)
	if err != nil {
		panic(err)
	}
	testSQL(assert.New(t), session)
}

func testSQL(assertions *assert.Assertions, session extensions.SQLDBSession) {
	ctx := context.Background()
	// start process transaction
	txn, err := session.StartTransaction(ctx)
	assertions.Nil(err)
	namespace := "test-ns"
	processId := fmt.Sprintf("test-prcid-%v", time.Now().String())
	prcExeId := uuid.MustNewUUID()
	err = txn.InsertCurrentProcessExecution(ctx, extensions.CurrentProcessExecutionRow{
		Namespace:          namespace,
		ProcessId:          processId,
		ProcessExecutionId: prcExeId,
	})
	assertions.Nil(err)

	info, err := json.Marshal(extensions.ProcessExecutionInfoJson{
		ProcessType: "test-type",
		WorkerURL:   "test-url",
	})
	assertions.Nil(err)
	stateIdSequenceJson, err := json.Marshal(extensions.StateExecutionIdSequenceJson{
		SequenceMap: map[string]int{
			"start-state": 1,
		},
	})
	assertions.Nil(err)
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
	assertions.Nil(err)

	inputJson, err := json.Marshal(extensions.EncodedDataJson{
		Encoding: ptr.Any("test-encoding"),
		Data:     ptr.Any("test-data"),
	})
	assertions.Nil(err)
	stateExeInfo, err := json.Marshal(extensions.AsyncStateExecutionInfoJson{})
	assertions.Nil(err)

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
	assertions.Nil(err)

	workerTaskRow := extensions.WorkerTaskRowForInsert{
		ShardId:            extensions.DefaultShardId,
		TaskType:           extensions.WorkerTaskTypeWaitUntil,
		ProcessExecutionId: prcExeId,
		StateId:            startStateId,
		StateIdSequence:    stateIdSequence,
	}
	err = txn.InsertWorkerTask(ctx, workerTaskRow)
	assertions.Nil(err)

	err = txn.Commit()
	assertions.Nil(err)

	row, err := session.SelectCurrentProcessExecution(ctx, namespace, processId)
	assertions.Nil(err)

	assertTimeEqual(assertions, prcExeRow.StartTime, row.StartTime)
	assertJsonValueEqual(assertions, prcExeRow.Info, row.Info, extensions.ProcessExecutionInfoJson{}, extensions.ProcessExecutionInfoJson{})
	assertJsonValueEqual(assertions, prcExeRow.StateIdSequence, row.StateIdSequence, extensions.StateExecutionIdSequenceJson{}, extensions.StateExecutionIdSequenceJson{})
	row.StartTime = prcExeRow.StartTime
	row.Info = prcExeRow.Info
	row.StateIdSequence = prcExeRow.StateIdSequence
	assertions.Equal(&prcExeRow, row)
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
