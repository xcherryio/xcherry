package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
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
	err = txn.InsertProcessExecution(ctx, extensions.ProcessExecutionRow{
		ProcessExecutionRowForUpdate: extensions.ProcessExecutionRowForUpdate{
			ProcessExecutionId:     prcExeId,
			IsCurrent:              true,
			Status:                 extensions.ExecutionStatusRunning,
			HistoryEventIdSequence: 1,
			StateIdSequence:        stateIdSequenceJson,
		},
		Namespace:      namespace,
		ProcessId:      processId,
		StartTime:      time.Now(),
		TimeoutSeconds: 10,
		Info:           info,
	})
	assertions.Nil(err)

	err = txn.Commit()
	assertions.Nil(err)
}
