// Copyright (c) XDBLab
// SPDX-License-Identifier: BUSL-1.1

package tests

import (
	"fmt"
	"github.com/xdblab/xdb/persistence"
	"os"
	"testing"
	"time"

	"github.com/xdblab/xdb/common/log"
	"github.com/xdblab/xdb/config"
	"github.com/xdblab/xdb/extensions"
	"github.com/xdblab/xdb/extensions/postgres"
	"github.com/xdblab/xdb/extensions/postgres/postgrestool"

	"github.com/xdblab/xdb/persistence/sql"
)

var store persistence.ProcessStore

func TestMain(m *testing.M) {
	testDBName := fmt.Sprintf("test%v", time.Now().UnixNano())
	fmt.Println("using database name ", testDBName)

	sqlConfig := &config.SQL{
		ConnectAddr:     fmt.Sprintf("%v:%v", postgrestool.DefaultEndpoint, postgrestool.DefaultPort),
		User:            postgrestool.DefaultUserName,
		Password:        postgrestool.DefaultPassword,
		DBExtensionName: postgres.ExtensionName,
		DatabaseName:    testDBName,
	}

	err := extensions.CreateDatabase(*sqlConfig, testDBName)
	if err != nil {
		panic(err)
	}

	err = extensions.SetupSchema(sqlConfig, "../../../"+postgrestool.DefaultSchemaFilePath)
	if err != nil {
		panic(err)
	}
	err = extensions.SetupSchema(sqlConfig, "../../../"+postgrestool.SampleTablesSchemaFilePath)
	if err != nil {
		panic(err)
	}

	store, err = sql.NewSQLProcessStore(*sqlConfig, log.NewDevelopmentLogger())
	if err != nil {
		panic(err)
	}

	resultCode := m.Run()
	fmt.Println("finished running persistence test with status code", resultCode)

	_ = extensions.DropDatabase(*sqlConfig, testDBName)
	fmt.Println("testing database deleted")
	os.Exit(resultCode)
}
