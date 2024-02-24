// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: Apache-2.0

package tests

import (
	"fmt"
	"github.com/xcherryio/xcherry/persistence"
	"os"
	"testing"
	"time"

	"github.com/xcherryio/xcherry/common/log"
	"github.com/xcherryio/xcherry/config"
	"github.com/xcherryio/xcherry/extensions"
	"github.com/xcherryio/xcherry/extensions/postgres"
	"github.com/xcherryio/xcherry/extensions/postgres/postgrestool"

	"github.com/xcherryio/xcherry/persistence/process"
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

	store, err = process.NewSQLProcessStore(*sqlConfig, log.NewDevelopmentLogger())
	if err != nil {
		panic(err)
	}

	resultCode := m.Run()
	fmt.Println("finished running persistence test with status code", resultCode)

	_ = extensions.DropDatabase(*sqlConfig, testDBName)
	fmt.Println("testing database deleted")
	os.Exit(resultCode)
}
