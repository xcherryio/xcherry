package tests

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/xdblab/xdb/common/log"
	"github.com/xdblab/xdb/config"
	"github.com/xdblab/xdb/extensions"
	"github.com/xdblab/xdb/extensions/postgres"
	"github.com/xdblab/xdb/extensions/postgres/postgrestool"
	"github.com/xdblab/xdb/persistence/sql"
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

	store, err := sql.NewSQLProcessStore(*sqlConfig, log.NewDevelopmentLogger())

	testSQLBasicExecution(ass, store)

	_ = extensions.DropDatabase(*sqlConfig, testDBName)
	fmt.Println("testing database deleted")
}
