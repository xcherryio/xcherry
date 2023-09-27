// Copyright (c) 2017 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package extensions

import (
	"context"
	"fmt"
	"github.com/urfave/cli/v2"
	"github.com/xdblab/xdb/config"
	"io/ioutil"
	"net"
)

// SetupSchemaByCli setup schema for a new database
func SetupSchemaByCli(cli *cli.Context, extensionName string) error {
	cfg, err := parseConnectConfig(cli, extensionName)
	if err != nil {
		panic(err)
	}
	filePath := cli.String(CLIFlagFile)
	return SetupSchema(cfg, filePath)
}

func SetupSchema(cfg *config.SQL, filePath string) error {
	content, err := ioutil.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("error reading contents of file %v:%v", filePath, err.Error())
	}

	adminSession, err := NewSQLAdminSession(cfg)
	if err != nil {
		return err
	}
	defer adminSession.Close()

	return adminSession.ExecuteSchemaDDL(context.Background(), string(content))
}

// CreateDatabaseByCli creates a sql database
func CreateDatabaseByCli(cli *cli.Context, extensionName string) error {
	cfg, err := parseConnectConfig(cli, extensionName)
	if err != nil {
		panic(err)
	}
	database := cli.String(CLIFlagDatabase)
	return CreateDatabase(*cfg, database)
}

func CreateDatabase(cfg config.SQL, name string) error {
	// cfg config.SQL will be modified as this cannot using pointer "cfg *config.SQL
	cfg.DatabaseName = ""
	// IMPORTATNT! set empty because the database is to be created(not exists yet). It's up to the extension to handle it
	// e.g.:
	// MySQL just use an account like root
	// Postgres will set it to postgres

	adminSession, err := NewSQLAdminSession(&cfg)
	if err != nil {
		return err
	}
	defer adminSession.Close()
	return adminSession.CreateDatabase(context.Background(), name)
}

func DropDatabase(cfg config.SQL, name string) error {
	cfg.DatabaseName = "" // similar as CreateDatabase, in Postgres, all connections must be closed before deleting a database
	adminSession, err := NewSQLAdminSession(&cfg)
	if err != nil {
		return err
	}
	defer adminSession.Close()
	return adminSession.DropDatabase(context.Background(), name)
}

func parseConnectConfig(cli *cli.Context, extensionName string) (*config.SQL, error) {
	cfg := new(config.SQL)

	host := cli.String(CLIFlagEndpoint)
	port := cli.Int(CLIFlagPort)
	cfg.ConnectAddr = fmt.Sprintf("%s:%v", host, port)
	cfg.User = cli.String(CLIFlagUser)
	cfg.Password = cli.String(CLIFlagPassword)
	cfg.DatabaseName = cli.String(CLIFlagDatabase)
	cfg.DBExtensionName = extensionName

	if err := ValidateConnectConfig(cfg); err != nil {
		return nil, err
	}

	return cfg, nil
}

// ValidateConnectConfig validates params
func ValidateConnectConfig(cfg *config.SQL) error {
	host, _, err := net.SplitHostPort(cfg.ConnectAddr)
	if err != nil {
		return fmt.Errorf("invalid host and port " + cfg.ConnectAddr)
	}
	if len(host) == 0 {
		return fmt.Errorf("missing sql endpoint argument " + flag(CLIFlagEndpoint))
	}
	if cfg.DatabaseName == "" {
		return fmt.Errorf("missing " + flag(CLIFlagDatabase) + " argument")
	}
	return nil
}

func flag(opt string) string {
	return "(-" + opt + ")"
}
