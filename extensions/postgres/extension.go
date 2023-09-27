// Copyright (c) 2019 Uber Technologies, Inc.
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

package postgres

import (
	"fmt"
	"github.com/xdblab/xdb/config"
	"github.com/xdblab/xdb/extensions"
	"net"
	"net/url"

	"github.com/iancoleman/strcase"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq" // load the SQL driver for postgres
)

const (
	dsnFmt = "postgres://%s@%s:%s/%s"
)

type extension struct{}

var _ extensions.DBExtension = (*extension)(nil)

func init() {
	extensions.RegisterDBExtension(ExtensionName, &extension{})
}

func (d *extension) StartAdminDBSession(cfg *config.SQL) (extensions.AdminDBSession, error) {
	conns, err := d.createSingleDBConn(cfg)
	if err != nil {
		return nil, err
	}
	return newAdminDBSession(conns), nil
}

// CreateDBConnection creates a returns a reference to a logical connection to the
// underlying SQL database. The returned object is to tied to a single
// SQL database and the object can be used to perform CRUD operations on
// the tables in the database
func (d *extension) createSingleDBConn(cfg *config.SQL) (*sqlx.DB, error) {
	host, port, err := net.SplitHostPort(cfg.ConnectAddr)
	if err != nil {
		return nil, fmt.Errorf("invalid connect address, it must be in host:port format, %v, err: %v", cfg.ConnectAddr, err)
	}

	// TODO there are a lot more config we need to support like in Cadence
	// https://github.com/uber/cadence/blob/2df19da3d4c6fdfd74a54a6df43447883e3d3567/common/persistence/sql/sqlplugin/postgres/plugin.go#L138
	sslParams := url.Values{}
	sslParams.Set("sslmode", "disable")
	db, err := sqlx.Connect(ExtensionName, buildDSN(cfg, host, port, sslParams))
	if err != nil {
		return nil, err
	}

	// Maps struct names in CamelCase to snake without need for db struct tags.
	db.MapperFunc(strcase.ToSnake)
	return db, nil
}

func buildDSN(cfg *config.SQL, host string, port string, params url.Values) string {
	dbName := cfg.DatabaseName
	//NOTE: postgres doesn't allow to connect with empty dbName, the admin dbName is "postgres"
	if dbName == "" {
		dbName = "postgres"
	}

	credentialString := generateCredentialString(cfg.User, cfg.Password)
	dsn := fmt.Sprintf(dsnFmt, credentialString, host, port, dbName)
	if attrs := params.Encode(); attrs != "" {
		dsn += "?" + attrs
	}
	return dsn
}

func generateCredentialString(user string, password string) string {
	userPass := url.PathEscape(user)
	if password != "" {
		userPass += ":" + url.PathEscape(password)
	}
	return userPass
}
