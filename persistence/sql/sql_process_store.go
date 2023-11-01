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

package sql

import (
	"context"
	"database/sql"
	"github.com/xdblab/xdb-apis/goapi/xdbapi"

	"github.com/xdblab/xdb/common/log"
	"github.com/xdblab/xdb/config"
	"github.com/xdblab/xdb/extensions"
	"github.com/xdblab/xdb/persistence"
)

type sqlProcessStoreImpl struct {
	session extensions.SQLDBSession
	logger  log.Logger
}

var defaultTxOpts *sql.TxOptions = &sql.TxOptions{
	Isolation: sql.LevelReadCommitted,
}

func NewSQLProcessStore(sqlConfig config.SQL, logger log.Logger) (persistence.ProcessStore, error) {
	session, err := extensions.NewSQLSession(&sqlConfig)
	return &sqlProcessStoreImpl{
		session: session,
		logger:  logger,
	}, err
}

func (p sqlProcessStoreImpl) Close() error {
	return p.session.Close()
}

func (p sqlProcessStoreImpl) updateGlobalAttributesIfNeeded(
	ctx context.Context, tx extensions.SQLTransaction, tableConfig *persistence.InternalGlobalAttributeConfig,
	updates []xdbapi.GlobalAttributeTableRowUpdate,
) error {
	if len(updates) > 0 {
		// TODO
	}
	return nil
}
