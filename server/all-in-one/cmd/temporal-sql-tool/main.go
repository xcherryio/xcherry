// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package main

import (
	"os"

	_ "github.com/superdurable/xtemporal/common/persistence/sql/sqlplugin/postgresql"
	"github.com/superdurable/xtemporal/tools/sql"
)

func main() {
	if err := sql.RunTool(os.Args); err != nil {
		os.Exit(1)
	}
}
