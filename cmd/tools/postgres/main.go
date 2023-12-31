// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"github.com/xcherryio/xcherry/extensions/postgres/postgrestool"
	"log"
	"os"
)

func main() {
	app := postgrestool.BuildCLIOptions()

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}
