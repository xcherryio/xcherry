// Copyright (c) 2023 xCherryIO Organization
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"log"
	"os"

	"github.com/urfave/cli/v2"
	"github.com/xcherryio/xcherry/cmd/server/bootstrap"

	_ "github.com/xcherryio/xcherry/extensions/postgres" // import postgres extension
)

func main() {
	app := &cli.App{
		Name:  "xCherry server",
		Usage: "start the xCherry server",
		Action: func(c *cli.Context) error {
			bootstrap.StartXCherryServerCli(c)
			return nil
		},
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  bootstrap.FlagConfig,
				Value: "./config/development-postgres.yaml",
				Usage: "the config to start xCherry server",
			},
			&cli.StringFlag{
				Name:  bootstrap.FlagService,
				Value: fmt.Sprintf("%v,%v", bootstrap.ApiServiceName, bootstrap.AsyncServiceName),
				Usage: "the services to start, separated by comma",
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}
