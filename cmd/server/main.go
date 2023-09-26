package main

import (
	"fmt"
	"github.com/urfave/cli/v2"
	"github.com/xdblab/xdb/cmd/server/bootstrap"
	"log"
	"os"
)

func main() {
	app := &cli.App{
		Name:  "xdb server",
		Usage: "start the xdb server",
		Action: func(c *cli.Context) error {
			bootstrap.StartXdbServer(c)
			return nil
		},
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  bootstrap.FlagConfig,
				Value: "./config/development.yaml",
				Usage: "the config to start xdb server",
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
