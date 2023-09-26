package postgrestool

import (
	"github.com/urfave/cli/v2"
	"github.com/xdblab/xdb/extensions"
	"github.com/xdblab/xdb/extensions/postgres"
)

const defaultSQLPort = 5432

// BuildCLIOptions builds the options for cli
func BuildCLIOptions() *cli.App {

	app := cli.NewApp()

	app.Name = "xdb postgres tool"
	app.Usage = "tool for XDB operation on postgres"

	app.Flags = []cli.Flag{
		&cli.StringFlag{
			Name:  extensions.CLIFlagEndpoint,
			Value: "127.0.0.1",
			Usage: "hostname or ip address of sql host to connect to",
		},
		&cli.IntFlag{
			Name:  extensions.CLIFlagPort,
			Value: defaultSQLPort,
			Usage: "port of sql host to connect to",
		},
		&cli.StringFlag{
			Name:  extensions.CLIFlagUser,
			Value: "",
			Usage: "user name used for authentication when connecting to sql host",
		},
		&cli.StringFlag{
			Name:  extensions.CLIFlagPassword,
			Value: "",
			Usage: "password used for authentication when connecting to sql host",
		},
		&cli.StringFlag{
			Name:  extensions.CLIFlagDatabase,
			Value: "cadence",
			Usage: "name of the sql database",
		},
	}

	app.Commands = []*cli.Command{
		{
			Name:    "create-database",
			Aliases: []string{"create"},
			Usage:   "creates a database",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:  extensions.CLIFlagDatabase,
					Usage: "name of the database",
				},
			},
			Action: func(c *cli.Context) error {
				return extensions.CreateDatabase(c, postgres.ExtensionName)
			},
		},
	}

	return app
}
