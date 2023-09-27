package postgrestool

import (
	"github.com/urfave/cli/v2"
	"github.com/xdblab/xdb/extensions"
	"github.com/xdblab/xdb/extensions/postgres"
)

const DefaultEndpoint = "127.0.0.1"
const DefaultPort = 5432
const DefaultUserName = "xdb"
const DefaultPassword = "xdbxdb"
const DefaultDatabaseName = "xdb"
const DefaultSchemaFilePath = "./extensions/postgres/schema/all_in_one.sql"

// BuildCLIOptions builds the options for cli
func BuildCLIOptions() *cli.App {

	app := cli.NewApp()

	app.Name = "xdb postgres tool"
	app.Usage = "tool for XDB operation on postgres"

	app.Flags = []cli.Flag{
		&cli.StringFlag{
			Name:    extensions.CLIFlagEndpoint,
			Aliases: []string{"e"},
			Value:   DefaultEndpoint,
			Usage:   "hostname or ip address of sql host to connect to postgres",
		},
		&cli.IntFlag{
			Name:    extensions.CLIFlagPort,
			Aliases: []string{"p"},
			Value:   DefaultPort,
			Usage:   "port of sql host to connect to postgres",
		},
		&cli.StringFlag{
			Name:    extensions.CLIFlagUser,
			Aliases: []string{"u"},
			Value:   DefaultUserName,
			Usage:   "user name used for authentication when connecting to postgres",
		},
		&cli.StringFlag{
			Name:    extensions.CLIFlagPassword,
			Aliases: []string{"pw"},
			Value:   DefaultPassword,
			Usage:   "password used for authentication when connecting to postgres",
		},
		&cli.StringFlag{
			Name:    extensions.CLIFlagDatabase,
			Aliases: []string{"db"},
			Value:   DefaultDatabaseName,
			Usage:   "name of the postgres database",
		},
	}

	app.Commands = []*cli.Command{
		{
			Name:    "create-database",
			Aliases: []string{"create"},
			Usage:   "creates a database",
			Action: func(c *cli.Context) error {
				return extensions.CreateDatabaseByCli(c, postgres.ExtensionName)
			},
		},
		{
			Name:    "install-schema",
			Aliases: []string{"install"},
			Usage:   "install schema into a database",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:    extensions.CLIFlagFile,
					Aliases: []string{"f"},
					Value:   DefaultSchemaFilePath,
					Usage:   "file path of the schema file to install",
				},
			},
			Action: func(c *cli.Context) error {
				return extensions.SetupSchemaByCli(c, postgres.ExtensionName)
			},
		},
	}

	return app
}
