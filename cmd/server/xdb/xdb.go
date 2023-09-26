package xdb

import (
	"fmt"
	rawLog "log"

	"github.com/xdblab/xdb/service/api"
	"github.com/xdblab/xdb/service/common/config"
	"github.com/xdblab/xdb/service/common/log"
	"github.com/xdblab/xdb/service/common/log/tag"
)

const serviceAPI = "api"

// BuildCLI is the main entry point for the xdb server
func BuildCLI() *cli.App {
	app := cli.NewApp()
	app.Name = "xdb service"
	app.Usage = "xdb service"
	app.Version = "beta"

	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "config, c",
			Value: "cmd/config/development.yaml",
			Usage: "config path is a path relative to root, or an absolute path",
		},
	}

	app.Commands = []cli.Command{
		{
			Name:    "start",
			Aliases: []string{""},
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "services",
					Value: fmt.Sprintf("%s", serviceAPI),
					Usage: "start services/components in this project",
				},
			},
			Usage:  "start xdb notification service",
			Action: start,
		},
	}
	return app
}

func start(c *cli.Context) {
	configPath := c.GlobalString("config")
	config, err := config.NewConfig(configPath)
	if err != nil {
		rawLog.Fatalf("Unable to load config for path %v because of error %v", configPath, err)
	}
	zapLogger, err := config.Log.NewZapLogger()
	if err != nil {
		rawLog.Fatalf("Unable to create a new zap logger %v", err)
	}
	logger := log.NewLogger(zapLogger)

	launchService(serviceAPI, *config, logger)
}

func launchService(svcName string, config config.Config, logger log.Logger) {
	apiService := api.NewService(config, logger.WithTags(tag.Service(svcName)))
	apiService.Run(fmt.Sprintf(":%v", config.Api.Port))
}
