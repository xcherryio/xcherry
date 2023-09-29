package bootstrap

import (
	"github.com/urfave/cli/v2"
	log2 "github.com/xdblab/xdb/common/log"
	"github.com/xdblab/xdb/common/log/tag"
	"github.com/xdblab/xdb/config"
	"github.com/xdblab/xdb/persistence"
	"github.com/xdblab/xdb/service/api"
	rawLog "log"
	"strings"
	"sync"
)

const ApiServiceName = "api"
const AsyncServiceName = "async"

const FlagConfig = "config"
const FlagService = "service"

func StartXdbServerCli(c *cli.Context) {
	configPath := c.String("config")
	services := getServices(c)

	cfg, err := config.NewConfig(configPath)
	if err != nil {
		rawLog.Fatalf("Unable to load config for path %v because of error %v", configPath, err)
	}
	StartXdbServer(cfg, services)

	// TODO improve by waiting for the started services to stop
	wg := sync.WaitGroup{}
	wg.Add(1)
	wg.Wait()
}

type StopFunc func() error

func StartXdbServer(cfg *config.Config, services map[string]bool) StopFunc {
	if len(services) == 0 {
		services = map[string]bool{ApiServiceName: true, AsyncServiceName: true}
	}

	zapLogger, err := cfg.Log.NewZapLogger()
	if err != nil {
		rawLog.Fatalf("Unable to create a new zap logger %v", err)
	}
	logger := log2.NewLogger(zapLogger)
	logger.Info("config is loaded", tag.Value(cfg.String()))
	err = cfg.Validate()
	if err != nil {
		logger.Fatal("config is invalid", tag.Error(err))
	}

	processOrm, err := persistence.NewProcessORMSQLImpl(*cfg.Database.SQL, logger)
	if err != nil {
		logger.Fatal("error on persistence setup", tag.Error(err))
	}

	if services[ApiServiceName] {
		go func() {
			ginController := api.NewAPIServiceGinController(*cfg, processOrm, logger.WithTags(tag.Service(ApiServiceName)))
			rawLog.Fatal(ginController.Run(cfg.ApiService.Address))
		}()
	}
	var processMQ persistence.ProcessMQ
	if services[AsyncServiceName] {
		// TODO
		mq := persistence.NewPulsarProcessMQ(*cfg, processOrm, logger)
		err := mq.Start()
		if err != nil {
			panic(err)
		}
	}
	return func() error {
		if processMQ != nil {
			err := processMQ.Stop()
			if err != nil {
				return err
			}
		}
		return processOrm.Close()
	}
}

func getServices(c *cli.Context) map[string]bool {
	val := strings.TrimSpace(c.String(FlagService))
	tokens := strings.Split(val, ",")

	if len(tokens) == 0 {
		rawLog.Fatal("No services specified for starting")
	}

	services := map[string]bool{}
	for _, token := range tokens {
		t := strings.TrimSpace(token)
		services[t] = true
	}

	return services
}
