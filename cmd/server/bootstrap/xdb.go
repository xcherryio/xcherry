package bootstrap

import (
	"fmt"
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

func StartXdbServer(cfg *config.Config, services []string) {
	if len(services) == 0 {
		services = []string{ApiServiceName, AsyncServiceName}
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

	for _, svc := range services {
		go launchService(svc, *cfg, processOrm, logger)
	}
}

func launchService(svcName string, cfg config.Config, processOrm persistence.ProcessORM, logger log2.Logger) {

	switch svcName {
	case ApiServiceName:
		ginController := api.NewAPIServiceGinController(cfg, processOrm, logger.WithTags(tag.Service(svcName)))
		rawLog.Fatal(ginController.Run(cfg.ApiService.Address))
	case AsyncServiceName:
		fmt.Println("TODO for starting async service")
	default:
		logger.Fatal("unsupported service", tag.Service(svcName))
	}
}

func getServices(c *cli.Context) []string {
	val := strings.TrimSpace(c.String(FlagService))
	tokens := strings.Split(val, ",")

	if len(tokens) == 0 {
		rawLog.Fatal("No services specified for starting")
	}

	var services []string
	for _, token := range tokens {
		t := strings.TrimSpace(token)
		services = append(services, t)
	}

	return services
}
