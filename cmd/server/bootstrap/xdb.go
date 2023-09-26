package bootstrap

import (
	"fmt"
	rawLog "log"
	"strings"
	"sync"

	"github.com/urfave/cli/v2"
	"github.com/xdblab/xdb/service/api"
	"github.com/xdblab/xdb/service/common/config"
	"github.com/xdblab/xdb/service/common/log"
	"github.com/xdblab/xdb/service/common/log/tag"
)

const ApiServiceName = "api"
const AsyncServiceName = "async"

const FlagConfig = "config"
const FlagService = "service"

func StartXdbServer(c *cli.Context) {
	configPath := c.String("config")
	loadedConfig, err := config.NewConfig(configPath)
	if err != nil {
		rawLog.Fatalf("Unable to load config for path %v because of error %v", configPath, err)
	}
	zapLogger, err := loadedConfig.Log.NewZapLogger()
	if err != nil {
		rawLog.Fatalf("Unable to create a new zap logger %v", err)
	}
	logger := log.NewLogger(zapLogger)
	logger.Info("config is loaded", tag.Value(loadedConfig.String()))

	services := getServices(c)

	for _, svc := range services {
		go launchService(svc, *loadedConfig, logger)
	}

	// TODO improve by waiting for the started services to stop
	wg := sync.WaitGroup{}
	wg.Add(1)
	wg.Wait()
}

func launchService(svcName string, config config.Config, logger log.Logger) {

	switch svcName {
	case ApiServiceName:
		apiService := api.NewService(config, logger.WithTags(tag.Service(svcName)))
		rawLog.Fatal(apiService.Run(fmt.Sprintf(":%v", config.ApiService.Port)))
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
