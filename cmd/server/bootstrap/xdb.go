package bootstrap

import (
	"fmt"
	log2 "github.com/xdblab/xdb/common/log"
	"github.com/xdblab/xdb/common/log/tag"
	"github.com/xdblab/xdb/config"
	rawLog "log"
	"strings"
	"sync"

	"github.com/urfave/cli/v2"
	"github.com/xdblab/xdb/service/api"
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
	logger := log2.NewLogger(zapLogger)
	logger.Info("config is loaded", tag.Value(loadedConfig.String()))
	err = loadedConfig.Validate()
	if err != nil {
		logger.Fatal("config is invalid", tag.Error(err))
	}

	services := getServices(c)

	for _, svc := range services {
		go launchService(svc, *loadedConfig, logger)
	}

	// TODO improve by waiting for the started services to stop
	wg := sync.WaitGroup{}
	wg.Add(1)
	wg.Wait()
}

func launchService(svcName string, config config.Config, logger log2.Logger) {

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
