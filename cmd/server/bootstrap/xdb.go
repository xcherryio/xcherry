package bootstrap

import (
	"context"
	"fmt"
	"github.com/urfave/cli/v2"
	log2 "github.com/xdblab/xdb/common/log"
	"github.com/xdblab/xdb/common/log/tag"
	"github.com/xdblab/xdb/config"
	"github.com/xdblab/xdb/persistence"
	"github.com/xdblab/xdb/service/api"
	"go.uber.org/multierr"
	rawLog "log"
	"os"
	"os/signal"
	"strings"
	"time"
)

const ApiServiceName = "api"
const AsyncServiceName = "async"

const FlagConfig = "config"
const FlagService = "service"

func StartXdbServerCli(c *cli.Context) {
	// register interrupt signal for graceful shutdown
	rootCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	configPath := c.String("config")
	services := getServices(c)

	cfg, err := config.NewConfig(configPath)
	if err != nil {
		rawLog.Fatalf("Unable to load config for path %v because of error %v", configPath, err)
	}
	shutdownFunc := StartXdbServer(rootCtx, cfg, services)
	// wait for os signals
	<-rootCtx.Done()

	ctx, cancF := context.WithTimeout(context.Background(), time.Second*10)
	defer cancF()
	err = shutdownFunc(ctx)
	if err != nil {
		fmt.Println("shutdown error:", err)
	}
}

type GracefulShutdown func(ctx context.Context) error

func StartXdbServer(rootCtx context.Context, cfg *config.Config, services map[string]bool) GracefulShutdown {
	if len(services) == 0 {
		services = map[string]bool{ApiServiceName: true, AsyncServiceName: true}
	}

	zapLogger, err := cfg.Log.NewZapLogger()
	if err != nil {
		rawLog.Fatalf("Unable to create a new zap logger %v", err)
	}
	logger := log2.NewLogger(zapLogger)
	logger.Info("config is loaded", tag.Value(cfg.String()))
	err = cfg.ValidateAndSetDefaults()
	if err != nil {
		logger.Fatal("config is invalid", tag.Error(err))
	}

	sqlStore, err := persistence.NewSQLPersistence(*cfg.Database.SQL, logger)
	if err != nil {
		logger.Fatal("error on persistence setup", tag.Error(err))
	}

	var apiServer api.Server
	if services[ApiServiceName] {
		apiServer = api.NewDefaultAPIServerWithGin(rootCtx, *cfg, sqlStore, logger.WithTags(tag.Service(ApiServiceName)))
		err = apiServer.Start()
		if err != nil {
			logger.Fatal("Failed to start api server", tag.Error(err))
		}
	}
	if services[AsyncServiceName] {
		// TODO implement a service
	}

	return func(ctx context.Context) error {
		// graceful shutdown
		var errs error
		// first stop api server
		if apiServer != nil {
			err := apiServer.Stop(ctx)
			if err != nil {
				errs = multierr.Append(errs, err)
			}
		}
		// stop sqlStore
		err := sqlStore.Close()
		if err != nil {
			errs = multierr.Append(errs, err)
		}
		return errs
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
