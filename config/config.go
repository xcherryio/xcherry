package config

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type (
	Config struct {
		// Log is the logging config
		Log Logger `yaml:"log"`

		// Database is the database that XDB will be extending on
		// either sql or nosql is needed
		Database DatabaseConfig `yaml:"database"`

		// ApiService is the API service config
		ApiService ApiServiceConfig `yaml:"apiService"`

		// AsyncService is config for async service
		AsyncService AsyncServiceConfig `yaml:"asyncService"`
	}

	DatabaseConfig struct {
		SQL *SQL `yaml:"sql"`
	}

	ApiServiceConfig struct {
		// HttpServer is the config for starting http.Server
		HttpServer HttpServerConfig `yaml:"httpServer"`
	}

	AsyncServiceConfig struct {
		Mode            AsyncServiceMode      `yaml:"mode"`
		WorkerTaskQueue WorkerTaskQueueConfig `yaml:"workerTaskQueue"`
		// InternalHttpServer is the config for starting a http.Server
		// to serve some internal APIs
		InternalHttpServer HttpServerConfig `yaml:"internalHttpServer"`
		// ClientAddress is the address for API service to call AsyncService's internal API
		ClientAddress string `yaml:"clientAddress"`
	}

	// HttpServerConfig is the config that will be mapped into http.Server
	HttpServerConfig struct {
		// Address optionally specifies the TCP address for the server to listen on,
		// in the form "host:port". If empty, ":http" (port 80) is used.
		// The service names are defined in RFC 6335 and assigned by IANA.
		// See net.Dial for details of the address format.
		// For more details, see https://blog.cloudflare.com/the-complete-guide-to-golang-net-http-timeouts/
		Address      string        `yaml:"address"`
		ReadTimeout  time.Duration `yaml:"readTimeout"`
		WriteTimeout time.Duration `yaml:"writeTimeout"`
		TLSConfig    *tls.Config   `yaml:"tlsConfig"`
		// the rest are less frequently used
		ReadHeaderTimeout time.Duration `yaml:"readHeaderTimeout"`
		IdleTimeout       time.Duration `yaml:"idleTimeout"`
		MaxHeaderBytes    int           `yaml:"maxHeaderBytes"`
	}

	WorkerTaskQueueConfig struct {
		MaxPollInterval      time.Duration `yaml:"maxPollInterval"`
		CommitInterval       time.Duration `yaml:"commitInterval"`
		IntervalJitter       time.Duration `yaml:"intervalJitter"`
		ProcessorConcurrency int           `yaml:"processorConcurrency"`
		ProcessorBufferSize  int           `yaml:"processorBufferSize"`
		PollPageSize         int32         `yaml:"pollPageSize"`
	}

	AsyncServiceMode string
)

const (
	// AsyncServiceModeStandalone means there is only one node for async service
	// This is the only supported mode now
	AsyncServiceModeStandalone = "standalone"
	// AsyncServiceModeConsistentHashingCluster means all the nodes of async service
	// will form a consistent hashing ring, which is used for shard ownership management
	// TODO
	//  1. add ringpop config
	//  2. add async client address config for APIService to call async service with LBS
	AsyncServiceModeConsistentHashingCluster = "consistent-hashing-cluster"
)

// NewConfig returns a new decoded Config struct
func NewConfig(configPath string) (*Config, error) {
	log.Printf("Loading configFile=%v\n", configPath)

	config := &Config{}

	file, err := os.Open(configPath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	d := yaml.NewDecoder(file)

	if err := d.Decode(&config); err != nil {
		return nil, err
	}

	return config, nil
}

func (c *Config) ValidateAndSetDefaults() error {
	if c.Database.SQL == nil {
		return fmt.Errorf("sql config is required")
	}
	sql := c.Database.SQL
	if anyAbsent(sql.DatabaseName, sql.DBExtensionName, sql.ConnectAddr, sql.User) {
		return fmt.Errorf("some required configs are missing: sql.DatabaseName, sql.DBExtensionName, sql.ConnectAddr, sql.User")
	}
	if c.AsyncService.Mode == "" {
		return fmt.Errorf("must set async service mode")
	}
	if c.AsyncService.Mode != AsyncServiceModeStandalone {
		return fmt.Errorf("currently only standalone mode is supported")
	}
	workerTaskQConfig := &c.AsyncService.WorkerTaskQueue
	if workerTaskQConfig.MaxPollInterval == 0 {
		workerTaskQConfig.MaxPollInterval = time.Minute
	}
	if workerTaskQConfig.CommitInterval == 0 {
		workerTaskQConfig.CommitInterval = time.Minute
	}
	if workerTaskQConfig.IntervalJitter == 0 {
		workerTaskQConfig.IntervalJitter = time.Second * 5
	}
	if workerTaskQConfig.ProcessorConcurrency == 0 {
		workerTaskQConfig.ProcessorConcurrency = 10
	}
	if workerTaskQConfig.ProcessorBufferSize == 0 {
		workerTaskQConfig.ProcessorBufferSize = 1000
	}
	if workerTaskQConfig.PollPageSize == 0 {
		workerTaskQConfig.PollPageSize = 1000
	}
	if c.AsyncService.ClientAddress == "" {
		c.AsyncService.ClientAddress = c.ApiService.HttpServer.Address
	}
	return nil
}

func anyAbsent(strs ...string) bool {
	for _, s := range strs {
		if s == "" {
			return true
		}
	}
	return false
}

// String converts the config object into a string
func (c *Config) String() string {
	out, err := json.MarshalIndent(c, "", "    ")
	if err != nil {
		panic(err)
	}
	return string(out)
}
