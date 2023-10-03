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
		// ApiService is the API service config
		ApiService ApiServiceConfig `yaml:"apiService"`
		// Database is the database that XDB will be extending on
		// either sql or nosql is needed
		Database DatabaseConfig `yaml:"database"`
		// AsyncService is config for async service
		AsyncService AsyncServiceConfig `yaml:"asyncService"`
	}

	// Logger contains the config items for logger
	Logger struct {
		// Stdout is true then the output needs to goto standard out
		// By default this is false and output will go to standard error
		Stdout bool `yaml:"stdout"`
		// Level is the desired log level
		Level string `yaml:"level"`
		// OutputFile is the path to the log output file
		// Stdout must be false, otherwise Stdout will take precedence
		OutputFile string `yaml:"outputFile"`
		// LevelKey is the desired log level, defaults to "level"
		LevelKey string `yaml:"levelKey"`
		// Encoding decides the format, supports "console" and "json".
		// "json" will print the log in JSON format(better for machine), while "console" will print in plain-text format(more human friendly)
		// Default is "json"
		Encoding string `yaml:"encoding"`
	}

	ApiServiceConfig struct {
		// HttpServer is the config for starting http.Server
		HttpServer HttpServerConfig `yaml:"httpServer"`
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

	DatabaseConfig struct {
		SQL *SQL `yaml:"sql"`
	}

	AsyncServiceConfig struct {
		WorkerTaskQueue WorkerTaskQueueConfig `yaml:"workerTaskQueue"`
	}

	WorkerTaskQueueConfig struct {
		MaxPollInterval      time.Duration `yaml:"maxPollInterval"`
		CommitInterval       time.Duration `yaml:"commitInterval"`
		IntervalJitter       time.Duration `yaml:"intervalJitter"`
		ProcessorConcurrency int           `yaml:"processorConcurrency"`
		ProcessorBufferSize  int           `yaml:"processorBufferSize"`
		PollPageSize         int32         `yaml:"pollPageSize"`
	}
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
	workerTaskQConfig := c.AsyncService.WorkerTaskQueue
	if workerTaskQConfig.MaxPollInterval == 0 {
		workerTaskQConfig.MaxPollInterval = time.Minute
	}
	if workerTaskQConfig.IntervalJitter == 0 {
		workerTaskQConfig.MaxPollInterval = time.Second * 5
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
