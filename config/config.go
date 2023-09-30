package config

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
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
		MessageQueue MessageQueueConfig `yaml:"messageQueue"`
	}

	MessageQueueConfig struct {
		// currently only Pulsar+CDC is the only option
		Pulsar *PulsarMQConfig `yaml:"pulsar"`
	}

	PulsarMQConfig struct {
		// PulsarClientOptions is the config to connect to Pulsar service
		PulsarClientOptions pulsar.ClientOptions `yaml:"pulsarClientOptions"`
		// CDCTopicsPrefix is the prefix of topics that pulsar CDC connector sends messages to
		// The topics are per database table
		// XDB will consume messages from those topics for processing
		CDCTopicsPrefix string `yaml:"cdcTopicsPrefix"`
		// DefaultCDCTopicSubscription is the subscription that XDB will use to consuming the CDC topic
		// currently only one subscription is supported, which means all the worker/timer tasks from all the XDB Process Types
		// will share the same subscription with the consumer groups.
		// In the future, we will support subscription based on different process types for better isolation
		DefaultCDCTopicSubscription string `yaml:"defaultCDCTopicSubscription"`
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

func (c *Config) Validate() error {
	if c.Database.SQL == nil {
		return fmt.Errorf("sql config is required")
	}
	sql := c.Database.SQL
	if anyAbsent(sql.DatabaseName, sql.DBExtensionName, sql.ConnectAddr, sql.User) {
		return fmt.Errorf("some required configs are missing: sql.DatabaseName, sql.DBExtensionName, sql.ConnectAddr, sql.User")
	}
	if c.AsyncService.MessageQueue.Pulsar == nil {
		return fmt.Errorf("pulsar config is required")
	}
	pulsarCfg := c.AsyncService.MessageQueue.Pulsar
	if anyAbsent(pulsarCfg.CDCTopicsPrefix, pulsarCfg.DefaultCDCTopicSubscription) {
		return fmt.Errorf("some required configs are missing:pulsarCfg.CDCTopic, pulsarCfg.DefaultCDCTopicSubscription")
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
