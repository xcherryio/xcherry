package config

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

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
		// Port is the port on which the API service will bind to
		Port int `yaml:"port"`
		// DefaultPollingMaxWaitSeconds is the default timeout for polling APIs
		DefaultPollingMaxWaitSeconds int64 `yaml:"defaultPollingMaxWaitSeconds"`
	}

	DatabaseConfig struct {
		SQL *SQL `yaml:"sql"`
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
		return fmt.Errorf("some required configs are missing")
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
