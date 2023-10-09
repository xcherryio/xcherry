// Apache License 2.0

// Copyright (c) XDBLab organization

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

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
		// SQL is the SQL database config
		// either sql or nosql is needed to run XDB
		// Only SQL is supported for now.
		SQL *SQL `yaml:"sql"`
	}

	ApiServiceConfig struct {
		// HttpServer is the config for starting http.Server
		HttpServer HttpServerConfig `yaml:"httpServer"`
	}

	AsyncServiceConfig struct {
		// Mode is the mode of async service. Currently only standalone mode is supported
		Mode            AsyncServiceMode      `yaml:"mode"`
		// WorkerTaskQueue is the config for worker task queue
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
		Address string `yaml:"address"`
		// ReadTimeout is the maximum duration for reading the entire
		// request, including the body. Because ReadTimeout does not
		// let Handlers make per-request decisions on each request body's acceptable
		// deadline or upload rate, most users will prefer to use
		// ReadHeaderTimeout. It is valid to use them both.
		ReadTimeout time.Duration `yaml:"readTimeout"`
		/// WriteTimeout is the maximum duration before timing out
		// writes of the response. It is valid to use them both ReadTimeout and WriteTimeout.
		// For more details, see https://blog.cloudflare.com/the-complete-guide-to-golang-net-http-timeouts/
		WriteTimeout time.Duration `yaml:"writeTimeout"`
		// TLSConfig optionally provides a TLS configuration for use
		// by ServeTLS and ListenAndServeTLS
		TLSConfig *tls.Config `yaml:"tlsConfig"`
		// the rest are less frequently used
		ReadHeaderTimeout time.Duration `yaml:"readHeaderTimeout"`
		IdleTimeout       time.Duration `yaml:"idleTimeout"`
		MaxHeaderBytes    int           `yaml:"maxHeaderBytes"`
	}

	WorkerTaskQueueConfig struct {
		// MaxPollInterval is the maximum interval that the poller will wait between
		// polls. If not specified then the default value of 1 minute is used.
		MaxPollInterval time.Duration `yaml:"maxPollInterval"`
		// CommitInterval is the interval that the poller will use to commit the progress of
		// the queue processing. If not specified then the default value of 1 minute is used.
		CommitInterval time.Duration `yaml:"commitInterval"`
		// IntervalJitterCoefficient is the jitter factor for the poll and commit interval.
		IntervalJitter time.Duration `yaml:"intervalJitter"`
		// ProcessorConcurrency is the number of goroutines that will be created to process
		// tasks per async service instance. If not specified then the default value of 10.
		ProcessorConcurrency int `yaml:"processorConcurrency"`
		// ProcessorBufferSize is the size of the buffer for each processor. The processor
		// will stop polling for tasks if the buffer is full and resume polling when it is
		// no longer full. If not specified then the default value of 1000 is used.
		ProcessorBufferSize int `yaml:"processorBufferSize"`
		// PollPageSize is the page size used by the poller to fetch tasks from the database.
		// If not specified then the default value of 1000 is used.
		PollPageSize int32 `yaml:"pollPageSize"`
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
		if c.AsyncService.InternalHttpServer.Address == "" {
			return fmt.Errorf("AsyncService.InternalHttpServer.Address cannot be empty")
		}
		c.AsyncService.ClientAddress = "http://" + c.AsyncService.InternalHttpServer.Address
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
