// Copyright (c) XDBLab
// SPDX-License-Identifier: BUSL-1.1

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
		Mode AsyncServiceMode `yaml:"mode"`
		// ImmediateTaskQueue is the config for immediate task queue
		ImmediateTaskQueue ImmediateTaskQueueConfig `yaml:"immediateTaskQueue"`
		// TimerTaskQueue is the config for timer task queue
		TimerTaskQueue TimerTaskQueueConfig `yaml:"timerTaskQueue"`
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

	ImmediateTaskQueueConfig struct {
		// MaxPollInterval is the maximum interval that the poller will wait between
		// polls. The poller will always poll immediately when receives a notification that there are new tasks.
		// But there is no atomicity/transaction guarantee for the notification.
		// Therefore, polling with this interval is to ensure not missing any tasks. This also
		// means that at worst case, the task could be delayed up to MaxPollInterval.
		// If not specified then the default value of 1 minute is used.
		MaxPollInterval time.Duration `yaml:"maxPollInterval"`
		// CommitInterval is the interval that the poller will use to commit the progress of
		// the queue processing.
		// If not specified then the default value of 1 minute is used.
		CommitInterval time.Duration `yaml:"commitInterval"`
		// IntervalJitterCoefficient is the jitter factor for the poll and commit interval.
		// Default value is 10 seconds.
		IntervalJitter time.Duration `yaml:"intervalJitter"`
		// ProcessorConcurrency is the number of goroutines that will be created to process
		// tasks per async service instance.
		// Note that a processor is shared by all task queues in the async service instance.
		// If not specified then the default value of 10.
		ProcessorConcurrency int `yaml:"processorConcurrency"`
		// ProcessorBufferSize is the size of the buffer for each processor. The processor
		// will stop polling for tasks if the buffer is full and resume polling when it is
		// no longer full.
		// It's also being used as size of the buffer for receiving completed tasks from processor for each queue.
		// Note that a processor is shared by all task queues in the async service instance.
		// If not specified then the default value of 1000 is used.
		ProcessorBufferSize int `yaml:"processorBufferSize"`
		// PollPageSize is the page size used by the poller to fetch tasks from the database.
		// If not specified then the default value of 1000 is used.
		PollPageSize int32 `yaml:"pollPageSize"`
		// MaxAsyncStateAPITimeout is the maximum timeout for async state APIs(waitUntil/execute)
		// Exceeding the timeout will cause the timeout to be capped at this value.
		// If not specified then the default value of 60 seconds is used.
		MaxAsyncStateAPITimeout time.Duration `yaml:"maxAsyncStateAPITimeout"`
		// DefaultAsyncStateAPITimeout is the default timeout for async state APIs(waitUntil/execute)
		// If not specified then the default value of 10 seconds is used.
		DefaultAsyncStateAPITimeout time.Duration `yaml:"defaultAsyncStateAPITimeout"`
		// MaxStateAPIFailureDetailSize is the maximum size of the failure detail that will be stored into
		// database for async state APIs(waitUntil/execute)
		// Default value is 1000 bytes
		MaxStateAPIFailureDetailSize int `yaml:"maxStateAPIFailureDetailSize"`
	}

	TimerTaskQueueConfig struct {
		// MaxTimerPreloadLookAhead defines how far in the future the timer queue will preload timers.
		// Together with MaxPreloadPageSize, the preload will load up to MaxPreloadPageSize timers,
		// or up to timers' fireTime <= now() + MaxTimerPreloadLookAhead, whichever comes first.
		// After preloading timers, the timer queue will wait for all the loaded timers
		// to complete, AND this lookahead duration passed, before making next preload.
		// During the duration, any new timers created that need to fire within the duration, will rely
		// on the "notifier" to trigger loading the new timers.
		// However, similar to ImmediateTaskQueue, there is no atomicity/transaction guarantee for the notification.
		// If missing the notification, the new timers will be loaded in the next preload( meaning that the timer
		// firing could be delayed up to MaxTimerPreloadLookAhead in worst case).
		// Also note that this duration being too large could lead to too many triggers of polling from notifications,
		// if there are many new timers created to fire within the duration.
		// which could not be as efficient as the preload(batch loading).
		// Default value is 1 minute.
		MaxTimerPreloadLookAhead time.Duration `yaml:"maxTimerLoadingWindowInterval"`
		// MaxPreloadPageSize is the maximum number of timers that a preload will load from database.
		// Together with MaxTimerPreloadLookAhead, the preload will load up to MaxPreloadPageSize timers,
		// or up to timers' fireTime <= now() + MaxTimerPreloadLookAhead, whichever comes first.
		// If not specified then the default value of 1000 is used.
		MaxPreloadPageSize int32 `yaml:"maxPreloadPageSize"`
		// IntervalJitter is the jitter factor for the MaxTimerPreloadLookAhead
		// Default value is 10 seconds.
		IntervalJitter time.Duration `yaml:"intervalJitter"`
		// ProcessorConcurrency is the number of goroutines that will be created to process
		// tasks per async service instance. If not specified then the default value of 3.
		// Note that a processor is shared by all task queues in the async service instance.
		ProcessorConcurrency int `yaml:"processorConcurrency"`
		// ProcessorBufferSize is the size of the buffer for each processor. The processor
		// will stop polling for tasks if the buffer is full and resume polling when it is
		// no longer full. If not specified then the default value of 1000 is used.
		// Note that a processor is shared by all task queues in the async service instance.
		// It's also being used as size of the buffer for receiving completed tasks from processor for each queue.
		ProcessorBufferSize int `yaml:"processorBufferSize"`
		// TriggerNotificationBufferSize is the size of the buffer for the channel that receives
		// trigger notification.
		// If not specified then the default value of 1000 is used.
		TriggerNotificationBufferSize int `yaml:"triggerNotificationBufferSize"`
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
	immediateTaskQConfig := &c.AsyncService.ImmediateTaskQueue
	if immediateTaskQConfig.MaxPollInterval == 0 {
		immediateTaskQConfig.MaxPollInterval = time.Minute
	}
	if immediateTaskQConfig.CommitInterval == 0 {
		immediateTaskQConfig.CommitInterval = time.Minute
	}
	if immediateTaskQConfig.IntervalJitter == 0 {
		immediateTaskQConfig.IntervalJitter = time.Second * 5
	}
	if immediateTaskQConfig.ProcessorConcurrency == 0 {
		immediateTaskQConfig.ProcessorConcurrency = 10
	}
	if immediateTaskQConfig.ProcessorBufferSize == 0 {
		immediateTaskQConfig.ProcessorBufferSize = 1000
	}
	if immediateTaskQConfig.PollPageSize == 0 {
		immediateTaskQConfig.PollPageSize = 1000
	}
	if immediateTaskQConfig.MaxAsyncStateAPITimeout == 0 {
		immediateTaskQConfig.MaxAsyncStateAPITimeout = 60 * time.Second
	}
	if immediateTaskQConfig.DefaultAsyncStateAPITimeout == 0 {
		immediateTaskQConfig.DefaultAsyncStateAPITimeout = 10 * time.Second
	}
	if immediateTaskQConfig.MaxStateAPIFailureDetailSize == 0 {
		immediateTaskQConfig.MaxStateAPIFailureDetailSize = 1000
	}
	timerTaskQConfig := &c.AsyncService.TimerTaskQueue
	if timerTaskQConfig.MaxTimerPreloadLookAhead == 0 {
		timerTaskQConfig.MaxTimerPreloadLookAhead = time.Minute
	}
	if timerTaskQConfig.MaxPreloadPageSize == 0 {
		timerTaskQConfig.MaxPreloadPageSize = 1000
	}
	if timerTaskQConfig.IntervalJitter == 0 {
		timerTaskQConfig.IntervalJitter = time.Second * 10
	}
	if timerTaskQConfig.ProcessorConcurrency == 0 {
		timerTaskQConfig.ProcessorConcurrency = 3
	}
	if timerTaskQConfig.ProcessorBufferSize == 0 {
		timerTaskQConfig.ProcessorBufferSize = 1000
	}
	if timerTaskQConfig.TriggerNotificationBufferSize == 0 {
		timerTaskQConfig.TriggerNotificationBufferSize = 1000
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
