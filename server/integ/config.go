// Legacy Materials in this file remain under their original licenses.
// See LEGACY_NOTICES.md.

// Modifications Copyright (c) 2026 Super Durable, Inc.
//
// Modifications after the Legacy Cutoff are licensed under the
// Sustainable Use License 1.0.
// Legacy Materials remain under their original licenses.
// See LICENSE and LEGACY_NOTICES.md.

package integ

import (
	"testing"
	"time"

	"github.com/superdurable/dex/config"
	"github.com/superdurable/dex/service"
	"github.com/superdurable/dex/service/common/ptr"
	"go.temporal.io/sdk/client"
)

var testNamespace = "default"

// Api.Port / fixed worker ports are unused: startWorker and startDexService bind 127.0.0.1:0.

type DexServiceTestConfig struct {
	BackendType                            service.BackendType
	CadenceDomain                          string
	MemoEncryption                         bool
	DefaultHeaders                         map[string]string
	S3TestThreshold                        int
	LocalBlobDirectory                     string
	LocalBlobThreshold                     int
	AttributeStore                         config.AttributeStoreConfig
	StreamStore                            config.StreamStoreConfig
	BlobCacheDirectory                     string
	BlobStoreEnabled                       *bool
	AsyncStepInputSnapshotsEnabled         *bool
	UseTemporalSynchronousUpdateForAllRPCs bool
	TemporalMetricsHandler                 client.MetricsHandler
	MaxWaitSeconds                         int64
	GrpcMaxMessageBytes                    int
	// UseBuiltInDefaultWorkflowConfig lets a test exercise package defaults.
	UseBuiltInDefaultWorkflowConfig bool
	// LazyLoading overrides BlobStore.LazyLoading.
	// Nil uses EffectiveLazyLoading default (true).
	LazyLoading *bool
	// SubFlowStartActivityConfig bounds SubFlow start retries; nil uses the server default.
	SubFlowStartActivityConfig *config.SubFlowStartActivityConfig
}

func createTestConfig(t *testing.T, testCfg DexServiceTestConfig) config.Config {
	t.Helper()
	blobStoreEnabled := testCfg.BlobStoreEnabled == nil || *testCfg.BlobStoreEnabled
	if blobStoreEnabled && testCfg.S3TestThreshold == 0 && testCfg.LocalBlobDirectory == "" {
		testCfg.LocalBlobDirectory = t.TempDir()
	}
	maxWaitSeconds := testCfg.MaxWaitSeconds
	if maxWaitSeconds == 0 {
		maxWaitSeconds = 12
	}
	defaultWorkflowConfig := syncDurabilityConfig()
	if testCfg.UseBuiltInDefaultWorkflowConfig {
		defaultWorkflowConfig = nil
	}
	cfg := config.Config{
		Api: config.ApiConfig{
			MaxWaitSeconds:                         maxWaitSeconds,
			GrpcMaxMessageBytes:                    testCfg.GrpcMaxMessageBytes,
			UseTemporalSynchronousUpdateForAllRPCs: testCfg.UseTemporalSynchronousUpdateForAllRPCs,
			QueryWorkflowFailedRetryPolicy: &config.RetryPolicy{
				InitialInterval: 100 * time.Millisecond,
				MaximumAttempts: 10,
			},
		},
		Worker: config.WorkerConfig{
			DefaultHeaders: testCfg.DefaultHeaders,
		},
		BlobStore: config.BlobStoreConfig{
			Enabled:                        testCfg.BlobStoreEnabled,
			AsyncStepInputSnapshotsEnabled: testCfg.AsyncStepInputSnapshotsEnabled,
		},
		Interpreter: config.Interpreter{
			DefaultWorkflowConfig: defaultWorkflowConfig,
			VerboseDebug:          false,
			InterpreterActivityConfig: config.InterpreterActivityConfig{
				MinimumStepHeartbeatTimeout: 2 * time.Second,
				SubFlowStartActivityConfig:  testCfg.SubFlowStartActivityConfig,
			},
		},
	}
	cfg.AttributeStore = testCfg.AttributeStore
	cfg.StreamStore = testCfg.StreamStore
	switch testCfg.BackendType {
	case service.BackendTypeTemporal:
		cfg.Interpreter.Temporal = &config.TemporalConfig{}
	case service.BackendTypeCadence:
		cfg.Interpreter.Cadence = &config.CadenceConfig{}
	}
	if testCfg.S3TestThreshold > 0 {
		blobStoreCfg := config.BlobStoreConfig{
			Enabled:                        ptr.Any(blobStoreEnabled),
			AsyncStepInputSnapshotsEnabled: testCfg.AsyncStepInputSnapshotsEnabled,
			LazyLoading:                    testCfg.LazyLoading,
			ThresholdInBytes:               testCfg.S3TestThreshold,
			HistoryRetentionInDays:         3,
			BlobCache: config.BlobCacheConfig{
				Directory: testCfg.BlobCacheDirectory,
			},
			SupportedStorages: []config.BlobStoreConfigEntry{
				{
					Status:      config.StorageStatusActive,
					StorageId:   "s3-store-id",
					StorageType: config.StorageTypeS3,
					S3Endpoint:  "http://localhost:9000",
					S3Bucket:    "dex-test-bucket",
					S3Region:    "us-east-1",
					S3AccessKey: "minioadmin",
					S3SecretKey: "minioadmin",
				},
			},
		}
		cfg.BlobStore = blobStoreCfg
	}
	if testCfg.LocalBlobDirectory != "" {
		threshold := testCfg.LocalBlobThreshold
		if threshold == 0 {
			threshold = config.DefaultBlobStoreThresholdInBytes
		}
		cfg.BlobStore = config.BlobStoreConfig{
			Enabled:                        ptr.Any(blobStoreEnabled),
			AsyncStepInputSnapshotsEnabled: testCfg.AsyncStepInputSnapshotsEnabled,
			LazyLoading:                    testCfg.LazyLoading,
			ThresholdInBytes:               threshold,
			HistoryRetentionInDays:         3,
			SupportedStorages: []config.BlobStoreConfigEntry{{
				Status:         config.StorageStatusActive,
				StorageId:      "local-store-id",
				StorageType:    config.StorageTypeLocal,
				LocalDirectory: testCfg.LocalBlobDirectory,
			}},
		}
	}
	return cfg
}
