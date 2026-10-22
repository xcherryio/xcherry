// Legacy Materials in this file remain under their original licenses.
// See LEGACY_NOTICES.md.

// Modifications Copyright (c) 2026 Super Durable, Inc.
//
// Modifications after the Legacy Cutoff are licensed under the
// Sustainable Use License 1.0.
// Legacy Materials remain under their original licenses.
// See LICENSE and LEGACY_NOTICES.md.

package config

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/superdurable/dex/gen/dexpb"
	"github.com/superdurable/dex/service/common/ptr"
	"github.com/uber-go/tally/v4/prometheus"
	temporalWorker "go.temporal.io/sdk/worker"
	cadenceWorker "go.uber.org/cadence/worker"
	"google.golang.org/grpc/codes"
	"gopkg.in/yaml.v3"
)

const (
	StorageStatusActive   = "active"
	StorageStatusInactive = "inactive"
)

const (
	StorageTypeS3    = "s3"
	StorageTypeLocal = "local"
)

const (
	StreamStoreBackendDisabled StreamStoreBackend = "disabled"
	StreamStoreBackendMemory   StreamStoreBackend = "memory"
	StreamStoreBackendRedis    StreamStoreBackend = "redis"
)

const (
	CleanupStrategyTypeAfterAllRunsDeleted CleanupStrategyType = "afterAllRunsDeleted"
)

const (
	// DefaultApiPort is the default FlowService/InternalService gRPC bind port.
	DefaultApiPort = 8801
	// DefaultWebBindAddress exposes Dex Web on every interface.
	DefaultWebBindAddress = "0.0.0.0"
	// DefaultWebPort is the default Dex Web HTTP bind port.
	DefaultWebPort = 8802
	// DefaultMembershipListen binds ringpop on every interface, outside the all-in-one backend port ranges.
	DefaultMembershipListen = "0.0.0.0:6833"
	// DefaultMembershipRefreshInterval re-resolves DNS seeds and refreshes role rings every ten seconds.
	DefaultMembershipRefreshInterval = 10 * time.Second
	// DefaultMatchingDispatchWait bounds a regular attempt's Worker search.
	DefaultMatchingDispatchWait = 10 * time.Second
	// DefaultMatchingStickyWait bounds a regular attempt's sticky queue search.
	DefaultMatchingStickyWait = 5 * time.Second
	// DefaultMatchingLocalDispatchWait bounds a local attempt's Worker search.
	DefaultMatchingLocalDispatchWait = 3 * time.Second
	// DefaultMatchingLocalStickyWait bounds a local attempt's sticky queue search.
	DefaultMatchingLocalStickyWait = 2 * time.Second
	// DefaultMatchingLocalExecutionReserve preserves time for local task execution.
	DefaultMatchingLocalExecutionReserve = 4 * time.Second
	// DefaultMatchingMaxConcurrentTasksCap bounds streams registered by one Worker.
	DefaultMatchingMaxConcurrentTasksCap = 256
	// DefaultMatchingMaxStreams bounds all registered Worker streams on one server.
	DefaultMatchingMaxStreams = 100000
	// DefaultMatchingMaxQueueLength bounds queue names in matching requests.
	DefaultMatchingMaxQueueLength = 256
	// DefaultMatchingMaxFlowTypes bounds Flow types in one Worker registration.
	DefaultMatchingMaxFlowTypes = 256
	// DefaultMatchingMaxFlowTypeLength bounds each declared Flow type.
	DefaultMatchingMaxFlowTypeLength = 256
	// DefaultMatchingPingInterval keeps idle Worker streams active through load balancers.
	DefaultMatchingPingInterval = 20 * time.Second
	// DefaultMaxWaitSeconds caps transport long polls when MaxWaitSeconds is 0.
	DefaultMaxWaitSeconds int64 = 60
	// DefaultGrpcMaxMessageBytes is 32 MiB so that application values up to 16 MiB keep 16 MiB of headroom
	// for the request or response envelope, such as loaded state, batch entries, and metadata.
	DefaultGrpcMaxMessageBytes = 32 * 1024 * 1024
	// DefaultWorkerConnectionIdleTimeout is how long an idle WorkerService conn may sit unused before eviction.
	DefaultWorkerConnectionIdleTimeout = 10 * time.Minute
	// DefaultMaxWorkerConnections caps the WorkerService dial pool; positive required at runtime.
	DefaultMaxWorkerConnections = 1000
	// DefaultHeadlessAddressRefreshInterval controls headless WorkerService DNS refresh.
	DefaultHeadlessAddressRefreshInterval = time.Minute
	// DefaultMaxStickyRoutingEntries caps remembered flow-to-worker routes.
	DefaultMaxStickyRoutingEntries = 100000
	// DefaultWorkerServiceRequestMaxAttempts includes the first headless WorkerService request.
	DefaultWorkerServiceRequestMaxAttempts = 3
	// DefaultBlobCacheMaxBytes caps the Server Attribute blob cache at one GiB.
	DefaultBlobCacheMaxBytes int64 = 1 << 30
	// DefaultBlobStoreThresholdInBytes offloads payloads larger than 100 bytes.
	DefaultBlobStoreThresholdInBytes = 100
	// DefaultBlobStoreObjectIDLength is the default Base36 object identifier length.
	DefaultBlobStoreObjectIDLength = 10
	// DefaultAttributeStoreSchemaSyncInterval refreshes table schemas every minute before jitter.
	DefaultAttributeStoreSchemaSyncInterval = time.Minute
	// DefaultAttributeStoreSyncBatchSize caps items in one Attribute Store upsert.
	DefaultAttributeStoreSyncBatchSize = 100
	// DefaultAttributeStoreSyncAttemptTimeout caps each regular Activity attempt at thirty seconds.
	DefaultAttributeStoreSyncAttemptTimeout = 30 * time.Second
	// DefaultAttributeStoreSyncTotalDuration caps regular Activity retries at one hour.
	DefaultAttributeStoreSyncTotalDuration = time.Hour
	// DefaultAttributeIndexSyncTimeout bounds backend index registration and propagation checks.
	DefaultAttributeIndexSyncTimeout = 2 * time.Minute
	// DefaultMinimumStepHeartbeatTimeout is the minimum explicit Step heartbeat timeout.
	DefaultMinimumStepHeartbeatTimeout = 10 * time.Second
	// DefaultSubFlowStartActivityScheduleToCloseTimeout bounds all local SubFlow start attempts.
	DefaultSubFlowStartActivityScheduleToCloseTimeout = 2 * time.Minute
	// DefaultStreamMaxMessageBytes limits each serialized Stream Value to 100 KiB.
	DefaultStreamMaxMessageBytes int64 = 100 * 1024
	// DefaultStreamMaxListMessagesPageSize caps one reverse Stream page at 1000 messages.
	DefaultStreamMaxListMessagesPageSize int32 = 1000
	// DefaultStreamEstimatedMessageOverheadBytes approximates backend bookkeeping per message.
	DefaultStreamEstimatedMessageOverheadBytes int64 = 512
	// DefaultStreamTrimTriggerPercent starts asynchronous trimming at ninety percent of capacity.
	DefaultStreamTrimTriggerPercent int32 = 90
	// DefaultStreamTrimTargetPercent stops asynchronous trimming at eighty percent of capacity.
	DefaultStreamTrimTargetPercent int32 = 80
	// DefaultStreamBackgroundTrimBatchSize caps messages removed by one trim batch.
	DefaultStreamBackgroundTrimBatchSize = 256
	// DefaultStreamTrimLeaseTTL keeps a distributed trim lease alive for five seconds.
	DefaultStreamTrimLeaseTTL = 5 * time.Second
	// DefaultStreamTrimLeaseRetry retries a held distributed trim lease every 100 milliseconds.
	DefaultStreamTrimLeaseRetry = 100 * time.Millisecond
	// DefaultStreamTrimBatchYieldTime pauses one millisecond between atomic trim batches.
	DefaultStreamTrimBatchYieldTime = time.Millisecond
	// DefaultStreamTrimWorkers bounds process-wide asynchronous trim concurrency.
	DefaultStreamTrimWorkers = 4
)

const (
	AttributeStoreTypeMySQL      AttributeStoreType = "mysql"
	AttributeStoreTypePostgres   AttributeStoreType = "postgres"
	AttributeStoreTypeDatabricks AttributeStoreType = "databricks"
	AttributeStoreTypeSnowflake  AttributeStoreType = "snowflake"
	AttributeStoreTypeMongoDB    AttributeStoreType = "mongodb"
)

const (
	// AttributeStoreColumnNamingExact writes each Attribute to the column with exactly its name.
	AttributeStoreColumnNamingExact AttributeStoreColumnNaming = ""
	// AttributeStoreColumnNamingSnakeCase writes each Attribute to the snake_case form of its name, such as deployment_url for DeploymentURL.
	AttributeStoreColumnNamingSnakeCase AttributeStoreColumnNaming = "snake_case"
)

var defaultHeadlessFailoverStatusCodes = [...]codes.Code{
	codes.Unavailable,
	codes.DeadlineExceeded,
	codes.Unknown,
}

var (
	DefaultQueryWorkflowFailedRetryPolicy = RetryPolicy{
		InitialInterval:    100 * time.Millisecond,
		BackoffCoefficient: 1,
		MaximumInterval:    2 * time.Second,
		MaximumAttempts:    5,
	}
	DefaultInvokeRPCContinuedAsNewErrorRetryPolicy = RetryPolicy{
		InitialInterval:    100 * time.Millisecond,
		BackoffCoefficient: 2,
		MaximumInterval:    500 * time.Millisecond,
		TotalDuration:      5 * time.Second,
	}
	DefaultAttributeStoreSyncRetryPolicy = RetryPolicy{
		InitialInterval:    100 * time.Millisecond,
		BackoffCoefficient: 2,
		MaximumInterval:    30 * time.Second,
		TotalDuration:      DefaultAttributeStoreSyncTotalDuration,
	}
	DefaultInternalActivityRetryPolicy = RetryPolicy{
		InitialInterval:    100 * time.Millisecond,
		BackoffCoefficient: 2,
		MaximumInterval:    60 * time.Second,
	}
)

type (
	RetryPolicy struct {
		// InitialInterval is the first delay between attempts. Zero uses the owning config field's default.
		InitialInterval time.Duration `yaml:"initialInterval"`
		// BackoffCoefficient multiplies the delay after each retry. Zero uses the owning config field's default.
		BackoffCoefficient float64 `yaml:"backoffCoefficient"`
		// MaximumInterval caps the delay between attempts. Zero uses the owning config field's default.
		MaximumInterval time.Duration `yaml:"maximumInterval"`
		// MaximumAttempts includes the initial attempt. Zero uses the owning config field's default or total-duration bound.
		MaximumAttempts int32 `yaml:"maximumAttempts"`
		// TotalDuration caps elapsed retry time. Zero uses the owning config field's default or disables the duration bound.
		TotalDuration time.Duration `yaml:"totalDuration"`
	}

	Config struct {
		// Log is process logging (stdout/stderr/file, level, encoding).
		Log Logger `yaml:"log"`
		// Api is the public FlowService and internal InternalService gRPC server config.
		Api ApiConfig `yaml:"api"`
		// Web configures the Dex Web HTTP server and its FlowService connection.
		Web WebConfig `yaml:"web"`
		// Worker configures shared WorkerService clients. Immutable after startup.
		Worker WorkerConfig `yaml:"worker"`
		// Membership configures optional multi-instance discovery. Nil uses an in-process single-member ring. Immutable after startup.
		Membership *MembershipConfig `yaml:"membership"`
		// Matching configures pull Worker dispatch and stream limits. Nil uses defaults. Immutable after startup.
		Matching *MatchingConfig `yaml:"matching"`
		// Interpreter selects Temporal or Cadence and worker activity settings. Exactly one of Temporal/Cadence must be set.
		Interpreter Interpreter `yaml:"interpreter"`
		// BlobStore offloads large Attribute payloads above ThresholdInBytes. Default enabled.
		BlobStore BlobStoreConfig `yaml:"blobStore"`
		// AttributeStore configures optional MySQL/Postgres Attribute synchronization. Default disabled when Stores is empty.
		AttributeStore AttributeStoreConfig `yaml:"attributeStore"`
		// StreamStore configures best-effort resumable Streams. Default backend is disabled.
		StreamStore StreamStoreConfig `yaml:"streamStore"`
		// Auth authenticates public API callers. Default empty leaves authentication off.
		Auth AuthConfig `yaml:"auth"`
		// Internal configures a private InternalService listener. Default empty uses the public listener when auth is off.
		// It also configures private MatchingService access.
		Internal InternalConfig `yaml:"internal"`
	}

	MembershipConfig struct {
		// Listen binds the ringpop TChannel listener. Default 0.0.0.0:6833. Its port is advertised with BroadcastAddress.
		Listen string `yaml:"listen"`
		// BroadcastAddress is the reachable IPv4 address advertised to peers. Default first private IPv4; it must differ from a wildcard bind address.
		BroadcastAddress string `yaml:"broadcastAddress"`
		// Seeds selects DNS or static peer addresses. Default empty, which starts a cluster containing only this listener.
		Seeds MembershipSeedsConfig `yaml:"seeds"`
		// RefreshInterval re-resolves DNS seeds and role membership. Default 10s. Must be positive when set.
		RefreshInterval time.Duration `yaml:"refreshInterval"`
	}

	MembershipSeedsConfig struct {
		// DNSName resolves a headless service's host:port to peer addresses. Default empty. Mutually exclusive with Static.
		DNSName string `yaml:"dnsName"`
		// Static lists peer host:port addresses. Default empty. Mutually exclusive with DNSName.
		Static []string `yaml:"static"`
	}

	// MatchingConfig sets waiting budgets and stream limits for pull Workers.
	MatchingConfig struct {
		// DispatchWait bounds a regular attempt's Worker search. Default 10s. Must be positive when set.
		DispatchWait time.Duration `yaml:"dispatchWait"`
		// StickyWait bounds searching a sticky queue while it has connected streams before fallback. Default 5s. When set, must not exceed DispatchWait.
		StickyWait time.Duration `yaml:"stickyWait"`
		// LocalDispatchWait bounds a local attempt's Worker search. Default 3s. Must be positive when set.
		LocalDispatchWait time.Duration `yaml:"localDispatchWait"`
		// LocalStickyWait bounds a local attempt's sticky queue search while it has connected streams. Default 2s. When set, must not exceed LocalDispatchWait.
		LocalStickyWait time.Duration `yaml:"localStickyWait"`
		// LocalExecutionReserve preserves time for local task execution. Default 4s. Must be positive when set.
		LocalExecutionReserve time.Duration `yaml:"localExecutionReserve"`
		// MaxConcurrentTasksCap bounds registered streams per appWorkerID on each owner node. Default 256. Must be positive when set.
		MaxConcurrentTasksCap int `yaml:"maxConcurrentTasksCap"`
		// MaxStreams bounds registered streams on this server. Default 100000. Must be positive when set.
		MaxStreams int `yaml:"maxStreams"`
		// MaxQueueLength bounds queue and fallback queue names. Default 256. Must be positive when set.
		MaxQueueLength int `yaml:"maxQueueLength"`
		// MaxFlowTypes bounds declared Flow types per Worker registration. Default 256. Must be positive when set.
		MaxFlowTypes int `yaml:"maxFlowTypes"`
		// MaxFlowTypeLength bounds each declared Flow type. Default 256. Must be positive when set.
		MaxFlowTypeLength int `yaml:"maxFlowTypeLength"`
		// PingInterval sends an application Ping on idle poll streams. Default 20s. Must be positive when set.
		PingInterval time.Duration `yaml:"pingInterval"`
	}

	StreamStoreConfig struct {
		// Backend selects disabled, memory, or redis storage. Default disabled. Immutable after startup.
		Backend StreamStoreBackend `yaml:"backend"`
		// RedisURL is a Redis 7+ Standalone URL. Default empty. Required only by the redis backend. Immutable after startup.
		RedisURL string `yaml:"redisURL"`
		// MaxMessageBytes limits each serialized Stream Value. Default 102400. Must be positive after defaults.
		MaxMessageBytes int64 `yaml:"maxMessageBytes"`
		// MaxReadMessages caps one ListStreamMessages page. Default 1000. Must be positive after defaults. Immutable after startup.
		MaxReadMessages int32 `yaml:"maxReadMessages"`
		// EstimatedMessageOverheadBytes is charged per message beyond payload and identity bytes. Default 512. Must be non-negative.
		EstimatedMessageOverheadBytes int64 `yaml:"estimatedMessageOverheadBytes"`
		// TrimTriggerPercent starts asynchronous trimming at this capacity percentage. Default 90. Valid range is 1 through 99.
		TrimTriggerPercent int32 `yaml:"trimTriggerPercent"`
		// TrimTargetPercent stops asynchronous trimming at this capacity percentage. Default 80. Must be below TrimTriggerPercent.
		TrimTargetPercent int32 `yaml:"trimTargetPercent"`
		// BackgroundTrimBatchSize caps messages removed by one trim batch. Default 256. Must be positive after defaults.
		BackgroundTrimBatchSize int `yaml:"backgroundTrimBatchSize"`
		// TrimLeaseTTL controls the Redis lease lifetime for one active trimmer. Default 5s. Must be positive. Redis backend only.
		TrimLeaseTTL time.Duration `yaml:"trimLeaseTTL"`
		// TrimLeaseRetry controls how often a server retries a held trim lease. Default 100ms. Must be positive. Redis backend only.
		TrimLeaseRetry time.Duration `yaml:"trimLeaseRetry"`
		// TrimBatchYieldTime controls the pause between atomic trim batches. Default 1ms. Must be positive after defaults.
		TrimBatchYieldTime time.Duration `yaml:"trimBatchYieldTime"`
		// TrimWorkers bounds concurrent background trim jobs per server process. Default 4. Must be positive after defaults.
		TrimWorkers int `yaml:"trimWorkers"`
	}

	StreamStoreBackend string

	BlobStoreConfig struct {
		// Enabled turns blob offload on or off. Default true when omitted.
		Enabled *bool `yaml:"enabled"`
		// AsyncStepInputSnapshotsEnabled stores successful asynchronous local Step method inputs for semantic history. Default true when omitted and Blob Store is enabled. Requires Enabled when true and is immutable after startup. Set false to skip this optional history cost.
		AsyncStepInputSnapshotsEnabled *bool `yaml:"asyncStepInputSnapshotsEnabled"`
		// LazyLoading turns lazy loading on or off.
		// When on, server will only send blobIDs to worker for worker APIs(invoke waitFor/execute/RPC) and GetAttribute API.
		// Worker wil call LoadBlobs API to get the actual values.
		// So that worker & server can minimize the data transfer, and worker can cache the values if needed.
		// Default true when omitted (nil).
		LazyLoading *bool `yaml:"lazyLoading"`
		// ThresholdInBytes triggers blob offload above this payload size. Default 100. Zero uses the default.
		ThresholdInBytes int `yaml:"thresholdInBytes"`
		// ObjectIDLength sets deterministic lowercase Base36 Blob object IDs. Default 10. Zero uses the default; negative values are invalid. Immutable after startup and identical across Servers sharing a namespace.
		ObjectIDLength int `yaml:"objectIdLength"`
		// SupportedStorages lists blob backends. Exactly one may have Status active for writes; others are read-only.
		SupportedStorages []BlobStoreConfigEntry `yaml:"supportedStorages"`
		// HistoryRetentionInDays must match the Temporal/Cadence history retention. Default 0; configure it explicitly.
		HistoryRetentionInDays int `yaml:"historyRetentionInDays"`
		// BlobCache configures the S3 Attribute disk cache. Default disabled because Directory is empty. Immutable after startup.
		BlobCache BlobCacheConfig `yaml:"blobCache"`
	}

	BlobCacheConfig struct {
		// Directory exclusively stores cached S3 Attribute blobs. Default empty disables caching. Immutable after startup.
		Directory string `yaml:"directory"`
		// MaxBytes caps cache-owned logical file bytes. Default 1073741824. Must be positive when Directory is configured.
		MaxBytes int64 `yaml:"maxBytes"`
	}

	AttributeStoreType string

	AttributeStoreColumnNaming string

	AttributeStoreConfig struct {
		// Stores maps FlowConfig names to immutable external destinations. Default empty disables Attribute synchronization.
		Stores map[string]AttributeStoreConfigEntry `yaml:"stores"`
		// SchemaSyncInterval refreshes SQL table schemas. Default 1m. Each interval receives uniform ±10% jitter.
		SchemaSyncInterval time.Duration `yaml:"schemaSyncInterval"`
		// SyncBatchSize caps contiguous items per Attribute Store upsert. Default 100. Must be positive after defaults.
		SyncBatchSize int `yaml:"syncBatchSize"`
		// SyncAttemptTimeout caps each regular Activity attempt. Default 30s. Must be positive after defaults.
		SyncAttemptTimeout time.Duration `yaml:"syncAttemptTimeout"`
		// SyncRetryPolicy controls regular Activity retries. Nil or zero fields default to 100ms/30s/2x/unlimited attempts/1h total.
		SyncRetryPolicy *RetryPolicy `yaml:"syncRetryPolicy"`
	}

	AttributeStoreConfigEntry struct {
		// Type selects mysql, postgres, databricks, snowflake, or mongodb. Default empty is invalid for a configured entry. Immutable after startup.
		Type AttributeStoreType `yaml:"type"`
		// DSN is the driver connection string or MongoDB URI. Default empty is invalid. It may contain credentials and is never logged.
		DSN string `yaml:"dsn"`
		// TableName selects the SQL target. MySQL and PostgreSQL accept up to two parts; warehouses accept up to three. Default empty is invalid for SQL stores. Immutable after startup.
		TableName string `yaml:"tableName"`
		// FlowIDColumn stores Flow IDs in Databricks and Snowflake. Default empty is invalid for warehouse stores. Immutable after startup.
		FlowIDColumn string `yaml:"flowIdColumn"`
		// DatabaseName selects the MongoDB database. Default empty is invalid for MongoDB stores. Immutable after startup.
		DatabaseName string `yaml:"databaseName"`
		// CollectionName selects the existing MongoDB collection. Default empty is invalid for MongoDB stores. Immutable after startup.
		CollectionName string `yaml:"collectionName"`
		// RowKey splits each Flow ID into the primary-key columns of a PostgreSQL or MySQL table, such as
		// release-{project_id}-{release_id}. Each {column} placeholder captures up to the next literal text, and the
		// last placeholder captures the rest; {*} captures without a column. The table's primary key must be exactly the
		// named columns, all character columns. A Flow ID that does not match skips its batch with an error log.
		// Default empty stores the whole Flow ID in the table's single primary-key column. Immutable after startup.
		RowKey string `yaml:"rowKey"`
		// ColumnNaming maps Attribute names, and the fields of AttributeMap instance values, to PostgreSQL or MySQL
		// column names: empty (default) uses each name exactly, and snake_case uses its snake_case form. Immutable after
		// startup.
		ColumnNaming AttributeStoreColumnNaming `yaml:"columnNaming"`
		// AttributeMaps writes each instance of the named AttributeMaps as one row of its own PostgreSQL or MySQL table,
		// keyed by AttributeMap name. Writing an instance upserts its row and deleting it deletes the row, in the same
		// transaction as the batch's other writes. Default empty writes AttributeMap instances like other Attributes.
		// Immutable after startup.
		AttributeMaps map[string]AttributeStoreMapTableConfig `yaml:"attributeMaps"`
	}

	AttributeStoreMapTableConfig struct {
		// TableName selects the instance table as table or schema.table. Its primary key must be exactly the store's
		// primary-key columns plus InstanceKeyColumn, all character columns. Required. Immutable after startup.
		TableName string `yaml:"tableName"`
		// InstanceKeyColumn stores the unescaped instance key, such as account-7 for the instance written as
		// Members/account-7. Required. Immutable after startup.
		InstanceKeyColumn string `yaml:"instanceKeyColumn"`
	}

	StorageStatus       string
	StorageType         string
	CleanupStrategyType string

	CleanupStrategy struct {
		// CleanupStrategyType selects cleanup eligibility. Default and only supported value: afterAllRunsDeleted.
		CleanupStrategyType CleanupStrategyType `yaml:"cleanupStrategyType"`
		// CleanupFrequencyInDays schedules cleanup at midnight every Nth day-of-month. Default 0 disables scheduled cleanup.
		CleanupFrequencyInDays int `yaml:"cleanupFrequencyInDays"`
	}

	BlobStoreConfigEntry struct {
		// Status is "active" (writable) or "inactive" (read-only). Only one active store is allowed.
		Status StorageStatus
		// StorageId identifies this backend inside blob ids persisted on Value.
		StorageId string `yaml:"storageId"`
		// StorageType selects "s3" or "local". Default is empty and invalid when storage is enabled.
		StorageType StorageType `yaml:"storageType"`
		// LocalDirectory stores blobs on the server filesystem for "local". Default empty; a directory is required.
		LocalDirectory string `yaml:"localDirectory"`
		// S3Endpoint overrides the S3 API base URL (e.g. http://localhost:9000 for MinIO). Default empty uses AWS endpoint resolution. Immutable after startup.
		S3Endpoint string `yaml:"s3Endpoint"`
		// S3Bucket is the bucket name for object storage.
		S3Bucket string `yaml:"s3Bucket"`
		// S3Region is the AWS/S3 region string.
		S3Region string `yaml:"s3Region"`
		// S3AccessKey is the optional static S3 access key ID. Default empty with S3SecretKey empty uses the AWS credential chain. Set both or neither. Immutable after startup.
		S3AccessKey string `yaml:"s3AccessKey"`
		// S3SecretKey is the optional static S3 secret access key. Default empty with S3AccessKey empty uses the AWS credential chain. Set both or neither. Immutable after startup.
		S3SecretKey string `yaml:"s3SecretKey"`
		// S3KeyPrefix is prepended to every object key of this S3 storage, before the namespace. Default empty.
		// It must not start with "/". Use it to place Dex objects under a bucket prefix that access policies scope,
		// for example "dex/". Immutable after startup.
		S3KeyPrefix string `yaml:"s3KeyPrefix"`
		// CleanupStrategy controls automatic blob cleanup. Default type is afterAllRunsDeleted; zero frequency disables scheduling.
		CleanupStrategy CleanupStrategy `yaml:"cleanupStrategy"`
	}

	ApiConfig struct {
		// Port is the TCP port for FlowService and, when auth is off, InternalService (plaintext gRPC). Default 8801. Bind is 0.0.0.0:Port; SDKs/integ and the interpreter CAN activity dial this port when auth is off.
		// WorkerTaskService also uses this port. MatchingService is private.
		Port int `yaml:"port"`
		// MaxWaitSeconds caps transport long polls. Zero uses DefaultMaxWaitSeconds (60). Positive values are the cap. Negatives are invalid. It does not limit Temporal Update handlers.
		MaxWaitSeconds int64 `yaml:"maxWaitSeconds"`
		// GrpcMaxMessageBytes is MaxRecv/MaxSend for FlowService, InternalService, and WorkerService clients. Default 32 MiB. Must be positive and larger than continue-as-new page size plus overhead.
		// Keep it above the largest application value plus envelope headroom; the default leaves 16 MiB for values up to 16 MiB.
		// SDK Clients and Workers apply their own limit (the Go SDK's GrpcMaxMessageBytes receive limit also defaults to 32 MiB); the smaller limit of each hop applies.
		GrpcMaxMessageBytes int `yaml:"grpcMaxMessageBytes"`
		// UseTemporalSynchronousUpdateForAllRPCs routes non-locking Temporal RPCs through synchronous Updates. Default false because Temporal limits concurrent Updates. Enabling trades that capacity for validator rejection and atomic effect commits. Locking Temporal RPCs always use Updates.
		UseTemporalSynchronousUpdateForAllRPCs bool `yaml:"useTemporalSynchronousUpdateForAllRPCs"`
		// QueryWorkflowFailedRetryPolicy retries failed backend queries. Nil or zero fields default to 100ms fixed intervals and 5 attempts.
		// Its intervals also pace retries of a Temporal query attempt that reached its own deadline before the caller's deadline.
		// Such an attempt waits for the Flow's unfinished workflow task, and the Temporal SDK gives one attempt at most
		// 10 seconds or half of the remaining caller deadline. These retries ignore MaximumAttempts and TotalDuration and
		// stop at the caller's deadline. A caller without a deadline gets one 10-second attempt.
		QueryWorkflowFailedRetryPolicy *RetryPolicy `yaml:"queryWorkflowFailedRetryPolicy"`
		// InvokeRPCContinuedAsNewErrorRetryPolicy retries transient InvokeRPC failures across current-run changes. Nil or zero fields default to 100ms initial, 2x backoff, 1s maximum, and 5s total duration.
		InvokeRPCContinuedAsNewErrorRetryPolicy *RetryPolicy `yaml:"invokeRPCContinuedAsNewErrorRetryPolicy"`
	}

	WebConfig struct {
		// BindAddress is the Dex Web HTTP bind address. Default 0.0.0.0. Immutable after startup.
		BindAddress string `yaml:"bindAddress"`
		// Port is the Dex Web HTTP bind port. Default 8802. Immutable after startup.
		Port int `yaml:"port"`
		// FlowServiceTarget is the plaintext FlowService gRPC target. Default localhost:<api.port>. Immutable after startup.
		FlowServiceTarget string `yaml:"flowServiceTarget"`
		// FlowServiceCredentialFile holds the bearer token Dex Web sends to FlowService on every call.
		// Default empty sends no credential. When Web starts, a configured file must exist and contain a non-empty token;
		// surrounding whitespace is trimmed.
		// The token is sent even when auth is disabled. Dex checks file changes at most once per second; an empty or
		// unreadable file then fails each call with Unauthenticated until restored. The path is immutable after startup.
		// When auth is enabled and Web runs in this process, an empty path fails startup. Validation uses this process's
		// auth config only; a Web-only process connecting to an authenticated remote FlowService must also set this field.
		FlowServiceCredentialFile string `yaml:"flowServiceCredentialFile"`
		// FlowRenderingDirectory supplies Flow Definition Graph JSON files. Default empty disables definitions. Immutable after startup.
		FlowRenderingDirectory string `yaml:"flowRenderingDirectory"`
		// StartFlowWorkerTargetHeadless applies headless routing to address targets started from Dex Web. Default false. Immutable after startup.
		IsStartFlowWorkerTargetHeadless bool `yaml:"startFlowWorkerTargetHeadless"`
		// IsStartFlowWorkerTargetPull selects a pull target when a Dex Web start omits the Worker target. Default false.
		// Explicit address targets still work; startFlowWorkerTargetHeadless applies to them only. Immutable after startup.
		IsStartFlowWorkerTargetPull bool `yaml:"startFlowWorkerTargetPull"`
	}

	WorkerConfig struct {
		// Wake requests unavailable Workers to start. Default empty disables the hook.
		Wake WakeConfig `yaml:"wake"`
		// DefaultHeaders are forwarded as outgoing gRPC metadata on WorkerService calls. Empty means none.
		// Default empty.
		DefaultHeaders map[string]string `yaml:"defaultHeaders"`
		// WorkerConnectionIdleTimeout evicts idle, unreferenced WorkerService connections. Non-positive defaults to 10m. Immutable after startup.
		WorkerConnectionIdleTimeout time.Duration `yaml:"workerConnectionIdleTimeout"`
		// MaxWorkerConnections caps the WorkerService connection pool. Non-positive defaults to 1000. Immutable after startup.
		MaxWorkerConnections int `yaml:"maxWorkerConnections"`
		// HeadlessAddressRefreshInterval refreshes headless WorkerService DNS addresses. Non-positive defaults to 60s. Immutable after startup.
		HeadlessAddressRefreshInterval time.Duration `yaml:"headlessAddressRefreshInterval"`
		// MaxStickyRoutingEntries caps remembered flow routes. Non-positive defaults to 100000. Immutable after startup.
		MaxStickyRoutingEntries int `yaml:"maxStickyRoutingEntries"`
		// WorkerServiceRequestMaxAttempts includes the first headless transport attempt. Non-positive defaults to 3. Activity retries are independent.
		WorkerServiceRequestMaxAttempts int `yaml:"workerServiceRequestMaxAttempts"`
		// HeadlessFailoverStatusCodes trigger endpoint failover. Empty defaults to Unavailable (14), DeadlineExceeded (4), and Unknown (2).
		// Errors returned by a Worker handler, which carry WorkerErrorResponse details, never fail over whatever their code, because
		// another endpoint would repeat them; they fail the Step or RPC attempt and follow its retry policy.
		// See https://grpc.io/docs/guides/status-codes/. Immutable after startup.
		HeadlessFailoverStatusCodes []int `yaml:"headlessFailoverStatusCodes"`
	}

	Interpreter struct {
		// Temporal connects the interpreter to a Temporal cluster. Mutually exclusive with Cadence.
		Temporal *TemporalConfig `yaml:"temporal"`
		// Cadence connects the interpreter to a Cadence cluster. Mutually exclusive with Temporal.
		Cadence *CadenceConfig `yaml:"cadence"`
		// DefaultWorkflowConfig is the default FlowConfig applied when StartFlow omits an override. Nil uses package DefaultWorkflowConfig.
		DefaultWorkflowConfig *dexpb.FlowConfig `yaml:"defaultWorkflowConfig"`
		// InterpreterActivityConfig tunes worker→API and worker→WorkerService dialing.
		InterpreterActivityConfig InterpreterActivityConfig `yaml:"interpreterActivityConfig"`
		// VerboseDebug enables extra interpreter debug logs. Default false.
		VerboseDebug bool `yaml:"verboseDebug"`
		// AttributeIndexSyncTimeout bounds registration and backend propagation checks. Default 2m. Immutable after startup.
		AttributeIndexSyncTimeout time.Duration `yaml:"attributeIndexSyncTimeout"`
		// AttributeIndexesManagedExternally skips backend index discovery and creation after validating declarations.
		// Operators must provision Dex system indexes and every indexed application Attribute before starting Dex or Workers.
		AttributeIndexesManagedExternally bool `yaml:"attributeIndexesManagedExternally"`
	}

	TemporalConfig struct {
		// HostPort is the Temporal frontend address. Default localhost:7233. Client dials this gRPC endpoint.
		HostPort string `yaml:"hostPort"`
		// CloudAPIKey authenticates to Temporal Cloud. Empty means no cloud credentials.
		CloudAPIKey string `yaml:"cloudAPIKey"`
		// APIKeyFile holds a rotating Temporal API key. Default empty disables file credentials. Dex reads it at startup and retains the last valid value if a later refresh fails.
		APIKeyFile string `yaml:"apiKeyFile"`
		// APIKeyFileRefreshInterval is the minimum interval between file checks. Default 30s when zero. Negative values are invalid.
		APIKeyFileRefreshInterval time.Duration `yaml:"apiKeyFileRefreshInterval"`
		// TLS configures the Temporal frontend connection. Nil enables TLS with system roots when an API key is configured and plaintext otherwise. A nonempty TLS block enables TLS when enabled is omitted.
		TLS *TemporalTLSConfig `yaml:"tls"`
		// CloudOps configures Temporal Cloud namespace operations for attribute index synchronization.
		// Nil uses the Temporal data-plane OperatorService for self-hosted clusters.
		CloudOps *TemporalCloudOpsConfig `yaml:"cloudOps"`
		// Namespace is the Temporal namespace. Default "default".
		Namespace string `yaml:"namespace"`
		// Prometheus configures the Temporal SDK metrics exposer. Nil disables.
		Prometheus *prometheus.Configuration `yaml:"prometheus"`
		// WorkerOptions are passed to the Temporal worker. Nil uses SDK defaults.
		WorkerOptions *temporalWorker.Options
	}

	TemporalTLSConfig struct {
		// Enabled explicitly enables or disables TLS. Nil enables TLS when an API key or another TLS setting is configured and plaintext otherwise.
		Enabled *bool `yaml:"enabled"`
		// CAFile supplies trusted server certificates. Default empty uses system roots. A configured file replaces system roots and is loaded once at startup; restart Dex after rotating it.
		CAFile string `yaml:"caFile"`
		// ServerName overrides the TLS server name. Default empty uses the host in hostPort for certificate verification.
		ServerName string `yaml:"serverName"`
		// CertFile supplies a client certificate for mutual TLS. Default empty sends no client certificate. Set keyFile too; Dex reloads changed files for new handshakes.
		CertFile string `yaml:"certFile"`
		// KeyFile supplies the private key for certFile. Default empty sends no client certificate. Set certFile too; Dex reloads changed files for new handshakes.
		KeyFile string `yaml:"keyFile"`
	}

	TemporalCloudOpsConfig struct {
		// HostPort is the Temporal Cloud Operations API gRPC endpoint.
		HostPort string `yaml:"hostPort"`
		// APIVersion pins the Temporal Cloud Operations API contract.
		APIVersion string `yaml:"apiVersion"`
	}

	CadenceConfig struct {
		// HostPort is the Cadence frontend address. Default 127.0.0.1:7833.
		HostPort string `yaml:"hostPort"`
		// Domain is the Cadence domain. Default "default".
		Domain string `yaml:"domain"`
		// AdminSecurityToken authorizes Cadence search attribute registration. Default empty.
		AdminSecurityToken string `yaml:"adminSecurityToken"`
		// WorkerOptions are passed to the Cadence worker. Nil uses SDK defaults.
		WorkerOptions *cadenceWorker.Options
	}

	InterpreterActivityConfig struct {
		// InternalServiceTarget is the plaintext gRPC dial target for InternalService (CAN dump). Empty defaults to localhost:<Api.Port>. YAML key internalServiceTarget.
		InternalServiceTarget string `yaml:"internalServiceTarget"`
		// MinimumStepHeartbeatTimeout rejects smaller explicit Step heartbeat timeouts. Default 10s. Zero uses the default; negative values are invalid.
		MinimumStepHeartbeatTimeout time.Duration `yaml:"minimumStepHeartbeatTimeout"`
		// DumpWorkflowInternalActivityConfig tunes the CAN dump activity timeouts/retries. Nil uses activity defaults.
		DumpWorkflowInternalActivityConfig *DumpWorkflowInternalActivityConfig `yaml:"dumpWorkflowInternalActivityConfig"`
		// SubFlowStartActivityConfig tunes engine-owned local SubFlow start retries. Nil uses a two-minute retry window.
		SubFlowStartActivityConfig *SubFlowStartActivityConfig `yaml:"subFlowStartActivityConfig"`
		// LogLocalActivityThresholdBytes logs local-activity I/O at warn when serialized size >= this. Zero disables. Default 0.
		LogLocalActivityThresholdBytes int `yaml:"logLocalActivityThresholdBytes"`
	}

	DumpWorkflowInternalActivityConfig struct {
		// StartToCloseTimeout is the activity start-to-close timeout. Zero uses the activity registration default.
		StartToCloseTimeout time.Duration `yaml:"startToCloseTimeout"`
		// RetryPolicy is the activity retry policy. Nil or zero fields default to 100ms/100s/2x/unlimited attempts and no total-duration bound.
		RetryPolicy *RetryPolicy `yaml:"retryPolicy"`
	}

	SubFlowStartActivityConfig struct {
		// ScheduleToCloseTimeout bounds all local attempts to start a SubFlow. Zero uses 2m; negative values are invalid.
		ScheduleToCloseTimeout time.Duration `yaml:"scheduleToCloseTimeout"`
		// RetryPolicy controls engine-owned SubFlow start retries within ScheduleToCloseTimeout. Nil or zero fields default to 100ms/60s/2x/unlimited attempts and no total-duration bound.
		RetryPolicy *RetryPolicy `yaml:"retryPolicy"`
	}

	Logger struct {
		// Stdout sends logs to stdout when true; otherwise stderr (unless OutputFile is set). Default false.
		Stdout bool `yaml:"stdout"`
		// Level is the zap log level string (debug/info/warn/error). Default depends on NewZapLogger.
		Level string `yaml:"level"`
		// OutputFile writes logs to this path when non-empty and Stdout is false.
		OutputFile string `yaml:"outputFile"`
		// LevelKey is the JSON field name for level. Default "level".
		LevelKey string `yaml:"levelKey"`
		// Encoding is "json" or "console". Default "json".
		Encoding string `yaml:"encoding"`
	}
)

// RetryPolicyWithDefaults applies field-specific defaults to a retry policy.
func RetryPolicyWithDefaults(policy *RetryPolicy, defaults RetryPolicy) RetryPolicy {
	effective := RetryPolicy{}
	if policy != nil {
		effective = *policy
	}
	if effective.InitialInterval == 0 {
		effective.InitialInterval = defaults.InitialInterval
	}
	if effective.BackoffCoefficient == 0 {
		effective.BackoffCoefficient = defaults.BackoffCoefficient
	}
	if effective.MaximumInterval == 0 {
		effective.MaximumInterval = defaults.MaximumInterval
	}
	if effective.MaximumAttempts == 0 {
		effective.MaximumAttempts = defaults.MaximumAttempts
	}
	if effective.TotalDuration == 0 {
		effective.TotalDuration = defaults.TotalDuration
	}
	return effective
}

// DefaultWorkflowConfig is used when Interpreter.DefaultWorkflowConfig is nil.
var DefaultWorkflowConfig = &dexpb.FlowConfig{
	ActiveStepSearchMode:   ptr.Any(dexpb.ActiveStepSearchMode_ACTIVE_STEP_SEARCH_MODE_DISABLED),
	ContinueAsNewThreshold: ptr.Any(int32(100)),
	StepDurability:         ptr.Any(dexpb.StepDurability_STEP_DURABILITY_SYNC),
}

// NewConfig returns a new decoded Config struct.
func NewConfig(configPath string) (*Config, error) {
	log.Printf("Loading configFile=%v\n", configPath)

	cfg := &Config{}

	file, err := os.Open(configPath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var document yaml.Node
	if err := yaml.NewDecoder(file).Decode(&document); err != nil {
		return nil, err
	}
	if err := rejectRemovedTenancyConfig(&document); err != nil {
		return nil, err
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	d := yaml.NewDecoder(file)
	d.KnownFields(true)

	if err := d.Decode(&cfg); err != nil {
		return nil, err
	}
	if err := applyWebEnvironment(cfg); err != nil {
		return nil, err
	}
	if cfg.Interpreter.Temporal != nil {
		if err := cfg.Interpreter.Temporal.ValidateTemporalConnection(); err != nil {
			return nil, err
		}
	}
	if cfg.Interpreter.Temporal != nil && cfg.Interpreter.Temporal.CloudOps != nil {
		cloudOps := cfg.Interpreter.Temporal.CloudOps
		if strings.TrimSpace(cfg.Interpreter.Temporal.CloudAPIKey) == "" && cfg.Interpreter.Temporal.APIKeyFile == "" {
			return nil, fmt.Errorf("interpreter.temporal.cloudAPIKey or apiKeyFile is required when cloudOps is configured")
		}
		if _, _, err := net.SplitHostPort(cloudOps.HostPort); err != nil {
			return nil, fmt.Errorf("interpreter.temporal.cloudOps.hostPort must be host:port: %w", err)
		}
		if matched, _ := regexp.MatchString(`^v[0-9]+\.[0-9]+\.[0-9]+$`, cloudOps.APIVersion); !matched {
			return nil, fmt.Errorf("interpreter.temporal.cloudOps.apiVersion must be a version such as v0.19.1")
		}
	}
	if err := cfg.validateRetryPolicies(); err != nil {
		return nil, err
	}
	if cfg.Membership != nil {
		if err := cfg.Membership.Validate(); err != nil {
			return nil, err
		}
	}
	if cfg.Matching != nil {
		if err := cfg.Matching.Validate(); err != nil {
			return nil, err
		}
	}
	if cfg.Interpreter.InterpreterActivityConfig.MinimumStepHeartbeatTimeout < 0 {
		return nil, fmt.Errorf("interpreter.interpreterActivityConfig.minimumStepHeartbeatTimeout must be non-negative")
	}
	if err := cfg.ValidateGeneralAccess(); err != nil {
		return nil, err
	}

	return cfg, nil
}

// WithDefaults fills omitted matching settings without modifying the receiver.
func (c MatchingConfig) WithDefaults() MatchingConfig {
	if c.DispatchWait == 0 {
		c.DispatchWait = DefaultMatchingDispatchWait
	}
	if c.StickyWait == 0 {
		c.StickyWait = DefaultMatchingStickyWait
	}
	if c.LocalDispatchWait == 0 {
		c.LocalDispatchWait = DefaultMatchingLocalDispatchWait
	}
	if c.LocalStickyWait == 0 {
		c.LocalStickyWait = DefaultMatchingLocalStickyWait
	}
	if c.LocalExecutionReserve == 0 {
		c.LocalExecutionReserve = DefaultMatchingLocalExecutionReserve
	}
	if c.MaxConcurrentTasksCap == 0 {
		c.MaxConcurrentTasksCap = DefaultMatchingMaxConcurrentTasksCap
	}
	if c.MaxStreams == 0 {
		c.MaxStreams = DefaultMatchingMaxStreams
	}
	if c.MaxQueueLength == 0 {
		c.MaxQueueLength = DefaultMatchingMaxQueueLength
	}
	if c.MaxFlowTypes == 0 {
		c.MaxFlowTypes = DefaultMatchingMaxFlowTypes
	}
	if c.MaxFlowTypeLength == 0 {
		c.MaxFlowTypeLength = DefaultMatchingMaxFlowTypeLength
	}
	if c.PingInterval == 0 {
		c.PingInterval = DefaultMatchingPingInterval
	}
	return c
}

// Validate checks matching wait relationships and stream limits.
func (c MatchingConfig) Validate() error {
	settings := c.WithDefaults()
	if settings.DispatchWait <= 0 || settings.StickyWait <= 0 || (c.StickyWait != 0 && settings.StickyWait > settings.DispatchWait) {
		return fmt.Errorf("matching.stickyWait must be positive and no greater than dispatchWait")
	}
	if settings.LocalDispatchWait <= 0 || settings.LocalStickyWait <= 0 || (c.LocalStickyWait != 0 && settings.LocalStickyWait > settings.LocalDispatchWait) {
		return fmt.Errorf("matching.localStickyWait must be positive and no greater than localDispatchWait")
	}
	if settings.LocalExecutionReserve <= 0 || settings.MaxConcurrentTasksCap <= 0 || settings.MaxStreams <= 0 || settings.MaxQueueLength <= 0 || settings.MaxFlowTypes <= 0 || settings.MaxFlowTypeLength <= 0 {
		return fmt.Errorf("matching execution reserve and stream limits must be positive")
	}
	if settings.PingInterval <= 0 {
		return fmt.Errorf("matching.pingInterval must be positive")
	}
	return nil
}

// EffectiveListen returns the configured ringpop bind address or its default.
func (c MembershipConfig) EffectiveListen() string {
	if c.Listen == "" {
		return DefaultMembershipListen
	}
	return c.Listen
}

// EffectiveRefreshInterval returns the configured DNS and ring refresh interval or its default.
func (c MembershipConfig) EffectiveRefreshInterval() time.Duration {
	if c.RefreshInterval == 0 {
		return DefaultMembershipRefreshInterval
	}
	return c.RefreshInterval
}

// Validate checks membership listener, advertised address, and seed configuration.
func (c MembershipConfig) Validate() error {
	if _, _, err := net.SplitHostPort(c.EffectiveListen()); err != nil {
		return fmt.Errorf("membership.listen must be host:port: %w", err)
	}
	if c.BroadcastAddress != "" && net.ParseIP(c.BroadcastAddress) == nil {
		return fmt.Errorf("membership.broadcastAddress must be an IP address")
	}
	if c.RefreshInterval < 0 {
		return fmt.Errorf("membership.refreshInterval must be positive")
	}
	if c.Seeds.DNSName != "" && len(c.Seeds.Static) != 0 {
		return fmt.Errorf("membership.seeds.dnsName and static are mutually exclusive")
	}
	if c.Seeds.DNSName != "" {
		if _, _, err := net.SplitHostPort(c.Seeds.DNSName); err != nil {
			return fmt.Errorf("membership.seeds.dnsName must be host:port: %w", err)
		}
	}
	for _, address := range c.Seeds.Static {
		if _, _, err := net.SplitHostPort(address); err != nil {
			return fmt.Errorf("membership.seeds.static address must be host:port: %w", err)
		}
	}
	return nil
}

// ValidateTemporalConnection checks Temporal connection settings.
func (c *TemporalConfig) ValidateTemporalConnection() error {
	if c.CloudAPIKey != "" && c.APIKeyFile != "" {
		return fmt.Errorf("interpreter.temporal.cloudAPIKey and apiKeyFile cannot both be configured")
	}
	if c.APIKeyFileRefreshInterval < 0 {
		return fmt.Errorf("interpreter.temporal.apiKeyFileRefreshInterval must be non-negative")
	}
	if c.TLS != nil && (c.TLS.CertFile == "") != (c.TLS.KeyFile == "") {
		return fmt.Errorf("interpreter.temporal.tls.certFile and keyFile must be configured together")
	}
	if c.TLS != nil && c.TLS.Enabled != nil && !*c.TLS.Enabled && c.TLS.hasConnectionSettings() {
		return fmt.Errorf("interpreter.temporal.tls.enabled cannot be false when other TLS settings are configured")
	}
	return nil
}

// IsTLSEnabled reports whether the Temporal frontend connection uses TLS.
func (c *TemporalConfig) IsTLSEnabled() bool {
	if c.TLS != nil {
		if c.TLS.Enabled != nil {
			return *c.TLS.Enabled
		}
		if c.TLS.hasConnectionSettings() {
			return true
		}
	}
	return c.CloudAPIKey != "" || c.APIKeyFile != ""
}

func (c *TemporalTLSConfig) hasConnectionSettings() bool {
	return c.CAFile != "" || c.ServerName != "" || c.CertFile != "" || c.KeyFile != ""
}

func applyWebEnvironment(cfg *Config) error {
	if value, ok := os.LookupEnv("DEX_WEB_FLOW_RENDERING_DIRECTORY"); ok {
		cfg.Web.FlowRenderingDirectory = value
	}
	if value, ok := os.LookupEnv("DEX_WEB_START_FLOW_WORKER_TARGET_HEADLESS"); ok {
		parsed, err := strconv.ParseBool(value)
		if err != nil {
			return fmt.Errorf("DEX_WEB_START_FLOW_WORKER_TARGET_HEADLESS must be a boolean: %w", err)
		}
		cfg.Web.IsStartFlowWorkerTargetHeadless = parsed
	}
	if value, ok := os.LookupEnv("DEX_WEB_START_FLOW_WORKER_TARGET_PULL"); ok {
		parsed, err := strconv.ParseBool(value)
		if err != nil {
			return fmt.Errorf("DEX_WEB_START_FLOW_WORKER_TARGET_PULL must be a boolean: %w", err)
		}
		cfg.Web.IsStartFlowWorkerTargetPull = parsed
	}
	return nil
}

func (c Config) validateRetryPolicies() error {
	queryRetryPolicy := RetryPolicyWithDefaults(
		c.Api.QueryWorkflowFailedRetryPolicy,
		DefaultQueryWorkflowFailedRetryPolicy,
	)
	if err := validateRetryPolicy("api.queryWorkflowFailedRetryPolicy", queryRetryPolicy); err != nil {
		return err
	}

	invokeRPCRetryPolicy := RetryPolicyWithDefaults(
		c.Api.InvokeRPCContinuedAsNewErrorRetryPolicy,
		DefaultInvokeRPCContinuedAsNewErrorRetryPolicy,
	)
	if err := validateRetryPolicy("api.invokeRPCContinuedAsNewErrorRetryPolicy", invokeRPCRetryPolicy); err != nil {
		return err
	}

	attributeStoreRetryPolicy := RetryPolicyWithDefaults(
		c.AttributeStore.SyncRetryPolicy,
		DefaultAttributeStoreSyncRetryPolicy,
	)
	if err := validateRetryPolicy("attributeStore.syncRetryPolicy", attributeStoreRetryPolicy); err != nil {
		return err
	}

	dumpWorkflowActivityConfig := c.Interpreter.InterpreterActivityConfig.DumpWorkflowInternalActivityConfig
	if dumpWorkflowActivityConfig != nil && dumpWorkflowActivityConfig.RetryPolicy != nil {
		dumpWorkflowRetryPolicy := RetryPolicyWithDefaults(
			dumpWorkflowActivityConfig.RetryPolicy,
			DefaultInternalActivityRetryPolicy,
		)
		if err := validateRetryPolicy(
			"interpreter.interpreterActivityConfig.dumpWorkflowInternalActivityConfig.retryPolicy",
			dumpWorkflowRetryPolicy,
		); err != nil {
			return err
		}
	}
	subFlowStartActivityConfig := c.Interpreter.InterpreterActivityConfig.SubFlowStartActivityConfig
	if subFlowStartActivityConfig != nil && subFlowStartActivityConfig.ScheduleToCloseTimeout < 0 {
		return fmt.Errorf("interpreter.interpreterActivityConfig.subFlowStartActivityConfig.scheduleToCloseTimeout must be non-negative")
	}
	if subFlowStartActivityConfig != nil && subFlowStartActivityConfig.RetryPolicy != nil {
		subFlowStartRetryPolicy := RetryPolicyWithDefaults(
			subFlowStartActivityConfig.RetryPolicy,
			DefaultInternalActivityRetryPolicy,
		)
		if err := validateRetryPolicy(
			"interpreter.interpreterActivityConfig.subFlowStartActivityConfig.retryPolicy",
			subFlowStartRetryPolicy,
		); err != nil {
			return err
		}
	}
	return nil
}

func validateRetryPolicy(name string, policy RetryPolicy) error {
	if policy.InitialInterval <= 0 || policy.MaximumInterval <= 0 ||
		policy.BackoffCoefficient <= 0 || policy.MaximumAttempts < 0 ||
		policy.TotalDuration < 0 {
		return fmt.Errorf("%s values must be positive except maximumAttempts and totalDuration may be zero", name)
	}
	if policy.MaximumInterval < policy.InitialInterval {
		return fmt.Errorf("%s maximumInterval must not be less than initialInterval", name)
	}
	return nil
}

// GetInternalServiceTargetWithDefault returns the plaintext gRPC dial target for InternalService.
func (c Config) GetInternalServiceTargetWithDefault() string {
	if c.Interpreter.InterpreterActivityConfig.InternalServiceTarget != "" {
		return c.Interpreter.InterpreterActivityConfig.InternalServiceTarget
	}
	if c.Internal.Listen != "" && c.Internal.TokenFile != "" {
		host, port, err := net.SplitHostPort(c.Internal.Listen)
		if err == nil && (host == "" || host == "0.0.0.0" || host == "::") {
			return net.JoinHostPort("127.0.0.1", port)
		}
		return c.Internal.Listen
	}
	port := c.Api.Port
	if port == 0 {
		port = DefaultApiPort
	}
	return fmt.Sprintf("localhost:%v", port)
}

// GetWebFlowServiceTargetWithDefault returns the plaintext FlowService target used by Dex Web.
func (c Config) GetWebFlowServiceTargetWithDefault() string {
	if c.Web.FlowServiceTarget != "" {
		return c.Web.FlowServiceTarget
	}
	port := c.Api.Port
	if port == 0 {
		port = DefaultApiPort
	}
	return fmt.Sprintf("localhost:%v", port)
}

// EffectiveBindAddress returns the configured Dex Web bind address or 0.0.0.0.
func (c WebConfig) EffectiveBindAddress() string {
	if c.BindAddress == "" {
		return DefaultWebBindAddress
	}
	return c.BindAddress
}

// EffectivePort returns the configured Dex Web port or 8802.
func (c WebConfig) EffectivePort() int {
	if c.Port == 0 {
		return DefaultWebPort
	}
	return c.Port
}

// EffectiveMaxWaitSeconds returns the transport long-poll cap: DefaultMaxWaitSeconds when MaxWaitSeconds is 0.
// Callers must reject negative MaxWaitSeconds before invoking this.
func (c ApiConfig) EffectiveMaxWaitSeconds() int64 {
	if c.MaxWaitSeconds == 0 {
		return DefaultMaxWaitSeconds
	}
	return c.MaxWaitSeconds
}

// EffectiveBackend returns the configured Stream Store backend or disabled.
func (c StreamStoreConfig) EffectiveBackend() StreamStoreBackend {
	if c.Backend == "" {
		return StreamStoreBackendDisabled
	}
	return c.Backend
}

// EffectiveMaxMessageBytes returns the configured limit or 100-KiB default.
func (c StreamStoreConfig) EffectiveMaxMessageBytes() int64 {
	if c.MaxMessageBytes == 0 {
		return DefaultStreamMaxMessageBytes
	}
	return c.MaxMessageBytes
}

// EffectiveMaxListMessagesPageSize returns the configured page limit or 1000-message default.
func (c StreamStoreConfig) EffectiveMaxListMessagesPageSize() int32 {
	if c.MaxReadMessages == 0 {
		return DefaultStreamMaxListMessagesPageSize
	}
	return c.MaxReadMessages
}

// EffectiveEstimatedMessageOverheadBytes returns the configured charge or 512-byte default.
func (c StreamStoreConfig) EffectiveEstimatedMessageOverheadBytes() int64 {
	if c.EstimatedMessageOverheadBytes == 0 {
		return DefaultStreamEstimatedMessageOverheadBytes
	}
	return c.EstimatedMessageOverheadBytes
}

// EffectiveTrimTriggerPercent returns the configured trigger or ninety-percent default.
func (c StreamStoreConfig) EffectiveTrimTriggerPercent() int32 {
	if c.TrimTriggerPercent == 0 {
		return DefaultStreamTrimTriggerPercent
	}
	return c.TrimTriggerPercent
}

// EffectiveTrimTargetPercent returns the configured target or eighty-percent default.
func (c StreamStoreConfig) EffectiveTrimTargetPercent() int32 {
	if c.TrimTargetPercent == 0 {
		return DefaultStreamTrimTargetPercent
	}
	return c.TrimTargetPercent
}

// EffectiveBackgroundTrimBatchSize returns the configured batch size or 256-message default.
func (c StreamStoreConfig) EffectiveBackgroundTrimBatchSize() int {
	if c.BackgroundTrimBatchSize == 0 {
		return DefaultStreamBackgroundTrimBatchSize
	}
	return c.BackgroundTrimBatchSize
}

// EffectiveTrimLeaseTTL returns the configured lease lifetime or five-second default.
func (c StreamStoreConfig) EffectiveTrimLeaseTTL() time.Duration {
	if c.TrimLeaseTTL == 0 {
		return DefaultStreamTrimLeaseTTL
	}
	return c.TrimLeaseTTL
}

// EffectiveTrimLeaseRetry returns the configured lease retry delay or 100-millisecond default.
func (c StreamStoreConfig) EffectiveTrimLeaseRetry() time.Duration {
	if c.TrimLeaseRetry == 0 {
		return DefaultStreamTrimLeaseRetry
	}
	return c.TrimLeaseRetry
}

// EffectiveTrimBatchYieldTime returns the configured batch pause or one-millisecond default.
func (c StreamStoreConfig) EffectiveTrimBatchYieldTime() time.Duration {
	if c.TrimBatchYieldTime == 0 {
		return DefaultStreamTrimBatchYieldTime
	}
	return c.TrimBatchYieldTime
}

// EffectiveTrimWorkers returns the configured worker count or four-worker default.
func (c StreamStoreConfig) EffectiveTrimWorkers() int {
	if c.TrimWorkers == 0 {
		return DefaultStreamTrimWorkers
	}
	return c.TrimWorkers
}

// Validate checks enabled Stream Store settings and trimming bounds.
func (c StreamStoreConfig) Validate() error {
	switch c.EffectiveBackend() {
	case StreamStoreBackendDisabled:
		if c.RedisURL != "" {
			return fmt.Errorf("stream store redisURL requires redis backend")
		}
	case StreamStoreBackendMemory:
		if c.RedisURL != "" {
			return fmt.Errorf("stream store memory backend does not use redisURL")
		}
	case StreamStoreBackendRedis:
		if c.RedisURL == "" {
			return fmt.Errorf("stream store redis backend requires redisURL")
		}
	default:
		return fmt.Errorf("unsupported stream store backend %q", c.Backend)
	}
	if c.EffectiveMaxMessageBytes() <= 0 {
		return fmt.Errorf("stream store maxMessageBytes must be positive")
	}
	if c.EffectiveMaxListMessagesPageSize() <= 0 {
		return fmt.Errorf("stream store maxReadMessages must be positive")
	}
	if c.EstimatedMessageOverheadBytes < 0 {
		return fmt.Errorf("stream store estimatedMessageOverheadBytes must be non-negative")
	}
	triggerPercent := c.EffectiveTrimTriggerPercent()
	if triggerPercent < 1 || triggerPercent > 99 {
		return fmt.Errorf("stream store trimTriggerPercent must be between 1 and 99")
	}
	targetPercent := c.EffectiveTrimTargetPercent()
	if targetPercent < 1 || targetPercent >= triggerPercent {
		return fmt.Errorf("stream store trimTargetPercent must be positive and less than trimTriggerPercent")
	}
	if c.EffectiveBackgroundTrimBatchSize() <= 0 {
		return fmt.Errorf("stream store backgroundTrimBatchSize must be positive")
	}
	if c.EffectiveTrimLeaseTTL() <= 0 {
		return fmt.Errorf("stream store trimLeaseTTL must be positive")
	}
	if c.EffectiveTrimLeaseRetry() <= 0 {
		return fmt.Errorf("stream store trimLeaseRetry must be positive")
	}
	if c.EffectiveTrimBatchYieldTime() <= 0 {
		return fmt.Errorf("stream store trimBatchYieldTime must be positive")
	}
	if c.EffectiveTrimWorkers() <= 0 {
		return fmt.Errorf("stream store trimWorkers must be positive")
	}
	return nil
}

// EffectiveLazyLoading returns LazyLoading, defaulting to true when omitted.
func (c BlobStoreConfig) EffectiveLazyLoading() bool {
	if c.LazyLoading == nil {
		return true
	}
	return *c.LazyLoading
}

// EffectiveEnabled returns Enabled, defaulting to true when omitted.
func (c BlobStoreConfig) EffectiveEnabled() bool {
	if c.Enabled == nil {
		return true
	}
	return *c.Enabled
}

// EffectiveThresholdInBytes returns the offload threshold or its 100-byte default.
func (c BlobStoreConfig) EffectiveThresholdInBytes() int {
	if c.ThresholdInBytes == 0 {
		return DefaultBlobStoreThresholdInBytes
	}
	return c.ThresholdInBytes
}

// EffectiveObjectIDLength returns the configured Blob object ID length or its default.
func (c BlobStoreConfig) EffectiveObjectIDLength() int {
	if c.ObjectIDLength == 0 {
		return DefaultBlobStoreObjectIDLength
	}
	return c.ObjectIDLength
}

// EffectiveAsyncStepInputSnapshotsEnabled returns whether successful ASYNC local Step inputs are stored for semantic history. Default true when omitted and Blob Store is enabled.
func (c BlobStoreConfig) EffectiveAsyncStepInputSnapshotsEnabled() bool {
	if c.AsyncStepInputSnapshotsEnabled == nil {
		return c.EffectiveEnabled()
	}
	return *c.AsyncStepInputSnapshotsEnabled
}

// Validate checks Blob Store identifier, credentials and cache settings.
func (c BlobStoreConfig) Validate() error {
	if c.EffectiveAsyncStepInputSnapshotsEnabled() && !c.EffectiveEnabled() {
		return fmt.Errorf("blobStore asyncStepInputSnapshotsEnabled requires blobStore.enabled")
	}
	if c.ObjectIDLength < 0 {
		return fmt.Errorf("blobStore objectIdLength must not be negative")
	}
	for _, storage := range c.SupportedStorages {
		if storage.StorageType == StorageTypeS3 && (storage.S3AccessKey == "") != (storage.S3SecretKey == "") {
			return fmt.Errorf("blobStore S3 access key and secret key must both be configured or both omitted")
		}
		if strings.HasPrefix(storage.S3KeyPrefix, "/") {
			return fmt.Errorf("blobStore s3KeyPrefix must not start with /")
		}
	}
	return c.BlobCache.Validate()
}

// EffectiveMaxBytes returns the configured cache budget or its one-GiB default.
func (c BlobCacheConfig) EffectiveMaxBytes() int64 {
	if c.MaxBytes == 0 {
		return DefaultBlobCacheMaxBytes
	}
	return c.MaxBytes
}

// Validate checks enabled blob cache settings.
func (c BlobCacheConfig) Validate() error {
	if c.Directory == "" {
		return nil
	}
	if c.EffectiveMaxBytes() <= 0 {
		return fmt.Errorf("blob cache maxBytes must be positive")
	}
	return nil
}

// EffectiveSchemaSyncInterval returns the configured schema refresh interval or one minute.
func (c AttributeStoreConfig) EffectiveSchemaSyncInterval() time.Duration {
	if c.SchemaSyncInterval == 0 {
		return DefaultAttributeStoreSchemaSyncInterval
	}
	return c.SchemaSyncInterval
}

// EffectiveSyncBatchSize returns the configured batch limit or 100.
func (c AttributeStoreConfig) EffectiveSyncBatchSize() int {
	if c.SyncBatchSize == 0 {
		return DefaultAttributeStoreSyncBatchSize
	}
	return c.SyncBatchSize
}

// EffectiveSyncAttemptTimeout returns the configured regular Activity timeout or thirty seconds.
func (c AttributeStoreConfig) EffectiveSyncAttemptTimeout() time.Duration {
	if c.SyncAttemptTimeout == 0 {
		return DefaultAttributeStoreSyncAttemptTimeout
	}
	return c.SyncAttemptTimeout
}

// EffectiveSyncRetryPolicy returns a finite policy with Attribute Store defaults.
func (c AttributeStoreConfig) EffectiveSyncRetryPolicy() *RetryPolicy {
	policy := RetryPolicyWithDefaults(c.SyncRetryPolicy, DefaultAttributeStoreSyncRetryPolicy)
	return &policy
}

// Validate checks Attribute Store names, destinations, timeouts, and retry bounds.
func (c AttributeStoreConfig) Validate() error {
	if c.EffectiveSchemaSyncInterval() <= 0 {
		return fmt.Errorf("attribute store schemaSyncInterval must be positive")
	}
	if c.EffectiveSyncBatchSize() <= 0 {
		return fmt.Errorf("attribute store syncBatchSize must be positive")
	}
	if c.EffectiveSyncAttemptTimeout() <= 0 {
		return fmt.Errorf("attribute store syncAttemptTimeout must be positive")
	}
	policy := c.EffectiveSyncRetryPolicy()
	if policy.InitialInterval <= 0 || policy.MaximumInterval <= 0 ||
		policy.BackoffCoefficient <= 0 || policy.MaximumAttempts < 0 ||
		policy.TotalDuration <= 0 {
		return fmt.Errorf("attribute store syncRetryPolicy values must be positive except maximumAttempts may be zero")
	}
	for name, store := range c.Stores {
		if name == "" {
			return fmt.Errorf("attribute store name must not be empty")
		}
		if !store.Type.IsSupported() {
			return fmt.Errorf("attribute store %q has unsupported type %q", name, store.Type)
		}
		if store.DSN == "" {
			return fmt.Errorf("attribute store %q requires dsn", name)
		}
		if store.Type == AttributeStoreTypeMongoDB {
			if store.DatabaseName == "" || store.CollectionName == "" {
				return fmt.Errorf("attribute store %q requires databaseName and collectionName", name)
			}
			if store.TableName != "" || store.FlowIDColumn != "" {
				return fmt.Errorf("attribute store %q mongodb configuration does not accept tableName or flowIdColumn", name)
			}
			if store.hasRelationalOptions() {
				return fmt.Errorf("attribute store %q mongodb configuration does not accept rowKey, columnNaming or attributeMaps", name)
			}
			continue
		}
		if store.TableName == "" {
			return fmt.Errorf("attribute store %q requires tableName", name)
		}
		if err := validateAttributeStoreTableName(store.Type, store.TableName); err != nil {
			return fmt.Errorf("attribute store %q has invalid tableName: %w", name, err)
		}
		if store.DatabaseName != "" || store.CollectionName != "" {
			return fmt.Errorf("attribute store %q SQL configuration does not accept databaseName or collectionName", name)
		}
		if store.Type.IsWarehouse() && store.FlowIDColumn == "" {
			return fmt.Errorf("attribute store %q requires flowIdColumn", name)
		}
		if !store.Type.IsWarehouse() && store.FlowIDColumn != "" {
			return fmt.Errorf("attribute store %q only warehouses accept flowIdColumn", name)
		}
		if store.Type.IsWarehouse() && store.hasRelationalOptions() {
			return fmt.Errorf("attribute store %q only mysql and postgres accept rowKey, columnNaming and attributeMaps", name)
		}
		if !store.ColumnNaming.IsSupported() {
			return fmt.Errorf("attribute store %q has unsupported columnNaming %q", name, store.ColumnNaming)
		}
		for mapName, mapTable := range store.AttributeMaps {
			if err := validateAttributeStoreMapTable(store.Type, mapName, mapTable); err != nil {
				return fmt.Errorf("attribute store %q attributeMaps: %w", name, err)
			}
		}
	}
	return nil
}

func (e AttributeStoreConfigEntry) hasRelationalOptions() bool {
	return e.RowKey != "" || e.ColumnNaming != AttributeStoreColumnNamingExact || len(e.AttributeMaps) > 0
}

func validateAttributeStoreMapTable(
	storeType AttributeStoreType,
	mapName string,
	mapTable AttributeStoreMapTableConfig,
) error {
	if mapName == "" || strings.Contains(mapName, "/") {
		return fmt.Errorf("AttributeMap name %q must be non-empty and contain no '/'", mapName)
	}
	if mapTable.TableName == "" || mapTable.InstanceKeyColumn == "" {
		return fmt.Errorf("AttributeMap %q requires tableName and instanceKeyColumn", mapName)
	}
	if err := validateAttributeStoreTableName(storeType, mapTable.TableName); err != nil {
		return fmt.Errorf("AttributeMap %q has invalid tableName: %w", mapName, err)
	}
	return nil
}

func validateAttributeStoreTableName(storeType AttributeStoreType, tableName string) error {
	parts := strings.Split(tableName, ".")
	maximumParts := 2
	if storeType.IsWarehouse() {
		maximumParts = 3
	}
	if len(parts) > maximumParts {
		return fmt.Errorf("must contain at most %d identifiers", maximumParts)
	}
	for _, part := range parts {
		if part == "" || strings.IndexByte(part, 0) >= 0 {
			return fmt.Errorf("contains an invalid identifier")
		}
	}
	return nil
}

// IsSupported reports whether the Attribute Store type has a built-in backend.
func (t AttributeStoreType) IsSupported() bool {
	switch t {
	case AttributeStoreTypeMySQL, AttributeStoreTypePostgres, AttributeStoreTypeDatabricks,
		AttributeStoreTypeSnowflake, AttributeStoreTypeMongoDB:
		return true
	default:
		return false
	}
}

// IsSupported reports whether the column naming rule is exact names or snake_case.
func (n AttributeStoreColumnNaming) IsSupported() bool {
	return n == AttributeStoreColumnNamingExact || n == AttributeStoreColumnNamingSnakeCase
}

// IsWarehouse reports whether the store uses an explicitly configured Flow ID column.
func (t AttributeStoreType) IsWarehouse() bool {
	return t == AttributeStoreTypeDatabricks || t == AttributeStoreTypeSnowflake
}

func (c CleanupStrategy) CronSchedule() (string, error) {
	strategyType := c.CleanupStrategyType
	if strategyType == "" {
		strategyType = CleanupStrategyTypeAfterAllRunsDeleted
	}
	if strategyType != CleanupStrategyTypeAfterAllRunsDeleted {
		return "", fmt.Errorf("unsupported cleanup strategy type: %s", strategyType)
	}
	if c.CleanupFrequencyInDays < 0 {
		return "", fmt.Errorf("cleanup frequency in days must be non-negative")
	}
	if c.CleanupFrequencyInDays == 0 {
		return "", nil
	}
	if c.CleanupFrequencyInDays == 1 {
		return "0 0 * * *", nil
	}
	return fmt.Sprintf("0 0 */%d * *", c.CleanupFrequencyInDays), nil
}

// EffectiveGrpcMaxMessageBytes returns GrpcMaxMessageBytes or DefaultGrpcMaxMessageBytes.
func (c ApiConfig) EffectiveGrpcMaxMessageBytes() int {
	if c.GrpcMaxMessageBytes <= 0 {
		return DefaultGrpcMaxMessageBytes
	}
	return c.GrpcMaxMessageBytes
}

// EffectiveWorkerConnectionIdleTimeout returns the idle eviction timeout for WorkerService conns.
func (c WorkerConfig) EffectiveWorkerConnectionIdleTimeout() time.Duration {
	if c.WorkerConnectionIdleTimeout <= 0 {
		return DefaultWorkerConnectionIdleTimeout
	}
	return c.WorkerConnectionIdleTimeout
}

// EffectiveMaxWorkerConnections returns the WorkerService pool size cap.
func (c WorkerConfig) EffectiveMaxWorkerConnections() int {
	if c.MaxWorkerConnections <= 0 {
		return DefaultMaxWorkerConnections
	}
	return c.MaxWorkerConnections
}

// EffectiveHeadlessAddressRefreshInterval returns the headless DNS refresh interval.
func (c WorkerConfig) EffectiveHeadlessAddressRefreshInterval() time.Duration {
	if c.HeadlessAddressRefreshInterval <= 0 {
		return DefaultHeadlessAddressRefreshInterval
	}
	return c.HeadlessAddressRefreshInterval
}

// EffectiveMaxStickyRoutingEntries returns the sticky routing LRU capacity.
func (c WorkerConfig) EffectiveMaxStickyRoutingEntries() int {
	if c.MaxStickyRoutingEntries <= 0 {
		return DefaultMaxStickyRoutingEntries
	}
	return c.MaxStickyRoutingEntries
}

// EffectiveWorkerServiceRequestMaxAttempts returns total attempts per WorkerService request.
func (c WorkerConfig) EffectiveWorkerServiceRequestMaxAttempts() int {
	if c.WorkerServiceRequestMaxAttempts <= 0 {
		return DefaultWorkerServiceRequestMaxAttempts
	}
	return c.WorkerServiceRequestMaxAttempts
}

// EffectiveHeadlessFailoverStatusCodes returns configured gRPC codes or defaults.
func (c WorkerConfig) EffectiveHeadlessFailoverStatusCodes() []codes.Code {
	if len(c.HeadlessFailoverStatusCodes) == 0 {
		return append([]codes.Code(nil), defaultHeadlessFailoverStatusCodes[:]...)
	}
	statusCodes := make([]codes.Code, len(c.HeadlessFailoverStatusCodes))
	for index, statusCode := range c.HeadlessFailoverStatusCodes {
		statusCodes[index] = codes.Code(statusCode)
	}
	return statusCodes
}

// EffectiveAttributeIndexSyncTimeout returns the configured timeout or 2m.
func (c Interpreter) EffectiveAttributeIndexSyncTimeout() time.Duration {
	if c.AttributeIndexSyncTimeout <= 0 {
		return DefaultAttributeIndexSyncTimeout
	}
	return c.AttributeIndexSyncTimeout
}

// EffectiveMinimumStepHeartbeatTimeout returns the configured minimum or ten seconds.
func (c InterpreterActivityConfig) EffectiveMinimumStepHeartbeatTimeout() time.Duration {
	if c.MinimumStepHeartbeatTimeout == 0 {
		return DefaultMinimumStepHeartbeatTimeout
	}
	return c.MinimumStepHeartbeatTimeout
}

// EffectiveSubFlowStartActivityConfig returns the engine-owned SubFlow start activity settings.
func (c InterpreterActivityConfig) EffectiveSubFlowStartActivityConfig() SubFlowStartActivityConfig {
	effective := SubFlowStartActivityConfig{}
	if c.SubFlowStartActivityConfig != nil {
		effective = *c.SubFlowStartActivityConfig
	}
	if effective.ScheduleToCloseTimeout == 0 {
		effective.ScheduleToCloseTimeout = DefaultSubFlowStartActivityScheduleToCloseTimeout
	}
	retryPolicy := RetryPolicyWithDefaults(effective.RetryPolicy, DefaultInternalActivityRetryPolicy)
	effective.RetryPolicy = &retryPolicy
	return effective
}
