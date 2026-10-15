// Legacy Materials in this file remain under their original licenses.
// See LEGACY_NOTICES.md.

// Modifications Copyright (c) 2026 Super Durable, Inc.
//
// Modifications after the Legacy Cutoff are licensed under the
// Sustainable Use License 1.0.
// Legacy Materials remain under their original licenses.
// See LICENSE and LEGACY_NOTICES.md.

package bootstrap

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/superdurable/dex/config"
	"github.com/superdurable/dex/service"
	uclient "github.com/superdurable/dex/service/client"
	temporalapi "github.com/superdurable/dex/service/client/temporal"
	"github.com/superdurable/dex/service/common/blobstore"
	dexconverter "github.com/superdurable/dex/service/common/converter"
	"github.com/superdurable/dex/service/interpreter/temporal"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func (r *Runtime) createTemporalServices() (
	uclient.UnifiedClient,
	blobstore.BlobStore,
	interpreterWorker,
	error,
) {
	temporalConfig := r.cfg.Interpreter.Temporal
	if err := temporalConfig.ValidateTemporalConnection(); err != nil {
		return nil, nil, nil, err
	}
	clientOptions := client.Options{
		HostPort:  temporalConfig.HostPort,
		Namespace: temporalConfig.Namespace,
		Logger:    newTemporalLogger(r.zapLogger),
	}
	metrics := client.MetricsNopHandler
	if temporalConfig.Prometheus != nil {
		var err error
		metrics, r.metricsCloser, err = newPrometheusScope(temporalConfig.Prometheus, r.logger)
		if err != nil {
			return nil, nil, nil, err
		}
		clientOptions.MetricsHandler = metrics
	}
	var apiKeyReader *temporalAPIKeyReader
	if temporalConfig.APIKeyFile != "" {
		var err error
		apiKeyReader, err = newTemporalAPIKeyReader(temporalConfig, r.zapLogger)
		if err != nil {
			return nil, nil, nil, err
		}
		clientOptions.Credentials = client.NewAPIKeyDynamicCredentials(apiKeyReader.GetAPIKey)
	} else if temporalConfig.CloudAPIKey != "" {
		clientOptions.Credentials = client.NewAPIKeyStaticCredentials(temporalConfig.CloudAPIKey)
	}
	hasAPIKey := temporalConfig.CloudAPIKey != "" || apiKeyReader != nil
	isTLSEnabled := temporalConfig.IsTLSEnabled()
	if isTLSEnabled && temporalConfig.TLS != nil {
		var err error
		clientOptions.ConnectionOptions.TLS, err = createTemporalTLSConfig(temporalConfig.TLS, r.zapLogger)
		if err != nil {
			return nil, nil, nil, err
		}
	} else if isTLSEnabled {
		// NOTE: this connectionOptions can be removed when upgrading temporal SDK to latest
		// see https://docs.temporal.io/cloud/api-keys#sdk
		clientOptions.ConnectionOptions.TLS = &tls.Config{}
	} else if hasAPIKey {
		clientOptions.ConnectionOptions.TLSDisabled = true
		r.zapLogger.Warn("Temporal API key will be sent over plaintext")
	}
	if hasAPIKey {
		clientOptions.ConnectionOptions.DialOptions = []grpc.DialOption{
			grpc.WithUnaryInterceptor(
				func(
					ctx context.Context,
					method string,
					request any,
					reply any,
					connection *grpc.ClientConn,
					invoker grpc.UnaryInvoker,
					options ...grpc.CallOption,
				) error {
					return invoker(
						metadata.AppendToOutgoingContext(ctx, "temporal-namespace", temporalConfig.Namespace),
						method,
						request,
						reply,
						connection,
						options...,
					)
				},
			),
		}
	}

	dataConverter := dexconverter.NewTemporalDataConverter()
	clientOptions.DataConverter = dataConverter
	temporalClient, err := client.Dial(clientOptions)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("connect to Temporal: %w", err)
	}
	var attributeIndexClient uclient.AttributeIndexClient
	if temporalConfig.CloudOps != nil {
		if apiKeyReader == nil {
			attributeIndexClient, err = temporalapi.NewCloudAttributeIndexClient(temporalConfig)
		} else {
			attributeIndexClient, err = temporalapi.NewCloudAttributeIndexClientWithAPIKeyReader(temporalConfig, apiKeyReader)
		}
		if err != nil {
			temporalClient.Close()
			return nil, nil, nil, err
		}
	}
	unifiedClient := temporalapi.NewTemporalClient(
		temporalClient,
		temporalConfig.Namespace,
		dataConverter,
		false,
		r.cfg.Api.QueryWorkflowFailedRetryPolicy,
		attributeIndexClient,
	)
	store, err := r.createBlobStore(
		context.Background(),
		temporalConfig.Namespace,
		metrics,
	)
	if err != nil {
		unifiedClient.Close()
		return nil, nil, nil, err
	}
	if !r.options.Services.Interpreter {
		return unifiedClient, store, nil, nil
	}
	worker := temporal.NewInterpreterWorker(
		r.cfg,
		temporalClient,
		service.TaskQueue,
		converter.DataConverter(dataConverter),
		unifiedClient,
		store,
		r.attributeStore,
		r.workerPool,
		r.streamStore,
		metrics,
	)
	return unifiedClient, store, worker, nil
}

func createTemporalTLSConfig(cfg *config.TemporalTLSConfig, logger *zap.Logger) (*tls.Config, error) {
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: cfg.ServerName}
	if cfg.CAFile != "" {
		caPEM, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("read Temporal CA file: %w", err)
		}
		rootCAs := x509.NewCertPool()
		if !rootCAs.AppendCertsFromPEM(caPEM) {
			return nil, fmt.Errorf("Temporal CA file contains no certificates")
		}
		tlsConfig.RootCAs = rootCAs
	}
	if cfg.CertFile != "" {
		reader, err := newTemporalClientCertificateReader(cfg, logger)
		if err != nil {
			return nil, err
		}
		tlsConfig.GetClientCertificate = reader.GetClientCertificate
	}
	return tlsConfig, nil
}

type temporalAPIKeyReader struct {
	cfg          *config.TemporalConfig
	logger       *zap.Logger
	mu           sync.Mutex
	key          string
	fileInfo     os.FileInfo
	nextCheck    time.Time
	isRefreshing bool
}

func newTemporalAPIKeyReader(cfg *config.TemporalConfig, logger *zap.Logger) (*temporalAPIKeyReader, error) {
	if cfg == nil {
		panic("Temporal config must not be nil")
	}
	fileInfo, err := os.Stat(cfg.APIKeyFile)
	if err != nil {
		return nil, fmt.Errorf("stat Temporal API key file: %w", err)
	}
	content, err := os.ReadFile(cfg.APIKeyFile)
	if err != nil {
		return nil, fmt.Errorf("read Temporal API key file: %w", err)
	}
	key := strings.TrimSpace(string(content))
	if key == "" {
		return nil, fmt.Errorf("Temporal API key file is empty")
	}
	return &temporalAPIKeyReader{cfg: cfg, logger: logger, key: key, fileInfo: fileInfo, nextCheck: time.Now().Add(effectiveAPIKeyFileRefreshInterval(cfg))}, nil
}

func (r *temporalAPIKeyReader) GetAPIKey(context.Context) (string, error) {
	r.mu.Lock()
	key := r.key
	if !r.isRefreshing && !time.Now().Before(r.nextCheck) {
		r.nextCheck = time.Now().Add(effectiveAPIKeyFileRefreshInterval(r.cfg))
		r.isRefreshing = true
		go r.refresh()
	}
	r.mu.Unlock()
	return key, nil
}

func effectiveAPIKeyFileRefreshInterval(cfg *config.TemporalConfig) time.Duration {
	if cfg.APIKeyFileRefreshInterval == 0 {
		return 30 * time.Second
	}
	return cfg.APIKeyFileRefreshInterval
}

func (r *temporalAPIKeyReader) refresh() {
	defer func() {
		r.mu.Lock()
		r.isRefreshing = false
		r.mu.Unlock()
	}()
	fileInfo, err := os.Stat(r.cfg.APIKeyFile)
	if err != nil {
		r.logger.Warn("check Temporal API key file", zap.Error(err))
		return
	}
	r.mu.Lock()
	isUnchanged := os.SameFile(fileInfo, r.fileInfo) && fileInfo.ModTime().Equal(r.fileInfo.ModTime()) && fileInfo.Size() == r.fileInfo.Size()
	r.mu.Unlock()
	if isUnchanged {
		return
	}
	content, err := os.ReadFile(r.cfg.APIKeyFile)
	if err != nil {
		r.logger.Warn("read Temporal API key file", zap.Error(err))
		return
	}
	key := strings.TrimSpace(string(content))
	if key == "" {
		r.logger.Warn("Temporal API key file is empty")
		return
	}
	r.mu.Lock()
	r.key = key
	r.fileInfo = fileInfo
	r.mu.Unlock()
}

type temporalClientCertificateReader struct {
	cfg         *config.TemporalTLSConfig
	logger      *zap.Logger
	mu          sync.Mutex
	certificate tls.Certificate
	certInfo    os.FileInfo
	keyInfo     os.FileInfo
}

func newTemporalClientCertificateReader(cfg *config.TemporalTLSConfig, logger *zap.Logger) (*temporalClientCertificateReader, error) {
	if cfg == nil {
		panic("Temporal TLS config must not be nil")
	}
	reader := &temporalClientCertificateReader{cfg: cfg, logger: logger}
	if err := reader.reload(); err != nil {
		return nil, err
	}
	return reader, nil
}

func (r *temporalClientCertificateReader) GetClientCertificate(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
	if err := r.reload(); err != nil {
		r.logger.Warn("reload Temporal client certificate", zap.Error(err))
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	certificate := r.certificate
	return &certificate, nil
}

func (r *temporalClientCertificateReader) reload() error {
	certInfo, err := os.Stat(r.cfg.CertFile)
	if err != nil {
		return fmt.Errorf("stat Temporal client certificate: %w", err)
	}
	keyInfo, err := os.Stat(r.cfg.KeyFile)
	if err != nil {
		return fmt.Errorf("stat Temporal client key: %w", err)
	}
	r.mu.Lock()
	isUnchanged := r.certInfo != nil && os.SameFile(certInfo, r.certInfo) && certInfo.ModTime().Equal(r.certInfo.ModTime()) && certInfo.Size() == r.certInfo.Size() && os.SameFile(keyInfo, r.keyInfo) && keyInfo.ModTime().Equal(r.keyInfo.ModTime()) && keyInfo.Size() == r.keyInfo.Size()
	r.mu.Unlock()
	if isUnchanged {
		return nil
	}
	certificate, err := tls.LoadX509KeyPair(r.cfg.CertFile, r.cfg.KeyFile)
	if err != nil {
		return fmt.Errorf("load Temporal client certificate: %w", err)
	}
	r.mu.Lock()
	r.certificate = certificate
	r.certInfo = certInfo
	r.keyInfo = keyInfo
	r.mu.Unlock()
	return nil
}
