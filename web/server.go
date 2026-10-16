// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package web

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"net/http"
	"path"
	"strings"
	"time"

	"github.com/superdurable/dex/gen/dexpb"
	"github.com/superdurable/dex/web/api"
)

const DefaultPort = 8802

type Config struct {
	// BindAddress defaults to 127.0.0.1 and controls the HTTP bind IP.
	BindAddress string
	// Port defaults to 8802 and controls the HTTP bind port.
	Port int
	// FlowRenderingDirectory defaults empty and supplies Flow Definition Graph JSON files to Dex Web.
	FlowRenderingDirectory string
	// StartFlowWorkerTargetHeadless defaults false and applies headless routing to Dex Web address targets.
	IsStartFlowWorkerTargetHeadless bool
	// IsStartFlowWorkerTargetPull defaults false and selects pull routing when no target is supplied. Headless routing applies to address targets.
	IsStartFlowWorkerTargetPull bool
	// ConnectorSetupEnabled defaults false and enables Connector configuration APIs.
	ConnectorSetupEnabled bool
	// ConnectorConfigDirectory defaults empty and stores local Connector configuration and verified UI artifacts.
	ConnectorConfigDirectory string
	// ConnectorReleaseOverrides maps Connector IDs to local release artifact directories for loopback development.
	ConnectorReleaseOverrides map[string]string
}

type Server struct {
	cfg        *Config
	httpServer *http.Server
}

func NewServer(cfg *Config, client dexpb.FlowServiceClient, assets fs.FS) (*Server, error) {
	if cfg == nil {
		panic("Web config must not be nil")
	}
	if client == nil {
		panic("Dex FlowService client must not be nil")
	}
	if assets == nil {
		panic("Web assets must not be nil")
	}
	flowDefinitions, err := NewDirectoryFlowDefinitionProvider(cfg.FlowRenderingDirectory)
	if err != nil {
		return nil, err
	}
	return newServer(cfg, client, assets, flowDefinitions)
}

// NewFlowRenderingServer serves one in-memory Flow Definition Graph without a Dex FlowService connection.
func NewFlowRenderingServer(cfg *Config, graph []byte, assets fs.FS) (*Server, error) {
	if cfg == nil {
		panic("Web config must not be nil")
	}
	if assets == nil {
		panic("Web assets must not be nil")
	}
	snapshot, err := snapshotFromGraph(graph)
	if err != nil {
		return nil, err
	}
	return newServer(cfg, nil, assets, staticFlowDefinitionProvider{snapshot: snapshot})
}

func newServer(cfg *Config, client dexpb.FlowServiceClient, assets fs.FS, flowDefinitions FlowDefinitionProvider) (*Server, error) {
	connectorSetup, err := newConnectorSetup(cfg, flowDefinitions)
	if err != nil {
		return nil, err
	}
	assetRoot, err := fs.Sub(assets, "dist")
	if err != nil {
		panic(fmt.Sprintf("open embedded Web assets: %v", err))
	}
	mux := http.NewServeMux()
	if client != nil {
		api.RegisterHandlers(mux, client)
		api.RegisterDynamicV2Handlers(mux, client, api.V2DefinitionLoader(func(ctx context.Context) (api.V2DefinitionSnapshot, error) {
			snapshot, loadErr := flowDefinitions.Load(ctx)
			if loadErr != nil {
				return api.V2DefinitionSnapshot{}, loadErr
			}
			return api.V2DefinitionSnapshot{
				Definitions: snapshot.V2Definitions,
				Revision:    snapshot.DefinitionRevision,
			}, nil
		}), api.V2HandlerConfig{
			IsStartFlowWorkerTargetHeadless: cfg.IsStartFlowWorkerTargetHeadless,
			IsStartFlowWorkerTargetPull:     cfg.IsStartFlowWorkerTargetPull,
		})
	}
	mux.HandleFunc("GET /api/flow-definitions", serveFlowDefinitions(flowDefinitions))
	if connectorSetup != nil {
		connectorSetup.registerHandlers(mux)
	}
	mux.HandleFunc("GET /readyz", readinessHandler(client, flowDefinitions))
	mux.Handle("/", spaHandler(assetRoot))
	return &Server{
		cfg: cfg,
		httpServer: &http.Server{
			Handler:           mux,
			ReadHeaderTimeout: 10 * time.Second,
			IdleTimeout:       90 * time.Second,
		},
	}, nil
}

func (s *Server) Run() error {
	address := s.cfg.BindAddress
	if address == "" {
		address = "127.0.0.1"
	}
	port := s.cfg.Port
	if port == 0 {
		port = DefaultPort
	}
	listener, err := net.Listen("tcp", net.JoinHostPort(address, fmt.Sprintf("%d", port)))
	if err != nil {
		return err
	}
	return s.Serve(listener)
}

func (s *Server) Serve(listener net.Listener) error {
	return s.httpServer.Serve(listener)
}

func (s *Server) Shutdown(ctx context.Context) error {
	return s.httpServer.Shutdown(ctx)
}

func (s *Server) Handler() http.Handler {
	return s.httpServer.Handler
}

func spaHandler(assets fs.FS) http.Handler {
	files := http.FileServer(http.FS(assets))
	index, err := fs.ReadFile(assets, "index.html")
	if err != nil {
		panic(fmt.Sprintf("read embedded Web index: %v", err))
	}
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet && request.Method != http.MethodHead {
			response.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		requestPath := strings.TrimPrefix(path.Clean(request.URL.Path), "/")
		if requestPath == "." || requestPath == "" {
			serveIndex(response, request, index)
			return
		}
		file, err := assets.Open(requestPath)
		if err == nil {
			if closeErr := file.Close(); closeErr != nil {
				http.Error(response, closeErr.Error(), http.StatusInternalServerError)
				return
			}
			files.ServeHTTP(response, request)
			return
		}
		if strings.HasPrefix(request.URL.Path, "/api/") {
			api.WriteError(response, http.StatusNotFound, "API route not found", nil)
			return
		}
		serveIndex(response, request, index)
	})
}

type staticFlowDefinitionProvider struct {
	snapshot *FlowDefinitionSnapshot
}

func (p staticFlowDefinitionProvider) Load(context.Context) (*FlowDefinitionSnapshot, error) {
	return p.snapshot, nil
}

func serveFlowDefinitions(provider FlowDefinitionProvider) http.HandlerFunc {
	return func(response http.ResponseWriter, request *http.Request) {
		snapshot, err := provider.Load(request.Context())
		if err != nil {
			writeFlowDefinitionSourceError(response, err)
			return
		}
		setSnapshotETag(response, snapshot.DefinitionRevision)
		response.Header().Set("Content-Type", "application/json; charset=utf-8")
		response.WriteHeader(http.StatusOK)
		_, _ = response.Write(snapshot.Response)
	}
}

func readinessHandler(client dexpb.FlowServiceClient, provider FlowDefinitionProvider) http.HandlerFunc {
	return func(response http.ResponseWriter, request *http.Request) {
		snapshot, err := provider.Load(request.Context())
		if err != nil {
			writeFlowDefinitionSourceError(response, err)
			return
		}
		if client != nil {
			if _, err := client.SearchFlows(request.Context(), &dexpb.SearchFlowsRequest{PageSize: 1}); err != nil {
				api.WriteCodedError(response, http.StatusServiceUnavailable, "FLOW_SERVICE_UNAVAILABLE", "Dex FlowService is unavailable")
				return
			}
		}
		setSnapshotETag(response, snapshot.DefinitionRevision)
		writeWebJSON(response, http.StatusOK, map[string]interface{}{
			"status":             "ready",
			"definitionRevision": snapshot.DefinitionRevision,
			"source":             snapshot.Source,
			"definitionCount":    snapshot.DefinitionCount,
		})
	}
}

func writeFlowDefinitionSourceError(response http.ResponseWriter, err error) {
	code := flowDefinitionSourceUnavailable
	message := "Flow Definition source is unavailable"
	var coded interface{ DefinitionErrorCode() string }
	if errors.As(err, &coded) {
		code = coded.DefinitionErrorCode()
		if code == flowDefinitionSourceInvalid {
			message = "Flow Definition source is invalid"
		}
	}
	api.WriteCodedError(response, http.StatusServiceUnavailable, code, message)
}

func setSnapshotETag(response http.ResponseWriter, revision string) {
	if revision != "" {
		response.Header().Set("ETag", fmt.Sprintf("%q", revision))
	}
}

func writeWebJSON(response http.ResponseWriter, statusCode int, value interface{}) {
	response.Header().Set("Content-Type", "application/json; charset=utf-8")
	response.WriteHeader(statusCode)
	if err := json.NewEncoder(response).Encode(value); err != nil {
		panic(fmt.Sprintf("encode HTTP response: %v", err))
	}
}

func serveIndex(response http.ResponseWriter, request *http.Request, index []byte) {
	response.Header().Set("Cache-Control", "no-store")
	response.Header().Set("Content-Security-Policy", "frame-ancestors 'none'")
	response.Header().Set("X-Frame-Options", "DENY")
	http.ServeContent(response, request, "index.html", time.Time{}, bytes.NewReader(index))
}
