// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package web

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/superdurable/dex/web/api"
)

const connectorCSRFHeader = "X-Dex-CSRF-Token"

// connectorDisplayNameTimeout bounds how long a connection list waits for uncached release metadata.
const connectorDisplayNameTimeout = 5 * time.Second

type connectorSetup struct {
	store              *connectorConnectionStore
	flowDefinitions    FlowDefinitionProvider
	releases           *connectorReleaseResolver
	csrfToken          string
	uiSessions         map[string]connectorUISession
	uiSessionsMu       sync.Mutex
	oauthSessions      map[string]connectorOAuthSession
	oauthSessionsMu    sync.Mutex
	providerHTTPClient *http.Client
	// endpointPolicy and settingHTTPClient serve the endpoints that connection settings supply.
	endpointPolicy    connectorAddressPolicy
	settingHTTPClient *http.Client
}

type connectorUISession struct {
	root           string
	entrypoint     string
	connectorID    string
	connectionName string
	commands       map[string]connectorManifestStudioCommand
	// canWriteConnection marks a setup that collects typed credentials, so it gets draft commands and a stricter CSP.
	canWriteConnection bool
	expiresAt          time.Time
}

type connectorCatalogDocument struct {
	DefinitionRevision string                       `json:"definitionRevision"`
	Definitions        []connectorCatalogDefinition `json:"definitions"`
}

type connectorCatalogDefinition struct {
	FlowName string `json:"flowName"`
	Graph    struct {
		Nodes []connectorCatalogNode `json:"nodes"`
		V2    *struct {
			ConnectorTriggerBindings []connectorCatalogTriggerBinding `json:"connectorTriggerBindings"`
		} `json:"v2,omitempty"`
	} `json:"graph"`
}

type connectorCatalogTriggerBinding struct {
	ConnectorID          string                         `json:"connectorId"`
	TriggerName          string                         `json:"triggerName"`
	ConnectionName       string                         `json:"connectionName"`
	BindingName          string                         `json:"bindingName"`
	ModulePath           string                         `json:"modulePath"`
	ModuleVersion        string                         `json:"moduleVersion"`
	ConfigurationEnabled bool                           `json:"configurationEnabled"`
	ConfigurationUI      api.V2ConnectorConfigurationUI `json:"configurationUI"`
}

type connectorCatalogNode struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Kind     string `json:"kind"`
	Metadata struct {
		ConnectorFactory bool                         `json:"connectorFactory"`
		Connector        *connectorDefinitionIdentity `json:"connector"`
	} `json:"metadata"`
}

type connectorAuthorizationSnapshot struct {
	DefinitionRevision string
	Response           []byte
}

type connectorDefinitionIdentity struct {
	ConnectorID          string                         `json:"connectorId"`
	OperationID          string                         `json:"operationId"`
	OperationKind        string                         `json:"operationKind"`
	ConnectionName       string                         `json:"connectionName"`
	ModulePath           string                         `json:"modulePath"`
	ModuleVersion        string                         `json:"moduleVersion"`
	ConfigurationEnabled bool                           `json:"configurationEnabled"`
	ConfigurationUI      api.V2ConnectorConfigurationUI `json:"configurationUI"`
}

type connectorConnectionView struct {
	ConnectorID            string                          `json:"connectorId"`
	DisplayName            string                          `json:"displayName,omitempty"`
	AuthMethodID           string                          `json:"authMethodId,omitempty"`
	AuthMethodIDs          []string                        `json:"authMethodIds,omitempty"`
	ConnectionName         string                          `json:"connectionName"`
	ModulePath             string                          `json:"modulePath,omitempty"`
	ModuleVersion          string                          `json:"moduleVersion,omitempty"`
	LocalOverride          bool                            `json:"localOverride,omitempty"`
	Provider               string                          `json:"provider,omitempty"`
	Status                 string                          `json:"status"`
	Configuration          map[string]json.RawMessage      `json:"configuration,omitempty"`
	StoredCredentialFields []string                        `json:"storedCredentialFields,omitempty"`
	CredentialExpiresAt    *time.Time                      `json:"credentialExpiresAt,omitempty"`
	Uses                   []connectorConnectionStepUse    `json:"uses"`
	TriggerUses            []connectorConnectionTriggerUse `json:"triggerUses,omitempty"`
}

type connectorConnectionStepUse struct {
	FlowName        string                         `json:"flowName"`
	StepID          string                         `json:"stepId"`
	StepName        string                         `json:"stepName"`
	OperationID     string                         `json:"operationId"`
	OperationKind   string                         `json:"operationKind"`
	ConfigurationUI api.V2ConnectorConfigurationUI `json:"configurationUI"`
	Configuration   map[string]json.RawMessage     `json:"configuration"`
	Configured      bool                           `json:"configured"`
}

type connectorConnectionTriggerUse struct {
	ConfigurationFields []connectorManifestField       `json:"configurationFields,omitempty"`
	SchemaAvailable     bool                           `json:"schemaAvailable,omitempty"`
	FlowName            string                         `json:"flowName"`
	TriggerName         string                         `json:"triggerName"`
	BindingName         string                         `json:"bindingName"`
	ConfigurationUI     api.V2ConnectorConfigurationUI `json:"configurationUI"`
	Configuration       map[string]json.RawMessage     `json:"configuration"`
	Configured          bool                           `json:"configured"`
}

type connectorConnectionListResponse struct {
	Enabled                   bool                      `json:"enabled"`
	Directory                 string                    `json:"directory,omitempty"`
	FilePath                  string                    `json:"filePath,omitempty"`
	UseConfigurationsFilePath string                    `json:"useConfigurationsFilePath,omitempty"`
	DefinitionRevision        string                    `json:"definitionRevision"`
	CSRFToken                 string                    `json:"csrfToken"`
	LaunchCommand             string                    `json:"launchCommand,omitempty"`
	Connections               []connectorConnectionView `json:"connections"`
}

type connectorConnectionWriteRequest struct {
	AuthMethodID         string                     `json:"authMethodId"`
	AuthMethodIDs        []string                   `json:"authMethodIds"`
	ModulePath           string                     `json:"modulePath"`
	ModuleVersion        string                     `json:"moduleVersion"`
	Provider             string                     `json:"provider"`
	Configuration        map[string]json.RawMessage `json:"configuration"`
	Credentials          map[string]json.RawMessage `json:"credentials"`
	KeepCredentialFields []string                   `json:"keepCredentialFields"`
	CredentialExpiresAt  *time.Time                 `json:"credentialExpiresAt"`
}

type connectorUISessionRequest struct {
	ConnectorID    string `json:"connectorId"`
	ConnectionName string `json:"connectionName"`
}

type connectorUISessionResponse struct {
	ConnectorID      string                                           `json:"connectorId"`
	ConnectionName   string                                           `json:"connectionName"`
	SessionNonce     string                                           `json:"sessionNonce,omitempty"`
	EntrypointURL    string                                           `json:"entrypointUrl,omitempty"`
	OAuthRedirectURI string                                           `json:"oauthRedirectUri,omitempty"`
	Manifest         connectorReleaseManifest                         `json:"manifest"`
	TriggerBindings  map[string]map[string]map[string]json.RawMessage `json:"triggerBindings,omitempty"`
}

type connectorTriggerBindingWriteRequest struct {
	Configuration map[string]json.RawMessage `json:"configuration"`
}

type connectorUseConfigurationWriteRequest struct {
	Configuration map[string]json.RawMessage `json:"configuration"`
}

func newConnectorSetup(cfg *Config, flowDefinitions FlowDefinitionProvider) (*connectorSetup, error) {
	if !cfg.ConnectorSetupEnabled {
		return nil, nil
	}
	bindIP := net.ParseIP(cfg.BindAddress)
	if bindIP == nil || !bindIP.IsLoopback() {
		return nil, fmt.Errorf("local Connector setup requires a loopback Dex Web bind address")
	}
	store, err := newConnectorConnectionStore(cfg.ConnectorConfigDirectory)
	if err != nil {
		return nil, err
	}
	csrfToken, err := randomConnectorToken(32)
	if err != nil {
		return nil, fmt.Errorf("create Connector CSRF token: %w", err)
	}
	releases, err := newConnectorReleaseResolver(store.directory, cfg.ConnectorReleaseOverrides)
	if err != nil {
		return nil, err
	}
	endpointPolicy := defaultConnectorAddressPolicy()
	return &connectorSetup{
		store: store, flowDefinitions: flowDefinitions, releases: releases, csrfToken: csrfToken,
		uiSessions: make(map[string]connectorUISession), oauthSessions: make(map[string]connectorOAuthSession),
		providerHTTPClient: newConnectorProviderHTTPClient(),
		endpointPolicy:     endpointPolicy,
		settingHTTPClient:  newConnectorAddressPolicyClient(nil, endpointPolicy),
	}, nil
}

func (setup *connectorSetup) registerHandlers(mux *http.ServeMux) {
	mux.HandleFunc("GET /api/v2/connector-connections", setup.handleListConnections)
	mux.HandleFunc("PUT /api/v2/connector-connections/{connectorId}/{connectionName}", setup.handlePutConnection)
	mux.HandleFunc("DELETE /api/v2/connector-connections/{connectorId}/{connectionName}", setup.handleDeleteConnection)
	mux.HandleFunc("POST /api/v2/connector-ui-sessions", setup.handleCreateUISession)
	mux.HandleFunc("GET /api/v2/connector-ui-sessions/{sessionNonce}/{assetPath...}", setup.handleUIAsset)
	mux.HandleFunc("PUT /api/v2/connector-trigger-bindings/{connectorId}/{connectionName}/{triggerName}/{bindingName}", setup.handlePutTriggerBinding)
	mux.HandleFunc("PUT /api/v2/connector-use-configurations/{connectorId}/{connectionName}/{operationId}/{flowType}/{stepType}", setup.handlePutUseConfiguration)
	mux.HandleFunc("POST /api/v2/connector-ui-sessions/{sessionNonce}/commands/{commandId}", setup.handleStudioProviderCommand)
	mux.HandleFunc("POST /api/v2/connector-connections/{connectorId}/{connectionName}/oauth/start", setup.handleOAuthStart)
	mux.HandleFunc("GET /api/v2/connector-oauth/callback", setup.handleOAuthCallback)
}

func (setup *connectorSetup) handleListConnections(response http.ResponseWriter, request *http.Request) {
	views, revision, err := setup.connectionViews(request.Context())
	if err != nil {
		api.WriteCodedError(response, http.StatusServiceUnavailable, "CONNECTOR_CONNECTIONS_UNAVAILABLE", "Connector connections are unavailable")
		return
	}
	writeWebJSON(response, http.StatusOK, connectorConnectionListResponse{
		Enabled: true, Directory: setup.store.directory, FilePath: setup.store.path,
		UseConfigurationsFilePath: setup.store.useConfigurationsPath,
		DefinitionRevision:        revision, CSRFToken: setup.csrfToken, Connections: views,
		LaunchCommand: "DEX_CONNECTOR_CONFIG_FILE=" + shellQuote(setup.store.path) + " <your-app-command>",
	})
}

func (setup *connectorSetup) handlePutConnection(response http.ResponseWriter, request *http.Request) {
	store := setup.store
	snapshot, requested, ok := setup.authorizeConnectionWrite(response, request)
	if !ok {
		return
	}
	request.Body = http.MaxBytesReader(response, request.Body, 1<<20)
	var body connectorConnectionWriteRequest
	if err := decodeStrictConnectorJSONReader(request.Body, &body); err != nil || body.Credentials == nil {
		api.WriteCodedError(response, http.StatusBadRequest, "CONNECTOR_REQUEST_INVALID", "Connector connection request is invalid")
		return
	}
	if hasConnectorAuthMethodCredential(body.Credentials) {
		api.WriteCodedError(response, http.StatusBadRequest, "CONNECTOR_REQUEST_INVALID", "Connector credentials must not set the authentication method")
		return
	}
	if body.ModulePath != requested.ModulePath || body.ModuleVersion != requested.ModuleVersion {
		api.WriteCodedError(response, http.StatusConflict, "CONNECTOR_VERSION_CONFLICT", "Connector module does not match the current Flow Definition")
		return
	}
	resolved, err := setup.releases.resolve(request.Context(), requested)
	if err != nil {
		api.WriteCodedError(response, http.StatusBadGateway, "CONNECTOR_RELEASE_UNAVAILABLE", "Connector release metadata is unavailable")
		return
	}
	manifest := resolved.release.Manifest
	if body.Provider != "" && body.Provider != manifest.Spec.Provider {
		api.WriteCodedError(response, http.StatusBadRequest, "CONNECTOR_REQUEST_INVALID", "Connector provider does not match the official release")
		return
	}
	selectedMethods, err := manifest.Spec.Auth.selectedMethods(body.AuthMethodID, body.AuthMethodIDs)
	if err != nil {
		api.WriteCodedError(response, http.StatusBadRequest, "CONNECTOR_AUTH_METHOD_INVALID", "Connector authentication method is invalid")
		return
	}
	configurationFields := connectorConfigurationFieldsForMethods(manifest.Spec.Configuration.Fields, selectedMethods)
	if err := validateRawConnectorFields(configurationFields, body.Configuration, false, nil); err != nil {
		api.WriteCodedError(response, http.StatusBadRequest, "CONNECTOR_REQUEST_INVALID", "Connector configuration is invalid")
		return
	}
	credentialFields := connectorCredentialFieldsForMethods(selectedMethods)
	var storedCredentialFields []string
	if len(body.KeepCredentialFields) > 0 {
		stored, found, loadErr := store.get(requested.ConnectorID, requested.ConnectionName)
		if loadErr != nil {
			writeConnectorConfigurationStoreError(response, loadErr, "CONNECTOR_CONNECTION_WRITE_FAILED", "Connector connection could not be saved")
			return
		}
		if found {
			storedCredentialFields = stored.StoredCredentialFields
		}
	}
	keptCredentialFields, err := validateConnectorKeepCredentialFields(
		body.KeepCredentialFields, storedCredentialFields, credentialFields, body.Credentials,
	)
	if err != nil {
		api.WriteCodedError(response, http.StatusBadRequest, "CONNECTOR_REQUEST_INVALID", "Connector credential fields to keep are invalid")
		return
	}
	if err := validateRawConnectorFields(credentialFields, body.Credentials, true, keptCredentialFields); err != nil {
		api.WriteCodedError(response, http.StatusBadRequest, "CONNECTOR_REQUEST_INVALID", "Connector credentials are invalid")
		return
	}
	connection := localConnectorConnection{
		ConnectorID: request.PathValue("connectorId"), ModulePath: body.ModulePath,
		ModuleVersion: body.ModuleVersion, Provider: manifest.Spec.Provider,
		ConnectionName: request.PathValue("connectionName"), Configuration: body.Configuration,
		Credentials: body.Credentials, CredentialExpiresAt: body.CredentialExpiresAt,
	}
	if err := stampConnectorAuthMethods(&connection, manifest.Spec.Auth, selectedMethods); err != nil {
		api.WriteCodedError(response, http.StatusInternalServerError, "CONNECTOR_CONNECTION_WRITE_FAILED", "Connector connection could not be saved")
		return
	}
	if err := validateLocalConnectorConnection(connection); err != nil {
		api.WriteCodedError(response, http.StatusBadRequest, "CONNECTOR_REQUEST_INVALID", "Connector connection request is invalid")
		return
	}
	if err := store.put(connection, body.KeepCredentialFields); err != nil {
		writeConnectorConfigurationStoreError(response, err, "CONNECTOR_CONNECTION_WRITE_FAILED", "Connector connection could not be saved")
		return
	}
	result := map[string]any{
		"connectorId": connection.ConnectorID, "connectionName": connection.ConnectionName,
		"status": "Ready", "definitionRevision": snapshot.DefinitionRevision,
		"credentialExpiresAt": connection.CredentialExpiresAt,
	}
	result["filePath"] = store.path
	writeWebJSON(response, http.StatusOK, result)
}

func (setup *connectorSetup) handleDeleteConnection(response http.ResponseWriter, request *http.Request) {
	store := setup.store
	_, _, ok := setup.authorizeConnectionWrite(response, request)
	if !ok {
		return
	}
	deleted, err := store.delete(request.PathValue("connectorId"), request.PathValue("connectionName"))
	if err != nil {
		writeConnectorConfigurationStoreError(response, err, "CONNECTOR_CONNECTION_DELETE_FAILED", "Connector connection could not be deleted")
		return
	}
	writeWebJSON(response, http.StatusOK, map[string]any{"deleted": deleted, "remoteGrantRevoked": false})
}

func writeConnectorConfigurationStoreError(response http.ResponseWriter, err error, code string, message string) {
	if errors.Is(err, errConnectorConfigurationRevisionConflict) {
		api.WriteCodedError(response, http.StatusConflict, "CONNECTOR_CONFIGURATION_REVISION_CONFLICT", "Connector configuration changed; reload before saving")
		return
	}
	api.WriteCodedError(response, http.StatusInternalServerError, code, message)
}

func (setup *connectorSetup) handleCreateUISession(response http.ResponseWriter, request *http.Request) {
	store := setup.store
	request.Body = http.MaxBytesReader(response, request.Body, 1<<16)
	var body connectorUISessionRequest
	if err := decodeStrictConnectorJSONReader(request.Body, &body); err != nil {
		api.WriteCodedError(response, http.StatusBadRequest, "CONNECTOR_REQUEST_INVALID", "Connector UI session request is invalid")
		return
	}
	_, identity, ok := setup.authorizeConnectionKey(response, request, body.ConnectorID, body.ConnectionName)
	if !ok {
		return
	}
	resolved, err := setup.releases.resolve(request.Context(), identity)
	if err != nil {
		api.WriteCodedError(response, http.StatusBadGateway, "CONNECTOR_RELEASE_UNAVAILABLE", "Connector release metadata or UI is unavailable")
		return
	}
	result := connectorUISessionResponse{
		ConnectorID: body.ConnectorID, ConnectionName: body.ConnectionName, Manifest: resolved.release.Manifest,
	}
	if resolved.release.Manifest.Spec.Auth.supportsOAuth2() {
		result.OAuthRedirectURI = connectorOAuthRedirectURI(request)
	}
	bindings, err := store.listTriggerBindings(body.ConnectorID, body.ConnectionName)
	if err != nil {
		api.WriteCodedError(response, http.StatusInternalServerError, "CONNECTOR_TRIGGER_BINDINGS_UNAVAILABLE", "Connector Trigger bindings are unavailable")
		return
	}
	if len(bindings) > 0 {
		result.TriggerBindings = make(map[string]map[string]map[string]json.RawMessage)
		for _, binding := range bindings {
			if result.TriggerBindings[binding.TriggerName] == nil {
				result.TriggerBindings[binding.TriggerName] = make(map[string]map[string]json.RawMessage)
			}
			result.TriggerBindings[binding.TriggerName][binding.BindingName] = binding.Configuration
		}
	}
	if resolved.uiRoot != "" && resolved.release.UI != nil {
		nonce, err := randomConnectorToken(24)
		if err != nil {
			api.WriteCodedError(response, http.StatusInternalServerError, "CONNECTOR_UI_SESSION_FAILED", "Connector UI session could not be created")
			return
		}
		setup.uiSessionsMu.Lock()
		setup.deleteExpiredUISessions(time.Now())
		commands := map[string]connectorManifestStudioCommand{}
		canWriteConnection := false
		if resolved.release.Manifest.Spec.Studio != nil {
			for _, command := range resolved.release.Manifest.Spec.Studio.Commands {
				commands[command.ID] = command
			}
			canWriteConnection = slices.Contains(resolved.release.Manifest.Spec.Studio.Setup.BackendCapabilities, connectorConnectionWriteCapability)
		}
		setup.uiSessions[nonce] = connectorUISession{
			root: resolved.uiRoot, entrypoint: resolved.release.UI.Entrypoint,
			connectorID: body.ConnectorID, connectionName: body.ConnectionName, commands: commands,
			canWriteConnection: canWriteConnection, expiresAt: time.Now().Add(10 * time.Minute),
		}
		setup.uiSessionsMu.Unlock()
		result.SessionNonce = nonce
		result.EntrypointURL = "/api/v2/connector-ui-sessions/" + nonce + "/" + resolved.release.UI.Entrypoint
	}
	writeWebJSON(response, http.StatusOK, result)
}

func (setup *connectorSetup) handlePutTriggerBinding(response http.ResponseWriter, request *http.Request) {
	store := setup.store
	snapshot, identity, ok := setup.authorizeConnectionWrite(response, request)
	if !ok {
		return
	}
	triggerName := request.PathValue("triggerName")
	bindingName := request.PathValue("bindingName")
	binding, found, conflict, err := connectorTriggerDefinitionForKey(
		snapshot.Response, identity.ConnectorID, identity.ConnectionName, triggerName, bindingName,
	)
	binding, _ = normalizeConnectorTriggerBinding(binding, setup.releases.overrideIdentities())
	if err != nil || !found || !binding.ConfigurationEnabled {
		api.WriteCodedError(response, http.StatusNotFound, "CONNECTOR_TRIGGER_BINDING_UNSUPPORTED", "Connector Trigger binding is not configurable")
		return
	}
	if conflict || binding.ModulePath != identity.ModulePath || binding.ModuleVersion != identity.ModuleVersion {
		api.WriteCodedError(response, http.StatusConflict, "CONNECTOR_VERSION_CONFLICT", "Connector Trigger binding conflicts with the current Flow Definition")
		return
	}
	request.Body = http.MaxBytesReader(response, request.Body, 1<<20)
	var body connectorTriggerBindingWriteRequest
	if err := decodeStrictConnectorJSONReader(request.Body, &body); err != nil || body.Configuration == nil {
		api.WriteCodedError(response, http.StatusBadRequest, "CONNECTOR_REQUEST_INVALID", "Connector Trigger binding request is invalid")
		return
	}
	if err := validateConnectorUseConfiguration(body.Configuration, binding.ConfigurationUI); err != nil {
		api.WriteCodedError(response, http.StatusBadRequest, "CONNECTOR_TRIGGER_CONFIGURATION_INVALID", err.Error())
		return
	}
	localBinding := localConnectorTriggerBinding{
		ConnectorID: identity.ConnectorID, ConnectionName: identity.ConnectionName,
		TriggerName: triggerName, BindingName: bindingName, Configuration: body.Configuration,
	}
	if err := store.putTriggerBinding(localBinding); err != nil {
		writeConnectorConfigurationStoreError(response, err, "CONNECTOR_TRIGGER_BINDING_WRITE_FAILED", "Connector Trigger binding could not be saved")
		return
	}
	writeWebJSON(response, http.StatusOK, map[string]any{
		"connectorId": identity.ConnectorID, "connectionName": identity.ConnectionName,
		"triggerName": triggerName, "bindingName": bindingName,
	})
}

func (setup *connectorSetup) handlePutUseConfiguration(response http.ResponseWriter, request *http.Request) {
	store := setup.store
	snapshot, identity, ok := setup.authorizeConnectionWrite(response, request)
	if !ok {
		return
	}
	operationID := request.PathValue("operationId")
	flowType := request.PathValue("flowType")
	stepType := request.PathValue("stepType")
	declared, found, err := connectorOperationDefinitionForUse(
		snapshot.Response, identity, operationID, flowType, stepType, setup.releases.overrideIdentities(),
	)
	if err != nil || !found || len(declared.ConfigurationUI.Units) == 0 {
		api.WriteCodedError(response, http.StatusNotFound, "CONNECTOR_USE_CONFIGURATION_UNSUPPORTED", "Connector operation use is not configurable")
		return
	}
	request.Body = http.MaxBytesReader(response, request.Body, 1<<20)
	var body connectorUseConfigurationWriteRequest
	if err := decodeStrictConnectorJSONReader(request.Body, &body); err != nil || body.Configuration == nil {
		api.WriteCodedError(response, http.StatusBadRequest, "CONNECTOR_REQUEST_INVALID", "Connector use configuration request is invalid")
		return
	}
	if err := validateConnectorUseConfiguration(body.Configuration, declared.ConfigurationUI); err != nil {
		api.WriteCodedError(response, http.StatusBadRequest, "CONNECTOR_USE_CONFIGURATION_INVALID", err.Error())
		return
	}
	configuration := localConnectorUseConfiguration{
		ConnectorID: identity.ConnectorID, ConnectionName: identity.ConnectionName, OperationID: operationID,
		FlowType: flowType, StepType: stepType, Configuration: body.Configuration,
	}
	if err := store.putUseConfiguration(configuration); err != nil {
		writeConnectorConfigurationStoreError(response, err, "CONNECTOR_USE_CONFIGURATION_WRITE_FAILED", "Connector use configuration could not be saved")
		return
	}
	writeWebJSON(response, http.StatusOK, configuration)
}

func (setup *connectorSetup) handleUIAsset(response http.ResponseWriter, request *http.Request) {
	nonce := request.PathValue("sessionNonce")
	setup.uiSessionsMu.Lock()
	setup.deleteExpiredUISessions(time.Now())
	session, found := setup.uiSessions[nonce]
	setup.uiSessionsMu.Unlock()
	if !found {
		api.WriteCodedError(response, http.StatusNotFound, "CONNECTOR_UI_SESSION_NOT_FOUND", "Connector UI session is missing or expired")
		return
	}
	serveConnectorUIAsset(response, request, request.PathValue("assetPath"), session.root, session.canWriteConnection)
}

func (setup *connectorSetup) deleteExpiredUISessions(now time.Time) {
	for nonce, session := range setup.uiSessions {
		if !now.Before(session.expiresAt) {
			delete(setup.uiSessions, nonce)
		}
	}
}

func (setup *connectorSetup) authorizeConnectionWrite(
	response http.ResponseWriter,
	request *http.Request,
) (*connectorAuthorizationSnapshot, connectorDefinitionIdentity, bool) {
	snapshot, identity, ok := setup.authorizeConnectionKey(
		response,
		request,
		request.PathValue("connectorId"),
		request.PathValue("connectionName"),
	)
	if !ok {
		return nil, connectorDefinitionIdentity{}, false
	}
	return snapshot, identity, true
}

func (setup *connectorSetup) authorizeConnectionKey(
	response http.ResponseWriter,
	request *http.Request,
	connectorID string,
	connectionName string,
) (*connectorAuthorizationSnapshot, connectorDefinitionIdentity, bool) {
	hasUnsafeMethod := request.Method != http.MethodGet && request.Method != http.MethodHead
	if request.Header.Get(connectorCSRFHeader) != setup.csrfToken ||
		(hasUnsafeMethod && !hasStrictConnectorOrigin(request)) {
		api.WriteCodedError(response, http.StatusForbidden, "CONNECTOR_WRITE_FORBIDDEN", "Connector write origin or CSRF token is invalid")
		return nil, connectorDefinitionIdentity{}, false
	}
	snapshot, err := setup.flowDefinitions.Load(request.Context())
	if err != nil {
		api.WriteCodedError(response, http.StatusServiceUnavailable, "FLOW_DEFINITION_SOURCE_UNAVAILABLE", "Flow Definition source is unavailable")
		return nil, connectorDefinitionIdentity{}, false
	}
	if snapshot.DefinitionRevision == "" || request.Header.Get(api.V2DefinitionRevisionHeader) != snapshot.DefinitionRevision {
		api.WriteCodedError(response, http.StatusConflict, "FLOW_DEFINITION_REVISION_CONFLICT", "Flow Definition revision is stale")
		return nil, connectorDefinitionIdentity{}, false
	}
	requested, found, conflict, err := connectorDefinitionForKey(
		snapshot.Response,
		connectorID,
		connectionName,
		setup.releases.overrideIdentities(),
	)
	if err != nil {
		api.WriteCodedError(response, http.StatusServiceUnavailable, "FLOW_DEFINITION_SOURCE_UNAVAILABLE", "Flow Definition source is unavailable")
		return nil, connectorDefinitionIdentity{}, false
	}
	if !found || !requested.ConfigurationEnabled {
		api.WriteCodedError(response, http.StatusNotFound, "CONNECTOR_CONNECTION_UNSUPPORTED", "Connector connection is not configurable")
		return nil, connectorDefinitionIdentity{}, false
	}
	if conflict {
		api.WriteCodedError(response, http.StatusConflict, "CONNECTOR_VERSION_CONFLICT", "Connector connection has conflicting module versions")
		return nil, connectorDefinitionIdentity{}, false
	}
	return &connectorAuthorizationSnapshot{DefinitionRevision: snapshot.DefinitionRevision, Response: snapshot.Response}, requested, true
}

func (setup *connectorSetup) connectionViews(ctx context.Context) ([]connectorConnectionView, string, error) {
	snapshot, err := setup.flowDefinitions.Load(ctx)
	if err != nil {
		return nil, "", err
	}
	connections, err := setup.store.list()
	if err != nil {
		return nil, "", err
	}
	stored := make(map[string]storedConnectorConnection, len(connections))
	for _, connection := range connections {
		stored[connection.ConnectorID+"\x00"+connection.ConnectionName] = connection
	}
	views, err := connectorViewsFromCatalog(
		snapshot.Response, stored, time.Now(), setup.releases.overrideIdentities(),
	)
	if err != nil {
		return nil, "", err
	}
	setup.applyConnectorDisplayNames(ctx, views)
	for viewIndex := range views {
		useConfigurations, loadErr := setup.store.listUseConfigurations(views[viewIndex].ConnectorID, views[viewIndex].ConnectionName)
		if loadErr != nil {
			return nil, "", loadErr
		}
		for useIndex := range views[viewIndex].Uses {
			use := &views[viewIndex].Uses[useIndex]
			for _, configuration := range useConfigurations {
				if configuration.OperationID == use.OperationID && configuration.FlowType == use.FlowName && configuration.StepType == use.StepName {
					use.Configuration = configuration.Configuration
					use.Configured = true
				}
			}
			if use.Configuration == nil {
				use.Configuration = map[string]json.RawMessage{}
			}
		}
		triggerBindings, loadErr := setup.store.listTriggerBindings(views[viewIndex].ConnectorID, views[viewIndex].ConnectionName)
		if loadErr != nil {
			return nil, "", loadErr
		}
		for triggerIndex := range views[viewIndex].TriggerUses {
			use := &views[viewIndex].TriggerUses[triggerIndex]
			for _, binding := range triggerBindings {
				if binding.TriggerName == use.TriggerName && binding.BindingName == use.BindingName {
					use.Configuration = binding.Configuration
					use.Configured = true
				}
			}
			if use.Configuration == nil {
				use.Configuration = map[string]json.RawMessage{}
			}
		}
	}
	return views, snapshot.DefinitionRevision, nil
}

// applyConnectorDisplayNames resolves every view's release metadata concurrently, bounded by one timeout.
func (setup *connectorSetup) applyConnectorDisplayNames(ctx context.Context, views []connectorConnectionView) {
	ctx, cancel := context.WithTimeout(ctx, connectorDisplayNameTimeout)
	defer cancel()
	var wg sync.WaitGroup
	for viewIndex := range views {
		wg.Add(1)
		go setup.applyConnectorDisplayName(ctx, &views[viewIndex], &wg)
	}
	wg.Wait()
}

func (setup *connectorSetup) applyConnectorDisplayName(ctx context.Context, view *connectorConnectionView, wg *sync.WaitGroup) {
	defer wg.Done()
	release, err := setup.releases.releaseMetadata(ctx, connectorDefinitionIdentity{
		ConnectorID: view.ConnectorID, ModulePath: view.ModulePath, ModuleVersion: view.ModuleVersion,
	})
	if err != nil {
		// The name is presentational: the page shows the Connector ID, and opening the connection reports the error.
		return
	}
	view.DisplayName = strings.TrimSpace(release.Manifest.Metadata.DisplayName)
}

func connectorViewsFromCatalog(
	catalogJSON []byte,
	stored map[string]storedConnectorConnection,
	now time.Time,
	overrides map[string]connectorDefinitionIdentity,
) ([]connectorConnectionView, error) {
	var catalog connectorCatalogDocument
	if err := json.Unmarshal(catalogJSON, &catalog); err != nil {
		return nil, fmt.Errorf("decode Flow Definition catalog for Connector connections: %w", err)
	}
	type aggregate struct {
		view     connectorConnectionView
		versions map[string]bool
		modules  map[string]bool
		enabled  bool
	}
	aggregates := make(map[string]*aggregate)
	for _, definition := range catalog.Definitions {
		for _, node := range definition.Graph.Nodes {
			declaredIdentity := node.Metadata.Connector
			if node.Kind != "step" || !node.Metadata.ConnectorFactory || declaredIdentity == nil {
				continue
			}
			identity, localOverride := normalizeConnectorDefinitionIdentity(*declaredIdentity, overrides)
			key := identity.ConnectorID + "\x00" + identity.ConnectionName
			current := aggregates[key]
			if current == nil {
				current = &aggregate{
					view: connectorConnectionView{
						ConnectorID: identity.ConnectorID, ConnectionName: identity.ConnectionName,
						ModulePath: identity.ModulePath, ModuleVersion: identity.ModuleVersion, LocalOverride: localOverride,
					},
					versions: make(map[string]bool), modules: make(map[string]bool), enabled: true,
				}
				aggregates[key] = current
			}
			current.enabled = current.enabled && identity.ConfigurationEnabled
			current.view.LocalOverride = current.view.LocalOverride || localOverride
			current.versions[identity.ModuleVersion] = true
			current.modules[identity.ModulePath] = true
			configurationUI := identity.ConfigurationUI
			if configurationUI.Units == nil {
				configurationUI.Units = []api.V2ConnectorUIUnit{}
			}
			current.view.Uses = append(current.view.Uses, connectorConnectionStepUse{
				FlowName: definition.FlowName, StepID: node.ID, StepName: node.Name,
				OperationID: identity.OperationID, OperationKind: identity.OperationKind, ConfigurationUI: configurationUI,
			})
		}
		if definition.Graph.V2 == nil {
			continue
		}
		for _, binding := range definition.Graph.V2.ConnectorTriggerBindings {
			binding, localOverride := normalizeConnectorTriggerBinding(binding, overrides)
			key := binding.ConnectorID + "\x00" + binding.ConnectionName
			current := aggregates[key]
			if current == nil {
				current = &aggregate{
					view: connectorConnectionView{
						ConnectorID: binding.ConnectorID, ConnectionName: binding.ConnectionName,
						ModulePath: binding.ModulePath, ModuleVersion: binding.ModuleVersion, LocalOverride: localOverride,
					},
					versions: make(map[string]bool), modules: make(map[string]bool), enabled: true,
				}
				aggregates[key] = current
			}
			current.enabled = current.enabled && binding.ConfigurationEnabled
			current.view.LocalOverride = current.view.LocalOverride || localOverride
			current.versions[binding.ModuleVersion] = true
			current.modules[binding.ModulePath] = true
			configurationUI := binding.ConfigurationUI
			if configurationUI.Units == nil {
				configurationUI.Units = []api.V2ConnectorUIUnit{}
			}
			current.view.TriggerUses = append(current.view.TriggerUses, connectorConnectionTriggerUse{
				FlowName: definition.FlowName, TriggerName: binding.TriggerName, BindingName: binding.BindingName, ConfigurationUI: configurationUI,
			})
		}
	}
	views := make([]connectorConnectionView, 0, len(aggregates))
	for key, current := range aggregates {
		current.view.Status = "Missing"
		if !current.enabled || current.view.ConnectionName == "" {
			current.view.Status = "Unsupported"
		} else if len(current.versions) > 1 || len(current.modules) > 1 {
			current.view.Status = "Conflict"
		} else if connection, ok := stored[key]; ok {
			current.view.Provider = connection.Provider
			current.view.AuthMethodID = connection.AuthMethodID
			current.view.AuthMethodIDs = connection.AuthMethodIDs
			current.view.Configuration = connection.Configuration
			current.view.StoredCredentialFields = connection.StoredCredentialFields
			current.view.CredentialExpiresAt = connection.CredentialExpiresAt
			if !current.view.LocalOverride && (connection.ModulePath != current.view.ModulePath || connection.ModuleVersion != current.view.ModuleVersion) {
				current.view.Status = "Conflict"
			} else if connection.CredentialExpiresAt != nil && !now.Before(*connection.CredentialExpiresAt) {
				current.view.Status = "Expired"
			} else {
				current.view.Status = "Ready"
			}
		}
		sort.Slice(current.view.Uses, func(left int, right int) bool {
			if current.view.Uses[left].FlowName == current.view.Uses[right].FlowName {
				return current.view.Uses[left].StepID < current.view.Uses[right].StepID
			}
			return current.view.Uses[left].FlowName < current.view.Uses[right].FlowName
		})
		sort.Slice(current.view.TriggerUses, func(left int, right int) bool {
			if current.view.TriggerUses[left].FlowName != current.view.TriggerUses[right].FlowName {
				return current.view.TriggerUses[left].FlowName < current.view.TriggerUses[right].FlowName
			}
			return current.view.TriggerUses[left].BindingName < current.view.TriggerUses[right].BindingName
		})
		views = append(views, current.view)
	}
	sort.Slice(views, func(left int, right int) bool {
		if views[left].ConnectorID == views[right].ConnectorID {
			return views[left].ConnectionName < views[right].ConnectionName
		}
		return views[left].ConnectorID < views[right].ConnectorID
	})
	return views, nil
}

func connectorTriggerDefinitionForKey(
	catalogJSON []byte,
	connectorID string,
	connectionName string,
	triggerName string,
	bindingName string,
) (connectorCatalogTriggerBinding, bool, bool, error) {
	var catalog connectorCatalogDocument
	if err := json.Unmarshal(catalogJSON, &catalog); err != nil {
		return connectorCatalogTriggerBinding{}, false, false, fmt.Errorf("decode Flow Definition catalog for Connector Trigger bindings: %w", err)
	}
	var matched connectorCatalogTriggerBinding
	found := false
	conflict := false
	for _, definition := range catalog.Definitions {
		if definition.Graph.V2 == nil {
			continue
		}
		for _, binding := range definition.Graph.V2.ConnectorTriggerBindings {
			if binding.ConnectorID != connectorID || binding.ConnectionName != connectionName ||
				binding.TriggerName != triggerName || binding.BindingName != bindingName {
				continue
			}
			if found && (matched.ModulePath != binding.ModulePath || matched.ModuleVersion != binding.ModuleVersion) {
				conflict = true
			}
			matched = binding
			found = true
		}
	}
	return matched, found, conflict, nil
}

func connectorOperationDefinitionForUse(
	catalogJSON []byte,
	identity connectorDefinitionIdentity,
	operationID string,
	flowType string,
	stepType string,
	overrides map[string]connectorDefinitionIdentity,
) (connectorDefinitionIdentity, bool, error) {
	var catalog connectorCatalogDocument
	if err := json.Unmarshal(catalogJSON, &catalog); err != nil {
		return connectorDefinitionIdentity{}, false, fmt.Errorf("decode Flow Definition catalog for Connector operation use: %w", err)
	}
	for _, definition := range catalog.Definitions {
		if definition.FlowName != flowType {
			continue
		}
		for _, node := range definition.Graph.Nodes {
			candidate := node.Metadata.Connector
			if node.Kind != "step" || !node.Metadata.ConnectorFactory || candidate == nil {
				continue
			}
			normalized, _ := normalizeConnectorDefinitionIdentity(*candidate, overrides)
			if node.Name != stepType || normalized.ConnectorID != identity.ConnectorID ||
				normalized.ConnectionName != identity.ConnectionName || normalized.OperationID != operationID ||
				normalized.ModulePath != identity.ModulePath || normalized.ModuleVersion != identity.ModuleVersion {
				continue
			}
			return normalized, true, nil
		}
	}
	return connectorDefinitionIdentity{}, false, nil
}

func connectorDefinitionForKey(
	catalogJSON []byte,
	connectorID string,
	connectionName string,
	overrides map[string]connectorDefinitionIdentity,
) (connectorDefinitionIdentity, bool, bool, error) {
	views, err := connectorViewsFromCatalog(catalogJSON, nil, time.Now(), overrides)
	if err != nil {
		return connectorDefinitionIdentity{}, false, false, err
	}
	for _, view := range views {
		if view.ConnectorID != connectorID || view.ConnectionName != connectionName {
			continue
		}
		return connectorDefinitionIdentity{
			ConnectorID: connectorID, ConnectionName: connectionName, ModulePath: view.ModulePath,
			ModuleVersion: view.ModuleVersion, ConfigurationEnabled: view.Status != "Unsupported",
		}, true, view.Status == "Conflict", nil
	}
	return connectorDefinitionIdentity{}, false, false, nil
}

func normalizeConnectorDefinitionIdentity(
	identity connectorDefinitionIdentity,
	overrides map[string]connectorDefinitionIdentity,
) (connectorDefinitionIdentity, bool) {
	override, found := overrides[identity.ConnectorID]
	if !found {
		return identity, false
	}
	identity.ModulePath = override.ModulePath
	identity.ModuleVersion = override.ModuleVersion
	identity.ConfigurationEnabled = identity.ConnectionName != ""
	return identity, true
}

func normalizeConnectorTriggerBinding(
	binding connectorCatalogTriggerBinding,
	overrides map[string]connectorDefinitionIdentity,
) (connectorCatalogTriggerBinding, bool) {
	override, found := overrides[binding.ConnectorID]
	if !found {
		return binding, false
	}
	binding.ModulePath = override.ModulePath
	binding.ModuleVersion = override.ModuleVersion
	binding.ConfigurationEnabled = binding.ConnectionName != "" && binding.BindingName != ""
	return binding, true
}

// The server stamps the selected authentication methods; a client value could contradict them.
func hasConnectorAuthMethodCredential(credentials map[string]json.RawMessage) bool {
	for name := range credentials {
		if isConnectorAuthMethodCredentialName(name) {
			return true
		}
	}
	return false
}

func stampConnectorAuthMethods(
	connection *localConnectorConnection,
	auth connectorManifestAuth,
	selectedMethods []connectorManifestAuthMethod,
) error {
	if auth.isMultipleSelection() {
		selectedMethodIDs := make([]string, 0, len(selectedMethods))
		for _, method := range selectedMethods {
			selectedMethodIDs = append(selectedMethodIDs, method.ID)
		}
		encodedMethodIDs, err := json.Marshal(selectedMethodIDs)
		if err != nil {
			return fmt.Errorf("encode Connector authentication methods: %w", err)
		}
		connection.AuthMethodIDs = selectedMethodIDs
		connection.Credentials["auth_methods"] = encodedMethodIDs
		return nil
	}
	selectedMethodID := selectedMethods[0].ID
	if selectedMethodID == "" {
		return nil
	}
	encodedMethodID, err := json.Marshal(selectedMethodID)
	if err != nil {
		return fmt.Errorf("encode Connector authentication method: %w", err)
	}
	connection.AuthMethodID = selectedMethodID
	connection.Credentials["auth_method"] = encodedMethodID
	return nil
}

// validateConnectorKeepCredentialFields returns the kept field names, or an error when a name is
// not stored, not declared by a selected method, or also set by the request.
func validateConnectorKeepCredentialFields(
	keepCredentialFields []string,
	storedCredentialFields []string,
	credentialFields []connectorManifestField,
	credentials map[string]json.RawMessage,
) (map[string]bool, error) {
	isStored := make(map[string]bool, len(storedCredentialFields))
	for _, name := range storedCredentialFields {
		isStored[name] = true
	}
	isDeclared := make(map[string]bool, len(credentialFields))
	for _, field := range credentialFields {
		isDeclared[field.Name] = true
	}
	kept := make(map[string]bool, len(keepCredentialFields))
	for _, name := range keepCredentialFields {
		_, isSet := credentials[name]
		if !isStored[name] || !isDeclared[name] || isSet {
			return nil, fmt.Errorf("credential field %q cannot be kept", name)
		}
		kept[name] = true
	}
	return kept, nil
}

func connectorConfigurationFieldsForMethods(
	connectionFields []connectorManifestField,
	selectedMethods []connectorManifestAuthMethod,
) []connectorManifestField {
	fields := append([]connectorManifestField{}, connectionFields...)
	for _, method := range selectedMethods {
		fields = append(fields, method.configurationFields()...)
	}
	return fields
}

func connectorCredentialFieldsForMethods(selectedMethods []connectorManifestAuthMethod) []connectorManifestField {
	var fields []connectorManifestField
	for _, method := range selectedMethods {
		fields = append(fields, method.Fields...)
	}
	return fields
}

func isConnectorAuthMethodCredentialName(name string) bool {
	return name == "auth_method" || name == "auth_methods"
}

func hasStrictConnectorOrigin(request *http.Request) bool {
	originValue := request.Header.Get("Origin")
	origin, err := url.Parse(originValue)
	if err != nil || origin.Scheme == "" || origin.Host == "" || origin.Path != "" {
		return false
	}
	scheme := "http"
	if request.TLS != nil {
		scheme = "https"
	}
	return origin.Scheme == scheme && origin.Host == request.Host
}

func randomConnectorToken(byteCount int) (string, error) {
	value := make([]byte, byteCount)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(value), nil
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\\''") + "'"
}
