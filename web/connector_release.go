// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package web

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"mime"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"
)

const (
	connectorReleaseBaseURL       = "https://github.com/superdurable/dex-connectors-library/releases/download"
	connectorReleaseMetadataName  = "connector-release.json"
	connectorReleaseDigestName    = "connector-release.json.sha256"
	connectorReleaseMetadataLimit = 1 << 20
	connectorUIArchiveLimit       = 8 << 20
	connectorUIExpandedLimit      = 8 << 20
	connectorUIFileLimit          = 128
	connectorUIHostAPIRange       = ">=0.2.0 <0.3.0"
)

var officialConnectorModulePattern = regexp.MustCompile(`^github\.com/superdurable/dex-connectors-library/connectors/[a-z0-9][a-z0-9-]*(?:/[a-z0-9][a-z0-9-]*)*$`)

// exactConnectorReleaseVersionPattern matches a published Connector release or release candidate.
var exactConnectorReleaseVersionPattern = regexp.MustCompile(`^v[0-9]+\.[0-9]+\.[0-9]+(?:-rc\.[0-9]+)?$`)
var connectorReleaseIDPattern = regexp.MustCompile(`^[a-z][a-z0-9-]{1,62}$`)

type connectorReleaseResolver struct {
	baseURL       string
	artifactRoot  string
	httpClient    *http.Client
	localReleases map[string]resolvedConnectorRelease
	// verifiedReleases maps an official release tag to its checksum-verified connectorRelease.
	verifiedReleases sync.Map
	mu               sync.Mutex
}

type connectorRelease struct {
	ConnectorID    string                   `json:"connectorId"`
	Manifest       connectorReleaseManifest `json:"manifest"`
	ModulePath     string                   `json:"modulePath"`
	Version        string                   `json:"version"`
	Tag            string                   `json:"tag"`
	SourceSHA      string                   `json:"sourceSha"`
	ManifestSHA256 string                   `json:"manifestSha256"`
	UI             *connectorUIRelease      `json:"ui,omitempty"`
}

type connectorReleaseManifest struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Metadata   struct {
		Name        string `json:"name"`
		DisplayName string `json:"displayName"`
		Description string `json:"description"`
	} `json:"metadata"`
	Spec struct {
		Provider      string                     `json:"provider"`
		Triggers      []connectorManifestTrigger `json:"triggers,omitempty"`
		Configuration struct {
			Fields []connectorManifestField `json:"fields"`
		} `json:"configuration"`
		Auth   connectorManifestAuth    `json:"auth"`
		Studio *connectorManifestStudio `json:"studio,omitempty"`
	} `json:"spec"`
}

const connectorAuthSelectionMultiple = "multiple"

type connectorManifestAuth struct {
	Type           string                        `json:"type,omitempty"`
	ConnectionKind string                        `json:"connectionKind,omitempty"`
	Fields         []connectorManifestField      `json:"fields,omitempty"`
	Guide          *connectorAuthorizationGuide  `json:"guide,omitempty"`
	OAuth2         *connectorManifestOAuth2      `json:"oauth2,omitempty"`
	DefaultMethod  string                        `json:"defaultMethod,omitempty"`
	Selection      string                        `json:"selection,omitempty"`
	MethodLabel    string                        `json:"methodLabel,omitempty"`
	Methods        []connectorManifestAuthMethod `json:"methods,omitempty"`
}

type connectorManifestAuthMethod struct {
	ID             string                                    `json:"id"`
	DisplayName    string                                    `json:"displayName"`
	Description    string                                    `json:"description"`
	Recommended    bool                                      `json:"recommended,omitempty"`
	Type           string                                    `json:"type"`
	ConnectionKind string                                    `json:"connectionKind"`
	Fields         []connectorManifestField                  `json:"fields"`
	Configuration  *connectorManifestAuthMethodConfiguration `json:"configuration,omitempty"`
	Guide          *connectorAuthorizationGuide              `json:"guide,omitempty"`
	OAuth2         *connectorManifestOAuth2                  `json:"oauth2,omitempty"`
}

type connectorManifestAuthMethodConfiguration struct {
	Fields []connectorManifestField `json:"fields"`
}

func (auth connectorManifestAuth) isMultipleSelection() bool {
	return auth.Selection == connectorAuthSelectionMultiple
}

// selectedMethods returns the methods a connection write selects, in request order, or an
// error when the selection is empty, repeated, undeclared, or uses the other selection field.
func (auth connectorManifestAuth) selectedMethods(methodID string, methodIDs []string) ([]connectorManifestAuthMethod, error) {
	if !auth.isMultipleSelection() {
		if len(methodIDs) != 0 {
			return nil, fmt.Errorf("authMethodIds requires multiple authentication method selection")
		}
		method, found := auth.method(methodID)
		if !found {
			return nil, fmt.Errorf("authentication method %q is not declared", methodID)
		}
		return []connectorManifestAuthMethod{method}, nil
	}
	if methodID != "" || len(methodIDs) == 0 {
		return nil, fmt.Errorf("multiple authentication method selection requires a non-empty authMethodIds")
	}
	selected := make([]connectorManifestAuthMethod, 0, len(methodIDs))
	seen := make(map[string]bool, len(methodIDs))
	for _, candidateID := range methodIDs {
		method, found := auth.method(candidateID)
		if candidateID == "" || !found || seen[candidateID] {
			return nil, fmt.Errorf("authentication method %q is empty, undeclared, or repeated", candidateID)
		}
		seen[candidateID] = true
		selected = append(selected, method)
	}
	return selected, nil
}

func (auth connectorManifestAuth) method(methodID string) (connectorManifestAuthMethod, bool) {
	if len(auth.Methods) == 0 {
		if methodID != "" {
			return connectorManifestAuthMethod{}, false
		}
		return connectorManifestAuthMethod{
			Type: auth.Type, ConnectionKind: auth.ConnectionKind, Fields: auth.Fields,
			Guide: auth.Guide, OAuth2: auth.OAuth2,
		}, true
	}
	if methodID == "" {
		methodID = auth.DefaultMethod
	}
	for _, method := range auth.Methods {
		if method.ID == methodID {
			return method, true
		}
	}
	return connectorManifestAuthMethod{}, false
}

func (auth connectorManifestAuth) supportsOAuth2() bool {
	if len(auth.Methods) == 0 {
		return auth.Type == "oauth2" && auth.OAuth2 != nil
	}
	for _, method := range auth.Methods {
		if method.Type == "oauth2" && method.OAuth2 != nil {
			return true
		}
	}
	return false
}

func (method connectorManifestAuthMethod) configurationFields() []connectorManifestField {
	if method.Configuration == nil {
		return nil
	}
	return method.Configuration.Fields
}

type connectorAuthorizationGuide struct {
	StartURL string   `json:"startURL"`
	Steps    []string `json:"steps"`
}

type connectorManifestStudio struct {
	Setup    connectorManifestStudioSetup     `json:"setup"`
	Commands []connectorManifestStudioCommand `json:"commands,omitempty"`
	Units    []connectorManifestStudioUnit    `json:"units,omitempty"`
}

type connectorManifestTrigger struct {
	Name          string                                    `json:"name"`
	Description   string                                    `json:"description"`
	Configuration *connectorManifestAuthMethodConfiguration `json:"configuration,omitempty"`
}

type connectorManifestField struct {
	UniqueItems bool                              `json:"uniqueItems,omitempty"`
	Name        string                            `json:"name"`
	Type        string                            `json:"type"`
	Description string                            `json:"description"`
	Required    bool                              `json:"required"`
	Default     any                               `json:"default,omitempty"`
	Enum        []string                          `json:"enum,omitempty"`
	StudioUnit  *connectorManifestFieldStudioUnit `json:"studioUnit,omitempty"`
}

type connectorManifestFieldStudioUnit struct {
	Unit string `json:"unit"`
	Port string `json:"port"`
}

type connectorManifestOAuth2 struct {
	AuthorizationEndpoint   string                               `json:"authorizationEndpoint"`
	TokenEndpoint           string                               `json:"tokenEndpoint"`
	TokenRequestEncoding    string                               `json:"tokenRequestEncoding,omitempty"`
	TokenEndpointAuthMethod string                               `json:"tokenEndpointAuthMethod,omitempty"`
	AuthorizationParameters map[string]string                    `json:"authorizationParameters,omitempty"`
	ClientIDCredential      string                               `json:"clientIDCredential,omitempty"`
	ClientSecretCredential  string                               `json:"clientSecretCredential,omitempty"`
	Scopes                  []string                             `json:"scopes,omitempty"`
	UserScopes              []string                             `json:"userScopes,omitempty"`
	CredentialMappings      []connectorOAuthCredentialMapping    `json:"credentialMappings,omitempty"`
	CredentialDerivations   []connectorOAuthCredentialDerivation `json:"credentialDerivations,omitempty"`
	PKCE                    bool                                 `json:"pkce"`

	// A Setting field names the configuration field that supplies the value; the manifest wrote {"fromSetting": name}.
	AuthorizationEndpointSetting string `json:"-"`
	TokenEndpointSetting         string `json:"-"`
	ScopesSetting                string `json:"-"`
}

type connectorOAuthCredentialMapping struct {
	Credential string `json:"credential"`
	Source     string `json:"source"`
}

type connectorOAuthCredentialDerivation struct {
	Credential string `json:"credential"`
	Endpoint   string `json:"endpoint"`
	Source     string `json:"source"`
	VerifiedBy string `json:"verifiedBy,omitempty"`

	EndpointSetting string `json:"-"`
}

type connectorManifestStudioSetup struct {
	Entrypoint          string   `json:"entrypoint"`
	HostAPIRange        string   `json:"hostApiRange"`
	BackendCapabilities []string `json:"backendCapabilities"`
}

type connectorManifestStudioCommand struct {
	ID         string                             `json:"id"`
	Capability string                             `json:"capability"`
	Request    connectorManifestStudioHTTPRequest `json:"request"`
}

type connectorManifestStudioHTTPRequest struct {
	Method       string                             `json:"method"`
	URL          string                             `json:"url"`
	Credential   connectorManifestStudioCredential  `json:"credential"`
	FixedQuery   map[string]string                  `json:"fixedQuery,omitempty"`
	FixedHeaders map[string]string                  `json:"fixedHeaders,omitempty"`
	Parameters   []connectorManifestStudioParameter `json:"parameters,omitempty"`
}

type connectorManifestStudioCredential struct {
	Field  string `json:"field"`
	Scheme string `json:"scheme"`
	Header string `json:"header,omitempty"`
}

type connectorManifestStudioParameter struct {
	Name     string `json:"name"`
	Location string `json:"location"`
	Target   string `json:"target"`
}

type connectorManifestStudioUnit struct {
	ID                  string                        `json:"id"`
	Description         string                        `json:"description"`
	BackendCapabilities []string                      `json:"backendCapabilities,omitempty"`
	Inputs              []connectorManifestStudioPort `json:"inputs,omitempty"`
	Outputs             []connectorManifestStudioPort `json:"outputs"`
}

type connectorManifestStudioPort struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type connectorUIRelease struct {
	Artifact     string   `json:"artifact"`
	SHA256       string   `json:"sha256"`
	Entrypoint   string   `json:"entrypoint"`
	HostAPIRange string   `json:"hostApiRange"`
	Capabilities []string `json:"backendCapabilities"`
}

type resolvedConnectorRelease struct {
	release connectorRelease
	uiRoot  string
}

func newConnectorReleaseResolver(configDirectory string, overrides map[string]string) (*connectorReleaseResolver, error) {
	artifactRoot := filepath.Join(configDirectory, "artifacts")
	if err := os.MkdirAll(artifactRoot, 0o700); err != nil {
		return nil, fmt.Errorf("create Connector artifact cache: %w", err)
	}
	resolver := &connectorReleaseResolver{
		baseURL: connectorReleaseBaseURL, artifactRoot: artifactRoot,
		localReleases: make(map[string]resolvedConnectorRelease, len(overrides)),
	}
	resolver.httpClient = resolver.newHTTPClient()
	for connectorID, directory := range overrides {
		resolved, loadErr := resolver.loadLocalRelease(connectorID, directory)
		if loadErr != nil {
			return nil, fmt.Errorf("load Connector release override %q: %w", connectorID, loadErr)
		}
		resolver.localReleases[connectorID] = resolved
	}
	return resolver, nil
}

func (resolver *connectorReleaseResolver) newHTTPClient() *http.Client {
	base, err := url.Parse(resolver.baseURL)
	if err != nil {
		panic("Connector release base URL is invalid")
	}
	return &http.Client{
		Timeout: 30 * time.Second,
		CheckRedirect: func(request *http.Request, via []*http.Request) error {
			if len(via) >= 5 {
				return fmt.Errorf("Connector artifact redirect limit exceeded")
			}
			host := strings.ToLower(request.URL.Hostname())
			if host == strings.ToLower(base.Hostname()) || host == "github.com" || strings.HasSuffix(host, ".githubusercontent.com") {
				return nil
			}
			return fmt.Errorf("Connector artifact redirect host is not allowed")
		},
	}
}

func (resolver *connectorReleaseResolver) loadLocalRelease(
	connectorID string,
	directory string,
) (resolvedConnectorRelease, error) {
	if !connectorReleaseIDPattern.MatchString(connectorID) {
		return resolvedConnectorRelease{}, fmt.Errorf("Connector ID is invalid")
	}
	absoluteDirectory, err := filepath.Abs(strings.TrimSpace(directory))
	if err != nil {
		return resolvedConnectorRelease{}, fmt.Errorf("resolve local release directory: %w", err)
	}
	directoryInfo, err := os.Lstat(absoluteDirectory)
	if err != nil || directoryInfo.Mode()&os.ModeSymlink != 0 || !directoryInfo.IsDir() {
		return resolvedConnectorRelease{}, fmt.Errorf("local release override must be a directory")
	}
	digestBytes, err := readLocalConnectorArtifact(absoluteDirectory, connectorReleaseDigestName, 1024)
	if err != nil {
		return resolvedConnectorRelease{}, err
	}
	expectedDigest, err := parseConnectorDigest(digestBytes, connectorReleaseMetadataName)
	if err != nil {
		return resolvedConnectorRelease{}, err
	}
	metadata, err := readLocalConnectorArtifact(absoluteDirectory, connectorReleaseMetadataName, connectorReleaseMetadataLimit)
	if err != nil {
		return resolvedConnectorRelease{}, err
	}
	actualDigest := sha256.Sum256(metadata)
	if hex.EncodeToString(actualDigest[:]) != expectedDigest {
		return resolvedConnectorRelease{}, fmt.Errorf("Connector release metadata checksum does not match")
	}
	var release connectorRelease
	if err := json.Unmarshal(metadata, &release); err != nil {
		return resolvedConnectorRelease{}, fmt.Errorf("decode Connector release metadata: %w", err)
	}
	expectedTag := strings.TrimPrefix(release.ModulePath, "github.com/superdurable/dex-connectors-library/") + "/" + release.Version
	if release.ConnectorID != connectorID || release.Manifest.Metadata.Name != connectorID ||
		!officialConnectorModulePattern.MatchString(release.ModulePath) ||
		!exactConnectorReleaseVersionPattern.MatchString(release.Version) || release.Tag != expectedTag {
		return resolvedConnectorRelease{}, fmt.Errorf("Connector release override identity is invalid")
	}
	manifestDigest, err := hex.DecodeString(release.ManifestSHA256)
	if err != nil || len(manifestDigest) != sha256.Size || release.SourceSHA == "" {
		return resolvedConnectorRelease{}, fmt.Errorf("Connector release provenance is invalid")
	}
	if err := validateConnectorReleaseStudioCommands(release.Manifest.Spec.Studio); err != nil {
		return resolvedConnectorRelease{}, err
	}
	resolved := resolvedConnectorRelease{release: release}
	if release.UI == nil {
		return resolved, nil
	}
	uiDigest, err := hex.DecodeString(release.UI.SHA256)
	if err != nil || len(uiDigest) != sha256.Size || path.Base(release.UI.Artifact) != release.UI.Artifact ||
		!isSafeConnectorAssetPath(release.UI.Entrypoint) {
		return resolvedConnectorRelease{}, fmt.Errorf("Connector UI artifact metadata is invalid")
	}
	if release.UI.HostAPIRange != connectorUIHostAPIRange || release.Manifest.Spec.Studio == nil ||
		release.Manifest.Spec.Studio.Setup.Entrypoint != release.UI.Entrypoint ||
		!equalConnectorCapabilities(release.Manifest.Spec.Studio.Setup.BackendCapabilities, release.UI.Capabilities) {
		return resolvedConnectorRelease{}, fmt.Errorf("Connector UI Host API or entrypoint is incompatible")
	}
	archive, err := readLocalConnectorArtifact(absoluteDirectory, release.UI.Artifact, connectorUIArchiveLimit)
	if err != nil {
		return resolvedConnectorRelease{}, err
	}
	uiRoot, err := resolver.cacheUIArchive(release, archive)
	if err != nil {
		return resolvedConnectorRelease{}, err
	}
	resolved.uiRoot = uiRoot
	return resolved, nil
}

func (resolver *connectorReleaseResolver) overrideIdentities() map[string]connectorDefinitionIdentity {
	identities := make(map[string]connectorDefinitionIdentity, len(resolver.localReleases))
	for connectorID, resolved := range resolver.localReleases {
		identities[connectorID] = connectorDefinitionIdentity{
			ConnectorID: connectorID, ModulePath: resolved.release.ModulePath,
			ModuleVersion: resolved.release.Version, ConfigurationEnabled: true,
		}
	}
	return identities
}

func readLocalConnectorArtifact(directory string, name string, limit int64) ([]byte, error) {
	if filepath.Base(name) != name {
		return nil, fmt.Errorf("Connector artifact name is invalid")
	}
	artifactPath := filepath.Join(directory, name)
	info, err := os.Lstat(artifactPath)
	if err != nil {
		return nil, fmt.Errorf("inspect local Connector artifact: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() || info.Size() > limit {
		return nil, fmt.Errorf("local Connector artifact must be a regular file within the size limit")
	}
	contents, err := os.ReadFile(artifactPath)
	if err != nil {
		return nil, fmt.Errorf("read local Connector artifact: %w", err)
	}
	return contents, nil
}

func (resolver *connectorReleaseResolver) resolve(
	ctx context.Context,
	identity connectorDefinitionIdentity,
) (resolvedConnectorRelease, error) {
	if resolved, ok := resolver.localReleases[identity.ConnectorID]; ok {
		return resolved, nil
	}
	release, err := resolver.releaseMetadata(ctx, identity)
	if err != nil {
		return resolvedConnectorRelease{}, err
	}
	resolved := resolvedConnectorRelease{release: release}
	if release.UI == nil {
		return resolved, nil
	}
	uiDigest, err := hex.DecodeString(release.UI.SHA256)
	if err != nil || len(uiDigest) != sha256.Size || path.Base(release.UI.Artifact) != release.UI.Artifact ||
		!isSafeConnectorAssetPath(release.UI.Entrypoint) {
		return resolvedConnectorRelease{}, fmt.Errorf("Connector UI artifact metadata is invalid")
	}
	if release.UI.HostAPIRange != connectorUIHostAPIRange || release.Manifest.Spec.Studio == nil ||
		release.Manifest.Spec.Studio.Setup.Entrypoint != release.UI.Entrypoint ||
		!equalConnectorCapabilities(release.Manifest.Spec.Studio.Setup.BackendCapabilities, release.UI.Capabilities) {
		return resolvedConnectorRelease{}, fmt.Errorf("Connector UI Host API or entrypoint is incompatible")
	}
	uiRoot, err := resolver.cacheUI(ctx, release.Tag, release)
	if err != nil {
		return resolvedConnectorRelease{}, err
	}
	resolved.uiRoot = uiRoot
	return resolved, nil
}

// releaseMetadata returns the verified release metadata for identity. A local override needs no
// download, and an official release is downloaded at most once per process because its tag is exact.
func (resolver *connectorReleaseResolver) releaseMetadata(
	ctx context.Context,
	identity connectorDefinitionIdentity,
) (connectorRelease, error) {
	if resolved, ok := resolver.localReleases[identity.ConnectorID]; ok {
		return resolved.release, nil
	}
	if !connectorReleaseIDPattern.MatchString(identity.ConnectorID) ||
		!officialConnectorModulePattern.MatchString(identity.ModulePath) ||
		!exactConnectorReleaseVersionPattern.MatchString(identity.ModuleVersion) {
		return connectorRelease{}, fmt.Errorf("Connector module is not an exact official release")
	}
	tag := strings.TrimPrefix(identity.ModulePath, "github.com/superdurable/dex-connectors-library/") + "/" + identity.ModuleVersion
	if verified, ok := resolver.verifiedReleases.Load(tag); ok {
		return verified.(connectorRelease), nil
	}
	digestBytes, err := resolver.download(ctx, tag, connectorReleaseDigestName, 1024)
	if err != nil {
		return connectorRelease{}, err
	}
	expectedDigest, err := parseConnectorDigest(digestBytes, connectorReleaseMetadataName)
	if err != nil {
		return connectorRelease{}, err
	}
	metadata, err := resolver.download(ctx, tag, connectorReleaseMetadataName, connectorReleaseMetadataLimit)
	if err != nil {
		return connectorRelease{}, err
	}
	actualDigest := sha256.Sum256(metadata)
	if hex.EncodeToString(actualDigest[:]) != expectedDigest {
		return connectorRelease{}, fmt.Errorf("Connector release metadata checksum does not match")
	}
	var release connectorRelease
	if err := json.Unmarshal(metadata, &release); err != nil {
		return connectorRelease{}, fmt.Errorf("decode Connector release metadata: %w", err)
	}
	if release.ConnectorID != identity.ConnectorID || release.Manifest.Metadata.Name != identity.ConnectorID ||
		release.ModulePath != identity.ModulePath || release.Version != identity.ModuleVersion || release.Tag != tag {
		return connectorRelease{}, fmt.Errorf("Connector release identity does not match Flow Definition")
	}
	manifestDigest, err := hex.DecodeString(release.ManifestSHA256)
	if err != nil || len(manifestDigest) != sha256.Size || release.SourceSHA == "" {
		return connectorRelease{}, fmt.Errorf("Connector release provenance is invalid")
	}
	resolver.verifiedReleases.Store(tag, release)
	return release, nil
}

func (resolver *connectorReleaseResolver) cacheUI(
	ctx context.Context,
	tag string,
	release connectorRelease,
) (cachePath string, returnErr error) {
	archive, err := resolver.download(ctx, tag, release.UI.Artifact, connectorUIArchiveLimit)
	if err != nil {
		return "", err
	}
	return resolver.cacheUIArchive(release, archive)
}

func (resolver *connectorReleaseResolver) cacheUIArchive(
	release connectorRelease,
	archive []byte,
) (cachePath string, returnErr error) {
	resolver.mu.Lock()
	defer resolver.mu.Unlock()
	ui := release.UI
	cacheRoot := filepath.Join(resolver.artifactRoot, release.ConnectorID, release.Version, ui.SHA256)
	entrypoint := filepath.Join(cacheRoot, filepath.FromSlash(ui.Entrypoint))
	if info, err := os.Stat(entrypoint); err == nil && info.Mode().IsRegular() {
		return cacheRoot, nil
	}
	digest := sha256.Sum256(archive)
	if hex.EncodeToString(digest[:]) != ui.SHA256 {
		return "", fmt.Errorf("Connector UI artifact checksum does not match")
	}
	temporaryRoot, err := os.MkdirTemp(filepath.Dir(cacheRoot), ".connector-ui-*")
	if err != nil {
		if makeErr := os.MkdirAll(filepath.Dir(cacheRoot), 0o700); makeErr != nil {
			return "", fmt.Errorf("create Connector UI cache parent: %w", makeErr)
		}
		temporaryRoot, err = os.MkdirTemp(filepath.Dir(cacheRoot), ".connector-ui-*")
		if err != nil {
			return "", fmt.Errorf("create temporary Connector UI cache: %w", err)
		}
	}
	cleanup := true
	defer func() {
		if cleanup {
			if cleanupErr := os.RemoveAll(temporaryRoot); cleanupErr != nil {
				returnErr = errors.Join(returnErr, fmt.Errorf("remove temporary Connector UI cache: %w", cleanupErr))
			}
		}
	}()
	if err := extractConnectorUI(archive, temporaryRoot); err != nil {
		return "", err
	}
	if info, err := os.Stat(filepath.Join(temporaryRoot, filepath.FromSlash(ui.Entrypoint))); err != nil || !info.Mode().IsRegular() {
		return "", fmt.Errorf("Connector UI entrypoint is missing")
	}
	if err := os.MkdirAll(filepath.Dir(cacheRoot), 0o700); err != nil {
		return "", fmt.Errorf("create Connector UI cache directory: %w", err)
	}
	if err := os.Rename(temporaryRoot, cacheRoot); err != nil {
		if info, statErr := os.Stat(entrypoint); statErr == nil && info.Mode().IsRegular() {
			return cacheRoot, nil
		}
		return "", fmt.Errorf("publish Connector UI cache: %w", err)
	}
	cleanup = false
	return cacheRoot, nil
}

func (resolver *connectorReleaseResolver) download(ctx context.Context, tag string, asset string, limit int64) ([]byte, error) {
	assetURL := strings.TrimRight(resolver.baseURL, "/") + "/" + tag + "/" + asset
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, assetURL, nil)
	if err != nil {
		return nil, fmt.Errorf("create Connector artifact request: %w", err)
	}
	response, err := resolver.httpClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("download Connector artifact: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("download Connector artifact: HTTP %d", response.StatusCode)
	}
	contents, err := io.ReadAll(io.LimitReader(response.Body, limit+1))
	if err != nil {
		return nil, fmt.Errorf("read Connector artifact: %w", err)
	}
	if int64(len(contents)) > limit {
		return nil, fmt.Errorf("Connector artifact exceeds size limit")
	}
	return contents, nil
}

func extractConnectorUI(archive []byte, root string) error {
	gzipReader, err := gzip.NewReader(bytes.NewReader(archive))
	if err != nil {
		return fmt.Errorf("open Connector UI archive: %w", err)
	}
	tarReader := tar.NewReader(gzipReader)
	fileCount := 0
	var expandedBytes int64
	for {
		header, nextErr := tarReader.Next()
		if errors.Is(nextErr, io.EOF) {
			break
		}
		if nextErr != nil {
			return fmt.Errorf("read Connector UI archive: %w", nextErr)
		}
		cleanName := path.Clean(header.Name)
		if header.Typeflag != tar.TypeReg || cleanName == "." || strings.HasPrefix(cleanName, "../") || path.IsAbs(cleanName) {
			return fmt.Errorf("Connector UI archive contains an unsafe path or file type")
		}
		fileCount++
		expandedBytes += header.Size
		if fileCount > connectorUIFileLimit || expandedBytes > connectorUIExpandedLimit || header.Size < 0 {
			return fmt.Errorf("Connector UI archive exceeds extraction limits")
		}
		destination := filepath.Join(root, filepath.FromSlash(cleanName))
		if err := os.MkdirAll(filepath.Dir(destination), 0o700); err != nil {
			return fmt.Errorf("create Connector UI asset directory: %w", err)
		}
		file, err := os.OpenFile(destination, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err != nil {
			return fmt.Errorf("create Connector UI asset: %w", err)
		}
		_, copyErr := io.CopyN(file, tarReader, header.Size)
		closeErr := file.Close()
		if copyErr != nil || closeErr != nil {
			return errors.Join(copyErr, closeErr)
		}
	}
	if err := gzipReader.Close(); err != nil {
		return fmt.Errorf("close Connector UI archive: %w", err)
	}
	if fileCount == 0 {
		return fmt.Errorf("Connector UI archive is empty")
	}
	return nil
}

func parseConnectorDigest(contents []byte, expectedFile string) (string, error) {
	fields := strings.Fields(string(contents))
	if len(fields) != 2 || fields[1] != expectedFile || len(fields[0]) != sha256.Size*2 {
		return "", fmt.Errorf("Connector artifact digest is invalid")
	}
	if _, err := hex.DecodeString(fields[0]); err != nil {
		return "", fmt.Errorf("Connector artifact digest is invalid")
	}
	return strings.ToLower(fields[0]), nil
}

func isSafeConnectorAssetPath(value string) bool {
	cleaned := path.Clean(value)
	return cleaned != "." && cleaned == value && !path.IsAbs(cleaned) && !strings.HasPrefix(cleaned, "../")
}

func equalConnectorCapabilities(manifestCapabilities []string, releaseCapabilities []string) bool {
	if len(manifestCapabilities) != len(releaseCapabilities) {
		return false
	}
	for index, capability := range manifestCapabilities {
		if capability != releaseCapabilities[index] {
			return false
		}
	}
	return true
}

// connectorUIContentSecurityPolicy allows remote images only to bundles that never hold a typed credential.
func connectorUIContentSecurityPolicy(canWriteConnection bool) string {
	images := "img-src 'self' data: https:"
	if canWriteConnection {
		images = "img-src 'self' data:"
	}
	return "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; " + images +
		"; connect-src 'none'; base-uri 'none'; form-action 'none'"
}

func serveConnectorUIAsset(response http.ResponseWriter, request *http.Request, assetPath string, root string, canWriteConnection bool) {
	cleanPath := path.Clean(assetPath)
	if cleanPath == "." || strings.HasPrefix(cleanPath, "../") || path.IsAbs(cleanPath) {
		http.NotFound(response, request)
		return
	}
	asset, err := os.Open(filepath.Join(root, filepath.FromSlash(cleanPath)))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			http.NotFound(response, request)
			return
		}
		http.Error(response, "Connector UI asset is unavailable", http.StatusInternalServerError)
		return
	}
	defer asset.Close()
	info, err := asset.Stat()
	if err != nil || !info.Mode().IsRegular() {
		http.NotFound(response, request)
		return
	}
	contentType := mime.TypeByExtension(filepath.Ext(cleanPath))
	if contentType != "" {
		response.Header().Set("Content-Type", contentType)
	}
	response.Header().Set("Content-Security-Policy", connectorUIContentSecurityPolicy(canWriteConnection))
	http.ServeContent(response, request, cleanPath, info.ModTime(), asset)
}
