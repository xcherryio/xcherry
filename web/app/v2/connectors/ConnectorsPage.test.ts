// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

import { createElement } from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { describe, expect, it } from 'vitest';
import { DexAPIError } from '@/lib/http';
import {
  ConnectionStatusChip,
  ConnectorForm,
  ConnectorStoreZone,
  ConnectorStudioFrameThemeContext,
  addConnectionAuthMethod,
  addableAuthMethods,
  connectionAuthMethodIds,
  connectionConfigurationSaveRequestBody,
  connectionSetupSaveRequestBody,
  connectionFieldPresentation,
  connectionFieldStudioTarget,
  connectionFormInitialValues,
  connectionKey,
  connectionDeleteLabel,
  connectionReleaseText,
  connectorDisplayName,
  connectorAuthMethodLabels,
  connectorConfigurationEffectText,
  connectorHostReadyMessage,
  connectorSetupTabs,
  connectorWriteHeaders,
  connectionWriteRequestBody,
  initialConnectionAuthMethodIds,
  initialConnectorSetupTabKey,
  mergeUnitValue,
  removeConnectionAuthMethod,
  requestedOAuthScopesText,
  storedCredentialPlaceholder,
  isConnectionSetupStudio,
  isStudioCommand,
  providerCommandRequestBody,
  isStudioFrameResize,
  descriptionParts,
  manifestFieldDefaultText,
  studioCommandFailureOutcome,
  selectedManifestAuth,
  studioHostCapabilities,
  studioState,
} from './ConnectorsPage';
import { connectorStudioStylesheet } from './connectorStudioTheme';
import { CONNECTORS_COPY } from './copy';

describe('Connections contract', () => {
  it('describes local file effects', () => {
    expect(connectionDeleteLabel()).toBe('Delete local credentials');
    expect(connectorConfigurationEffectText()).toContain('app restart');
  });

  it('selects the declared auth method and falls back to the manifest default', () => {
    const auth = {
      fields: [],
      defaultMethod: 'googleOAuth',
      methods: [
        {id: 'googleOAuth', displayName: 'Google OAuth', description: 'Recommended', recommended: true, type: 'oauth2', fields: [{name: 'access_token'}]},
        {id: 'workspaceServiceAccount', displayName: 'Workspace service account', description: 'Advanced', type: 'serviceAccount', fields: [{name: 'service_account_key'}]},
      ],
    } as never;
    expect(selectedManifestAuth(auth, 'workspaceServiceAccount').id).toBe('workspaceServiceAccount');
    expect(selectedManifestAuth(auth, 'missing').id).toBe('googleOAuth');
  });

  it('saves a non-OAuth method of a multi-method manifest without sending auth_method', () => {
    const manifest = {
      metadata: {displayName: 'Gmail', description: 'Gmail connection'},
      spec: {
        provider: 'google',
        configuration: {fields: [{name: 'endpoint', type: 'url', description: '', required: false}]},
        auth: {
          fields: [],
          defaultMethod: 'apiKey',
          methods: [
            {id: 'apiKey', displayName: 'API key', description: '', type: 'apiKey', fields: [
              {name: 'api_key', type: 'secretString', description: '', required: true},
            ]},
            {id: 'workspaceServiceAccount', displayName: 'Workspace service account', description: '', type: 'serviceAccount', fields: [
              {name: 'service_account_key', type: 'secretString', description: '', required: true},
              {name: 'delegated_user', type: 'string', description: '', required: true},
            ]},
          ],
        },
      },
    } as never;
    const connection = {connectorId: 'gmail', connectionName: 'sender', modulePath: 'github.com/superdurable/dex-connectors-library/connectors/google/gmail', moduleVersion: 'v0.1.1'} as never;
    const body = connectionWriteRequestBody(connection, manifest, ['workspaceServiceAccount'], {
      'configuration:endpoint': 'https://gmail.example.test',
      'credential:service_account_key': '{"type":"service_account"}',
      'credential:delegated_user': 'owner@example.com',
      'credential:api_key': 'from-the-other-method',
    });
    expect(body).toEqual({
      modulePath: 'github.com/superdurable/dex-connectors-library/connectors/google/gmail',
      moduleVersion: 'v0.1.1',
      provider: 'google',
      authMethodId: 'workspaceServiceAccount',
      configuration: {endpoint: 'https://gmail.example.test'},
      credentials: {service_account_key: '{"type":"service_account"}', delegated_user: 'owner@example.com'},
      keepCredentialFields: [],
      credentialExpiresAt: null,
    });
  });

  it('shows non-secret manifest defaults parenthetically and never exposes secret defaults', () => {
    expect(manifestFieldDefaultText({name: 'endpoint', type: 'url', description: '', required: false, default: 'https://api.example.com/v1'})).toBe('(Default: https://api.example.com/v1)');
    expect(manifestFieldDefaultText({name: 'limits', type: 'stringMap', description: '', required: false, default: {rows: 10}})).toBe('(Default: {"rows":10})');
    expect(manifestFieldDefaultText({name: 'api_key', type: 'secretString', description: '', required: true, default: 'never-render'})).toBeUndefined();
  });

  it('renders required Connector fields before an expanded optional settings group', () => {
    const markup = renderConnectorForm({
      metadata: {displayName: 'Stripe', description: 'Stripe connection'},
      spec: {
        provider: 'stripe',
        configuration: {fields: [
          {name: 'endpoint', type: 'url', description: 'API endpoint.', required: false, default: 'https://api.stripe.com/v1'},
          {name: 'required_with_default', type: 'integer', description: 'Defaulted limit.', required: true, default: 1048576},
          {name: 'nickname', type: 'string', description: 'Optional label.', required: false},
        ]},
        auth: {type: 'apiKey', fields: [
          {name: 'secret_key', type: 'secretString', description: 'Stripe key.', required: true},
          {name: 'webhook_secret', type: 'secretString', description: 'Webhook secret.', required: true},
        ]},
      },
    });

    expect(markup.indexOf('<legend class="sc-blockhead">Required</legend>')).toBeLessThan(markup.indexOf('secret_key *'));
    expect(markup.indexOf('secret_key *')).toBeLessThan(markup.indexOf('webhook_secret *'));
    expect(markup.indexOf('webhook_secret *')).toBeLessThan(markup.indexOf('<legend class="sc-blockhead">Optional settings (3)</legend>'));
    expect(markup.indexOf('endpoint')).toBeLessThan(markup.indexOf('required_with_default'));
    expect(markup.indexOf('required_with_default')).toBeLessThan(markup.indexOf('nickname'));
    expect(markup).toMatch(/<span class="sc-fname">secret_key \*<\/span><input[^>]*required=""[^>]*type="password"/);
    expect(markup).toMatch(/<span class="sc-fname">required_with_default<\/span><input(?![^>]*required="")[^>]*type="text"/);
    expect(markup).toContain('(Default: 1048576)');
  });

  it('keeps OAuth fields first and omits mapped and derived credentials', () => {
    const markup = renderConnectorForm({
      metadata: {displayName: 'OAuth Connector', description: 'OAuth connection'},
      spec: {
        provider: 'oauth-provider',
        configuration: {fields: [
          {name: 'endpoint', type: 'url', description: 'API endpoint.', required: false, default: 'https://api.example.com'},
        ]},
        auth: {
          type: 'oauth2',
          fields: [
            {name: 'access_token', type: 'secretString', description: 'Mapped token.', required: true},
            {name: 'primary_email', type: 'string', description: 'Derived identity.', required: true},
            {name: 'app_token', type: 'secretString', description: 'Additional token.', required: true},
          ],
          oauth2: {
            scopes: ['messages:write'],
            credentialMappings: [{credential: 'access_token', source: 'access_token'}],
            credentialDerivations: [{credential: 'primary_email', endpoint: 'https://api.example.com/me', source: 'email'}],
          },
        },
      },
    });

    expect(markup.indexOf('OAuth client ID *')).toBeLessThan(markup.indexOf('OAuth client secret *'));
    expect(markup.indexOf('OAuth client secret *')).toBeLessThan(markup.indexOf('app_token *'));
    expect(markup.indexOf('app_token *')).toBeLessThan(markup.indexOf('<legend class="sc-blockhead">Optional settings (1)</legend>'));
    expect(markup).not.toContain('access_token');
    expect(markup).not.toContain('primary_email');
  });

  it('lists requested OAuth scopes and omits the line when a provider defines none', () => {
    expect(requestedOAuthScopesText({scopes: ['chat:write', 'channels:read'], userScopes: ['channels:history']}))
      .toBe('Requested bot scopes: chat:write, channels:read; user scopes: channels:history');
    expect(requestedOAuthScopesText({scopes: ['read']})).toBe('Requested bot scopes: read');
    expect(requestedOAuthScopesText({scopes: [], userScopes: ['search:read']})).toBe('Requested user scopes: search:read');
    expect(requestedOAuthScopesText({scopes: []})).toBe('');
    expect(requestedOAuthScopesText({scopes: {fromSetting: 'scopes'}})).toBe('');
    expect(requestedOAuthScopesText({scopes: {fromSetting: 'scopes'}, userScopes: ['search:read']})).toBe('Requested user scopes: search:read');
    expect(requestedOAuthScopesText({})).toBe('');

    const markup = renderConnectorForm({
      metadata: {displayName: 'Scope-free OAuth', description: 'OAuth without scopes'},
      spec: {
        provider: 'scope-free', configuration: {fields: []},
        auth: {
          type: 'oauth2',
          fields: [{name: 'access_token', type: 'secretString', description: 'Mapped token.', required: true}],
          oauth2: {},
        },
      },
    });
    expect(markup).toContain('Authorize');
    expect(markup).not.toContain('Requested');
  });

  it('omits empty required and optional Connector field groups', () => {
    const requiredOnlyMarkup = renderConnectorForm({
      metadata: {displayName: 'Required', description: 'Required fields'},
      spec: {
        provider: 'required', configuration: {fields: []},
        auth: {type: 'apiKey', fields: [{name: 'api_key', type: 'secretString', description: 'API key.', required: true}]},
      },
    });
    const optionalOnlyMarkup = renderConnectorForm({
      metadata: {displayName: 'Optional', description: 'Optional fields'},
      spec: {
        provider: 'optional',
        configuration: {fields: [{name: 'endpoint', type: 'url', description: 'API endpoint.', required: false}]},
        auth: {type: 'apiKey', fields: []},
      },
    });

    expect(requiredOnlyMarkup).toContain('<legend class="sc-blockhead">Required</legend>');
    expect(requiredOnlyMarkup).not.toContain('Optional settings');
    expect(optionalOnlyMarkup).not.toContain('<legend class="sc-blockhead">Required</legend>');
    expect(optionalOnlyMarkup).toContain('<legend class="sc-blockhead">Optional settings (1)</legend>');
  });

  it('turns provider guidance URLs into explicit links', () => {
    expect(descriptionParts('Start at https://example.com/settings/keys and copy the value.')).toEqual([
      {text: 'Start at '},
      {text: 'https://example.com/settings/keys', url: 'https://example.com/settings/keys'},
      {text: ' and copy the value.'},
    ]);
  });

  it('keys named connections independently and maps visible status', () => {
    expect(connectionKey({ connectorId: 'gmail', connectionName: 'sender' } as never)).toBe('gmail\u0000sender');
    expect(connectionKey({ connectorId: 'gmail', connectionName: 'support' } as never)).toBe('gmail\u0000support');
    expect(studioState('Ready')).toBe('connected');
    expect(studioState('Expired')).toBe('expired');
    expect(studioState('Missing')).toBe('not_configured');
  });

  it('binds writes to CSRF and the current definition revision', () => {
    expect(connectorWriteHeaders({ csrfToken: 'csrf', definitionRevision: 'sha256:current' } as never)).toEqual({
      'Content-Type': 'application/json',
      'X-Dex-CSRF-Token': 'csrf',
      'X-Dex-Flow-Definition-Revision': 'sha256:current',
    });
  });

  it('accepts iframe commands only for the active connector session', () => {
    const session = { connectorId: 'gmail', sessionNonce: 'nonce' } as never;
    expect(isStudioCommand({
      type: 'connector.command', protocolVersion: '0.2.0', sessionNonce: 'nonce',
      connectorId: 'gmail', requestId: 'request', command: 'oauth.connect',
    }, session)).toBe(true);
    expect(isStudioCommand({
      type: 'connector.command', protocolVersion: '0.2.0', sessionNonce: 'other',
      connectorId: 'gmail', requestId: 'request', command: 'oauth.connect',
    }, session)).toBe(false);
    expect(isStudioCommand({
      type: 'connector.command', protocolVersion: '0.2.0', sessionNonce: 'nonce',
      connectorId: 'gmail', requestId: 'request', command: 'credential.read',
    }, session)).toBe(false);
  });

  it('accepts bounded iframe heights only from the active connector session', () => {
    const session = { connectorId: 'slack', sessionNonce: 'nonce' } as never;
    const resize = {
      type: 'connector.frame.resize', protocolVersion: '0.2.0', sessionNonce: 'nonce',
      connectorId: 'slack', height: 384,
    };
    expect(isStudioFrameResize(resize, session)).toBe(true);
    expect(isStudioFrameResize({...resize, sessionNonce: 'other'}, session)).toBe(false);
    expect(isStudioFrameResize({...resize, height: 4097}, session)).toBe(false);
  });

  it('sends the Dex Web theme, its tokens, and the Studio stylesheet as optional fields of the 0.2.0 ready message', () => {
    const session = {
      connectorId: 'slack', sessionNonce: 'nonce',
      manifest: { spec: { studio: { setup: { backendCapabilities: ['use.configuration.write'] } } } },
    } as never;
    const connection = { connectorId: 'slack', connectionName: 'workspace', status: 'Ready' } as never;
    const appearance = { theme: 'dark', themeTokens: { '--studio-cta': '#70eea9' }, stylesheet: connectorStudioStylesheet } as const;
    const ready = connectorHostReadyMessage(session, connection, { kind: 'connection' }, appearance);
    expect(ready).toEqual({
      type: 'connector.host.ready', protocolVersion: '0.2.0', sessionNonce: 'nonce', connectorId: 'slack',
      capabilities: ['use.configuration.write'],
      connection: { state: 'connected', grantedScopes: [], detail: 'Ready', authMethodIds: [], configuration: {}, storedCredentialFields: [] },
      target: { kind: 'connection' },
      theme: 'dark',
      themeTokens: { '--studio-cta': '#70eea9' },
      stylesheet: connectorStudioStylesheet,
    });
    expect(ready.stylesheet).toContain('.studio-button {');
    expect(structuredClone(ready)).toEqual(ready);
  });

  it('advertises only host capabilities that are implemented', () => {
    expect(studioHostCapabilities({
      manifest: { spec: { studio: {
        setup: { backendCapabilities: ['oauth.connection.manage', 'use.configuration.write', 'provider.resources-list', 'credential.read'] },
        commands: [{id: 'listResources', capability: 'provider.resources-list'}],
      } } },
    } as never)).toEqual(['oauth.connection.manage', 'use.configuration.write', 'provider.resources-list']);
  });

  it('lets a release that declares connection.write own the whole connection setup', () => {
    const setupManifest = {spec: {provider: 'llm', studio: {
      setup: {backendCapabilities: ['use.configuration.write', 'llm.models-list', 'connection.write']},
      commands: [{id: 'listAnthropicModels', capability: 'llm.models-list'}],
    }}};
    const setupSession = {connectorId: 'llm', sessionNonce: 'nonce', entrypointUrl: '/ui/index.html', manifest: setupManifest} as never;
    expect(isConnectionSetupStudio(setupSession)).toBe(true);
    expect(studioHostCapabilities(setupSession)).toContain('connection.write');
    expect(isConnectionSetupStudio({...(setupSession as object), entrypointUrl: undefined} as never), 'no UI artifact').toBe(false);
    expect(isConnectionSetupStudio(llmSession as never), 'a release without connection.write keeps the host form').toBe(false);
    expect(isStudioCommand({
      type: 'connector.command', protocolVersion: '0.2.0', sessionNonce: 'nonce', connectorId: 'llm', requestId: 'request',
      command: 'connection.save', input: {configuration: {provider: 'anthropic'}, credentials: {api_key: 'typed'}},
    }, setupSession)).toBe(true);
  });

  it('forwards typed credentials only for a connection.write setup', () => {
    const setupSession = {manifest: {spec: {studio: {setup: {backendCapabilities: ['llm.models-list', 'connection.write']},
      commands: [{id: 'listAnthropicModels', capability: 'llm.models-list'}]}}}} as never;
    expect(providerCommandRequestBody(setupSession, {commandId: 'listAnthropicModels', parameters: {limit: '1000'}, credentials: {api_key: 'typed'}}))
      .toEqual({parameters: {limit: '1000'}, credentials: {api_key: 'typed'}});
    expect(providerCommandRequestBody(llmSession as never, {commandId: 'listAnthropicModels', parameters: {}})).toEqual({parameters: {}});
    expect(() => providerCommandRequestBody(llmSession as never, {commandId: 'listAnthropicModels', parameters: {}, credentials: {api_key: 'typed'}}))
      .toThrow('cannot send credentials');
  });

  it('saves the setup configuration, typed credentials, and kept credentials in one request', () => {
    const connection = {
      connectorId: 'llm', connectionName: 'gtm-llm', status: 'Ready', modulePath: 'example.com/llm', moduleVersion: 'v0.21.0',
      storedCredentialFields: ['api_key'],
    } as never;
    expect(connectionSetupSaveRequestBody(connection, {spec: {provider: 'llm'}} as never, {
      configuration: {provider: 'anthropic', model: 'claude-opus-5-5'}, credentials: {}, keepCredentialFields: ['api_key'],
    })).toEqual({
      modulePath: 'example.com/llm', moduleVersion: 'v0.21.0', provider: 'llm', authMethodId: '',
      configuration: {provider: 'anthropic', model: 'claude-opus-5-5'}, credentials: {}, keepCredentialFields: ['api_key'],
      credentialExpiresAt: null,
    });
    expect(() => connectionSetupSaveRequestBody(connection, {spec: {provider: 'llm'}} as never, {configuration: {}, credentials: {api_key: 7}}))
      .toThrow('credentials must contain string values');
    expect(() => connectionSetupSaveRequestBody(connection, {spec: {provider: 'llm'}} as never, {credentials: {}}))
      .toThrow('configuration is required');
  });

  it('reports provider command failures only to the Studio frame and save failures to both', () => {
    const connection = { connectorId: 'llm', connectionName: 'default' } as never;
    const session = { connectorId: 'llm', connectionName: 'default', sessionNonce: 'nonce' } as never;
    const frameResult = (requestId: string, message: string) => ({
      type: 'connector.command.result', protocolVersion: '0.2.0', sessionNonce: 'nonce',
      connectorId: 'llm', requestId, ok: false, error: { code: 'COMMAND_FAILED', message },
    });
    const providerCommandFailures = [
      new DexAPIError('Connector provider command failed', 502, undefined, 'CONNECTOR_PROVIDER_COMMAND_FAILED'),
      new DexAPIError('Connector UI session is missing or expired', 404, undefined, 'CONNECTOR_UI_SESSION_NOT_FOUND'),
      new DexAPIError('Connector Studio command parameters are invalid', 400, undefined, 'CONNECTOR_STUDIO_COMMAND_INVALID'),
      new Error('parameters must contain string values'),
      new TypeError('Failed to fetch'),
    ];
    for (const commandError of providerCommandFailures) {
      const outcome = studioCommandFailureOutcome(connection, session, { requestId: 'list', command: 'provider.command.execute' }, commandError);
      expect(outcome.pageBannerMessage).toBeUndefined();
      expect(outcome.frameResult).toEqual(frameResult('list', commandError.message));
    }
    const staleRevision = new DexAPIError('Flow Definition revision is stale', 409, undefined, 'FLOW_DEFINITION_REVISION_CONFLICT');
    expect(studioCommandFailureOutcome(connection, session, { requestId: 'save', command: 'use.configuration.save' }, staleRevision)).toEqual({
      pageBannerMessage: 'Flow Definition revision is stale',
      frameResult: frameResult('save', 'Flow Definition revision is stale'),
    });
    expect(studioCommandFailureOutcome(connection, session, { requestId: 'save', command: 'use.configuration.save' }, 'offline')).toEqual({
      pageBannerMessage: 'Connector request failed',
      frameResult: frameResult('save', 'Connector request failed'),
    });
  });

  it('orders authorization before configurable operations and triggers', () => {
    const connection = {
      connectionName: 'workspace',
      status: 'Ready',
      uses: [
        {flowName: 'ReadThread', stepId: 'read', operationId: 'listMessages', operationKind: 'query', configured: false, configurationUI: {units: []}},
        {flowName: 'PostCompletion', stepId: 'post', operationId: 'postReply', operationKind: 'mutation', configured: false, configurationUI: {units: [{id: 'reply'}]}},
      ],
      triggerUses: [
        {flowName: 'Approval', bindingName: 'reply', triggerName: 'threadReplyCreated', configured: true, configurationUI: {units: [{id: 'channel'}]}},
      ],
    } as never;
    expect(connectorSetupTabs(connection).map((tab) => [tab.kind, tab.label, tab.configured])).toEqual([
      ['authorize', 'Authorize', true],
      ['operation', 'postReply', false],
      ['trigger', 'threadReplyCreated', true],
    ]);
    expect(initialConnectorSetupTabKey(connection)).toBe('operation:PostCompletion:post');
  });

  it('keeps configuration tabs behind authorization and opens the first tab when all are configured', () => {
    const missing = {
      connectionName: 'workspace', status: 'Missing',
      uses: [{flowName: 'Flow', stepId: 'step', operationId: 'send', operationKind: 'mutation', configured: false, configurationUI: {units: [{id: 'message'}]}}],
      triggerUses: [],
    };
    expect(initialConnectorSetupTabKey(missing as never)).toBe('authorize');
    const ready = {
      ...missing, status: 'Ready', uses: missing.uses.map((use: {configured: boolean}) => ({...use, configured: true})),
    };
    expect(initialConnectorSetupTabKey(ready as never)).toBe('operation:Flow:step');
  });
});

const modelField = {
  name: 'model', type: 'string', description: 'Default model.', required: false,
  studioUnit: {unit: 'modelPicker', port: 'model'},
};

const llmManifest = {
  metadata: {displayName: 'LLM', description: 'Several model providers'},
  spec: {
    provider: 'llm',
    configuration: {fields: [
      modelField,
      {name: 'timeout', type: 'duration', description: 'Request timeout.', required: false},
      {name: 'maxOutputTokens', type: 'integer', description: 'Output limit.', required: false},
    ]},
    auth: {
      fields: [],
      selection: 'multiple',
      methodLabel: 'Provider',
      methods: [
        {id: 'openai', displayName: 'OpenAI', description: 'OpenAI API key.', type: 'apiKey', fields: [
          {name: 'openai_api_key', type: 'secretString', description: 'OpenAI key.', required: true},
        ]},
        {id: 'anthropic', displayName: 'Claude', description: 'Anthropic API key.', type: 'apiKey', fields: [
          {name: 'anthropic_api_key', type: 'secretString', description: 'Anthropic key.', required: true},
        ], configuration: {fields: [
          {name: 'anthropicWorkspaceId', type: 'string', description: 'Claude workspace.', required: false},
        ]}},
        {id: 'gemini', displayName: 'Gemini', description: 'Gemini API key.', type: 'apiKey', fields: [
          {name: 'gemini_api_key', type: 'secretString', description: 'Gemini key.', required: true},
        ]},
      ],
    },
    studio: {
      setup: {backendCapabilities: ['use.configuration.write']},
      units: [{id: 'modelPicker', description: 'Pick a model.', outputs: [{name: 'model', type: 'string'}]}],
    },
  },
};

const llmSession = {connectorId: 'llm', connectionName: 'default', sessionNonce: 'nonce', entrypointUrl: '/ui/index.html', manifest: llmManifest};

describe('Connections with several authentication methods', () => {
  it('adds providers in order without duplicates and removes them', () => {
    expect(addConnectionAuthMethod(['openai'], 'anthropic')).toEqual(['openai', 'anthropic']);
    expect(addConnectionAuthMethod(['openai', 'anthropic'], 'openai')).toEqual(['openai', 'anthropic']);
    expect(removeConnectionAuthMethod(['openai', 'anthropic'], 'openai')).toEqual(['anthropic']);
    expect(addableAuthMethods(llmManifest as never, ['anthropic']).map((method) => method.id)).toEqual(['openai', 'gemini']);
    expect(initialConnectionAuthMethodIds(llmManifest as never, {authMethodIds: ['anthropic', 'retired']} as never)).toEqual(['anthropic']);
    expect(initialConnectionAuthMethodIds(llmManifest as never, {} as never)).toEqual([]);
    const withDefault = {...llmManifest, spec: {...llmManifest.spec, auth: {...llmManifest.spec.auth, defaultMethod: 'anthropic'}}};
    expect(initialConnectionAuthMethodIds(withDefault as never, {} as never)).toEqual(['anthropic']);
    expect(initialConnectionAuthMethodIds(withDefault as never, {authMethodIds: ['openai']} as never)).toEqual(['openai']);
  });

  it('pre-adds the default method card to a never-saved connection', () => {
    const withDefault = {...llmManifest, spec: {...llmManifest.spec, auth: {...llmManifest.spec.auth, defaultMethod: 'openai'}}};
    const markup = renderConnectorForm(withDefault, {status: 'Missing'}, llmSession);
    expect(markup).toContain('aria-label="Remove OpenAI"');
    expect(markup).toMatch(/<span class="sc-fname">openai_api_key \*<\/span><input autoComplete="off" required="" type="password"/);
    expect(markup).not.toContain('Add at least one provider');
  });

  it('names the section and menu from methodLabel', () => {
    expect(connectorAuthMethodLabels('Provider')).toEqual({
      plural: 'Providers', add: 'Add provider', empty: 'Add at least one provider to save this connection.',
    });
    expect(connectorAuthMethodLabels(undefined)).toEqual({
      plural: 'Authentication methods', add: 'Add authentication method',
      empty: 'Add at least one authentication method to save this connection.',
    });
    expect(connectorAuthMethodLabels('API key').add).toBe('Add API key');
  });

  it('renders one card per added provider with its fields, a Remove button, and the Add provider menu', () => {
    const markup = renderConnectorForm(llmManifest, {authMethodIds: ['anthropic', 'openai'], status: 'Missing'}, llmSession);
    expect(markup).toContain('<section aria-label="Providers" class="sc-block connector-auth-method-cards">');
    expect(markup).toContain('<h4 class="sc-blockhead">Providers</h4>');
    expect(markup).toMatch(/aria-haspopup="menu"[^>]*>Add provider<\/button>/);
    expect(markup.indexOf('<b>Claude</b>')).toBeLessThan(markup.indexOf('<b>OpenAI</b>'));
    expect(markup).toContain('aria-label="Remove Claude"');
    expect(markup).toContain('aria-label="Remove OpenAI"');
    expect(markup.indexOf('anthropic_api_key *')).toBeLessThan(markup.indexOf('anthropicWorkspaceId'));
    expect(markup.indexOf('anthropicWorkspaceId')).toBeLessThan(markup.indexOf('<b>OpenAI</b>'));
    expect(markup).not.toContain('gemini_api_key');
    expect(markup).not.toContain('name="connector-auth-method"');
    expect(markup).toMatch(/<button class="v2-primary" type="submit">Save local credentials<\/button>/);
  });

  it('requires at least one provider before saving', () => {
    const markup = renderConnectorForm(llmManifest, {status: 'Missing'}, llmSession);
    expect(markup).toContain('Add at least one provider to save this connection.');
    expect(markup).toMatch(/<button class="v2-primary" disabled="" type="submit">/);
  });

  it('sends the added providers, their typed keys, and keeps blank stored keys of added providers only', () => {
    const connection = {
      connectorId: 'llm', connectionName: 'default', modulePath: 'github.com/superdurable/dex-connectors-library/connectors/llm', moduleVersion: 'v0.2.0',
      status: 'Ready', authMethodIds: ['anthropic', 'openai'], storedCredentialFields: ['anthropic_api_key', 'openai_api_key'],
      configuration: {anthropicWorkspaceId: 'ws_1', timeout: '30s'},
    } as never;
    expect(connectionWriteRequestBody(connection, llmManifest as never, ['anthropic', 'gemini'], {
      'configuration:anthropicWorkspaceId': 'ws_1', 'configuration:timeout': '30s',
      'credential:gemini_api_key': 'gemini-secret', 'credential:openai_api_key': '',
    })).toEqual({
      modulePath: 'github.com/superdurable/dex-connectors-library/connectors/llm', moduleVersion: 'v0.2.0', provider: 'llm',
      authMethodIds: ['anthropic', 'gemini'],
      configuration: {anthropicWorkspaceId: 'ws_1', timeout: '30s'},
      credentials: {gemini_api_key: 'gemini-secret'},
      keepCredentialFields: ['anthropic_api_key'],
      credentialExpiresAt: null,
    });
    const removedClaude = connectionWriteRequestBody(connection, llmManifest as never, ['openai'], {
      'configuration:anthropicWorkspaceId': 'ws_1', 'credential:openai_api_key': 'rotated',
    });
    expect(removedClaude.configuration).toEqual({});
    expect(removedClaude.credentials).toEqual({openai_api_key: 'rotated'});
    expect(removedClaude.keepCredentialFields).toEqual([]);
  });

  it('shows stored secrets as optional inputs that keep the stored value', () => {
    const stripeManifest = {
      metadata: {displayName: 'Stripe', description: 'Stripe connection'},
      spec: {
        provider: 'stripe', configuration: {fields: []},
        auth: {type: 'apiKey', fields: [
          {name: 'secret_key', type: 'secretString', description: 'Stripe key.', required: true},
          {name: 'webhook_secret', type: 'secretString', description: 'Webhook secret.', required: true},
        ]},
      },
    };
    const markup = renderConnectorForm(stripeManifest, {status: 'Ready', storedCredentialFields: ['secret_key']});
    expect(markup).toMatch(new RegExp(`<span class="sc-fname">secret_key</span><input autoComplete="off" placeholder="${storedCredentialPlaceholder}" type="password"`));
    expect(markup).toMatch(/<span class="sc-fname">webhook_secret \*<\/span><input autoComplete="off" required="" type="password"/);
    const body = connectionWriteRequestBody(
      {connectorId: 'stripe', connectionName: 'payments', storedCredentialFields: ['secret_key']} as never,
      stripeManifest as never, [''], {'credential:webhook_secret': 'whsec'},
    );
    expect(body).toMatchObject({authMethodId: '', credentials: {webhook_secret: 'whsec'}, keepCredentialFields: ['secret_key']});
  });

  it('prefills stored non-secret configuration and resends unchanged values with their stored type', () => {
    const connection = {
      connectorId: 'llm', connectionName: 'default', status: 'Missing', authMethodIds: ['anthropic'],
      configuration: {timeout: '30s', maxOutputTokens: 1024, anthropicWorkspaceId: 'ws_1'},
    } as never;
    expect(connectionFormInitialValues(llmManifest as never, connection)).toEqual({
      'configuration:timeout': '30s', 'configuration:maxOutputTokens': '1024', 'configuration:anthropicWorkspaceId': 'ws_1',
    });
    const markup = renderConnectorForm(llmManifest, connection, llmSession);
    expect(markup).toMatch(/<span class="sc-fname">timeout<\/span><input autoComplete="off" type="text"[^>]* value="30s"/);
    expect(markup).toMatch(/<span class="sc-fname">anthropicWorkspaceId<\/span><input autoComplete="off" type="text"[^>]* value="ws_1"/);
    const body = connectionWriteRequestBody(connection, llmManifest as never, ['anthropic'], connectionFormInitialValues(llmManifest as never, connection));
    expect(body.configuration).toEqual({timeout: '30s', maxOutputTokens: 1024, anthropicWorkspaceId: 'ws_1'});
  });

  it('mounts the studioUnit field frame only once the connection is Ready', () => {
    const ready = {connectorId: 'llm', connectionName: 'default', status: 'Ready'} as never;
    const missing = {connectorId: 'llm', connectionName: 'default', status: 'Missing'} as never;
    expect(connectionFieldPresentation(modelField as never, ready, llmSession as never)).toBe('studioFrame');
    expect(connectionFieldPresentation(modelField as never, missing, llmSession as never)).toBe('studioNote');
    expect(connectionFieldPresentation({...modelField, required: true} as never, missing, llmSession as never)).toBe('input');
    expect(connectionFieldPresentation(modelField as never, ready, {...llmSession, entrypointUrl: undefined} as never)).toBe('input');
    expect(connectionFieldPresentation({...modelField, studioUnit: {unit: 'undeclared', port: 'model'}} as never, ready, llmSession as never)).toBe('input');

    const missingMarkup = renderConnectorForm(llmManifest, {status: 'Missing', authMethodIds: ['openai']}, llmSession);
    expect(missingMarkup).toContain('Save the connection to choose <code>model</code>.');
    expect(missingMarkup).not.toContain('<iframe');
    const readyMarkup = renderConnectorForm(llmManifest, {status: 'Ready', authMethodIds: ['openai']}, llmSession);
    expect(readyMarkup).toContain('<iframe class="connector-studio" sandbox="allow-scripts" src="/ui/index.html"');
    expect(readyMarkup).toContain('title="test-connector Connector model setting"');
    expect(readyMarkup).toContain('data-surface="connectionField"');
    expect(readyMarkup).not.toContain('Save the connection to choose');

    const body = connectionWriteRequestBody(
      {connectorId: 'llm', connectionName: 'default', status: 'Ready', configuration: {model: 'anthropic/claude-sonnet-5'}} as never,
      llmManifest as never, ['anthropic'], {'configuration:model': 'stale form text'}, new Set(['model']),
    );
    expect(body.configuration).toEqual({model: 'anthropic/claude-sonnet-5'});
  });

  it('targets the connection field frame and saves only that field, keeping every stored credential', () => {
    const connection = {
      connectorId: 'llm', connectionName: 'default', modulePath: 'github.com/superdurable/dex-connectors-library/connectors/llm', moduleVersion: 'v0.2.0',
      status: 'Ready', authMethodIds: ['anthropic', 'openai'],
      storedCredentialFields: ['anthropic_api_key', 'openai_api_key', 'retired_key'],
      configuration: {model: 'anthropic/claude-sonnet-5', timeout: '30s'},
    } as never;
    const target = connectionFieldStudioTarget(modelField as never, connection);
    expect(target).toEqual({
      kind: 'connection', unitId: 'modelPicker',
      bindings: [{port: 'model', jsonPointer: '/model'}],
      value: {model: 'anthropic/claude-sonnet-5'},
    });
    expect(connectionFieldStudioTarget(modelField as never, {} as never)).toMatchObject({value: {}});
    if (target.kind !== 'connection' || !target.bindings) throw new Error('connection target expected');
    const configuration = mergeUnitValue({model: 'anthropic/claude-sonnet-5', timeout: '30s'}, target.bindings, {model: 'openai/gpt-5'});
    expect(configuration).toEqual({model: 'openai/gpt-5', timeout: '30s'});
    expect(() => mergeUnitValue({}, target.bindings ?? [], {provider: 'openai'})).toThrow('undeclared port provider');
    expect(connectionConfigurationSaveRequestBody(connection, llmManifest as never, configuration)).toEqual({
      modulePath: 'github.com/superdurable/dex-connectors-library/connectors/llm', moduleVersion: 'v0.2.0', provider: 'llm',
      authMethodIds: ['anthropic', 'openai'],
      configuration: {model: 'openai/gpt-5', timeout: '30s'},
      credentials: {},
      keepCredentialFields: ['anthropic_api_key', 'openai_api_key'],
      credentialExpiresAt: null,
    });
    const singleManifest = {spec: {provider: 'openai', configuration: {fields: [modelField]}, auth: {type: 'apiKey', fields: [
      {name: 'api_key', type: 'secretString', description: '', required: true},
    ]}}};
    expect(connectionConfigurationSaveRequestBody(
      {connectorId: 'openai', connectionName: 'default', storedCredentialFields: ['api_key']} as never, singleManifest as never, {model: 'gpt-5'},
    )).toMatchObject({authMethodId: '', configuration: {model: 'gpt-5'}, credentials: {}, keepCredentialFields: ['api_key']});
  });

  it('gives every Studio frame the connection methods and non-secret configuration', () => {
    expect(connectionAuthMethodIds({authMethodIds: ['anthropic', 'openai']} as never)).toEqual(['anthropic', 'openai']);
    expect(connectionAuthMethodIds({authMethodId: 'googleOAuth'} as never)).toEqual(['googleOAuth']);
    expect(connectionAuthMethodIds({} as never)).toEqual([]);
    const connection = {
      connectorId: 'llm', connectionName: 'default', status: 'Ready', authMethodIds: ['anthropic', 'openai'],
      configuration: {model: 'anthropic/claude-sonnet-5'},
    } as never;
    const appearance = {theme: 'light', themeTokens: {}, stylesheet: ''} as const;
    const stepTarget = {
      kind: 'configurationUnit', scope: {kind: 'operation', operationId: 'generateText', flowType: 'Flow', stepType: 'Step'},
      instanceId: 'model', unitId: 'modelPicker', label: 'Model', required: false,
      bindings: [{port: 'model', jsonPointer: '/model'}], value: {},
    } as never;
    for (const target of [stepTarget, connectionFieldStudioTarget(modelField as never, connection)]) {
      expect(connectorHostReadyMessage(llmSession as never, connection, target, appearance).connection).toEqual({
        state: 'connected', grantedScopes: [], detail: 'Ready',
        authMethodIds: ['anthropic', 'openai'], configuration: {model: 'anthropic/claude-sonnet-5'}, storedCredentialFields: [],
      });
    }
  });
});

describe('Connectors page presentation', () => {
  it('names a connector by its release displayName and falls back to the Connector ID', () => {
    expect(connectorDisplayName({connectorId: 'llm', displayName: 'LLM'})).toBe('LLM');
    expect(connectorDisplayName({connectorId: 'gemini', displayName: '  '})).toBe('gemini');
    expect(connectorDisplayName({connectorId: 'slack'})).toBe('slack');
    expect(CONNECTORS_COPY.connectionLabel('llm')).toBe('Connection llm');
    expect(CONNECTORS_COPY.connectionLabel('')).toBe('Unnamed connection');
    expect(connectionReleaseText({localOverride: true, moduleVersion: 'v0.2.0'})).toBe('Local override · v0.2.0');
    expect(connectionReleaseText({moduleVersion: ''})).toBe('No exact release');
  });

  it('titles the setup and every Studio frame with the displayName', () => {
    const markup = renderConnectorForm(
      {...llmManifest, metadata: {...llmManifest.metadata, displayName: ''}},
      {connectorId: 'llm', displayName: 'LLM', status: 'Ready', authMethodIds: ['openai']},
      llmSession,
    );
    expect(markup).toContain('<h3 class="sc-blockhead">LLM setup</h3>');
    expect(markup).toContain('title="LLM Connector model setting"');
  });

  it('paints Studio frames in the Dex Web theme', () => {
    const connection = {connectorId: 'llm', displayName: 'LLM', status: 'Ready', authMethodIds: ['openai']};
    expect(renderConnectorForm(llmManifest, connection, llmSession)).toMatch(/<iframe [^>]*style="color-scheme:light"/);
    expect(renderConnectorForm(llmManifest, connection, llmSession, 'dark')).toMatch(/<iframe [^>]*style="color-scheme:dark"/);
  });

  it('shows the local store with copy buttons', () => {
    const local = renderToStaticMarkup(createElement(ConnectorStoreZone, {catalog: {
      directory: '/home/dev/.dex/connectors', filePath: '/home/dev/.dex/connectors/connections.json',
      useConfigurationsFilePath: '/home/dev/.dex/connectors/use-configurations.json',
      launchCommand: "DEX_CONNECTOR_CONFIG_FILE='/home/dev/.dex/connectors/connections.json' <your-app-command>",
    }}));
    expect(local).toContain('<section aria-label="Local store" class="sc-block connector-store-zone" data-zone="store"><h3 class="sc-blockhead">Local store</h3>');
    expect(local).toContain('<code>/home/dev/.dex/connectors</code>');
    expect(local).not.toContain('aria-label="Copy Store directory"');
    for (const label of ['Connection file', 'Flow configuration file', 'Start your app']) {
      expect(local).toContain(`aria-label="Copy ${label}" class="v2-ghost connector-copy" type="button">Copy</button>`);
    }
  });

  it('shows each connection status as a chip keyed by status', () => {
    expect(renderToStaticMarkup(createElement(ConnectionStatusChip, {status: 'Expired'})))
      .toBe('<span class="connector-status" data-status="expired">Expired</span>');
  });
});

function renderConnectorForm(
  manifest: unknown,
  connection: Record<string, unknown> = {},
  session: Record<string, unknown> = {},
  theme: 'light' | 'dark' = 'light',
): string {
  return renderToStaticMarkup(createElement(ConnectorStudioFrameThemeContext.Provider, {value: theme}, createElement(ConnectorForm, {
    catalog: {} as never,
    connection: {connectorId: 'test-connector', ...connection} as never,
    session: {...session, manifest} as never,
    onConfigured: async () => undefined,
    onError: () => undefined,
  })));
}
