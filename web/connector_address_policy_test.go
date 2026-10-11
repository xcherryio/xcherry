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
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"strings"
	"sync"
	"testing"
)

// Keep the address tables identical to the connectors library's address_policy_test.go.

// namedNetwork resolves host names to test addresses and dials the test server instead.
type namedNetwork struct {
	target    string
	mu        sync.Mutex
	addresses map[string][]netip.Addr
	dialed    []string
}

func newNamedNetwork(target string) *namedNetwork {
	return &namedNetwork{target: target, addresses: map[string][]netip.Addr{}}
}

func (network *namedNetwork) resolveTo(host string, addresses ...string) {
	network.mu.Lock()
	defer network.mu.Unlock()
	network.addresses[host] = nil
	for _, address := range addresses {
		network.addresses[host] = append(network.addresses[host], netip.MustParseAddr(address))
	}
}

func (network *namedNetwork) policy() connectorAddressPolicy {
	return connectorAddressPolicy{
		lookupAddresses: func(_ context.Context, host string) ([]netip.Addr, error) {
			network.mu.Lock()
			defer network.mu.Unlock()
			if address, err := netip.ParseAddr(host); err == nil {
				return []netip.Addr{address}, nil
			}
			return append([]netip.Addr(nil), network.addresses[host]...), nil
		},
		dial: func(ctx context.Context, networkName string, address string) (net.Conn, error) {
			network.mu.Lock()
			network.dialed = append(network.dialed, address)
			network.mu.Unlock()
			return (&net.Dialer{}).DialContext(ctx, networkName, network.target)
		},
	}
}

func (network *namedNetwork) dialedAddresses() []string {
	network.mu.Lock()
	defer network.mu.Unlock()
	return append([]string(nil), network.dialed...)
}

func serverPort(t *testing.T, server *httptest.Server) string {
	t.Helper()
	parsed, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	return parsed.Port()
}

// useConnectorAddressPolicyForTest makes the setup resolve and dial through policy, with base supplying TLS settings.
func useConnectorAddressPolicyForTest(setup *connectorSetup, base *http.Transport, policy connectorAddressPolicy) {
	setup.endpointPolicy = policy
	setup.settingHTTPClient = newConnectorAddressPolicyClient(base, policy)
}

type connectorAddressPolicyTestOutcome struct {
	start    *httptest.ResponseRecorder
	callback *httptest.ResponseRecorder
}

// runConnectorOAuthAddressPolicyTest starts OAuth with the settings overridden and, when start succeeds, completes the callback.
func runConnectorOAuthAddressPolicyTest(
	t *testing.T, setup *connectorSetup, provider *connectorOAuthSettingsTestProvider, overrides map[string]string,
) connectorAddressPolicyTestOutcome {
	t.Helper()
	outcome := connectorAddressPolicyTestOutcome{start: startConnectorOAuthSettingsTest(t, setup, provider.configuration(overrides))}
	if outcome.start.Code == http.StatusOK {
		outcome.callback = callbackConnectorOAuthSettingsTest(t, setup, outcome.start)
	}
	return outcome
}

// requireConnectorOAuthSettingRefusal checks a 400 CONNECTOR_OAUTH_SETTING_INVALID that names settingName and the rule.
func requireConnectorOAuthSettingRefusal(t *testing.T, recorder *httptest.ResponseRecorder, settingName string, reason string) {
	t.Helper()
	body := recorder.Body.String()
	if recorder.Code != http.StatusBadRequest || !strings.Contains(body, "CONNECTOR_OAUTH_SETTING_INVALID") ||
		!strings.Contains(body, `setting \"`+settingName+`\"`) || !strings.Contains(body, "will not connect") || !strings.Contains(body, reason) {
		t.Fatalf("response = %d: %s, want a refusal of %s for %q", recorder.Code, body, settingName, reason)
	}
}

func TestConnectorOAuthPlainHTTPConnectsOnlyWhenEveryResolvedAddressIsLoopbackOrPrivate(t *testing.T) {
	for _, test := range []struct {
		name        string
		addresses   []string
		isPermitted bool
	}{
		{"IPv4 loopback", []string{"127.0.0.1"}, true},
		{"IPv6 loopback", []string{"::1"}, true},
		{"10.0.0.0/8", []string{"10.1.2.3"}, true},
		{"172.16.0.0/12", []string{"172.16.0.5"}, true},
		{"192.168.0.0/16", []string{"192.168.1.1"}, true},
		{"IPv6 unique local", []string{"fd12:3456::1"}, true},
		{"several private addresses", []string{"10.0.0.1", "192.168.0.2", "::1"}, true},
		{"public IPv4", []string{"8.8.8.8"}, false},
		{"public IPv6", []string{"2001:4860:4860::8888"}, false},
		{"carrier-grade NAT is not private", []string{"100.64.0.1"}, false},
		{"one public address among private ones", []string{"10.0.0.1", "8.8.8.8"}, false},
		{"cloud metadata address", []string{"169.254.169.254"}, false},
		{"another link-local IPv4 address", []string{"169.254.0.1"}, false},
		{"IPv6 link-local", []string{"fe80::1"}, false},
		{"IPv4-mapped metadata address", []string{"::ffff:169.254.169.254"}, false},
		{"IPv6 metadata address", []string{"fd00:ec2::254"}, false},
		{"loopback together with the metadata address", []string{"127.0.0.1", "169.254.169.254"}, false},
		{"unspecified address", []string{"0.0.0.0"}, false},
		{"multicast address", []string{"224.0.0.1"}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			provider := newConnectorOAuthSettingsTestProvider(t, connectorOAuthSettingsTestTokenResponse)
			network := newNamedNetwork(provider.Listener.Addr().String())
			network.resolveTo("api.internal.test", test.addresses...)
			setup := newConnectorOAuthSettingsTestSetup(t)
			useConnectorAddressPolicyForTest(setup, nil, network.policy())
			host := "http://api.internal.test:" + serverPort(t, provider.Server)

			outcome := runConnectorOAuthAddressPolicyTest(t, setup, provider, map[string]string{
				"tokenEndpoint": `"` + host + `/token"`, "userInfoEndpoint": `"` + host + `/userinfo"`,
			})

			if test.isPermitted {
				if outcome.callback == nil || outcome.callback.Code != http.StatusSeeOther {
					t.Fatalf("OAuth start = %d: %s; callback = %+v", outcome.start.Code, outcome.start.Body.String(), outcome.callback)
				}
				want := net.JoinHostPort(netip.MustParseAddr(test.addresses[0]).Unmap().String(), serverPort(t, provider.Server))
				// The token exchange and the identity derivation each dial the address they checked, not the name.
				if dialed := network.dialedAddresses(); len(dialed) != 2 || dialed[0] != want || dialed[1] != want {
					t.Fatalf("dialed %v, want two connections to %s", dialed, want)
				}
				return
			}
			requireConnectorOAuthSettingRefusal(t, outcome.start, "tokenEndpoint", "")
			if dialed := network.dialedAddresses(); len(dialed) != 0 {
				t.Fatalf("a refused address was dialed: %v", dialed)
			}
			if codes := provider.redeemedCodes(); len(codes) != 0 {
				t.Fatalf("the provider received %d token requests", len(codes))
			}
		})
	}
}

func TestConnectorOAuthHTTPSAcceptsPublicAddressesButNeverLinkLocalOnes(t *testing.T) {
	for _, test := range []struct {
		name        string
		addresses   []string
		isPermitted bool
	}{
		{"public IPv4", []string{"8.8.8.8"}, true},
		{"private IPv4", []string{"10.1.2.3"}, true},
		{"loopback", []string{"127.0.0.1"}, true},
		{"cloud metadata address", []string{"169.254.169.254"}, false},
		{"IPv6 link-local", []string{"fe80::1"}, false},
		{"IPv4-mapped metadata address", []string{"::ffff:169.254.169.254"}, false},
		{"IPv6 metadata address", []string{"fd00:ec2::254"}, false},
		{"public address together with the metadata address", []string{"8.8.8.8", "169.254.169.254"}, false},
		{"unspecified address", []string{"::"}, false},
	} {
		expectedReason := "link-local"
		if test.name == "unspecified address" {
			expectedReason = "only unicast"
		}
		t.Run(test.name, func(t *testing.T) {
			// The test certificate of httptest is valid for example.com.
			provider := newConnectorOAuthSettingsTestTLSProvider(t, connectorOAuthSettingsTestTokenResponse)
			network := newNamedNetwork(provider.Listener.Addr().String())
			network.resolveTo("example.com", test.addresses...)
			setup := newConnectorOAuthSettingsTestSetup(t)
			useConnectorAddressPolicyForTest(setup, provider.Client().Transport.(*http.Transport), network.policy())
			host := "https://example.com:" + serverPort(t, provider.Server)

			outcome := runConnectorOAuthAddressPolicyTest(t, setup, provider, map[string]string{
				"tokenEndpoint": `"` + host + `/token"`, "userInfoEndpoint": `"` + host + `/userinfo"`,
			})

			if test.isPermitted {
				if outcome.callback == nil || outcome.callback.Code != http.StatusSeeOther {
					t.Fatalf("OAuth start = %d: %s; callback = %+v", outcome.start.Code, outcome.start.Body.String(), outcome.callback)
				}
				if dialed := network.dialedAddresses(); len(dialed) != 2 {
					t.Fatalf("dialed %v, want the token exchange and the identity derivation", dialed)
				}
				return
			}
			requireConnectorOAuthSettingRefusal(t, outcome.start, "tokenEndpoint", expectedReason)
			if dialed := network.dialedAddresses(); len(dialed) != 0 {
				t.Fatalf("a refused address was dialed: %v", dialed)
			}
		})
	}
}

func TestConnectorOAuthIdentityEndpointFollowsTheSameAddressPolicy(t *testing.T) {
	for _, test := range []struct {
		name        string
		address     string
		isPermitted bool
	}{
		{"private address", "10.0.0.7", true},
		{"public address", "8.8.8.8", false},
		{"cloud metadata address", "169.254.169.254", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			provider := newConnectorOAuthSettingsTestProvider(t, connectorOAuthSettingsTestTokenResponse)
			network := newNamedNetwork(provider.Listener.Addr().String())
			network.resolveTo("auth.internal.test", test.address)
			setup := newConnectorOAuthSettingsTestSetup(t)
			useConnectorAddressPolicyForTest(setup, nil, network.policy())

			outcome := runConnectorOAuthAddressPolicyTest(t, setup, provider, map[string]string{
				"userInfoEndpoint": `"http://auth.internal.test:` + serverPort(t, provider.Server) + `/userinfo"`,
			})

			if test.isPermitted {
				if outcome.callback == nil || outcome.callback.Code != http.StatusSeeOther || provider.userInfoAuthorization() == "" {
					t.Fatalf("OAuth start = %d: %s; callback = %+v", outcome.start.Code, outcome.start.Body.String(), outcome.callback)
				}
				return
			}
			requireConnectorOAuthSettingRefusal(t, outcome.start, "userInfoEndpoint", "")
			if codes := provider.redeemedCodes(); len(codes) != 0 || provider.userInfoAuthorization() != "" {
				t.Fatalf("the provider received requests although the start was refused: %v", codes)
			}
		})
	}
}

func TestConnectorOAuthDefaultPolicyRefusesLiteralAddressesBeforeAnyConnection(t *testing.T) {
	for _, endpoint := range []string{
		"http://8.8.8.8/token", "https://169.254.169.254/latest/api/token", "http://169.254.169.254/latest/api/token",
		"http://[fe80::1]/token", "https://[::ffff:169.254.169.254]/token", "http://0.0.0.0:8080/token",
	} {
		t.Run(endpoint, func(t *testing.T) {
			provider := newConnectorOAuthSettingsTestProvider(t, connectorOAuthSettingsTestTokenResponse)
			setup := newConnectorOAuthSettingsTestSetup(t)

			outcome := runConnectorOAuthAddressPolicyTest(t, setup, provider, map[string]string{"tokenEndpoint": `"` + endpoint + `"`})

			requireConnectorOAuthSettingRefusal(t, outcome.start, "tokenEndpoint", "")
		})
	}
}

func TestConnectorOAuthRefusesAnAddressThatChangesBetweenStartAndCallback(t *testing.T) {
	for _, test := range []struct {
		name            string
		changedHost     string
		changedSetting  string
		wantTokenSent   bool
		wantDialedCount int
	}{
		{"token endpoint", "auth.internal.test", "tokenEndpoint", false, 0},
		{"identity endpoint", "identity.internal.test", "userInfoEndpoint", true, 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			provider := newConnectorOAuthSettingsTestProvider(t, connectorOAuthSettingsTestTokenResponse)
			network := newNamedNetwork(provider.Listener.Addr().String())
			network.resolveTo("auth.internal.test", "10.0.0.1")
			network.resolveTo("identity.internal.test", "10.0.0.2")
			setup := newConnectorOAuthSettingsTestSetup(t)
			useConnectorAddressPolicyForTest(setup, nil, network.policy())
			port := serverPort(t, provider.Server)
			overrides := map[string]string{
				"tokenEndpoint":    `"http://auth.internal.test:` + port + `/token"`,
				"userInfoEndpoint": `"http://identity.internal.test:` + port + `/userinfo"`,
			}
			start := startConnectorOAuthSettingsTest(t, setup, provider.configuration(overrides))
			if start.Code != http.StatusOK {
				t.Fatalf("OAuth start = %d: %s", start.Code, start.Body.String())
			}

			network.resolveTo(test.changedHost, "169.254.169.254")
			callback := callbackConnectorOAuthSettingsTest(t, setup, start)

			requireConnectorOAuthSettingRefusal(t, callback, test.changedSetting, "link-local")
			if dialed := network.dialedAddresses(); len(dialed) != test.wantDialedCount {
				t.Fatalf("dialed %v, want %d connections", dialed, test.wantDialedCount)
			}
			if sent := len(provider.redeemedCodes()) == 1; sent != test.wantTokenSent || (!test.wantTokenSent && provider.userInfoAuthorization() != "") {
				t.Fatalf("token request sent = %v, want %v", sent, test.wantTokenSent)
			}
			if _, found, err := setup.store.get("oauth2-http", "keycloak"); err != nil || found {
				t.Fatalf("a connection was saved: found = %v, err = %v", found, err)
			}
		})
	}
}

func TestConnectorOAuthSettingEndpointsKeepTLSSettingsAndIgnoreProxies(t *testing.T) {
	provider := newConnectorOAuthSettingsTestTLSProvider(t, connectorOAuthSettingsTestTokenResponse)
	proxyWasUsed := false
	proxy := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { proxyWasUsed = true }))
	t.Cleanup(proxy.Close)
	proxyURL, err := url.Parse(proxy.URL)
	if err != nil {
		t.Fatal(err)
	}
	transport := provider.Client().Transport.(*http.Transport)
	transport.Proxy = http.ProxyURL(proxyURL)
	setup := newConnectorOAuthSettingsTestSetup(t)
	useConnectorAddressPolicyForTest(setup, transport, defaultConnectorAddressPolicy())

	outcome := runConnectorOAuthAddressPolicyTest(t, setup, provider, nil)

	if outcome.callback == nil || outcome.callback.Code != http.StatusSeeOther {
		t.Fatalf("the caller's root certificates must still verify the server: start = %d %s, callback = %+v", outcome.start.Code, outcome.start.Body.String(), outcome.callback)
	}
	if proxyWasUsed {
		t.Fatal("a proxy would receive the connection instead of the address the dialer checked")
	}
}

func TestConnectorOAuthSettingEndpointsNeverFollowRedirects(t *testing.T) {
	redirectTargetWasRequested := false
	redirecting := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/redirected" {
			redirectTargetWasRequested = true
			return
		}
		http.Redirect(response, request, "/redirected", http.StatusTemporaryRedirect)
	}))
	t.Cleanup(redirecting.Close)
	provider := newConnectorOAuthSettingsTestProvider(t, connectorOAuthSettingsTestTokenResponse)
	setup := newConnectorOAuthSettingsTestSetup(t)

	outcome := runConnectorOAuthAddressPolicyTest(t, setup, provider, map[string]string{"tokenEndpoint": `"` + redirecting.URL + `/token"`})

	if outcome.callback == nil || outcome.callback.Code != http.StatusBadGateway || redirectTargetWasRequested {
		t.Fatalf("start = %d, callback = %+v, redirect followed = %v", outcome.start.Code, outcome.callback, redirectTargetWasRequested)
	}
}

func TestConnectorOAuthStartDoesNotRequireTheEndpointHostsToResolve(t *testing.T) {
	provider := newConnectorOAuthSettingsTestProvider(t, connectorOAuthSettingsTestTokenResponse)
	network := newNamedNetwork(provider.Listener.Addr().String())
	setup := newConnectorOAuthSettingsTestSetup(t)
	useConnectorAddressPolicyForTest(setup, nil, network.policy())

	outcome := runConnectorOAuthAddressPolicyTest(t, setup, provider, map[string]string{
		"tokenEndpoint": `"https://idp.unresolvable.test/token"`, "userInfoEndpoint": `"https://idp.unresolvable.test/userinfo"`,
	})

	if outcome.start.Code != http.StatusOK || outcome.callback == nil || outcome.callback.Code != http.StatusBadGateway ||
		!strings.Contains(outcome.callback.Body.String(), "CONNECTOR_OAUTH_TOKEN_EXCHANGE_FAILED") {
		t.Fatalf("start = %d: %s; callback = %+v", outcome.start.Code, outcome.start.Body.String(), outcome.callback)
	}
	if dialed := network.dialedAddresses(); len(dialed) != 0 {
		t.Fatalf("dialed %v for a host that does not resolve", dialed)
	}
}

// The https table of the library has no row for a public and a private address together; https accepts them.
func TestConnectorOAuthHTTPSAcceptsPublicAndPrivateAddressesTogether(t *testing.T) {
	provider := newConnectorOAuthSettingsTestTLSProvider(t, connectorOAuthSettingsTestTokenResponse)
	network := newNamedNetwork(provider.Listener.Addr().String())
	network.resolveTo("example.com", "8.8.8.8", "10.0.0.1")
	setup := newConnectorOAuthSettingsTestSetup(t)
	useConnectorAddressPolicyForTest(setup, provider.Client().Transport.(*http.Transport), network.policy())
	host := "https://example.com:" + serverPort(t, provider.Server)

	outcome := runConnectorOAuthAddressPolicyTest(t, setup, provider, map[string]string{
		"tokenEndpoint": `"` + host + `/token"`, "userInfoEndpoint": `"` + host + `/userinfo"`,
	})

	if outcome.callback == nil || outcome.callback.Code != http.StatusSeeOther {
		t.Fatalf("OAuth start = %d: %s; callback = %+v", outcome.start.Code, outcome.start.Body.String(), outcome.callback)
	}
}
