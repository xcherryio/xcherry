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
	"errors"
	"net"
	"net/http"
	"net/netip"
	"time"
)

const (
	connectorDialTimeout    = 10 * time.Second
	connectorRequestTimeout = 30 * time.Second
)

// errConnectorAddressNotAllowed marks a connection that the address policy refused before any byte was sent.
var errConnectorAddressNotAllowed = errors.New("address not allowed")

// connectorAWSIPv6MetadataAddress is private, so only a named rule keeps plain http away from it.
var connectorAWSIPv6MetadataAddress = netip.MustParseAddr("fd00:ec2::254")

// connectorAddressRefusal is the typed error of a refused connection. Its reason never names the host.
type connectorAddressRefusal struct{ reason string }

// Error names the rule that refused the address.
func (refusal *connectorAddressRefusal) Error() string {
	return errConnectorAddressNotAllowed.Error() + ": " + refusal.reason
}

// Unwrap lets errors.Is match errConnectorAddressNotAllowed.
func (*connectorAddressRefusal) Unwrap() error { return errConnectorAddressNotAllowed }

// connectorAddressPolicy resolves a host once, checks every address, and dials only a checked one.
type connectorAddressPolicy struct {
	lookupAddresses func(ctx context.Context, host string) ([]netip.Addr, error)
	dial            func(ctx context.Context, network string, address string) (net.Conn, error)
}

func defaultConnectorAddressPolicy() connectorAddressPolicy {
	dialer := &net.Dialer{Timeout: connectorDialTimeout, KeepAlive: 30 * time.Second}
	return connectorAddressPolicy{
		lookupAddresses: func(ctx context.Context, host string) ([]netip.Addr, error) {
			return net.DefaultResolver.LookupNetIP(ctx, "ip", host)
		},
		dial: dialer.DialContext,
	}
}

// checkedAddresses resolves host once. One refused address refuses the whole host.
func (policy connectorAddressPolicy) checkedAddresses(ctx context.Context, host string, isPlainHTTP bool) ([]netip.Addr, error) {
	addresses, err := policy.lookupAddresses(ctx, host)
	if err != nil {
		return nil, err
	}
	if len(addresses) == 0 {
		return nil, &net.DNSError{Err: "no addresses", Name: host, IsNotFound: true}
	}
	for index := range addresses {
		addresses[index] = addresses[index].WithZone("").Unmap()
		if reason := connectorAddressRefusalReason(addresses[index], isPlainHTTP); reason != "" {
			return nil, &connectorAddressRefusal{reason: reason}
		}
	}
	return addresses, nil
}

// dialContext returns the dial function of a transport that serves plain http or https requests.
func (policy connectorAddressPolicy) dialContext(isPlainHTTP bool) func(ctx context.Context, network string, address string) (net.Conn, error) {
	return func(ctx context.Context, network string, address string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(address)
		if err != nil {
			return nil, err
		}
		addresses, err := policy.checkedAddresses(ctx, host, isPlainHTTP)
		if err != nil {
			return nil, err
		}
		var dialErrors error
		for _, checkedAddress := range addresses {
			connection, dialErr := policy.dial(ctx, network, net.JoinHostPort(checkedAddress.String(), port))
			if dialErr == nil {
				return connection, nil
			}
			dialErrors = errors.Join(dialErrors, dialErr)
		}
		return nil, dialErrors
	}
}

// connectorAddressRefusalReason returns why address is refused, or an empty string when it is allowed.
func connectorAddressRefusalReason(address netip.Addr, isPlainHTTP bool) string {
	switch {
	case address.IsLinkLocalUnicast(), address.IsLinkLocalMulticast(), address == connectorAWSIPv6MetadataAddress:
		return "link-local addresses, which include the cloud metadata address, are never allowed"
	case address.IsUnspecified(), address.IsMulticast():
		return "only unicast addresses are allowed"
	case isPlainHTTP && !address.IsLoopback() && !address.IsPrivate():
		return "plain http is allowed only when every resolved address is loopback or private; use https"
	}
	return ""
}

// connectorAddressPolicyTransport sends http and https requests through transports with different dial rules.
type connectorAddressPolicyTransport struct {
	plainHTTP *http.Transport
	secure    *http.Transport
}

// RoundTrip sends request through the transport that matches its scheme.
func (transport *connectorAddressPolicyTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	if request.URL.Scheme == "http" {
		return transport.plainHTTP.RoundTrip(request)
	}
	return transport.secure.RoundTrip(request)
}

// newConnectorAddressPolicyClient never redirects, ignores proxies, and dials only checked addresses. base may be nil.
func newConnectorAddressPolicyClient(base *http.Transport, policy connectorAddressPolicy) *http.Client {
	if base == nil {
		base = http.DefaultTransport.(*http.Transport)
	}
	clone := func(dialContext func(context.Context, string, string) (net.Conn, error)) *http.Transport {
		transport := base.Clone()
		transport.Proxy = nil
		transport.DialContext = dialContext
		// Any other dial hook of the base transport would bypass the policy.
		transport.Dial, transport.DialTLS, transport.DialTLSContext = nil, nil, nil
		transport.DisableKeepAlives = true
		return transport
	}
	return &http.Client{
		Transport: &connectorAddressPolicyTransport{plainHTTP: clone(policy.dialContext(true)), secure: clone(policy.dialContext(false))},
		Timeout:   connectorRequestTimeout,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}
