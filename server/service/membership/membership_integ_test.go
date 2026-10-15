// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package membership

import (
	"context"
	"net"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/superdurable/dex/config"
	"go.uber.org/zap"
)

var nextTestRPCPort atomic.Int32

func TestMembershipIntegration(t *testing.T) {
	first := startTestMonitor(t, &config.MembershipConfig{Listen: "127.0.0.1:0", BroadcastAddress: "127.0.0.1", RefreshInterval: 100 * time.Millisecond}, []Role{RoleAPI, RoleMatching}, nil)
	firstAddress := first.Members(RoleMatching)[0].Address
	second := startTestMonitor(t, &config.MembershipConfig{Listen: "127.0.0.1:0", BroadcastAddress: "127.0.0.1", RefreshInterval: 100 * time.Millisecond, Seeds: config.MembershipSeedsConfig{Static: []string{firstAddress}}}, []Role{RoleMatching}, nil)
	third := startTestMonitor(t, &config.MembershipConfig{Listen: "127.0.0.1:0", BroadcastAddress: "127.0.0.1", RefreshInterval: 100 * time.Millisecond, Seeds: config.MembershipSeedsConfig{Static: []string{firstAddress}}}, []Role{RoleAPI, RoleMatching}, nil)
	monitors := []*Monitor{first, second, third}
	rpcAddresses := make(map[string]string, len(monitors))
	for _, monitor := range monitors {
		for _, host := range monitor.Members(RoleMatching) {
			if host.IsSelf {
				rpcAddresses[host.Address] = host.RPCAddress
			}
		}
	}
	secondAddress := ""
	for _, host := range second.Members(RoleMatching) {
		if host.IsSelf {
			secondAddress = host.Address
		}
	}
	require.NotEmpty(t, secondAddress)
	require.Eventually(t, func() bool { return allMembers(monitors, RoleMatching, 3) && allMembers(monitors, RoleAPI, 2) }, 10*time.Second, 20*time.Millisecond)
	keys := make([]string, 100)
	for index := range keys {
		keys[index] = strconv.Itoa(index)
	}
	for _, key := range keys {
		owner, err := first.Lookup(RoleMatching, key)
		require.NoError(t, err)
		require.Equal(t, rpcAddresses[owner.Address], owner.RPCAddress)
		for _, monitor := range monitors[1:] {
			other, err := monitor.Lookup(RoleMatching, key)
			require.NoError(t, err)
			require.Equal(t, owner.Address, other.Address)
			require.Equal(t, rpcAddresses[other.Address], other.RPCAddress)
		}
		apiOwner, err := first.Lookup(RoleAPI, key)
		require.NoError(t, err)
		require.NotEqual(t, secondAddress, apiOwner.Address)
	}
	ownedKeys := make([]string, 0)
	for _, key := range keys {
		owner, err := first.Lookup(RoleMatching, key)
		require.NoError(t, err)
		if owner.Address == secondAddress {
			ownedKeys = append(ownedKeys, key)
		}
	}
	require.NotEmpty(t, ownedKeys)
	changes, unsubscribe := first.Subscribe()
	defer unsubscribe()
	second.Stop()
	require.Eventually(t, func() bool { return allMembers([]*Monitor{first, third}, RoleMatching, 2) }, 10*time.Second, 20*time.Millisecond)
	select {
	case change := <-changes:
		require.Equal(t, RoleMatching, change.Role)
	case <-time.After(5 * time.Second):
		t.Fatal("missing matching change event")
	}
	for _, key := range ownedKeys {
		owner, err := first.Lookup(RoleMatching, key)
		require.NoError(t, err)
		require.NotEqual(t, secondAddress, owner.Address)
	}
	replacement := startTestMonitor(t, &config.MembershipConfig{Listen: "127.0.0.1:0", BroadcastAddress: "127.0.0.1", RefreshInterval: 100 * time.Millisecond, Seeds: config.MembershipSeedsConfig{Static: []string{firstAddress}}}, []Role{RoleMatching}, nil)
	require.Eventually(t, func() bool { return allMembers([]*Monitor{first, third, replacement}, RoleMatching, 3) }, 10*time.Second, 20*time.Millisecond)
	changed := false
	for _, key := range keys {
		before, err := first.Lookup(RoleMatching, key)
		require.NoError(t, err)
		if before.Address == replacement.Members(RoleMatching)[0].Address {
			changed = true
			break
		}
	}
	require.True(t, changed)
}

func TestMembershipDNSSeedsIntegration(t *testing.T) {
	first := startTestMonitor(t, &config.MembershipConfig{Listen: "127.0.0.1:0", BroadcastAddress: "127.0.0.1"}, []Role{RoleMatching}, nil)
	_, port, err := net.SplitHostPort(first.Members(RoleMatching)[0].Address)
	require.NoError(t, err)
	resolver := &stubResolver{}
	second := startTestMonitor(t, &config.MembershipConfig{Listen: "127.0.0.1:0", BroadcastAddress: "127.0.0.1", RefreshInterval: 50 * time.Millisecond, Seeds: config.MembershipSeedsConfig{DNSName: net.JoinHostPort("dex-headless.test", port)}}, []Role{RoleMatching}, resolver)
	require.Eventually(t, func() bool { return allMembers([]*Monitor{first, second}, RoleMatching, 2) }, 10*time.Second, 20*time.Millisecond)
	require.Eventually(t, func() bool { return resolver.calls.Load() > 1 }, 3*time.Second, 20*time.Millisecond)
}

func TestMembershipSingleNodeIntegration(t *testing.T) {
	monitor := startTestMonitor(t, nil, []Role{RoleAPI, RoleMatching}, nil)
	for _, role := range []Role{RoleAPI, RoleMatching} {
		owner, err := monitor.Lookup(role, "any-key")
		require.NoError(t, err)
		require.True(t, owner.IsSelf)
		require.Empty(t, owner.RPCAddress)
		require.Len(t, monitor.Members(role), 1)
	}
	configured, err := NewMonitor(nil, []Role{RoleMatching}, "127.0.0.1:8801", nil, zap.NewNop())
	require.NoError(t, err)
	require.NoError(t, configured.Start(context.Background()))
	t.Cleanup(configured.Stop)
	owner, err := configured.Lookup(RoleMatching, "any-key")
	require.NoError(t, err)
	require.Equal(t, "127.0.0.1:8801", owner.RPCAddress)
}

func startTestMonitor(t *testing.T, cfg *config.MembershipConfig, roles []Role, resolver HostResolver) *Monitor {
	t.Helper()
	rpcAddress := ""
	if cfg != nil {
		rpcAddress = net.JoinHostPort("127.0.0.1", strconv.Itoa(int(18000+nextTestRPCPort.Add(1))))
	}
	monitor, err := NewMonitor(cfg, roles, rpcAddress, resolver, zap.NewNop())
	require.NoError(t, err)
	require.NoError(t, monitor.Start(context.Background()))
	t.Cleanup(monitor.Stop)
	return monitor
}

func allMembers(monitors []*Monitor, role Role, count int) bool {
	for _, monitor := range monitors {
		if len(monitor.Members(role)) != count {
			return false
		}
	}
	return true
}

type stubResolver struct{ calls atomic.Int32 }

func (r *stubResolver) LookupHost(context.Context, string) ([]string, error) {
	r.calls.Add(1)
	return []string{"127.0.0.1"}, nil
}
