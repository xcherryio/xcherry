// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

// Package membership maintains a ringpop cluster and a consistent hash ring for each Dex service role.
package membership

import (
	"context"
	"errors"
	"fmt"
	"net"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dgryski/go-farm"
	"github.com/superdurable/dex/config"
	"github.com/temporalio/ringpop-go"
	"github.com/temporalio/ringpop-go/events"
	"github.com/temporalio/ringpop-go/hashring"
	ringmembership "github.com/temporalio/ringpop-go/membership"
	"github.com/temporalio/ringpop-go/swim"
	"github.com/temporalio/tchannel-go"
	"go.uber.org/zap"
)

// Role identifies a Dex server service.
type Role string

const (
	// RoleAPI hosts the Dex API.
	RoleAPI Role = "api"
	// RoleInterpreter interprets Flows.
	RoleInterpreter Role = "interpreter"
	// RoleMatching owns pull-mode queues.
	RoleMatching Role = "matching"
	// RoleWeb hosts Dex Web.
	RoleWeb Role = "web"
)

const (
	roleLabel       = "dexRoles"
	rpcAddressLabel = "dexRPCAddress"
)

// HostInfo describes a member's advertised membership and RPC addresses and roles.
type HostInfo struct {
	Address    string
	RPCAddress string
	Roles      []Role
	IsSelf     bool
}

// Change is a coalesced hint that a role ring changed; consumers must re-read Lookup or Members.
type Change struct{ Role Role }

// HostResolver resolves a DNS seed through an injected resolver.
type HostResolver interface {
	LookupHost(context.Context, string) ([]string, error)
}

// Monitor owns membership lifecycle and role-filtered hash rings.
type Monitor struct {
	cfg         *config.MembershipConfig
	roles       []Role
	rpcAddress  string
	resolver    HostResolver
	logger      *zap.Logger
	mu          sync.RWMutex
	started     bool
	everStarted bool
	running     bool
	address     string
	channel     *tchannel.Channel
	ringpop     *ringpop.Ringpop
	rings       map[Role]*hashring.HashRing
	members     map[Role][]HostInfo
	subscribers map[chan Change]struct{}
	refresh     chan struct{}
	stop        chan struct{}
	stopped     chan struct{}
}

// NewMonitor creates a monitor. Nil config selects network-free single-member mode.
func NewMonitor(cfg *config.MembershipConfig, roles []Role, rpcAddress string, resolver HostResolver, logger *zap.Logger) (*Monitor, error) {
	if logger == nil {
		panic("membership logger is required")
	}
	if rpcAddress != "" {
		host, _, err := net.SplitHostPort(rpcAddress)
		if err != nil {
			return nil, fmt.Errorf("membership RPC address must be host:port: %w", err)
		}
		if host == "" {
			return nil, errors.New("membership RPC address must advertise a host")
		}
	}
	if cfg != nil && rpcAddress == "" {
		return nil, errors.New("membership RPC address is required in network mode")
	}
	if len(roles) == 0 {
		return nil, errors.New("membership requires a local role")
	}
	seen := make(map[Role]struct{}, len(roles))
	for _, role := range roles {
		if !isValidRole(role) {
			return nil, fmt.Errorf("invalid membership role %q", role)
		}
		if _, exists := seen[role]; exists {
			return nil, fmt.Errorf("duplicate membership role %q", role)
		}
		seen[role] = struct{}{}
	}
	if cfg != nil {
		if err := cfg.Validate(); err != nil {
			return nil, err
		}
	}
	if resolver == nil {
		resolver = net.DefaultResolver
	}
	return &Monitor{cfg: cfg, roles: slices.Clone(roles), rpcAddress: rpcAddress, resolver: resolver, logger: logger, rings: make(map[Role]*hashring.HashRing), members: make(map[Role][]HostInfo), subscribers: make(map[chan Change]struct{})}, nil
}

// AdvertisedRPCAddress replaces a wildcard internal bind host with the membership broadcast host.
func AdvertisedRPCAddress(cfg *config.MembershipConfig, listen string) (string, error) {
	host, port, err := net.SplitHostPort(listen)
	if err != nil {
		return "", fmt.Errorf("internal.listen must be host:port: %w", err)
	}
	if port == "0" {
		return "", errors.New("internal.listen must use a fixed port with membership")
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		if cfg != nil && cfg.BroadcastAddress != "" {
			host = cfg.BroadcastAddress
		} else {
			host, err = discoverPrivateIPv4()
			if err != nil {
				return "", err
			}
		}
	}
	return net.JoinHostPort(host, port), nil
}

// Start begins discovery. A monitor can start only once.
func (m *Monitor) Start(ctx context.Context) error {
	m.mu.Lock()
	if m.everStarted {
		m.mu.Unlock()
		return errors.New("membership monitor cannot restart")
	}
	m.started = true
	m.everStarted = true
	m.mu.Unlock()
	if m.cfg == nil {
		m.mu.Lock()
		m.address = "self"
		for _, role := range m.roles {
			m.setRoleMembersLocked(role, []HostInfo{{Address: "self", RPCAddress: m.rpcAddress, Roles: slices.Clone(m.roles), IsSelf: true}})
		}
		m.mu.Unlock()
		return nil
	}
	if err := m.startNetwork(ctx); err != nil {
		m.Stop()
		return err
	}
	return nil
}

func (m *Monitor) startNetwork(ctx context.Context) error {
	listener, err := net.Listen("tcp", m.cfg.EffectiveListen())
	if err != nil {
		return fmt.Errorf("listen for membership: %w", err)
	}
	channel, err := tchannel.NewChannel("dex-membership", &tchannel.ChannelOptions{})
	if err != nil {
		if closeErr := listener.Close(); closeErr != nil {
			m.logger.Error("close membership listener", zap.Error(closeErr))
		}
		return fmt.Errorf("create membership channel: %w", err)
	}
	if err := channel.Serve(listener); err != nil {
		channel.Close()
		if closeErr := listener.Close(); closeErr != nil {
			m.logger.Error("close membership listener", zap.Error(closeErr))
		}
		return fmt.Errorf("serve membership channel: %w", err)
	}
	broadcastHost := m.cfg.BroadcastAddress
	if broadcastHost == "" {
		broadcastHost, err = discoverPrivateIPv4()
		if err != nil {
			channel.Close()
			return err
		}
	}
	address := net.JoinHostPort(broadcastHost, strconv.Itoa(listener.Addr().(*net.TCPAddr).Port))
	ring, err := ringpop.New("dex", ringpop.Channel(channel), ringpop.AddressResolverFunc((&addressResolver{address: address}).resolve))
	if err != nil {
		channel.Close()
		return fmt.Errorf("create ringpop: %w", err)
	}
	m.mu.Lock()
	m.channel, m.ringpop, m.address = channel, ring, address
	m.refresh, m.stop, m.stopped = make(chan struct{}, 1), make(chan struct{}), make(chan struct{})
	m.mu.Unlock()
	provider := &seedProvider{cfg: m.cfg, resolver: m.resolver, self: address}
	if _, err := provider.resolve(ctx); err != nil {
		return err
	}
	if _, err := ring.Bootstrap(&swim.BootstrapOptions{DiscoverProvider: provider, JoinSize: 1, MaxJoinDuration: 3 * time.Second}); err != nil {
		return fmt.Errorf("join membership cluster: %w", err)
	}
	labels, err := ring.Labels()
	if err != nil {
		return err
	}
	if err := labels.Set(rpcAddressLabel, m.rpcAddress); err != nil {
		return fmt.Errorf("publish membership RPC address: %w", err)
	}
	if err := labels.Set(roleLabel, joinRoles(m.roles)); err != nil {
		return fmt.Errorf("publish membership roles: %w", err)
	}
	ring.AddListener(m)
	if err := m.refreshRings(); err != nil {
		return err
	}
	m.mu.Lock()
	m.running = true
	m.mu.Unlock()
	go m.runRefresh(provider)
	return nil
}

// Stop evicts the local member and closes its listener.
func (m *Monitor) Stop() {
	m.mu.Lock()
	if !m.started {
		m.mu.Unlock()
		return
	}
	m.started = false
	ring, channel, stop, stopped, running := m.ringpop, m.channel, m.stop, m.stopped, m.running
	m.mu.Unlock()
	if stop != nil && running {
		close(stop)
		<-stopped
	}
	if ring != nil {
		ring.RemoveListener(m)
		if ring.Ready() {
			if err := ring.SelfEvict(); err != nil {
				m.logger.Error("membership self-eviction", zap.Error(err))
			}
		}
		ring.Destroy()
	}
	if channel != nil {
		channel.Close()
	}
	m.mu.Lock()
	m.rings, m.members = make(map[Role]*hashring.HashRing), make(map[Role][]HostInfo)
	m.mu.Unlock()
}

// Lookup returns the key owner among members carrying role.
func (m *Monitor) Lookup(role Role, key string) (HostInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	ring := m.rings[role]
	if ring == nil {
		return HostInfo{}, fmt.Errorf("no members for role %q", role)
	}
	address, found := ring.Lookup(key)
	if !found {
		return HostInfo{}, fmt.Errorf("no members for role %q", role)
	}
	for _, host := range m.members[role] {
		if host.Address == address {
			host.Roles = slices.Clone(host.Roles)
			return host, nil
		}
	}
	return HostInfo{}, fmt.Errorf("owner %q missing from role %q", address, role)
}

// Members returns a snapshot of members carrying role.
func (m *Monitor) Members(role Role) []HostInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()
	hosts := slices.Clone(m.members[role])
	for index := range hosts {
		hosts[index].Roles = slices.Clone(hosts[index].Roles)
	}
	return hosts
}

// Subscribe returns non-blocking, coalesced change hints and a removal function. Re-read Lookup or Members after each hint.
func (m *Monitor) Subscribe() (<-chan Change, func()) {
	changes := make(chan Change, 16)
	m.mu.Lock()
	m.subscribers[changes] = struct{}{}
	m.mu.Unlock()
	return changes, (&subscription{monitor: m, changes: changes}).cancel
}

type subscription struct {
	monitor *Monitor
	changes chan Change
}

func (s *subscription) cancel() {
	s.monitor.mu.Lock()
	delete(s.monitor.subscribers, s.changes)
	s.monitor.mu.Unlock()
}

type addressResolver struct{ address string }

func (r *addressResolver) resolve() (string, error) { return r.address, nil }

// HandleEvent schedules a refresh after ringpop changes its member list.
func (m *Monitor) HandleEvent(event events.Event) {
	if _, isChange := event.(ringmembership.ChangeEvent); !isChange {
		return
	}
	select {
	case m.refresh <- struct{}{}:
	default:
	}
}

func (m *Monitor) runRefresh(provider *seedProvider) {
	defer close(m.stopped)
	ticker := time.NewTicker(m.cfg.EffectiveRefreshInterval())
	defer ticker.Stop()
	for {
		select {
		case <-m.stop:
			return
		case <-m.refresh:
			if err := m.refreshRings(); err != nil {
				m.logger.Error("membership refresh", zap.Error(err))
			}
		case <-ticker.C:
			if _, err := provider.Hosts(); err != nil {
				m.logger.Error("membership seed resolution", zap.Error(err))
			}
			if err := m.refreshRings(); err != nil {
				m.logger.Error("membership refresh", zap.Error(err))
			}
		}
	}
}

func (m *Monitor) refreshRings() error {
	members, err := m.ringpop.GetReachableMemberObjects()
	if err != nil {
		return err
	}
	byRole := make(map[Role][]HostInfo)
	for _, member := range members {
		roles := splitRoles(member.Labels[roleLabel])
		rpcAddress := member.Labels[rpcAddressLabel]
		if len(roles) == 0 || rpcAddress == "" {
			continue
		}
		for _, role := range roles {
			byRole[role] = append(byRole[role], HostInfo{Address: member.Address, RPCAddress: rpcAddress, Roles: roles, IsSelf: member.Address == m.address})
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, role := range []Role{RoleAPI, RoleInterpreter, RoleMatching, RoleWeb} {
		hosts := byRole[role]
		slices.SortFunc(hosts, func(left, right HostInfo) int { return strings.Compare(left.Address, right.Address) })
		if !equalHosts(hosts, m.members[role]) {
			m.setRoleMembersLocked(role, hosts)
			for subscriber := range m.subscribers {
				select {
				case subscriber <- Change{Role: role}:
				default:
				}
			}
		}
	}
	return nil
}

func (m *Monitor) setRoleMembersLocked(role Role, hosts []HostInfo) {
	ring := hashring.New(farm.Fingerprint32, 100)
	for _, host := range hosts {
		ring.AddMembers(hostMember{address: host.Address})
	}
	m.rings[role], m.members[role] = ring, hosts
}

type hostMember struct{ address string }

func (h hostMember) GetAddress() string          { return h.address }
func (h hostMember) Identity() string            { return h.address }
func (h hostMember) Label(string) (string, bool) { return "", false }

func equalHosts(left, right []HostInfo) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index].Address != right[index].Address || left[index].RPCAddress != right[index].RPCAddress || !slices.Equal(left[index].Roles, right[index].Roles) {
			return false
		}
	}
	return true
}

func isValidRole(role Role) bool {
	return role == RoleAPI || role == RoleInterpreter || role == RoleMatching || role == RoleWeb
}
func joinRoles(roles []Role) string {
	values := make([]string, len(roles))
	for index, role := range roles {
		values[index] = string(role)
	}
	return strings.Join(values, ",")
}
func splitRoles(value string) []Role {
	if value == "" {
		return nil
	}
	roles := make([]Role, 0)
	for _, part := range strings.Split(value, ",") {
		if role := Role(part); isValidRole(role) {
			roles = append(roles, role)
		}
	}
	return roles
}

func discoverPrivateIPv4() (string, error) {
	interfaces, err := net.Interfaces()
	if err != nil {
		return "", err
	}
	for _, networkInterface := range interfaces {
		if networkInterface.Flags&net.FlagUp == 0 || networkInterface.Flags&net.FlagLoopback != 0 {
			continue
		}
		addresses, err := networkInterface.Addrs()
		if err != nil {
			return "", err
		}
		for _, address := range addresses {
			ipAddress, _, err := net.ParseCIDR(address.String())
			if err == nil && ipAddress.IsPrivate() && ipAddress.To4() != nil {
				return ipAddress.String(), nil
			}
		}
	}
	return "", errors.New("membership.broadcastAddress is required without a private IPv4")
}

type seedProvider struct {
	cfg      *config.MembershipConfig
	resolver HostResolver
	self     string
}

func (p *seedProvider) Hosts() ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), p.cfg.EffectiveRefreshInterval())
	defer cancel()
	return p.resolve(ctx)
}
func (p *seedProvider) resolve(ctx context.Context) ([]string, error) {
	seeds := slices.Clone(p.cfg.Seeds.Static)
	if p.cfg.Seeds.DNSName != "" {
		host, port, err := net.SplitHostPort(p.cfg.Seeds.DNSName)
		if err != nil {
			return nil, err
		}
		addresses, err := p.resolver.LookupHost(ctx, host)
		if err != nil {
			return nil, fmt.Errorf("resolve membership seed: %w", err)
		}
		for _, address := range addresses {
			seeds = append(seeds, net.JoinHostPort(address, port))
		}
	}
	return append(seeds, p.self), nil
}
