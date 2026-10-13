// Legacy Materials in this file remain under their original licenses.
// See LEGACY_NOTICES.md.

// Modifications Copyright (c) 2026 Super Durable, Inc.
//
// Modifications after the Legacy Cutoff are licensed under the
// Sustainable Use License 1.0.
// Legacy Materials remain under their original licenses.
// See LICENSE and LEGACY_NOTICES.md.

package workerclient

import (
	"container/list"
	"context"
	"fmt"
	"log"
	"net"
	"sort"
	"sync"
	"time"

	"github.com/superdurable/dex/config"
	"github.com/superdurable/dex/gen/dexpb"
	"github.com/superdurable/dex/service/common/grpctarget"
	"golang.org/x/sync/singleflight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// NoWorkerAddressError reports a worker target that currently resolves to no address.
type NoWorkerAddressError struct {
	err error
}

func (e *NoWorkerAddressError) Error() string { return e.err.Error() }

func (e *NoWorkerAddressError) Unwrap() error { return e.err }

// IsNoWorkerAddress lets callers that cannot import this package recognize the error.
func (e *NoWorkerAddressError) IsNoWorkerAddress() bool { return true }

type hostResolver interface {
	LookupHost(context.Context, string) ([]string, error)
}

type pooledConn struct {
	conn     *grpc.ClientConn
	refs     int
	lastIdle time.Time
}

type headlessTarget struct {
	addresses []string
	available map[string]struct{}
	next      uint64
}

type stickyKey struct {
	target string
	flowID string
}

type stickyRoute struct {
	key     stickyKey
	address string
}

// WorkerClientPool shares WorkerService connections and routing state.
type WorkerClientPool struct {
	cfg                 *config.Config
	header              metadata.MD
	resolver            hostResolver
	failoverStatusCodes map[codes.Code]struct{}

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu        sync.Mutex
	conns     map[string]*pooledConn
	headless  map[string]*headlessTarget
	sticky    map[stickyKey]*list.Element
	stickyLRU *list.List
	closed    bool
	flight    singleflight.Group
}

// NewWorkerClientPool constructs a WorkerService client pool.
func NewWorkerClientPool(cfg *config.Config) (*WorkerClientPool, error) {
	return newWorkerClientPool(cfg, net.DefaultResolver)
}

func newWorkerClientPool(cfg *config.Config, resolver hostResolver) (*WorkerClientPool, error) {
	if cfg == nil {
		panic("workerclient: config must not be nil")
	}
	if resolver == nil {
		panic("workerclient: resolver must not be nil")
	}
	workerCfg := &cfg.Worker
	if err := ValidateDefaultHeaders(workerCfg.DefaultHeaders); err != nil {
		return nil, err
	}
	failoverStatusCodes, err := newHeadlessFailoverStatusCodes(workerCfg)
	if err != nil {
		return nil, err
	}
	poolCtx, cancel := context.WithCancel(context.Background())
	return &WorkerClientPool{
		cfg:                 cfg,
		header:              metadata.New(workerCfg.DefaultHeaders),
		resolver:            resolver,
		failoverStatusCodes: failoverStatusCodes,
		ctx:                 poolCtx,
		cancel:              cancel,
		conns:               make(map[string]*pooledConn),
		headless:            make(map[string]*headlessTarget),
		sticky:              make(map[stickyKey]*list.Element),
		stickyLRU:           list.New(),
	}, nil
}

func newHeadlessFailoverStatusCodes(workerCfg *config.WorkerConfig) (map[codes.Code]struct{}, error) {
	statusCodes := workerCfg.EffectiveHeadlessFailoverStatusCodes()
	result := make(map[codes.Code]struct{}, len(statusCodes))
	for _, statusCode := range statusCodes {
		if statusCode <= codes.OK || statusCode > codes.Unauthenticated {
			return nil, fmt.Errorf("workerclient: invalid headless failover gRPC status code %d", statusCode)
		}
		result[statusCode] = struct{}{}
	}
	return result, nil
}

// Acquire returns a flow-routed WorkerService client.
func (p *WorkerClientPool) Acquire(
	ctx context.Context,
	workerTarget *dexpb.WorkerTarget,
	flowID string,
) (dexpb.WorkerServiceClient, context.Context, func(), error) {
	if flowID == "" {
		return nil, ctx, nil, fmt.Errorf("workerclient: flowID is required")
	}
	normalized, err := grpctarget.NormalizeWorkerTarget(workerTarget)
	if err != nil {
		return nil, ctx, nil, err
	}
	logicalTarget := normalized.GetAddress()
	if normalized.GetIsHeadlessAddress() {
		if err := p.ensureHeadlessTarget(ctx, logicalTarget); err != nil {
			return nil, ctx, nil, err
		}
	}
	address, err := p.selectAddress(logicalTarget, normalized.GetIsHeadlessAddress(), flowID)
	if err != nil {
		return nil, ctx, nil, err
	}
	conn, err := p.acquireConn(address)
	if err != nil {
		return nil, ctx, nil, err
	}
	callCtx, routeState := newResolvedWorkerAddressContext(p.withHeaders(ctx), address)
	client := &routedWorkerClient{
		pool:       p,
		target:     logicalTarget,
		isHeadless: normalized.GetIsHeadlessAddress(),
		flowID:     flowID,
		routeState: routeState,
		address:    address,
		conn:       conn,
	}
	return client, callCtx, client.release, nil
}

func (p *WorkerClientPool) ensureHeadlessTarget(ctx context.Context, target string) error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return fmt.Errorf("workerclient: pool closed")
	}
	if _, ok := p.headless[target]; ok {
		p.mu.Unlock()
		return nil
	}
	p.mu.Unlock()

	_, err, _ := p.flight.Do("resolve:"+target, func() (interface{}, error) {
		return nil, p.createHeadlessTarget(ctx, target)
	})
	return err
}

func (p *WorkerClientPool) createHeadlessTarget(ctx context.Context, target string) error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return fmt.Errorf("workerclient: pool closed")
	}
	if _, ok := p.headless[target]; ok {
		p.mu.Unlock()
		return nil
	}
	p.mu.Unlock()

	addresses, err := p.resolveHeadlessTarget(ctx, target)
	if err != nil {
		return err
	}

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return fmt.Errorf("workerclient: pool closed")
	}
	if _, ok := p.headless[target]; ok {
		p.mu.Unlock()
		return nil
	}
	p.headless[target] = newHeadlessTarget(addresses)
	p.wg.Add(1)
	p.mu.Unlock()
	go p.refreshHeadlessTarget(target)
	return nil
}

func (p *WorkerClientPool) refreshHeadlessTarget(target string) {
	defer p.wg.Done()
	ticker := time.NewTicker(p.cfg.Worker.EffectiveHeadlessAddressRefreshInterval())
	defer ticker.Stop()
	for {
		select {
		case <-p.ctx.Done():
			return
		case <-ticker.C:
			addresses, err := p.resolveHeadlessTarget(p.ctx, target)
			if err != nil {
				continue
			}
			p.replaceResolvedAddresses(target, addresses)
		}
	}
}

func (p *WorkerClientPool) replaceResolvedAddresses(target string, addresses []string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return
	}
	current, ok := p.headless[target]
	if !ok {
		return
	}
	replacement := newHeadlessTarget(addresses)
	replacement.next = current.next
	p.headless[target] = replacement
}

func (p *WorkerClientPool) resolveHeadlessTarget(ctx context.Context, target string) ([]string, error) {
	host, port, err := net.SplitHostPort(target)
	if err != nil {
		return nil, fmt.Errorf("workerclient: split headless target %q: %w", target, err)
	}
	resolvedHosts, err := p.resolver.LookupHost(ctx, host)
	if err != nil {
		return nil, &NoWorkerAddressError{err: fmt.Errorf("workerclient: resolve headless target %q: %w", target, err)}
	}
	unique := make(map[string]struct{}, len(resolvedHosts))
	for _, resolvedHost := range resolvedHosts {
		if net.ParseIP(resolvedHost) == nil {
			continue
		}
		unique[net.JoinHostPort(resolvedHost, port)] = struct{}{}
	}
	if len(unique) == 0 {
		return nil, &NoWorkerAddressError{err: fmt.Errorf("workerclient: headless target %q resolved no IP addresses", target)}
	}
	addresses := make([]string, 0, len(unique))
	for address := range unique {
		addresses = append(addresses, address)
	}
	sort.Strings(addresses)
	return addresses, nil
}

func newHeadlessTarget(addresses []string) *headlessTarget {
	available := make(map[string]struct{}, len(addresses))
	for _, address := range addresses {
		available[address] = struct{}{}
	}
	return &headlessTarget{
		addresses: addresses,
		available: available,
	}
}

func (p *WorkerClientPool) selectAddress(
	target string,
	isHeadless bool,
	flowID string,
) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return "", fmt.Errorf("workerclient: pool closed")
	}
	if !isHeadless {
		return target, nil
	}
	resolved, ok := p.headless[target]
	if !ok || len(resolved.addresses) == 0 {
		return "", &NoWorkerAddressError{err: fmt.Errorf("workerclient: headless target %q has no resolved addresses", target)}
	}
	key := stickyKey{target: target, flowID: flowID}
	if element, ok := p.sticky[key]; ok {
		route := element.Value.(*stickyRoute)
		if _, available := resolved.available[route.address]; available {
			p.stickyLRU.MoveToFront(element)
			return route.address, nil
		}
		p.removeStickyLocked(element)
	}
	address := resolved.addresses[resolved.next%uint64(len(resolved.addresses))]
	resolved.next++
	return address, nil
}

func (p *WorkerClientPool) rememberSticky(target, flowID, address string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return
	}
	resolved, ok := p.headless[target]
	if !ok {
		return
	}
	if _, available := resolved.available[address]; !available {
		return
	}
	key := stickyKey{target: target, flowID: flowID}
	if element, ok := p.sticky[key]; ok {
		route := element.Value.(*stickyRoute)
		route.address = address
		p.stickyLRU.MoveToFront(element)
		return
	}
	element := p.stickyLRU.PushFront(&stickyRoute{key: key, address: address})
	p.sticky[key] = element
	for p.stickyLRU.Len() > p.cfg.Worker.EffectiveMaxStickyRoutingEntries() {
		p.removeStickyLocked(p.stickyLRU.Back())
	}
}

func (p *WorkerClientPool) forgetSticky(target, flowID, address string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	element, ok := p.sticky[stickyKey{target: target, flowID: flowID}]
	if !ok {
		return
	}
	route := element.Value.(*stickyRoute)
	if route.address == address {
		p.removeStickyLocked(element)
	}
}

func (p *WorkerClientPool) removeStickyLocked(element *list.Element) {
	if element == nil {
		return
	}
	route := element.Value.(*stickyRoute)
	delete(p.sticky, route.key)
	p.stickyLRU.Remove(element)
}

func (p *WorkerClientPool) nextFailoverAddress(
	target string,
	current string,
	failed map[string]struct{},
) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return "", fmt.Errorf("workerclient: pool closed")
	}
	resolved, ok := p.headless[target]
	if !ok || len(resolved.addresses) == 0 {
		return "", &NoWorkerAddressError{err: fmt.Errorf("workerclient: headless target %q has no resolved addresses", target)}
	}
	start := 0
	for index, address := range resolved.addresses {
		if address == current {
			start = index + 1
			break
		}
	}
	for offset := 0; offset < len(resolved.addresses); offset++ {
		address := resolved.addresses[(start+offset)%len(resolved.addresses)]
		if _, alreadyFailed := failed[address]; !alreadyFailed {
			return address, nil
		}
	}
	clear(failed)
	return resolved.addresses[start%len(resolved.addresses)], nil
}

func (p *WorkerClientPool) acquireConn(address string) (*grpc.ClientConn, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, fmt.Errorf("workerclient: pool closed")
	}
	if existing, ok := p.conns[address]; ok {
		existing.refs++
		return existing.conn, nil
	}
	if err := p.ensureCapacityLocked(); err != nil {
		return nil, err
	}
	conn, err := grpc.NewClient(
		address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(p.cfg.Api.EffectiveGrpcMaxMessageBytes()),
			grpc.MaxCallSendMsgSize(p.cfg.Api.EffectiveGrpcMaxMessageBytes()),
		),
	)
	if err != nil {
		return nil, err
	}
	p.conns[address] = &pooledConn{conn: conn, refs: 1}
	return conn, nil
}

func (p *WorkerClientPool) ensureCapacityLocked() error {
	if len(p.conns) < p.cfg.Worker.EffectiveMaxWorkerConnections() {
		return nil
	}
	p.evictIdleLocked(true)
	if len(p.conns) < p.cfg.Worker.EffectiveMaxWorkerConnections() {
		return nil
	}
	return fmt.Errorf("workerclient: max connections %d exhausted", p.cfg.Worker.EffectiveMaxWorkerConnections())
}

func (p *WorkerClientPool) releaseConn(address string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	entry, ok := p.conns[address]
	if !ok {
		return
	}
	if entry.refs > 0 {
		entry.refs--
	}
	if entry.refs == 0 {
		entry.lastIdle = time.Now()
	}
	p.evictIdleLocked(false)
}

func (p *WorkerClientPool) evictIdleLocked(force bool) {
	now := time.Now()
	for address, entry := range p.conns {
		if entry.refs != 0 {
			continue
		}
		if force || (!entry.lastIdle.IsZero() && now.Sub(entry.lastIdle) >= p.cfg.Worker.EffectiveWorkerConnectionIdleTimeout()) {
			p.closeConn(entry.conn)
			delete(p.conns, address)
			if force && len(p.conns) < p.cfg.Worker.EffectiveMaxWorkerConnections() {
				return
			}
		}
	}
}

func (p *WorkerClientPool) withHeaders(ctx context.Context) context.Context {
	if len(p.header) == 0 {
		return ctx
	}
	return metadata.NewOutgoingContext(ctx, p.header)
}

// Close stops refreshes and closes pooled connections.
func (p *WorkerClientPool) Close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	p.cancel()
	for address, entry := range p.conns {
		p.closeConn(entry.conn)
		delete(p.conns, address)
	}
	clear(p.headless)
	clear(p.sticky)
	p.stickyLRU.Init()
	p.mu.Unlock()
	p.wg.Wait()
}

func (p *WorkerClientPool) closeConn(conn *grpc.ClientConn) {
	if err := conn.Close(); err != nil {
		log.Printf("workerclient: close connection: %v", err)
	}
}
