// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package integ

import (
	"crypto/tls"
	"errors"
	"io"
	"net"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

type temporalProxyFrame []byte

type temporalProxyCodec struct{}

func (temporalProxyCodec) Name() string { return "proto" }
func (temporalProxyCodec) Marshal(message any) ([]byte, error) {
	return []byte(*(message.(*temporalProxyFrame))), nil
}
func (temporalProxyCodec) Unmarshal(data []byte, message any) error {
	*(message.(*temporalProxyFrame)) = append((*message.(*temporalProxyFrame))[:0], data...)
	return nil
}

type trackedTemporalListener struct {
	net.Listener
	mu          sync.Mutex
	connections map[net.Conn]struct{}
}

func (listener *trackedTemporalListener) Accept() (net.Conn, error) {
	connection, err := listener.Listener.Accept()
	if err != nil {
		return nil, err
	}
	tracked := &trackedTemporalConnection{Conn: connection, listener: listener}
	listener.mu.Lock()
	listener.connections[tracked] = struct{}{}
	listener.mu.Unlock()
	return tracked, nil
}

func (listener *trackedTemporalListener) closeConnections() error {
	listener.mu.Lock()
	connections := make([]net.Conn, 0, len(listener.connections))
	for connection := range listener.connections {
		connections = append(connections, connection)
	}
	listener.mu.Unlock()
	for _, connection := range connections {
		if err := connection.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			return err
		}
	}
	return nil
}

type trackedTemporalConnection struct {
	net.Conn
	listener *trackedTemporalListener
}

func (connection *trackedTemporalConnection) Close() error {
	connection.listener.mu.Lock()
	delete(connection.listener.connections, connection)
	connection.listener.mu.Unlock()
	return connection.Conn.Close()
}

type temporalAuthProxy struct {
	listener           *trackedTemporalListener
	server             *grpc.Server
	backend            *grpc.ClientConn
	mu                 sync.Mutex
	currentKey         string
	acceptedKeys       []string
	rejectedKeys       []string
	clientCertificates []string
	hasNamespaceHeader bool
}

func newTemporalAuthProxy(t *testing.T, tlsConfig *tls.Config, initialKey string) *temporalAuthProxy {
	t.Helper()
	backendAddress := *temporalHostPort
	if backendAddress == "" {
		backendAddress = "127.0.0.1:7233"
	}
	backend, err := grpc.NewClient(backendAddress, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultCallOptions(grpc.ForceCodec(temporalProxyCodec{})))
	require.NoError(t, err)
	baseListener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	listener := &trackedTemporalListener{Listener: baseListener, connections: make(map[net.Conn]struct{})}
	proxy := &temporalAuthProxy{listener: listener, backend: backend, currentKey: initialKey}
	serverOptions := []grpc.ServerOption{grpc.ForceServerCodec(temporalProxyCodec{}), grpc.UnknownServiceHandler(proxy.handleStream)}
	if tlsConfig != nil {
		serverOptions = append(serverOptions, grpc.Creds(credentials.NewTLS(tlsConfig)))
	}
	proxy.server = grpc.NewServer(serverOptions...)
	serveError := make(chan error, 1)
	go func() { serveError <- proxy.server.Serve(listener) }()
	t.Cleanup(func() {
		proxy.server.Stop()
		require.NoError(t, backend.Close())
		if err := listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			require.NoError(t, err)
		}
		if err := <-serveError; err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			require.NoError(t, err)
		}
	})
	return proxy
}

func (proxy *temporalAuthProxy) handleStream(_ any, incoming grpc.ServerStream) error {
	method, ok := grpc.MethodFromServerStream(incoming)
	if !ok {
		return status.Error(codes.Internal, "missing method")
	}
	incomingMetadata, _ := metadata.FromIncomingContext(incoming.Context())
	key := ""
	if values := incomingMetadata.Get("authorization"); len(values) > 0 {
		key = values[0]
	}
	proxy.mu.Lock()
	if key != "" && key != "Bearer "+proxy.currentKey {
		proxy.rejectedKeys = append(proxy.rejectedKeys, key)
		proxy.mu.Unlock()
		return status.Error(codes.Unauthenticated, "invalid API key")
	}
	proxy.acceptedKeys = append(proxy.acceptedKeys, key)
	for _, namespace := range incomingMetadata.Get("temporal-namespace") {
		if namespace == testNamespace {
			proxy.hasNamespaceHeader = true
		}
	}
	if peerInfo, ok := peer.FromContext(incoming.Context()); ok {
		if tlsInfo, ok := peerInfo.AuthInfo.(credentials.TLSInfo); ok && len(tlsInfo.State.PeerCertificates) > 0 {
			proxy.clientCertificates = append(proxy.clientCertificates, tlsInfo.State.PeerCertificates[0].Subject.CommonName)
		}
	}
	proxy.mu.Unlock()
	outgoingContext := metadata.NewOutgoingContext(incoming.Context(), incomingMetadata)
	outgoing, err := proxy.backend.NewStream(outgoingContext, &grpc.StreamDesc{ClientStreams: true, ServerStreams: true}, method)
	if err != nil {
		return err
	}
	requestDone := make(chan error, 1)
	go func() { requestDone <- copyTemporalProxyRequests(incoming, outgoing) }()
	responseErr := copyTemporalProxyResponses(outgoing, incoming)
	requestErr := <-requestDone
	if responseErr != nil {
		return responseErr
	}
	return requestErr
}

func copyTemporalProxyRequests(incoming grpc.ServerStream, outgoing grpc.ClientStream) error {
	for {
		var frame temporalProxyFrame
		err := incoming.RecvMsg(&frame)
		if errors.Is(err, io.EOF) {
			return outgoing.CloseSend()
		}
		if err != nil {
			return err
		}
		if err := outgoing.SendMsg(&frame); err != nil {
			return err
		}
	}
}

func copyTemporalProxyResponses(outgoing grpc.ClientStream, incoming grpc.ServerStream) error {
	headers, err := outgoing.Header()
	if err != nil {
		return err
	}
	if err := incoming.SendHeader(headers); err != nil {
		return err
	}
	defer func() { incoming.SetTrailer(outgoing.Trailer()) }()
	for {
		var frame temporalProxyFrame
		err := outgoing.RecvMsg(&frame)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := incoming.SendMsg(&frame); err != nil {
			return err
		}
	}
}

func (proxy *temporalAuthProxy) setCurrentKey(key string) {
	proxy.mu.Lock()
	proxy.currentKey = key
	proxy.mu.Unlock()
}

func (proxy *temporalAuthProxy) snapshot() ([]string, []string, []string) {
	proxy.mu.Lock()
	defer proxy.mu.Unlock()
	return append([]string(nil), proxy.acceptedKeys...), append([]string(nil), proxy.rejectedKeys...), append([]string(nil), proxy.clientCertificates...)
}

func (proxy *temporalAuthProxy) observedNamespaceHeader() bool {
	proxy.mu.Lock()
	defer proxy.mu.Unlock()
	return proxy.hasNamespaceHeader
}
