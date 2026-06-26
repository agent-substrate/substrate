// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package egresscapture

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"
)

type contextKey string

func TestNewCaptureContextIgnoresParentCancellation(t *testing.T) {
	parent, parentCancel := context.WithCancel(context.WithValue(context.Background(), contextKey("trace"), "value"))
	parentCancel()

	ctx, cancel := newCaptureContext(parent)
	defer cancel()

	if err := ctx.Err(); err != nil {
		t.Fatalf("capture context is cancelled by parent: %v", err)
	}
	if got := ctx.Value(contextKey("trace")); got != "value" {
		t.Fatalf("capture context did not preserve values: got %v", got)
	}

	cancel()
	if err := ctx.Err(); err == nil {
		t.Fatal("capture context was not cancelled by its own cancel func")
	}
}

func TestConfigFromEnvRequiresPEPAddress(t *testing.T) {
	t.Setenv(EnvPEPAddress, "")

	_, err := ConfigFromEnv(nil)
	if err == nil {
		t.Fatal("ConfigFromEnv() returned nil error, want error")
	}
	if !strings.Contains(err.Error(), EnvPEPAddress) {
		t.Fatalf("ConfigFromEnv() error = %v, want %s", err, EnvPEPAddress)
	}
}

func TestConfigFromEnv(t *testing.T) {
	t.Setenv(EnvPEPAddress, "ate-egress.example:15008")
	t.Setenv(EnvTunnelProtocol, TunnelProtocolConnectTLS)
	t.Setenv(EnvConnectTLSServerName, "ate-egress.example")
	t.Setenv(EnvConnectTLSCAFile, "/run/egress-ca/ca.crt")
	t.Setenv(EnvConnectTLSInsecureSkipVerify, "true")

	listeners := []Listener{{Port: 15001}}
	cfg, err := ConfigFromEnv(listeners)
	if err != nil {
		t.Fatalf("ConfigFromEnv() returned error: %v", err)
	}
	if cfg.PEPAddress != "ate-egress.example:15008" {
		t.Fatalf("cfg.PEPAddress = %q, want ate-egress.example:15008", cfg.PEPAddress)
	}
	if cfg.Protocol != TunnelProtocolConnectTLS {
		t.Fatalf("cfg.Protocol = %q, want %s", cfg.Protocol, TunnelProtocolConnectTLS)
	}
	if cfg.TLS.ServerName != "ate-egress.example" {
		t.Fatalf("cfg.TLS.ServerName = %q, want ate-egress.example", cfg.TLS.ServerName)
	}
	if cfg.TLS.CAFile != "/run/egress-ca/ca.crt" {
		t.Fatalf("cfg.TLS.CAFile = %q, want /run/egress-ca/ca.crt", cfg.TLS.CAFile)
	}
	if !cfg.TLS.InsecureSkipVerify {
		t.Fatal("cfg.TLS.InsecureSkipVerify = false, want true")
	}
	if len(cfg.Listeners) != 1 || cfg.Listeners[0].Port != 15001 {
		t.Fatalf("cfg.Listeners = %+v, want port 15001", cfg.Listeners)
	}
}

func TestNewTunnelTransportUsesRegisteredFactories(t *testing.T) {
	for _, tc := range []struct {
		name     string
		protocol string
		wantType any
	}{
		{
			name:     "default connect",
			protocol: "",
			wantType: &PlaintextCONNECTTunnelTransport{},
		},
		{
			name:     "plaintext alias",
			protocol: TunnelProtocolPlaintext,
			wantType: &PlaintextCONNECTTunnelTransport{},
		},
		{
			name:     "tls connect alias",
			protocol: TunnelProtocolTLSConnect,
			wantType: &TLSCONNECTTunnelTransport{},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := NewTunnelTransport(Config{PEPAddress: "ate-egress.example:15008", Protocol: tc.protocol})
			if err != nil {
				t.Fatalf("NewTunnelTransport() returned error: %v", err)
			}
			if fmt.Sprintf("%T", got) != fmt.Sprintf("%T", tc.wantType) {
				t.Fatalf("NewTunnelTransport() = %T, want %T", got, tc.wantType)
			}
		})
	}

	if _, err := NewTunnelTransport(Config{Protocol: "does-not-exist"}); err == nil {
		t.Fatal("NewTunnelTransport() returned nil error for unsupported protocol")
	}
}

func TestNewConnectRequestUsesConfiguredAuthority(t *testing.T) {
	originalDst := &net.TCPAddr{IP: net.ParseIP("203.0.113.10"), Port: 443}
	req, pr, pw := newConnectRequest(context.Background(), "http", ActorIdentity{
		Namespace: "default",
		Template:  "counter",
		ActorID:   "my-counter-1",
	}, originalDst, "httpbin.org:443")
	defer pr.Close()
	defer pw.Close()

	if req.Host != "httpbin.org:443" {
		t.Fatalf("req.Host = %q, want httpbin.org:443", req.Host)
	}
	if req.URL.Host != "httpbin.org:443" {
		t.Fatalf("req.URL.Host = %q, want httpbin.org:443", req.URL.Host)
	}
	if got := req.Header.Get("x-ate-original-destination"); got != originalDst.String() {
		t.Fatalf("x-ate-original-destination = %q, want %q", got, originalDst.String())
	}
	if got := req.Header.Get("x-ate-connect-authority"); got != "httpbin.org:443" {
		t.Fatalf("x-ate-connect-authority = %q, want httpbin.org:443", got)
	}
}

func TestDeriveConnectAuthorityFromTLSClientHelloSNI(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	errCh := make(chan error, 1)
	go func() {
		tlsConn := tls.Client(clientConn, &tls.Config{
			ServerName:         "httpbin.org",
			InsecureSkipVerify: true,
		})
		errCh <- tlsConn.Handshake()
	}()

	originalDst := &net.TCPAddr{IP: net.ParseIP("203.0.113.10"), Port: 443}
	authority, initialBytes := deriveConnectAuthority(context.Background(), serverConn, originalDst)
	if authority != "httpbin.org:443" {
		t.Fatalf("deriveConnectAuthority() authority = %q, want httpbin.org:443", authority)
	}
	if len(initialBytes) == 0 {
		t.Fatal("deriveConnectAuthority() returned no initial bytes")
	}
	if _, ok, _ := tlsClientHelloSNI(initialBytes); !ok {
		t.Fatal("initial bytes do not contain a parseable TLS ClientHello SNI")
	}

	_ = clientConn.Close()
	if err := <-errCh; err == nil {
		t.Fatal("TLS handshake unexpectedly succeeded")
	} else if err != io.ErrClosedPipe && !strings.Contains(err.Error(), "closed") {
		t.Fatalf("TLS handshake error = %v, want closed connection", err)
	}
}

func TestDeriveConnectAuthorityFromHTTPHost(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	errCh := make(chan error, 1)
	go func() {
		_, err := clientConn.Write([]byte("GET /get HTTP/1.1\r\nHost: httpbin.org\r\nUser-Agent: test\r\n\r\n"))
		errCh <- err
	}()

	originalDst := &net.TCPAddr{IP: net.ParseIP("203.0.113.10"), Port: 80}
	authority, initialBytes := deriveConnectAuthority(context.Background(), serverConn, originalDst)
	if authority != "httpbin.org:80" {
		t.Fatalf("deriveConnectAuthority() authority = %q, want httpbin.org:80", authority)
	}
	if string(initialBytes) != "GET /get HTTP/1.1\r\nHost: httpbin.org\r\nUser-Agent: test\r\n\r\n" {
		t.Fatalf("initial bytes = %q", string(initialBytes))
	}
	if err := <-errCh; err != nil {
		t.Fatalf("client write returned error: %v", err)
	}
}

func TestProxyByteStreamStopsWhenContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	actorConn, actorPeer := net.Pipe()
	defer actorPeer.Close()
	tunnelConn, tunnelPeer := net.Pipe()
	defer tunnelPeer.Close()

	done := make(chan struct{})
	go func() {
		proxyByteStream(ctx, actorConn, tunnelConn, nil)
		close(done)
	}()

	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("proxyByteStream did not stop after context cancellation")
	}
}

func TestHBONEStreamCloseClosesIdleTransportConnections(t *testing.T) {
	pr, pw := io.Pipe()
	defer pr.Close()

	called := false
	stream := &hboneStream{
		requestWriter: pw,
		responseBody:  io.NopCloser(strings.NewReader("")),
		closeIdle: func() {
			called = true
		},
	}

	if err := stream.Close(); err != nil {
		t.Fatalf("stream.Close() returned error: %v", err)
	}
	if !called {
		t.Fatal("stream.Close() did not close idle transport connections")
	}
}

func TestHTTPHostHeaderWithPort(t *testing.T) {
	host, ok, needMore := httpHostHeader([]byte("GET / HTTP/1.1\r\nHost: example.com:8080\r\n\r\n"))
	if !ok || needMore {
		t.Fatalf("httpHostHeader() ok=%t needMore=%t, want ok=true needMore=false", ok, needMore)
	}
	if got := authorityWithDefaultPort(host, 80); got != "example.com:8080" {
		t.Fatalf("authorityWithDefaultPort() = %q, want example.com:8080", got)
	}
}
