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

package egress

import (
	"context"
	"crypto/tls"
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

func TestConfigForPEPAddress(t *testing.T) {
	listeners := []Listener{{Port: 15001}}
	cfg, ok := ConfigForPEPAddress("ate-egress.example:15008", listeners)
	if !ok {
		t.Fatal("ConfigForPEPAddress() ok = false, want true")
	}
	if cfg.PEPAddress != "ate-egress.example:15008" {
		t.Fatalf("cfg.PEPAddress = %q, want ate-egress.example:15008", cfg.PEPAddress)
	}
	if len(cfg.Listeners) != 1 || cfg.Listeners[0].Port != 15001 {
		t.Fatalf("cfg.Listeners = %+v, want port 15001", cfg.Listeners)
	}
}

func TestConfigForPEPAddressEmpty(t *testing.T) {
	if _, ok := ConfigForPEPAddress("", nil); ok {
		t.Fatal("ConfigForPEPAddress() ok = true, want false")
	}
}

func TestNewConnectRequestUsesConfiguredAuthority(t *testing.T) {
	originalDst := &net.TCPAddr{IP: net.ParseIP("203.0.113.10"), Port: 443}
	req, pr, pw := newConnectRequest(context.Background(), ActorIdentity{
		Namespace: "default",
		Template:  "counter",
		ActorID:   "my-counter-1",
		Atespace:  "team-a",
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
	if got := req.Header.Get("x-ate-atespace"); got != "team-a" {
		t.Fatalf("x-ate-atespace = %q, want team-a", got)
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

func TestDeriveConnectAuthorityFromTLSClientHelloSNIOnAnyPort(t *testing.T) {
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

	authority, initialBytes := deriveConnectAuthority(context.Background(), serverConn, &net.TCPAddr{
		IP:   net.ParseIP("203.0.113.10"),
		Port: 8443,
	})
	if authority != "httpbin.org:8443" {
		t.Fatalf("deriveConnectAuthority() authority = %q, want httpbin.org:8443", authority)
	}
	if len(initialBytes) == 0 {
		t.Fatal("deriveConnectAuthority() returned no initial bytes")
	}

	_ = clientConn.Close()
	<-errCh
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

func TestDeriveConnectAuthorityFromHTTPHostOnAnyPort(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	errCh := make(chan error, 1)
	go func() {
		_, err := clientConn.Write([]byte("GET /get HTTP/1.1\r\nHost: httpbin.org\r\n\r\n"))
		errCh <- err
	}()

	authority, initialBytes := deriveConnectAuthority(context.Background(), serverConn, &net.TCPAddr{
		IP:   net.ParseIP("203.0.113.10"),
		Port: 8080,
	})
	if authority != "httpbin.org:8080" {
		t.Fatalf("deriveConnectAuthority() authority = %q, want httpbin.org:8080", authority)
	}
	if string(initialBytes) != "GET /get HTTP/1.1\r\nHost: httpbin.org\r\n\r\n" {
		t.Fatalf("initial bytes = %q", string(initialBytes))
	}
	if err := <-errCh; err != nil {
		t.Fatalf("client write returned error: %v", err)
	}
}

func TestDeriveConnectAuthorityFallsBackToOriginalDestination(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	go func() {
		_, _ = clientConn.Write([]byte("not http or tls"))
		_ = clientConn.Close()
	}()

	originalDst := &net.TCPAddr{IP: net.ParseIP("203.0.113.10"), Port: 2222}
	authority, initialBytes := deriveConnectAuthority(context.Background(), serverConn, originalDst)
	if authority != originalDst.String() {
		t.Fatalf("deriveConnectAuthority() authority = %q, want %q", authority, originalDst.String())
	}
	if string(initialBytes) != "not http or tls" {
		t.Fatalf("initial bytes = %q", string(initialBytes))
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

func TestConnectStreamCloseClosesPipes(t *testing.T) {
	pr, pw := io.Pipe()
	defer pr.Close()

	stream := &connectStream{
		requestWriter: pw,
		responseBody:  io.NopCloser(strings.NewReader("")),
	}

	if err := stream.Close(); err != nil {
		t.Fatalf("stream.Close() returned error: %v", err)
	}
	// The request pipe is closed: a subsequent write fails.
	if _, err := pw.Write([]byte("x")); err == nil {
		t.Fatal("stream.Close() did not close the request writer")
	}
}

func TestHTTPHeadersCompleteDetectsMarkerSplitAcrossReads(t *testing.T) {
	// The "\r\n\r\n" marker is delivered across two calls: "...\r\n\r" then "\n".
	// The incremental search must still detect it via the small re-scan overlap.
	scanned := 0
	first := []byte("GET / HTTP/1.1\r\nHost: example.com\r\n\r")
	if httpHeadersComplete(first, &scanned) {
		t.Fatal("httpHeadersComplete() = true on partial marker, want false")
	}
	if scanned != len(first) {
		t.Fatalf("scanned = %d, want %d", scanned, len(first))
	}
	full := append(first, '\n')
	if !httpHeadersComplete(full, &scanned) {
		t.Fatal("httpHeadersComplete() = false after marker completed, want true")
	}
}

func TestDeriveConnectAuthorityFromHTTPHostByteAtATime(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	request := "GET /get HTTP/1.1\r\nHost: httpbin.org\r\n\r\n"
	go func() {
		for i := 0; i < len(request); i++ {
			if _, err := clientConn.Write([]byte{request[i]}); err != nil {
				return
			}
		}
	}()

	authority, _ := deriveConnectAuthority(context.Background(), serverConn, &net.TCPAddr{
		IP:   net.ParseIP("203.0.113.10"),
		Port: 80,
	})
	if authority != "httpbin.org:80" {
		t.Fatalf("deriveConnectAuthority() authority = %q, want httpbin.org:80", authority)
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
