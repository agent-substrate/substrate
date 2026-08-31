// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package atunnel

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestRelayIngressWithHalfClose(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := listener.Accept()
		if err == nil {
			accepted <- conn
		}
	}()
	actor, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer actor.Close()
	upstream := <-accepted
	defer upstream.Close()

	clientReader, clientInput := io.Pipe()
	defer clientReader.Close()
	var clientOutput bytes.Buffer
	done := make(chan struct{})
	go func() {
		relayIngressWithHalfClose(context.Background(), upstream, clientReader, &clientOutput, clientReader)
		close(done)
	}()

	if _, err := io.WriteString(clientInput, "request"); err != nil {
		t.Fatal(err)
	}
	if err := clientInput.Close(); err != nil {
		t.Fatal(err)
	}
	if err := actor.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	request, err := io.ReadAll(actor)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(request); got != "request" {
		t.Fatalf("actor received %q, want request", got)
	}

	if _, err := io.WriteString(actor, "response"); err != nil {
		t.Fatal(err)
	}
	if err := actor.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("relay did not finish after the actor response ended")
	}
	if got := clientOutput.String(); got != "response" {
		t.Errorf("client received %q, want response", got)
	}
}

func TestRelayIngressCancellationClosesBothSides(t *testing.T) {
	upstream, actor := net.Pipe()
	defer actor.Close()
	clientReader, clientInput := io.Pipe()
	defer clientInput.Close()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		relayIngressWithHalfClose(ctx, upstream, clientReader, io.Discard, clientReader)
		close(done)
	}()

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("relay did not return after cancellation")
	}
	// Deadline only guards against hanging; it fails once the relay closed the pipe.
	_ = actor.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := actor.Read(make([]byte, 1)); err == nil {
		t.Fatal("actor connection remained open after relay cancellation")
	}
	if _, err := io.WriteString(clientInput, "request"); err == nil {
		t.Fatal("client stream remained open after relay cancellation")
	}
}

func TestServeHTTP(t *testing.T) {
	upstreamHost := make(chan string, 4)
	upstreamURL, err := url.Parse("http://actor.internal:80")
	if err != nil {
		t.Fatal(err)
	}

	s := newTestServer(t, upstreamURL)
	s.proxy.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		upstreamHost <- r.Host
		return &http.Response{
			StatusCode: http.StatusNoContent,
			Header:     make(http.Header),
			Body:       http.NoBody,
		}, nil
	})
	if err := s.Activate("team-a", "actor-1"); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name         string
		host         string
		originalHost string
		wantStatus   int
	}{
		{name: "active actor", host: "actor-1.team-a.actors.resources.substrate.ate.dev", wantStatus: http.StatusNoContent},
		{name: "active actor with port", host: "actor-1.team-a.actors.resources.substrate.ate.dev:443", wantStatus: http.StatusNoContent},
		{name: "DNS case insensitive", host: "ACTOR-1.TEAM-A.ACTORS.RESOURCES.SUBSTRATE.ATE.DEV", wantStatus: http.StatusNoContent},
		{name: "router original host", host: "10.0.0.52:443", originalHost: "actor-1.team-a.actors.resources.substrate.ate.dev", wantStatus: http.StatusNoContent},
		{name: "wrong actor", host: "actor-2.team-a.actors.resources.substrate.ate.dev", wantStatus: http.StatusMisdirectedRequest},
		{name: "wrong atespace", host: "actor-1.team-b.actors.resources.substrate.ate.dev", wantStatus: http.StatusMisdirectedRequest},
		{name: "suffix confusion", host: "actor-1.team-a.actors.resources.substrate.ate.dev.example.com", wantStatus: http.StatusMisdirectedRequest},
		{name: "malformed port", host: "actor-1.team-a.actors.resources.substrate.ate.dev:nope", wantStatus: http.StatusMisdirectedRequest},
		{name: "empty", wantStatus: http.StatusMisdirectedRequest},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "https://worker/hello", nil)
			req.Host = tt.host
			if tt.originalHost != "" {
				req.Header.Set(OriginalHostHeader, tt.originalHost)
			}
			rec := httptest.NewRecorder()
			s.ServeHTTP(rec, req)
			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d", rec.Code, tt.wantStatus)
			}
			if tt.wantStatus == http.StatusMisdirectedRequest && rec.Header().Get(StaleAssignmentHeader) != "true" {
				t.Errorf("missing %s response header", StaleAssignmentHeader)
			}
		})
	}

	for range 4 {
		if got := <-upstreamHost; got != "actor-1.team-a.actors.resources.substrate.ate.dev" && got != "actor-1.team-a.actors.resources.substrate.ate.dev:443" && got != "ACTOR-1.TEAM-A.ACTORS.RESOURCES.SUBSTRATE.ATE.DEV" {
			t.Errorf("upstream Host = %q", got)
		}
	}
}

func TestServeHTTPHonorsTargetPortHeader(t *testing.T) {
	upstreamURL, err := url.Parse("http://actor.internal:80")
	if err != nil {
		t.Fatal(err)
	}

	s := newTestServer(t, upstreamURL)
	var gotURLHost, gotHost, gotHeader string
	s.proxy.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		gotURLHost = r.URL.Host
		gotHost = r.Host
		gotHeader = r.Header.Get(TargetPortHeader)
		return &http.Response{
			StatusCode: http.StatusNoContent,
			Header:     make(http.Header),
			Body:       http.NoBody,
		}, nil
	})
	if err := s.Activate("team-a", "actor-1"); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name           string
		targetPort     string
		wantDialedHost string
	}{
		{"arbitrary port overrides upstream's", "9090", "actor.internal:9090"},
		{"absent falls back to upstream's own port", "", "actor.internal:80"},
		{"invalid falls back to upstream's own port", "not-a-port", "actor.internal:80"},
		{"out of range falls back to upstream's own port", "70000", "actor.internal:80"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "https://worker/hello", nil)
			req.Host = "actor-1.team-a.actors.resources.substrate.ate.dev"
			if tt.targetPort != "" {
				req.Header.Set(TargetPortHeader, tt.targetPort)
			}
			rec := httptest.NewRecorder()
			s.ServeHTTP(rec, req)
			if rec.Code != http.StatusNoContent {
				t.Fatalf("status = %d, want %d", rec.Code, http.StatusNoContent)
			}
			if gotURLHost != tt.wantDialedHost {
				t.Errorf("dialed host = %q, want %q", gotURLHost, tt.wantDialedHost)
			}
			if gotHost != "actor-1.team-a.actors.resources.substrate.ate.dev" {
				t.Errorf("Host header changed to %q; the actor should see its stable mesh hostname", gotHost)
			}
			if gotHeader != "" {
				t.Errorf("%s leaked to the actor upstream: %q", TargetPortHeader, gotHeader)
			}
		})
	}
}

func TestServeConnectHTTPValidatesMethodAndAuthority(t *testing.T) {
	upstreamURL, err := url.Parse("http://actor.internal:80")
	if err != nil {
		t.Fatal(err)
	}
	s := newTestServer(t, upstreamURL)
	if err := s.Activate("team-a", "actor-1"); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name   string
		method string
		host   string
		want   int
	}{
		{name: "rejects non CONNECT", method: http.MethodGet, host: "actor-1.team-a.actors.resources.substrate.ate.dev:9090", want: http.StatusMethodNotAllowed},
		{name: "requires authority port", method: http.MethodConnect, host: "actor-1.team-a.actors.resources.substrate.ate.dev", want: http.StatusBadRequest},
		{name: "rejects invalid authority port", method: http.MethodConnect, host: "actor-1.team-a.actors.resources.substrate.ate.dev:70000", want: http.StatusMisdirectedRequest},
		{name: "rejects inactive actor", method: http.MethodConnect, host: "actor-2.team-a.actors.resources.substrate.ate.dev:9090", want: http.StatusMisdirectedRequest},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(tt.method, "https://worker/", nil)
			req.Host = tt.host
			rec := httptest.NewRecorder()
			s.ServeConnectHTTP(rec, req)
			if rec.Code != tt.want {
				t.Fatalf("status = %d, want %d", rec.Code, tt.want)
			}
		})
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

type idleClosingRoundTripper struct {
	closed bool
}

func (t *idleClosingRoundTripper) RoundTrip(*http.Request) (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusNoContent,
		Header:     make(http.Header),
		Body:       http.NoBody,
	}, nil
}

func (t *idleClosingRoundTripper) CloseIdleConnections() {
	t.closed = true
}

func TestDeactivateClosesIdleUpstreamConnections(t *testing.T) {
	upstream, err := url.Parse("http://actor.internal:80")
	if err != nil {
		t.Fatal(err)
	}
	s := newTestServer(t, upstream)
	transport := &idleClosingRoundTripper{}
	s.proxy.Transport = transport
	if err := s.Activate("team-a", "actor-1"); err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodGet, "https://worker/", nil)
	req.Host = "actor-1.team-a.actors.resources.substrate.ate.dev"
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNoContent)
	}
	if err := s.Deactivate(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !transport.closed {
		t.Fatal("Deactivate did not close idle upstream connections")
	}
}

func TestInactive(t *testing.T) {
	upstream, err := url.Parse("http://127.0.0.1:1")
	if err != nil {
		t.Fatal(err)
	}
	s := newTestServer(t, upstream)
	req := httptest.NewRequest(http.MethodGet, "https://worker/", nil)
	req.Host = "actor-1.team-a.actors.resources.substrate.ate.dev"

	for _, phase := range []string{"before activation", "after deactivation"} {
		t.Run(phase, func(t *testing.T) {
			rec := httptest.NewRecorder()
			s.ServeHTTP(rec, req)
			if rec.Code != http.StatusMisdirectedRequest {
				t.Fatalf("status = %d, want %d", rec.Code, http.StatusMisdirectedRequest)
			}
		})
		if phase == "before activation" {
			if err := s.Activate("team-a", "actor-1"); err != nil {
				t.Fatal(err)
			}
			if err := s.Deactivate(context.Background()); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestMutualTLSClientIdentity(t *testing.T) {
	dir := t.TempDir()
	ca := newTestCA(t)
	serverCert := ca.issue(t, "", []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth})
	bundlePath := filepath.Join(dir, "server.pem")
	trustPath := filepath.Join(dir, "trust.pem")
	writeCredentialBundle(t, bundlePath, serverCert)
	if err := os.WriteFile(trustPath, ca.certPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	upstream, err := url.Parse("http://actor.internal:80")
	if err != nil {
		t.Fatal(err)
	}
	s, err := NewServer(Config{
		CredentialBundlePath: bundlePath,
		TrustBundlePath:      trustPath,
		AllowedClientID:      "spiffe://cluster.local/ns/ate-system/sa/atenet-router",
		Upstream:             upstream,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := s.tlsConfig.NextProtos; len(got) != 0 {
		t.Fatalf("ordinary ingress ALPN protocols = %v, want none", got)
	}

	untrustedCA := newTestCA(t)
	tests := []struct {
		name    string
		cert    tls.Certificate
		wantErr bool
	}{
		{
			name: "allowed identity",
			cert: ca.issue(t, "spiffe://cluster.local/ns/ate-system/sa/atenet-router", []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}),
		},
		{
			name:    "wrong identity",
			cert:    ca.issue(t, "spiffe://cluster.local/ns/ate-system/sa/not-the-gateway", []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}),
			wantErr: true,
		},
		{
			name:    "untrusted identity",
			cert:    untrustedCA.issue(t, "spiffe://cluster.local/ns/ate-system/sa/atenet-router", []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}),
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			serverErr, clientErr := tlsHandshake(s.tlsConfig, &tls.Config{
				MinVersion:         tls.VersionTLS12,
				InsecureSkipVerify: true, // Test only the server's client authentication here.
				Certificates:       []tls.Certificate{tt.cert},
			})
			gotErr := serverErr != nil || clientErr != nil
			if gotErr != tt.wantErr {
				t.Fatalf("server error = %v, client error = %v, want error %v", serverErr, clientErr, tt.wantErr)
			}
		})
	}
}

// TestServeNegotiatesH2 checks that the ingress server negotiates h2 with a
// client that offers it, and HTTP/1.1 with one that does not. The router's
// HTTP/2 pool depends on the h2 side, which holds only because ServeTLS
// enables HTTP/2 when tlsConfig.NextProtos is empty — this pins that.
func TestServeNegotiatesH2(t *testing.T) {
	dir := t.TempDir()
	ca := newTestCA(t)
	serverCert := ca.issue(t, "", []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth})
	bundlePath := filepath.Join(dir, "server.pem")
	trustPath := filepath.Join(dir, "trust.pem")
	writeCredentialBundle(t, bundlePath, serverCert)
	if err := os.WriteFile(trustPath, ca.certPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	clientCert := ca.issue(t, "spiffe://cluster.local/ns/ate-system/sa/atenet-router", []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth})

	// The actor: an h2c-capable backend, so gRPC-shaped requests can arrive
	// as HTTP/2 while everything else must still be downgraded to HTTP/1.1.
	backendAddr, protoSeen := mirrorBackend(t, true, true)
	upstream, err := url.Parse("http://" + backendAddr)
	if err != nil {
		t.Fatal(err)
	}

	s, err := NewServer(Config{
		CredentialBundlePath: bundlePath,
		TrustBundlePath:      trustPath,
		AllowedClientID:      "spiffe://cluster.local/ns/ate-system/sa/atenet-router",
		Upstream:             upstream,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Activate("team-a", "actor-1"); err != nil {
		t.Fatal(err)
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan error, 1)
	go func() { served <- s.Serve(ctx, lis) }()
	t.Cleanup(func() {
		cancel()
		if err := <-served; err != nil {
			t.Errorf("Serve: %v", err)
		}
	})

	client := func(h2 bool) *http.Client {
		transport := &http.Transport{
			TLSClientConfig: &tls.Config{
				MinVersion:         tls.VersionTLS12,
				InsecureSkipVerify: true, // The handshake identity checks live in TestMutualTLSClientIdentity.
				Certificates:       []tls.Certificate{clientCert},
			},
			// With h2, the transport offers "h2" via ALPN like Envoy's
			// HTTP/2 pool; without it, only http/1.1 is offered, like the
			// HTTP/1.1 pool.
			ForceAttemptHTTP2: h2,
		}
		return &http.Client{Transport: transport, Timeout: 10 * time.Second}
	}
	request := func(t *testing.T, c *http.Client, method, contentType string) *http.Response {
		t.Helper()
		req, err := http.NewRequest(method, "https://"+lis.Addr().String()+"/", http.NoBody)
		if err != nil {
			t.Fatal(err)
		}
		req.Host = "actor-1.team-a.actors.resources.substrate.ate.dev"
		if contentType != "" {
			req.Header.Set("Content-Type", contentType)
		}
		res, err := c.Do(req)
		if err != nil {
			t.Fatalf("%s request: %v", method, err)
		}
		res.Body.Close()
		return res
	}

	h2Client := client(true)
	res := request(t, h2Client, http.MethodPost, "application/grpc")
	if res.Proto != "HTTP/2.0" {
		t.Fatalf("h2-only client negotiated %s, want HTTP/2.0 — Envoy's mirrored HTTP/2 pool cannot connect", res.Proto)
	}
	if got := <-protoSeen; got != "HTTP/2.0" {
		t.Errorf("gRPC-shaped request reached the actor as %s, want HTTP/2.0", got)
	}
	// A non-gRPC request on the same negotiated h2 connection is downgraded
	// before the actor.
	request(t, h2Client, http.MethodGet, "")
	if got := <-protoSeen; got != "HTTP/1.1" {
		t.Errorf("plain GET over h2 reached the actor as %s, want HTTP/1.1", got)
	}

	// Envoy's HTTP/1.1 pool offers only http/1.1; it must not be dragged onto h2.
	h1Client := client(false)
	res = request(t, h1Client, http.MethodGet, "")
	if res.Proto != "HTTP/1.1" {
		t.Errorf("http/1.1-only client negotiated %s, want HTTP/1.1", res.Proto)
	}
	if got := <-protoSeen; got != "HTTP/1.1" {
		t.Errorf("HTTP/1.1 request reached the actor as %s, want HTTP/1.1", got)
	}
}

func TestDeactivateCancelsInflightRequest(t *testing.T) {
	upstream, err := url.Parse("http://actor.internal:80")
	if err != nil {
		t.Fatal(err)
	}
	s := newTestServer(t, upstream)
	if err := s.Activate("team-a", "actor-1"); err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	s.proxy.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		close(started)
		<-r.Context().Done()
		return nil, r.Context().Err()
	})

	done := make(chan struct{})
	go func() {
		defer close(done)
		req := httptest.NewRequest(http.MethodGet, "https://worker/", nil)
		req.Host = "actor-1.team-a.actors.resources.substrate.ate.dev"
		s.ServeHTTP(httptest.NewRecorder(), req)
	}()
	<-started
	if err := s.Deactivate(context.Background()); err != nil {
		t.Fatal(err)
	}
	<-done
}

func newTestServer(t *testing.T, upstream *url.URL) *Server {
	t.Helper()
	dir := t.TempDir()
	bundle, trust := makeCertFiles(t, dir)
	s, err := NewServer(Config{
		CredentialBundlePath: bundle,
		TrustBundlePath:      trust,
		AllowedClientID:      "spiffe://cluster.local/ns/ate-system/sa/atenet-router",
		Upstream:             upstream,
	})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func makeCertFiles(t *testing.T, dir string) (bundlePath, trustPath string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test"},
		NotBefore:             now.Add(-time.Minute),
		NotAfter:              now.Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	bundlePath = filepath.Join(dir, "bundle.pem")
	trustPath = filepath.Join(dir, "trust.pem")
	if err := os.WriteFile(bundlePath, append(certPEM, keyPEM...), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(trustPath, certPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	return bundlePath, trustPath
}

type testCA struct {
	cert    *x509.Certificate
	key     *ecdsa.PrivateKey
	certPEM []byte
}

func newTestCA(t *testing.T) *testCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(now.UnixNano()),
		Subject:               pkix.Name{CommonName: "test CA"},
		NotBefore:             now.Add(-time.Minute),
		NotAfter:              now.Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return &testCA{
		cert:    cert,
		key:     key,
		certPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
	}
}

func (ca *testCA) issue(t *testing.T, spiffeID string, usages []x509.ExtKeyUsage) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber: big.NewInt(now.UnixNano()),
		NotBefore:    now.Add(-time.Minute),
		NotAfter:     now.Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  usages,
	}
	if spiffeID != "" {
		uri, err := url.Parse(spiffeID)
		if err != nil {
			t.Fatal(err)
		}
		template.URIs = []*url.URL{uri}
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := tls.X509KeyPair(
		append(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), ca.certPEM...),
		pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}),
	)
	if err != nil {
		t.Fatal(err)
	}
	return cert
}

func writeCredentialBundle(t *testing.T, path string, cert tls.Certificate) {
	t.Helper()
	key, ok := cert.PrivateKey.(*ecdsa.PrivateKey)
	if !ok {
		t.Fatalf("private key has type %T", cert.PrivateKey)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	var bundle []byte
	for _, der := range cert.Certificate {
		bundle = append(bundle, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})...)
	}
	bundle = append(bundle, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})...)
	if err := os.WriteFile(path, bundle, 0o600); err != nil {
		t.Fatal(err)
	}
}

func tlsHandshake(serverConfig, clientConfig *tls.Config) (serverErr, clientErr error) {
	serverConn, clientConn := net.Pipe()
	serverTLS := tls.Server(serverConn, serverConfig)
	clientTLS := tls.Client(clientConn, clientConfig)
	done := make(chan error, 1)
	go func() {
		err := serverTLS.Handshake()
		_ = serverConn.Close()
		done <- err
	}()
	clientErr = clientTLS.Handshake()
	_ = clientConn.Close()
	serverErr = <-done
	return serverErr, clientErr
}

func TestIsGRPC(t *testing.T) {
	for _, tt := range []struct {
		name        string
		protoMajor  int
		method      string
		contentType string
		want        bool
	}{
		{name: "grpc", protoMajor: 2, method: http.MethodPost, contentType: "application/grpc", want: true},
		{name: "grpc+proto", protoMajor: 2, method: http.MethodPost, contentType: "application/grpc+proto", want: true},
		{name: "grpc with params", protoMajor: 2, method: http.MethodPost, contentType: "application/grpc;charset=utf-8", want: true},
		{name: "uppercase content type", protoMajor: 2, method: http.MethodPost, contentType: "Application/GRPC", want: true},
		{name: "uppercase with subtype", protoMajor: 2, method: http.MethodPost, contentType: "APPLICATION/GRPC+PROTO", want: true},
		{name: "grpc-web is not grpc", protoMajor: 2, method: http.MethodPost, contentType: "application/grpc-web+proto", want: false},
		{name: "grpc content type over http/1.1", protoMajor: 1, method: http.MethodPost, contentType: "application/grpc", want: false},
		{name: "non-POST", protoMajor: 2, method: http.MethodGet, contentType: "application/grpc", want: false},
		{name: "plain h2 json", protoMajor: 2, method: http.MethodPost, contentType: "application/json", want: false},
		{name: "no content type", protoMajor: 2, method: http.MethodPost, contentType: "", want: false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			req, err := http.NewRequest(tt.method, "http://actor/", nil)
			if err != nil {
				t.Fatal(err)
			}
			req.ProtoMajor = tt.protoMajor
			if tt.contentType != "" {
				req.Header.Set("Content-Type", tt.contentType)
			}
			if got := isGRPC(req); got != tt.want {
				t.Errorf("isGRPC() = %v, want %v", got, tt.want)
			}
		})
	}
}

// mirrorBackend starts a backend speaking the given protocols and returns its
// address plus a channel yielding the protocol each request arrived with.
func mirrorBackend(t *testing.T, h1, h2c bool) (addr string, protoSeen chan string) {
	t.Helper()
	protoSeen = make(chan string, 1)
	protocols := new(http.Protocols)
	protocols.SetHTTP1(h1)
	protocols.SetUnencryptedHTTP2(h2c)
	backend := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			protoSeen <- r.Proto
		}),
		Protocols: protocols,
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go backend.Serve(lis)
	t.Cleanup(func() { backend.Close() })
	return lis.Addr().String(), protoSeen
}

// TestProtocolMirrorTransport verifies the upstream leg's protocol choice:
// only gRPC (HTTP/2 + POST + application/grpc*) goes out as cleartext
// prior-knowledge HTTP/2, preserving the trailers and streaming it needs;
// everything else — including non-gRPC requests that arrived over HTTP/2 —
// is sent as HTTP/1.1, so an HTTP/1.1-only actor keeps working no matter
// what protocol the client spoke at the edge.
func TestProtocolMirrorTransport(t *testing.T) {
	for _, tt := range []struct {
		name        string
		backendH1   bool
		backendH2C  bool
		protoMajor  int
		method      string
		contentType string
		want        string
		wantErr     bool
	}{
		// HTTP/1.1-only backend: the common actor. Every non-gRPC shape must
		// reach it as HTTP/1.1, whatever the client negotiated with the edge.
		{name: "h1 GET to h1-only actor", backendH1: true, protoMajor: 1, method: http.MethodGet, want: "HTTP/1.1"},
		{name: "h2 GET downgraded for h1-only actor", backendH1: true, protoMajor: 2, method: http.MethodGet, want: "HTTP/1.1"},
		{name: "h2 POST json downgraded for h1-only actor", backendH1: true, protoMajor: 2, method: http.MethodPost, contentType: "application/json", want: "HTTP/1.1"},
		{name: "grpc-web stays h1 for h1-only actor", backendH1: true, protoMajor: 2, method: http.MethodPost, contentType: "application/grpc-web+proto", want: "HTTP/1.1"},
		// gRPC to an actor that can't speak h2c must fail loudly (the proxy
		// surfaces it as 502) rather than silently fall back to HTTP/1.1,
		// which would strip the trailers gRPC needs.
		{name: "grpc to h1-only actor fails", backendH1: true, protoMajor: 2, method: http.MethodPost, contentType: "application/grpc", wantErr: true},
		// gRPC actor (h2c-capable): gRPC stays HTTP/2 end to end.
		{name: "grpc to grpc actor", backendH1: true, backendH2C: true, protoMajor: 2, method: http.MethodPost, contentType: "application/grpc", want: "HTTP/2.0"},
		{name: "grpc+proto to grpc actor", backendH1: true, backendH2C: true, protoMajor: 2, method: http.MethodPost, contentType: "application/grpc+proto", want: "HTTP/2.0"},
		// Mixed traffic to the same h2c-capable actor still downgrades
		// non-gRPC, mirroring what a browser or curl sends.
		{name: "h2 GET downgraded even for h2c-capable actor", backendH1: true, backendH2C: true, protoMajor: 2, method: http.MethodGet, want: "HTTP/1.1"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			addr, protoSeen := mirrorBackend(t, tt.backendH1, tt.backendH2C)
			transport := newProtocolMirrorTransport()
			req, err := http.NewRequest(tt.method, "http://"+addr+"/", http.NoBody)
			if err != nil {
				t.Fatal(err)
			}
			req.ProtoMajor = tt.protoMajor
			if tt.contentType != "" {
				req.Header.Set("Content-Type", tt.contentType)
			}
			res, err := transport.RoundTrip(req)
			if tt.wantErr {
				if err == nil {
					res.Body.Close()
					t.Fatal("RoundTrip succeeded, want an error (no silent HTTP/1.1 fallback for gRPC)")
				}
				return
			}
			if err != nil {
				t.Fatalf("RoundTrip: %v", err)
			}
			res.Body.Close()
			if got := <-protoSeen; got != tt.want {
				t.Errorf("upstream saw %s, want %s", got, tt.want)
			}
		})
	}
}
