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
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestClientDialContext(t *testing.T) {
	ca := newTestCA(t)
	request := make(chan *http.Request, 1)
	gatewayAddress := serveTestConnectGateway(t, ca, func(conn net.Conn, req *http.Request) {
		request <- req
		if _, err := io.WriteString(conn, "HTTP/1.1 200 Connection Established\r\n\r\nhello"); err != nil {
			t.Errorf("writing CONNECT response: %v", err)
			return
		}
		payload := make([]byte, len("ping"))
		if _, err := io.ReadFull(conn, payload); err != nil {
			t.Errorf("reading tunneled payload: %v", err)
			return
		}
		if string(payload) != "ping" {
			t.Errorf("tunneled payload = %q, want ping", payload)
		}
	})
	client := newTestClient(t, ca, WithDialer(dialFixedAddress(gatewayAddress)))

	conn, err := client.DialContext(context.Background(), "192.0.2.10:443")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	gotRequest := <-request
	if gotRequest.Method != http.MethodConnect {
		t.Errorf("method = %q, want CONNECT", gotRequest.Method)
	}
	if gotRequest.Host != "192.0.2.10:443" {
		t.Errorf("authority = %q, want 192.0.2.10:443", gotRequest.Host)
	}
	for name := range gotRequest.Header {
		if strings.HasPrefix(strings.ToLower(name), "x-ate-") {
			t.Errorf("legacy identity header %q was sent", name)
		}
	}
	if got := gotRequest.Header.Get("Authorization"); got != "" {
		t.Errorf("Authorization = %q, want empty", got)
	}

	buffered := make([]byte, len("hello"))
	if _, err := io.ReadFull(conn, buffered); err != nil {
		t.Fatal(err)
	}
	if string(buffered) != "hello" {
		t.Errorf("buffered payload = %q, want hello", buffered)
	}
	if _, err := io.WriteString(conn, "ping"); err != nil {
		t.Fatal(err)
	}
}

func TestClientDialContextRejected(t *testing.T) {
	ca := newTestCA(t)
	gatewayAddress := serveTestConnectGateway(t, ca, func(conn net.Conn, _ *http.Request) {
		body := "denied by policy"
		_, _ = fmt.Fprintf(conn, "HTTP/1.1 403 Forbidden\r\nContent-Length: %d\r\n\r\n%s", len(body), body)
	})
	client := newTestClient(t, ca, WithDialer(dialFixedAddress(gatewayAddress)))

	_, err := client.DialContext(context.Background(), "192.0.2.10:443")
	if err == nil || !strings.Contains(err.Error(), "denied by policy") {
		t.Fatalf("DialContext error = %v, want policy rejection", err)
	}
}

// TestClientDialContextGatewayRefusesClientCertificate pins the classification
// of a front-door refusal to ErrGatewayHandshake under both TLS versions, which
// is not one behavior but two.
//
// Under 1.2 the server rejects the certificate mid-handshake and
// HandshakeContext returns the error. Under 1.3 the client's handshake has
// already returned nil by the time the server looks at the certificate, so the
// refusal lands on the CONNECT exchange instead -- and 1.3 is what a real
// gateway negotiates. A caller asking "did the door turn me away?" has to get
// the same answer either way, or it is really asking which version was
// negotiated.
func TestClientDialContextGatewayRefusesClientCertificate(t *testing.T) {
	for _, tt := range []struct {
		name          string
		maxTLSVersion uint16
	}{
		{name: "TLS 1.3 refuses after the client handshake completes", maxTLSVersion: tls.VersionTLS13},
		{name: "TLS 1.2 refuses during the handshake", maxTLSVersion: tls.VersionTLS12},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ca := newTestCA(t)
			// The gateway trusts a different CA than the one that issued the
			// client's certificate, which is the shape of an actor presenting a
			// podidentity credential to a door that only accepts actor identity.
			gatewayAddress := serveTestRefusingGateway(t, ca, newTestCA(t), tt.maxTLSVersion)
			client := newTestClient(t, ca, WithDialer(dialFixedAddress(gatewayAddress)))

			_, err := client.DialContext(context.Background(), "192.0.2.10:443")
			if err == nil {
				t.Fatal("DialContext succeeded against a gateway that refuses the client certificate")
			}
			if !errors.Is(err, ErrGatewayHandshake) {
				t.Errorf("DialContext error = %v, want it to wrap ErrGatewayHandshake", err)
			}
			var rejected *ConnectRejectedError
			if errors.As(err, &rejected) {
				// A refusal at the door is not a CONNECT the gateway answered:
				// conflating them would let an authorization denial read as an
				// authentication failure.
				t.Errorf("DialContext error = %v, want no ConnectRejectedError", err)
			}
		})
	}
}

// TestClientDialContextGatewayHangsUpBeforeResponding covers the same window
// without an alert in it: the gateway closes the connection after the handshake
// and says nothing. There is still nobody but the front door on the other end,
// since no CONNECT response means no upstream was ever dialed.
func TestClientDialContextGatewayHangsUpBeforeResponding(t *testing.T) {
	ca := newTestCA(t)
	gatewayAddress := serveTestConnectGateway(t, ca, func(conn net.Conn, _ *http.Request) {
		_ = conn.Close()
	})
	client := newTestClient(t, ca, WithDialer(dialFixedAddress(gatewayAddress)))

	_, err := client.DialContext(context.Background(), "192.0.2.10:443")
	if err == nil {
		t.Fatal("DialContext succeeded against a gateway that hung up")
	}
	if !errors.Is(err, ErrGatewayHandshake) {
		t.Errorf("DialContext error = %v, want it to wrap ErrGatewayHandshake", err)
	}
}

// TestConnectExchangeError is the other half of gatewayHungUp: a failure on
// this side of the connection must not be reported as the door refusing, or
// "the gateway rejected my certificate" stops meaning anything. The cases are
// exercised directly because the ones worth pinning -- a deadline, a response
// the gateway did send but malformed -- are awkward to provoke through a real
// listener and trivial to state as errors.
func TestConnectExchangeError(t *testing.T) {
	for _, tt := range []struct {
		name          string
		err           error
		wantFrontDoor bool
	}{
		{
			name:          "peer TLS alert",
			err:           &net.OpError{Op: "remote error", Err: errors.New("tls: unknown certificate authority")},
			wantFrontDoor: true,
		},
		{
			name:          "peer closed without alerting",
			err:           io.EOF,
			wantFrontDoor: true,
		},
		{
			name:          "peer reset mid-response",
			err:           io.ErrUnexpectedEOF,
			wantFrontDoor: true,
		},
		{
			name:          "peer reset while the CONNECT was going out",
			err:           &net.OpError{Op: "write", Err: syscall.EPIPE},
			wantFrontDoor: true,
		},
		{
			name: "our own deadline expired",
			err:  os.ErrDeadlineExceeded,
		},
		{
			name: "the gateway answered, but not with HTTP",
			err:  errors.New("malformed HTTP response \"garbage\""),
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := connectExchangeError("reading CONNECT response", tt.err)
			if got := errors.Is(err, ErrGatewayHandshake); got != tt.wantFrontDoor {
				t.Errorf("errors.Is(%v, ErrGatewayHandshake) = %t, want %t", err, got, tt.wantFrontDoor)
			}
			if !errors.Is(err, tt.err) {
				t.Errorf("%v no longer unwraps to the underlying %v", err, tt.err)
			}
		})
	}
}

func TestClientDialContextValidatesInput(t *testing.T) {
	ca := newTestCA(t)
	client := newTestClient(t, ca)
	tests := []struct {
		name        string
		destination string
	}{
		{
			name:        "destination has no port",
			destination: "192.0.2.10",
		},
		{
			name:        "destination is a hostname",
			destination: "example.com:443",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := client.DialContext(context.Background(), tt.destination); err == nil {
				t.Fatal("DialContext unexpectedly succeeded")
			}
		})
	}
}

// dialFixedAddress ignores the requested address and connects to address, so
// tests can point a client at a listener on an ephemeral port.
func dialFixedAddress(address string) DialFunc {
	return func(ctx context.Context, network, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, address)
	}
}

func newTestClient(t *testing.T, ca *testCA, opts ...ClientOption) *Client {
	t.Helper()
	dir := t.TempDir()
	trustPath := filepath.Join(dir, "trust.pem")
	certificate := ca.issue(t,
		"spiffe://substrate-actor.local/atespace/team/actor/actor",
		[]x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	)
	if err := os.WriteFile(trustPath, ca.certPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	client, err := NewClient(ClientConfig{
		GatewayAddress:       "127.0.0.1:1",
		ServerName:           "egress.test",
		GetClientCertificate: func(*tls.CertificateRequestInfo) (*tls.Certificate, error) { return &certificate, nil },
		TrustBundlePath:      trustPath,
	}, opts...)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func serveTestConnectGateway(t *testing.T, ca *testCA, handle func(net.Conn, *http.Request)) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	serverCert := issueDNSCertificate(t, ca, "egress.test")
	clientCAs := x509.NewCertPool()
	clientCAs.AppendCertsFromPEM(ca.certPEM)
	tlsListener := tls.NewListener(listener, &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{serverCert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    clientCAs,
	})
	t.Cleanup(func() { _ = tlsListener.Close() })

	go func() {
		conn, err := tlsListener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		req, err := http.ReadRequest(bufio.NewReader(conn))
		if err != nil {
			t.Errorf("reading CONNECT request: %v", err)
			return
		}
		handle(conn, req)
	}()
	return listener.Addr().String()
}

// serveTestRefusingGateway serves a front door that presents a certificate from
// serverCA and requires a client certificate from clientCA, so a client holding
// one from anywhere else is turned away. maxVersion caps the negotiated TLS
// version, which is what decides whether the refusal reaches the client during
// its handshake or after it.
//
// It handshakes and closes rather than reading a request: a refused client
// never gets to send one, and any error here is the refusal working.
func serveTestRefusingGateway(t *testing.T, serverCA, clientCA *testCA, maxVersion uint16) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })

	clientCAs := x509.NewCertPool()
	clientCAs.AppendCertsFromPEM(clientCA.certPEM)
	config := &tls.Config{
		MinVersion:   tls.VersionTLS12,
		MaxVersion:   maxVersion,
		Certificates: []tls.Certificate{issueDNSCertificate(t, serverCA, "egress.test")},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    clientCAs,
	}

	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		tlsConn := tls.Server(conn, config)
		_ = tlsConn.Handshake()
		_ = tlsConn.Close()
	}()
	return listener.Addr().String()
}

func issueDNSCertificate(t *testing.T, ca *testCA, dnsName string) tls.Certificate {
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
		DNSNames:     []string{dnsName},
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
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
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}),
	)
	if err != nil {
		t.Fatal(err)
	}
	return cert
}
