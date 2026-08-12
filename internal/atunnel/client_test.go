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
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
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
