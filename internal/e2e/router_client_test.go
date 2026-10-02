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

package e2e

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/atenet"
	"github.com/agent-substrate/substrate/internal/resources"
)

func TestRouterClientPostJSON(t *testing.T) {
	client := &RouterClient{
		baseURL: "http://router.test",
		http: &http.Client{Transport: testRoundTripper(func(request *http.Request) (*http.Response, error) {
			if got := request.Header.Get(atenet.TargetActorHeader); got != "demo/fetcher" {
				t.Errorf("target actor = %q, want demo/fetcher", got)
			}
			if request.Method != http.MethodPost {
				t.Errorf("method = %q, want POST", request.Method)
			}
			if request.Host != "router.test" {
				t.Errorf("host = %q, want router.test", request.Host)
			}
			if request.URL.Path != "/fetch" {
				t.Errorf("path = %q, want /fetch", request.URL.Path)
			}
			if request.Header.Get("Content-Type") != "application/json" {
				t.Errorf("content type = %q, want application/json", request.Header.Get("Content-Type"))
			}
			body, err := io.ReadAll(request.Body)
			if err != nil {
				t.Fatalf("reading body: %v", err)
			}
			if string(body) != `{"url":"https://example.com/"}` {
				t.Errorf("body = %q", body)
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader("ok")),
				Header:     make(http.Header),
			}, nil
		})},
	}

	actorRef := resources.ActorRef{Atespace: "demo", Name: "fetcher"}
	response, err := client.PostJSON(context.Background(), actorRef, "/fetch", []byte(`{"url":"https://example.com/"}`))
	if err != nil {
		t.Fatalf("PostJSON: %v", err)
	}
	response.Body.Close()
}

type testRoundTripper func(*http.Request) (*http.Response, error)

func (f testRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

// With the router in static-mtls mode, Connect has to complete a TLS
// handshake, presenting the client certificate, before it sends the CONNECT.
func TestRouterClientConnectTLS(t *testing.T) {
	clientCert := selfSignedClientCert(t, "spiffe://cluster.local/ns/test/sa/client")
	clientCAs := x509.NewCertPool()
	clientCAs.AddCert(clientCert.Leaf)

	var gotPeer string
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodConnect {
			t.Errorf("method = %q, want CONNECT", r.Method)
		}
		if r.Host != "fetcher:9090" {
			t.Errorf("CONNECT authority = %q, want fetcher:9090", r.Host)
		}
		if got := r.Header.Get(atenet.TargetActorHeader); got != "demo/fetcher" {
			t.Errorf("target actor = %q, want demo/fetcher", got)
		}
		if len(r.TLS.PeerCertificates) > 0 && len(r.TLS.PeerCertificates[0].URIs) > 0 {
			gotPeer = r.TLS.PeerCertificates[0].URIs[0].String()
		}
		conn, buf, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Errorf("hijacking: %v", err)
			return
		}
		defer conn.Close()
		_, _ = buf.WriteString("HTTP/1.1 200 OK\r\n\r\ntunneled")
		_ = buf.Flush()
	}))
	server.TLS = &tls.Config{ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: clientCAs}
	server.StartTLS()
	defer server.Close()

	tlsConfig := &tls.Config{
		RootCAs:      x509.NewCertPool(),
		Certificates: []tls.Certificate{clientCert},
		ServerName:   "example.com",
	}
	tlsConfig.RootCAs.AddCert(server.Certificate())
	client := &RouterClient{tlsConfig: tlsConfig, connectAddr: server.Listener.Addr().String()}
	client.connectOnce.Do(func() {})

	conn, err := client.Connect(context.Background(), resources.ActorRef{Atespace: "demo", Name: "fetcher"}, 9090)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer conn.Close()
	if _, ok := conn.(*bufferedConn).Conn.(*tls.Conn); !ok {
		t.Errorf("Connect returned a %T tunnel, want one over TLS", conn.(*bufferedConn).Conn)
	}
	got, err := io.ReadAll(conn)
	if err != nil {
		t.Fatalf("reading the tunnel: %v", err)
	}
	if string(got) != "tunneled" {
		t.Errorf("tunnel carried %q, want %q", got, "tunneled")
	}
	if gotPeer != "spiffe://cluster.local/ns/test/sa/client" {
		t.Errorf("router saw client identity %q, want the presented certificate's", gotPeer)
	}
}

// selfSignedClientCert returns a self-signed client certificate carrying uri
// as its only URI SAN.
func selfSignedClientCert(t *testing.T, uri string) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(uri)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		URIs:                  []*url.URL{u},
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf}
}
