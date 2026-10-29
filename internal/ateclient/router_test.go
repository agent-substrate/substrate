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

package ateclient

import (
	"bufio"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"io"
	"math/big"
	"net"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"go.ate.dev/substrate/internal/atenet"
	"go.ate.dev/substrate/internal/resources"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func testTLSConfigs(t *testing.T) (serverCfg, clientCfg *tls.Config) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: routerServerName()},
		DNSNames:              []string{routerServerName()},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, pub, priv)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(cert)

	serverCfg = &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: priv}},
	}
	clientCfg = &tls.Config{
		MinVersion: tls.VersionTLS13,
		RootCAs:    roots,
		ServerName: routerServerName(),
	}
	return serverCfg, clientCfg
}

func testTCPPair(t *testing.T) (clientConn, serverConn net.Conn) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer lis.Close()

	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := lis.Accept()
		if err != nil {
			close(accepted)
			return
		}
		accepted <- conn
	}()

	c, err := net.Dial("tcp", lis.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	s := <-accepted
	t.Cleanup(func() {
		_ = c.Close()
		if s != nil {
			_ = s.Close()
		}
	})
	return c, s
}

func TestConnectActor(t *testing.T) {
	serverTLS, clientTLS := testTLSConfigs(t)
	clientRaw, serverRaw := testTCPPair(t)

	var gotReq *http.Request
	serverDone := make(chan error, 1)
	go func() {
		defer close(serverDone)
		tlsConn := tls.Server(serverRaw, serverTLS)
		defer tlsConn.Close()
		reader := bufio.NewReader(tlsConn)
		req, err := http.ReadRequest(reader)
		if err != nil {
			serverDone <- err
			return
		}
		gotReq = req
		if _, err := io.WriteString(tlsConn, "HTTP/1.1 200 OK\r\n\r\nhello from actor"); err != nil {
			serverDone <- err
			return
		}
		gotClient, err := io.ReadAll(reader)
		if err != nil {
			serverDone <- err
			return
		}
		if string(gotClient) != "ping" {
			serverDone <- io.ErrUnexpectedEOF
		}
	}()

	actorRef := resources.ActorRef{Atespace: "team", Name: "my-counter"}
	conn, err := connectActor(context.Background(), clientRaw, clientTLS, actorRef, 9090, http.Header{
		atenet.TargetActorHeader: []string{actorRef.String()},
	})
	if err != nil {
		t.Fatalf("connectActor: %v", err)
	}
	stopped := false
	conn.stop = func() { stopped = true }

	if _, err := io.WriteString(conn, "ping"); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := conn.CloseWrite(); err != nil {
		t.Fatalf("CloseWrite: %v", err)
	}
	got, err := io.ReadAll(conn)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := <-serverDone; err != nil {
		t.Fatalf("server: %v", err)
	}

	if gotReq.Method != http.MethodConnect {
		t.Errorf("Method = %q, want CONNECT", gotReq.Method)
	}
	if gotReq.Host != "my-counter:9090" {
		t.Errorf("Host = %q, want my-counter:9090", gotReq.Host)
	}
	if values, want := gotReq.Header.Values(atenet.TargetActorHeader), []string{"team/my-counter"}; !slices.Equal(values, want) {
		t.Errorf("%s = %q, want %q", atenet.TargetActorHeader, values, want)
	}
	if string(got) != "hello from actor" {
		t.Errorf("read = %q, want %q", got, "hello from actor")
	}
	if !stopped {
		t.Error("Close did not stop the port-forward")
	}
}

func TestConnectActorRejected(t *testing.T) {
	serverTLS, clientTLS := testTLSConfigs(t)
	clientRaw, serverRaw := testTCPPair(t)

	go func() {
		tlsConn := tls.Server(serverRaw, serverTLS)
		defer tlsConn.Close()
		_, _ = http.ReadRequest(bufio.NewReader(tlsConn))
		_, _ = io.WriteString(tlsConn, "HTTP/1.1 403 Forbidden\r\nContent-Length: 13\r\n\r\naccess denied")
	}()

	actorRef := resources.ActorRef{Atespace: "team", Name: "my-counter"}
	_, err := connectActor(context.Background(), clientRaw, clientTLS, actorRef, 80, http.Header{
		atenet.TargetActorHeader: []string{actorRef.String()},
	})
	if err == nil || !strings.Contains(err.Error(), "403 Forbidden: access denied") {
		t.Fatalf("connectActor error = %v, want 403 Forbidden: access denied", err)
	}
}

func TestConnectActorContextCanceled(t *testing.T) {
	serverTLS, clientTLS := testTLSConfigs(t)
	clientRaw, serverRaw := testTCPPair(t)

	ctx, cancel := context.WithCancel(context.Background())
	unblockServer := make(chan struct{})
	defer close(unblockServer)
	go func() {
		tlsConn := tls.Server(serverRaw, serverTLS)
		defer tlsConn.Close()
		_, _ = http.ReadRequest(bufio.NewReader(tlsConn))
		cancel()
		<-unblockServer
	}()

	actorRef := resources.ActorRef{Atespace: "team", Name: "my-counter"}
	_, err := connectActor(ctx, clientRaw, clientTLS, actorRef, 80, http.Header{
		atenet.TargetActorHeader: []string{actorRef.String()},
	})
	if err == nil {
		t.Fatal("connectActor succeeded after context cancellation, want error")
	}
}

func TestRouterClientCertsFallback(t *testing.T) {
	clientset := fake.NewClientset()
	clientset.Resources = []*metav1.APIResourceList{{
		GroupVersion: "certificates.k8s.io/v1",
		APIResources: []metav1.APIResource{{Name: "podcertificaterequests", Kind: "PodCertificateRequest", Namespaced: true}},
	}}
	createCalls := 0
	clientset.PrependReactor("create", "podcertificaterequests", func(k8stesting.Action) (bool, runtime.Object, error) {
		createCalls++
		return true, nil, io.ErrUnexpectedEOF
	})

	cached := &tls.Certificate{
		Leaf: &x509.Certificate{
			NotBefore: time.Now().Add(-40 * time.Minute),
			NotAfter:  time.Now().Add(20 * time.Minute),
		},
	}
	certs := &routerClientCerts{
		clientset: clientset,
		cert:      cached,
		refreshAt: time.Now().Add(-10 * time.Minute),
	}

	got, err := certs.get(context.Background())
	if err != nil {
		t.Fatalf("get with unexpired cached cert: %v", err)
	}
	if got != cached {
		t.Fatal("get did not fall back to cached certificate")
	}
	if createCalls != 1 {
		t.Fatalf("createCalls = %d, want 1", createCalls)
	}

	// A second call during the backoff window must reuse the cached cert
	// without hitting the API server again.
	if _, err := certs.get(context.Background()); err != nil {
		t.Fatalf("second get during backoff: %v", err)
	}
	if createCalls != 1 {
		t.Fatalf("createCalls after backoff call = %d, want 1", createCalls)
	}

	// If NotAfter is sooner than the 1-minute backoff, refreshAt is capped
	// at NotAfter so an expired cert is never handed out.
	shortNotAfter := time.Now().Add(5 * time.Second)
	certs.cert.Leaf.NotAfter = shortNotAfter
	certs.refreshAt = time.Now().Add(-1 * time.Minute)
	if _, err := certs.get(context.Background()); err != nil {
		t.Fatalf("get with short NotAfter: %v", err)
	}
	if !certs.refreshAt.Equal(shortNotAfter) {
		t.Fatalf("refreshAt = %v, want capped at NotAfter %v", certs.refreshAt, shortNotAfter)
	}

	certs.refreshAt = time.Now().Add(-1 * time.Minute)
	certs.cert.Leaf.NotAfter = time.Now().Add(-1 * time.Minute)
	if _, err := certs.get(context.Background()); err == nil {
		t.Fatal("get succeeded with expired cached cert, want error")
	}
}
