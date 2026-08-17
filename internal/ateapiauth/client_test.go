//  Copyright 2026 Google LLC
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package ateapiauth

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"k8s.io/client-go/kubernetes/fake"
)

func TestDialOptionsRequiresCAFile(t *testing.T) {
	for name, cfg := range map[string]ClientConfig{
		"token": {UseTokenAuth: true, TokenFile: "token"},
		"cert":  {ClientCredBundle: "bundle.pem"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := DialOptions(cfg)
			if err == nil {
				t.Fatalf("DialOptions() error = nil, want error")
			}
		})
	}
}

func TestDialOptionsRequiresModeCredential(t *testing.T) {
	fakeClient := fake.NewSimpleClientset()
	for name, cfg := range map[string]ClientConfig{
		"cert mode without bundle":              {K8sClient: fakeClient, CAFile: "ca.pem"},
		"token mode without token":              {K8sClient: fakeClient, CAFile: "ca.pem", UseTokenAuth: true},
		"cert path does not satisfy token mode": {K8sClient: fakeClient, CAFile: "ca.pem", UseTokenAuth: true, ClientCredBundle: "bundle.pem"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := DialOptions(cfg)
			if err == nil {
				t.Fatalf("DialOptions() error = nil, want error")
			}
		})
	}
}

// TestDialOptionsMTLSHandshake dials a server that requires and verifies
// client certificates — the configuration ateapi will move to — and checks
// that DialOptions with a client credential bundle completes the handshake,
// while a certificate-less client is rejected.
func TestDialOptionsMTLSHandshake(t *testing.T) {
	ca := newTestCA(t)
	dir := t.TempDir()
	caFile := filepath.Join(dir, "ca.pem")
	writeFile(t, caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ca.certDER}))

	clientBundle := filepath.Join(dir, "client-bundle.pem")
	writeFile(t, clientBundle, ca.issueClientBundle(t, "spiffe://cluster.local/ns/ate-system/sa/ate-controller"))

	serverCert := ca.issueServerCert(t)
	caPool := x509.NewCertPool()
	caPool.AddCert(ca.cert)
	srv := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{serverCert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    caPool,
		MinVersion:   tls.VersionTLS13,
	})))
	healthpb.RegisterHealthServer(srv, health.NewServer())
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go srv.Serve(lis)
	defer srv.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	t.Run("with client cert", func(t *testing.T) {
		// TokenFile is ignored in cert mode (the default).
		opts, err := DialOptions(ClientConfig{
			K8sClient:        fake.NewSimpleClientset(),
			CAFile:           caFile,
			ClientCredBundle: clientBundle,
			TokenFile:        filepath.Join(dir, "does-not-exist-token"),
		})
		if err != nil {
			t.Fatalf("DialOptions() error = %v", err)
		}
		if code := healthCheckCode(ctx, t, lis.Addr().String(), opts); code != codes.OK {
			t.Fatalf("health check code = %v, want %v", code, codes.OK)
		}
	})

	t.Run("without client cert is rejected", func(t *testing.T) {
		opts := []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
			RootCAs:    caPool,
			MinVersion: tls.VersionTLS13,
		}))}
		if code := healthCheckCode(ctx, t, lis.Addr().String(), opts); code == codes.OK {
			t.Fatalf("health check code = %v, want handshake failure", code)
		}
	})
}

// TestDialOptionsTokenSendsBearer dials a server that accepts certless
// clients — ateapi's current configuration — and checks that DialOptions
// with a token file attaches the token as an `authorization: Bearer` header
// on every RPC.
func TestDialOptionsTokenSendsBearer(t *testing.T) {
	ca := newTestCA(t)
	dir := t.TempDir()
	caFile := filepath.Join(dir, "ca.pem")
	writeFile(t, caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ca.certDER}))
	tokenFile := filepath.Join(dir, "token")
	writeFile(t, tokenFile, []byte("test-token\n"))

	gotAuth := make(chan string, 1)
	srv := grpc.NewServer(
		grpc.Creds(credentials.NewTLS(&tls.Config{
			Certificates: []tls.Certificate{ca.issueServerCert(t)},
			ClientAuth:   tls.VerifyClientCertIfGiven,
			MinVersion:   tls.VersionTLS13,
		})),
		grpc.UnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			md, _ := metadata.FromIncomingContext(ctx)
			gotAuth <- strings.Join(md.Get("authorization"), ",")
			return handler(ctx, req)
		}),
	)
	healthpb.RegisterHealthServer(srv, health.NewServer())
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go srv.Serve(lis)
	defer srv.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// ClientCredBundle stays set (as the base manifests leave it) but is
	// ignored in token mode — it doesn't even need to exist on disk.
	opts, err := DialOptions(ClientConfig{
		K8sClient:        fake.NewSimpleClientset(),
		CAFile:           caFile,
		UseTokenAuth:     true,
		TokenFile:        tokenFile,
		ClientCredBundle: filepath.Join(dir, "does-not-exist.pem"),
	})
	if err != nil {
		t.Fatalf("DialOptions() error = %v", err)
	}
	if code := healthCheckCode(ctx, t, lis.Addr().String(), opts); code != codes.OK {
		t.Fatalf("health check code = %v, want %v", code, codes.OK)
	}
	if got, want := <-gotAuth, "Bearer test-token"; got != want {
		t.Errorf("authorization header = %q, want %q", got, want)
	}
}

func healthCheckCode(ctx context.Context, t *testing.T, target string, opts []grpc.DialOption) codes.Code {
	t.Helper()
	conn, err := grpc.NewClient(target, opts...)
	if err != nil {
		t.Fatalf("grpc.NewClient() error = %v", err)
	}
	defer conn.Close()
	_, err = healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
	return status.Code(err)
}

type testCA struct {
	cert    *x509.Certificate
	certDER []byte
	key     *ecdsa.PrivateKey
}

func newTestCA(t *testing.T) *testCA {
	t.Helper()
	key := generateKey(t)
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create CA certificate: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse CA certificate: %v", err)
	}
	return &testCA{cert: cert, certDER: der, key: key}
}

// issueClientBundle returns a PEM credential bundle (leaf certificate + PKCS8
// private key) for a client certificate carrying the given SPIFFE URI SAN.
func (ca *testCA) issueClientBundle(t *testing.T, spiffeID string) []byte {
	t.Helper()
	uri, err := url.Parse(spiffeID)
	if err != nil {
		t.Fatalf("parse SPIFFE ID: %v", err)
	}
	key := generateKey(t)
	template := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "test-client"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		URIs:         []*url.URL{uri},
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatalf("create client certificate: %v", err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatalf("marshal PKCS8 key: %v", err)
	}
	return append(
		pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}),
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})...,
	)
}

func (ca *testCA) issueServerCert(t *testing.T) tls.Certificate {
	t.Helper()
	key := generateKey(t)
	template := &x509.Certificate{
		SerialNumber: big.NewInt(3),
		Subject:      pkix.Name{CommonName: "test-server"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatalf("create server certificate: %v", err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

func generateKey(t *testing.T) *ecdsa.PrivateKey {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	return key
}

func writeFile(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}
