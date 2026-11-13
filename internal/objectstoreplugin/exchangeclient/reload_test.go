// Copyright 2026 The Agent Substrate Authors
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

package exchangeclient

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
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"go.ate.dev/substrate/internal/objectstoreplugin/credential"
	exchangepb "go.ate.dev/substrate/internal/proto/objectstoreexchangepb"
	"go.ate.dev/substrate/pkg/objectstorage"
)

// stubExchange answers every exchange with one token.
type stubExchange struct {
	exchangepb.UnimplementedCredentialExchangeServer
}

func (stubExchange) ExchangeActorJwt(context.Context, *exchangepb.ExchangeActorJwtRequest) (*exchangepb.ExchangeActorJwtResponse, error) {
	return &exchangepb.ExchangeActorJwtResponse{AccessToken: "tok", ExpireTime: timestamppb.New(time.Now().Add(time.Hour))}, nil
}

// TestNew_ReloadsTheCAFile verifies that a rotation of the exchange CA bundle
// on disk is picked up by the next handshake, without rebuilding the Source:
// the node plugin outlives the rotation of the exchange's serving CA.
func TestNew_ReloadsTheCAFile(t *testing.T) {
	ca := newTestCA(t)
	caFile := filepath.Join(t.TempDir(), "ca.pem")
	writeFile(t, caFile, ca.pem())

	rotatedCA := newTestCA(t)
	addr := serveExchange(t, rotatedCA.issueServerCert(t))

	s, err := New(Config{Address: addr, CAFile: caFile, ServerName: "localhost"})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer s.Close()
	s.newClient = func(context.Context, credential.Token) (objectstorage.ObjectStorage, error) {
		return &fakeClient{}, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, err := s.Client(ctx, "jwt", false); status.Code(err) != codes.Unavailable {
		t.Fatalf("Client() against the rotated CA's cert before trusting it: error = %v, want a handshake failure", err)
	}

	// Step 1 of the rotation: the bundle carries the old and the new root.
	writeFile(t, caFile, append(ca.pem(), rotatedCA.pem()...))

	// The connection redials on its own after the failed handshake's backoff.
	for {
		_, err := s.Client(ctx, "jwt", false)
		if err == nil {
			return
		}
		if ctx.Err() != nil {
			t.Fatalf("Client() after the CA rotation: error = %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// serveExchange serves a stub exchange over TLS with cert and returns its
// address.
func serveExchange(t *testing.T, cert tls.Certificate) string {
	t.Helper()
	srv := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS13})))
	exchangepb.RegisterCredentialExchangeServer(srv, stubExchange{})
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)
	return lis.Addr().String()
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

func (ca *testCA) pem() []byte {
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ca.certDER})
}

func (ca *testCA) issueServerCert(t *testing.T) tls.Certificate {
	t.Helper()
	key := generateKey(t)
	template := &x509.Certificate{
		SerialNumber: big.NewInt(3),
		Subject:      pkix.Name{CommonName: "test-server"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		DNSNames:     []string{"localhost"},
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
