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

package actorlookup

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"go.ate.dev/substrate/internal/actoridjwt"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
)

const serverName = "api.ate-system.svc"

// fakeControl serves GetActor for one actor and records the client identity
// it saw.
type fakeControl struct {
	ateapipb.UnimplementedControlServer
	actor    *ateapipb.Actor
	peerURIs []*url.URL
}

func (f *fakeControl) GetActor(ctx context.Context, req *ateapipb.GetActorRequest) (*ateapipb.Actor, error) {
	if p, ok := peerCert(ctx); ok {
		f.peerURIs = p.URIs
	}
	if req.GetActor().GetAtespace() != f.actor.GetMetadata().GetAtespace() || req.GetActor().GetName() != f.actor.GetMetadata().GetName() {
		return nil, status.Errorf(codes.NotFound, "no actor %s/%s", req.GetActor().GetAtespace(), req.GetActor().GetName())
	}
	return f.actor, nil
}

func peerCert(ctx context.Context) (*x509.Certificate, bool) {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return nil, false
	}
	info, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok || len(info.State.PeerCertificates) == 0 {
		return nil, false
	}
	return info.State.PeerCertificates[0], true
}

// startServer serves fake over mTLS on the loopback and returns a Config that
// reaches it with a client certificate carrying spiffeID.
func startServer(t *testing.T, fake *fakeControl, spiffeID string) Config {
	t.Helper()
	ca := newTestCA(t)
	dir := t.TempDir()
	caFile := filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ca.certDER}), 0o600); err != nil {
		t.Fatal(err)
	}
	bundle := filepath.Join(dir, "client.pem")
	if err := os.WriteFile(bundle, ca.issueClientBundle(t, spiffeID), 0o600); err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(ca.cert)
	srv := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{ca.issueServerCert(t)},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    pool,
	})))
	ateapipb.RegisterControlServer(srv, fake)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return Config{Address: lis.Addr().String(), CAFile: caFile, ServerName: serverName, ClientCredBundle: bundle}
}

func testActor(uid string) *ateapipb.Actor {
	return &ateapipb.Actor{Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "agent-1", Uid: uid}}
}

func TestActor(t *testing.T) {
	const spiffeID = "spiffe://ate.dev/ns/ate-system/sa/ate-api-server"
	fake := &fakeControl{actor: testActor("uid-1")}
	l, err := New(startServer(t, fake, spiffeID))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer l.Close()

	claims := &actoridjwt.WireSubstrateClaims{Atespace: "team-a", ActorName: "agent-1", ActorUID: "uid-1"}
	actor, err := l.Actor(t.Context(), claims)
	if err != nil {
		t.Fatalf("Actor: %v", err)
	}
	if actor.GetMetadata().GetUid() != "uid-1" {
		t.Errorf("got actor %v", actor.GetMetadata())
	}
	if len(fake.peerURIs) != 1 || fake.peerURIs[0].String() != spiffeID {
		t.Errorf("server saw client identity %v, want %s", fake.peerURIs, spiffeID)
	}
}

func TestActor_Errors(t *testing.T) {
	fake := &fakeControl{actor: testActor("uid-1")}
	l, err := New(startServer(t, fake, "spiffe://ate.dev/ns/ate-system/sa/ate-api-server"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer l.Close()

	for _, tc := range []struct {
		name   string
		claims *actoridjwt.WireSubstrateClaims
		code   codes.Code
		is     error
	}{
		{"recreated actor", &actoridjwt.WireSubstrateClaims{Atespace: "team-a", ActorName: "agent-1", ActorUID: "uid-0"}, codes.Unknown, ErrStale},
		{"unknown actor", &actoridjwt.WireSubstrateClaims{Atespace: "team-a", ActorName: "agent-2", ActorUID: "uid-2"}, codes.NotFound, nil},
		{"incomplete claims", &actoridjwt.WireSubstrateClaims{Atespace: "team-a", ActorName: "agent-1"}, codes.Unknown, nil},
		{"nil claims", nil, codes.Unknown, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := l.Actor(t.Context(), tc.claims)
			if err == nil {
				t.Fatal("got an actor, want an error")
			}
			if tc.is != nil && !errors.Is(err, tc.is) {
				t.Errorf("err = %v, want %v", err, tc.is)
			}
			if status.Code(err) != tc.code {
				t.Errorf("code = %v, want %v (%v)", status.Code(err), tc.code, err)
			}
		})
	}
}

func TestActor_RefusesUnverifiedServer(t *testing.T) {
	// A server whose certificate the CA file does not sign must be refused,
	// otherwise a wrong loopback peer could answer for ate-api-server.
	cfg := startServer(t, &fakeControl{actor: testActor("uid-1")}, "spiffe://ate.dev/x")
	other := startServer(t, &fakeControl{actor: testActor("uid-1")}, "spiffe://ate.dev/x")
	cfg.Address = other.Address
	l, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer l.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	_, err = l.Actor(ctx, &actoridjwt.WireSubstrateClaims{Atespace: "team-a", ActorName: "agent-1", ActorUID: "uid-1"})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("err = %v, want Unavailable from a failed handshake", err)
	}
}

func TestNew_RequiresAddress(t *testing.T) {
	if _, err := New(Config{CAFile: "x", ClientCredBundle: "y"}); err == nil {
		t.Fatal("New accepted an empty Address")
	}
}

type testCA struct {
	cert    *x509.Certificate
	certDER []byte
	key     *ecdsa.PrivateKey
}

func generateKey(t *testing.T) *ecdsa.PrivateKey {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return key
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
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return &testCA{cert: cert, certDER: der, key: key}
}

func (ca *testCA) issueClientBundle(t *testing.T, spiffeID string) []byte {
	t.Helper()
	uri, err := url.Parse(spiffeID)
	if err != nil {
		t.Fatal(err)
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
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
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
		DNSNames:     []string{serverName},
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}
