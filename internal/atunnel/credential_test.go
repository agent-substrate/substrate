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
	"context"
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/internal/substratex509"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

func TestBrokerCertificateSourceMintsAndReusesKey(t *testing.T) {
	source, broker := newTestBrokerCertificateSource(t, testAteletIdentity("node-a"), time.Hour)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	for range 2 {
		if _, err := source.Mint(ctx); err != nil {
			t.Fatal(err)
		}
	}
	first, second := <-broker.publicKeys, <-broker.publicKeys
	if string(first) != string(second) {
		t.Fatal("renewal replaced the actor private key")
	}
	cert, err := source.GetClientCertificate(nil)
	if err != nil {
		t.Fatal(err)
	}
	identity, err := substratex509.ActorIdentityFromCertificate(cert.Leaf)
	if err != nil {
		t.Fatal(err)
	}
	if identity == nil || identity.ActorUid != "actor-uid" || identity.Purpose != substratex509.ActorIdentityPurposeAtunnel {
		t.Fatalf("actor identity = %+v", identity)
	}
}

func TestBrokerCertificateSourceRejectsAteletOnDifferentNode(t *testing.T) {
	source, _ := newTestBrokerCertificateSource(t, testAteletIdentity("node-b"), time.Hour)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := source.Mint(ctx); err == nil || !strings.Contains(err.Error(), "not on worker node") {
		t.Fatalf("Mint() error = %v, want node identity rejection", err)
	}
}

func TestBrokerCertificateSourceRejectsExpiredCertificate(t *testing.T) {
	source, _ := newTestBrokerCertificateSource(t, testAteletIdentity("node-a"), -time.Minute)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := source.Mint(ctx); err == nil || !strings.Contains(err.Error(), "invalid actor certificate lifetime") {
		t.Fatalf("Mint() error = %v, want expired certificate rejection", err)
	}
}

func TestBrokerCertificateSourceRejectsUnexpectedActor(t *testing.T) {
	source, broker := newTestBrokerCertificateSource(t, testAteletIdentity("node-a"), time.Hour)
	broker.actorUID = "another-actor-uid"
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := source.Mint(ctx); err == nil || !strings.Contains(err.Error(), "unexpected actor") {
		t.Fatalf("Mint() error = %v, want actor UID rejection", err)
	}
}

type credentialBrokerStub struct {
	ateletpb.UnimplementedCredentialBrokerServer
	ca         *testCA
	lifetime   time.Duration
	publicKeys chan []byte
	actorUID   string
}

func (s *credentialBrokerStub) MintActorCertificate(_ context.Context, req *ateletpb.MintActorCertificateRequest) (*ateletpb.MintActorCertificateResponse, error) {
	if req.GetExpectedActorUid() != "actor-uid" {
		return nil, status.Error(codes.FailedPrecondition, "unexpected actor UID")
	}
	csr, err := x509.ParseCertificateRequest(req.GetCertificateSigningRequest())
	if err != nil || csr.CheckSignature() != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid CSR")
	}
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber: big.NewInt(now.UnixNano()),
		Subject:      pkix.Name{CommonName: "actor"},
		NotBefore:    now.Add(-time.Minute),
		NotAfter:     now.Add(s.lifetime),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	if err := substratex509.AddActorIdentityToCertificate(&substratex509.ActorIdentity{Atespace: "team", ActorName: "actor", ActorUid: s.actorUID, Purpose: substratex509.ActorIdentityPurposeAtunnel}, template); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	der, err := x509.CreateCertificate(rand.Reader, template, s.ca.cert, csr.PublicKey, s.ca.key)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	s.publicKeys <- csr.RawSubjectPublicKeyInfo
	return &ateletpb.MintActorCertificateResponse{ActorCertificates: [][]byte{der}}, nil
}

func newTestBrokerCertificateSource(t *testing.T, ateletIdentity *substratex509.PodIdentity, lifetime time.Duration) (*BrokerCertificateSource, *credentialBrokerStub) {
	t.Helper()
	ca := newTestCA(t)
	workerCert := issueTestPodCertificate(t, ca, &substratex509.PodIdentity{
		Namespace:          "ate-demo",
		ServiceAccountName: "ateom",
		ServiceAccountUID:  "ateom-sa-uid",
		PodName:            "worker",
		PodUID:             "worker-uid",
		NodeName:           "node-a",
		NodeUID:            "node-uid",
	}, "spiffe://cluster.local/ns/ate-demo/sa/ateom", []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth})
	ateletCert := issueTestPodCertificate(t, ca, ateletIdentity,
		"spiffe://cluster.local/ns/ate-system/sa/atelet", []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth})

	dir := t.TempDir()
	credentialPath := filepath.Join(dir, "worker.pem")
	trustPath := filepath.Join(dir, "trust.pem")
	writeCredentialBundle(t, credentialPath, workerCert)
	if err := os.WriteFile(trustPath, ca.certPEM, 0o600); err != nil {
		t.Fatal(err)
	}

	clientCAs := x509.NewCertPool()
	clientCAs.AppendCertsFromPEM(ca.certPEM)
	// t.TempDir() embeds the test name, which overruns the ~104 byte unix
	// socket path limit on darwin, so the socket gets its own short dir.
	socketDir, err := os.MkdirTemp("", "atunnel")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(socketDir) })
	socketPath := filepath.Join(socketDir, "broker.sock")
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{ateletCert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    clientCAs,
	})))
	broker := &credentialBrokerStub{ca: ca, lifetime: lifetime, publicKeys: make(chan []byte, 2), actorUID: "actor-uid"}
	ateletpb.RegisterCredentialBrokerServer(server, broker)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})

	source, err := NewBrokerCertificateSource(BrokerConfig{
		SocketPath:           socketPath,
		CredentialBundlePath: credentialPath,
		TrustBundlePath:      trustPath,
		ExpectedActorUID:     "actor-uid",
	})
	if err != nil {
		t.Fatal(err)
	}
	return source, broker
}

func testAteletIdentity(nodeName string) *substratex509.PodIdentity {
	return &substratex509.PodIdentity{
		Namespace:          "ate-system",
		ServiceAccountName: "atelet",
		ServiceAccountUID:  "atelet-sa-uid",
		PodName:            "atelet",
		PodUID:             "atelet-uid",
		NodeName:           nodeName,
		NodeUID:            "node-uid",
	}
}

func issueTestPodCertificate(t *testing.T, ca *testCA, identity *substratex509.PodIdentity, spiffeID string, usages []x509.ExtKeyUsage) tls.Certificate {
	t.Helper()
	cert := ca.issue(t, spiffeID, usages)
	template, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	if err := substratex509.AddPodIdentityToCertificate(identity, template); err != nil {
		t.Fatal(err)
	}
	key, ok := cert.PrivateKey.(*ecdsa.PrivateKey)
	if !ok {
		t.Fatalf("private key has type %T", cert.PrivateKey)
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatal(err)
	}
	cert.Certificate[0] = der
	cert.Leaf, err = x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return cert
}
