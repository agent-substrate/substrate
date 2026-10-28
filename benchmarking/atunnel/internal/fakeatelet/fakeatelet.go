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

// Package fakeatelet serves the part of atelet's WorkerSupport API that atunnel
// calls: minting actor certificates for egress, and actor egress policies for
// the DNS relay. It listens where atelet does, on the node-local socket, with
// atelet's identity, so atunnel's own client code talks to it unchanged.
package fakeatelet

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"go.ate.dev/substrate/benchmarking/atunnel/internal/pki"
	"go.ate.dev/substrate/internal/credbundle"
	"go.ate.dev/substrate/internal/proto/ateletpb"
	"go.ate.dev/substrate/internal/substratex509"
)

// actorCertificateLifetime is how long minted actor certificates last. atunnel
// renews at 90% of it, which no benchmark run reaches.
const actorCertificateLifetime = 24 * time.Hour

// Server is the fake atelet.
type Server struct {
	ateletpb.UnimplementedWorkerSupportServer
	ca    *x509.Certificate
	caKey crypto.Signer
}

// New loads the CA from pkiDir, written by pki.Generate.
func New(pkiDir string) (*Server, error) {
	ca, caKey, err := pki.LoadCA(pkiDir)
	if err != nil {
		return nil, err
	}
	return &Server{ca: ca, caKey: caKey}, nil
}

// Serve listens on socketPath with atelet's certificate, requiring a worker
// certificate signed by the CA, until ctx ends.
func (s *Server) Serve(ctx context.Context, pkiDir, socketPath string) error {
	cert, err := credbundle.Parse(filepath.Join(pkiDir, pki.AteletBundle))
	if err != nil {
		return err
	}
	clientCAs, err := credbundle.ParsePool(filepath.Join(pkiDir, pki.CAFile))
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(socketPath), 0o755); err != nil {
		return err
	}
	_ = os.Remove(socketPath)
	lis, err := net.Listen("unix", socketPath)
	if err != nil {
		return err
	}
	server := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{*cert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    clientCAs,
	})))
	ateletpb.RegisterWorkerSupportServer(server, s)
	context.AfterFunc(ctx, server.Stop)
	return server.Serve(lis)
}

// MintActorCertificate signs the CSR atunnel sends as the actor's identity.
func (s *Server) MintActorCertificate(_ context.Context, req *ateletpb.MintActorCertificateRequest) (*ateletpb.MintActorCertificateResponse, error) {
	csr, err := x509.ParseCertificateRequest(req.GetCertificateSigningRequest())
	if err != nil {
		return nil, fmt.Errorf("parsing CSR: %w", err)
	}
	if err := csr.CheckSignature(); err != nil {
		return nil, fmt.Errorf("CSR signature: %w", err)
	}
	id := &url.URL{Scheme: "spiffe", Host: "substrate-actor.local", Path: "/worker/actor/" + req.GetActorAtespace() + "/" + req.GetActorName()}
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber: pki.Serial(),
		Subject:      pkix.Name{CommonName: req.GetActorName()},
		NotBefore:    now.Add(-time.Minute),
		NotAfter:     now.Add(actorCertificateLifetime),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		URIs:         []*url.URL{id},
	}
	actor := &substratex509.ActorIdentity{Atespace: req.GetActorAtespace(), ActorName: req.GetActorName(), ActorUid: req.GetActorUid()}
	if err := substratex509.AddActorIdentityToCertificate(actor, template); err != nil {
		return nil, err
	}
	der, err := x509.CreateCertificate(rand.Reader, template, s.ca, csr.PublicKey, s.caKey)
	if err != nil {
		return nil, err
	}
	return &ateletpb.MintActorCertificateResponse{ActorCertificates: [][]byte{der}}, nil
}

// GetActorEgressPolicy reports no policy. Only the DNS relay asks, and the
// benchmark's egress goes to addresses, not names.
func (s *Server) GetActorEgressPolicy(context.Context, *ateletpb.GetActorEgressPolicyRequest) (*ateletpb.GetActorEgressPolicyResponse, error) {
	return &ateletpb.GetActorEgressPolicyResponse{}, nil
}
