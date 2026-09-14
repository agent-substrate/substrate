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
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/agent-substrate/substrate/internal/ateletdial"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/internal/substratex509"
)

// BrokerCertificateSource owns atunnel's actor private key and obtains the
// matching short-lived certificate from the node-local atelet.
type BrokerCertificateSource struct {
	socketPath string

	actorAtespace string
	actorName     string
	actorUID      string

	tlsConfig  *tls.Config
	privateKey *ecdsa.PrivateKey

	mu          sync.RWMutex
	certificate *tls.Certificate
}

// BrokerConfig configures the node-local atelet credential broker client.
type BrokerConfig struct {
	// SocketPath is the atelet-owned Unix socket shared with this worker.
	SocketPath string
	// CredentialBundlePath is the worker Pod certificate and private key used
	// only to authenticate atunnel to atelet.
	CredentialBundlePath string
	// TrustBundlePath verifies atelet's Pod certificate.
	TrustBundlePath string

	// Which actor are we currently running?
	ActorAtespace string
	ActorName     string
	ActorUID      string
}

// NewBrokerCertificateSource creates one actor key for this activation. The key
// is reused across renewals and never leaves atunnel; only its CSR crosses the
// credential broker socket.
func NewBrokerCertificateSource(cfg BrokerConfig) (*BrokerCertificateSource, error) {
	if cfg.SocketPath == "" || cfg.CredentialBundlePath == "" || cfg.TrustBundlePath == "" {
		return nil, fmt.Errorf("credential broker socket, credentials, trust bundle are required")
	}

	if cfg.ActorAtespace == "" || cfg.ActorName == "" || cfg.ActorUID == "" {
		return nil, fmt.Errorf("actor information is required")
	}

	tlsConfig, err := ateletdial.TLSConfig(cfg.CredentialBundlePath, cfg.TrustBundlePath)
	if err != nil {
		return nil, fmt.Errorf("atunnel: %w", err)
	}
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("atunnel: generate actor private key: %w", err)
	}

	return &BrokerCertificateSource{
		socketPath:    cfg.SocketPath,
		actorAtespace: cfg.ActorAtespace,
		actorName:     cfg.ActorName,
		actorUID:      cfg.ActorUID,
		tlsConfig:     tlsConfig,
		privateKey:    privateKey,
	}, nil
}

// MintAteomCertificate requests and installs a fresh certificate for the source's existing
// actor key. It returns the new expiry for renewal scheduling.
func (s *BrokerCertificateSource) MintAteomCertificate(ctx context.Context) (time.Time, error) {
	csr, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{}, s.privateKey)
	if err != nil {
		return time.Time{}, fmt.Errorf("atunnel: create actor CSR: %w", err)
	}

	// TODO(identity): We should not be re-establishing a gRPC connection for
	// every mint request.  Do it once at atunnel startup.

	// A fresh connection picks up rotated worker credentials and forces atelet's
	// current certificate and node identity to be verified for every mint.
	conn, err := ateletdial.Dial(s.socketPath, s.tlsConfig)
	if err != nil {
		return time.Time{}, err
	}
	defer conn.Close()
	resp, err := ateletpb.NewCredentialBrokerClient(conn).MintActorCertificate(ctx, &ateletpb.MintActorCertificateRequest{
		ActorAtespace:             s.actorAtespace,
		ActorName:                 s.actorName,
		ActorUid:                  s.actorUID,
		CertificateSigningRequest: csr,
	})
	if err != nil {
		return time.Time{}, fmt.Errorf("atunnel: mint actor certificate: %w", err)
	}
	chain := resp.GetActorCertificates()
	if len(chain) == 0 {
		return time.Time{}, fmt.Errorf("atunnel: credential broker returned no actor certificate")
	}
	leaf, err := x509.ParseCertificate(chain[0])
	if err != nil {
		return time.Time{}, fmt.Errorf("atunnel: parse actor certificate: %w", err)
	}
	if !s.privateKey.PublicKey.Equal(leaf.PublicKey) {
		return time.Time{}, fmt.Errorf("atunnel: actor certificate does not match private key")
	}
	now := time.Now()
	if now.Before(leaf.NotBefore) || !leaf.NotAfter.After(now) {
		return time.Time{}, fmt.Errorf("atunnel: credential broker returned an invalid actor certificate lifetime")
	}
	if !slices.Contains(leaf.ExtKeyUsage, x509.ExtKeyUsageClientAuth) {
		return time.Time{}, fmt.Errorf("atunnel: actor certificate cannot authenticate a TLS client")
	}
	identity, err := substratex509.ActorIdentityFromCertificate(leaf)
	if err != nil || identity == nil {
		return time.Time{}, fmt.Errorf("atunnel: actor certificate has no valid actor identity")
	}
	if identity.Purpose != substratex509.ActorIdentityPurposeAtunnel {
		return time.Time{}, fmt.Errorf("atunnel: actor certificate is not scoped to atunnel")
	}
	if identity.ActorUid != s.actorUID {
		return time.Time{}, fmt.Errorf("atunnel: actor certificate is for an unexpected actor")
	}
	cert := &tls.Certificate{Certificate: chain, PrivateKey: s.privateKey, Leaf: leaf}
	s.mu.Lock()
	s.certificate = cert
	s.mu.Unlock()
	return leaf.NotAfter, nil
}

// GetClientCertificate supplies the current actor certificate to the egress
// gateway TLS handshake and refuses to use it after expiry.
func (s *BrokerCertificateSource) GetClientCertificate(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.certificate == nil || !s.certificate.Leaf.NotAfter.After(time.Now()) {
		return nil, fmt.Errorf("atunnel: no valid actor certificate")
	}
	return s.certificate, nil
}
