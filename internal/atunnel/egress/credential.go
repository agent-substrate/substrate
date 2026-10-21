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

package egress

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

	"go.ate.dev/substrate/internal/ateletdial"
	"go.ate.dev/substrate/internal/proto/ateletpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

	// AteletSPIFFEID is the identity the node-local atelet must present on the
	// credential broker connection. It names atelet's namespace, not this
	// worker's, so it is configured rather than derived from the downward API.
	AteletSPIFFEID string
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

	if cfg.AteletSPIFFEID == "" {
		return nil, fmt.Errorf("egress: expected atelet SPIFFE ID is required")
	}
	tlsConfig, err := ateletdial.TLSConfig(cfg.CredentialBundlePath, cfg.TrustBundlePath, cfg.AteletSPIFFEID)
	if err != nil {
		return nil, fmt.Errorf("egress: %w", err)
	}
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("egress: generate actor private key: %w", err)
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

// mintAttempts bounds the retries of a mint whose broker connection came back
// Unavailable: a node under a restore wave can refuse or drop the fresh connection, and
// a failed mint fails the restore that needs the certificate.
const mintAttempts = 4

// MintWorkerCertificate requests and installs a fresh certificate for the source's existing
// actor key. It returns the new expiry for renewal scheduling. A mint that fails with
// Unavailable is retried a few times with a short back-off; every other error is final.
func (s *BrokerCertificateSource) MintWorkerCertificate(ctx context.Context) (time.Time, error) {
	csr, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{}, s.privateKey)
	if err != nil {
		return time.Time{}, fmt.Errorf("egress: create actor CSR: %w", err)
	}
	for attempt := 1; ; attempt++ {
		expiresAt, err := s.mintOnce(ctx, csr)
		if err == nil {
			return expiresAt, nil
		}
		if attempt >= mintAttempts || status.Code(err) != codes.Unavailable {
			return time.Time{}, err
		}
		select {
		case <-ctx.Done():
			return time.Time{}, err
		case <-time.After(time.Duration(attempt) * 500 * time.Millisecond):
		}
	}
}

// mintOnce is one connection to the credential broker and one mint over it.
func (s *BrokerCertificateSource) mintOnce(ctx context.Context, csr []byte) (time.Time, error) {
	// TODO(identity): We should not be re-establishing a gRPC connection for
	// every mint request.  Do it once at atunnel startup.

	// A fresh connection picks up rotated worker credentials and forces atelet's
	// current certificate and node identity to be verified for every mint.
	conn, err := ateletdial.Dial(s.socketPath, s.tlsConfig)
	if err != nil {
		return time.Time{}, err
	}
	defer conn.Close()
	resp, err := ateletpb.NewWorkerSupportClient(conn).MintActorCertificate(ctx, &ateletpb.MintActorCertificateRequest{
		ActorAtespace:             s.actorAtespace,
		ActorName:                 s.actorName,
		ActorUid:                  s.actorUID,
		CertificateSigningRequest: csr,
	})
	if err != nil {
		return time.Time{}, fmt.Errorf("egress: mint actor certificate: %w", err)
	}

	// TODO(identity): I don't think we need to check all this stuff.  We can
	// trust that ateapi is working properly.

	chain := resp.GetActorCertificates()
	if len(chain) == 0 {
		return time.Time{}, fmt.Errorf("egress: credential broker returned no actor certificate")
	}
	leaf, err := x509.ParseCertificate(chain[0])
	if err != nil {
		return time.Time{}, fmt.Errorf("egress: parse actor certificate: %w", err)
	}
	if !s.privateKey.PublicKey.Equal(leaf.PublicKey) {
		return time.Time{}, fmt.Errorf("egress: actor certificate does not match private key")
	}
	now := time.Now()
	if now.Before(leaf.NotBefore) || !leaf.NotAfter.After(now) {
		return time.Time{}, fmt.Errorf("egress: credential broker returned an invalid actor certificate lifetime")
	}
	if !slices.Contains(leaf.ExtKeyUsage, x509.ExtKeyUsageClientAuth) {
		return time.Time{}, fmt.Errorf("egress: actor certificate cannot authenticate a TLS client")
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
		return nil, fmt.Errorf("egress: no valid actor certificate")
	}
	return s.certificate, nil
}
