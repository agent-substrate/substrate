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

package credbundle

import (
	"crypto/tls"
	"fmt"
)

// ServerConfig configures PrepareServerTLSConfig.
//
// It is the server-side subset of substrate's internal/credbundle.ServerConfig,
// copied so this module depends only on substrate's public packages.
type ServerConfig struct {
	// GetCertificate presents this side's serving identity, e.g. Loader.
	// Required.
	GetCertificate func(*tls.ClientHelloInfo) (*tls.Certificate, error)

	// ClientCAPath is a PEM file of CA certificates a client certificate must
	// chain to. Empty leaves ClientCAs unset: ClientAuth still applies, but a
	// presented client certificate (if any) chains against the platform root
	// CAs instead of a pinned pool — crypto/tls's own behavior for a nil
	// ClientCAs, not a check this package adds.
	ClientCAPath string

	// ClientAuth is the client certificate policy, e.g.
	// tls.RequireAndVerifyClientCert. Required.
	ClientAuth tls.ClientAuthType

	// VerifyPeer, if set, runs after a successfully chain-verified client
	// certificate, to check an identity the chain check cannot express — a
	// SPIFFE URI SAN, for example. Only consulted when ClientCAPath is set,
	// matching where ClientCAs-based chain verification itself runs.
	VerifyPeer func(tls.ConnectionState) error

	NextProtos []string
}

// PrepareServerTLSConfig builds a *tls.Config for serving TLS whose client
// trust root can rotate during the process's lifetime. It always requires
// TLS 1.3.
//
// A server can rebuild its config per connection via GetConfigForClient, so
// when ClientCAPath is set this reloads the pool (via PoolLoader) there and
// lets the standard library verify the client certificate's chain against it
// — no InsecureSkipVerify or manual verification needed. VerifyPeer layers on
// any identity check beyond that.
func PrepareServerTLSConfig(cfg ServerConfig) (*tls.Config, error) {
	if cfg.GetCertificate == nil {
		return nil, fmt.Errorf("credbundle: GetCertificate is required")
	}

	if cfg.ClientCAPath == "" {
		return &tls.Config{
			MinVersion:     tls.VersionTLS13,
			NextProtos:     cfg.NextProtos,
			GetCertificate: cfg.GetCertificate,
			ClientAuth:     cfg.ClientAuth,
		}, nil
	}

	loadClientCAs := PoolLoader(cfg.ClientCAPath)
	if _, err := loadClientCAs(); err != nil {
		return nil, fmt.Errorf("credbundle: loading trust bundle: %w", err)
	}

	return &tls.Config{
		MinVersion: tls.VersionTLS13,
		// GetConfigForClient's returned Config replaces this one entirely for
		// the handshake, so every field the handshake needs — including
		// NextProtos — must be repeated inside it rather than left here.
		GetConfigForClient: func(*tls.ClientHelloInfo) (*tls.Config, error) {
			clientCAs, err := loadClientCAs()
			if err != nil {
				return nil, err
			}
			var verifyConnection func(tls.ConnectionState) error
			if cfg.VerifyPeer != nil {
				verifyConnection = func(cs tls.ConnectionState) error {
					// Guards against a caller pairing VerifyPeer with a laxer ClientAuth
					// than RequireAndVerifyClientCert.
					if len(cs.PeerCertificates) == 0 {
						return fmt.Errorf("credbundle: client certificate is required")
					}
					return cfg.VerifyPeer(cs)
				}
			}
			return &tls.Config{
				MinVersion:       tls.VersionTLS13,
				NextProtos:       cfg.NextProtos,
				GetCertificate:   cfg.GetCertificate,
				ClientAuth:       cfg.ClientAuth,
				ClientCAs:        clientCAs,
				VerifyConnection: verifyConnection,
			}, nil
		},
	}, nil
}
